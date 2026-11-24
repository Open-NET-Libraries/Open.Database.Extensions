using System.Buffers;
using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The rows of a chunk are cleared as it goes back to the pool, off the
// read path: a chunk of ChunkConfig.DeferFromClearedBytes bytes or more through the
// recycle bin, a smaller one where it is released. These tests give every
// query its own configuration, with deferFromClearedBytes of 1 so that every chunk
// goes through a bin: one the test owns where it counts, refuses or breaks
// posts, otherwise the process-wide default bin. Nothing global is changed,
// so they can run beside everything else.
// The immediate path (small chunks) is in ArrayLeaseTests.
[ExcludeFromCodeCoverage]
public static class ClearOnReturnTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public static TheoryData<string> Terminals
		=> ["buffer", "stream", "buffered"];

	// The query, renting from the pool, clearing as asked, and sending every
	// chunk to the bin (deferFromClearedBytes 1).
	static MappedCommand<T> Pooled<T>(
		MappedCommand<T> view,
		ArrayPool<T> pool,
		bool? clearOnReturn,
		RecycleBin? bin = null)
		=> view.WithConfig(TestConfig.Of(pool, clearOnReturn, bin, deferFromClearedBytes: 1));

	// ---- rigs ---------------------------------------------------------------

	static MappedCommand<ChannelRow> Rows(int n)
		=> new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(n))
			.Map<ChannelRow>()
			.AsyncReads();

	static MappedCommand<int> Ints(int n)
		=> new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(n))
			.Map<int>()
			.AsyncReads();

	// A pool that sees any row that is left in an array it gets back.
	static CountingPool<ChannelRow> RowPool(bool roundUp = false)
		=> new()
		{
			RoundUp = roundUp,
			Sentinel = static () => new ChannelRow(-1),
			IsDirty = static a => a.Any(r => r is { Id: > 0 }),
		};

	static async Task<int> ReadAll<T>(
		string terminal,
		MappedCommand<T> view,
		ArrayPool<T> pool,
		bool? clearOnReturn,
		RecycleBin? bin = null)
	{
		MappedCommand<T> pooled = Pooled(view, pool, clearOnReturn, bin);
		int rows = 0;
		switch (terminal)
		{
			case "buffer":
				{
					await using var buffer = pooled.ToPooledBuffer(Ct);
					rows = (await buffer.Items.Within()).Count;
					break;
				}

			case "stream":
				await foreach (var chunk in pooled.ToPooledBufferStream(Ct))
				{
					rows += chunk.Length;
				}

				break;

			default:
				await foreach (var _ in pooled.ToPooledStream(Ct))
				{
					++rows;
				}

				break;
		}

		return rows;
	}

	static void AssertBalanced<T>(CountingPool<T> pool)
	{
		Assert.True(pool.Rents > 0, "nothing was rented");
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
		Assert.Equal(0, pool.Outstanding);
	}

	// ---- a read that clears nothing never posts to the bin ------------------

	[Theory, MemberData(nameof(Terminals))]
	public static async Task AReadThatClearsNothing_NeverPostsToTheBin(
		string terminal)
	{
		// Rows with no references, clearOnReturn false: nothing to clear,
		// so every array goes straight back.
		var pool = new CountingPool<int>();
		var bin = new TestBin();

		Assert.Equal(
			100,
			await ReadAll(terminal, Ints(100), pool, false, bin));

		AssertBalanced(pool);
		Assert.Equal(0, bin.Posts);
	}

	// The counting bin does count: an opt-in read posts to it.
	[Fact]
	public static async Task TheCountingBin_SeesAnOptInRead()
	{
		var pool = RowPool();
		var bin = new TestBin { Mode = "Full" };

		await ReadAll("stream", Rows(100), pool, true, bin);

		Assert.True(bin.Posts > 0);

		// "Full" refuses: the arrays were cleared where they were released,
		// and returned clean.
		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// Rows that hold references are cleared whatever was asked (the floor), also
	// on the bin path: a chunk sent to the bin is cleared there, not returned dirty.
	[Theory]
	[InlineData(false)]
	[InlineData(null)]
	public static async Task ReferenceRows_AreClearedInTheBin_EvenWhenNotAskedTo(
		bool? clearOnReturn)
	{
		var pool = RowPool(roundUp: true);
		var bin = new TestBin();

		Assert.Equal(
			100,
			await ReadAll("stream", Rows(100), pool, clearOnReturn, bin));
		await Until(() => pool.Outstanding == 0, "the arrays back");

		Assert.True(bin.Posts > 0);
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// ---- opting in: every returned array is clean ---------------------------

	[Theory, MemberData(nameof(Terminals))]
	public static async Task OptingIn_ClearsTheUsedRows_OfAReferenceType(
		string terminal)
	{
		var pool = RowPool(roundUp: true);

		Assert.Equal(100, await ReadAll(terminal, Rows(100), pool, true));
		await Until(() => pool.Outstanding == 0, "the arrays back");

		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);

		// Only the used rows are cleared: the sentinel in the unused
		// tail of every (longer) array is untouched.
		ChannelRow?[][] arrays = pool.Captured;
		int total = arrays.Sum(a => a.Length);
		Assert.Equal(100, arrays.Sum(a => a.Count(r => r is null)));
		Assert.Equal(
			total - 100,
			arrays.Sum(a => a.Count(r => r is { Id: -1 })));
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task OptingIn_ClearsTheUsedRows_OfAValueType(
		string terminal)
	{
		var pool = new CountingPool<int>
		{
			RoundUp = true,
			Sentinel = static () => -1,
			IsDirty = static a => a.Any(v => v > 0),
		};

		Assert.Equal(100, await ReadAll(terminal, Ints(100), pool, true));
		await Until(() => pool.Outstanding == 0, "the arrays back");

		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);

		// Every type is cleared, value types included, and only the used
		// rows (the unused tail keeps its -1).
		int[][] arrays = pool.Captured;
		int total = arrays.Sum(a => a.Length);
		Assert.Equal(100, arrays.Sum(a => a.Count(v => v == 0)));
		Assert.Equal(total - 100, arrays.Sum(a => a.Count(v => v == -1)));
	}

	// ---- the consumer's read path does not clear ----------------------------

	[Fact]
	public static async Task TheConsumer_DoesNotWaitForTheClear()
	{
		using var started = new ManualResetEventSlim(false);
		using var gate = new ManualResetEventSlim(false);
		var bin = new TestBin();
		var pool = RowPool();
		try
		{
			// The bin's single loop is busy with this, and the clears
			// queue up behind it.
			bin.Post(TestBin.Item(new TestBin.GateRunner(started, gate)));
			Assert.True(started.Wait(TimeSpan.FromSeconds(10), Ct));

			int rows = await ReadAll("stream", Rows(100), pool, true, bin)
				.Within();

			// The stream completed, and returned nothing: the consumer
			// neither cleared nor waited for the bin.
			Assert.Equal(100, rows);
			Assert.Equal(0, pool.Returns);
			Assert.True(pool.Outstanding > 0);
		}
		finally
		{
			gate.Set();
		}

		await Until(() => pool.Outstanding == 0, "the queue drained");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// ---- a full bin discards ------------------------------------------------

	// A bin that is full or broken does not lose the array: the code that
	// releases it clears it and returns it, as it does for a small chunk. A
	// dirty array never reaches a pool, and none is left behind with its rows.
	[Theory, MemberData(nameof(Terminals))]
	public static async Task AFullBin_ClearsInline_AndReturnsTheArrays(
		string terminal)
	{
		var pool = RowPool();
		var bin = new TestBin { Mode = "Full" };

		Assert.Equal(
			100,
			await ReadAll(terminal, Rows(100), pool, true, bin));

		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
		Assert.True(bin.Overflowed > 0);
	}

	// Whatever goes wrong posting, the array is cleared and returned here.
	[Theory, MemberData(nameof(Terminals))]
	public static async Task ABinThatThrows_ClearsInline_AndReturnsTheArrays(
		string terminal)
	{
		var pool = RowPool();
		var bin = new TestBin { Mode = "Throw" };

		Assert.Equal(
			100,
			await ReadAll(terminal, Rows(100), pool, true, bin));

		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
		Assert.True(bin.Overflowed > 0);
	}
	// ---- the bin survives bad work ------------------------------------------

	[Fact]
	public static async Task TheBin_SurvivesAnItemThatThrows()
	{
		var bin = new TestBin();
		bin.Post(TestBin.Item(new TestBin.ActionRunner(
			() => throw new InvalidOperationException("a bad item"))));

		TaskCompletionSource ran = new(
			TaskCreationOptions.RunContinuationsAsynchronously);
		bin.Post(TestBin.Item(new TestBin.ActionRunner(() => ran.TrySetResult())));

		// The next item still runs.
		await ran.Task.Within();
	}

	[Fact]
	public static async Task TheBin_SurvivesAPoolWhoseReturnThrows()
	{
		var bin = new TestBin();
		var broken = new CountingPool<ChannelRow> { ThrowOnReturn = true };

		// The release posts the clear; the pool's failure stays in the
		// bin, and never reaches the caller.
		await ReadAll("buffer", Rows(100), broken, true, bin);
		await Until(() => broken.Returns > 0, "the bin ran");

		var pool = RowPool();
		Assert.Equal(100, await ReadAll("stream", Rows(100), pool, true, bin));
		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// ---- the writer decides: chunks that never reach a consumer --------------

	// Serves some rows, then stalls until the token it was given fires.
	sealed class StallReader(int rows, int stallAfter)
		: TypedReader(ReaderFixtures.Columns, ReaderFixtures.Rows(rows))
	{
		int _reads;
		volatile bool _stalled;

		public bool Stalled => _stalled;

		public override async Task<bool> ReadAsync(
			CancellationToken cancellationToken)
		{
			if (++_reads > stallAfter)
			{
				_stalled = true;
				await Task.Delay(Timeout.Infinite, cancellationToken);
			}

			return await base.ReadAsync(cancellationToken);
		}
	}

	static MappedCommand<ChannelRow> Stalling(
		Action<StallReader> created)
		=> new FakeDbCommand(
			new FakeDbConnection(),
			() =>
			{
				var reader = new StallReader(100, 40);
				created(reader);
				return reader;
			})
			.Map<ChannelRow>()
			.AsyncReads();

	[Fact]
	public static async Task ABreakMidStream_ClearsTheLeftoverChunks()
	{
		StallReader? reader = null;
		var view = Stalling(r => reader = r);
		var pool = RowPool();

		await foreach (var chunk in Pooled(view, pool, true).ToPooledBufferStream(Ct))
		{
			// The partial chunk after the cancel is queued behind this
			// one: it holds rows, and is released by the drain.
			await Until(() => reader is { Stalled: true }, "the stall");
			break;
		}

		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static async Task ADisposeWhileFilling_ClearsTheRefusedAndTheKeptChunks()
	{
		StallReader? reader = null;
		var view = Stalling(r => reader = r);
		var pool = RowPool();
		var buffer = Pooled(view, pool, true).ToPooledBuffer(Ct);
		await Until(() => reader is { Stalled: true }, "the stall");

		// The published chunk is kept; the partial one is sealed after
		// the dispose, refused, and released by the producer.
		await buffer.DisposeAsync().AsTask().Within();

		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffer")]
	public static async Task AFaultMidRead_ClearsEverythingItHeld(
		string terminal)
	{
		var pool = RowPool();
		var view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(40, failAtRow: 30))
			.Map<ChannelRow>();

		await Assert.ThrowsAsync<InvalidOperationException>(
			() => ReadAll(terminal, view, pool, true));

		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// ---- the boundaries: one post per chunk ---------------------------------

	[Theory]
	[InlineData("stream", 0, 0)]
	[InlineData("stream", 1, 1)]
	[InlineData("stream", 16, 1)]
	[InlineData("stream", 17, 2)]
	[InlineData("stream", 48, 2)]
	[InlineData("buffered", 17, 2)]
	[InlineData("buffer", 0, 0)]
	[InlineData("buffer", 1, 1)]
	[InlineData("buffer", 16, 1)]
	[InlineData("buffer", 17, 2)]
	[InlineData("buffer", 48, 2)]
	public static async Task Clearing_PostsOncePerChunk_AtTheBoundaries(
		string terminal,
		int rows,
		int chunks)
	{
		var pool = RowPool();
		var bin = new TestBin { Mode = "Full" };

		Assert.Equal(
			rows,
			await ReadAll(terminal, Rows(rows), pool, true, bin));

		Assert.Equal(chunks, bin.Posts);
		Assert.Equal(chunks, pool.Rents);

		// "Full" refuses, so every array was cleared and returned here.
		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.Equal(chunks, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static async Task ATakeOfZero_RentsNothing()
	{
		var pool = RowPool();

		await using var buffer = Rows(100).Take(0).UsePool(pool, clearOnReturn: true).ToPooledBuffer(Ct);
		Assert.Empty(await buffer.Items.Within());

		Assert.Equal(0, pool.Rents);
	}
	// ---- the overloads bind -------------------------------------------------

	static async Task<int> Count(PooledBuffer<ChannelRow> buffer)
	{
		await using (buffer)
			return (await buffer.Items.Within()).Count;
	}

	static async Task<int> Count(
		IAsyncEnumerable<ReadOnlyMemory<ChannelRow>> chunks)
	{
		int rows = 0;
		await foreach (var chunk in chunks) rows += chunk.Length;

		return rows;
	}

	static async Task<int> Count(IAsyncEnumerable<ChannelRow> items)
	{
		int rows = 0;
		await foreach (var _ in items) ++rows;

		return rows;
	}

	static CountingPool<ChannelRow> Own()
		=> new();

	[Fact]
	public static async Task EveryOverload_OfTheQueryView_Binds()
	{
		var v = new FakeExecuteReader(() => new ChannelTestReader(100))
			.Map<ChannelRow>();

		Assert.Equal(100, await Count(v.ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBufferStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.ToPooledStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledStream(Ct)));
	}

	[Fact]
	public static async Task EveryOverload_OfTheSkipView_Binds()
	{
		var v = new FakeExecuteReader(() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.Skip(0);

		Assert.Equal(100, await Count(v.ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBufferStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.ToPooledStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledStream(Ct)));
	}

	[Fact]
	public static async Task EveryOverload_OfTheTakeView_Binds()
	{
		var v = new FakeExecuteReader(() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.Take(1000);

		Assert.Equal(100, await Count(v.ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBufferStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.ToPooledStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledStream(Ct)));
	}

	[Fact]
	public static async Task EveryOverload_OfTheCommandView_Binds()
	{
		var v = Rows(100);

		Assert.Equal(100, await Count(v.ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBuffer(Ct)));
		Assert.Equal(100, await Count(v.ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledBufferStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledBufferStream(Ct)));
		Assert.Equal(100, await Count(v.ToPooledStream(Ct)));
		Assert.Equal(100, await Count(v.UsePool(Own(), clearOnReturn: false).ToPooledStream(Ct)));
		Assert.Equal(
			100, await Count(v.UsePool(Own(), clearOnReturn: true).ToPooledStream(Ct)));
	}

	// null does not convert to bool or to CancellationToken, so it can
	// only be the pool: unambiguous, and refused.
	[Fact]
	public static void ALiteralNull_BindsToThePoolOverload_AndIsRefused()
	{
		var v = Rows(100);

		Assert.Throws<ArgumentNullException>(() => v.UsePool(null!, clearOnReturn: false).ToPooledBuffer(Ct));
		Assert.Throws<ArgumentNullException>(
			() => v.UsePool(null!, clearOnReturn: false).ToPooledBufferStream(Ct));
		Assert.Throws<ArgumentNullException>(
			() => v.UsePool(null!, clearOnReturn: false).ToPooledStream(Ct));
	}
}
