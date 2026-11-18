using System.Buffers;
using System.Linq.Expressions;
using System.Reflection;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// clearOnDispose (ToAsyncBuffer) and clearOnReturn (the two streams):
// the rows are cleared as each buffer goes back to the pool, off the
// read path, through the internal recycle bin.
// All the tests that opt in live in this one class: they share the bin
// (a process-wide static), one of them blocks it, and one of them sets
// its test hook, so they must not run beside each other. Everything else
// in the suite passes false and never touches the bin.
[ExcludeFromCodeCoverage]
[Collection("ChunkSizing")]
public static class ClearOnReturnTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public static TheoryData<string> Terminals
		=> ["buffer", "stream", "buffered"];

	// ---- reaching the internal bin (no InternalsVisibleTo) ----------------

	static readonly Type Bin = typeof(PooledBuffer<>).Assembly
		.GetType("Open.Database.Extensions.Core.RecycleBin")!;

	const BindingFlags Internal
		= BindingFlags.Static | BindingFlags.NonPublic;

	static int _hookCalls;

	internal static void CountPost()
		=> Interlocked.Increment(ref _hookCalls);

	static void Post(Action action)
		=> Bin.GetMethod("Post", Internal)!.Invoke(null, [action]);

	// Makes the bin answer every post with "Full", or throw ("Throw"),
	// or count the posts and answer "Full" ("Count"), or back to normal
	// with null.
	static void SetHook(string? result)
	{
		FieldInfo field = Bin.GetField("_hook", Internal)!;
		if (result is null)
		{
			field.SetValue(null, null);
			return;
		}

		Type outcome = Bin.GetNestedType(
			"PostResult",
			BindingFlags.NonPublic)!;
		Type hookType = typeof(Func<,>)
			.MakeGenericType(typeof(Action), outcome);
		ParameterExpression action = Expression.Parameter(typeof(Action));
		Expression full = Expression.Constant(
			Enum.Parse(outcome, "Full"), outcome);
		Expression body = result switch
		{
			"Throw" => Expression.Block(
				outcome,
				Expression.Throw(
					Expression.New(typeof(InvalidOperationException))),
				full),
			"Count" => Expression.Block(
				Expression.Call(
					typeof(ClearOnReturnTests)
						.GetMethod(
							nameof(CountPost),
							Internal)!),
				full),
			_ => full,
		};
		field.SetValue(
			null,
			Expression.Lambda(hookType, body, action).Compile());
	}

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
		MemoryPool<T> pool,
		bool clear)
	{
		int rows = 0;
		switch (terminal)
		{
			case "buffer":
				{
					await using var buffer = view.ToAsyncBuffer(
						pool, clearOnDispose: clear, Ct);
					rows = (await buffer.Items.Within()).Count;
					break;
				}

			case "stream":
				await foreach (var chunk in view.ToAsyncBufferStream(
					pool, clearOnReturn: clear, Ct))
				{
					rows += chunk.Length;
				}

				break;

			default:
				await foreach (var _ in view.ToBufferedAsyncStream(
					pool, clearOnReturn: clear, Ct))
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

	// ---- the default path never starts the bin -----------------------------

	[Theory, MemberData(nameof(Terminals))]
	public static async Task TheDefaultPath_NeverPostsToTheBin(string terminal)
	{
		var pool = new CountingPool<ChannelRow>();
		int before = Volatile.Read(ref _hookCalls);
		SetHook("Count");
		try
		{
			Assert.Equal(
				100,
				await ReadAll(terminal, Rows(100), pool, clear: false));
			Assert.Equal(
				100,
				await ReadAll(
					terminal,
					Rows(100),
					MemoryPool<ChannelRow>.Shared,
					false));
		}
		finally
		{
			SetHook(null);
		}

		AssertBalanced(pool);
		Assert.Equal(before, Volatile.Read(ref _hookCalls));
	}

	// The counting hook does count: an opt-in read posts to the bin.
	[Fact]
	public static async Task TheCountingHook_SeesAnOptInRead()
	{
		var pool = RowPool();
		int before = Volatile.Read(ref _hookCalls);
		SetHook("Count");
		try
		{
			await ReadAll("stream", Rows(100), pool, true);
		}
		finally
		{
			SetHook(null);
		}

		Assert.True(Volatile.Read(ref _hookCalls) > before);

		// "Count" answers Full: the clears ran inline.
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

		// Only the used slice is cleared: the sentinel in the unused
		// tail of every (larger) array is untouched.
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
		// slice (the unused tail keeps its -1).
		int[][] arrays = pool.Captured;
		int total = arrays.Sum(a => a.Length);
		Assert.Equal(100, arrays.Sum(a => a.Count(v => v == 0)));
		Assert.Equal(total - 100, arrays.Sum(a => a.Count(v => v == -1)));
	}

	// ---- the consumer's read path does not clear ----------------------------

	[Fact]
	public static async Task TheConsumer_DoesNotWaitForTheClear()
	{
		using var gate = new ManualResetEventSlim(false);
		var pool = RowPool();
		try
		{
			// The bin's single loop is busy with this, and the clears
			// queue up behind it.
			Post(() => gate.Wait(TimeSpan.FromSeconds(30)));

			int rows = await ReadAll("stream", Rows(100), pool, true)
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

	// ---- a full bin clears inline, a closed bin drops -----------------------

	[Theory, MemberData(nameof(Terminals))]
	public static async Task AFullBin_ClearsInline(string terminal)
	{
		var pool = RowPool();
		SetHook("Full");
		try
		{
			Assert.Equal(
				100,
				await ReadAll(terminal, Rows(100), pool, true));

			// Nothing was left to a worker: it is all done by the time
			// the read is.
			AssertBalanced(pool);
			Assert.Equal(0, pool.DirtyReturns);
			Assert.Equal(
				100,
				pool.Captured.Sum(a => a.Count(r => r is null)));
		}
		finally
		{
			SetHook(null);
		}
	}

	// Whatever goes wrong posting, the caller clears inline.
	[Theory, MemberData(nameof(Terminals))]
	public static async Task ABinThatThrows_ClearsInline(string terminal)
	{
		var pool = RowPool();
		SetHook("Throw");
		try
		{
			Assert.Equal(
				100,
				await ReadAll(terminal, Rows(100), pool, true));
		}
		finally
		{
			SetHook(null);
		}

		AssertBalanced(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// ---- the bin survives bad actions ---------------------------------------

	[Fact]
	public static async Task TheBin_SurvivesAnActionThatThrows()
	{
		Post(() => throw new InvalidOperationException("a bad action"));

		TaskCompletionSource ran = new(
			TaskCreationOptions.RunContinuationsAsynchronously);
		Post(() => ran.TrySetResult());

		// The next item still runs.
		await ran.Task.Within();
	}

	[Fact]
	public static async Task TheBin_SurvivesAPoolWhoseReturnThrows()
	{
		var broken = new CountingPool<ChannelRow> { ThrowOnReturn = true };

		// The dispose posts the clear; the pool's failure stays in the
		// bin, and never reaches the caller.
		await ReadAll("buffer", Rows(100), broken, true);
		await Until(() => broken.Returns > 0, "the bin ran");

		var pool = RowPool();
		Assert.Equal(100, await ReadAll("stream", Rows(100), pool, true));
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

		await foreach (var chunk in view.ToAsyncBufferStream(
			pool, clearOnReturn: true, Ct))
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
		var buffer = view.ToAsyncBuffer(pool, clearOnDispose: true, Ct);
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
		int before = Volatile.Read(ref _hookCalls);
		SetHook("Count");
		try
		{
			Assert.Equal(
				rows,
				await ReadAll(terminal, Rows(rows), pool, true));
		}
		finally
		{
			SetHook(null);
		}

		Assert.Equal(chunks, Volatile.Read(ref _hookCalls) - before);
		Assert.Equal(chunks, pool.Rents);
		Assert.Equal(0, pool.DirtyReturns);
		Assert.Equal(pool.Rents, pool.Returns);
	}

	[Fact]
	public static async Task ATakeOfZero_RentsNothing()
	{
		var pool = RowPool();

		await using var buffer = Rows(100).Take(0).ToAsyncBuffer(
			pool, clearOnDispose: true, Ct);
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

		Assert.Equal(100, await Count(v.ToAsyncBuffer(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToAsyncBufferStream(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToBufferedAsyncStream(Own(), true, Ct)));
	}

	[Fact]
	public static async Task EveryOverload_OfTheSkipView_Binds()
	{
		var v = new FakeExecuteReader(() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.Skip(0);

		Assert.Equal(100, await Count(v.ToAsyncBuffer(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToAsyncBufferStream(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToBufferedAsyncStream(Own(), true, Ct)));
	}

	[Fact]
	public static async Task EveryOverload_OfTheTakeView_Binds()
	{
		var v = new FakeExecuteReader(() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.Take(1000);

		Assert.Equal(100, await Count(v.ToAsyncBuffer(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToAsyncBufferStream(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToBufferedAsyncStream(Own(), true, Ct)));
	}

	[Fact]
	public static async Task EveryOverload_OfTheCommandView_Binds()
	{
		var v = Rows(100);

		Assert.Equal(100, await Count(v.ToAsyncBuffer(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBuffer(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToAsyncBufferStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToAsyncBufferStream(Own(), true, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(false, Ct)));
		Assert.Equal(100, await Count(v.ToBufferedAsyncStream(Own(), Ct)));
		Assert.Equal(
			100, await Count(v.ToBufferedAsyncStream(Own(), true, Ct)));
	}

	// null does not convert to bool or to CancellationToken, so it can
	// only be the pool: unambiguous, and refused.
	[Fact]
	public static void ALiteralNull_BindsToThePoolOverload_AndIsRefused()
	{
		var v = Rows(100);

		Assert.Throws<ArgumentNullException>(() => v.ToAsyncBuffer(null!, Ct));
		Assert.Throws<ArgumentNullException>(
			() => v.ToAsyncBufferStream(null!, Ct));
		Assert.Throws<ArgumentNullException>(
			() => v.ToBufferedAsyncStream(null!, Ct));
	}
}
