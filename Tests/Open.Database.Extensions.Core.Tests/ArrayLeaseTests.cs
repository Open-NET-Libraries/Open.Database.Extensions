using System.Buffers;
using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A lease is a pooled array and the rows written to it, released once. What
// releasing does is decided when it is sealed, from the number of rows:
// - nothing to clear: the array goes straight back;
// - a small chunk: the rows are cleared where it is released;
// - a large chunk: it is posted to the recycle bin;
// and whichever clears, only the rows that were written are cleared: when
// that is the whole array the pool is asked to do it (Return with the flag),
// otherwise the rows are cleared with a span and the array goes back without.
[ExcludeFromCodeCoverage]
public static class ArrayLeaseTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A pool of Row arrays that holds -1 in every fresh slot: a slot with an
	// Id above 0 is a row that was written and not cleared.
	static CountingPool<ChannelRow> Pool(bool exact = false)
		=> new()
		{
			Exact = exact,
			RoundUp = !exact,
			Sentinel = static () => new ChannelRow(-1),
			IsDirty = static a => a.Any(r => r is { Id: > 0 }),
		};

	static ChannelRow[] Written(CountingPool<ChannelRow> pool, int size, int rows)
	{
		ChannelRow[] array = pool.Rent(size);
		for (int i = 0; i < rows; ++i) array[i] = new ChannelRow(i + 1);

		return array;
	}

	// ---- no clearing ----------------------------------------------------------

	[Fact]
	public static void WithNoClearing_ValueRows_GoStraightBack_RowsAndAll()
	{
		var pool = new CountingPool<int>
		{
			RoundUp = true,
			IsDirty = static a => a.Any(v => v > 0),
		};
		var bin = new TestBin();
		var config = TestConfig.Of<int>(pool, false, bin);
		int[] array = pool.Rent(16);
		for (int i = 0; i < 5; ++i) array[i] = i + 1;

		config.Seal(array, 5).Release();

		Assert.Equal(1, pool.Returns);
		Assert.Equal(0, pool.FlagClears);
		Assert.Equal(1, pool.DirtyReturns); // The rows were left, as asked.
		Assert.Equal(0, bin.Posts);
	}

	[Fact]
	public static void ReferenceRows_AreClearedWhateverIsAsked_NeverStraightBack()
	{
		var pool = Pool();
		var bin = new TestBin();
		var config = TestConfig.Of<ChannelRow>(pool, false, bin);
		ChannelRow[] array = Written(pool, 16, 5);

		config.Seal(array, 5).Release();

		Assert.Equal(1, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
		Assert.All(array.Take(5), r => Assert.Null(r));
	}
	[Fact]
	public static void NothingWritten_ReturnsWithoutClearing_WhateverTheRule()
	{
		var pool = Pool();
		var bin = new TestBin();
		var config = TestConfig.Of<ChannelRow>(pool, true, bin, 1);
		ChannelRow[] array = Written(pool, 16, 0);

		config.Seal(array, 0).Release();

		Assert.Equal(1, pool.Returns);
		Assert.Equal(0, pool.FlagClears);
		Assert.Equal(0, pool.DirtyReturns);
		Assert.Equal(0, bin.Posts);
	}

	// ---- clearing where it is released (a small chunk) -------------------------

	[Fact]
	public static void WhenTheWholeArrayWasUsed_ThePoolIsAskedToClearIt()
	{
		var pool = Pool(exact: true);
		var config = TestConfig.Of<ChannelRow>(pool, true);
		ChannelRow[] array = Written(pool, 16, 16);

		config.Seal(array, 16).Release();

		Assert.Equal(1, pool.Returns);
		Assert.Equal(1, pool.FlagClears);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static void WhenPartOfTheArrayWasUsed_OnlyThoseRowsAreCleared_AndNoFlagIsUsed()
	{
		var pool = Pool();
		var config = TestConfig.Of<ChannelRow>(pool, true);
		ChannelRow[] array = Written(pool, 16, 5);
		Assert.True(array.Length > 5);

		config.Seal(array, 5).Release();

		Assert.Equal(1, pool.Returns);
		Assert.Equal(0, pool.FlagClears);
		Assert.Equal(0, pool.DirtyReturns);

		// The five rows are gone; every slot after them still holds the
		// sentinel, so nothing past the rows was touched.
		Assert.All(array.Take(5), r => Assert.Null(r));
		Assert.All(array.Skip(5), r => Assert.Equal(-1, r!.Id));
	}

	// ---- clearing in the bin (a large chunk) ---------------------------------

	[Fact]
	public static async Task ALargeChunk_IsPostedToTheBin_AndClearedThere()
	{
		var pool = Pool();
		var bin = new TestBin();
		var config = TestConfig.Of<ChannelRow>(pool, true, bin, 8);
		ChannelRow[] array = Written(pool, 16, 10);

		config.Seal(array, 10).Release();

		Assert.Equal(1, bin.Posts);
		await Until(() => pool.Returns == 1, "the bin ran");
		Assert.Equal(0, pool.DirtyReturns);
		Assert.Equal(0, pool.Unmatched);

		// Cleared there as the inline release does: the ten rows are gone and
		// every slot after them still holds the sentinel.
		Assert.All(array.Take(10), r => Assert.Null(r));
		Assert.All(array.Skip(10), r => Assert.Equal(-1, r!.Id));
	}

	[Fact]
	public static void TheDeferThreshold_IsTheMeasuredFourThousandNinetySix()
		=> Assert.Equal(4096, ChunkConfig<ChannelRow>.DefaultDeferFromRows);

	[Fact]
	public static void AChunkOfExactlyTheThreshold_IsPosted()
	{
		var pool = Pool();
		var bin = new TestBin { Mode = "Full" };
		var config = TestConfig.Of<ChannelRow>(pool, true, bin, 8);
		ChannelRow[] array = Written(pool, 16, 8);

		config.Seal(array, 8).Release();

		Assert.Equal(1, bin.Posts);
	}

	[Fact]
	public static void ASmallChunk_IsNeverPosted()
	{
		var pool = Pool();
		var bin = new TestBin();
		var config = TestConfig.Of<ChannelRow>(pool, true, bin, 8);
		ChannelRow[] array = Written(pool, 16, 7);

		config.Seal(array, 7).Release();

		Assert.Equal(0, bin.Posts);
		Assert.Equal(1, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static void AFullBin_ClearsTheArrayInline_AndReturnsItClean()
	{
		var pool = Pool();
		var bin = new TestBin { Mode = "Full" };
		var config = TestConfig.Of<ChannelRow>(pool, true, bin, 1);
		ChannelRow[] array = Written(pool, 16, 5);

		config.Seal(array, 5).Release();

		Assert.Equal(1, bin.Posts);
		Assert.Equal(1, bin.Overflowed);
		Assert.Equal(1, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
		Assert.Equal(0, pool.Outstanding);
		Assert.All(array.Take(5), r => Assert.Null(r));
	}

	[Fact]
	public static void ALargeChunkOfValueRows_IsClearedInline_NeverPosted()
	{
		// The bin pays only for a large clear of rows that hold references:
		// value rows are cleared where they are released, whatever the size.
		var pool = new CountingPool<int>
		{
			RoundUp = true,
			IsDirty = static a => a.Any(v => v > 0),
		};
		var bin = new TestBin();
		var config = TestConfig.Of<int>(pool, true, bin, 8);
		int[] array = pool.Rent(64);
		for (int i = 0; i < 40; ++i) array[i] = i + 1;

		config.Seal(array, 40).Release();

		Assert.Equal(0, bin.Posts);
		Assert.Equal(1, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
	}
	// ---- release loops continue past a throwing Return -----------------------

	[Fact]
	public static async Task ACollectorWhoseReturnThrows_ReturnsEveryArray_AndThrowsTheFirstAfterTheLast()
	{
		// Every return throws (after it is counted): the other arrays must
		// still be released, and the first exception comes out at the end.
		var pool = new CountingPool<ChannelRow> { ThrowOnReturn = true };
		MappedCommand<ChannelRow> view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.AsyncReads()
			.UsePool(pool, clearOnReturn: false);

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await view.ToListAsync(Ct));

		Assert.True(pool.Rents > 1);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
	}

	[Fact]
	public static async Task ABufferWhoseReturnThrows_ReturnsEveryArray_AndThrowsTheFirstAfterTheLast()
	{
		var pool = new CountingPool<ChannelRow>();
		MappedCommand<ChannelRow> view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.AsyncReads()
			.UsePool(pool, clearOnReturn: false);

		var buffer = view.ToAsyncBuffer(Ct);
		await buffer.Items.Within();
		pool.ThrowOnReturn = true;

		await Assert.ThrowsAsync<InvalidOperationException>(
			() => buffer.DisposeAsync().AsTask());

		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
	}

	[Fact]
	public static void AToArrayWhoseReturnThrows_ReturnsEveryArrayOnce()
	{
		// The sink outgrows its first array: that release throws, and the
		// sink must hold the new array once, so its Dispose releases the new
		// array and never the old one again.
		var pool = new CountingPool<ChannelRow> { ThrowOnReturn = true };
		MappedCommand<ChannelRow> view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.UsePool(pool, clearOnReturn: false);

		Assert.Throws<InvalidOperationException>(() => view.ToArray());

		Assert.True(pool.Rents > 1);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task AStreamWhoseReturnThrows_ReturnsEveryArrayOnce(
		string terminal)
	{
		var pool = new CountingPool<ChannelRow> { ThrowOnReturn = true };
		MappedCommand<ChannelRow> view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(100))
			.Map<ChannelRow>()
			.AsyncReads()
			.UsePool(pool, clearOnReturn: false);

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () =>
			{
				if (terminal == "stream")
				{
					await foreach (var _ in view.ToAsyncBufferStream(Ct)) { }
				}
				else
				{
					await foreach (var _ in view.ToBufferedAsyncStream(Ct)) { }
				}
			});

		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
	}

	// ---- once ---------------------------------------------------------------

	[Theory]
	[InlineData("ToListAsync")]
	[InlineData("ToArrayAsync")]
	[InlineData("ToArray")]
	[InlineData("ToHashSetAsync")]
	public static async Task ACollector_ReturnsEveryArrayExactlyOnce_OnSuccessAndOnAFault(
		string terminal)
	{
		foreach (int? failAt in new int?[] { null, 40 })
		{
			var pool = Pool();
			MappedCommand<ChannelRow> view = new FakeDbCommand(
				new FakeDbConnection(),
				() => new ChannelTestReader(100, failAtRow: failAt))
				.Map<ChannelRow>()
				.AsyncReads()
				.UsePool(pool, clearOnReturn: true);

			Func<Task> run = terminal switch
			{
				"ToListAsync" => async () => _ = await view.ToListAsync(Ct),
				"ToArrayAsync" => async () => _ = await view.ToArrayAsync(Ct),
				"ToArray" => () =>
				{
					_ = view.ToArray();
					return Task.CompletedTask;
				},
				_ => async () => _ = await view.ToHashSetAsync(null, Ct),
			};

			if (failAt is null) await run();
			else await Assert.ThrowsAsync<InvalidOperationException>(run);

			Assert.True(pool.Rents > 0);
			Assert.Equal(pool.Rents, pool.Returns);
			Assert.Equal(0, pool.Unmatched);
			Assert.Equal(0, pool.DirtyReturns);
		}
	}
}
