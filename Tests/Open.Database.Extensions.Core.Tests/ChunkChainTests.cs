using Open.Database.Extensions.Core;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ToArrayAsync and ToListAsync read into a chain of pooled arrays and copy
// ONCE, after the read, into a result of exactly the right size. The
// pooled arrays go back however the call ends, and are clean when they do.
// Each test gives its query its own pool and its own chunk sizes (a
// ChunkConfig): nothing is global.
/// <summary>The row of the chain tests: a type of its own, so its pool is its own.</summary>
[ExcludeFromCodeCoverage]
internal sealed record PoolRow(int Id);

[ExcludeFromCodeCoverage]
public static class ChunkChainTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A caller's pool that counts what goes out and what comes back, and
	// sees whether a row is left in an array when it does. A fresh array
	// holds -1 rows, so a row with an Id above 0 is one that was written
	// and not cleared.
	static CountingPool<PoolRow> NewPool(bool exact = false)
		=> new()
		{
			RoundUp = true,
			Exact = exact,
			Sentinel = static () => new PoolRow(-1),
			IsDirty = static a => a.Any(static r => r is { Id: > 0 }),
		};

	static MappedCommand<PoolRow> Rows(
		int n,
		int? failAtRow = null,
		ArrayPool<PoolRow>? pool = null,
		ChunkSizes? sizes = null)
	{
		MappedCommand<PoolRow> view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(n, failAtRow: failAtRow))
			.Map<PoolRow>()
			.AsyncReads();

		if (sizes is not null)
		{
			return view.WithConfig(
				TestConfig.Of(
					pool ?? ArrayPool<PoolRow>.Shared,
					false,
					sizes: sizes));
		}

		return pool is null
			? view
			: view.UsePool(pool, clearOnReturn: false);
	}
	static void AssertIds(IReadOnlyList<PoolRow> rows, int n, int first = 1)
	{
		Assert.Equal(n, rows.Count);
		for (int i = 0; i < n; ++i)
			Assert.Equal(first + i, rows[i].Id);
	}

	public static TheoryData<int> Sizes
		=> [0, 1, 15, 16, 17, 48, 49, 100, 8191, 8192, 8193, 20_000];

	// ---- the result ----------------------------------------------------------

	[Theory, MemberData(nameof(Sizes))]
	public static async Task ToArrayAsync_ReturnsEveryRowInOrder_ExactlySized(
		int n)
	{
		PoolRow[] rows = await Rows(n).ToArrayAsync(Ct);

		AssertIds(rows, n);
		Assert.Equal(n, rows.Length);
	}

	[Theory, MemberData(nameof(Sizes))]
	public static async Task ToListAsync_ReturnsEveryRowInOrder_ExactlySized(
		int n)
	{
		List<PoolRow> rows = await Rows(n).ToListAsync(Ct);

		AssertIds(rows, n);
		Assert.Equal(n, rows.Capacity);
	}

	[Fact]
	public static async Task WithSkipAndTake_OnlyThoseRowsArrive()
	{
		PoolRow[] array = await Rows(100).Skip(10).Take(30).ToArrayAsync(Ct);
		List<PoolRow> list = await Rows(100).Skip(10).Take(30).ToListAsync(Ct);

		AssertIds(array, 30, first: 11);
		AssertIds(list, 30, first: 11);
	}

	[Fact]
	public static async Task ATakeOfZero_RentsNothingAndReturnsEmpty()
	{
		CountingPool<PoolRow> pool = NewPool();
		Assert.Empty(await Rows(50, pool: pool).Take(0).ToArrayAsync(Ct));
		Assert.Empty(await Rows(50, pool: pool).Take(0).ToListAsync(Ct));

		Assert.Equal(0, pool.Rents);
	}

	// ---- the pooled arrays ---------------------------------------------------

	[Theory, MemberData(nameof(Sizes))]
	public static async Task EveryArray_GoesBack_AndIsCleanWhenItDoes(int n)
	{
		CountingPool<PoolRow> pool = NewPool();
		await Rows(n, pool: pool).ToArrayAsync(Ct);
		await Rows(n, pool: pool).ToListAsync(Ct);

		// A large chunk of reference rows is cleared and returned by the bin.
		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
		Assert.Equal(n == 0, pool.Rents == 0);
	}

	[Fact]
	public static async Task AFailureMidRead_ReturnsEveryArray_AndThrows()
	{
		CountingPool<PoolRow> pool = NewPool();
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Rows(100, failAtRow: 40, pool: pool)
				.ToArrayAsync(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Rows(100, failAtRow: 40, pool: pool)
				.ToListAsync(Ct));

		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static async Task ACancelledToken_ThrowsAndLeavesNothingRented()
	{
		CountingPool<PoolRow> pool = NewPool();
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Rows(100, pool: pool).ToArrayAsync(cts.Token));

		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// ---- the sizing schedule (a test seam, not an option) --------------------

	[Fact]
	public static async Task TheChunkSizes_FollowTheSchedule()
	{
		CountingPool<PoolRow> pool = NewPool(exact: true);
		var sizes = new ChunkSizes(
			collectFirstRows: 1024,
			collectShift: 2,
			collectMaxRows: 16_384);
		PoolRow[] rows = await Rows(50_000, pool: pool, sizes: sizes)
			.ToArrayAsync(Ct);
		AssertIds(rows, 50_000);

		// 1,024 then x4 up to the cap, then the cap: 1,024 + 4,096 +
		// 16,384 + 16,384 = 37,888, and one more to hold 50,000.
		Assert.Equal(
			[1024, 4096, 16_384, 16_384, 16_384],
			pool.Requests);
	}

	[Fact]
	public static async Task AValueThatDoesNotFit_FallsBackToTheDefaults()
	{
		CountingPool<PoolRow> pool = NewPool(exact: true);
		// A shift of 9 and a max of 1000 do not fit: the defaults are used.
		var sizes = new ChunkSizes(
			collectFirstRows: 100,
			collectShift: 9,
			collectMaxRows: 1000);
		PoolRow[] rows = await Rows(100, pool: pool, sizes: sizes)
			.ToArrayAsync(Ct);
		AssertIds(rows, 100);

		// Defaults: 16, 32, 64 holds 100 rows (16 + 32 + 64 = 112).
		Assert.Equal([16, 32, 64], pool.Requests);
	}

	// ---- the contract: a fresh array nobody else holds -----------------------

	[Theory]
	[InlineData(1)]
	[InlineData(17)]
	[InlineData(9000)]
	public static async Task ToArrayAsync_ReturnsAFreshArrayOnEveryCall(int n)
	{
		PoolRow[] a = await Rows(n).ToArrayAsync(Ct);
		PoolRow[] b = await Rows(n).ToArrayAsync(Ct);

		Assert.NotSame(a, b);
		a[0] = null!;
		Assert.NotNull(b[0]);
	}

	[Fact]
	public static async Task ToArrayAsync_WithNoRows_IsEmpty()
	{
		Assert.Empty(await Rows(0).ToArrayAsync(Ct));
		Assert.Empty(await Rows(0).ToImmutableArrayAsync(Ct));
	}

	[Theory]
	[InlineData(1)]
	[InlineData(40)]
	[InlineData(9000)]
	public static async Task ToImmutableArrayAsync_WrapsAnArrayNobodyElseHolds(
		int n)
	{
		ImmutableArray<PoolRow> first = await Rows(n).ToImmutableArrayAsync(Ct);
		PoolRow[] second = await Rows(n).ToArrayAsync(Ct);

		AssertIds(first, n);
		Assert.NotSame(ImmutableCollectionsMarshal.AsArray(first), second);
		second[0] = null!;
		Assert.NotNull(first[0]);
	}

	// ---- the view's own copies -----------------------------------------------

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(16)]
	[InlineData(17)]
	[InlineData(3500)]
	public static async Task APooledArray_CopiesItselfToAnExactArrayAndList(
		int n)
	{
		await using var buffer = Rows(n).ToPooledBuffer(Ct);
		PooledArray<PoolRow> items = await buffer.Items;

		PoolRow[] array = items.ToArray();
		var list = items.ToList();

		AssertIds(array, n);
		AssertIds(list, n);
		Assert.Equal(n, array.Length);
		Assert.Equal(n, list.Capacity);
	}

	[Fact]
	public static async Task TheCopies_OutliveTheView()
	{
		PoolRow[] array;
		List<PoolRow> list;
		await using (var buffer = Rows(100).ToPooledBuffer(Ct))
		{
			PooledArray<PoolRow> items = await buffer.Items;
			array = items.ToArray();
			list = items.ToList();
		}

		AssertIds(array, 100);
		AssertIds(list, 100);
	}

	[Fact]
	public static async Task ADisposedView_ThrowsFromBothCopies()
	{
		await using var buffer = Rows(10).ToPooledBuffer(Ct);
		PooledArray<PoolRow> items = await buffer.Items;
		items.Dispose();

		Assert.Throws<ObjectDisposedException>(() => items.ToArray());
		Assert.Throws<ObjectDisposedException>(() => items.ToList());
	}

	// ---- the chain is reused, and never shared -------------------------------

	// These tests use a row type no other test uses: the spare chain is one
	// static slot per row type, so a test elsewhere that collects rows (in
	// parallel) cannot take or leave the chain these tests are counting on.
	sealed class Cell(int value)
	{
		public int Value { get; } = value;
	}

	static ChunkChain<Cell> RentChain()
		=> ChunkChain<Cell>.Rent(TestConfig.Of(ArrayPool<Cell>.Shared, false));

	// Fills a rented chain past its first chunk, copies it out and gives it
	// back. Returns the chain and weak references to the rows. Kept out of
	// line, and the rows kept out of the caller's locals, so a collection
	// in the caller can tell whether the chain let go of them.
	[MethodImpl(MethodImplOptions.NoInlining)]
	static (ChunkChain<Cell> Chain, WeakReference[] Rows) UseOnce(int rows)
	{
		ChunkChain<Cell> chain = RentChain();
		var weak = new WeakReference[rows];
		for (int i = 0; i < rows; ++i)
		{
			Cell row = new(i);
			weak[i] = new WeakReference(row);
			chain.Add(row);
		}

		var list = chain.ToList();
		Assert.Equal(rows, list.Count);
		Assert.Equal(rows, chain.Count);
		chain.Dispose();
		return (chain, weak);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static void UseAgain(ChunkChain<Cell> chain, int rows)
	{
		for (int i = 0; i < rows; ++i)
			chain.Add(new Cell(i * 7));

		Cell[] array = chain.ToArray();
		Assert.Equal(rows, array.Length);
		for (int i = 0; i < rows; ++i)
			Assert.Equal(i * 7, array[i].Value);

		var list = chain.ToList();
		Assert.Equal(array, list);
	}

	[Fact]
	public static void ADisposedChain_IsTheNextOneRented_EmptyAndCorrectAgain_AndKeepsNoRows()
	{
		(ChunkChain<Cell> first, WeakReference[] rows) = UseOnce(40);

		ChunkChain<Cell> second = RentChain();
		Assert.Same(first, second);
		Assert.Equal(0, second.Count);

		UseAgain(second, 40);
		second.Dispose();

		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();
		Assert.DoesNotContain(rows, static w => w.IsAlive);
	}

	[Fact]
	public static void ADisposedTwice_ReturnsEveryArrayOnce()
	{
		CountingPool<PoolRow> pool = NewPool();
		var chain = ChunkChain<PoolRow>.Rent(
			TestConfig.Of<PoolRow>(pool, false));
		for (int i = 0; i < 100; ++i) chain.Add(new PoolRow(i + 1));

		chain.Dispose();
		int returns = pool.Returns;
		chain.Dispose();

		Assert.Equal(pool.Rents, returns);
		Assert.Equal(returns, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
	}

	// ---- the queue is made from the exact count ------------------------------

	// ---- calls that really suspend never share a chain -----------------------

	// A reader whose every ReadAsync gives up the thread first, so the
	// continuations run elsewhere and the calls overlap.
	[ExcludeFromCodeCoverage]
	sealed class YieldingReader(object?[][] rows)
		: TypedReader(ReaderFixtures.Columns, rows)
	{
		public override async Task<bool> ReadAsync(
			CancellationToken cancellationToken)
		{
			await Task.Yield();
			return await base.ReadAsync(cancellationToken);
		}
	}

	[Fact]
	public static async Task ParallelCalls_ThatSuspend_EachGetTheirOwnRows()
	{
		int[] sizes = [1, 5, 1000];
		for (int round = 0; round < 4; ++round)
		{
			int r = round;
			Task[] calls =
			[
				.. Enumerable.Range(0, 96).Select(i => Task.Run(async () =>
				{
					int n = sizes[i % sizes.Length];
					int offset = (r * 100 + i) * 10_000;
					object?[][] data =
					[
						.. Enumerable.Range(1, n)
							.Select(k => (object?[])[offset + k, "n", null]),
					];
					MappedCommand<PoolRow> query = new FakeDbCommand(
						new FakeDbConnection(),
						() => new YieldingReader(data))
						.Map<PoolRow>()
						.AsyncReads();

					if (i % 2 == 0)
						AssertIds(await query.ToListAsync(Ct), n, offset + 1);
					else
						AssertIds(await query.ToArrayAsync(Ct), n, offset + 1);
				}, Ct)),
			];
			await Task.WhenAll(calls);
		}
	}
}
