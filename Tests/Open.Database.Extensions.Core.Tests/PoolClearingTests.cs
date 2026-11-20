using System.Buffers;
using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The clearing rule of every pooled array ODX rents, and what a caller's own
// pool may hand out:
// - rows that hold references are ALWAYS cleared before their array goes
//   back (nothing turns that off);
// - rows with no references are cleared only when asked: by
//   UsePool(pool, clearOnReturn: true), or, for a query that does not call
//   UsePool, by the app-wide switch; UsePool(pool, clearOnReturn: false) wins over
//   the switch;
// - the pool a caller passes decides what the memory is backed by and how
//   much it gives; the collectors use what they were given and write only
//   inside it (a pool that gives less than asked is refused).
/// <summary>A row with no references, a type of its own (so its pool is its own).</summary>
[ExcludeFromCodeCoverage]
internal readonly record struct PoolValue(int Id);

[ExcludeFromCodeCoverage]
public static class PoolClearingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// ---- rigs ---------------------------------------------------------------

	static MappedCommand<T> Rows<T>(int n)
		=> new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(n))
			.Map<T>()
			.AsyncReads();

	// What each terminal makes of the rows: how many it saw.
	static async Task<int> Run<T>(
		string terminal,
		MappedCommand<T> view,
		Func<T, int> id)
		where T : notnull
	{
		switch (terminal)
		{
			case "ToListAsync":
				return (await view.ToListAsync(Ct)).Count;
			case "ToArrayAsync":
				return (await view.ToArrayAsync(Ct)).Length;
			case "ToArray":
				return view.ToArray().Length;
			case "ToImmutableArrayAsync":
				return (await view.ToImmutableArrayAsync(Ct)).Length;
			case "ToHashSetAsync":
				return (await view.ToHashSetAsync(null, Ct)).Count;
			case "ToHashSet":
				return view.ToHashSet().Count;
			case "ToDictionaryAsync":
				return (await view.ToDictionaryAsync(id, null, Ct)).Count;
			case "buffer":
				{
					await using var buffer = view.ToAsyncBuffer(Ct);
					return (await buffer.Items.Within()).Count;
				}

			case "stream":
				{
					int rows = 0;
					await foreach (var chunk in view.ToAsyncBufferStream(Ct))
						rows += chunk.Length;

					return rows;
				}

			default:
				{
					int rows = 0;
					await foreach (var _ in view.ToBufferedAsyncStream(Ct))
						++rows;

					return rows;
				}
		}
	}

	// Every terminal that rents pooled arrays.
	public static TheoryData<string> AllTerminals
		=> [
			"ToListAsync",
			"ToArrayAsync",
			"ToArray",
			"ToImmutableArrayAsync",
			"ToHashSetAsync",
			"ToHashSet",
			"ToDictionaryAsync",
			"buffer",
			"stream",
			"buffered",
		];

	const int N = 100;

	// ---- the decision, with the switch given --------------------------------

	// The switch is read once, so a test does not set it: the decision is a
	// pure method that takes the switch as an argument, and it is checked
	// here for every combination. A caller's own pool (below) shows what the
	// decision does to the arrays.
	[Theory]
	[InlineData(null, false, false)]
	[InlineData(null, true, true)]
	[InlineData(false, false, false)]
	[InlineData(false, true, false)]
	[InlineData(true, false, true)]
	[InlineData(true, true, true)]
	public static void ValueRows_AreClearedWhenAsked_ElseByTheSwitch(
		bool? clear,
		bool all,
		bool expected)
		=> Assert.Equal(expected, PoolClearing.Resolve<PoolValue>(clear, all));

	[Theory]
	[InlineData(null, false)]
	[InlineData(null, true)]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public static void ReferenceRows_AreAlwaysCleared(bool? clear, bool all)
		=> Assert.True(PoolClearing.Resolve<PoolRow>(clear, all));
	// ---- a caller's own pool -------------------------------------------------

	static CountingPool<int> IntPool()
		=> new()
		{
			RoundUp = true,
			Sentinel = static () => -1,
			IsDirty = static a => a.Any(v => v > 0),
		};

	static CountingPool<ChannelRow> RowPool()
		=> new()
		{
			RoundUp = true,
			Sentinel = static () => new ChannelRow(-1),
			IsDirty = static a => a.Any(r => r is { Id: > 0 }),
		};

	static async Task AssertBack<T>(CountingPool<T> pool)
	{
		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.True(pool.Rents > 0, "nothing was rented");
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
	}

	[Theory, MemberData(nameof(AllTerminals))]
	public static async Task ACallerPool_ValueRows_ClearFalse_AreNotCleared(
		string terminal)
	{
		CountingPool<int> pool = IntPool();

		Assert.Equal(
			N,
			await Run(
				terminal,
				Rows<int>(N).UsePool(pool, clearOnReturn: false),
				static r => r));

		await AssertBack(pool);
		Assert.True(pool.DirtyReturns > 0, "the rows were cleared");
	}

	[Theory, MemberData(nameof(AllTerminals))]
	public static async Task ACallerPool_ValueRows_ClearTrue_AreCleared(
		string terminal)
	{
		CountingPool<int> pool = IntPool();

		Assert.Equal(
			N,
			await Run(
				terminal,
				Rows<int>(N).UsePool(pool, clearOnReturn: true),
				static r => r));

		await AssertBack(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Theory, MemberData(nameof(AllTerminals))]
	public static async Task ACallerPool_ReferenceRows_AreAlwaysCleared(
		string terminal)
	{
		CountingPool<ChannelRow> pool = RowPool();

		Assert.Equal(
			N,
			await Run(
				terminal,
				Rows<ChannelRow>(N).UsePool(pool, clearOnReturn: false),
				static r => r.Id));

		await AssertBack(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static void UsePool_ANullPool_IsRefused()
	{
		var ex = Assert.Throws<ArgumentNullException>(
			() => Rows<int>(1).UsePool(null!, clearOnReturn: false));
		Assert.Equal("pool", ex.ParamName);
	}

	[Fact]
	public static async Task UsePool_SurvivesSkipAndTake_AndIsAvailableAfterThem()
	{
		CountingPool<int> pool = IntPool();

		List<int> rows = await Rows<int>(N)
			.Skip(10)
			.Take(20)
			.UsePool(pool, clearOnReturn: true)
			.ToListAsync(Ct);

		Assert.Equal(20, rows.Count);
		await AssertBack(pool);
		Assert.Equal(0, pool.DirtyReturns);

		CountingPool<int> second = IntPool();
		int[] array = await Rows<int>(N)
			.UsePool(second, clearOnReturn: true)
			.Skip(10)
			.Take(20)
			.ToArrayAsync(Ct);

		Assert.Equal(20, array.Length);
		await AssertBack(second);
		Assert.Equal(0, second.DirtyReturns);
	}

	// ---- the switch -----------------------------------------------------------

	// The switch is cached when the type is first used, so a test cannot
	// make the process read it again: what can be proved here is that the
	// method that fills the cache asks the AppContext for the documented
	// name (and nothing else), and answers what it is set to.
	[Fact]
	public static void TheSwitch_IsReadByItsDocumentedName()
	{
		// The cache is filled (with the switch as the process has it) before
		// the switch is changed here, so nothing else can read a changed one.
		_ = PoolClearing.All;

		const string Name = "Open.Database.Extensions.ClearPooledArrays";
		Assert.Equal(Name, PoolClearing.SwitchName);

		bool had = AppContext.TryGetSwitch(Name, out bool before);
		try
		{
			AppContext.SetSwitch(Name, true);
			Assert.True(PoolClearing.ReadSwitch());

			AppContext.SetSwitch(Name, false);
			Assert.False(PoolClearing.ReadSwitch());

			// Another name changes nothing.
			AppContext.SetSwitch(Name, false);
			AppContext.SetSwitch(Name + "X", true);
			Assert.False(PoolClearing.ReadSwitch());
		}
		finally
		{
			AppContext.SetSwitch(Name, had && before);
			AppContext.SetSwitch(Name + "X", false);
		}
	}
	// ---- a pool decides how long its arrays are ------------------------------

	public static TheoryData<string> Collectors
		=> ["ToListAsync", "ToArrayAsync", "ToArray", "ToHashSetAsync"];

	// A pool may give longer arrays than asked for (the shared pool rounds
	// up to a power of two): ODX uses all of what it was given, the rows
	// come back in order, and every array goes back exactly once.
	[Theory, MemberData(nameof(Collectors))]
	public static async Task APoolThatRoundsUp_GivesTheRightRows(
		string collector)
	{
		CountingPool<int> pool = IntPool();

		Assert.Equal(
			10_000,
			await Run(
				collector,
				Rows<int>(10_000).UsePool(pool, clearOnReturn: true),
				static r => r));

		await AssertBack(pool);
		Assert.True(pool.Rents > 1);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static async Task APoolThatRoundsUp_KeepsTheRowsInOrder()
	{
		CountingPool<int> pool = IntPool();

		int[] array = await Rows<int>(10_000)
			.UsePool(pool, clearOnReturn: false)
			.ToArrayAsync(Ct);
		List<int> list = await Rows<int>(10_000)
			.UsePool(pool, clearOnReturn: false)
			.ToListAsync(Ct);
		int[] sync = Rows<int>(10_000)
			.UsePool(pool, clearOnReturn: false)
			.ToArray();

		Assert.Equal(Enumerable.Range(1, 10_000), array);
		Assert.Equal(Enumerable.Range(1, 10_000), list);
		Assert.Equal(Enumerable.Range(1, 10_000), sync);
		Assert.Equal(0, pool.Outstanding);
		Assert.Equal(0, pool.Unmatched);
	}

	// A pool that gives less than was asked for cannot be written to
	// safely: the read fails, that array goes back at once, and the chain's
	// own disposal gives back every array before it.
	[Theory, MemberData(nameof(Collectors))]
	public static async Task AShortRent_Throws_AndEveryArrayGoesBack(
		string collector)
	{
		// The first two rents are whole; every one after is one slot short.
		var pool = new ShortAfterPool(good: 2);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(
			() => Run(
				collector,
				Rows<int>(10_000).UsePool(pool, clearOnReturn: true),
				static r => r));

		Assert.Contains("less memory", ex.Message);
		Assert.Equal(3, pool.Rents);
		Assert.Equal(3, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
		Assert.Equal(0, pool.Outstanding);
	}

	// A refused read does not break the next one: the spare chain is clean.
	[Fact]
	public static async Task AfterARefusedRent_TheNextCallWorks()
	{
		var bad = new ShortAfterPool(good: 1);
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => Rows<int>(1_000)
				.UsePool(bad, clearOnReturn: true)
				.ToListAsync(Ct)
				.AsTask());

		List<int> rows = await Rows<int>(1_000).ToListAsync(Ct);
		Assert.Equal(Enumerable.Range(1, 1_000), rows);

		CountingPool<int> good = IntPool();
		List<int> again = await Rows<int>(1_000)
			.UsePool(good, clearOnReturn: true)
			.ToListAsync(Ct);
		Assert.Equal(Enumerable.Range(1, 1_000), again);
		Assert.Equal(good.Rents, good.Returns);
	}

	// The first `good` rents are whole; every later one is one slot short.
	sealed class ShortAfterPool(int good) : ArrayPool<int>
	{
		readonly System.Collections.Concurrent.ConcurrentDictionary<int[], int> _out
			= new(ReferenceEqualityComparer.Instance);

		int _rents;
		int _returns;
		int _unmatched;

		public int Rents => Volatile.Read(ref _rents);
		public int Returns => Volatile.Read(ref _returns);
		public int Unmatched => Volatile.Read(ref _unmatched);
		public int Outstanding => _out.Count;

		public override int[] Rent(int minimumLength)
		{
			int number = Interlocked.Increment(ref _rents);
			var array = new int[number <= good ? minimumLength : minimumLength - 1];
			_out[array] = 0;
			return array;
		}

		public override void Return(int[] array, bool clearArray = false)
		{
			if (!_out.TryRemove(array, out _))
			{
				Interlocked.Increment(ref _unmatched);
				return;
			}

			Interlocked.Increment(ref _returns);
		}
	}
}