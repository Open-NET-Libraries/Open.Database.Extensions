using System.Buffers;
using System.Reflection;
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

	static readonly Assembly Core = typeof(PooledBuffer<>).Assembly;

	const BindingFlags Internal
		= BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

	// ---- the seams, reached by reflection (no InternalsVisibleTo) ----------

	static Type Clearing
		=> Core.GetType("Open.Database.Extensions.Core.PoolClearing")!;

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
	static bool Resolve<T>(bool? clear, bool all)
		=> (bool)Clearing
			.GetMethods(BindingFlags.Public | BindingFlags.Static)
			.Single(static m => m.Name == "Resolve" && m.GetParameters().Length == 2)
			.MakeGenericMethod(typeof(T))
			.Invoke(null, [clear, all])!;

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
		=> Assert.Equal(expected, Resolve<PoolValue>(clear, all));

	[Theory]
	[InlineData(null, false)]
	[InlineData(null, true)]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public static void ReferenceRows_AreAlwaysCleared(bool? clear, bool all)
		=> Assert.True(Resolve<PoolRow>(clear, all));
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
		const string Name = "Open.Database.Extensions.ClearPooledArrays";
		Assert.Equal(
			Name,
			(string)Clearing
				.GetField("SwitchName", BindingFlags.Public | BindingFlags.Static)!
				.GetRawConstantValue()!);

		MethodInfo read = Clearing.GetMethod(
			"ReadSwitch",
			BindingFlags.Public | BindingFlags.Static)!;
		bool had = AppContext.TryGetSwitch(Name, out bool before);
		try
		{
			AppContext.SetSwitch(Name, true);
			Assert.True((bool)read.Invoke(null, null)!);

			AppContext.SetSwitch(Name, false);
			Assert.False((bool)read.Invoke(null, null)!);

			// Another name changes nothing.
			AppContext.SetSwitch(Name, false);
			AppContext.SetSwitch(Name + "X", true);
			Assert.False((bool)read.Invoke(null, null)!);
		}
		finally
		{
			AppContext.SetSwitch(Name, had && before);
			AppContext.SetSwitch(Name + "X", false);
		}
	}

	// ---- a pool decides what it hands out ------------------------------------

	enum Shape
	{
		// The whole array (the memory is the array).
		Whole,

		// A larger array than asked for, the memory all of it.
		WholeAndLarger,

		// Memory that is not backed by an array.
		Opaque,

		// A view that starts after the first element of its array.
		Offset,

		// A view of the first elements of a longer array.
		Partial,

		// Less memory than was asked for.
		Short,
	}

	// A pool: the first `good` rents are whole arrays, every later one has
	// the shape given. It sees every dispose: once, and only once.
	sealed class ShapedPool(int good, Shape bad) : MemoryPool<int>
	{
		int _rents;
		int _disposed;
		int _disposedTwice;
		int _outstanding;

		public int Rents => Volatile.Read(ref _rents);
		public int Disposed => Volatile.Read(ref _disposed);
		public int DisposedTwice => Volatile.Read(ref _disposedTwice);
		public int Outstanding => Volatile.Read(ref _outstanding);

		public override int MaxBufferSize => Array.MaxLength;

		public override IMemoryOwner<int> Rent(int minBufferSize = -1)
		{
			int size = Math.Max(minBufferSize, 1);
			int number = Interlocked.Increment(ref _rents);
			Interlocked.Increment(ref _outstanding);
			Shape shape = number <= good ? Shape.Whole : bad;
			return shape switch
			{
				Shape.Opaque => new OpaqueOwner(this, size),
				Shape.Offset => new ArrayOwner(
					this,
					new int[size + 1],
					1,
					size),
				Shape.Partial => new ArrayOwner(
					this,
					new int[size + 8],
					0,
					size),
				Shape.WholeAndLarger => new ArrayOwner(
					this,
					new int[size + 7],
					0,
					size + 7),
				Shape.Short => new ArrayOwner(this, new int[size], 0, size - 1),
				_ => new ArrayOwner(this, new int[size], 0, size),
			};
		}

		protected override void Dispose(bool disposing) { }

		void Count(bool first)
		{
			if (first)
			{
				Interlocked.Increment(ref _disposed);
				Interlocked.Decrement(ref _outstanding);
			}
			else
			{
				Interlocked.Increment(ref _disposedTwice);
			}
		}

		sealed class ArrayOwner(
			ShapedPool pool,
			int[] array,
			int offset,
			int count)
			: IMemoryOwner<int>
		{
			bool _done;

			public Memory<int> Memory => new(array, offset, count);

			public void Dispose()
			{
				pool.Count(!_done);
				_done = true;
			}
		}

		// Memory made by a manager reports no array to TryGetArray.
		sealed class OpaqueOwner(ShapedPool pool, int length)
			: MemoryManager<int>
		{
			readonly int[] _store = new int[length];
			bool _done;

			public override Span<int> GetSpan() => _store;

			public override MemoryHandle Pin(int elementIndex = 0)
				=> throw new NotSupportedException();

			public override void Unpin() { }

			protected override void Dispose(bool disposing)
			{
				pool.Count(!_done);
				_done = true;
			}
		}
	}

	public static TheoryData<string, string> AnyShape
	{
		get
		{
			var data = new TheoryData<string, string>();
			foreach (string collector in new[]
			{
				"ToListAsync",
				"ToArrayAsync",
				"ToArray",
				"ToHashSetAsync",
			})
			{
				foreach (Shape shape in new[]
				{
					Shape.Whole,
					Shape.WholeAndLarger,
					Shape.Opaque,
					Shape.Offset,
					Shape.Partial,
				})
				{
					data.Add(collector, shape.ToString());
				}
			}

			return data;
		}
	}

	// The pool decides what the memory is backed by and how much it gives:
	// the collectors use what they were given, and write only inside it.
	// Every rent is disposed exactly once.
	[Theory, MemberData(nameof(AnyShape))]
	public static async Task AnyKindOfMemory_GivesTheRightRows_AndEveryRentIsDisposedOnce(
		string collector,
		string shape)
	{
		var pool = new ShapedPool(0, Enum.Parse<Shape>(shape));

		Assert.Equal(
			10_000,
			await Run(
				collector,
				Rows<int>(10_000).UsePool(pool, clearOnReturn: true),
				static r => r));

		Assert.True(pool.Rents > 1);
		Assert.Equal(pool.Rents, pool.Disposed);
		Assert.Equal(0, pool.DisposedTwice);
		Assert.Equal(0, pool.Outstanding);
	}

	[Theory]
	[InlineData(nameof(Shape.Opaque))]
	[InlineData(nameof(Shape.Offset))]
	[InlineData(nameof(Shape.Partial))]
	public static async Task AViewOrMemoryThatIsNotAnArray_KeepsTheRowsInOrder(
		string shape)
	{
		var pool = new ShapedPool(0, Enum.Parse<Shape>(shape));

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
		Assert.Equal(0, pool.DisposedTwice);
	}

	public static TheoryData<string> Collectors
		=> ["ToListAsync", "ToArrayAsync", "ToArray", "ToHashSetAsync"];

	// A pool that gives less than was asked for cannot be written to
	// safely: the read fails, that rent is disposed at once, and the chain's
	// own disposal gives back every rent before it.
	[Theory, MemberData(nameof(Collectors))]
	public static async Task AShortRent_Throws_AndEveryRentIsDisposed(
		string collector)
	{
		var pool = new ShapedPool(2, Shape.Short);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(
			() => Run(
				collector,
				Rows<int>(10_000).UsePool(pool, clearOnReturn: true),
				static r => r));

		Assert.Contains("less memory", ex.Message);
		Assert.Equal(3, pool.Rents);
		Assert.Equal(3, pool.Disposed);
		Assert.Equal(0, pool.DisposedTwice);
		Assert.Equal(0, pool.Outstanding);
	}
	// A pool may round up: the chain uses the array's real length, and only
	// the used part of the last chunk is copied.
	[Theory, MemberData(nameof(Collectors))]
	public static async Task APoolThatRoundsUp_GivesTheRightRows(
		string collector)
	{
		var pool = new ShapedPool(0, Shape.WholeAndLarger);

		Assert.Equal(
			1_000,
			await Run(
				collector,
				Rows<int>(1_000).UsePool(pool, clearOnReturn: true),
				static r => r));

		Assert.True(pool.Rents > 1);
		Assert.Equal(pool.Rents, pool.Disposed);
		Assert.Equal(0, pool.DisposedTwice);
		Assert.Equal(0, pool.Outstanding);
	}

	[Fact]
	public static async Task APoolThatRoundsUp_KeepsTheRowsInOrder()
	{
		var pool = new ShapedPool(0, Shape.WholeAndLarger);

		int[] rows = await Rows<int>(1_000)
			.UsePool(pool, clearOnReturn: false)
			.ToArrayAsync(Ct);

		Assert.Equal(Enumerable.Range(1, 1_000), rows);

		List<int> list = await Rows<int>(1_000)
			.UsePool(pool, clearOnReturn: false)
			.ToListAsync(Ct);

		Assert.Equal(Enumerable.Range(1, 1_000), list);
		Assert.Equal(pool.Rents, pool.Disposed);
		Assert.Equal(0, pool.DisposedTwice);
	}

	// A refused read does not break the next one: the spare chain is clean.
	[Fact]
	public static async Task AfterARefusedRent_TheNextCallWorks()
	{
		var bad = new ShapedPool(1, Shape.Short);
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => Rows<int>(1_000)
				.UsePool(bad, clearOnReturn: true)
				.ToListAsync(Ct)
				.AsTask());

		List<int> rows = await Rows<int>(1_000).ToListAsync(Ct);
		Assert.Equal(Enumerable.Range(1, 1_000), rows);

		var good = new ShapedPool(0, Shape.Whole);
		List<int> again = await Rows<int>(1_000)
			.UsePool(good, clearOnReturn: true)
			.ToListAsync(Ct);
		Assert.Equal(Enumerable.Range(1, 1_000), again);
		Assert.Equal(good.Rents, good.Disposed);
	}
}
