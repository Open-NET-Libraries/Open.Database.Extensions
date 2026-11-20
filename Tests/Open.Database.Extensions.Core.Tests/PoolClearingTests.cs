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
//   UsePool(pool, clear: true), or, for a query that does not call
//   UsePool, by the app-wide switch; UsePool(pool, clear: false) wins over
//   the switch;
// - the pool a caller passes must hand out WHOLE arrays: anything else is
//   refused with a NotSupportedException, after disposing that rent.
// The tests change statics (the shared pool seam of the chunk chain, the
// cached switch), so they run in the ChunkSizing collection, one at a time,
// and put every one back.
/// <summary>A row with no references, a type of its own (so its pool is its own).</summary>
[ExcludeFromCodeCoverage]
internal readonly record struct PoolValue(int Id);

[ExcludeFromCodeCoverage]
[Collection("ChunkSizing")]
public static class PoolClearingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly Assembly Core = typeof(PooledBuffer<>).Assembly;

	const BindingFlags Internal
		= BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

	// ---- the seams, reached by reflection (no InternalsVisibleTo) ----------

	static Type Clearing
		=> Core.GetType("Open.Database.Extensions.Core.PoolClearing")!;

	static FieldInfo ChainPool<T>()
		=> Core.GetType("Open.Database.Extensions.Core.ChunkChain`1")!
			.MakeGenericType(typeof(T))
			.GetField("_pool", Internal)!;

	// The cached switch is put in and taken out again, whatever happens.
	static async Task WithSwitch(bool on, Func<Task> body)
	{
		FieldInfo field = Clearing.GetField("_all", Internal)!;
		object? before = field.GetValue(null);
		field.SetValue(null, on);
		try { await body(); }
		finally { field.SetValue(null, before); }
	}

	// An ArrayPool that gives out fresh arrays and sees whether any row is
	// left in one that comes back.
	sealed class DirtyArrayPool<T> : ArrayPool<T>
	{
		int _rents;
		int _returns;
		int _dirty;

		public int Rents => Volatile.Read(ref _rents);
		public int Returns => Volatile.Read(ref _returns);
		public int Dirty => Volatile.Read(ref _dirty);

		public override T[] Rent(int minimumLength)
		{
			Interlocked.Increment(ref _rents);
			int size = 16;
			while (size < minimumLength) size <<= 1;
			return new T[size];
		}

		public override void Return(T[] array, bool clearArray = false)
		{
			Interlocked.Increment(ref _returns);
			if (array.Any(static row
				=> !EqualityComparer<T>.Default.Equals(row, default!)))
			{
				Interlocked.Increment(ref _dirty);
			}
		}
	}

	// The shared array pool of the chain, swapped for a counting one.
	static async Task WithSharedPool<T>(
		DirtyArrayPool<T> pool,
		Func<Task> body)
	{
		FieldInfo field = ChainPool<T>();
		object? before = field.GetValue(null);
		field.SetValue(null, pool);
		try { await body(); }
		finally { field.SetValue(null, before); }
	}

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

	// The terminals that collect through the chunk chain (the shared array
	// pool seam reaches these; ToArray uses its own buffer).
	public static TheoryData<string> ChainTerminals
		=> [
			"ToListAsync",
			"ToArrayAsync",
			"ToImmutableArrayAsync",
			"ToHashSetAsync",
			"ToHashSet",
			"ToDictionaryAsync",
		];

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

	// ---- the shared pool: the default path ----------------------------------

	[Theory, MemberData(nameof(ChainTerminals))]
	public static async Task ValueRows_AreNotClearedByDefault(string terminal)
	{
		var pool = new DirtyArrayPool<PoolValue>();
		await WithSwitch(false, () => WithSharedPool(pool, async () =>
			Assert.Equal(
				N,
				await Run(terminal, Rows<PoolValue>(N), static r => r.Id))));

		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.True(pool.Dirty > 0, "the rows were cleared");
	}

	[Theory, MemberData(nameof(ChainTerminals))]
	public static async Task ReferenceRows_AreAlwaysCleared_ByDefault(
		string terminal)
	{
		var pool = new DirtyArrayPool<PoolRow>();
		await WithSwitch(false, () => WithSharedPool(pool, async () =>
			Assert.Equal(
				N,
				await Run(terminal, Rows<PoolRow>(N), static r => r.Id))));

		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Dirty);
	}

	[Theory, MemberData(nameof(ChainTerminals))]
	public static async Task ReferenceRows_AreAlwaysCleared_EvenWithClearFalse(
		string terminal)
	{
		var pool = new DirtyArrayPool<PoolRow>();
		await WithSwitch(false, () => WithSharedPool(pool, async () =>
			Assert.Equal(
				N,
				await Run(
					terminal,
					Rows<PoolRow>(N).UsePool(
						MemoryPool<PoolRow>.Shared, clear: false),
					static r => r.Id))));

		Assert.True(pool.Rents > 0);
		Assert.Equal(0, pool.Dirty);
	}

	[Theory, MemberData(nameof(ChainTerminals))]
	public static async Task UsePoolClearTrue_ClearsValueRows(string terminal)
	{
		var pool = new DirtyArrayPool<PoolValue>();
		await WithSwitch(false, () => WithSharedPool(pool, async () =>
			Assert.Equal(
				N,
				await Run(
					terminal,
					Rows<PoolValue>(N).UsePool(
						MemoryPool<PoolValue>.Shared, clear: true),
					static r => r.Id))));

		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Dirty);
	}

	[Theory, MemberData(nameof(ChainTerminals))]
	public static async Task TheSwitch_ClearsValueRows_OfAQueryWithoutUsePool(
		string terminal)
	{
		var pool = new DirtyArrayPool<PoolValue>();
		await WithSwitch(true, () => WithSharedPool(pool, async () =>
			Assert.Equal(
				N,
				await Run(terminal, Rows<PoolValue>(N), static r => r.Id))));

		Assert.True(pool.Rents > 0);
		Assert.Equal(0, pool.Dirty);
	}

	// The explicit choice wins over the switch, in both directions.
	[Theory, MemberData(nameof(ChainTerminals))]
	public static async Task ClearFalse_WinsOverTheSwitch_ForValueRows(
		string terminal)
	{
		var pool = new DirtyArrayPool<PoolValue>();
		await WithSwitch(true, () => WithSharedPool(pool, async () =>
			Assert.Equal(
				N,
				await Run(
					terminal,
					Rows<PoolValue>(N).UsePool(
						MemoryPool<PoolValue>.Shared, clear: false),
					static r => r.Id))));

		Assert.True(pool.Rents > 0);
		Assert.True(pool.Dirty > 0, "the switch overrode clear: false");
	}

	[Theory, MemberData(nameof(ChainTerminals))]
	public static async Task ClearTrue_WinsOverTheSwitchBeingOff(
		string terminal)
	{
		var pool = new DirtyArrayPool<PoolValue>();
		await WithSwitch(false, () => WithSharedPool(pool, async () =>
			Assert.Equal(
				N,
				await Run(
					terminal,
					Rows<PoolValue>(N).UsePool(
						MemoryPool<PoolValue>.Shared, clear: true),
					static r => r.Id))));

		Assert.Equal(0, pool.Dirty);
	}

	// MemoryPool<T>.Shared is recognised by reference: the chunks come from
	// the shared ArrayPool<T> (the seam sees every rent), no owner is made.
	[Fact]
	public static async Task MemoryPoolShared_RentsFromTheSharedArrayPool()
	{
		var pool = new DirtyArrayPool<PoolValue>();
		await WithSwitch(false, () => WithSharedPool(pool, async () =>
		{
			List<PoolValue> rows = await Rows<PoolValue>(N)
				.UsePool(MemoryPool<PoolValue>.Shared, clear: false)
				.ToListAsync(Ct);
			Assert.Equal(N, rows.Count);
		}));

		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
	}

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
				Rows<int>(N).UsePool(pool, clear: false),
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
				Rows<int>(N).UsePool(pool, clear: true),
				static r => r));

		await AssertBack(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Theory, MemberData(nameof(AllTerminals))]
	public static async Task UsePoolClearFalse_WinsOverTheSwitch_OnACallerPool(
		string terminal)
	{
		// UsePool always states the choice, so the switch is not asked.
		CountingPool<int> pool = IntPool();

		await WithSwitch(true, async () =>
			Assert.Equal(
				N,
				await Run(
					terminal,
					Rows<int>(N).UsePool(pool, clear: false),
					static r => r)));

		await AssertBack(pool);
		Assert.True(pool.DirtyReturns > 0, "the switch overrode clear: false");
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
				Rows<ChannelRow>(N).UsePool(pool, clear: false),
				static r => r.Id));

		await AssertBack(pool);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static void UsePool_ANullPool_IsRefused()
	{
		var ex = Assert.Throws<ArgumentNullException>(
			() => Rows<int>(1).UsePool(null!, clear: false));
		Assert.Equal("pool", ex.ParamName);
	}

	[Fact]
	public static async Task UsePool_SurvivesSkipAndTake_AndIsAvailableAfterThem()
	{
		CountingPool<int> pool = IntPool();

		List<int> rows = await Rows<int>(N)
			.Skip(10)
			.Take(20)
			.UsePool(pool, clear: true)
			.ToListAsync(Ct);

		Assert.Equal(20, rows.Count);
		await AssertBack(pool);
		Assert.Equal(0, pool.DirtyReturns);

		CountingPool<int> second = IntPool();
		int[] array = await Rows<int>(N)
			.UsePool(second, clear: true)
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

	// ---- a pool must hand out whole arrays -----------------------------------

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

	public static TheoryData<string> Collectors
		=> ["ToListAsync", "ToArrayAsync", "ToArray", "ToHashSetAsync"];

	public static TheoryData<string, string> RefusedShapes
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
				foreach (string shape in new[]
				{
					nameof(Shape.Opaque),
					nameof(Shape.Offset),
					nameof(Shape.Partial),
				})
				{
					data.Add(collector, shape);
				}
			}

			return data;
		}
	}

	// The first chunk (and so the first rent) already fails.
	[Theory, MemberData(nameof(RefusedShapes))]
	public static async Task AnUnusableRent_ThrowsNotSupported_NamingThePool_AndIsDisposed(
		string collector,
		string shape)
	{
		var pool = new ShapedPool(0, Enum.Parse<Shape>(shape));

		var ex = await Assert.ThrowsAsync<NotSupportedException>(
			() => Run(
				collector,
				Rows<int>(1_000).UsePool(pool, clear: false),
				static r => r));

		Assert.Contains(typeof(ShapedPool).FullName!, ex.Message);
		Assert.Equal(1, pool.Rents);
		Assert.Equal(1, pool.Disposed);
		Assert.Equal(0, pool.DisposedTwice);
		Assert.Equal(0, pool.Outstanding);
	}

	// Two good chunks, then one that cannot be used: the chain's own
	// disposal gives the two back, the refused rent was disposed at the rent.
	[Theory, MemberData(nameof(RefusedShapes))]
	public static async Task AnUnusableRentLater_DisposesEveryEarlierRentToo(
		string collector,
		string shape)
	{
		var pool = new ShapedPool(2, Enum.Parse<Shape>(shape));

		var ex = await Assert.ThrowsAsync<NotSupportedException>(
			() => Run(
				collector,
				Rows<int>(10_000).UsePool(pool, clear: true),
				static r => r));

		Assert.Contains(typeof(ShapedPool).FullName!, ex.Message);
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
				Rows<int>(1_000).UsePool(pool, clear: true),
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
			.UsePool(pool, clear: false)
			.ToArrayAsync(Ct);

		Assert.Equal(Enumerable.Range(1, 1_000), rows);

		List<int> list = await Rows<int>(1_000)
			.UsePool(pool, clear: false)
			.ToListAsync(Ct);

		Assert.Equal(Enumerable.Range(1, 1_000), list);
		Assert.Equal(pool.Rents, pool.Disposed);
		Assert.Equal(0, pool.DisposedTwice);
	}

	// A refused read does not break the next one: the spare chain is clean.
	[Fact]
	public static async Task AfterARefusedRent_TheNextCallWorks()
	{
		var bad = new ShapedPool(1, Shape.Partial);
		await Assert.ThrowsAsync<NotSupportedException>(
			() => Rows<int>(1_000)
				.UsePool(bad, clear: true)
				.ToListAsync(Ct)
				.AsTask());

		List<int> rows = await Rows<int>(1_000).ToListAsync(Ct);
		Assert.Equal(Enumerable.Range(1, 1_000), rows);

		var good = new ShapedPool(0, Shape.Whole);
		List<int> again = await Rows<int>(1_000)
			.UsePool(good, clear: true)
			.ToListAsync(Ct);
		Assert.Equal(Enumerable.Range(1, 1_000), again);
		Assert.Equal(good.Rents, good.Disposed);
	}
}
