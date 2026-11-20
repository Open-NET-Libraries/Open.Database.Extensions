using System.Buffers;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A SPIKE, not a test of the library: what does a MemoryPool<T> rent cost
// over an ArrayPool<T> rent, in the shape ChunkChain has today (pooled
// chunks, rows added on the calling flow, one exact copy at the end)?
//
// SpikeChain copies ChunkChain's structure and takes its chunk sizes from
// the real ChunkSizing (the collect-first/shift/max schedule, reached by
// reflection as the other chunk tests do). The rental strategy is a struct
// type argument, so the three arms differ only in what the rent does, and
// no delegate is allocated by either arm:
//   A  ArrayPool<T>.Shared, as production does.
//   B  MemoryPool<T>.Shared: one IMemoryOwner<T> per chunk.
//   C  a caller's own array-backed MemoryPool<T> (defined below): the shape
//      a real caller pool has.
//
// Method (as ToListArrayAllocationProbe): the calls are synchronous on the
// test thread, so GC.GetAllocatedBytesForCurrentThread sees every byte.
// Warm-up 50 calls, then the minimum of 3 rounds of 20 calls, in bytes per
// call. The rows are made before the measurement, so the bytes are the
// chain's own plus the exact-size copy (the same in every arm).
//
// The probe is Explicit (CI never runs it) and prints a table. The one
// ordinary test pins the failure path of a pool whose memory is not backed
// by an array.
[ExcludeFromCodeCoverage]
[Collection("ChunkSizing")]
public sealed class PoolSpikeProbe(ITestOutputHelper output)
{
	public sealed class ClassRow
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	public struct StructRow
	{
		public int Id { get; set; }
		public long Score { get; set; }
		public double Ratio { get; set; }
	}

	static readonly int[] Sizes = [1, 100, 10_000];

	static long BytesPerCall(Func<int> call, out int chunks)
	{
		chunks = 0;
		for (int i = 0; i < 50; ++i) chunks = call();

		long best = long.MaxValue;
		for (int round = 0; round < 3; ++round)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 20; ++i) call();
			long used = GC.GetAllocatedBytesForCurrentThread() - before;
			if (used < best) best = used;
		}

		return best / 20;
	}

	// The operation measured: rent chunks as rows are added, copy once
	// into an exact result, return everything. Gives the chunks rented.
	static int Run<T, TRental>(TRental rental, T[] rows)
		where TRental : struct, IRental<T>
	{
		using var chain = SpikeChain<T, TRental>.Rent(rental);
		for (int i = 0; i < rows.Length; ++i) chain.Add(rows[i]);

		T[] result = chain.ToArray();
		GC.KeepAlive(result);
		return chain.ChunkCount;
	}

	static long Measure<T, TRental>(
		TRental rental,
		T[] rows,
		out int chunks)
		where TRental : struct, IRental<T>
		=> BytesPerCall(() => Run(rental, rows), out chunks);

	void Table<T>(string kind, Func<int, T[]> make)
	{
		foreach (int n in Sizes)
		{
			T[] rows = make(n);
			long a = Measure(default(ArrayRental<T>), rows, out int chunksA);
			long b = Measure(
				new PoolRental<T>(MemoryPool<T>.Shared),
				rows,
				out int chunksB);
			long c = Measure(
				new PoolRental<T>(SpikeArrayPool<T>.Instance),
				rows,
				out int chunksC);

			output.WriteLine(
				$"{kind,-6} {n,6} | A ArrayPool.Shared      {a,9} B {chunksA,3} chunks");
			output.WriteLine(
				$"{kind,-6} {n,6} | B MemoryPool.Shared     {b,9} B {chunksB,3} chunks"
				+ $" | B-A {b - a,6} B = {Per(b - a, chunksB),5} B/chunk");
			output.WriteLine(
				$"{kind,-6} {n,6} | C caller array pool     {c,9} B {chunksC,3} chunks"
				+ $" | C-A {c - a,6} B = {Per(c - a, chunksC),5} B/chunk");
		}
	}

	static string Per(long delta, int chunks)
		=> (delta / (double)chunks).ToString("0.0");

	[Fact(Explicit = true)]
	public void Probe_OwnerObjectCost()
	{
		output.WriteLine(
			$"{RuntimeInformation.FrameworkDescription}, "
			+ $"{(Environment.Is64BitProcess ? "64" : "32")}-bit"
#if DEBUG
			+ ", DEBUG BUILD: the numbers mean nothing, run Release"
#endif
			);

		// What the chain schedule asks for, so the table can be read.
		var first = SpikeSizing.First();
		output.WriteLine(
			$"collect schedule: first {first}, then {SpikeSizing.Next(first)}, "
			+ $"{SpikeSizing.Next(SpikeSizing.Next(first))}, ...");

		// The owner on its own: one rent and one dispose, bytes per call.
		long owner = BytesPerCall(
			static () =>
			{
				using IMemoryOwner<int> rented = MemoryPool<int>.Shared.Rent(16);
				return rented.Memory.Length;
			},
			out _);
		long array = BytesPerCall(
			static () =>
			{
				int[] rented = ArrayPool<int>.Shared.Rent(16);
				int length = rented.Length;
				ArrayPool<int>.Shared.Return(rented);
				return length;
			},
			out _);
		output.WriteLine(
			$"MemoryPool<int>.Shared.Rent(16)+Dispose {owner} B per call; "
			+ $"ArrayPool<int>.Shared.Rent(16)+Return {array} B per call");

		// Can a rent hand back a larger array than asked for?
		var lengths = new List<string>();
		foreach (int ask in new[] { 1, 15, 16, 17, 100, 5000 })
		{
			using IMemoryOwner<int> rented = MemoryPool<int>.Shared.Rent(ask);
			lengths.Add($"{ask}->{rented.Memory.Length}");
		}

		using (IMemoryOwner<int> rented = MemoryPool<int>.Shared.Rent())
			lengths.Add($"default->{rented.Memory.Length}");

		output.WriteLine("Memory.Length by request: " + string.Join(", ", lengths));

		output.WriteLine("");
		Table("class", static n =>
		{
			var rows = new ClassRow[n];
			for (int i = 0; i < n; ++i)
				rows[i] = new ClassRow { Id = i, Name = "name" };

			return rows;
		});
		Table("struct", static n =>
		{
			var rows = new StructRow[n];
			for (int i = 0; i < n; ++i)
				rows[i] = new StructRow { Id = i, Score = i, Ratio = i / 3.0 };

			return rows;
		});
	}

	// ---- a pool whose memory is not backed by an array --------------------

	// A counting pool: the first `arrayBacked` rents are array-backed, every
	// later one is memory that is not (a MemoryManager). Every owner counts
	// its dispose, and a second dispose of one owner is counted apart.
	sealed class MixedPool(int arrayBacked) : MemoryPool<int>
	{
		public int Rents { get; private set; }
		public int Disposed { get; private set; }
		public int DisposedTwice { get; private set; }

		public override int MaxBufferSize => Array.MaxLength;

		public override IMemoryOwner<int> Rent(int minBufferSize = -1)
			=> ++Rents <= arrayBacked
				? new ArrayOwner(this, new int[Math.Max(minBufferSize, 1)])
				: new OpaqueOwner(this, Math.Max(minBufferSize, 1));

		protected override void Dispose(bool disposing) { }

		void Count(bool first)
		{
			if (first) ++Disposed;
			else ++DisposedTwice;
		}

		sealed class ArrayOwner(MixedPool pool, int[] array) : IMemoryOwner<int>
		{
			bool _done;

			public Memory<int> Memory => array;

			public void Dispose()
			{
				pool.Count(!_done);
				_done = true;
			}
		}

		// Memory made by a manager reports no array to TryGetArray.
		sealed class OpaqueOwner(MixedPool pool, int length)
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

	[Fact]
	public void APoolWithoutArrays_ThrowsNotSupported_NamingThePool_AndEveryRentIsDisposed()
	{
		var pool = new MixedPool(arrayBacked: 2);

		var ex = Assert.Throws<NotSupportedException>(() =>
		{
			using var chain
				= SpikeChain<int, PoolRental<int>>.Rent(new(pool));
			for (int i = 0; i < 1_000_000; ++i) chain.Add(i);
		});

		Assert.Contains(typeof(MixedPool).FullName!, ex.Message);

		// Two array-backed chunks, then the rent that failed: three in all,
		// and each disposed once (the failed one by the prototype at the
		// rent, the two before it when the chain was disposed).
		Assert.Equal(3, pool.Rents);
		Assert.Equal(3, pool.Disposed);
		Assert.Equal(0, pool.DisposedTwice);
	}
}

// ---- the prototype ---------------------------------------------------------

// How a chunk is rented and given back: a struct type argument, so the
// choice costs nothing per call and arms differ by nothing else.
internal interface IRental<T>
{
	// True when a rent comes with an owner that must be disposed.
	static abstract bool UsesOwners { get; }

	T[] Rent(int size, out IMemoryOwner<T>? owner);

	void Return(T[] array, IMemoryOwner<T>? owner);
}

// What production does today.
internal readonly struct ArrayRental<T> : IRental<T>
{
	public static bool UsesOwners => false;

	public T[] Rent(int size, out IMemoryOwner<T>? owner)
	{
		owner = null;
		return ArrayPool<T>.Shared.Rent(size);
	}

	public void Return(T[] array, IMemoryOwner<T>? owner)
		=> ArrayPool<T>.Shared.Return(array, clearArray: false);
}

// Any MemoryPool<T>: the owner is kept per chunk, the array is taken from
// its memory, and the owner is disposed when the chunk is given back.
internal readonly struct PoolRental<T>(MemoryPool<T> pool) : IRental<T>
{
	public static bool UsesOwners => true;

	public T[] Rent(int size, out IMemoryOwner<T>? owner)
	{
		IMemoryOwner<T> rented = pool.Rent(size);
		if (MemoryMarshal.TryGetArray(rented.Memory, out ArraySegment<T> segment)
			&& segment.Array is not null
			&& segment.Offset == 0)
		{
			owner = rented;
			return segment.Array;
		}

		rented.Dispose();
		throw new NotSupportedException(
			$"The pool {pool.GetType().FullName} hands out memory that is not "
			+ "backed by an array (from its start), which pooled chunks need.");
	}

	public void Return(T[] array, IMemoryOwner<T>? owner)
		=> owner!.Dispose();
}

// The shape a caller's own pool has: array-backed, with an owner object of
// its own per rent (the same shape as the BCL's shared pool).
internal sealed class SpikeArrayPool<T> : MemoryPool<T>
{
	public static readonly SpikeArrayPool<T> Instance = new();

	public override int MaxBufferSize => Array.MaxLength;

	public override IMemoryOwner<T> Rent(int minBufferSize = -1)
		=> new Owner(ArrayPool<T>.Shared.Rent(Math.Max(minBufferSize, 1)));

	protected override void Dispose(bool disposing) { }

	sealed class Owner(T[] array) : IMemoryOwner<T>
	{
		T[]? _array = array;

		public Memory<T> Memory
			=> _array is { } held
				? new Memory<T>(held)
				: throw new ObjectDisposedException(nameof(Owner));

		public void Dispose()
		{
			if (Interlocked.Exchange(ref _array, null) is { } held)
				ArrayPool<T>.Shared.Return(held, clearArray: false);
		}
	}
}

// ChunkSizing's collect schedule, through the same reflection the chunk
// tests use (there is no InternalsVisibleTo).
internal static class SpikeSizing
{
	const BindingFlags Internal
		= BindingFlags.Static | BindingFlags.NonPublic;

	static readonly Type Sizing
		= typeof(PooledBuffer<>).Assembly
			.GetType("Open.Database.Extensions.Core.ChunkSizing")!;

	static readonly Func<int> CollectFirst
		= Sizing.GetMethod("ResolveCollectFirstRows", Internal)!
			.CreateDelegate<Func<int>>();

	static readonly Func<int, int> CollectNext
		= Sizing.GetMethod("NextCollectRows", Internal)!
			.CreateDelegate<Func<int, int>>();

	public static int First() => CollectFirst();

	public static int Next(int size) => CollectNext(size);
}

// ChunkChain's structure: one spare chain per type, chunks never moved
// once written, one exact copy at the end, the chunks (and, with a pool of
// owners, the owners) given back by Dispose after the used part of a chunk
// that may hold references is cleared.
internal sealed class SpikeChain<T, TRental> : IDisposable
	where TRental : struct, IRental<T>
{
	static SpikeChain<T, TRental>? _spare;

	static readonly bool MustClear
		= RuntimeHelpers.IsReferenceOrContainsReferences<T>();

	T[][] _chunks = new T[4][];
	IMemoryOwner<T>?[]? _owners = TRental.UsesOwners
		? new IMemoryOwner<T>?[4]
		: null;
	TRental _rental;
	bool _parked;
	T[] _current = [];
	int _index;
	int _filled;
	int _nextSize;

	public static SpikeChain<T, TRental> Rent(TRental rental)
	{
		SpikeChain<T, TRental>? chain = Interlocked.Exchange(ref _spare, null);
		chain ??= new SpikeChain<T, TRental>();
		chain._rental = rental;
		chain._parked = false;
		return chain;
	}

	public int ChunkCount { get; private set; }

	public int Count => _filled + _index;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
	{
		T[] current = _current;
		int index = _index;
		if ((uint)index < (uint)current.Length)
		{
			current[index] = item;
			_index = index + 1;
			return;
		}

		AddSlow(item);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	void AddSlow(T item)
	{
		ObjectDisposedException.ThrowIf(_parked, this);

		T[][] chunks = _chunks;
		if (ChunkCount == chunks.Length)
		{
			Array.Resize(ref chunks, chunks.Length * 2);
			_chunks = chunks;
			if (TRental.UsesOwners)
			{
				IMemoryOwner<T>?[] owners = _owners!;
				Array.Resize(ref owners, owners.Length * 2);
				_owners = owners;
			}
		}

		int size = _nextSize;
		if (size == 0) size = SpikeSizing.First();

		// Counted only once the rent has succeeded, so a failed rent leaves
		// the chain as it was.
		T[] next = _rental.Rent(size, out IMemoryOwner<T>? owner);
		_nextSize = SpikeSizing.Next(size);
		chunks[ChunkCount] = next;
		if (TRental.UsesOwners) _owners![ChunkCount] = owner;

		++ChunkCount;
		_filled += _index;
		_current = next;
		next[0] = item;
		_index = 1;
	}

	public T[] ToArray()
	{
		int count = Count;
		if (count == 0) return [];

		T[] result = GC.AllocateUninitializedArray<T>(count);
		Span<T> destination = result;
		T[][] chunks = _chunks;
		int last = ChunkCount - 1;
		for (int i = 0; i < last; ++i)
		{
			T[] chunk = chunks[i];
			chunk.CopyTo(destination);
			destination = destination[chunk.Length..];
		}

		chunks[last].AsSpan(0, _index).CopyTo(destination);
		return result;
	}

	public void Dispose()
	{
		if (_parked) return;

		_parked = true;
		T[][] chunks = _chunks;
		IMemoryOwner<T>?[]? owners = _owners;
		_current = [];
		int last = ChunkCount - 1;
		for (int i = 0; i <= last; ++i)
		{
			T[] chunk = chunks[i];
			if (MustClear)
				Array.Clear(chunk, 0, i == last ? _index : chunk.Length);

			_rental.Return(chunk, owners?[i]);
		}

		Array.Clear(chunks, 0, ChunkCount);
		if (owners is not null) Array.Clear(owners, 0, ChunkCount);

		_index = 0;
		_filled = 0;
		_nextSize = 0;
		ChunkCount = 0;
		_rental = default;
		Interlocked.CompareExchange(ref _spare, this, null);
	}
}
