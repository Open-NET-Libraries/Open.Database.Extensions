using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

// The collector behind ToArrayAsync and ToListAsync: rows are written
// into a chain of pooled chunks of memory that are never moved once
// written, so absorbing the read costs one store per row and no growth
// copy. When the read is over (and the connection is released) the chain
// is copied ONCE into a result of exactly the right size, then its chunks
// go back to the pool.
//
// The chain is read on the caller's own flow: there is no producer task
// (that is ToAsyncBuffer's job, and it is for a different purpose: a
// consumer that works while the read is still running).
//
// Chunk sizes start at ChunkSizing's collect-first value and grow by
// ChunkSizing's collect-shift per chunk up to its collect-max; they are
// what the chain ASKS for. A pool decides what it gives: it may give more
// (the shared array pool rounds up to a power of two), and the chain uses
// all of what it was given, so the last chunk is the only one that is not
// full and the only one whose tail is never written, read or cleared.
//
// The chain object is reused: one spare is kept per T, taken by Rent and
// parked again by Dispose, so a small result allocates nothing for it
// (as the sink struct of the earlier path allocated nothing). A chain is
// rented by one call, used by that call alone, and disposed once, by a
// using declaration (the chain is IDisposable): after that it belongs to
// the spare slot, and no reference to it is used again.
//
// What a chunk is.
// A chunk is a Memory<T>, whatever it is backed by: the chain does not
// need to know. Reading it back (the copy, the set, the dictionary) goes
// through a span of the used part. Writing is one row at a time as the
// read produces them, so when the memory happens to be backed by an array
// (the shared array pool always is; so is almost every pool) the chain
// keeps that array, the offset of the memory in it and the length, and
// stores a row with a plain array write; memory that is not backed by an
// array is written through its span. Either way the chain writes only
// inside the memory it was given.
//
// Where the memory comes from (PoolRent).
// By default (and for MemoryPool<T>.Shared, which is recognised by
// reference) the chunks are rented from ArrayPool<T>.Shared directly: no
// owner object is made, so a rent costs what it always did. A caller's own
// MemoryPool<T> (UsePool) hands out an IMemoryOwner<T> per chunk (24 bytes
// each, measured: PoolSpikeProbe); the chain keeps it beside the chunk and
// disposes it when the chunk is released, AFTER the clearing rule. The
// chain never disposes the pool itself: it is the caller's.
//
// Clearing: whether the used part of every chunk is cleared before it
// goes back is decided once per call (PoolClearing: the floor for row
// types that hold references, then the query's explicit choice, then the
// app-wide switch). Plain value rows are returned as they are when it is
// not asked for. Only the rows that were written are cleared: a chunk the
// read only started on is cleared up to where it stopped, and nothing is
// cleared of what was never written.
internal sealed class ChunkChain<T> : ICollection<T>, IDisposable
{
	// The one spare chain of this T.
	static ChunkChain<T>? _spare;

	Memory<T>[] _chunks = new Memory<T>[4];

	// The owners of the chunks, in step with _chunks, only for a caller's
	// own pool (null otherwise). Kept by the spare chain between uses.
	IMemoryOwner<T>?[]? _owners;

	// The caller's own pool of this call, or null for ArrayPool<T>.Shared.
	MemoryPool<T>? _custom;
	bool _clear;
	bool _parked;
	int _chunkCount;

	// The chunk being written: its memory; the array behind it and the
	// memory's offset in that array (null when the memory is not backed by
	// an array); how many rows it holds; how many are written.
	Memory<T> _current;
	T[]? _array;
	int _offset;
	int _capacity;
	int _index;
	int _filled; // rows in the chunks before the current one
	int _nextSize;

	/// <summary>
	/// An empty chain: the spare one if there is one. The chunks come from
	/// <paramref name="pool"/>, or from the shared array pool when it is
	/// null or <see cref="MemoryPool{T}.Shared"/>; <paramref name="clearOnReturn"/>
	/// is what <c>PoolClearing.Resolve</c> decided.
	/// </summary>
	public static ChunkChain<T> Rent(MemoryPool<T>? pool, bool clearOnReturn)
	{
		ChunkChain<T>? chain = Interlocked.Exchange(ref _spare, null);
		chain ??= new ChunkChain<T>();

		chain._parked = false;
		chain._clear = clearOnReturn;
		chain._custom = pool is null
			|| ReferenceEquals(pool, MemoryPool<T>.Shared)
			? null
			: pool;
		return chain;
	}

	/// <summary>How many rows were added.</summary>
	public int Count
		=> _filled + _index;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
	{
		int index = _index;
		if ((uint)index < (uint)_capacity && _array is { } array)
		{
			array[_offset + index] = item;
			_index = index + 1;
			return;
		}

		AddSlow(item);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	void AddSlow(T item)
	{
		if (_parked) ThrowDisposed();

		// Memory that is not backed by an array: written through its span.
		if ((uint)_index < (uint)_capacity)
		{
			_current.Span[_index++] = item;
			return;
		}

		Memory<T>[] chunks = _chunks;
		if (_chunkCount == chunks.Length)
		{
			Array.Resize(ref chunks, chunks.Length * 2);
			_chunks = chunks;
			if (_owners is not null)
				Array.Resize(ref _owners, chunks.Length);
		}

		int size = _nextSize;
		if (size == 0) size = ChunkSizing.ResolveCollectFirstRows();

		// Counted only once the rent has succeeded, so a failed rent leaves
		// the chain as it was.
		Memory<T> next = PoolRent.Rent(_custom, size, out IMemoryOwner<T>? owner);
		if (owner is not null)
		{
			(_owners ??= new IMemoryOwner<T>?[chunks.Length])
				[_chunkCount] = owner;
		}

		_nextSize = ChunkSizing.NextCollectRows(size);
		chunks[_chunkCount++] = next;
		_filled += _index;
		_current = next;
		_capacity = next.Length;
		if (MemoryMarshal.TryGetArray<T>(next, out ArraySegment<T> segment)
			&& segment.Array is { } array)
		{
			_array = array;
			_offset = segment.Offset;
			array[_offset] = item;
		}
		else
		{
			_array = null;
			_offset = 0;
			next.Span[0] = item;
		}

		_index = 1;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static void ThrowDisposed()
		=> throw new ObjectDisposedException(nameof(ChunkChain<>));

	// How many rows of chunk i are written: all of them, except in the last.
	int Used(int i)
		=> i == _chunkCount - 1 ? _index : _chunks[i].Length;

	/// <summary>The rows, in order, in an array of exactly that length.</summary>
	public T[] ToArray()
	{
		int count = Count;
		if (count == 0) return [];

#if NET
		T[] result = GC.AllocateUninitializedArray<T>(count);
#else
		var result = new T[count];
#endif
		CopyTo(result);
		return result;
	}

	/// <summary>The rows, in order, in a list of exactly that capacity.</summary>
	public List<T> ToList()
	{
		int count = Count;

#if NET8_0_OR_GREATER
		List<T> list = new(count);
		if (count == 0) return list;

		CollectionsMarshal.SetCount(list, count);
		CopyTo(CollectionsMarshal.AsSpan(list));
		return list;
#else
		List<T> list = new(count);
		if (count == 0) return list;

		for (int i = 0; i < _chunkCount; ++i)
		{
			ReadOnlySpan<T> rows = _chunks[i].Span[..Used(i)];
			for (int j = 0; j < rows.Length; ++j)
				list.Add(rows[j]);
		}

		return list;
#endif
	}

	/// <summary>
	/// The rows, in order, in a queue made from exactly that many rows.
	/// </summary>
	/// <remarks>
	/// Queue has no way in but its constructors; the one that takes an
	/// ICollection (this chain is one) makes ONE array of exactly Count and
	/// block-copies into it, and keeps that array (the constructor path is
	/// checked by a test on the shipped runtimes; older runtimes enqueue
	/// one by one, which is the same result).
	/// </remarks>
	public Queue<T> ToQueue()
		=> Count == 0 ? new Queue<T>() : new Queue<T>(this);

	/// <summary>
	/// The rows in a hash set that never regrows (duplicates are
	/// dropped, as a hash set does).
	/// </summary>
	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer)
	{
#if NETSTANDARD2_0
		// No HashSet constructor takes a capacity here.
		HashSet<T> set = new(comparer);
#else
		HashSet<T> set = new(Count, comparer);
#endif
		for (int i = 0; i < _chunkCount; ++i)
		{
			ReadOnlySpan<T> rows = _chunks[i].Span[..Used(i)];
			for (int j = 0; j < rows.Length; ++j)
				set.Add(rows[j]);
		}

		return set;
	}

	/// <summary>
	/// The rows in a dictionary that never regrows, keyed by
	/// <paramref name="keySelector"/>. A duplicate key throws the
	/// dictionary's own exception.
	/// </summary>
	public Dictionary<TKey, T> ToDictionary<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer)
		where TKey : notnull
	{
		Dictionary<TKey, T> dictionary = new(Count, comparer);
		for (int i = 0; i < _chunkCount; ++i)
		{
			ReadOnlySpan<T> rows = _chunks[i].Span[..Used(i)];
			for (int j = 0; j < rows.Length; ++j)
			{
				T item = rows[j];
				dictionary.Add(keySelector(item), item);
			}
		}

		return dictionary;
	}

	/// <summary>
	/// As <see cref="ToDictionary{TKey}"/>, with the value chosen by
	/// <paramref name="valueSelector"/>.
	/// </summary>
	public Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer)
		where TKey : notnull
	{
		Dictionary<TKey, TValue> dictionary = new(Count, comparer);
		for (int i = 0; i < _chunkCount; ++i)
		{
			ReadOnlySpan<T> rows = _chunks[i].Span[..Used(i)];
			for (int j = 0; j < rows.Length; ++j)
			{
				T item = rows[j];
				dictionary.Add(keySelector(item), valueSelector(item));
			}
		}

		return dictionary;
	}

	void CopyTo(Span<T> destination)
	{
		for (int i = 0; i < _chunkCount; ++i)
		{
			ReadOnlySpan<T> rows = _chunks[i].Span[..Used(i)];
			rows.CopyTo(destination);
			destination = destination[rows.Length..];
		}
	}

	/// <summary>
	/// Clears what is to be cleared (see the notes at the top), gives every
	/// chunk back to its pool and parks the chain as the spare. Call it
	/// once, from the finally of the call that rented the chain, which a
	/// using declaration on the rented chain does (a second call, before
	/// the chain is rented again, does nothing). Public only because the
	/// interface requires it: the class is internal.
	/// </summary>
	public void Dispose()
	{
		if (_parked) return;

		_parked = true;
		Memory<T>[] chunks = _chunks;
		IMemoryOwner<T>?[]? owners = _owners;
		bool clear = _clear;
		int count = _chunkCount;
		int last = count - 1;
		int lastUsed = _index;
		_current = default;
		_array = null;
		_custom = null;
		for (int i = 0; i <= last; ++i)
		{
			IMemoryOwner<T>? owner = owners?[i];
			if (owner is not null) owners![i] = null;

			PoolRent.Return(
				chunks[i],
				owner,
				i == last ? lastUsed : chunks[i].Length,
				clear);
		}

		Array.Clear(chunks, 0, count);
		_index = 0;
		_offset = 0;
		_capacity = 0;
		_filled = 0;
		_nextSize = 0;
		_chunkCount = 0;
		Interlocked.CompareExchange(ref _spare, this, null);
	}

	// The chain is an ICollection only so that a Queue can be made from it
	// in one exact copy (see ToQueue); nothing else about it is a collection.
	int ICollection<T>.Count => Count;

	bool ICollection<T>.IsReadOnly => true;

	void ICollection<T>.CopyTo(T[] array, int arrayIndex)
		=> CopyTo(array.AsSpan(arrayIndex));

	IEnumerator<T> IEnumerable<T>.GetEnumerator()
	{
		for (int i = 0; i < _chunkCount; ++i)
		{
			// A span cannot live across a yield: the memory is kept, and
			// each row is read through it.
			Memory<T> chunk = _chunks[i];
			int used = Used(i);
			for (int j = 0; j < used; ++j)
				yield return chunk.Span[j];
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
		=> ((IEnumerable<T>)this).GetEnumerator();

	void ICollection<T>.Add(T item)
		=> throw new NotSupportedException();

	void ICollection<T>.Clear()
		=> throw new NotSupportedException();

	bool ICollection<T>.Contains(T item)
		=> throw new NotSupportedException();

	bool ICollection<T>.Remove(T item)
		=> throw new NotSupportedException();
}
