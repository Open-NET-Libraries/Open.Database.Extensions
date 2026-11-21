using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

// The collector behind ToArrayAsync and ToListAsync: rows are written
// into a chain of pooled arrays that are never moved once written, so
// absorbing the read costs one store per row and no growth copy. When
// the read is over (and the connection is released) the chain is copied
// ONCE into a result of exactly the right size, then its arrays go back
// to the pool.
//
// The chain is read on the caller's own flow: there is no producer task
// (that is ToAsyncBuffer's job, and it is for a different purpose: a
// consumer that works while the read is still running).
//
// Chunk sizes start at the configuration's collect-first value and grow by
// its collect-shift per chunk up to its collect-max (ChunkSizes); they are
// what the chain ASKS for. A pool may give a longer array (the shared array
// pool rounds up to a power of two), and the chain uses all of it, so the
// last chunk is the only one that is not full and the only one whose tail
// is never written, read or cleared.
//
// The chain object is reused: one spare is kept per T, taken by Rent and
// parked again by Dispose, so a small result allocates nothing for it. A
// chain is rented by one call, used by that call alone, and disposed once,
// by a using declaration (the chain is IDisposable): after that it belongs
// to the spare slot, and no reference to it is used again.
//
// What the chain holds is arrays: the hot write is one array store. The
// number of rows in each is known (every array is full but the last), so
// nothing needs an array's true length except the write itself.
//
// Where the arrays come from and go back to: the call's ChunkConfig (the
// shared array pool, or the ArrayPool a caller passed to UsePool). Clearing
// is decided once per call (PoolClearing: the floor for row types that
// hold references, then the query's explicit choice, then the app-wide
// switch) and only the rows that were written are cleared: Dispose seals
// each array with its row count (ArrayLease) and releases it, and the
// lease knows whether that clears where it runs or in the recycle bin.
internal sealed class ChunkChain<T> : ICollection<T>, IDisposable
{
	// The one spare chain of this T.
	static ChunkChain<T>? _spare;

	T[][] _chunks = new T[4][];
	ChunkConfig<T>? _config;
	bool _parked;
	int _chunkCount;
	T[] _array = [];
	int _index;
	int _filled; // rows in the chunks before the current one
	int _nextSize;

	/// <summary>
	/// An empty chain: the spare one if there is one. The arrays come from
	/// the pool of <paramref name="config"/>.
	/// </summary>
	public static ChunkChain<T> Rent(ChunkConfig<T> config)
	{
		ChunkChain<T>? chain = Interlocked.Exchange(ref _spare, null);
		chain ??= new ChunkChain<T>();

		chain._parked = false;
		chain._config = config;
		return chain;
	}

	/// <summary>How many rows were added.</summary>
	public int Count
		=> _filled + _index;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
	{
		T[] array = _array;
		int index = _index;
		if ((uint)index < (uint)array.Length)
		{
			array[index] = item;
			_index = index + 1;
			return;
		}

		AddSlow(item);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	void AddSlow(T item)
	{
		if (_parked) ThrowDisposed();

		T[][] chunks = _chunks;
		if (_chunkCount == chunks.Length)
		{
			Array.Resize(ref chunks, chunks.Length * 2);
			_chunks = chunks;
		}

		ChunkSizes sizes = _config!.Sizes;
		int size = _nextSize;
		if (size == 0) size = sizes.CollectFirstRows;

		// Counted only once the rent has succeeded, so a failed rent leaves
		// the chain as it was.
		T[] next = _config.Rent(size);
		_nextSize = sizes.NextCollectRows(size);
		chunks[_chunkCount++] = next;
		_filled += _index;
		_array = next;
		next[0] = item;
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
			list.AddRange(new ArraySegment<T>(_chunks[i], 0, Used(i)));

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
			T[] chunk = _chunks[i];
			int used = Used(i);
			for (int j = 0; j < used; ++j)
				set.Add(chunk[j]);
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
			T[] chunk = _chunks[i];
			int used = Used(i);
			for (int j = 0; j < used; ++j)
			{
				T item = chunk[j];
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
			T[] chunk = _chunks[i];
			int used = Used(i);
			for (int j = 0; j < used; ++j)
			{
				T item = chunk[j];
				dictionary.Add(keySelector(item), valueSelector(item));
			}
		}

		return dictionary;
	}

	void CopyTo(Span<T> destination)
	{
		for (int i = 0; i < _chunkCount; ++i)
		{
			int used = Used(i);
			_chunks[i].AsSpan(0, used).CopyTo(destination);
			destination = destination[used..];
		}
	}

	/// <summary>
	/// Gives every array back (see the notes at the top) and parks the
	/// chain as the spare. Call it once, from the finally of the call that
	/// rented the chain, which a using declaration on the rented chain does
	/// (a second call, before the chain is rented again, does nothing). A
	/// release that throws (a pool whose Return throws) does not stop the
	/// others: every array is released, and the first exception is thrown
	/// after the last. Public only because the interface requires it: the
	/// class is internal.
	/// </summary>
	public void Dispose()
	{
		if (_parked) return;

		_parked = true;
		T[][] chunks = _chunks;
		ChunkConfig<T> config = _config!;
		int count = _chunkCount;
		int lastUsed = _index;
		Exception? first = null;
		for (int i = 0; i < count; ++i)
		{
			try
			{
				int used = i == count - 1 ? lastUsed : chunks[i].Length;
				config.Seal(chunks[i], used).Release();
			}
			catch (Exception ex)
			{
				first ??= ex;
			}
		}

		Array.Clear(chunks, 0, count);
		_array = [];
		_config = null;
		_index = 0;
		_filled = 0;
		_nextSize = 0;
		_chunkCount = 0;
		Interlocked.CompareExchange(ref _spare, this, null);

		if (first is not null)
			System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(first).Throw();
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
			T[] chunk = _chunks[i];
			int used = Used(i);
			for (int j = 0; j < used; ++j)
				yield return chunk[j];
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
