using System.Buffers;
using System.Collections;
using System.Runtime.CompilerServices;
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
// Chunk sizes start at ChunkSizing's collect-first value and grow by
// ChunkSizing's collect-shift per chunk up to its collect-max; they are
// always powers of two, so a pool hands back exactly that size.
//
// The chain object is reused: one spare is kept per T, taken by Rent and
// parked again by Dispose, so a small result allocates nothing for it
// (as the sink struct of the earlier path allocated nothing). A chain is
// rented by one call, used by that call alone, and disposed once, by a
// using declaration (the chain is IDisposable): after that it belongs to
// the spare slot, and no reference to it is used again.
//
// Clearing: a chunk that may hold references is cleared before it goes
// back (the used part only), so a pooled array never keeps a row alive.
// A chunk of plain values is returned as it is.
internal sealed class ChunkChain<T> : ICollection<T>, IDisposable
{
	// The one spare chain of this T.
	static ChunkChain<T>? _spare;

	// A test seam (like ChunkSizing): set by reflection to count rents and
	// returns, then put back. Per T.
	internal static ArrayPool<T> _pool = ArrayPool<T>.Shared;

#if NETSTANDARD2_0
	static readonly bool MustClear = true;
#else
	static readonly bool MustClear
		= RuntimeHelpers.IsReferenceOrContainsReferences<T>();
#endif

	T[][] _chunks = new T[4][];
	bool _parked;
	int _chunkCount;
	T[] _current = [];
	int _index;
	int _filled; // rows in the chunks before the current one
	int _nextSize;

	/// <summary>An empty chain: the spare one if there is one.</summary>
	internal static ChunkChain<T> Rent()
	{
		ChunkChain<T>? chain = Interlocked.Exchange(ref _spare, null);
		if (chain is null) return new ChunkChain<T>();

		chain._parked = false;
		return chain;
	}

	/// <summary>How many rows were added.</summary>
	internal int Count
		=> _filled + _index;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Add(T item)
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
		if (_parked) ThrowDisposed();

		T[][] chunks = _chunks;
		if (_chunkCount == chunks.Length)
		{
			Array.Resize(ref chunks, chunks.Length * 2);
			_chunks = chunks;
		}

		int size = _nextSize;
		if (size == 0) size = ChunkSizing.ResolveCollectFirstRows();
		_nextSize = ChunkSizing.NextCollectRows(size);

		// Counted only once the rent has succeeded, so a failed rent leaves
		// the chain as it was.
		T[] next = _pool.Rent(size);
		chunks[_chunkCount++] = next;
		_filled += _index;
		_current = next;
		next[0] = item;
		_index = 1;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static void ThrowDisposed()
		=> throw new ObjectDisposedException(nameof(ChunkChain<>));

	/// <summary>The rows, in order, in an array of exactly that length.</summary>
	internal T[] ToArray()
	{
		int count = Count;
		if (count == 0) return [];

#if NET
		T[] result = GC.AllocateUninitializedArray<T>(count);
#else
		T[] result = new T[count];
#endif
		CopyTo(result);
		return result;
	}

	/// <summary>The rows, in order, in a list of exactly that capacity.</summary>
	internal List<T> ToList()
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

		T[][] chunks = _chunks;
		for (int i = 0; i < _chunkCount; ++i)
		{
			T[] chunk = chunks[i];
			int length = i == _chunkCount - 1 ? _index : chunk.Length;
			list.AddRange(new ArraySegment<T>(chunk, 0, length));
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
	internal Queue<T> ToQueue()
		=> Count == 0 ? new Queue<T>() : new Queue<T>(this);

	/// <summary>
	/// The rows in a hash set sized for exactly that many (duplicates are
	/// dropped, as a hash set does).
	/// </summary>
	internal HashSet<T> ToHashSet(IEqualityComparer<T>? comparer)
	{
#if NETSTANDARD2_0
		// No HashSet constructor takes a capacity here.
		HashSet<T> set = new(comparer);
#else
		HashSet<T> set = new(Count, comparer);
#endif
		T[][] chunks = _chunks;
		int last = _chunkCount - 1;
		for (int i = 0; i <= last; ++i)
		{
			T[] chunk = chunks[i];
			int length = i == last ? _index : chunk.Length;
			for (int j = 0; j < length; ++j)
				set.Add(chunk[j]);
		}

		return set;
	}

	/// <summary>
	/// The rows in a dictionary sized for exactly that many, keyed by
	/// <paramref name="keySelector"/>. A duplicate key throws the
	/// dictionary's own exception.
	/// </summary>
	internal Dictionary<TKey, T> ToDictionary<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer)
		where TKey : notnull
	{
		Dictionary<TKey, T> dictionary = new(Count, comparer);
		T[][] chunks = _chunks;
		int last = _chunkCount - 1;
		for (int i = 0; i <= last; ++i)
		{
			T[] chunk = chunks[i];
			int length = i == last ? _index : chunk.Length;
			for (int j = 0; j < length; ++j)
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
	internal Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer)
		where TKey : notnull
	{
		Dictionary<TKey, TValue> dictionary = new(Count, comparer);
		T[][] chunks = _chunks;
		int last = _chunkCount - 1;
		for (int i = 0; i <= last; ++i)
		{
			T[] chunk = chunks[i];
			int length = i == last ? _index : chunk.Length;
			for (int j = 0; j < length; ++j)
			{
				T item = chunk[j];
				dictionary.Add(keySelector(item), valueSelector(item));
			}
		}

		return dictionary;
	}

	void CopyTo(Span<T> destination)
	{
		T[][] chunks = _chunks;
		int last = _chunkCount - 1;
		for (int i = 0; i < last; ++i)
		{
			T[] chunk = chunks[i];
			chunk.CopyTo(destination);
			destination = destination[chunk.Length..];
		}

		chunks[last].AsSpan(0, _index).CopyTo(destination);
	}

	/// <summary>
	/// Clears what may hold references, returns every array to the pool
	/// and parks the chain as the spare. Call it once, from the finally of
	/// the call that rented the chain, which a using declaration on the
	/// rented chain does (a second call, before the chain is rented again,
	/// does nothing). Public only because the interface requires it: the
	/// class is internal.
	/// </summary>
	public void Dispose()
	{
		if (_parked) return;

		_parked = true;
		T[][] chunks = _chunks;
		_current = [];
		int last = _chunkCount - 1;
		for (int i = 0; i <= last; ++i)
		{
			T[] chunk = chunks[i];
			if (MustClear)
			{
				Array.Clear(chunk, 0, i == last ? _index : chunk.Length);
			}

			_pool.Return(chunk, clearArray: false);
		}

		Array.Clear(chunks, 0, _chunkCount);
		_index = 0;
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
		T[][] chunks = _chunks;
		for (int i = 0; i < _chunkCount; ++i)
		{
			T[] chunk = chunks[i];
			int length = i == _chunkCount - 1 ? _index : chunk.Length;
			for (int j = 0; j < length; ++j)
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
