using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

/// <summary>
/// The collector behind <c>ToArrayAsync</c> and <c>ToListAsync</c>: rows are
/// written into a chain of pooled arrays that are never moved once written.
/// </summary>
/// <remarks>
/// <para>
/// Absorbing the read costs one store per row and no growth copy.
/// When the read is over (and the connection is released) the chain is
/// copied once into a result of exactly the right size, then its arrays go
/// back to the pool.
/// </para>
/// <para>
/// The chain is read on the caller's own flow: there is no producer task
/// (that is the job of <c>ToAsyncBuffer</c>, and it is for a different
/// purpose: a consumer that works while the read is still running).
/// </para>
/// <para>
/// Chunk sizes start at the configuration's collect-first value and grow by
/// its collect-shift per chunk up to its collect-max
/// (<see cref="ChunkSizes"/>); they are what the chain <em>asks</em> for.
/// A pool may give a longer array (the shared array pool rounds up to a
/// power of two), and the chain uses all of it, so the last chunk is the
/// only one that is not full and the only one whose tail is never written,
/// read or cleared.
/// </para>
/// <para>
/// The chain object is reused: one spare is kept per <typeparamref name="T"/>,
/// taken by <see cref="Rent"/> and parked again by <see cref="Dispose"/>, so a
/// small result allocates nothing for it.
/// A chain is rented by one call, used by that call alone, and disposed once,
/// by a <see langword="using"/> declaration (the chain is
/// <see cref="IDisposable"/>): after that it belongs to the spare slot, and
/// no reference to it is used again.
/// </para>
/// <para>
/// What the chain holds is arrays: the hot write is one array store.
/// The number of rows in each is known (every array is full but the last),
/// so nothing needs an array's true length except the write itself.
/// </para>
/// <para>
/// The arrays come from, and go back to, the pool of the call's
/// <see cref="ChunkConfig{T}"/> (the shared array pool, or the
/// <see cref="ArrayPool{T}"/> a caller passed to <c>UsePool</c>).
/// Clearing is decided once per call (<see cref="PoolClearing"/>: the floor
/// for row types that hold references, then the query's explicit choice,
/// then the app-wide switch) and only the rows that were written are
/// cleared: <see cref="Dispose"/> seals each array with its row count
/// (<see cref="ArrayLease{T}"/>) and releases it, and the lease knows
/// whether that clears where it runs or in the recycle bin.
/// </para>
/// </remarks>
/// <include file="Internals.docs.xml" path="docs/members[@name='row']/*" />
internal sealed class ChunkChain<T> : ICollection<T>, IDisposable
{
	/// <summary>The one spare chain of this <typeparamref name="T"/>.</summary>
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
	/// Gets an empty chain: the spare one if there is one.
	/// </summary>
	/// <param name="config">
	/// Supplies the pool the arrays come from and go back to.
	/// </param>
	/// <returns>An empty chain, ready to be filled.</returns>
	public static ChunkChain<T> Rent(ChunkConfig<T> config)
	{
		ChunkChain<T>? chain = Interlocked.Exchange(ref _spare, null);
		chain ??= new ChunkChain<T>();

		chain._parked = false;
		chain._config = config;
		return chain;
	}

	/// <summary>Gets the number of rows added.</summary>
	public int Count
		=> _filled + _index;

	/// <summary>Stores one row, in the current array if it has room.</summary>
	/// <param name="item">The row.</param>
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

	/// <summary>Stores one row in a newly rented array.</summary>
	/// <param name="item">The row.</param>
	/// <exception cref="ObjectDisposedException">
	/// The chain was disposed.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// The pool gave less than the size asked for.
	/// </exception>
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

	/// <summary>
	/// Gets how many rows of chunk <paramref name="i"/> are written: all of
	/// them, except in the last.
	/// </summary>
	int Used(int i)
		=> i == _chunkCount - 1 ? _index : _chunks[i].Length;

	/// <summary>Copies the rows, in order, into an array of exactly that length.</summary>
	/// <returns>A new array of <see cref="Count"/> rows.</returns>
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

	/// <summary>Copies the rows, in order, into a list of exactly that capacity.</summary>
	/// <returns>A new list of <see cref="Count"/> rows.</returns>
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
	/// Adds the rows to a new hash set sized to them, so that it does not
	/// regrow (duplicates are dropped, as a hash set does).
	/// </summary>
	/// <remarks>
	/// Where no <see cref="HashSet{T}"/> constructor takes a capacity
	/// (netstandard2.0), the set is created empty and grows as usual.
	/// </remarks>
	/// <param name="comparer">
	/// The equality comparer, or <see langword="null"/> for the default.
	/// </param>
	/// <returns>A new hash set of the distinct rows.</returns>
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
	/// Adds the rows to a new dictionary sized to them, so that it does not
	/// regrow, keyed by <paramref name="keySelector"/>.
	/// </summary>
	/// <typeparam name="TKey">The type of the key.</typeparam>
	/// <param name="keySelector">Gets the key of a row.</param>
	/// <param name="comparer">
	/// The equality comparer of the keys, or <see langword="null"/> for the
	/// default.
	/// </param>
	/// <returns>A new dictionary of the rows.</returns>
	/// <exception cref="ArgumentException">
	/// Two rows have the same key (the dictionary's own exception).
	/// </exception>
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
	/// <typeparam name="TKey">The type of the key.</typeparam>
	/// <typeparam name="TValue">The type of the value.</typeparam>
	/// <param name="keySelector">Gets the key of a row.</param>
	/// <param name="valueSelector">Gets the value of a row.</param>
	/// <param name="comparer">
	/// The equality comparer of the keys, or <see langword="null"/> for the
	/// default.
	/// </param>
	/// <returns>A new dictionary of the values.</returns>
	/// <exception cref="ArgumentException">
	/// Two rows have the same key (the dictionary's own exception).
	/// </exception>
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
	/// Gives every array back (see the remarks of the type) and parks the
	/// chain as the spare.
	/// </summary>
	/// <remarks>
	/// Call it once, from the <see langword="finally"/> of the call that
	/// rented the chain, which a <see langword="using"/> declaration on the
	/// rented chain does (a second call, before the chain is rented again,
	/// does nothing).
	/// <include file="Internals.docs.xml" path="docs/members[@name='release-all']/*" />
	/// Public only because the interface requires it: the class is internal.
	/// </remarks>
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

	/// <summary>Gets the number of rows added.</summary>
	/// <remarks>
	/// The chain is an <see cref="ICollection{T}"/> only so that a collection
	/// constructor can be given the exact count; nothing else about it is a collection.
	/// </remarks>
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
