using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Open.Database.Extensions;

// How this works (notes for a reader of the code; not the contract).
//
// The view holds a snapshot of the buffer's array leases, taken once when
// the read completed, and the row count. It owns nothing: the buffer
// does, and every member first asks the buffer whether it is disposed
// (ThrowIfDisposed), so the view never reads an array that went back to
// the pool. (It is not safe against ANOTHER thread disposing while a
// member runs; the docs say so.)
//
// Why the indexer is O(1).
// The producer rents arrays of F, 2F, 4F ... rows (FirstRows << k; F is
// 16 by default, so 16, 32, 64 ...) and every array but the last is
// full, so array k starts at row F * (2^k - 1). Row i is therefore in
// array k = floor(log2(i/F + 1)), at offset i - (F * 2^k - F): one
// shift-and-log, no search. That
// relies on every non-last chunk holding exactly its planned number of
// rows, which the producer guarantees (each lease's Used is that number),
// even for a pool that hands out longer arrays than it was asked for.
//
// Why it is a read-only ICollection<T> too: LINQ's ToArray and ToList
// and the List constructor look for ICollection<T>, and then know the
// count and copy one array at a time instead of enumerating.

/// <summary>
/// The complete result of a <see cref="PooledBuffer{T}"/>:
/// a read-only list over its pooled arrays.
/// </summary>
/// <remarks>
/// <para>
/// It is also a read-only <see cref="ICollection{T}"/>
/// (explicitly: <c>Add</c>, <c>Clear</c> and <c>Remove</c> throw
/// <see cref="NotSupportedException"/>),
/// so LINQ's <c>ToArray</c> and <c>ToList</c> and the
/// <see cref="List{T}"/> constructor know its count and copy it
/// with one pass per array.
/// The buffer itself is not a collection,
/// because it may still be filling.
/// </para>
/// <para>
/// It is valid until it is disposed. Disposing it disposes the
/// buffer it came from, which returns the arrays to the pool (and,
/// if the rows are to be cleared (see UsePool), clears
/// the rows: that is when the clear takes effect).
/// After that, every member throws
/// <see cref="ObjectDisposedException"/>, an enumeration in
/// progress included, except <see cref="Dispose"/>, which does
/// nothing again and does not throw.
/// It is not safe to use while another thread disposes it.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
public sealed class PooledArray<T>
	: IReadOnlyList<T>, ICollection<T>, IDisposable
{
	readonly PooledBuffer<T> _owner;

	// Per chunk: the pooled array and how many of its slots hold rows.
	readonly ArrayLease<T>[] _chunks;
	readonly int _count;

	// The rows of the first chunk (a power of two) and its log2: the
	// indexer finds a row's chunk with a shift.
	readonly int _first;
	readonly int _shift;

	internal PooledArray(
		PooledBuffer<T> owner,
		ArrayLease<T>[] chunks,
		int count,
		int firstRows)
	{
		_owner = owner;
		_chunks = chunks;
		_count = count;
		_first = firstRows;
		_shift = ChunkSizing.Log2(firstRows);
	}

	/// <summary>The number of rows.</summary>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public int Count
	{
		get
		{
			_owner.ThrowIfDisposed();
			return _count;
		}
	}

	/// <summary>The row at the index, in O(1).</summary>
	/// <param name="index">The zero-based index of the row.</param>
	/// <exception cref="ArgumentOutOfRangeException">
	/// The index is not less than <see cref="Count"/>.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public T this[int index]
	{
		get
		{
			_owner.ThrowIfDisposed();
			if ((uint)index >= (uint)_count) ThrowOutOfRange(index);

			int segment = ChunkSizing.Log2((index >> _shift) + 1);
			int at = index - ((_first << segment) - _first);
			return _chunks[segment].Array[at];
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static void ThrowOutOfRange(int index)
		=> throw new ArgumentOutOfRangeException(
			nameof(index), index, "Not a row of this array.");

	/// <summary>Copies every row, in order, to the span.</summary>
	/// <param name="destination">
	/// At least <see cref="Count"/> long.
	/// </param>
	/// <exception cref="ArgumentException">
	/// The span is shorter than <see cref="Count"/>.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public void CopyTo(Span<T> destination)
	{
		if (destination.Length < Count)
		{
			throw new ArgumentException(
				"The destination is shorter than Count.",
				nameof(destination));
		}

		foreach (ArrayLease<T> chunk in _chunks)
		{
			chunk.Span.CopyTo(destination);
			destination = destination[chunk.Used..];
		}
	}

	/// <summary>
	/// Copies every row, in order, into a new array of exactly
	/// <see cref="Count"/> rows: one copy per chunk, nothing grown.
	/// </summary>
	/// <returns>
	/// The rows in a new array; it does not depend on this view, which
	/// can be disposed afterwards.
	/// </returns>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
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

	/// <summary>
	/// Copies every row, in order, into a new list sized to the rows:
	/// one copy per chunk, nothing grown.
	/// </summary>
	/// <returns>
	/// The rows in a new list; it does not depend on this view, which
	/// can be disposed afterwards.
	/// </returns>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public List<T> ToList()
	{
		int count = Count;
		List<T> list = new(count);
		if (count == 0) return list;

#if NET8_0_OR_GREATER
		CollectionsMarshal.SetCount(list, count);
		CopyTo(CollectionsMarshal.AsSpan(list));
#else
		foreach (ArrayLease<T> chunk in _chunks)
			list.AddRange(new ArraySegment<T>(chunk.Array, 0, chunk.Used));
#endif

		return list;
	}

	/// <summary>Enumerates the rows in order.</summary>
	/// <remarks>
	/// <c>foreach</c> binds to this method: the enumerator is a struct
	/// (like <see cref="List{T}.Enumerator"/>), so walking the rows
	/// allocates nothing and costs no interface call.
	/// It checks that the array is not disposed once per
	/// <c>MoveNext</c>, so an enumeration in progress throws
	/// <see cref="ObjectDisposedException"/> as soon as the array is
	/// disposed, even when no row is left.
	/// Through <see cref="IEnumerable{T}"/> the same enumerator is boxed.
	/// </remarks>
	/// <returns>An enumerator over the rows.</returns>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public Enumerator GetEnumerator()
	{
		_owner.ThrowIfDisposed();
		return new Enumerator(this);
	}

	IEnumerator<T> IEnumerable<T>.GetEnumerator()
		=> GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator()
		=> GetEnumerator();

	/// <summary>
	/// Walks the rows of a <see cref="PooledArray{T}"/>, chunk by chunk,
	/// straight over the arrays behind them.
	/// </summary>
	public struct Enumerator : IEnumerator<T>
	{
		readonly PooledArray<T> _view;
		int _segment;      // the next chunk to enter
		T[]? _array;       // the current chunk's array
		int _index;        // the next element of the current chunk
		int _end;          // one past its last element

		internal Enumerator(PooledArray<T> view)
		{
			_view = view;
			_segment = 0;
			_array = null;
			_index = 0;
			_end = 0;
			Current = default!;
		}

		/// <summary>The row the last <c>MoveNext</c> moved to.</summary>
		public T Current { readonly get; private set; }

		readonly object? IEnumerator.Current
			=> Current;

		/// <summary>Moves to the next row.</summary>
		/// <returns>False when there is none.</returns>
		/// <exception cref="ObjectDisposedException">
		/// The array is disposed.
		/// </exception>
		public bool MoveNext()
		{
			// The hot path is one test: in a chunk of an array. (A
			// default enumerator has no array, so it takes the slow path,
			// where it is empty.)
			T[]? array = _array;
			int index = _index;
			if (array is not null && index < _end)
			{
				// One check per row: the arrays go back to the pool on
				// dispose, so no row of one is handed out after it.
				_view._owner.ThrowIfDisposed();
				Current = array[index];
				_index = index + 1;
				return true;
			}

			return MoveNextSlow();
		}

		// The end of a chunk.
		bool MoveNextSlow()
		{
			// default(Enumerator): no view, nothing to walk.
			if (_view is null) return false;

			_view._owner.ThrowIfDisposed();

			if (_index >= _end)
			{
				if (_segment >= _view._chunks.Length)
				{
					Current = default!;
					return false;
				}

				ArrayLease<T> chunk = _view._chunks[_segment++];
				_array = chunk.Array;
				_index = 0;
				_end = chunk.Used;

				// A loaded chunk is never empty: nothing publishes an
				// empty one (Seal hands on a buffer only if it has rows).
				Debug.Assert(_end > _index);
			}

			Current = _array![_index];
			++_index;
			return true;
		}

		void IEnumerator.Reset()
		{
			if (_view is null) return;

			_view._owner.ThrowIfDisposed();
			_segment = 0;
			_array = null;
			_index = 0;
			_end = 0;
			Current = default!;
		}

		/// <summary>Does nothing: there is nothing to release.</summary>
		public readonly void Dispose() { }
	}

	// Every member checks the owner first, so a disposed view
	// throws ObjectDisposedException, mutators included.
	bool ICollection<T>.IsReadOnly
	{
		get
		{
			_owner.ThrowIfDisposed();
			return true;
		}
	}

	void ICollection<T>.Add(T item)
		=> throw ReadOnly();

	void ICollection<T>.Clear()
		=> throw ReadOnly();

	bool ICollection<T>.Remove(T item)
		=> throw ReadOnly();

	NotSupportedException ReadOnly()
	{
		_owner.ThrowIfDisposed();
		return new("A PooledArray is read-only.");
	}

	// A linear scan over the rows only (never a pooled array's
	// stale tail).
	bool ICollection<T>.Contains(T item)
	{
		_owner.ThrowIfDisposed();
		EqualityComparer<T> comparer = EqualityComparer<T>.Default;
		foreach (ArrayLease<T> chunk in _chunks)
		{
			ReadOnlySpan<T> rows = chunk.Span;
			for (int i = 0; i < rows.Length; ++i)
			{
				if (comparer.Equals(rows[i], item)) return true;
			}
		}

		return false;
	}

	// What List<T>, ToArray and ToList call: one copy per array.
	void ICollection<T>.CopyTo(T[] array, int arrayIndex)
	{
		int count = Count;
		if (array is null)
			throw new ArgumentNullException(nameof(array));

		if (arrayIndex < 0)
		{
			throw new ArgumentOutOfRangeException(
				nameof(arrayIndex), arrayIndex, "Cannot be negative.");
		}

		if (array.Length - arrayIndex < count)
		{
			throw new ArgumentException(
				"The destination is too small.", nameof(array));
		}

		foreach (ArrayLease<T> chunk in _chunks)
		{
			chunk.Span.CopyTo(array.AsSpan(arrayIndex));
			arrayIndex += chunk.Used;
		}
	}

	/// <summary>
	/// Disposes the buffer this came from:
	/// the arrays go back to the pool.
	/// </summary>
	public void Dispose()
		=> _owner.Dispose();
}
