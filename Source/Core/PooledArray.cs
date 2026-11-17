#if NET
using System.Numerics;
#endif

namespace Open.Database.Extensions;

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
/// buffer it came from, which returns the arrays to the pool.
/// After that, every member throws
/// <see cref="ObjectDisposedException"/>, an enumeration in
/// progress included.
/// It is not safe to use while another thread disposes it.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
public sealed class PooledArray<T>
	: IReadOnlyList<T>, ICollection<T>, IDisposable
{
	readonly PooledBuffer<T> _owner;
	readonly IMemoryOwner<T>[] _chunks;

	internal PooledArray(
		PooledBuffer<T> owner,
		IMemoryOwner<T>[] chunks,
		int count)
	{
		_owner = owner;
		_chunks = chunks;
		Count = count;
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
			return field;
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
			if ((uint)index >= (uint)Count)
			{
				throw new ArgumentOutOfRangeException(
					nameof(index), index, "Not a row of this array.");
			}

			int first = PooledBuffer<T>.FirstSize;
			int segment = Log2((index / first) + 1);
			int start = (first << segment) - first;
			return _chunks[segment].Memory.Span[index - start];
		}
	}

	static int Log2(int value)
	{
#if NET
		return BitOperations.Log2((uint)value);
#else
		int log = 0;
		while ((value >>= 1) != 0) ++log;
		return log;
#endif
	}

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

		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			Span<T> rows = chunk.Memory.Span;
			rows.CopyTo(destination);
			destination = destination[rows.Length..];
		}
	}

	/// <summary>Enumerates the rows in order.</summary>
	/// <returns>An enumerator over the rows.</returns>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public IEnumerator<T> GetEnumerator()
	{
		_owner.ThrowIfDisposed();
		return Enumerate();
	}

	IEnumerator<T> Enumerate()
	{
		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			Memory<T> memory = chunk.Memory;
			for (int i = 0; i < memory.Length; ++i)
			{
				// The arrays go back to the pool on dispose: never
				// hand out a row from one after it.
				_owner.ThrowIfDisposed();
				T item = memory.Span[i];
				yield return item;
			}
		}

		// Nothing is left, but a disposed view still throws: the
		// end is never a quiet false after the dispose.
		_owner.ThrowIfDisposed();
	}

	IEnumerator IEnumerable.GetEnumerator()
		=> GetEnumerator();

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
		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			ReadOnlySpan<T> rows = chunk.Memory.Span;
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

		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			Span<T> rows = chunk.Memory.Span;
			rows.CopyTo(array.AsSpan(arrayIndex));
			arrayIndex += rows.Length;
		}
	}

	/// <summary>
	/// Disposes the buffer this came from:
	/// the arrays go back to the pool.
	/// </summary>
	public void Dispose()
		=> _owner.Dispose();
}
