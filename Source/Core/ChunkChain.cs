using System.Buffers;
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
// Clearing: a chunk that may hold references is cleared before it goes
// back (the used part only), so a pooled array never keeps a row alive.
// A chunk of plain values is returned as it is.
internal sealed class ChunkChain<T>
{
	// A test seam (like ChunkSizing): set by reflection to count rents and
	// returns, then put back. Per T.
	internal static ArrayPool<T> _pool = ArrayPool<T>.Shared;

#if NETSTANDARD2_0
	static readonly bool MustClear = true;
#else
	static readonly bool MustClear
		= RuntimeHelpers.IsReferenceOrContainsReferences<T>();
#endif

	T[][]? _chunks = new T[4][];
	int _chunkCount;
	T[] _current = [];
	int _index;
	int _filled; // rows in the chunks before the current one
	int _nextSize;

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
		T[][] chunks = _chunks
			?? throw new ObjectDisposedException(nameof(ChunkChain<>));

		_filled += _index;

		if (_chunkCount == chunks.Length)
		{
			Array.Resize(ref chunks, chunks.Length * 2);
			_chunks = chunks;
		}

		int size = _nextSize;
		if (size == 0) size = ChunkSizing.ResolveCollectFirstRows();
		_nextSize = ChunkSizing.NextCollectRows(size);

		T[] next = _pool.Rent(size);
		chunks[_chunkCount++] = next;
		_current = next;
		next[0] = item;
		_index = 1;
	}

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

		T[][] chunks = _chunks!;
		for (int i = 0; i < _chunkCount; ++i)
		{
			T[] chunk = chunks[i];
			int length = i == _chunkCount - 1 ? _index : chunk.Length;
			list.AddRange(new ArraySegment<T>(chunk, 0, length));
		}

		return list;
#endif
	}

	void CopyTo(Span<T> destination)
	{
		T[][] chunks = _chunks!;
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
	/// Clears what may hold references and returns every array to the
	/// pool. Safe to call more than once.
	/// </summary>
	internal void Dispose()
	{
		T[][]? chunks = _chunks;
		if (chunks is null) return;

		_chunks = null;
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

		_index = 0;
	}
}
