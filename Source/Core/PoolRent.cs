using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

// A rent from a caller's own MemoryPool<T>, for the collectors that work
// on arrays (ChunkChain, ArraySink): the whole array behind the memory
// the pool handed out, and the owner that gives it back.
//
// The array must be WHOLE: TryGetArray succeeds, the offset is 0 and the
// count is the array's Length. The collectors size their chunks by the
// array's Length, so a pool that hands out the first N elements of a
// larger shared array would let them write past the memory they were
// given, into another renter's data. Anything else is refused with a
// NotSupportedException naming the pool type, after disposing THAT rent
// (every earlier rent belongs to the caller of this method, which
// disposes it). A pool may give a LARGER array than asked for; the
// caller uses the array's real Length.
internal static class PoolRent
{
	public static T[] Rent<T>(
		MemoryPool<T> pool,
		int size,
		out IMemoryOwner<T> owner)
	{
		owner = pool.Rent(size);
		try
		{
			if (!MemoryMarshal.TryGetArray<T>(
					owner.Memory,
					out ArraySegment<T> segment)
				|| segment.Array is not { } array
				|| segment.Offset != 0
				|| segment.Count != array.Length)
			{
				throw new NotSupportedException(
					$"The pool ({pool.GetType()}) must hand out whole"
					+ " arrays: memory that is not backed by an array, or"
					+ " that is a part of a larger array, cannot be used."
					+ " MemoryPool<T>.Shared and ArrayPool-based pools do.");
			}

			if (array.Length < size)
			{
				throw new InvalidOperationException(
					"The pool returned less memory than asked for.");
			}

			return array;
		}
		catch
		{
			owner.Dispose();
			throw;
		}
	}
}
