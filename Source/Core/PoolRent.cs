using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

/// <summary>
/// Where a collector's chunk of memory comes from and where it goes back
/// to: the shared array pool, or a caller's own <see cref="MemoryPool{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// A chunk is a <see cref="Memory{T}"/>, whatever it is backed by. The pool
/// decides how much it gives (it may give more than was asked for) and what
/// it is backed by; the collectors write into it and read it back through a
/// span, and never need to know. The shared array pool is the default and
/// costs no owner object: the memory is the whole array, and the array goes
/// back to <see cref="ArrayPool{T}.Shared"/>.
/// </para>
/// <para>
/// Only the used part of a chunk is cleared when a clear is asked for: the
/// rows that were written, not the whole of what the pool gave.
/// </para>
/// </remarks>
internal static class PoolRent
{
	/// <summary>
	/// Rents memory of at least <paramref name="size"/> from
	/// <paramref name="custom"/>, or, when that is null, from
	/// <see cref="ArrayPool{T}.Shared"/> (then
	/// <paramref name="owner"/> is null).
	/// </summary>
	/// <exception cref="InvalidOperationException">
	/// A pool gave less memory than asked for (the rent is disposed first).
	/// </exception>
	public static Memory<T> Rent<T>(
		MemoryPool<T>? custom,
		int size,
		out IMemoryOwner<T>? owner)
	{
		if (custom is null)
		{
			owner = null;
			return ArrayPool<T>.Shared.Rent(size);
		}

		owner = custom.Rent(size);
		Memory<T> memory = owner.Memory;
		if (memory.Length < size)
		{
			owner.Dispose();
			throw new InvalidOperationException(
				"The pool returned less memory than asked for.");
		}

		return memory;
	}

	/// <summary>
	/// Clears the first <paramref name="used"/> items when
	/// <paramref name="clear"/> is true, then gives the memory back: the
	/// owner is disposed, or (no owner) the array goes back to the shared
	/// array pool.
	/// </summary>
	public static void Return<T>(
		Memory<T> memory,
		IMemoryOwner<T>? owner,
		int used,
		bool clear)
	{
		if (clear && used > 0)
			memory.Span[..used].Clear();

		if (owner is not null)
		{
			owner.Dispose();
			return;
		}

		if (MemoryMarshal.TryGetArray<T>(memory, out ArraySegment<T> segment)
			&& segment.Array is { } array)
		{
			ArrayPool<T>.Shared.Return(array, clearArray: false);
		}
	}
}
