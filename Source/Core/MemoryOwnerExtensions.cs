namespace Open.Database.Extensions.Core;

// What the buffer terminals do to the IMemoryOwner<T> the pool gave
// them: cut it to the rows it holds, and let it go.
//
// Why Slice exists.
// A pool may hand out more than was asked for (a pool's memory may be
// longer than the size requested; a custom pool may round up
// anywhere). Every reader needs memory that is exactly the rows: the
// indexer arithmetic, CopyTo and the chunks yielded to the consumer
// all use Memory.Length as the row count. So the producer slices each
// filled buffer once, and that is the unit that is published, read
// and released.
//
// Why Release exists.
// It is the one place an owner is let go, and it is only Dispose:
// NOTHING IS CLEARED by default (the BCL's behaviour), because a clear
// on the consumer's read path costs tens of percent when the consumer
// does little. The trade-off: with the default pool a reference row can
// stay reachable through a pooled array until it is overwritten or the
// pool trims. A caller who wants rows scrubbed passes a MemoryPool<T>
// whose owners clear on Dispose (the recipe is in the pool parameter's
// docs); there is no flag on any terminal.
//
// Ownership: an owner is released exactly once, by whoever holds it
// last (the buffer's ReleaseAll, the stream's next step or drain, or
// the producer when the target refused it).
internal static class MemoryOwnerExtensions
{
	/// <summary>
	/// The owner itself when its memory is exactly
	/// <paramref name="count"/> rows (the usual case);
	/// otherwise a wrapper whose memory is the first
	/// <paramref name="count"/> rows and whose disposal disposes
	/// the owner.
	/// </summary>
	internal static IMemoryOwner<T> Slice<T>(
		this IMemoryOwner<T> owner,
		int count)
		=> owner.Memory.Length == count
			? owner
			: new SlicedOwner<T>(owner, count);

	/// <summary>Disposes the owner (nothing is cleared).</summary>
	internal static void Release<T>(this IMemoryOwner<T> owner)
		=> owner.Dispose();
}

// Stateless: the memory is a slice of the host's, and disposing it
// disposes the host. After that the host's own Memory throws.
file sealed class SlicedOwner<T>(IMemoryOwner<T> host, int count)
	: IMemoryOwner<T>
{
	public Memory<T> Memory
		=> host.Memory.Slice(0, count);

	public void Dispose()
		=> host.Dispose();
}
