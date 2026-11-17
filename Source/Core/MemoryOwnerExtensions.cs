namespace Open.Database.Extensions.Core;

// What the buffer terminals do to the IMemoryOwner<T> the pool gave
// them: cut it to the rows it holds, and let it go.
//
// Why both exist.
// A pool may hand out more than was asked for (a pool's memory may be
// longer than the size requested; a custom pool may round up anywhere). Every reader needs memory that is exactly the
// rows: the indexer arithmetic, CopyTo and the chunks yielded to the
// consumer all use Memory.Length as the row count. So the producer
// slices each filled buffer once (Slice), and that is the unit that is
// published, read and released.
//
// Ownership: an owner is released exactly once, by whoever holds it
// last (the buffer's ReturnAll, the stream's next step or drain, or the
// producer when the target refused it). Release clears the rows of a
// type that holds references first, so a pooled buffer never keeps a
// row alive. Which types: on netstandard2.0 every type is cleared;
// on the other targets only a type that is or contains a reference,
// and a value type is left as it is.
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

	/// <summary>
	/// Clears the rows (when the type holds references, so a pooled
	/// buffer never keeps a row alive) and disposes the owner.
	/// </summary>
	internal static void Release<T>(this IMemoryOwner<T> owner)
	{
#if NETSTANDARD2_0
		owner.Memory.Span.Clear();
#else
		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
			owner.Memory.Span.Clear();
#endif
		owner.Dispose();
	}
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
