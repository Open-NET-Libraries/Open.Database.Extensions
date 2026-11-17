namespace Open.Database.Extensions.Core;

// What the buffer terminals do to the IMemoryOwner<T> the pool gave
// them: cut it to the rows it holds, and let it go.
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
