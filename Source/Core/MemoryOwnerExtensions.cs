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
// It is the one place an owner is let go. By default it is only
// Dispose: NOTHING IS CLEARED (the BCL's behaviour), because a clear on
// the consumer's read path costs tens of percent when the consumer does
// little. A caller who passes clearOnReturn: true gets every row cleared (any
// type) over the used slice before the array goes back to the pool,
// because the shared pool does not clear and other code can rent the
// array next. The owner at a release site is already the exactly-sized
// one from Slice, so its Memory is exactly the rows to clear.
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

	/// <summary>
	/// Disposes the owner. With <paramref name="clear"/>, the rows are
	/// cleared first, off the caller's path (see RecycleBin): posted to
	/// the bin; inline if the bin is full; and if the bin cannot take it
	/// the owner is NOT disposed, so a dirty array is never returned to
	/// a pool (it goes to the garbage collector).
	/// </summary>
	internal static void Release<T>(
		this IMemoryOwner<T> owner,
		bool clear)
	{
		if (!clear)
		{
			owner.Dispose();
			return;
		}

		switch (RecycleBin.Post(() => ClearAndDispose(owner)))
		{
			case RecycleBin.PostResult.Full:
				ClearAndDispose(owner);
				break;

			// Posted: the bin runs it. Closed: dropped to the GC.
		}
	}

	/// <summary>
	/// Releases every owner of a buffer that is being disposed:
	/// one piece of work for the whole list when clearing.
	/// </summary>
	internal static void ReleaseAll<T>(
		this List<IMemoryOwner<T>> owners,
		bool clear)
	{
		if (!clear)
		{
			foreach (IMemoryOwner<T> owner in owners) owner.Dispose();

			return;
		}

		switch (RecycleBin.Post(() =>
		{
			// One owner failing (a pool that throws) does not stop the
			// rest from being cleared and returned.
			foreach (IMemoryOwner<T> owner in owners)
			{
				try { ClearAndDispose(owner); }
				catch (Exception) { }
			}
		}))
		{
			case RecycleBin.PostResult.Full:
				foreach (IMemoryOwner<T> owner in owners)
					ClearAndDispose(owner);

				break;
		}
	}

	// The owner at a release site is already the exactly-sized one (see
	// Slice), so its Memory is exactly the rows to clear. Every type.
	static void ClearAndDispose<T>(IMemoryOwner<T> owner)
	{
		owner.Memory.Span.Clear();
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
