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
// It is the one place an owner is let go, and it is only Dispose. NOTHING
// is cleared on the consumer's side: a clear on the read path costs tens
// of percent when the consumer does little.
//
// Why ClearOnDispose exists.
// When the caller asked for clearing (clearOnReturn / clearOnDispose:
// true) the WRITER decides: the producer wraps each exactly-sized owner
// (right after Slice, before it is published) in a DeferredClearOwner.
// Whoever disposes it later, wherever and whenever (the stream's next
// step, the buffer's ReleaseAll, a refused or abandoned chunk), just
// calls Dispose; the wrapper posts "clear the rows, then dispose the
// real owner" to the recycle bin. So the consumer side has no flag and
// no extra code, and a chunk that never reaches a consumer (refused,
// leftover on a cancel or a fault) is cleared too, because it holds rows.
// The shared pool does not clear what it gets back and other code can
// rent the array next, so a dirty array is NEVER returned to a pool:
// the bin posts it; if the bin is saturated the clear runs inline; if the
// bin cannot take it the array is dropped to the garbage collector.
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

	/// <summary>Disposes the owner (nothing is cleared here).</summary>
	internal static void Release<T>(this IMemoryOwner<T> owner)
		=> owner.Dispose();

	/// <summary>
	/// Wraps the owner so that disposing it clears the rows first, off
	/// the caller's path (see the notes above and RecycleBin).
	/// </summary>
	internal static IMemoryOwner<T> ClearOnDispose<T>(
		this IMemoryOwner<T> owner)
		=> new DeferredClearOwner<T>(owner);
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

// Disposing it hands "clear the rows, then dispose the real owner" to the
// recycle bin and returns at once. Idempotent and thread-safe (the inner
// owner is taken once). The owner it wraps is already exactly the rows.
file sealed class DeferredClearOwner<T>(IMemoryOwner<T> inner)
	: IMemoryOwner<T>
{
	IMemoryOwner<T>? _inner = inner;

	public Memory<T> Memory
		=> Volatile.Read(ref _inner)?.Memory
			?? throw new ObjectDisposedException(
				nameof(DeferredClearOwner<T>));

	public void Dispose()
	{
		IMemoryOwner<T>? owner = Interlocked.Exchange(ref _inner, null);
		if (owner is null) return;

		switch (RecycleBin.Post(() => ClearAndDispose(owner)))
		{
			// The bin is saturated: clear here, never wait for it.
			case RecycleBin.PostResult.Full:
				ClearAndDispose(owner);
				break;

			// Posted: the bin runs it.
			// Closed: the array is dropped to the garbage collector,
			// never returned dirty.
		}
	}

	static void ClearAndDispose(IMemoryOwner<T> owner)
	{
		owner.Memory.Span.Clear();
		owner.Dispose();
	}
}
