namespace Open.Database.Extensions;

// How this works (notes for a reader of the code; not the contract).
//
// Who does what.
// The constructor starts ONE producer (ChunkProducer) on a thread-pool
// thread. That producer is the only code that rents buffers and writes
// rows. It calls Publish for each filled buffer and Finish once, last.
// Everyone else (Items, Dispose) runs on callers' threads. The buffer is
// not readable while it fills: the rows are reachable only through the
// completed view (Items); overlapping reads are the stream terminals.
//
// There is no lock. These fields are shared, each with one rule:
// - _chunks and _total are written only by the producer (Publish), and
//   read by others only after _done is set, which publishes them.
// - _error is written once, before _done is set.
// - _done is set by Finish with an Interlocked.Exchange (a full fence).
// - _disposed is set by Dispose with an Interlocked.Exchange: the
//   dispose-once guard.
// - _items (the Items task source) is published with a CompareExchange:
//   see Items, below.
// Finish and Dispose each set their flag, THEN read the other's. With
// full fences on both writes, at least one of them sees the other's
// flag, so the arrays are always released by someone; both may see it,
// and ReleaseAll takes the list with an Interlocked.Exchange, so the
// arrays are released exactly once.
//
// Who owns each array.
// The producer owns the buffer it is filling. Publish appends it to
// _chunks (or refuses once the buffer is disposed, and the producer
// releases it itself). From then on _chunks owns it, until ReleaseAll
// takes the list and releases each one. An array is therefore released
// exactly once, and only when nothing can write or add to it: ReleaseAll
// runs in Finish (the producer's last act) when the buffer was disposed
// while the read ran, and in Dispose when the read had already finished.
//
// Why dispose is ordered the way it is.
// 1. Set _disposed. From then on Publish refuses and Items throws.
// 2. Cancel Items if it is still pending.
// 3. If the read is still going, Cancel the producer: its linked token
//    cancels the query itself. The producer then ends, swallows that
//    cancel (the owner asked for it: IsDisposed), and Finish releases
//    the arrays. If the read already finished, Dispose releases them.
// DisposeAsync additionally awaits the producer, so its arrays are back
// when it returns; the sync Dispose does not wait. The await has NO
// timeout, on purpose: correctness (no array goes back while the
// producer might still write) comes before promptness. So a dispose
// waits for the provider call in progress (the execute, or a read) to
// return; a provider that ignores the cancel token delays it by that
// call. After that the producer stops at the next chunk boundary at the
// latest: the read loop checks the token per row in the default Read
// mode, the provider does in ReadAsync mode, and the producer itself
// checks once per chunk. So with AsyncReads() and a provider that
// ignores the token, a dispose or a cancel is seen at the next chunk
// (a buffer's chunks double: up to the rows of the current chunk).
// A caller's own cancel is the opposite: it is NOT swallowed. Finish
// stores it, and Items is cancelled with the caller's token.
//
// Items is lazy. The view (the PooledArray, with its snapshot of the
// owners) is built only if someone asks, and then once. The task source
// is published with a CompareExchange (the winner is taken); the field
// is read into a local first, so a repeat access is one volatile read.
// Finish sets _done and THEN reads the field; Items publishes the
// source and THEN reads _done: one of them sees the other, and both
// complete with idempotent TrySet, so no completion is lost. It is not
// LazyInitializer or Lazy<T> because those take a delegate. The snapshot
// is safe because no Publish follows Finish.
//
// The connection ODX opened is closed by the query when the read ends,
// which is before Finish: slow readers never hold it.

/// <summary>
/// Every row of a query, held in pooled arrays until the buffer is
/// disposed.
/// </summary>
/// <remarks>
/// <para>
/// Made by <c>ToAsyncBuffer()</c> on a typed view.
/// The read starts when that method is called,
/// on a background producer, once,
/// even if nobody reads the buffer yet; it may finish before anyone
/// asks for the rows.
/// The rows are reached through <see cref="Items"/>, once the read is
/// done: the buffer is not readable while it fills.
/// To read while the query is still running, use
/// <c>ToAsyncBufferStream()</c> or <c>ToBufferedAsyncStream()</c>.
/// </para>
/// <para>
/// The rows live in buffers rented from the pool the caller passed
/// (<see cref="MemoryPool{T}.Shared"/> when none was),
/// each an <see cref="IMemoryOwner{T}"/> of that pool.
/// There is no finalizer: a forgotten owner of the shared pool is
/// just garbage-collected.
/// A custom pool must not throw from <c>Return</c>. If it does,
/// the exception propagates from <see cref="DisposeAsync"/>, and
/// from <see cref="Dispose"/> when the read had already finished;
/// from <see cref="Dispose"/> while the read is still running it
/// ends in the producer's task, which nobody observes.
/// Without clearing, in every case the arrays not yet returned stay
/// with the garbage collector.
/// With clearing on, a <c>Return</c> that throws inside the
/// background queue is swallowed there and costs only that array. When
/// the queue is full the
/// buffer is discarded instead (never cleared, never returned), so a
/// custom pool must tolerate an owner that is never disposed.
/// While an array sits in the pool, or in a buffer that is not
/// disposed, every reference-type row in it stays reachable.
/// They are returned to the pool when the buffer is disposed,
/// and not before: every <see cref="PooledArray{T}"/> it hands out
/// is valid until then.
/// Dispose the buffer (<c>await using</c>) when you are done.
/// A buffer that is never disposed is not a leak: it holds its
/// arrays (and the finished read) until the garbage collector
/// takes it, and the pool does not get them back (there is no
/// finalizer).
/// </para>
/// <para>
/// If the buffer was made with <c>clearOnDispose: true</c>, the rows
/// are cleared as the arrays go back to the pool, and that happens
/// when the buffer, or the <see cref="PooledArray{T}"/> view of it,
/// is disposed (not when the read completes). The clear is off the
/// caller's path, on an internal background queue. (If that queue is
/// ever full, the buffer is discarded instead.)
/// </para>
/// <para>
/// Disposing the buffer while it is still reading cancels the query,
/// and waits (<see cref="DisposeAsync"/>) for the provider call in
/// progress to return: a provider that ignores cancellation delays
/// the dispose by that call.
/// After the read finished, disposing only releases the arrays.
/// </para>
/// </remarks>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
public sealed class PooledBuffer<T>
	: IDisposable,
		IAsyncDisposable,
		IChunkTarget<T>
{
	// The first array holds FirstRows rows (a power of two, 16 unless a
	// test changed it: see ChunkSizing); each next one doubles. So row i
	// is in array floor(log2(i / FirstRows + 1)): O(1).
	readonly int _firstRows = ChunkSizing.ResolveFirstRows();

	readonly ChunkProducer<T> _producer;
	readonly Task _producerTask;

	// Producer only, until _done; then read-only. Null once released.
	List<IMemoryOwner<T>>? _chunks = [];
	TaskCompletionSource<PooledArray<T>>? _items;
	Exception? _error;
	int _total;
	int _done;
	int _disposed;

	internal PooledBuffer(
		MappedQueryCore<T> query,
		MemoryPool<T> pool,
		bool clearOnDispose,
		CancellationToken cancellationToken)
	{
		// The read starts now, whether or not anyone reads yet.
		_producer = new(query, this, pool, clearOnDispose, cancellationToken);
		_producerTask = _producer.Start();
	}

	/// <summary>
	/// The complete result, once the read is done.
	/// </summary>
	/// <remarks>
	/// The read started with the buffer; this never starts another.
	/// The task is the same on every access, and the view is built
	/// only if it is asked for.
	/// It faults with the read's exception, except that it is
	/// cancelled when the read ended with an
	/// <see cref="OperationCanceledException"/> of any origin (the
	/// caller's token, which it then carries, or a provider's own),
	/// or when the buffer was disposed first.
	/// Disposing the <see cref="PooledArray{T}"/> it returns
	/// disposes this buffer.
	/// In a narrow race, when a dispose lands just as the read
	/// completes, the task can still complete successfully; every
	/// member of the view then throws
	/// <see cref="ObjectDisposedException"/>.
	/// </remarks>
	/// <exception cref="ObjectDisposedException">
	/// The buffer is disposed.
	/// </exception>
	public Task<PooledArray<T>> Items
	{
		get
		{
			ThrowIfDisposed();

			// A repeat access is one read of the field.
			TaskCompletionSource<PooledArray<T>>? items
				= Volatile.Read(ref _items);
			if (items is not null) return items.Task;

			// Racing first accesses all make a source, one is
			// published, and the losers are dropped, never completed.
			TaskCompletionSource<PooledArray<T>> created = new(
				TaskCreationOptions.RunContinuationsAsynchronously);
			items = Interlocked.CompareExchange(
				ref _items, created, null) ?? created;

			// Finish may have run before the source was published
			// (it read the field first): complete it here.
			if (Volatile.Read(ref _done) != 0) Complete(items);

			return items.Task;
		}
	}

	/// <summary>
	/// Cancels the read if it is still running, and releases
	/// the buffer.
	/// </summary>
	/// <remarks>
	/// Does not throw and does not wait: the producer returns the
	/// arrays to the pool when it exits.
	/// For a pool whose <c>Return</c> throws, see the remarks of the
	/// class.
	/// Use <see cref="DisposeAsync"/> to wait for that.
	/// Calling it again does nothing.
	/// After it, <see cref="Items"/> throws
	/// <see cref="ObjectDisposedException"/>.
	/// </remarks>
	public void Dispose()
		=> _ = DisposeCore();

	/// <summary>
	/// Cancels the read if it is still running, waits for the
	/// producer to exit, and returns the arrays to the pool.
	/// </summary>
	/// <remarks>
	/// The wait has no timeout: it ends when the provider call in
	/// progress returns, so a provider that ignores cancellation
	/// delays it by that call.
	/// Does not throw for a well-behaved pool; for a pool whose
	/// <c>Return</c> throws, see the remarks of the class.
	/// Calling it again does nothing (it throws again if a pool's
	/// <c>Return</c> threw the first time).
	/// After it, <see cref="Items"/> throws
	/// <see cref="ObjectDisposedException"/>.
	/// </remarks>
	/// <returns>A task that completes when the arrays are back.</returns>
	public async ValueTask DisposeAsync()
		=> await DisposeCore().ConfigureAwait(false);

	// The check is load, compare, branch: the throw is out of line.
	internal void ThrowIfDisposed()
	{
		if (Volatile.Read(ref _disposed) != 0) ThrowDisposed();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static void ThrowDisposed()
		=> throw new ObjectDisposedException(nameof(PooledBuffer<>));

	// The producer's task: it completes when the producer has exited.
	Task DisposeCore()
	{
		// The dispose-once guard (a full fence: see the notes).
		if (Interlocked.Exchange(ref _disposed, 1) != 0)
			return _producerTask;

		Volatile.Read(ref _items)?.TrySetCanceled();

		// A running producer releases the arrays itself, in Finish.
		// A finished one has nothing left to cancel or to write.
		if (Volatile.Read(ref _done) == 0) _producer.Cancel();
		else ReleaseAll();

		return _producerTask;
	}

	void ReleaseAll()
	{
		List<IMemoryOwner<T>>? chunks
			= Interlocked.Exchange(ref _chunks, null);
		if (chunks is null) return;

		foreach (IMemoryOwner<T> chunk in chunks) chunk.Release();
	}

	// Completes the Items source from the finished read. Idempotent
	// (TrySet), so Finish and the first accessor may both call it.
	void Complete(TaskCompletionSource<PooledArray<T>> items)
	{
		if (items.Task.IsCompleted) return;

		List<IMemoryOwner<T>>? chunks = Volatile.Read(ref _chunks);
		if (Volatile.Read(ref _disposed) != 0 || chunks is null)
		{
			items.TrySetCanceled();
		}
		else if (_error is OperationCanceledException cancelled)
		{
			items.TrySetCanceled(cancelled.CancellationToken);
		}
		else if (_error is { } error)
		{
			// Observed here: an Items nobody awaits must not raise
			// UnobservedTaskException.
			if (items.TrySetException(error)) _ = items.Task.Exception;
		}
		else
		{
			try
			{
				items.TrySetResult(
					new(this, [.. chunks], _total, _firstRows));
			}
			catch (ObjectDisposedException)
			{
				// A dispose landed while the view was being built (it asks
				// each owner for its memory).
				items.TrySetCanceled();
			}
			catch (Exception ex)
			{
				// An owner that fails when asked for its memory (a pool that
				// breaks its contract): the view cannot be built, and Items
				// fails with that, observed, instead of staying pending.
				if (items.TrySetException(ex)) _ = items.Task.Exception;
			}
		}
	}

	bool IChunkTarget<T>.IsDisposed
		=> Volatile.Read(ref _disposed) != 0;

	int IChunkTarget<T>.CapacityOf(int index)
		=> index < 31 - ChunkSizing.Log2(_firstRows)
			? _firstRows << index
			: throw new InvalidOperationException(
				"A buffer cannot hold more rows than an int counts.");

	// Producer thread only, before Finish.
	bool IChunkTarget<T>.Publish(IMemoryOwner<T> chunk)
	{
		if (Volatile.Read(ref _disposed) != 0) return false;

		_chunks!.Add(chunk);
		_total += chunk.Memory.Length;
		return true;
	}

	void IChunkTarget<T>.Finish(Exception? error)
	{
		_error = error;
		Interlocked.Exchange(ref _done, 1);

		// Done is set, then the field is read (see the notes).
		TaskCompletionSource<PooledArray<T>>? items
			= Volatile.Read(ref _items);
		if (items is not null) Complete(items);

		// Disposed while the producer ran: it is the last that can
		// write, so the arrays go back now.
		if (Volatile.Read(ref _disposed) != 0) ReleaseAll();
	}
}
