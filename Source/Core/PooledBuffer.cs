using System.Runtime.ExceptionServices;

namespace Open.Database.Extensions;

// How this works (notes for a reader of the code; not the contract).
//
// Who does what.
// The constructor starts ONE producer (ChunkProducer) on a thread-pool
// thread. That producer is the only code that rents buffers and writes
// rows. It calls Publish for each filled buffer and Finish once, last.
// Readers (Items, any number of enumerations) and Dispose run on the
// callers' threads.
//
// The handshake between them.
// _gate guards _chunks, _signal, _items, _done, _error, _total and the
// write of _disposed. (Finish reads _disposed under the same lock
// DisposeCore sets it under: that is what makes the return of the
// arrays exactly-once, see below.) Publish adds a chunk and swaps in a new _signal;
// a reader that must wait takes the CURRENT signal's task under the
// same lock that told it nothing was ready, so a Publish after that
// completes exactly the task it holds: no wake-up is lost. Finish sets
// _done and _error once and completes Items. Items is a snapshot of
// _chunks, safe because no Publish follows Finish. Every task completes with
// RunContinuationsAsynchronously, so no caller code ever runs under
// _gate.
//
// Who owns each array.
// The producer owns the buffer it is filling. Publish hands it to
// _chunks (or refuses once the buffer is disposed, and the producer
// releases it itself). From then on _chunks owns it, until ReturnAll
// takes the whole list out under the lock and releases each one: so an
// array is returned exactly once, and only when nothing can write or
// add to it.
//
// Why dispose is ordered the way it is.
// 1. Under the lock, set _disposed. From then on Publish refuses and
//    readers throw ObjectDisposedException.
// 2. Wake the waiting readers (they find the buffer disposed).
// 3. If the read is still going, Cancel the producer: its linked token
//    cancels the query itself. The producer then ends, swallows that
//    cancel (the owner asked for it: IsDisposed), and in Finish, the
//    last thing that can write, calls ReturnAll. If the read already
//    finished, nothing can write, and Dispose calls ReturnAll itself.
// Either way ReturnAll runs once the producer cannot add anything, and
// DisposeAsync additionally awaits the producer, so its arrays are
// back when it returns. The sync Dispose does not wait.
// A caller's own cancel is the opposite: it is NOT swallowed. Finish
// stores it, Items is cancelled with the caller's token, and every
// enumeration throws it after the arrays already delivered.
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
/// even if nobody reads the buffer yet; it may finish before any
/// read happens.
/// <see cref="Items"/> and every enumeration attach to that one
/// read, however many readers there are.
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
/// In every case the arrays not yet returned stay with the garbage
/// collector.
/// They are returned to the pool when the buffer is disposed,
/// and not before: every <see cref="ReadOnlyMemory{T}"/> this buffer
/// yields, and every <see cref="PooledArray{T}"/> it hands out,
/// is valid until then.
/// Dispose the buffer (<c>await using</c>) when you are done.
/// A buffer that is never disposed is not a leak: it holds its
/// arrays (and the finished read) until the garbage collector
/// takes it, and the pool does not get them back (there is no
/// finalizer).
/// </para>
/// <para>
/// Enumerating yields each array as it fills,
/// sliced to the rows it holds.
/// It may be enumerated many times and by many readers.
/// Cancelling an enumeration stops that reader only:
/// it does not cancel the read.
/// Disposing the buffer while it is still reading cancels the query;
/// after the read finished it only releases the arrays.
/// </para>
/// </remarks>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
public sealed class PooledBuffer<T>
	: IAsyncEnumerable<ReadOnlyMemory<T>>,
		IDisposable,
		IAsyncDisposable,
		IChunkTarget<T>
{
	// The first array holds this many rows; each next one doubles.
	// So row i is in array floor(log2(i / 16 + 1)): O(1).
	internal const int FirstSize = 16;

	readonly object _gate = new();
	readonly ChunkProducer<T> _producer;
	readonly Task _producerTask;
	readonly List<IMemoryOwner<T>> _chunks = [];

	TaskCompletionSource<bool> _signal = NewSignal();
	TaskCompletionSource<PooledArray<T>>? _items;
	Exception? _error;
	int _total;
	bool _done;
	int _disposed;

	internal PooledBuffer(
		MappedQueryCore<T> query,
		MemoryPool<T>? pool,
		CancellationToken cancellationToken)
	{
		// The read starts now, whether or not anyone reads yet.
		_producer = new(query, this, pool, cancellationToken);
		_producerTask = _producer.Start();
	}

	static TaskCompletionSource<bool> NewSignal()
		=> new(TaskCreationOptions.RunContinuationsAsynchronously);

	/// <summary>
	/// The complete result, once the read is done.
	/// It attaches to the read that started with the buffer,
	/// and never starts another.
	/// </summary>
	/// <remarks>
	/// The task is the same on every access.
	/// It faults with the read's exception, except that it is
	/// cancelled when the read ended with an
	/// <see cref="OperationCanceledException"/> of any origin (the
	/// caller's token, which it then carries, or a provider's own),
	/// or when the buffer was disposed first.
	/// Disposing the <see cref="PooledArray{T}"/> it returns
	/// disposes this buffer.
	/// </remarks>
	/// <exception cref="ObjectDisposedException">
	/// The buffer is disposed.
	/// </exception>
	public Task<PooledArray<T>> Items
	{
		get
		{
			lock (_gate)
			{
				ThrowIfDisposed();
				if (_items is null)
				{
					_items = new(
						TaskCreationOptions
							.RunContinuationsAsynchronously);

					if (_done) CompleteItems();
				}

				return _items.Task;
			}
		}
	}

	/// <summary>
	/// Yields each array as it fills, as a
	/// <see cref="ReadOnlyMemory{T}"/> of exactly the rows it holds.
	/// It attaches to the read that started with the buffer,
	/// and never starts another.
	/// </summary>
	/// <remarks>
	/// The memory is valid until the buffer is disposed.
	/// If the read failed, the exception is thrown after the arrays
	/// that were delivered.
	/// <paramref name="cancellationToken"/> stops this enumeration
	/// only, never the read.
	/// </remarks>
	/// <param name="cancellationToken">
	/// Stops this enumeration; the read goes on.
	/// </param>
	/// <returns>An enumerator over the arrays of rows.</returns>
	/// <exception cref="ObjectDisposedException">
	/// Thrown by a <c>MoveNextAsync</c> (the first one, or a later one)
	/// when the buffer is, or is being, disposed; not by this call.
	/// </exception>
	public async IAsyncEnumerator<ReadOnlyMemory<T>> GetAsyncEnumerator(
		CancellationToken cancellationToken = default)
	{
		ThrowIfDisposed();
		for (int index = 0; ; ++index)
		{
			Reading next;
			while (true)
			{
				// Stops this enumeration even when arrays are ready;
				// the read goes on.
				cancellationToken.ThrowIfCancellationRequested();

				next = Next(index);
				if (next.Wait is null) break;

				await TaskWait
					.Async(next.Wait, cancellationToken)
					.ConfigureAwait(false);
			}

			if (next.Chunk is null)
			{
				// The end of the read, or its failure after the arrays
				// that were delivered.
				if (next.Error is not null)
					ExceptionDispatchInfo.Capture(next.Error).Throw();

				yield break;
			}

			yield return next.Chunk.Memory;
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
	/// After it, access throws <see cref="ObjectDisposedException"/>.
	/// </remarks>
	public void Dispose()
		=> _ = DisposeCore();

	/// <summary>
	/// Cancels the read if it is still running, waits for the
	/// producer to exit, and returns the arrays to the pool.
	/// </summary>
	/// <remarks>
	/// Does not throw for a well-behaved pool; for a pool whose
	/// <c>Return</c> throws, see the remarks of the class.
	/// Calling it again does nothing.
	/// After it, access throws <see cref="ObjectDisposedException"/>.
	/// </remarks>
	/// <returns>A task that completes when the arrays are back.</returns>
	public async ValueTask DisposeAsync()
	{
		await DisposeCore().ConfigureAwait(false);
	}

	internal void ThrowIfDisposed()
	{
		if (Volatile.Read(ref _disposed) != 0)
		{
			throw new ObjectDisposedException(
				nameof(PooledBuffer<T>));
		}
	}

	// What a reader finds at its next step, read under the lock:
	// an array to yield; or a task to wait on (nothing is ready and the
	// read is still going); or neither, which is the end, with the
	// failure of the read if it failed.
	readonly struct Reading(
		IMemoryOwner<T>? chunk,
		Task? wait,
		Exception? error)
	{
		internal IMemoryOwner<T>? Chunk { get; } = chunk;

		internal Task? Wait { get; } = wait;

		internal Exception? Error { get; } = error;
	}

	Reading Next(int index)
	{
		lock (_gate)
		{
			ThrowIfDisposed();
			if (index < _chunks.Count)
				return new(_chunks[index], null, null);

			if (_done) return new(null, null, _error);

			return new(null, _signal.Task, null);
		}
	}

	// The producer's task: it completes when the producer has exited.
	Task DisposeCore()
	{
		bool running;
		TaskCompletionSource<bool> signal;
		lock (_gate)
		{
			if (_disposed != 0) return _producerTask;

			// From here on Publish refuses, so nothing is added.
			Volatile.Write(ref _disposed, 1);
			running = !_done;
			signal = _signal;
			_items?.TrySetCanceled();
		}

		// A reader that waits wakes up and finds the buffer disposed.
		signal.TrySetResult(true);

		// The query is cancelled by the linked token. Nothing is
		// returned while the producer can still write: a running
		// producer returns the arrays itself (Finish), the last thing
		// that can write. A finished one has nothing left to cancel.
		if (running) _producer.Cancel();
		else ReturnAll();

		return _producerTask;
	}

	void ReturnAll()
	{
		IMemoryOwner<T>[] taken;
		lock (_gate)
		{
			taken = [.. _chunks];
			_chunks.Clear();
		}

		foreach (IMemoryOwner<T> chunk in taken) chunk.Release();
	}

	// Under the lock, when the read is done and Items was asked for.
	void CompleteItems()
	{
		TaskCompletionSource<PooledArray<T>> items = _items!;
		if (Volatile.Read(ref _disposed) != 0)
			items.TrySetCanceled();
		else if (_error is OperationCanceledException cancelled)
			items.TrySetCanceled(cancelled.CancellationToken);
		else if (_error is not null)
		{
			items.TrySetException(_error);

			// Observed here: an Items nobody awaits must not raise
			// UnobservedTaskException.
			_ = items.Task.Exception;
		}
		else
			items.TrySetResult(new(this, [.. _chunks], _total));
	}

	bool IChunkTarget<T>.IsDisposed
		=> Volatile.Read(ref _disposed) != 0;

	int IChunkTarget<T>.CapacityOf(int index)
		=> index < 27
			? FirstSize << index
			: throw new InvalidOperationException(
				"A buffer cannot hold more rows than an int counts.");

	bool IChunkTarget<T>.Publish(IMemoryOwner<T> chunk)
	{
		TaskCompletionSource<bool> signal;
		lock (_gate)
		{
			if (_disposed != 0) return false;

			_chunks.Add(chunk);
			_total += chunk.Memory.Length;
			signal = _signal;
			_signal = NewSignal();
		}

		signal.TrySetResult(true);
		return true;
	}

	void IChunkTarget<T>.Finish(Exception? error)
	{
		TaskCompletionSource<bool> signal;
		bool dispose;
		lock (_gate)
		{
			_done = true;
			_error = error;
			signal = _signal;
			dispose = _disposed != 0;
			if (_items is not null) CompleteItems();
		}

		signal.TrySetResult(true);

		// Disposed while the producer ran: it is the last that can
		// write, so the arrays go back now.
		if (dispose) ReturnAll();
	}
}
