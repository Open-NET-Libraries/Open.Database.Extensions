using System.Runtime.ExceptionServices;

namespace Open.Database.Extensions;

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
/// A custom pool must not throw from <c>Return</c>: if it does,
/// the exception propagates from the dispose, and the arrays not
/// yet returned stay with the garbage collector.
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

	readonly MappedQueryCore<T> _query;
	readonly MemoryPool<T> _pool;
	readonly CancellationToken _caller;
	readonly object _gate = new();
	readonly List<IMemoryOwner<T>> _chunks = [];

	TaskCompletionSource<bool> _signal = NewSignal();
	TaskCompletionSource<PooledArray<T>>? _items;
	ChunkProducer<T>? _producer;
	Task? _producerTask;
	Exception? _error;
	int _total;
	bool _running;
	bool _done;
	int _disposed;

	internal PooledBuffer(
		MappedQueryCore<T> query,
		MemoryPool<T> pool,
		CancellationToken cancellationToken)
	{
		_query = query;
		_pool = pool;
		_caller = cancellationToken;

		// The read starts now, whether or not anyone reads yet.
		Start();
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
	/// It faults with the read's exception, and is cancelled
	/// when the caller's token cancelled the read
	/// or the buffer was disposed first.
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
	/// The buffer is, or is being, disposed.
	/// </exception>
	public async IAsyncEnumerator<ReadOnlyMemory<T>> GetAsyncEnumerator(
		CancellationToken cancellationToken = default)
	{
		ThrowIfDisposed();
		for (int index = 0; ; ++index)
		{
			while (true)
			{
				// Stops this enumeration even when arrays are ready;
				// the read goes on.
				cancellationToken.ThrowIfCancellationRequested();

				Step step = Next(
					index,
					out IMemoryOwner<T>? chunk,
					out Task? wait,
					out Exception? error);

				if (step == Step.Chunk)
				{
					yield return chunk!.Memory;
					break;
				}

				if (step == Step.End)
				{
					if (error is not null)
						ExceptionDispatchInfo.Capture(error).Throw();

					yield break;
				}

				await TaskWait
					.Async(wait!, cancellationToken)
					.ConfigureAwait(false);
			}
		}
	}

	/// <summary>
	/// Cancels the read if it is still running, and releases
	/// the buffer.
	/// </summary>
	/// <remarks>
	/// Does not throw and does not wait: the producer returns the
	/// arrays to the pool when it exits.
	/// A pool whose <c>Return</c> throws breaks its contract: the
	/// exception is lost with the producer's task, or thrown here
	/// when the producer had already finished.
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
	/// Does not throw for a well-behaved pool; the exception of a pool
	/// whose <c>Return</c> throws propagates from here.
	/// Calling it again does nothing.
	/// After it, access throws <see cref="ObjectDisposedException"/>.
	/// </remarks>
	/// <returns>A task that completes when the arrays are back.</returns>
	public async ValueTask DisposeAsync()
	{
		Task? producer = DisposeCore();
		if (producer is not null)
			await producer.ConfigureAwait(false);
	}

	internal void ThrowIfDisposed()
	{
		if (Volatile.Read(ref _disposed) != 0)
		{
			throw new ObjectDisposedException(
				nameof(PooledBuffer<T>));
		}
	}

	void Start()
	{
		lock (_gate)
		{
			_running = true;
			_producer = new(_query, this, _pool, _caller);
			_producerTask = _producer.Start();
		}
	}

	enum Step
	{
		Chunk,
		Wait,
		End,
	}

	Step Next(
		int index,
		out IMemoryOwner<T>? chunk,
		out Task? wait,
		out Exception? error)
	{
		lock (_gate)
		{
			ThrowIfDisposed();
			wait = null;
			error = null;
			chunk = null;

			if (index < _chunks.Count)
			{
				chunk = _chunks[index];
				return Step.Chunk;
			}

			if (_done)
			{
				error = _error;
				return Step.End;
			}

			wait = _signal.Task;
			return Step.Wait;
		}
	}

	Task? DisposeCore()
	{
		ChunkProducer<T>? producer;
		Task? task;
		bool running;
		TaskCompletionSource<bool> signal;
		lock (_gate)
		{
			if (_disposed != 0) return _producerTask;

			Volatile.Write(ref _disposed, 1);
			producer = _producer;
			task = _producerTask;
			running = _running;
			signal = _signal;
			_items?.TrySetCanceled();
		}

		// A reader that waits wakes up and finds the buffer disposed.
		signal.TrySetResult(true);

		// The query is cancelled by the linked token. Nothing is
		// returned while the producer can still write.
		// A producer that is not running has finished: nothing to
		// cancel, and nothing can write.
		if (running) producer?.Cancel();
		else ReturnAll();

		return task;
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

	// Under the lock.
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
			_running = false;
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
