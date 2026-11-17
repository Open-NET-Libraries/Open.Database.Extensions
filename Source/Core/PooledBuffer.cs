using System.Runtime.ExceptionServices;
#if NET
using System.Numerics;
#endif

namespace Open.Database.Extensions;

/// <summary>
/// Every row of a query, held in pooled arrays until the buffer is
/// disposed.
/// </summary>
/// <remarks>
/// <para>
/// Made by <c>ToAsyncBuffer()</c> on a typed view.
/// Nothing runs until the first access:
/// awaiting <see cref="Items"/> or the first read of an enumeration.
/// The read then runs on a background producer, once,
/// however many readers there are.
/// </para>
/// <para>
/// The rows live in arrays rented from the pool the caller passed
/// (<see cref="ArrayPool{T}.Shared"/> when none was).
/// They are returned to the pool when the buffer is disposed,
/// and not before: every <see cref="ReadOnlyMemory{T}"/> this buffer
/// yields, and every <see cref="PooledArray{T}"/> it hands out,
/// is valid until then.
/// Dispose the buffer (<c>await using</c>) when you are done.
/// A buffer that is never disposed is not a leak: its arrays go
/// to the garbage collector, only the pool does not get them back.
/// </para>
/// <para>
/// Enumerating yields each array as it fills,
/// sliced to the rows it holds.
/// It may be enumerated many times and by many readers.
/// Cancelling an enumeration stops that reader only:
/// it does not cancel the read.
/// Disposing the buffer while it is still reading cancels the query.
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
	readonly ArrayPool<T> _pool;
	readonly CancellationToken _caller;
	readonly object _gate = new();
	readonly List<IMemoryOwner<T>> _chunks = [];

	TaskCompletionSource<bool> _signal = NewSignal();
	TaskCompletionSource<PooledArray<T>>? _items;
	ChunkProducer<T>? _producer;
	Task? _producerTask;
	Exception? _error;
	int _total;
	bool _started;
	bool _running;
	bool _done;
	int _disposed;

	internal PooledBuffer(
		MappedQueryCore<T> query,
		ArrayPool<T> pool,
		CancellationToken cancellationToken)
	{
		_query = query;
		_pool = pool;
		_caller = cancellationToken;
	}

	static TaskCompletionSource<bool> NewSignal()
		=> new(TaskCreationOptions.RunContinuationsAsynchronously);

	/// <summary>
	/// The complete result, once the read is done.
	/// Awaiting it starts the read if it has not started.
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
			Start();
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
	/// The first read starts the read of the query if it has not
	/// started.
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
		Start();
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
	/// Never throws and does not wait: the producer returns the
	/// arrays to the pool when it exits.
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
	/// Never throws.
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
			ThrowIfDisposed();
			if (_started) return;

			_started = true;
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
		producer?.Cancel();
		if (!running)
		{
			ReturnAll();
			producer?.DisposeSource();
		}

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

		foreach (IMemoryOwner<T> chunk in taken) chunk.Dispose();
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
			items.TrySetException(_error);
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

/// <summary>
/// The complete result of a <see cref="PooledBuffer{T}"/>:
/// a read-only list over its pooled arrays.
/// </summary>
/// <remarks>
/// <para>
/// It is also a read-only <see cref="ICollection{T}"/>
/// (explicitly: <c>Add</c>, <c>Clear</c> and <c>Remove</c> throw
/// <see cref="NotSupportedException"/>),
/// so LINQ's <c>ToArray</c> and <c>ToList</c> and the
/// <see cref="List{T}"/> constructor know its count and copy it
/// with one pass per array.
/// The buffer itself is not a collection,
/// because it may still be filling.
/// </para>
/// <para>
/// It is valid until it is disposed. Disposing it disposes the
/// buffer it came from, which returns the arrays to the pool.
/// After that, every member throws
/// <see cref="ObjectDisposedException"/>, an enumeration in
/// progress included.
/// It is not safe to use while another thread disposes it.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
public sealed class PooledArray<T>
	: IReadOnlyList<T>, ICollection<T>, IDisposable
{
	readonly PooledBuffer<T> _owner;
	readonly IMemoryOwner<T>[] _chunks;

	internal PooledArray(
		PooledBuffer<T> owner,
		IMemoryOwner<T>[] chunks,
		int count)
	{
		_owner = owner;
		_chunks = chunks;
		Count = count;
	}

	/// <summary>The number of rows.</summary>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public int Count
	{
		get
		{
			_owner.ThrowIfDisposed();
			return field;
		}
	}

	/// <summary>The row at the index, in O(1).</summary>
	/// <param name="index">The zero-based index of the row.</param>
	/// <exception cref="ArgumentOutOfRangeException">
	/// The index is not less than <see cref="Count"/>.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public T this[int index]
	{
		get
		{
			_owner.ThrowIfDisposed();
			if ((uint)index >= (uint)Count)
			{
				throw new ArgumentOutOfRangeException(
					nameof(index), index, "Not a row of this array.");
			}

			int first = PooledBuffer<T>.FirstSize;
			int segment = Log2((index / first) + 1);
			int start = (first << segment) - first;
			return _chunks[segment].Memory.Span[index - start];
		}
	}

	static int Log2(int value)
	{
#if NET
		return BitOperations.Log2((uint)value);
#else
		int log = 0;
		while ((value >>= 1) != 0) ++log;
		return log;
#endif
	}

	/// <summary>Copies every row, in order, to the span.</summary>
	/// <param name="destination">
	/// At least <see cref="Count"/> long.
	/// </param>
	/// <exception cref="ArgumentException">
	/// The span is shorter than <see cref="Count"/>.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public void CopyTo(Span<T> destination)
	{
		if (destination.Length < Count)
		{
			throw new ArgumentException(
				"The destination is shorter than Count.",
				nameof(destination));
		}

		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			Span<T> rows = chunk.Memory.Span;
			rows.CopyTo(destination);
			destination = destination[rows.Length..];
		}
	}

	/// <summary>Enumerates the rows in order.</summary>
	/// <returns>An enumerator over the rows.</returns>
	/// <exception cref="ObjectDisposedException">
	/// The array is disposed.
	/// </exception>
	public IEnumerator<T> GetEnumerator()
	{
		_owner.ThrowIfDisposed();
		return Enumerate();
	}

	IEnumerator<T> Enumerate()
	{
		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			Memory<T> memory = chunk.Memory;
			for (int i = 0; i < memory.Length; ++i)
			{
				// The arrays go back to the pool on dispose: never
				// hand out a row from one after it.
				_owner.ThrowIfDisposed();
				T item = memory.Span[i];
				yield return item;
			}
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
		=> GetEnumerator();

	// Every member checks the owner first, so a disposed view
	// throws ObjectDisposedException, mutators included.
	bool ICollection<T>.IsReadOnly
	{
		get
		{
			_owner.ThrowIfDisposed();
			return true;
		}
	}

	void ICollection<T>.Add(T item)
		=> throw ReadOnly();

	void ICollection<T>.Clear()
		=> throw ReadOnly();

	bool ICollection<T>.Remove(T item)
		=> throw ReadOnly();

	NotSupportedException ReadOnly()
	{
		_owner.ThrowIfDisposed();
		return new("A PooledArray is read-only.");
	}

	// A linear scan over the rows only (never a pooled array's
	// stale tail).
	bool ICollection<T>.Contains(T item)
	{
		_owner.ThrowIfDisposed();
		EqualityComparer<T> comparer = EqualityComparer<T>.Default;
		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			ReadOnlySpan<T> rows = chunk.Memory.Span;
			for (int i = 0; i < rows.Length; ++i)
			{
				if (comparer.Equals(rows[i], item)) return true;
			}
		}

		return false;
	}

	// What List<T>, ToArray and ToList call: one copy per array.
	void ICollection<T>.CopyTo(T[] array, int arrayIndex)
	{
		int count = Count;
		if (array is null)
			throw new ArgumentNullException(nameof(array));

		if (arrayIndex < 0)
		{
			throw new ArgumentOutOfRangeException(
				nameof(arrayIndex), arrayIndex, "Cannot be negative.");
		}

		if (array.Length - arrayIndex < count)
		{
			throw new ArgumentException(
				"The destination is too small.", nameof(array));
		}

		foreach (IMemoryOwner<T> chunk in _chunks)
		{
			Span<T> rows = chunk.Memory.Span;
			rows.CopyTo(array.AsSpan(arrayIndex));
			arrayIndex += rows.Length;
		}
	}

	/// <summary>
	/// Disposes the buffer this came from:
	/// the arrays go back to the pool.
	/// </summary>
	public void Dispose()
		=> _owner.Dispose();
}
