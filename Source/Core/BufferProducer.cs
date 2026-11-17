using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// The two buffer terminals of the typed views (ToAsyncBuffer and
// ToAsyncBufferStream) share one producer. It runs the query through
// the view's own handler stream (so the connection, the read mode,
// the skip and take and the read-through are the ones every other
// terminal has, as for the channel feed), fills pooled arrays, and
// hands each filled array to its target. The target decides what
// happens to the chunks: the buffer keeps them until it is disposed,
// the stream queues them for one consumer.

// The unit of every chunk is an IMemoryOwner<T> rented from the pool:
// exactly the rows of one buffer. Releasing it clears the rows (for
// types that hold references) and disposes it, which returns the
// buffer to its pool (see MemoryOwnerExtensions).

/// <summary>Where the producer hands its chunks.</summary>
internal interface IChunkTarget<T>
{
	/// <summary>
	/// True once the owner disposed the target: the producer
	/// then stops and what it was cancelled with is not an error.
	/// </summary>
	bool IsDisposed { get; }

	/// <summary>The size of the array that holds chunk number index.</summary>
	int CapacityOf(int index);

	/// <summary>
	/// Takes over a chunk. False means refused (disposed):
	/// the chunk stays the producer's, which returns it.
	/// </summary>
	bool Publish(IMemoryOwner<T> chunk);

	/// <summary>
	/// The producer ended; the error is null for an end that is not
	/// a failure (a complete read, or a cancel the owner asked for).
	/// </summary>
	void Finish(Exception? error);
}

internal static class TaskWait
{
	/// <summary>
	/// Waits for a task that never faults, or until the token fires
	/// (the cancel is thrown; the task is not affected).
	/// </summary>
	internal static async Task Async(
		Task task,
		CancellationToken cancellationToken)
	{
		if (task.IsCompleted || !cancellationToken.CanBeCanceled)
		{
			await task.ConfigureAwait(false);
			return;
		}

		TaskCompletionSource<bool> fired = new(
			TaskCreationOptions.RunContinuationsAsynchronously);
		using (cancellationToken.Register(
			static s => ((TaskCompletionSource<bool>)s!)
				.TrySetResult(true),
			fired))
		{
			Task first = await Task
				.WhenAny(task, fired.Task)
				.ConfigureAwait(false);

			if (first != task)
				cancellationToken.ThrowIfCancellationRequested();
		}
	}
}

internal sealed class ChunkProducer<T>(
	MappedQueryCore<T> query,
	IChunkTarget<T> target,
	MemoryPool<T> pool,
	CancellationToken caller)
{
	// The caller's token and the owner's dispose, as one token:
	// it goes to the query, so a dispose cancels the database read.
	readonly CancellationTokenSource _linked
		= CancellationTokenSource.CreateLinkedTokenSource(caller);

	IMemoryOwner<T>? _owner;
	T[]? _array;
	int _offset;
	int _count;
	int _index;
	int _capacity;

	/// <summary>
	/// Starts the producer, once. A token that is already cancelled
	/// starts nothing: the target ends with that cancel.
	/// </summary>
	internal Task Start()
	{
		if (caller.IsCancellationRequested)
		{
			target.Finish(new OperationCanceledException(caller));
			_linked.Dispose();
			return Task.CompletedTask;
		}

		// StartNew plus Unwrap, not Task.Run: it keeps
		// TaskScheduler.Current (the lambda is async, so unwrap it).
		return Task.Factory.StartNew(
			RunAsync,
			CancellationToken.None,
			TaskCreationOptions.DenyChildAttach,
			TaskScheduler.Current)
			.Unwrap();
	}

	/// <summary>Cancels the query. Never throws.</summary>
	/// <remarks>
	/// A provider's cancel callback may throw (the source then throws
	/// an <see cref="AggregateException"/>), and the source may be
	/// disposed already: the owner asked for the cancel, so neither
	/// matters to it.
	/// </remarks>
	internal void Cancel()
	{
		try { _linked.Cancel(); }
		catch (Exception) { }
	}

	async Task RunAsync()
	{
		Exception? error = null;
		try
		{
			await query
				.ReadAsync<long>(FillAsync, _linked.Token)
				.ConfigureAwait(false);
		}
		catch (OperationCanceledException) when (target.IsDisposed)
		{
			// The owner asked for this: not an error.
		}
		catch (OperationCanceledException ex)
			when (caller.IsCancellationRequested)
		{
			// The query ran on the linked token: the reader sees the
			// caller's own.
			error = new OperationCanceledException(
				ex.Message, ex, caller);
		}
		catch (Exception ex)
		{
			error = ex;
		}

		// The rows read so far are delivered, even on a failure.
		// The source is disposed whatever happens here (a pool whose
		// Return throws, for one); Cancel() stays safe after that.
		try
		{
			Seal();
			target.Finish(error);
		}
		finally
		{
			_linked.Dispose();
		}
	}

	async ValueTask<long> FillAsync(
		IAsyncEnumerable<T> rows,
		CancellationToken cancellationToken)
	{
		long total = 0;
		await foreach (T row in rows
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			// A provider that reads synchronously never looks at
			// the token, so the loop does.
			cancellationToken.ThrowIfCancellationRequested();

			if (_owner is null || _count == _capacity) Advance();

			// Straight into the array behind the memory (the shared
			// pool's memory is an array); a memory that is not an array
			// goes through its span.
			if (_array is not null)
				_array[_offset + _count] = row;
			else
				_owner!.Memory.Span[_count] = row;

			++_count;
			++total;
		}

		return total;
	}

	// Hands the full buffer on and rents the next one.
	void Advance()
	{
		if (_owner is not null)
		{
			IMemoryOwner<T> full = Take();
			if (!target.Publish(full))
			{
				full.Release();
				throw new OperationCanceledException();
			}
		}

		_capacity = target.CapacityOf(_index++);
		IMemoryOwner<T> owner = pool.Rent(_capacity);
		Debug.Assert(owner.Memory.Length >= _capacity);
		_owner = owner;

		if (MemoryMarshal.TryGetArray<T>(
			owner.Memory,
			out ArraySegment<T> segment)
			&& segment.Array is not null)
		{
			_array = segment.Array;
			_offset = segment.Offset;
		}
		else
		{
			_array = null;
			_offset = 0;
		}
	}

	// The buffer being filled, cut to the rows it holds
	// (the same owner when the pool gave exactly that).
	IMemoryOwner<T> Take()
	{
		IMemoryOwner<T> owner = _owner!.Slice(_count);
		_owner = null;
		_array = null;
		_offset = 0;
		_count = 0;
		return owner;
	}

	// Hands on the buffer that is still being filled, if it has rows.
	void Seal()
	{
		if (_owner is null) return;

		// Advance runs just before a row is stored: an open buffer
		// always holds at least one row.
		Debug.Assert(_count > 0);
		IMemoryOwner<T> last = Take();
		if (!target.Publish(last)) last.Release();
	}
}

// The stream of ToAsyncBufferStream: one consumer, one chunk at a
// time. The channel is unbounded, so the producer never waits for the
// consumer and the connection is held for the read only.
internal sealed class BufferStream<T>(
	MappedQueryCore<T> query,
	MemoryPool<T> pool,
	CancellationToken caller)
	: IAsyncEnumerable<ReadOnlyMemory<T>>
{
	int _taken;

	public IAsyncEnumerator<ReadOnlyMemory<T>> GetAsyncEnumerator(
		CancellationToken cancellationToken = default)
	{
		if (Interlocked.Exchange(ref _taken, 1) != 0)
		{
			throw new InvalidOperationException(
				"This stream can be enumerated only once."
				+ " Call ToAsyncBuffer() to read it many times.");
		}

		return new BufferStreamEnumerator<T>(
			query, pool, caller, cancellationToken);
	}
}

internal sealed class BufferStreamEnumerator<T>(
	MappedQueryCore<T> query,
	MemoryPool<T> pool,
	CancellationToken caller,
	CancellationToken cancellationToken)
	: IAsyncEnumerator<ReadOnlyMemory<T>>, IChunkTarget<T>
{
	// One writer (the producer) and one reader (the consumer).
	readonly Channel<IMemoryOwner<T>> _channel
		= Channel.CreateUnbounded<IMemoryOwner<T>>(
			new UnboundedChannelOptions
			{
				SingleWriter = true,
				SingleReader = true,
			});

	ChunkProducer<T>? _producer;
	Task _producerTask = Task.CompletedTask;
	IMemoryOwner<T>? _current;
	int _disposed;

	/// <summary>
	/// The rows of the current chunk: valid until the next
	/// <see cref="MoveNextAsync"/> or the disposal.
	/// </summary>
	/// <remarks>
	/// After <see cref="DisposeAsync"/>, <see cref="MoveNextAsync"/>
	/// returns false (the <see cref="IAsyncEnumerator{T}"/>
	/// convention), unlike <see cref="PooledBuffer{T}"/>, which
	/// throws <see cref="ObjectDisposedException"/>.
	/// </remarks>
	public ReadOnlyMemory<T> Current
		=> _current is null ? default : _current.Memory;

	bool IChunkTarget<T>.IsDisposed
		=> Volatile.Read(ref _disposed) != 0;

	int IChunkTarget<T>.CapacityOf(int index)
		=> 16 << Math.Min(index, 8); // 16, 32 ... 4096.

	bool IChunkTarget<T>.Publish(IMemoryOwner<T> chunk)
		=> _channel.Writer.TryWrite(chunk);

	void IChunkTarget<T>.Finish(Exception? error)
		=> _channel.Writer.TryComplete(error);

	public async ValueTask<bool> MoveNextAsync()
	{
		if (Volatile.Read(ref _disposed) != 0) return false;

		// Stops this enumeration even when chunks are ready.
		cancellationToken.ThrowIfCancellationRequested();

		// The previous chunk's array goes back now.
		ReleaseCurrent();

		if (_producer is null)
		{
			_producer = new(query, this, pool, caller);
			_producerTask = _producer.Start();
		}

		ChannelReader<IMemoryOwner<T>> reader = _channel.Reader;
		while (true)
		{
			if (reader.TryRead(out IMemoryOwner<T>? chunk))
			{
				_current = chunk;
				return true;
			}

			// False: the producer ended without a failure and every
			// chunk was read. A failure or a cancel is thrown here,
			// after the chunks that were delivered.
			if (!await reader
				.WaitToReadAsync(cancellationToken)
				.ConfigureAwait(false))
			{
				return false;
			}
		}
	}

	void ReleaseCurrent()
	{
		IMemoryOwner<T>? current = _current;
		_current = null;
		current?.Release();
	}

	public async ValueTask DisposeAsync()
	{
		if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

		ReleaseCurrent();
		if (_producer is not { } producer) return;

		// Cancel the query, wait for the producer (it never throws),
		// and only then return what it left in the channel: no array
		// goes back while something can still write to it.
		// The drain is in a finally: every array goes back whatever
		// the cancel or the wait does.
		try
		{
			producer.Cancel();
			await _producerTask.ConfigureAwait(false);
		}
		finally
		{
			while (_channel.Reader.TryRead(out IMemoryOwner<T>? chunk))
				chunk.Release();
		}
	}
}

// ToBufferedAsyncStream: the chunk stream above, flattened. Each
// element is copied into a local before it is yielded, so no span
// lives across a yield and the chunk's array goes back to the pool
// at the next chunk step, as for the chunk stream.
internal sealed class BufferedStream<T>(
	MappedQueryCore<T> query,
	MemoryPool<T> pool,
	CancellationToken caller)
	: IAsyncEnumerable<T>
{
	readonly BufferStream<T> _chunks = new(query, pool, caller);

	public IAsyncEnumerator<T> GetAsyncEnumerator(
		CancellationToken cancellationToken = default)
		// Throws at the call when the stream was already taken.
		=> Flatten(_chunks.GetAsyncEnumerator(cancellationToken));

	static async IAsyncEnumerator<T> Flatten(
		IAsyncEnumerator<ReadOnlyMemory<T>> chunks)
	{
		await using (chunks.ConfigureAwait(false))
		{
			while (await chunks.MoveNextAsync().ConfigureAwait(false))
			{
				ReadOnlyMemory<T> memory = chunks.Current;
				for (int i = 0; i < memory.Length; ++i)
				{
					T item = memory.Span[i];
					yield return item;
				}
			}
		}
	}
}
