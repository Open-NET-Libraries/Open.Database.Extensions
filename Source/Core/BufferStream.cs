using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

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
