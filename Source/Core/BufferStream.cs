using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// ToAsyncBufferStream: one consumer, one chunk at a time.
//
// Who does what.
// The consumer runs the iterator below. The first MoveNextAsync starts
// ONE producer (ChunkProducer) on a thread-pool thread; the producer is
// the only writer. The channel between them is unbounded and has one
// writer and one reader, so the producer never waits for the consumer:
// the query is read at full speed and the connection ODX opened is
// closed when the read ends, however slowly the consumer goes.
//
// Who owns each array.
// The producer owns the buffer it is filling. Publish (a channel write)
// hands it over; the channel owns it until the consumer takes it; the
// consumer owns it until the next step (or the end of the iterator),
// which releases it. An array therefore goes back to its pool exactly
// once, and only when nothing can write or read it any more.
//
// How it ends.
// The iterator's finally runs when the consumer finishes, fails, breaks
// out of its loop, or disposes the enumerator (await foreach does that
// for a break). It marks the target disposed FIRST (so the producer
// treats the cancel it is about to receive as asked for, and does not
// report it), cancels the producer (the linked token reaches the query
// itself), waits for the producer (so nothing can write any more), and
// only then releases every array still in the channel.
// A caller's own cancel is different: the producer reports it through
// the channel, so the consumer's next step throws it, after the chunks
// that were already delivered.
//
// An iterator that was never started starts nothing and owns nothing.
// A stream that is abandoned without disposing its enumerator returns
// nothing to the pool (there is no finalizer): the arrays are
// garbage-collected.
internal sealed class BufferStream<T>(
	MappedQueryCore<T> query,
	MemoryPool<T>? pool,
	CancellationToken caller)
	: IAsyncEnumerable<ReadOnlyMemory<T>>
{
	int _taken;

	// The one-shot guard is here, not in the iterator: an iterator
	// body runs at the first step, and the second call must throw at
	// the call.
	public IAsyncEnumerator<ReadOnlyMemory<T>> GetAsyncEnumerator(
		CancellationToken cancellationToken = default)
	{
		if (Interlocked.Exchange(ref _taken, 1) != 0)
		{
			throw new InvalidOperationException(
				"This stream can be enumerated only once."
				+ " Call ToAsyncBuffer() to read it many times.");
		}

		return Read(cancellationToken);
	}

	async IAsyncEnumerator<ReadOnlyMemory<T>> Read(
		CancellationToken cancellationToken)
	{
		StreamTarget<T> target = new();
		ChunkProducer<T> producer = new(query, target, pool, caller);
		Task producerTask = producer.Start();
		try
		{
			ChannelReader<IMemoryOwner<T>> reader = target.Reader;
			while (true)
			{
				// Stops this enumeration even when chunks are ready.
				cancellationToken.ThrowIfCancellationRequested();

				if (reader.TryRead(out IMemoryOwner<T>? chunk))
				{
					// The array goes back at the next step.
					try { yield return chunk.Memory; }
					finally { chunk.Release(); }

					continue;
				}

				// False: the producer ended without a failure and every
				// chunk was read. A failure or a cancel is thrown here,
				// after the chunks that were delivered.
				if (!await reader
					.WaitToReadAsync(cancellationToken)
					.ConfigureAwait(false))
				{
					yield break;
				}
			}
		}
		finally
		{
			target.Dispose();
			producer.Cancel();

			// The drain is in a finally: every array goes back
			// whatever the wait does (a pool whose Return throws can
			// fault the producer).
			try { await producerTask.ConfigureAwait(false); }
			finally { target.Drain(); }
		}
	}
}

// The stream's side of the producer: an unbounded channel with one
// writer (the producer) and one reader (the consumer), and the flag
// the consumer sets when it is done.
internal sealed class StreamTarget<T> : IChunkTarget<T>
{
	readonly Channel<IMemoryOwner<T>> _channel
		= Channel.CreateUnbounded<IMemoryOwner<T>>(
			new UnboundedChannelOptions
			{
				SingleWriter = true,
				SingleReader = true,
			});

	int _disposed;

	internal ChannelReader<IMemoryOwner<T>> Reader
		=> _channel.Reader;

	bool IChunkTarget<T>.IsDisposed
		=> Volatile.Read(ref _disposed) != 0;

	// 16, 32 ... 4096 rows, then 4096 again.
	int IChunkTarget<T>.CapacityOf(int index)
		=> 16 << Math.Min(index, 8);

	bool IChunkTarget<T>.Publish(IMemoryOwner<T> chunk)
		=> _channel.Writer.TryWrite(chunk);

	void IChunkTarget<T>.Finish(Exception? error)
		=> _channel.Writer.TryComplete(error);

	internal void Dispose()
		=> Volatile.Write(ref _disposed, 1);

	// Only after the producer ended: nothing can write any more.
	internal void Drain()
	{
		while (_channel.Reader.TryRead(out IMemoryOwner<T>? chunk))
			chunk.Release();
	}
}
