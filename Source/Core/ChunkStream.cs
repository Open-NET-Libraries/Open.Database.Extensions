using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// The chunk stream behind ToAsyncBufferStream (and, flattened, behind
// ToBufferedAsyncStream): one consumer, one chunk at a time.
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
// The wait has NO timeout, on purpose (an await in a finally is
// legal; only a yield is not): correctness, meaning no array goes back
// while the producer might still write, comes before promptness. So a
// break or a dispose waits for the provider call in progress (the
// execute, or a read) to return. A provider that ignores the cancel
// token delays it by that call. After that the producer stops at the
// next chunk boundary at the latest: the read loop checks the token
// per row in the default Read mode, the provider does in ReadAsync
// mode, and the producer checks once per chunk (a stream's chunks are
// 16 to 4,096 rows).
// A pool whose Return throws surfaces at the release (the next step,
// or the dispose) and replaces what the consumer was already about to
// see; with clearing on, one that throws inside the background queue is
// swallowed there instead. The remaining arrays go to the garbage
// collector.
// A caller's own cancel is different: the producer reports it through
// the channel, so the consumer's next step throws it, after the chunks
// that were already delivered.
//
// Invariants worth knowing.
// - target.Dispose() BEFORE producer.Cancel() is required: the producer
//   swallows a cancel only while its target says it is disposed;
//   otherwise a cancel that we caused would reach the consumer as an
//   error.
// - StreamTarget never refuses a Publish (an unbounded channel with one
//   writer always accepts), so every published array is in the channel
//   or with the consumer; nothing is returned until the producer has
//   ended, and Drain (after it) releases what the consumer did not
//   take.
// - The producer's first step runs on a thread of the current
//   scheduler (see ChunkProducer).
//
// An iterator that was never started starts nothing and owns nothing.
// So does one whose token was already cancelled at its first step:
// that is checked BEFORE the producer starts, so no command is run and
// no connection is opened.
//
// Abandoning.
// A stream whose enumerator is abandoned without being disposed gives
// nothing back (there is no finalizer): the producer keeps reading the
// WHOLE query into the unbounded channel, so the connection is held
// until that read ends, and the arrays stay reachable until the
// producer exits and the channel is garbage; they are then collected,
// not returned to the pool. Always dispose the enumerator
// (await foreach does).
internal sealed class ChunkStream<T>(
	MappedQueryCore<T> query,
	MemoryPool<T> pool,
	bool clearOnReturn,
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
		// A token that is already cancelled starts nothing: no
		// producer, so no command and no connection.
		cancellationToken.ThrowIfCancellationRequested();

		StreamTarget<T> target = new();
		ChunkProducer<T> producer = new(
			query, target, pool, clearOnReturn, caller);
		Task producerTask = producer.Start();
		try
		{
			ChannelReader<IMemoryOwner<T>> reader = target.Reader;
			do
			{
				// Drain what is ready without awaiting. The token is
				// checked AHEAD of TryRead: a throw after it would leak
				// the chunk's array, and a cancelled token stops the
				// enumeration even when chunks are ready.
				while (
					!cancellationToken.IsCancellationRequested
					&& reader.TryRead(out IMemoryOwner<T>? chunk))
				{
					// The array goes back at the next step.
					try { yield return chunk.Memory; }
					finally { chunk.Release(); }
				}

				cancellationToken.ThrowIfCancellationRequested();
			}

			// False: the producer ended without a failure and every
			// chunk was read. A failure or a cancel is thrown here,
			// after the chunks that were delivered.
			while (await reader
				.WaitToReadAsync(cancellationToken)
				.ConfigureAwait(false));
		}
		finally
		{
			target.Dispose();
			producer.Cancel();

			// The producer cannot fault on this path (Publish is never
			// refused here, so it never releases anything itself), but
			// the drain is in a finally anyway: every array goes back
			// whatever the wait does.
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

	internal StreamTarget()
		=> _lastDoubling = ChunkSizing.Log2(
			ChunkSizing.ResolveStreamMaxRows(_first) / _first);

	internal ChannelReader<IMemoryOwner<T>> Reader
		=> _channel.Reader;

	bool IChunkTarget<T>.IsDisposed
		=> Volatile.Read(ref _disposed) != 0;

	// FirstRows, twice that ... StreamMaxRows rows (16, 32 ... 4096), then
	// the maximum again. Read once, when the stream is made.
	readonly int _first = ChunkSizing.ResolveFirstRows();
	readonly int _lastDoubling;

	int IChunkTarget<T>.CapacityOf(int index)
		=> _first << Math.Min(index, _lastDoubling);

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
