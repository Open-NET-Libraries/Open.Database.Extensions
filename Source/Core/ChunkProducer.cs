namespace Open.Database.Extensions.Core;

/// <summary>
/// Reads a query into pooled chunks and hands each filled chunk to an
/// <see cref="IChunkTarget{T}"/>: the one producer behind the buffer
/// terminals (<c>ToAsyncBuffer</c>, <c>ToAsyncBufferStream</c> and
/// <c>ToBufferedAsyncStream</c>).
/// </summary>
/// <remarks>
/// Start it once; it runs on a thread of the ambient task scheduler.
/// Each filled chunk goes to the target, or is released if the target refuses it,
/// and the rows read before a failure or a cancel are delivered first.
/// The target is finished last, exactly once (<see cref="IChunkTarget{T}.Finish"/>),
/// with the read's error if there was one: a cancel the owner asked for (the target
/// is disposed) is swallowed, and a cancel from the caller's token reaches the target
/// as an <see cref="OperationCanceledException"/> carrying that token.
/// A caller that blocks on the returned task while the scheduler has a single thread
/// can deadlock: await it instead.
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
/// <param name="query">The query to read.</param>
/// <param name="target">Receives each filled chunk, and the end of the read.</param>
/// <param name="config">Rents, seals and releases the pooled chunks.</param>
/// <param name="caller">The caller's cancellation token.</param>
// What it does.
// It runs the query through the same internal row-sink path as
// ToArrayAsync and ToListAsync
// (MappedQueryCore.FillAsync into an IRowSink, via
// ChunkSink<T>), so the connection, the read mode, Skip and
// Take and the read-through of later result sets are the ones every
// other terminal has, and there is no per-row enumerator.
// It copies each row into a buffer rented from the pool and hands each
// filled buffer to its target.
// The target decides what happens to the chunks: the buffer keeps them
// until it is disposed, the stream queues them for its one consumer.
//
// Threads.
// Start runs RunAsync once, on a thread of the
// ambient task scheduler (normally the thread pool; see below).
// It is the only code that writes rows or rents buffers.
// Everything else (readers, dispose) runs on callers' threads and talks
// to it only through the target and through Cancel.
//
// Why each non-obvious construct is here.
//
// - Task.Factory.StartNew(..., TaskScheduler.Current).Unwrap(), not
//   Task.Run: the producer runs on the ambient scheduler (a design
//   decision).
//   RunAsync is async, so StartNew returns a
//   Task<Task>, which is unwrapped.
//   TaskCreationOptions.DenyChildAttach makes a child task that
//   the read creates with TaskCreationOptions.AttachedToParent
//   not attach to the producer's task (its completion would
//   otherwise wait for the child).
//   The producer's own task is detached because it is not created with
//   AttachedToParent.
//   Consequence: on a single-thread scheduler, the producer's first step
//   (everything up to the first real await, which for a
//   provider that completes synchronously can be the whole read) needs that
//   thread.
//   A caller that blocks that thread waiting for Items.Result can
//   therefore deadlock; await it instead.
// - The linked token source: the query runs on a token that fires when the
//   caller's token fires or when the owner disposes, so a dispose
//   cancels the database read itself and not only this loop.
// - Both kinds of cancel end the same way here, but are reported
//   differently: a cancel the owner asked for (the target is disposed) is
//   swallowed; a cancel from the caller's token is reported to the target
//   as an OperationCanceledException carrying the
//   caller's token; any other failure is reported as it is.
// - The rows read before a failure or a cancel are delivered first: the
//   buffer that is still being filled is handed over (sealed) before
//   IChunkTarget<T>.Finish.
//
// Invariants.
//
// - Every chunk is sealed into an ArrayLease<T> when it leaves
//   the producer: the array, the rows written to it and what releasing it
//   does (ClearMode: none, clear where it is released, or
//   clear in the recycle bin; see ChunkConfig<T>).
//   The lease is made before the attempt to publish it, so a chunk the
//   target refuses, and the partial chunk sealed on a cancel or a fault, are
//   cleared like any other: they hold rows.
//   Only the rows that were written are cleared (a row is stored before the
//   count moves, so a fault leaves nothing past it).
//   Nothing on the consumer's side knows.
// - The pool must give an array of at least the capacity asked for.
//   ChunkConfig<T>.Rent checks it once per chunk: a pool that
//   gives less breaks its contract, the array is given back and the read
//   fails with an InvalidOperationException (it goes the
//   normal fault path, so Items and the stream complete with it).
//   It does not clamp: the O(1) indexer needs every chunk exactly its
//   planned number of rows.
//   A pool may give a longer array; only the planned rows are used.
// - A buffer is owned by exactly one party at a time: the producer while it
//   fills it, then the target (IChunkTarget<T>.Publish returned
//   true), or the producer again if the target refused it
//   (the target is disposed), in which case it releases it at once.
// - Once the producer has started,
//   IChunkTarget<T>.Finish is called exactly once, last,
//   whatever happens: a failure of the final hand-over (a pool whose
//   Return throws) is recorded as the read's error, so Items
//   and the stream still complete.
//   Nothing is written after it.
// - The linked source is disposed when RunAsync ends, whatever
//   happens.
//   Cancel is safe at any time: once the read is over it does
//   nothing, so a stream that ends normally does not touch the disposed
//   source and throws no exception.
// - The connection ODX opened is closed by the query when the read ends, not
//   by this class.
internal sealed class ChunkProducer<T>(
	MappedQueryCore<T> query,
	IChunkTarget<T> target,
	ChunkConfig<T> config,
	CancellationToken caller)
{
	/// <summary>
	/// The caller's token and the owner's dispose, as one token: it goes to
	/// the query, so a dispose cancels the database read.
	/// </summary>
	readonly CancellationTokenSource _linked
		= CancellationTokenSource.CreateLinkedTokenSource(caller);

	/// <summary>The buffer being filled: the hot path of <see cref="Add"/>.</summary>
	/// <remarks>
	/// <c>_next</c> is the next free slot and <c>_limit</c> the number of
	/// rows this buffer holds: <see cref="Add"/> is one compare, one store and
	/// one increment, and the slow path (a full or a missing buffer) is a
	/// separate method.
	/// </remarks>
	T[]? _array;
	int _next;
	int _limit;

	int _index;
	int _ended;

	/// <summary>The rows in the buffers already handed on.</summary>
	/// <remarks>Moves once per buffer, in <see cref="Take"/>, never per row.</remarks>
	int _taken;

	/// <summary>Gets the number of rows stored so far.</summary>
	// Read when a row fails to map, to name the row.
	public int Count
		=> _taken + _next;

	/// <summary>
	/// Starts the producer, once. A token that is already cancelled
	/// starts nothing: the target ends with that cancel.
	/// </summary>
	/// <returns>
	/// A task that completes when the producer has ended (and the target has
	/// been finished).
	/// </returns>
	[SuppressMessage("Roslynator", "RCS1229:Use async/await when necessary")]
	public Task Start()
	{
		if (caller.IsCancellationRequested)
		{
			// Nothing starts, and the read is over: a Cancel can do nothing.
			Volatile.Write(ref _ended, 1);
			target.Finish(new OperationCanceledException(caller));
			_linked.Dispose();
			return Task.CompletedTask;
		}

		// StartNew plus Unwrap, not Task.Run: it keeps
		// TaskScheduler.Current (RunAsync is async: unwrap the Task<Task>).
		// A scheduler that throws when queueing leaves nothing started, so
		// the source is disposed here and the exception goes to the caller.
		try
		{
			return Task.Factory.StartNew(
				RunAsync,
				CancellationToken.None,
				TaskCreationOptions.DenyChildAttach,
				TaskScheduler.Current)
				.Unwrap();
		}
		catch
		{
			_linked.Dispose();
			throw;
		}
	}

	/// <summary>Cancels the query. Never throws.</summary>
	/// <remarks>
	/// Does nothing once the read is over.
	/// </remarks>
	// A provider's cancel callback may throw (the source then throws an
	// AggregateException), and in a narrow race the source may be disposed
	// already: the owner asked for the cancel, so neither matters to it.
	public void Cancel()
	{
		if (Volatile.Read(ref _ended) != 0) return;

		try { _linked.Cancel(); }
		catch { }
	}

	/// <summary>
	/// Runs the read to its end, then delivers what was read and tells the
	/// target how it ended.
	/// </summary>
	/// <returns>A task that represents the asynchronous operation.</returns>
	async Task RunAsync()
	{
		Exception? error = null;
		try
		{
			await query
				.FillAsync(new ChunkSink<T>(this), _linked.Token)
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

		// The read is over: a Cancel can do nothing more.
		Volatile.Write(ref _ended, 1);

		// The rows read so far are delivered, even on a failure. If
		// handing them over fails (a pool whose release throws), that is
		// the read's error (the first error stays), and Finish still
		// runs: without it Items and the stream would never complete.
		// The source is disposed whatever happens here.
		try { Seal(); }
		catch (Exception ex) { error ??= ex; }

		try
		{
			target.Finish(error);
		}
		finally
		{
			_linked.Dispose();
		}
	}

	/// <summary>Stores one row, from the read loop, in the current buffer.</summary>
	/// <param name="row">The mapped row.</param>
	// It goes straight into the buffer's array through ChunkSink<T>: no
	// enumerator and no allocation, the same loop that ToArrayAsync uses, with
	// the buffer's bounds held in two fields.
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T row)
	{
		int next = _next;
		if (next < _limit)
		{
			_array![next] = row;
			_next = next + 1;
			return;
		}

		AddSlow(row);
	}

	/// <summary>Stores a row when the buffer is full or there is none yet.</summary>
	/// <param name="row">The mapped row.</param>
	void AddSlow(T row)
	{
		if (_array is null || _next == _limit) Advance();

		// The row is stored before the count moves: a store that fails leaves
		// nothing counted (Seal relies on it).
		_array![_next] = row;
		++_next;
	}

	/// <summary>Hands the full buffer on and rents the next one.</summary>
	/// <exception cref="OperationCanceledException">
	/// The token was cancelled, or the target refused the full chunk.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// The pool gave less than the capacity asked for, or the target cannot take more rows.
	/// </exception>
	void Advance()
	{
		// Once per chunk: the read loop looks at the token per row in the
		// default read mode, and the provider does in the async read mode, so a
		// cancel that a provider ignores still ends the read here, at the next
		// chunk. The chunk being filled is sealed and delivered, as for every
		// cancel.
		_linked.Token.ThrowIfCancellationRequested();

		if (_array is not null)
		{
			ArrayLease<T> full = Take();
			if (!target.Publish(full))
			{
				full.Release();
				throw new OperationCanceledException();
			}
		}

		int capacity = target.CapacityOf(_index++);

		// The rent checks that the pool gave at least what was asked for,
		// and gives the array back if not: nothing is half-open.
		T[] array = config.Rent(capacity);

		_next = 0;
		_limit = capacity;

		// Kept last: the buffer is open only once its bounds are set.
		_array = array;
	}

	/// <summary>The buffer being filled, sealed with the rows it holds.</summary>
	/// <returns>The lease of the buffer, which is no longer open.</returns>
	ArrayLease<T> Take()
	{
		ArrayLease<T> lease = config.Seal(_array!, _next);
		_taken += _next;
		_array = null;
		_next = 0;
		_limit = 0;
		return lease;
	}

	/// <summary>Hands on the buffer that is still being filled, if there is one.</summary>
	/// <exception cref="InvalidOperationException">
	/// The buffer holds no row (it is released, not published).
	/// </exception>
	void Seal()
	{
		if (_array is null) return;

		// A buffer is opened just before a row is stored in it, so it
		// holds a row unless storing that row failed (the failure is the
		// read's error, and it is recorded first). An empty buffer is
		// never published: it is released, and this is reported (the
		// first error stays).
		if (_next == 0)
		{
			ArrayLease<T> empty = Take();
			empty.Release();
			throw new InvalidOperationException(
				"The producer was left holding a buffer without a row.");
		}

		ArrayLease<T> last = Take();
		if (!target.Publish(last)) last.Release();
	}
}
