namespace Open.Database.Extensions;

/// <summary>
/// Holds every row of a query in pooled arrays until the buffer is
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
/// The connection ODX opened is closed by the query when the read ends,
/// which is before the result is complete: slow readers never hold it.
/// </para>
/// <para>
/// While the buffer is not disposed its rows stay reachable, and every
/// <see cref="PooledArray{T}"/> it hands out is valid.
/// The arrays go back to the pool when the buffer is disposed, and not
/// before.
/// Dispose the buffer (<see langword="await"/> <see langword="using"/>) when you are done.
/// There is no finalizer: a buffer that is never disposed is not a leak, but
/// it holds its arrays (and the finished read) until the garbage collector
/// takes it, and the pool does not get them back.
/// </para>
/// <para>
/// Returning an array to a custom pool must not throw.
/// If it does, every other array is still returned, and the first exception
/// propagates from <see cref="DisposeAsync"/>, and from
/// <see cref="Dispose"/> when the read had already finished;
/// from <see cref="Dispose"/> while the read is still running it
/// ends in the producer's task, which nobody observes.
/// With clearing on, an array of 128 KB or more of rows (value rows included) is cleared
/// and returned by an internal background queue, where a return that throws
/// is swallowed and costs only that array.
/// When the queue is full the array is cleared and returned by the caller
/// that releases it instead.
/// </para>
/// <para>
/// When the rows are to be cleared (see <c>UsePool</c>), they are cleared as
/// the arrays go back to the pool, and that happens when the buffer, or the
/// <see cref="PooledArray{T}"/> view of it, is disposed (not when the read
/// completes).
/// An array of less than 128 KB of rows is cleared where
/// it is released; an array of 128 KB or more of rows (value rows included) is cleared off
/// the caller's path, on an internal background queue.
/// (If that queue is ever full, it is cleared where it is released.)
/// </para>
/// <para>
/// Disposing the buffer while it is still reading cancels the query,
/// and waits (<see cref="DisposeAsync"/>) for the provider call in
/// progress to return: a provider that ignores cancellation delays
/// the dispose by that call.
/// After the read finished, disposing only releases the arrays.
/// </para>
/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
/// </remarks>
/// <include file="Internals.docs.xml" path="docs/members[@name='row']/*" />
public sealed class PooledBuffer<T>
	: IDisposable,
		IAsyncDisposable,
		IChunkTarget<T>
{
	/// <summary>The rows of the first array.</summary>
	/// <remarks>
	/// A power of two, 16 unless a test changed it (see
	/// <see cref="ChunkSizes"/>); each next array doubles. So row <c>i</c> is
	/// in array <c>floor(log2(i / FirstRows + 1))</c>: O(1).
	/// </remarks>
	readonly int _firstRows;

	/// <summary>The one producer of this buffer.</summary>
	/// <remarks>
	/// <para>
	/// The constructor starts it on a thread of the ambient task scheduler
	/// (normally the thread pool). It is the only code
	/// that rents buffers and writes rows. It calls
	/// <see cref="IChunkTarget{T}.Publish"/> for each filled buffer and
	/// <see cref="IChunkTarget{T}.Finish"/> once, last.
	/// Everyone else (<see cref="Items"/>, dispose) runs on callers' threads.
	/// The buffer is not readable while it fills: the rows are reachable only
	/// through the completed view (<see cref="Items"/>); overlapping reads are
	/// the stream terminals.
	/// </para>
	/// <para>
	/// There is no lock. The fields that are shared each have one rule, given
	/// on each of them.
	/// </para>
	/// <para>
	/// <see langword="null"/> for a buffer made by <see cref="From(ReadOnlySpan{T})"/>: it is done from the
	/// start, and <c>DisposeCore</c> only reads this when it is not.
	/// </para>
	/// </remarks>
	readonly ChunkProducer<T>? _producer;
	/// <summary>The producer's task: it completes when the producer has exited.</summary>
	readonly Task _producerTask;

	/// <summary>The filled chunks.</summary>
	/// <remarks>
	/// <para>
	/// Written only by the producer (in <see cref="IChunkTarget{T}.Publish"/>),
	/// and read by others only after <c>_done</c> is set, which publishes it.
	/// <see langword="null"/> once released.
	/// </para>
	/// <para>
	/// <em>Who owns each array.</em>
	/// The producer owns the buffer it is filling.
	/// Publish appends it here (or refuses once the buffer is disposed, and the
	/// producer releases it itself).
	/// From then on this list owns it, until <c>ReleaseAll</c> takes the list
	/// and releases each one.
	/// An array is therefore released exactly once, and only when nothing can
	/// write or add to it: <c>ReleaseAll</c> runs in
	/// <see cref="IChunkTarget{T}.Finish"/> (the producer's last act) when the
	/// buffer was disposed while the read ran, and in <see cref="Dispose"/>
	/// when the read had already finished.
	/// </para>
	/// </remarks>
	List<ArrayLease<T>>? _chunks = [];
	/// <summary>The source of the <see cref="Items"/> task.</summary>
	/// <remarks>
	/// <para>
	/// Items is lazy. The view (the <see cref="PooledArray{T}"/>, with its
	/// snapshot of the leases) is built only if someone asks, and then once.
	/// The task source is published with a <c>CompareExchange</c> (the winner is
	/// taken); the field is read into a local first, so a repeat access is one
	/// volatile read.
	/// </para>
	/// <para>
	/// Finish sets <c>_done</c> and <em>then</em> reads this field; Items
	/// publishes the source and <em>then</em> reads <c>_done</c>: one of them
	/// sees the other, and both complete with idempotent <c>TrySet</c>, so no
	/// completion is lost.
	/// It is not <c>LazyInitializer</c> or <see cref="Lazy{T}"/> because those
	/// take a delegate.
	/// The snapshot is safe because no Publish follows Finish.
	/// </para>
	/// </remarks>
	TaskCompletionSource<PooledArray<T>>? _items;
	/// <summary>How the read failed, if it did.</summary>
	/// <remarks>Written once, before <c>_done</c> is set.</remarks>
	Exception? _error;
	/// <summary>The number of rows in the published chunks.</summary>
	/// <remarks>Written only by the producer, like <c>_chunks</c>.</remarks>
	int _total;
	/// <summary>Whether the read is over (<see cref="IChunkTarget{T}.Finish"/> ran).</summary>
	/// <remarks>
	/// Set by Finish with an <c>Interlocked.Exchange</c> (a full fence).
	/// Finish and dispose each set their flag, <em>then</em> read the other's.
	/// With full fences on both writes, at least one of them sees the other's
	/// flag, so the arrays are always released by someone; both may see it,
	/// and <c>ReleaseAll</c> takes the list with an <c>Interlocked.Exchange</c>,
	/// so the arrays are released exactly once.
	/// </remarks>
	int _done;
	/// <summary>Whether the buffer is disposed: the dispose-once guard.</summary>
	/// <remarks>Set by dispose with an <c>Interlocked.Exchange</c>.</remarks>
	int _disposed;

	/// <summary>Creates the buffer and starts its read.</summary>
	/// <remarks>The read starts now, whether or not anyone reads yet.</remarks>
	/// <include file="Internals.docs.xml" path="docs/members[@name='chunk-read']/*" />
	/// <param name="cancellationToken">The caller's cancellation token.</param>
	internal PooledBuffer(
		MappedQueryCore<T> query,
		ChunkConfig<T> config,
		CancellationToken cancellationToken)
	{
		_firstRows = config.Sizes.FirstRows;

		// The read starts now, whether or not anyone reads yet.
		_producer = new(query, this, config, cancellationToken);
		_producerTask = _producer.Start();
	}

	/// <summary>Creates a buffer whose read is already over: no producer, done from the start.</summary>
	/// <param name="chunks">The leases of its chunks.</param>
	/// <param name="total">The number of rows in them.</param>
	/// <param name="firstRows">The rows of the first chunk (a power of two).</param>
	PooledBuffer(List<ArrayLease<T>> chunks, int total, int firstRows)
	{
		_firstRows = firstRows;
		_producerTask = Task.CompletedTask;
		_chunks = chunks;
		_total = total;
		_done = 1;
	}

	/// <summary>Makes the buffer that owns <see cref="PooledArray{T}.Empty"/>: it holds nothing and is never disposed.</summary>
	/// <returns>A finished buffer with no rows.</returns>
	internal static PooledBuffer<T> CreateEmptyOwner()
		=> new([], 0, 1);

	/// <summary>
	/// Makes a finished buffer from rows you already have, as if a read had produced them.
	/// </summary>
	/// <remarks>
	/// For a test that fakes the code that returns a buffer, and for any code that needs a buffer without a database:
	/// one array is rented from <see cref="ArrayPool{T}.Shared"/> and the rows are copied into it.
	/// Nothing runs in the background and <see cref="Items"/> is complete at once.
	/// Disposing the buffer sends the array back to the pool: at once for a small array, and for an array of
	/// 128 KB or more of rows that must be cleared, through the recycle bin, so it reaches the pool shortly after.
	/// Rows that are references are cleared when it goes back, as for a read.
	/// No rows give <see cref="PooledArray{T}.Empty"/> and rent nothing.
	/// </remarks>
	/// <param name="rows">The rows. They are copied: the buffer does not keep this span.</param>
	/// <returns>The finished buffer.</returns>
	/// <exception cref="ArgumentOutOfRangeException">There are more than 2^30 rows.</exception>
	public static PooledBuffer<T> From(ReadOnlySpan<T> rows)
	{
		if (rows.Length > MaxFromRows) throw TooManyRows(rows.Length);
		Contract.EndContractBlock();

		// This entry picks the pool and the clearing: the shared pool, and the app-wide default.
		return Create(
			rows,
			new(ArrayPool<T>.Shared, null, ChunkSizes.Default, RecycleBin.Default, ChunkConfig<T>.DefaultDeferFromBytes));
	}

	/// <summary>Makes a finished buffer from rows you already have, with an array from a pool you choose.</summary>
	/// <remarks>
	/// The same as <see cref="From(ReadOnlySpan{T})"/>, except that the one array is rented from
	/// <paramref name="pool"/> and goes back to it when the buffer is disposed.
	/// Rows that are references are always cleared when it goes back; other rows are cleared when
	/// <paramref name="clearOnReturn"/> is <see langword="true"/>.
	/// </remarks>
	/// <param name="rows">The rows. They are copied: the buffer does not keep this span.</param>
	/// <param name="pool">The pool the array comes from and goes back to.</param>
	/// <param name="clearOnReturn">Whether the rows are cleared when the array goes back.</param>
	/// <returns>The finished buffer.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="pool"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentOutOfRangeException">There are more than 2^30 rows.</exception>
	public static PooledBuffer<T> From(ReadOnlySpan<T> rows, ArrayPool<T> pool, bool clearOnReturn)
	{
		if (pool is null) throw new ArgumentNullException(nameof(pool));
		if (rows.Length > MaxFromRows) throw TooManyRows(rows.Length);
		Contract.EndContractBlock();

		return Create(
			rows,
			new(pool, clearOnReturn, ChunkSizes.Default, RecycleBin.Default, ChunkConfig<T>.DefaultDeferFromBytes));
	}

	/// <summary>The most rows <c>From</c> takes: its one array is a first chunk, which a buffer holds as a power of two.</summary>
	const int MaxFromRows = 1 << 30;

	static ArgumentOutOfRangeException TooManyRows(int rows)
		=> new(nameof(rows), rows, "A buffer holds at most 2^30 rows from a span.");

	/// <summary>Builds the buffer for <c>From</c>; the entry has checked the arguments and chosen the configuration.</summary>
	static PooledBuffer<T> Create(ReadOnlySpan<T> rows, ChunkConfig<T> config)
	{
		System.Diagnostics.Debug.Assert(rows.Length <= MaxFromRows);

		if (rows.IsEmpty) return new([], 0, 1);

		// One array that is one first chunk: row i is at i, so the indexer always lands in chunk 0.
		int firstRows = 1;
		while (firstRows < rows.Length) firstRows <<= 1;

		T[] array = config.Rent(firstRows);
		rows.CopyTo(array);
		return new([config.Seal(array, rows.Length)], rows.Length, firstRows);
	}

	/// <summary>
	/// Gets a task that completes with the rows, once the read is done.
	/// </summary>
	/// <value>
	/// A task whose result is the <see cref="PooledArray{T}"/> over the rows.
	/// </value>
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
	/// A read that found no rows completes with <see cref="PooledArray{T}.Empty"/>, which holds nothing and
	/// keeps working after the buffer is disposed.
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
	/// Does not wait: while the read is running the producer returns the
	/// arrays to the pool when it exits, and nothing is thrown.
	/// For a pool whose <c>Return</c> throws, see the remarks of the class.
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
	/// Calling it again does nothing: it only observes the producer's task,
	/// so it throws again only if that task faulted.
	/// After it, <see cref="Items"/> throws
	/// <see cref="ObjectDisposedException"/>.
	/// </remarks>
	/// <returns>
	/// A task that completes when the producer has stopped and the arrays
	/// are returned (a chunk of 128 KB or more of rows goes to the
	/// cleanup queue).
	/// </returns>
	public async ValueTask DisposeAsync()
		=> await DisposeCore().ConfigureAwait(false);

	/// <summary>Throws if the buffer is disposed.</summary>
	/// <remarks>The check is load, compare, branch: the throw is out of line.</remarks>
	/// <exception cref="ObjectDisposedException">
	/// The buffer is disposed.
	/// </exception>
	internal void ThrowIfDisposed()
	{
		if (Volatile.Read(ref _disposed) != 0) ThrowDisposed();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static void ThrowDisposed()
		=> throw new ObjectDisposedException(nameof(PooledBuffer<>));

	/// <summary>Disposes the buffer, once.</summary>
	/// <remarks>
	/// <para>
	/// <em>Why dispose is ordered the way it is.</em>
	/// </para>
	/// <list type="number">
	/// <item>
	/// <description>
	/// Set <c>_disposed</c>. From then on Publish refuses and
	/// <see cref="Items"/> throws.
	/// </description>
	/// </item>
	/// <item>
	/// <description>
	/// Cancel <see cref="Items"/> if it is still pending.
	/// </description>
	/// </item>
	/// <item>
	/// <description>
	/// If the read is still going, cancel the producer: its linked token
	/// cancels the query itself. The producer then ends, swallows that cancel
	/// (the owner asked for it: <see cref="IChunkTarget{T}.IsDisposed"/>), and
	/// Finish releases the arrays. If the read already finished, dispose
	/// releases them.
	/// </description>
	/// </item>
	/// </list>
	/// <para>
	/// <see cref="DisposeAsync"/> additionally awaits the producer, so its
	/// arrays are back when it returns; <see cref="Dispose"/> does not wait.
	/// The <see langword="await"/> has no timeout, on purpose: correctness (no array goes back
	/// while the producer might still write) comes before promptness.
	/// So a dispose waits for the provider call in progress (the execute, or a
	/// read) to return; a provider that ignores the cancel token delays it by
	/// that call.
	/// After that the producer stops at the next chunk boundary at the latest:
	/// the read loop checks the token per row in the default read mode, the
	/// provider does in the async read mode, and the producer itself checks
	/// once per chunk.
	/// So with <c>AsyncReads()</c> and a provider that ignores the token, a
	/// dispose or a cancel is seen at the next chunk (a buffer's chunks double:
	/// up to the rows of the current chunk).
	/// A caller's own cancel is the opposite: it is <em>not</em> swallowed.
	/// Finish stores it, and <see cref="Items"/> is cancelled with the
	/// caller's token.
	/// </para>
	/// </remarks>
	/// <returns>The producer's task, which completes when it has exited.</returns>
	Task DisposeCore()
	{
		// The dispose-once guard (a full fence: see the remarks of this method).
		if (Interlocked.Exchange(ref _disposed, 1) != 0)
			return _producerTask;

		Volatile.Read(ref _items)?.TrySetCanceled();

		// A running producer releases the arrays itself, in Finish.
		// A finished one has nothing left to cancel or to write.
		if (Volatile.Read(ref _done) == 0) _producer!.Cancel();
		else ReleaseAll();

		return _producerTask;
	}

	/// <summary>Releases every chunk, once.</summary>
	/// <remarks>
	/// The list is taken with an exchange, so a second call does nothing.
	/// <include file="Internals.docs.xml" path="docs/members[@name='release-all']/*" />
	/// </remarks>
	void ReleaseAll()
	{
		List<ArrayLease<T>>? chunks
			= Interlocked.Exchange(ref _chunks, null);
		if (chunks is null) return;

		Exception? first = null;
		foreach (ArrayLease<T> chunk in chunks)
		{
			try { chunk.Release(); }
			catch (Exception ex) { first ??= ex; }
		}

		if (first is not null)
			System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(first).Throw();
	}

	/// <summary>Completes the <see cref="Items"/> source from the finished read.</summary>
	/// <remarks>
	/// Idempotent (<c>TrySet</c>), so Finish and the first accessor may both
	/// call it.
	/// </remarks>
	/// <param name="items">The source to complete.</param>
	void Complete(TaskCompletionSource<PooledArray<T>> items)
	{
		if (items.Task.IsCompleted) return;

		List<ArrayLease<T>>? chunks = Volatile.Read(ref _chunks);
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
		else if (_total == 0)
		{
			// No rows: the shared empty view, so no view is built. The buffer still
			// owns whatever it holds, and releases it when it is disposed.
			items.TrySetResult(PooledArray<T>.Empty);
		}
		else
		{
			// The view takes a snapshot of the leases: nothing is asked of
			// the arrays, so building it cannot fail.
			items.TrySetResult(new(this, [.. chunks], _total, _firstRows));
		}
	}

	/// <inheritdoc/>
	bool IChunkTarget<T>.IsDisposed
		=> Volatile.Read(ref _disposed) != 0;

	/// <inheritdoc/>
	int IChunkTarget<T>.CapacityOf(int index)
		=> index < 31 - ChunkSizing.Log2(_firstRows)
			? _firstRows << index
			: throw new InvalidOperationException(
				"A buffer cannot hold more rows than an int counts.");

	/// <inheritdoc/>
	/// <remarks>Called on the producer thread only, before Finish.</remarks>
	bool IChunkTarget<T>.Publish(ArrayLease<T> chunk)
	{
		if (Volatile.Read(ref _disposed) != 0) return false;

		_chunks!.Add(chunk);
		_total += chunk.Used;
		return true;
	}

	/// <inheritdoc/>
	void IChunkTarget<T>.Finish(Exception? error)
	{
		_error = error;
		Interlocked.Exchange(ref _done, 1);

		// Done is set, then the field is read (see the remarks of the fields).
		TaskCompletionSource<PooledArray<T>>? items
			= Volatile.Read(ref _items);
		if (items is not null) Complete(items);

		// Disposed while the producer ran: it is the last that can
		// write, so the arrays go back now.
		if (Volatile.Read(ref _disposed) != 0) ReleaseAll();
	}
}
