namespace Open.Database.Extensions;

/// <summary>
/// Rows from a mapped query into a Dataflow block.
/// </summary>
public static partial class DataflowExtensions
{
	#region Produce

	/// <summary>
	/// Reads the query into a new unbounded block and returns it.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The read starts at the call, on a background task.
	/// The query is read at full speed into pooled chunks and the
	/// connection is closed when the read ends, however slowly you
	/// receive from the block; the rows you have not received yet wait in memory.
	/// </para>
	/// <para>
	/// The block is always finished:
	/// completed after the last row, or faulted with the exception that
	/// ended the read, a cancel included.
	/// A faulted block drops the rows it holds that were not received yet.
	/// </para>
	/// <para>
	/// To control how the block runs (its task scheduler, a bound, its name),
	/// pass <see cref="DataflowBlockOptions"/>;
	/// to put the rows into a pipeline you built, use
	/// <see cref="ToTargetBlock{T}(IMappedQuery{T}, ITargetBlock{T}, CancellationToken)"/>.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	/// <returns>The new block.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(
		this IMappedQuery<T> source,
		CancellationToken cancellationToken = default)
		=> Produce(source, null, cancellationToken);

	/// <summary>
	/// Reads the query into a new block made from <paramref name="options"/> and returns it.
	/// </summary>
	/// <inheritdoc cref="AsSourceBlock{T}(IMappedQuery{T}, CancellationToken)" path="/remarks"/>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="options">
	/// The options of the new block: its task scheduler, its bound and the rest.
	/// A bound limits the block, not memory: the query is still read at full
	/// speed, and the rows the block has no room for wait in an internal queue.
	/// </param>
	/// <param name="cancellationToken">Cancels the read.</param>
	/// <returns>The new block.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(
		this IMappedQuery<T> source,
		DataflowBlockOptions options,
		CancellationToken cancellationToken = default)
	{
		if (options is null) throw new ArgumentNullException(nameof(options));
		Contract.EndContractBlock();

		return Produce(source, options, cancellationToken);
	}

	// The produce shape: the package makes the block, starts at the call,
	// and always finishes it.
	// Not async: the guards, and a default struct (which Core refuses in
	// ToAsyncBufferStream), throw at the call.
	static BufferBlock<T> Produce<T>(
		IMappedQuery<T> source,
		DataflowBlockOptions? options,
		CancellationToken cancellationToken)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();
		IAsyncEnumerable<ReadOnlyMemory<T>> chunks
			= source.ToAsyncBufferStream(cancellationToken);

		BufferBlock<T> block = options is null
			? new()
			: new(options);

		// A failure reaches the block through Fault: the task's own fault is
		// read so that it is never reported as unobserved.
		_ = BlockPump
			.Run<T, ChunkFeed<T>>(
				new(chunks),
				block,
				BlockEnd.Complete,
				false,
				cancellationToken)
			.AsTask()
			.ContinueWith(
				static task => task.Exception,
				CancellationToken.None,
				TaskContinuationOptions.OnlyOnFaulted
				| TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Default);

		return block;
	}

	#endregion

	#region Target

	/// <summary>
	/// Reads the query and sends each row to a block you own,
	/// leaving the block open.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The task is the done signal: it completes with the number of rows
	/// the block accepted after the last result set was read through.
	/// A failure or a cancel comes back through the task, and the block
	/// is left as it was.
	/// </para>
	/// <para>
	/// The block stays open, so several passes can share one.
	/// Pass <see cref="BlockEnd.Complete"/> to complete it when the pass ends.
	/// </para>
	/// <para>
	/// The query is read at full speed and the connection is closed when
	/// the read ends. Each row is sent with <c>SendAsync</c>, which waits
	/// while a bounded block is full, so no row is dropped; the rows wait in
	/// an internal queue meanwhile, so your block does not limit memory.
	/// A block has no way to ask for room: only a block that is already
	/// finished is known before the command runs.
	/// When the block declines a row for good (completed or faulted), the
	/// pass stops without an exception and returns the rows accepted so far.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="target">The block to send to.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	/// <returns>The number of rows the block accepted.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static ValueTask<long> ToTargetBlock<T>(
		this IMappedQuery<T> source,
		ITargetBlock<T> target,
		CancellationToken cancellationToken = default)
		=> ToTargetBlock(
			source,
			target,
			BlockEnd.KeepOpen,
			cancellationToken);

	/// <summary>
	/// Reads the query and sends each row to a block you own,
	/// then ends the block as <paramref name="end"/> says.
	/// </summary>
	/// <remarks>
	/// With <see cref="BlockEnd.KeepOpen"/> it is
	/// <see cref="ToTargetBlock{T}(IMappedQuery{T}, ITargetBlock{T}, CancellationToken)"/>.
	/// With <see cref="BlockEnd.Complete"/> the block is completed when the
	/// pass ends, or faulted with the exception that ended it, a cancel included.
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="target">The block to send to.</param>
	/// <param name="end">What happens to the block when the pass ends.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	/// <returns>The number of rows the block accepted.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="end"/> is not a defined value.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static ValueTask<long> ToTargetBlock<T>(
		this IMappedQuery<T> source,
		ITargetBlock<T> target,
		BlockEnd end,
		CancellationToken cancellationToken = default)
	{
		// Not async: every guard throws at the call, before the
		// target is touched and before a stream is made.
		if (source is null) throw new ArgumentNullException(nameof(source));
		if (target is null) throw new ArgumentNullException(nameof(target));
		CheckEnd(end);
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();
		return BlockPump.Run<T, ChunkFeed<T>>(
			new(source.ToAsyncBufferStream(cancellationToken)),
			target,
			end,
			true,
			cancellationToken);
	}

	#endregion

	#region Streaming

	/// <summary>
	/// Reads the query one row at a time and sends each row to a block
	/// you own, leaving the block open.
	/// </summary>
	/// <remarks>
	/// Pulls <see cref="IMappedQuery{T}.Stream(TimeSpan, CancellationToken)"/>:
	/// while the block is full, the reader and the connection stay open.
	/// See that member for when to use it and what <paramref name="stallTimeout"/> does.
	/// Otherwise it behaves as
	/// <see cref="ToTargetBlock{T}(IMappedQuery{T}, ITargetBlock{T}, CancellationToken)"/>.
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="target">The block to send to.</param>
	/// <param name="stallTimeout">
	/// The longest the block may take no row while the reader is held.
	/// Required; passed to <c>Stream</c>.
	/// </param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	/// <returns>The number of rows the block accepted.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="stallTimeout"/> is zero or negative and not <see cref="Timeout.InfiniteTimeSpan"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	/// <exception cref="TimeoutException">The block took no row for <paramref name="stallTimeout"/>.</exception>
	public static ValueTask<long> ToTargetBlockHoldingConnection<T>(
		this IMappedQuery<T> source,
		ITargetBlock<T> target,
		TimeSpan stallTimeout,
		CancellationToken cancellationToken = default)
		=> ToTargetBlockHoldingConnection(
			source,
			target,
			stallTimeout,
			BlockEnd.KeepOpen,
			cancellationToken);

	/// <summary>
	/// Reads the query one row at a time and sends each row to a block
	/// you own, then ends the block as <paramref name="end"/> says.
	/// </summary>
	/// <remarks>
	/// Pulls <see cref="IMappedQuery{T}.Stream(TimeSpan, CancellationToken)"/>:
	/// while the block is full, the reader and the connection stay open.
	/// See that member for when to use it and what <paramref name="stallTimeout"/> does.
	/// <see cref="BlockEnd"/> works as for
	/// <see cref="ToTargetBlock{T}(IMappedQuery{T}, ITargetBlock{T}, BlockEnd, CancellationToken)"/>.
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="target">The block to send to.</param>
	/// <param name="stallTimeout">
	/// The longest the block may take no row while the reader is held.
	/// Required; passed to <c>Stream</c>.
	/// </param>
	/// <param name="end">What happens to the block when the pass ends.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	/// <returns>The number of rows the block accepted.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="end"/> is not a defined value, or <paramref name="stallTimeout"/> is zero or negative and not <see cref="Timeout.InfiniteTimeSpan"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	/// <exception cref="TimeoutException">The block took no row for <paramref name="stallTimeout"/>.</exception>
	public static ValueTask<long> ToTargetBlockHoldingConnection<T>(
		this IMappedQuery<T> source,
		ITargetBlock<T> target,
		TimeSpan stallTimeout,
		BlockEnd end,
		CancellationToken cancellationToken = default)
	{
		// Not async: the guards throw at the call, before the target is
		// touched. Stream checks the timeout and refuses a default struct.
		if (source is null) throw new ArgumentNullException(nameof(source));
		if (target is null) throw new ArgumentNullException(nameof(target));
		CheckEnd(end);
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();
		return BlockPump.Run<T, RowFeed<T>>(
			new(source.Stream(stallTimeout, cancellationToken)),
			target,
			end,
			true,
			cancellationToken);
	}

	#endregion

	static void CheckEnd(BlockEnd end)
	{
		if (end is not (BlockEnd.KeepOpen or BlockEnd.Complete))
		{
			throw new ArgumentOutOfRangeException(
				nameof(end),
				end,
				"Not a defined value.");
		}
	}
}
