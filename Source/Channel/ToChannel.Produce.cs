namespace Open.Database.Extensions;

/// <summary>
/// Rows from a mapped query into a channel.
/// </summary>
public static partial class ChannelDbExtensions
{
	#region ToChannelReader

	/// <summary>
	/// Reads the query into a new unbounded channel and returns its reader.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The read starts at the call, on a background task.
	/// The query is read at full speed into pooled chunks and the
	/// connection is closed when the read ends, however slowly you read
	/// the channel; the rows you have not read wait in memory.
	/// </para>
	/// <para>
	/// The channel is always completed:
	/// without an error after the last row, or with the exception that
	/// ended the read, a cancel included.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	/// <returns>The reader of the new channel.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static ChannelReader<T> ToChannelReader<T>(
		this IMappedQuery<T> source,
		CancellationToken cancellationToken = default)
		=> ToChannelReader(source, false, cancellationToken);

	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/exception"/>
	/// <param name="singleReader">
	/// <see langword="true"/> if at most one consumer reads the channel at a time.
	/// </param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	public static ChannelReader<T> ToChannelReader<T>(
		this IMappedQuery<T> source,
		bool singleReader,
		CancellationToken cancellationToken = default)
		=> Produce(source, singleReader, 0, cancellationToken, out _)
			.Reader;

	#endregion

	#region ToChannel

	/// <summary>
	/// Reads the query into a new unbounded channel and returns the channel.
	/// </summary>
	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/remarks"/>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	/// <returns>The new channel.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static Channel<T> ToChannel<T>(
		this IMappedQuery<T> source,
		CancellationToken cancellationToken = default)
		=> ToChannel(source, false, cancellationToken);

	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/exception"/>
	/// <param name="singleReader">
	/// <see langword="true"/> if at most one consumer reads the channel at a time.
	/// </param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	public static Channel<T> ToChannel<T>(
		this IMappedQuery<T> source,
		bool singleReader,
		CancellationToken cancellationToken = default)
		=> Produce(source, singleReader, 0, cancellationToken, out _);

	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, CancellationToken)" path="/exception"/>
	/// <param name="readCount">
	/// The task that completes with the number of rows written to the
	/// channel when the read ends.
	/// It faults with the exception that ended the read.
	/// </param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	public static Channel<T> ToChannel<T>(
		this IMappedQuery<T> source,
		out Task<long> readCount,
		CancellationToken cancellationToken = default)
		=> Produce(source, false, 0, cancellationToken, out readCount);

	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, out Task{long}, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, out Task{long}, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, out Task{long}, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, out Task{long}, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, out Task{long}, CancellationToken)" path="/exception"/>
	/// <param name="singleReader">
	/// <see langword="true"/> if at most one consumer reads the channel at a time.
	/// </param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="readCount">
	/// The task that completes with the number of rows written to the
	/// channel when the read ends.
	/// It faults with the exception that ended the read.
	/// </param>
	/// <param name="cancellationToken">Cancels the read.</param>
	public static Channel<T> ToChannel<T>(
		this IMappedQuery<T> source,
		out Task<long> readCount,
		bool singleReader,
		CancellationToken cancellationToken = default)
		=> Produce(
			source,
			singleReader,
			0,
			cancellationToken,
			out readCount);

	#endregion

	#region Bounded

	/// <summary>
	/// Reads the query into a new bounded channel and returns its reader.
	/// </summary>
	/// <remarks>
	/// The bound limits the channel, not memory:
	/// the query is still read at full speed, and the rows the channel has
	/// no room for wait in an internal queue.
	/// </remarks>
	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/typeparam"/>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="capacity">The most rows the channel holds.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	/// <returns>The reader of the new channel.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is less than one.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static ChannelReader<T> ToBoundedChannelReader<T>(
		this IMappedQuery<T> source,
		int capacity,
		CancellationToken cancellationToken = default)
		=> ToBoundedChannel(source, capacity, cancellationToken).Reader;

	/// <summary>
	/// Reads the query into a new bounded channel and returns the channel.
	/// </summary>
	/// <inheritdoc cref="ToBoundedChannelReader{T}(IMappedQuery{T}, int, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannelReader{T}(IMappedQuery{T}, CancellationToken)" path="/typeparam"/>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="capacity">The most rows the channel holds.</param>
	/// <param name="cancellationToken">Cancels the read.</param>
	/// <returns>The new channel.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is less than one.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static Channel<T> ToBoundedChannel<T>(
		this IMappedQuery<T> source,
		int capacity,
		CancellationToken cancellationToken = default)
	{
		if (capacity < 1)
		{
			throw new ArgumentOutOfRangeException(
				nameof(capacity),
				capacity,
				"Must be at least one.");
		}

		return Produce(source, false, capacity, cancellationToken, out _);
	}

	#endregion

	// The produce shape: the package makes the channel (single writer,
	// no synchronous continuations: a consumer's code never runs inside
	// the write loop), starts at the call, and always completes it.
	// Not async: the guards, and a default struct (which Core refuses in
	// ToAsyncBufferStream), throw at the call.
	static Channel<T> Produce<T>(
		IMappedQuery<T> source,
		bool singleReader,
		int capacity,
		CancellationToken cancellationToken,
		out Task<long> readCount)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();
		IAsyncEnumerable<ReadOnlyMemory<T>> chunks
			= source.ToAsyncBufferStream(cancellationToken);

		Channel<T> channel = capacity > 0
			? Channel.CreateBounded<T>(
				new BoundedChannelOptions(capacity)
				{
					SingleWriter = true,
					SingleReader = singleReader,
					FullMode = BoundedChannelFullMode.Wait,
					AllowSynchronousContinuations = false,
				})
			: Channel.CreateUnbounded<T>(
				new UnboundedChannelOptions
				{
					SingleWriter = true,
					SingleReader = singleReader,
					AllowSynchronousContinuations = false,
				});

		// The channel is empty: there is room, so the first wait is skipped.
		// A failure reaches the channel through Complete; the task
		// is dropped unobserved unless the caller asked for the count.
		readCount = ChannelPump
			.Run<T, ChunkFeed<T>>(
				new(chunks),
				channel.Writer,
				ChannelEnd.Complete,
				false,
				cancellationToken)
			.AsTask();

		return channel;
	}
}
