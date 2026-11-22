namespace Open.Database.Extensions;

public static partial class ChannelDbExtensions
{
	#region Channel

	/// <summary>
	/// Reads the query one row at a time and writes each row to a channel
	/// you own, leaving the channel open.
	/// </summary>
	/// <remarks>
	/// Pulls <see cref="IMappedQuery{T}.Stream(TimeSpan, CancellationToken)"/>:
	/// while the channel is full, the reader and the connection stay open.
	/// See that member for when to use it and what <paramref name="stallTimeout"/> does.
	/// Otherwise it behaves as
	/// <see cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)"/>.
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="channel">The channel to write to.</param>
	/// <param name="stallTimeout">
	/// The longest the channel may make no room while the reader is held. Required; passed to <c>Stream</c>.
	/// The wait for room before the command runs is not timed.
	/// </param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	/// <returns>The number of rows written to the channel.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="channel"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="stallTimeout"/> is zero or negative and not <see cref="Timeout.InfiniteTimeSpan"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	/// <exception cref="TimeoutException">The channel made no room for <paramref name="stallTimeout"/>.</exception>
	public static ValueTask<long> ToChannelHoldingConnection<T>(
		this IMappedQuery<T> source,
		Channel<T> channel,
		TimeSpan stallTimeout,
		CancellationToken cancellationToken = default)
		=> ToChannelHoldingConnection(
			source,
			channel,
			stallTimeout,
			ChannelEnd.KeepOpen,
			cancellationToken);

	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/exception"/>
	/// <param name="end">What happens to the channel when the pass ends.</param>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="end"/> is not a defined value, or <paramref name="stallTimeout"/> is zero or negative and not <see cref="Timeout.InfiniteTimeSpan"/>.</exception>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="channel">The channel to write to.</param>
	/// <param name="stallTimeout">The longest the channel may make no room while the reader is held. Required; passed to <c>Stream</c>.
	/// The wait for room before the command runs is not timed.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	public static ValueTask<long> ToChannelHoldingConnection<T>(
		this IMappedQuery<T> source,
		Channel<T> channel,
		TimeSpan stallTimeout,
		ChannelEnd end,
		CancellationToken cancellationToken = default)
	{
		if (channel is null) throw new ArgumentNullException(nameof(channel));
		Contract.EndContractBlock();

		return ToChannelHoldingConnection(
			source,
			channel.Writer,
			stallTimeout,
			end,
			cancellationToken);
	}

	#endregion

	#region ChannelWriter

	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, CancellationToken)" path="/exception"/>
	/// <param name="writer">The writer to write to.</param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="stallTimeout">The longest the writer may make no room while the reader is held. Required; passed to <c>Stream</c>.
	/// The wait for room before the command runs is not timed.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	public static ValueTask<long> ToChannelHoldingConnection<T>(
		this IMappedQuery<T> source,
		ChannelWriter<T> writer,
		TimeSpan stallTimeout,
		CancellationToken cancellationToken = default)
		=> ToChannelHoldingConnection(
			source,
			writer,
			stallTimeout,
			ChannelEnd.KeepOpen,
			cancellationToken);

	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, ChannelEnd, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, ChannelEnd, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, ChannelEnd, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, ChannelEnd, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannelHoldingConnection{T}(IMappedQuery{T}, Channel{T}, TimeSpan, ChannelEnd, CancellationToken)" path="/exception"/>
	/// <param name="writer">The writer to write to.</param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="stallTimeout">The longest the writer may make no room while the reader is held. Required; passed to <c>Stream</c>.
	/// The wait for room before the command runs is not timed.</param>
	/// <param name="end">What happens to the writer when the pass ends.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	public static ValueTask<long> ToChannelHoldingConnection<T>(
		this IMappedQuery<T> source,
		ChannelWriter<T> writer,
		TimeSpan stallTimeout,
		ChannelEnd end,
		CancellationToken cancellationToken = default)
	{
		// Not async: the guards throw at the call, before the target is
		// touched. Stream checks the timeout and refuses a default struct.
		if (source is null) throw new ArgumentNullException(nameof(source));
		if (writer is null) throw new ArgumentNullException(nameof(writer));
		CheckEnd(end);
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();
		return ChannelPump.Run<T, RowFeed<T>>(
			new(source.Stream(stallTimeout, cancellationToken)),
			writer,
			end,
			true,
			cancellationToken);
	}

	#endregion
}
