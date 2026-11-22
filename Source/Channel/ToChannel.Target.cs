namespace Open.Database.Extensions;

public static partial class ChannelDbExtensions
{
	#region Channel

	/// <summary>
	/// Reads the query and writes each row to a channel you own,
	/// leaving the channel open.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The task is the done signal: it completes with the number of rows
	/// handed to the channel after the last result set was read through.
	/// A failure or a cancel comes back through the task, and the channel
	/// is left as it was.
	/// </para>
	/// <para>
	/// The channel stays open, so several passes can share one.
	/// Pass <see cref="ChannelEnd.Complete"/> to complete it when the pass ends.
	/// </para>
	/// <para>
	/// The query is read at full speed and the connection is closed when the
	/// read ends. When the channel is full, the write waits for room,
	/// before the command runs and then between rows, while the rows wait in
	/// an internal queue: the connection is free, but your channel does not
	/// limit memory.
	/// When the channel is closed, the pass stops without an exception and
	/// returns the rows written so far.
	/// A channel completed with an error throws that error.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type of each row.</typeparam>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="channel">The channel to write to.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	/// <returns>The number of rows written to the channel.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="channel"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static ValueTask<long> ToChannel<T>(
		this IMappedQuery<T> source,
		Channel<T> channel,
		CancellationToken cancellationToken = default)
		=> ToChannel(
			source,
			channel,
			ChannelEnd.KeepOpen,
			cancellationToken);

	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/exception"/>
	/// <param name="end">What happens to the channel when the pass ends.</param>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="end"/> is not a defined value.</exception>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="channel">The channel to write to.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	public static ValueTask<long> ToChannel<T>(
		this IMappedQuery<T> source,
		Channel<T> channel,
		ChannelEnd end,
		CancellationToken cancellationToken = default)
	{
		if (channel is null) throw new ArgumentNullException(nameof(channel));
		Contract.EndContractBlock();

		return ToChannel(source, channel.Writer, end, cancellationToken);
	}

	#endregion

	#region ChannelWriter

	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, CancellationToken)" path="/exception"/>
	/// <param name="writer">The writer to write to.</param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	public static ValueTask<long> ToChannel<T>(
		this IMappedQuery<T> source,
		ChannelWriter<T> writer,
		CancellationToken cancellationToken = default)
		=> ToChannel(
			source,
			writer,
			ChannelEnd.KeepOpen,
			cancellationToken);

	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, ChannelEnd, CancellationToken)" path="/summary"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, ChannelEnd, CancellationToken)" path="/remarks"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, ChannelEnd, CancellationToken)" path="/typeparam"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, ChannelEnd, CancellationToken)" path="/returns"/>
	/// <inheritdoc cref="ToChannel{T}(IMappedQuery{T}, Channel{T}, ChannelEnd, CancellationToken)" path="/exception"/>
	/// <param name="writer">The writer to write to.</param>
	/// <param name="source">The mapped query to read.</param>
	/// <param name="end">What happens to the writer when the pass ends.</param>
	/// <param name="cancellationToken">Cancels the pass.</param>
	public static ValueTask<long> ToChannel<T>(
		this IMappedQuery<T> source,
		ChannelWriter<T> writer,
		ChannelEnd end,
		CancellationToken cancellationToken = default)
	{
		// Not async: every guard throws at the call, before the
		// target is touched and before a stream is made.
		if (source is null) throw new ArgumentNullException(nameof(source));
		if (writer is null) throw new ArgumentNullException(nameof(writer));
		CheckEnd(end);
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();
		return ChannelPump.Run<T, ChunkFeed<T>>(
			new(source.ToAsyncBufferStream(cancellationToken)),
			writer,
			end,
			true,
			cancellationToken);
	}

	#endregion

	static void CheckEnd(ChannelEnd end)
	{
		if (end is not (ChannelEnd.KeepOpen or ChannelEnd.Complete))
		{
			throw new ArgumentOutOfRangeException(
				nameof(end),
				end,
				"Not a defined value.");
		}
	}
}
