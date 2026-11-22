namespace Open.Database.Extensions;

/// <summary>
/// A query that reads its rows into a channel or a block feed, or one at a time.
/// </summary>
/// <remarks>
/// <see cref="MappedQuery{T}"/>, <see cref="MappedQueryWithSkip{T}"/>, <see cref="MappedQueryWithTake{T}"/> and <see cref="MappedCommand{T}"/> implement it.
/// The Channel and Dataflow packages take it as their source.
/// </remarks>
/// <typeparam name="T">The type of a mapped row.</typeparam>
public interface IMappedQuery<T>
{
	/// <summary>
	/// Reads the rows into pooled chunks, one chunk at a time, and frees the connection before the consumer sees a row.
	/// </summary>
	/// <param name="cancellationToken">Cancels the read.</param>
	/// <returns>The chunks, in order. The enumeration can be taken once.</returns>
	IAsyncEnumerable<ReadOnlyMemory<T>> ToAsyncBufferStream(
		CancellationToken cancellationToken = default);

	/// <summary>
	/// Reads the rows one at a time off the open reader, lazily, for a caller that expects to stop early.
	/// </summary>
	/// <remarks>
	/// While you pull rows, this holds the reader and the connection open.
	/// Buffered reads are preferred and very good: they free the connection before you see a row.
	/// Use <c>Stream</c> only to stop early.
	/// If you go <paramref name="stallTimeout"/> without pulling the next row, the read stops, the command is cancelled,
	/// the connection this library opened is closed, and your next pull throws <see cref="TimeoutException"/>.
	/// </remarks>
	/// <param name="stallTimeout">
	/// The longest you may go without pulling the next row. Required; there is no default.
	/// <see cref="Timeout.InfiniteTimeSpan"/> (or a span over 24 days) accepts an unbounded hold.
	/// </param>
	/// <param name="cancellationToken">Cancels the read; the enumeration then throws <see cref="OperationCanceledException"/>.</param>
	/// <returns>The rows. The enumeration can be taken once.</returns>
	IAsyncEnumerable<T> Stream(
		TimeSpan stallTimeout,
		CancellationToken cancellationToken = default);
}
