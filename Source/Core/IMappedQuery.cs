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
	/// <returns>The chunks, in order. A chunk is valid until the enumeration moves on, so copy what you keep. The enumeration can be taken once.</returns>
	IAsyncEnumerable<ReadOnlyMemory<T>> ToAsyncBufferStream(
		CancellationToken cancellationToken = default);

	/// <summary>
	/// Reads the rows one at a time off the open reader, lazily, for a caller that expects to stop early.
	/// </summary>
	/// <remarks>
	/// While you pull rows, this holds the reader and the connection open.
	/// To read the rows as a list or an array, use <see cref="MappedQuery{T}.ToListAsync(CancellationToken)"/>
	/// or <see cref="MappedQuery{T}.ToArrayAsync(CancellationToken)"/>: they release the connection before the task completes.
	/// To read while the query runs, use <see cref="ToAsyncBufferStream(CancellationToken)"/>, which releases it when the read ends.
	/// Use <c>Stream</c> only to stop early, for example after the first few rows.
	/// If you go <paramref name="stallTimeout"/> without pulling the next row, the read stops, the command is cancelled,
	/// the connection this library opened is closed, and your next pull throws <see cref="TimeoutException"/>.
	/// Dispose the enumeration (<c>await foreach</c> does).
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
