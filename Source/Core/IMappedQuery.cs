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
	IAsyncEnumerable<ReadOnlyMemory<T>> ToPooledBufferStream(
		CancellationToken cancellationToken = default);

	/// <summary>
	/// Reads the rows one at a time off the open reader, lazily, handing each to your loop as it is read (one row at a time, with a row or two read ahead).
	/// </summary>
	/// <remarks>
	/// While you pull rows, this holds the reader and the connection open, and each row is handed from the read to your loop one at a time.
	/// That is not the best way to read rows: to read all of them use <c>ToArrayAsync</c> or <c>ToListAsync</c>; to read while the query runs use <c>StreamBuffered</c> or <c>ToPooledStream</c>.
	/// To read the rows as a list or an array, use <see cref="MappedQuery{T}.ToListAsync(CancellationToken)"/>
	/// or <see cref="MappedQuery{T}.ToArrayAsync(CancellationToken)"/>: they release the connection before the task completes.
	/// To read while the query runs, use <see cref="ToPooledBufferStream(CancellationToken)"/>, which releases it when the read ends.
	/// If you go <paramref name="stallTimeout"/> without pulling the next row, the read stops, the command is cancelled,
	/// the connection this library opened is closed, and your next pull throws <see cref="TimeoutException"/>.
	/// Dispose the enumeration (<c>await foreach</c> does).
	/// <include file="Mapped.docs.xml" path="docs/members[@name='stream-models']/*" />
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

	/// <summary>
	/// Reads the rows lazily through a bounded channel that buffers up to 1,024 rows ahead of your loop, stopping the read when you stop pulling once the buffer is full.
	/// </summary>
	/// <remarks>
	/// The buffer is a ceiling, not a target, and it counts rows, not bytes: wide rows (blobs) cost memory, for each stream running at once.
	/// A consumer that keeps up leaves it nearly empty; a larger buffer lets the read ride out a slow consumer instead of waiting on every row.
	/// A result that fits is read whole, the later result sets included, and the connection released before you have pulled every row.
	/// Breaking out early never throws for rows or result sets you did not reach.
	/// If the buffer is full and you go <paramref name="stallTimeout"/> without pulling the next row, the read stops, the command is cancelled,
	/// the connection this library opened is closed, and your next pull throws <see cref="TimeoutException"/>.
	/// Rows still waiting in the buffer when the stall trips are discarded, not delivered.
	/// Dispose the enumeration (<c>await foreach</c> does).
	/// <include file="Mapped.docs.xml" path="docs/members[@name='stream-models']/*" />
	/// </remarks>
	/// <param name="stallTimeout">
	/// The longest you may go without pulling the next row once the buffer is full. Required; there is no default.
	/// <see cref="Timeout.InfiniteTimeSpan"/> (or a span over 24 days) accepts an unbounded hold.
	/// </param>
	/// <param name="cancellationToken">Cancels the read; the enumeration then throws <see cref="OperationCanceledException"/>.</param>
	/// <returns>The rows. The enumeration can be taken once.</returns>
	IAsyncEnumerable<T> StreamBuffered(
		TimeSpan stallTimeout,
		CancellationToken cancellationToken = default);

	/// <summary>
	/// Reads the rows lazily through a bounded channel that buffers up to <paramref name="bufferSize"/> rows (it counts rows, not bytes); a size of 0 reads one row at a time, as <c>Stream</c>.
	/// </summary>
	/// <param name="stallTimeout">The longest you may go without pulling the next row once the buffer is full. Required; there is no default.</param>
	/// <param name="bufferSize">The most rows the read runs ahead of your loop; 0 reads one row at a time, as <c>Stream</c>, and a negative size throws.</param>
	/// <param name="cancellationToken">Cancels the read; the enumeration then throws <see cref="OperationCanceledException"/>.</param>
	/// <returns>The rows. The enumeration can be taken once.</returns>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="bufferSize"/> is negative.</exception>
	IAsyncEnumerable<T> StreamBuffered(
		TimeSpan stallTimeout,
		int bufferSize,
		CancellationToken cancellationToken = default);
}
