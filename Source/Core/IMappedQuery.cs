namespace Open.Database.Extensions;

/// <summary>
/// A deferred query whose rows are mapped to <typeparamref name="T"/>. Nothing runs until one of the terminal methods is called.
/// </summary>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
/// <remarks>
/// <para>
/// The <c>Read</c> and <c>ReadAsync</c> methods are the primitive terminals: they execute the command, hand the handler a forward-only stream of mapped rows backed by the open data reader, and release the connection when the handler returns. The convenience terminals in <see cref="MappedQueryExtensions"/> (for example <see cref="MappedQueryExtensions.ToList{T}(IMappedQuery{T})"/> and <see cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>) are built on them.
/// </para>
/// <para>
/// Rows are read as the handler enumerates, so enumerate inside the handler and do not let the sequence escape it: the reader is disposed as soon as the handler returns. Each terminal call executes the command again.
/// </para>
/// <para>
/// The asynchronous terminals take a <see cref="CancellationToken"/>. A call is canceled when either that token or the cancellation token of the underlying command is canceled, and one combined token covers opening the connection, looking up stored-procedure parameter names, executing the command and reading the rows. Cancellation surfaces as <see cref="OperationCanceledException"/>.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedQueryExtensions"/>
public interface IMappedQuery<T>
{
	/// <summary>
	/// Executes the query and passes the mapped rows to
	/// <paramref name="handler"/> as they are read;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <param name="handler">Receives a forward-only sequence of mapped rows. Enumerate it inside the handler; it is not valid after the handler returns.</param>
	/// <remarks>
	/// <note type="caution">
	/// <b>This is not the recommended way to read rows: it holds the
	/// connection while your code runs for each row.</b>
	/// The reader, and the connection under it, stay open for as long as
	/// your code takes per row, added up over all the rows.
	/// <br/>
	/// <br/>
	/// In async code that time has no bound: every <c>await</c> in the
	/// loop body can yield, and nothing says when the loop resumes.
	/// Until it does, the connection is on hold: a pooled connection is
	/// not returned to the pool, and the server keeps its locks and the
	/// open result set.
	/// <br/>
	/// <br/>
	/// It is here for the cases that need exactly that, such as a result
	/// too large to buffer or a row-by-row pipeline.
	/// Otherwise use a call that lets go of the connection first:
	/// <see cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// and
	/// <see cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// buffer the rows and release the connection, and
	/// <see cref="MappedQuery{T}.ToChannelReader(CancellationToken)"/>
	/// streams them to a consumer without holding it
	/// (it is unbounded by default: the read runs at full speed and
	/// frees the connection while the consumer takes its time, and the
	/// rows wait in memory until the consumer takes them).
	/// A bounded channel that waits when full brings the hold back,
	/// because the read then waits for room.
	/// </note>
	/// <para>
	/// The connection is opened if it is not already open and is released when the handler returns, whether the handler completes or throws. Exceptions from the provider propagate unchanged.
	/// </para>
	/// </remarks>
	void Read(Action<IEnumerable<T>> handler);

	/// <summary>
	/// Executes the query, passes the mapped rows to
	/// <paramref name="handler"/> as they are read,
	/// and returns the handler's result;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <typeparam name="TResult">The type returned by the handler.</typeparam>
	/// <param name="handler">Receives a forward-only sequence of mapped rows. Enumerate it inside the handler; it is not valid after the handler returns.</param>
	/// <returns>The value returned by <paramref name="handler"/>. Materialize it before returning (for example into a list) rather than returning the lazy sequence.</returns>
	/// <inheritdoc cref="Read(Action{IEnumerable{T}})" path="/remarks"/>
	TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler);

	/// <summary>
	/// Asynchronously executes the query and passes the mapped rows to
	/// <paramref name="handler"/> as they are read;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows and the cancellation token in effect. Enumerate it inside the handler; the sequence and the token are not valid after the handler returns.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that completes after the handler has completed and the connection has been released.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <inheritdoc cref="Read(Action{IEnumerable{T}})" path="/remarks"/>
	ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken);

	/// <summary>
	/// Asynchronously executes the query, passes the mapped rows to
	/// <paramref name="handler"/> as they are read,
	/// and returns the handler's result;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <typeparam name="TResult">The type returned by the handler.</typeparam>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows and the cancellation token in effect. Enumerate it inside the handler; the sequence and the token are not valid after the handler returns.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces the value returned by <paramref name="handler"/> after the connection has been released.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <inheritdoc cref="Read(Action{IEnumerable{T}})" path="/remarks"/>
	ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken);
}
