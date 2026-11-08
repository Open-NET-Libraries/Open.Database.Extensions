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
	/// Executes the query and passes the mapped rows to <paramref name="handler"/> as they are read.
	/// </summary>
	/// <param name="handler">Receives a forward-only sequence of mapped rows. Enumerate it inside the handler; it is not valid after the handler returns.</param>
	/// <remarks>
	/// The connection is opened if it is not already open and is released when the handler returns, whether the handler completes or throws. Exceptions from the provider propagate unchanged.
	/// </remarks>
	void Read(Action<IEnumerable<T>> handler);

	/// <summary>
	/// Executes the query, passes the mapped rows to <paramref name="handler"/> as they are read, and returns the handler's result.
	/// </summary>
	/// <typeparam name="TResult">The type returned by the handler.</typeparam>
	/// <param name="handler">Receives a forward-only sequence of mapped rows. Enumerate it inside the handler; it is not valid after the handler returns.</param>
	/// <returns>The value returned by <paramref name="handler"/>. Materialize it before returning (for example into a list) rather than returning the lazy sequence.</returns>
	/// <remarks>
	/// The connection is opened if it is not already open and is released when the handler returns, whether the handler completes or throws. Exceptions from the provider propagate unchanged.
	/// </remarks>
	TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler);

	/// <summary>
	/// Asynchronously executes the query and passes the mapped rows to <paramref name="handler"/> as they are read.
	/// </summary>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows and the cancellation token in effect. Enumerate it inside the handler; the sequence and the token are not valid after the handler returns.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that completes after the handler has completed and the connection has been released.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// The connection is opened if it is not already open and is released when the handler returns, whether the handler completes or throws. Exceptions from the provider propagate unchanged.
	/// </remarks>
	ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken);

	/// <summary>
	/// Asynchronously executes the query, passes the mapped rows to <paramref name="handler"/> as they are read, and returns the handler's result.
	/// </summary>
	/// <typeparam name="TResult">The type returned by the handler.</typeparam>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows and the cancellation token in effect. Enumerate it inside the handler; the sequence and the token are not valid after the handler returns.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces the value returned by <paramref name="handler"/> after the connection has been released.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// The connection is opened if it is not already open and is released when the handler returns, whether the handler completes or throws. Exceptions from the provider propagate unchanged.
	/// </remarks>
	ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken);
}
