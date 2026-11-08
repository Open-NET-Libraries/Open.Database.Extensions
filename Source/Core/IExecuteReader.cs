namespace Open.Database.Extensions;

/// <summary>
/// Common interface used for expressive commands when dealing with a data reader.
/// </summary>
public interface IExecuteReader
{
	/// <summary>
	/// The cancellation token to use with supported methods.
	/// </summary>
	CancellationToken CancellationToken { get; }

	/// <summary>
	/// Executes a reader on a command with a handler function.
	/// </summary>
	/// <param name="handler">The handler function for the data reader.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default);
	/// <summary>
	/// Executes a reader on a command with a transform function.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	/// <returns>The result of the transform.</returns>
	T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <inheritdoc cref="ExecuteReader(Action{IDataReader}, CommandBehavior)"/>
	ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes a reader on a command with a handler function.
	/// </summary>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes a reader on a command, passing <paramref name="state"/> to <paramref name="transform"/>.
	/// Pass a <see langword="static"/> lambda and the state it needs to avoid allocating a closure per call.
	/// </summary>
	/// <typeparam name="TState">The type of the state passed to the transform.</typeparam>
	/// <typeparam name="TResult">The return type of the transform function.</typeparam>
	/// <param name="state">The state to pass to the transform.</param>
	/// <param name="transform">The transform function for the data reader.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	/// <returns>The result of the transform.</returns>
	TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes a reader on a command, passing <paramref name="state"/> to <paramref name="transform"/>.
	/// Pass a <see langword="static"/> lambda and the state it needs to avoid allocating a closure per call.
	/// </summary>
	/// <inheritdoc cref="ExecuteReader{TState, TResult}(TState, Func{IDataReader, TState, TResult}, CommandBehavior)"/>
	ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes a reader on a command, passing <paramref name="state"/> to <paramref name="transform"/>, with the cancellation token to use for the whole call.
	/// Pass a <see langword="static"/> lambda and the state it needs to avoid allocating a closure per call.
	/// </summary>
	/// <typeparam name="TState">The type of the state passed to the transform.</typeparam>
	/// <typeparam name="TResult">The return type of the transform function.</typeparam>
	/// <param name="state">The state to pass to the transform.</param>
	/// <param name="transform">The transform function for the data reader.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	/// <param name="cancellationToken">
	/// The token used for this call <b>instead of</b> <see cref="CancellationToken"/>; the command's own token is not consulted.
	/// It is checked before anything starts and cancels opening the connection, looking up stored-procedure parameter names and executing the command.
	/// The caller has already combined every token it wants honored (including <see cref="CancellationToken"/>), and uses the same token for the reads that <paramref name="transform"/> makes.
	/// </param>
	/// <returns>The result of the transform.</returns>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
	ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken);
}

/// <summary>
/// Common interface used for expressive commands when dealing with a data reader.
/// </summary>
/// <typeparam name="TReader">The type of the data reader.</typeparam>
public interface IExecuteReader<out TReader> : IExecuteReader
	where TReader : IDataReader
{
	/// <inheritdoc cref="IExecuteReader.ExecuteReader(Action{IDataReader}, CommandBehavior)"/>
	void ExecuteReader(Action<TReader> handler, CommandBehavior behavior = CommandBehavior.Default);
	/// <inheritdoc cref="IExecuteReader.ExecuteReader{T}(Func{IDataReader, T}, CommandBehavior)"/>
	T ExecuteReader<T>(Func<TReader, T> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <inheritdoc cref="IExecuteReader.ExecuteReader(Action{IDataReader}, CommandBehavior)"/>
	ValueTask ExecuteReaderAsync(Func<TReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes a reader on a command with a handler function.
	/// </summary>
	/// <param name="handler">The handler function for the data reader.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	ValueTask<T> ExecuteReaderAsync<T>(Func<TReader, ValueTask<T>> handler, CommandBehavior behavior = CommandBehavior.Default);
}

/// <inheritdoc />
public interface IExecuteReaderAsync : IExecuteReader
{
	/// <summary>
	/// Indicates if reader.ReadAsync() will be used in favor of reader.Read().
	/// </summary>
	bool UseAsyncRead { get; }
}

/// <inheritdoc />
public interface IExecuteReaderAsync<TReader> : IExecuteReaderAsync, IExecuteReader<TReader>
	where TReader : IDataReader;
