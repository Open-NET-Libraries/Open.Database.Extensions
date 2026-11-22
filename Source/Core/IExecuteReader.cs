namespace Open.Database.Extensions;

/// <summary>
/// Represents an expressive command that runs a data reader and hands it to
/// a function of the caller.
/// </summary>
/// <seealso cref="IExecuteCommand"/>
public interface IExecuteReader
{
	/// <summary>
	/// Gets the cancellation token the command uses with the methods that support one.
	/// </summary>
	/// <value>
	/// The cancellation token of the command,
	/// or <see cref="CancellationToken.None"/> when none was set.
	/// </value>
	CancellationToken CancellationToken { get; }

	/// <summary>
	/// Asynchronously executes the command and passes the data reader to
	/// <paramref name="handler"/>.
	/// </summary>
	/// <inheritdoc cref="Synchronous.IExecuteReaderSync.ExecuteReader(Action{IDataReader}, CommandBehavior)" path="/param"/>
	/// <returns>A task that represents the asynchronous operation.</returns>
	ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Asynchronously executes the command, passes the data reader to
	/// <paramref name="transform"/> and returns its result.
	/// </summary>
	/// <typeparam name="T">The type the transform returns.</typeparam>
	/// <param name="transform">The function that processes the data reader and produces the result.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <returns>A task that produces the result of <paramref name="transform"/>.</returns>
	ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Asynchronously executes the command, passes the data reader and
	/// <paramref name="state"/> to <paramref name="transform"/>, and returns its
	/// result.
	/// Pass a <see langword="static"/> lambda and the state it needs to avoid
	/// allocating a closure per call.
	/// </summary>
	/// <inheritdoc cref="Synchronous.IExecuteReaderSync.ExecuteReader{TState, TResult}(TState, Func{IDataReader, TState, TResult}, CommandBehavior)" path="/*[not(self::summary) and not(self::returns)]"/>
	/// <returns>A task that produces the result of <paramref name="transform"/>.</returns>
	ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Asynchronously executes the command, passes the data reader and
	/// <paramref name="state"/> to <paramref name="transform"/>, and returns its
	/// result, with the cancellation token to use for the whole call.
	/// Pass a <see langword="static"/> lambda and the state it needs to avoid
	/// allocating a closure per call.
	/// </summary>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='reader-state-transform']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <param name="cancellationToken">
	/// The token used for this call <b>instead of</b> <see cref="CancellationToken"/>; the command's own token is not consulted.
	/// It is checked before anything starts and cancels opening the connection, looking up stored-procedure parameter names and executing the command.
	/// The caller has already combined every token it wants honored (including <see cref="CancellationToken"/>), and uses the same token for the reads that <paramref name="transform"/> makes.
	/// </param>
	/// <returns>A task that produces the result of <paramref name="transform"/>.</returns>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
	ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken);
}

/// <summary>
/// Represents an expressive command whose data reader is of type
/// <typeparamref name="TReader"/>.
/// </summary>
/// <typeparam name="TReader">The type of the data reader.</typeparam>
public interface IExecuteReader<out TReader> : IExecuteReader
	where TReader : IDataReader
{
	/// <inheritdoc cref="IExecuteReader.ExecuteReaderAsync(Func{IDataReader, ValueTask}, CommandBehavior)"/>
	ValueTask ExecuteReaderAsync(Func<TReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <param name="handler">The function that processes the data reader and produces the result.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <inheritdoc cref="IExecuteReader.ExecuteReaderAsync{T}(Func{IDataReader, ValueTask{T}}, CommandBehavior)" path="/*[not(self::param)]"/>
	ValueTask<T> ExecuteReaderAsync<T>(Func<TReader, ValueTask<T>> handler, CommandBehavior behavior = CommandBehavior.Default);
}

/// <summary>
/// Represents an expressive command that can read the rows of its data reader
/// asynchronously.
/// </summary>
public interface IExecuteReaderAsync : IExecuteReader
{
	/// <summary>
	/// Gets a value that indicates whether the rows are read with
	/// <see cref="DbDataReader.ReadAsync(CancellationToken)"/> instead of
	/// <see cref="IDataReader.Read"/>.
	/// </summary>
	/// <value>
	/// <see langword="true"/> if the rows are read asynchronously;
	/// otherwise, <see langword="false"/>.
	/// </value>
	bool UseAsyncRead { get; }
}

/// <summary>
/// Represents an expressive command whose data reader is of type
/// <typeparamref name="TReader"/> and that can read its rows asynchronously.
/// </summary>
/// <typeparam name="TReader">The type of the data reader.</typeparam>
public interface IExecuteReaderAsync<TReader> : IExecuteReaderAsync, IExecuteReader<TReader>
	where TReader : IDataReader;
