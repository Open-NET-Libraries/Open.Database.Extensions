namespace Open.Database.Extensions;

/// <summary>
/// Provides extension methods that execute a <see cref="SqlCommand"/> and read its results, and that add typed parameters to it.
/// </summary>
public static partial class SqlCommandExtensions
{
	#region Open and execute the reader

	/// <summary>
	/// Asynchronously opens the command's connection if it is closed and executes the reader, in one step.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <param name="behavior">The behavior to run the reader with. <see cref="CommandBehavior.CloseConnection"/> is added when the connection was opened here.</param>
	/// <param name="cancellationToken">A token that can cancel opening the connection and executing the command.</param>
	/// <returns>A task whose result is the reader.</returns>
	/// <remarks>
	/// Core's shared helpers close a connection that was opened here when the provider fails to execute.
	/// The execute delegates are static lambdas, so there is no closure.
	/// </remarks>
	static ValueTask<SqlDataReader> OpenAndExecuteReaderAsync(this SqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
		=> CommandExtensions.OpenAndExecuteReaderAsync(command, behavior, static (c, b, ct) => c.ExecuteReaderAsync(b, ct), cancellationToken);
	#endregion

	/// <inheritdoc cref="ExecuteReaderAsync(SqlCommand, Func{SqlDataReader, ValueTask}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask ExecuteReaderAsync(this SqlCommand command,
		Func<SqlDataReader, ValueTask> handler,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync(command, handler, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to the handler.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-command']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-handler']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-behavior']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-token']/*" />
	/// <returns>A task that represents the asynchronous operation. It completes after <paramref name="handler"/> has completed and the reader has been disposed.</returns>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="handler"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled before or while the connection was being opened or the command executed.</exception>
	public static async ValueTask ExecuteReaderAsync(this SqlCommand command,
		Func<SqlDataReader, ValueTask> handler,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

#if NET472
		using SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		await handler(reader).ConfigureAwait(false);
	}

	/// <inheritdoc cref="ExecuteReaderAsync(SqlCommand, Action{SqlDataReader}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask ExecuteReaderAsync(this SqlCommand command,
		Action<SqlDataReader> handler,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync(command, handler, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to a synchronous handler.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <returns>A task that represents the asynchronous operation. It completes after <paramref name="handler"/> has completed and the reader has been disposed.</returns>
	/// <inheritdoc cref="ExecuteReaderAsync(SqlCommand, Func{SqlDataReader, ValueTask}, CommandBehavior, CancellationToken)" path="/*[not(self::summary) and not(self::returns)]"/>
	public static async ValueTask ExecuteReaderAsync(this SqlCommand command,
		Action<SqlDataReader> handler,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

#if NET472
		using SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		handler(reader);
	}

	/// <inheritdoc cref="ExecuteReaderAsync{T}(SqlCommand, Func{SqlDataReader, T}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask<T> ExecuteReaderAsync<T>(this SqlCommand command,
		Func<SqlDataReader, T> transform,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync<T>(command, transform, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to the transform function, returning its result.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-command']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-transform']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-behavior']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-token']/*" />
	/// <returns>A task that represents the asynchronous operation; its result is the value returned by <paramref name="transform"/>.</returns>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="transform"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled before or while the connection was being opened or the command executed.</exception>
	public static async ValueTask<T> ExecuteReaderAsync<T>(this SqlCommand command,
		Func<SqlDataReader, T> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

#if NET472
		using SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		return transform(reader);
	}

	/// <inheritdoc cref="ExecuteReaderAsync{T}(SqlCommand, Func{SqlDataReader, ValueTask{T}}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask<T> ExecuteReaderAsync<T>(this SqlCommand command,
		Func<SqlDataReader, ValueTask<T>> transform,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync<T>(command, transform, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to an asynchronous transform function, returning its result.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <returns>A task that represents the asynchronous operation; its result is the value produced by <paramref name="transform"/>.</returns>
	/// <inheritdoc cref="ExecuteReaderAsync{T}(SqlCommand, Func{SqlDataReader, T}, CommandBehavior, CancellationToken)" path="/*[not(self::summary) and not(self::returns)]"/>
	public static async ValueTask<T> ExecuteReaderAsync<T>(this SqlCommand command,
		Func<SqlDataReader, ValueTask<T>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

#if NET472
		using SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		SqlDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		return await transform(reader).ConfigureAwait(false);
	}
}
