namespace Open.Database.Extensions;

/// <summary>
/// Provides extension methods that execute a <see cref="SqlCommand"/> and read its results, and that add typed parameters to it.
/// </summary>
public static partial class SqlCommandExtensions
{
	#region Open and execute the reader

	/// <summary>
	/// Opens the command's connection if it is closed and executes the reader, in one step.
	/// </summary>
	/// <remarks>
	/// Core's shared helpers close a connection that was opened here when the provider fails to execute.
	/// The execute delegates are static lambdas, so there is no closure.
	/// </remarks>
	/// <param name="command">The command to execute.</param>
	/// <param name="behavior">The behavior to run the reader with. <see cref="CommandBehavior.CloseConnection"/> is added when the connection was opened here.</param>
	/// <returns>The reader.</returns>
	static SqlDataReader OpenAndExecuteReader(this SqlCommand command, CommandBehavior behavior)
		=> CommandExtensions.OpenAndExecuteReader(command, behavior, static (c, b) => c.ExecuteReader(b));

	/// <summary>
	/// Asynchronously opens the command's connection if it is closed and executes the reader, in one step.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <param name="behavior">The behavior to run the reader with. <see cref="CommandBehavior.CloseConnection"/> is added when the connection was opened here.</param>
	/// <param name="cancellationToken">A token that can cancel opening the connection and executing the command.</param>
	/// <returns>A task whose result is the reader.</returns>
	/// <inheritdoc cref="OpenAndExecuteReader(SqlCommand, CommandBehavior)" path="/remarks"/>
	static ValueTask<SqlDataReader> OpenAndExecuteReaderAsync(this SqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
		=> CommandExtensions.OpenAndExecuteReaderAsync(command, behavior, static (c, b, ct) => c.ExecuteReaderAsync(b, ct), cancellationToken);
	#endregion

	/// <summary>
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the handler.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-command']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-handler']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-behavior']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
	public static void ExecuteReader(this SqlCommand command, Action<SqlDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		using SqlDataReader reader = command.OpenAndExecuteReader(behavior);
		handler(reader);
	}

	/// <summary>
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the transform function, returning its result.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-command']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-transform']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-behavior']/*" />
	/// <returns>The value returned by <paramref name="transform"/>.</returns>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='reader-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="transform"/> is <see langword="null"/>.</exception>
	public static T ExecuteReader<T>(this SqlCommand command, Func<SqlDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		using SqlDataReader reader = command.OpenAndExecuteReader(behavior);
		return transform(reader);
	}

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
		CommandBehavior behavior = CommandBehavior.Default,
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

	/// <summary>
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to a synchronous handler.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <returns>A task that represents the asynchronous operation. It completes after <paramref name="handler"/> has completed and the reader has been disposed.</returns>
	/// <inheritdoc cref="ExecuteReaderAsync(SqlCommand, Func{SqlDataReader, ValueTask}, CommandBehavior, CancellationToken)" path="/*[not(self::summary) and not(self::returns)]"/>
	public static async ValueTask ExecuteReaderAsync(this SqlCommand command,
		Action<SqlDataReader> handler,
		CommandBehavior behavior = CommandBehavior.Default,
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
		CommandBehavior behavior = CommandBehavior.Default,
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

	/// <summary>
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to an asynchronous transform function, returning its result.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <returns>A task that represents the asynchronous operation; its result is the value produced by <paramref name="transform"/>.</returns>
	/// <inheritdoc cref="ExecuteReaderAsync{T}(SqlCommand, Func{SqlDataReader, T}, CommandBehavior, CancellationToken)" path="/*[not(self::summary) and not(self::returns)]"/>
	public static async ValueTask<T> ExecuteReaderAsync<T>(this SqlCommand command,
		Func<SqlDataReader, ValueTask<T>> transform,
		CommandBehavior behavior = CommandBehavior.Default,
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
