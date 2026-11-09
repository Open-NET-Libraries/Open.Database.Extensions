namespace Open.Database.Extensions;

/// <summary>
/// SqlClient extensions for building a command and retrieving data using best practices.
/// </summary>
public static partial class SqlCommandExtensions
{
	#region Connection.EnsureOpen Shortcuts.
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ConnectionState EnsureOpen(this IDbCommand command)
	{
#if DEBUG
		if (command.Connection is null) throw new InvalidOperationException("Cannot execute a command with a null connection.");
#endif
		return command.Connection!.EnsureOpen();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ValueTask<ConnectionState> EnsureOpenAsync(this IDbCommand command, CancellationToken cancellationToken)
	{
#if DEBUG
		if (command.Connection is null) throw new ArgumentException("Cannot execute a command with a null connection.");
#endif
		return command.Connection!.EnsureOpenAsync(false, cancellationToken);
	}

	// These open a closed connection and execute the reader in one step. CommandBehavior.CloseConnection closes the connection when the
	// reader is disposed, but when the provider fails to execute there is no reader, so the connection that was opened here is closed here
	// (never disposed). One the caller had opened is left as it was. A failure to close is ignored so that it cannot replace the exception
	// being reported.
	static SqlDataReader OpenAndExecuteReader(this SqlCommand command, CommandBehavior behavior)
	{
		if (command.EnsureOpen() != ConnectionState.Closed)
			return command.ExecuteReader(behavior);

		try
		{
			return command.ExecuteReader(behavior | CommandBehavior.CloseConnection);
		}
		catch
		{
			CommandExtensions.CloseOpened(command);
			throw;
		}
	}

	/// <inheritdoc cref="OpenAndExecuteReader(SqlCommand, CommandBehavior)"/>
	static async ValueTask<SqlDataReader> OpenAndExecuteReaderAsync(this SqlCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
	{
		if (await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false) != ConnectionState.Closed)
			return await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

		try
		{
			return await command.ExecuteReaderAsync(behavior | CommandBehavior.CloseConnection, cancellationToken).ConfigureAwait(false);
		}
		catch
		{
			await CommandExtensions.CloseOpenedAsync(command).ConfigureAwait(false);
			throw;
		}
	}
	#endregion

	/// <summary>
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the handler. The reader is disposed afterwards.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="handler">The function that reads from the reader. Do not let the reader escape it.</param>
	/// <param name="behavior">The behavior to use with the data reader. <see cref="CommandBehavior.CloseConnection"/> is added when the command opens a closed connection.</param>
	/// <remarks>
	/// Exceptions from the provider (for example <see cref="SqlException"/>) propagate unchanged. The reader is disposed, and with it a connection that this method opened, even when the handler throws.
	/// </remarks>
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
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the transform function, returning its result. The reader is disposed afterwards.
	/// </summary>
	/// <typeparam name="T">The type returned by the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="transform">The function that reads from the reader and produces the result. Do not let the reader escape it.</param>
	/// <param name="behavior">The behavior to use with the data reader. <see cref="CommandBehavior.CloseConnection"/> is added when the command opens a closed connection.</param>
	/// <returns>The value returned by <paramref name="transform"/>.</returns>
	/// <remarks>
	/// Exceptions from the provider (for example <see cref="SqlException"/>) propagate unchanged. The reader is disposed, and with it a connection that this method opened, even when the transform function throws.
	/// </remarks>
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
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to the handler. The reader is disposed afterwards.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="handler">The function that reads from the reader. Do not let the reader escape it.</param>
	/// <param name="behavior">The behavior to use with the data reader. <see cref="CommandBehavior.CloseConnection"/> is added when the command opens a closed connection.</param>
	/// <param name="cancellationToken">A token that can cancel opening the connection and executing the command.</param>
	/// <returns>A task that completes after <paramref name="handler"/> has completed and the reader has been disposed.</returns>
	/// <remarks>
	/// Exceptions from the provider (for example <see cref="SqlException"/>) propagate unchanged. The reader is disposed, and with it a connection that this method opened, even when the handler throws.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="handler"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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

	/// <inheritdoc cref="ExecuteReaderAsync(SqlCommand, Func{SqlDataReader, ValueTask}, CommandBehavior, CancellationToken)"/>
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
	/// Asynchronously executes the command and passes the <see cref="SqlDataReader"/> to the transform function, returning its result. The reader is disposed afterwards.
	/// </summary>
	/// <typeparam name="T">The type returned by the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="transform">The function that reads from the reader and produces the result. Do not let the reader escape it.</param>
	/// <param name="behavior">The behavior to use with the data reader. <see cref="CommandBehavior.CloseConnection"/> is added when the command opens a closed connection.</param>
	/// <param name="cancellationToken">A token that can cancel opening the connection and executing the command.</param>
	/// <returns>A task that produces the value returned by <paramref name="transform"/>.</returns>
	/// <remarks>
	/// Exceptions from the provider (for example <see cref="SqlException"/>) propagate unchanged. The reader is disposed, and with it a connection that this method opened, even when the transform function throws.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="transform"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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

	/// <inheritdoc cref="ExecuteReaderAsync{T}(SqlCommand, Func{SqlDataReader, T}, CommandBehavior, CancellationToken)"/>
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
