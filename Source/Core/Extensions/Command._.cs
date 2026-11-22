
namespace Open.Database.Extensions;

/// <summary>
/// Provides extension methods that execute a command and read its results.
/// </summary>
public static partial class CommandExtensions
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
		return command.Connection!.EnsureOpenAsync(cancellationToken);
	}

	/// <summary>
	/// Gets the scope of the command's own connection:
	/// it is opened if needed, and closed again if the scope opened it.
	/// </summary>
	/// <remarks>
	/// A command with no connection gets the empty scope,
	/// and the command reports the missing connection itself when it is executed, as it always has.
	/// </remarks>
	/// <param name="command">The command whose connection to scope.</param>
	/// <returns>The scope of the connection.</returns>
	internal static ConnectionScope<IDbConnection> EnsureOpenScope(this IDbCommand command)
		=> command.Connection is { } connection
			? connection.EnsureOpenScope()
			: default;

	internal static async ValueTask<ConnectionScope<IDbConnection>> EnsureOpenScopeAsync(
		this IDbCommand command,
		CancellationToken cancellationToken)
		=> command.Connection is { } connection
			? await connection.EnsureOpenScopeAsync(cancellationToken).ConfigureAwait(false)
			: default;

	/// <summary>
	/// Opens the command's connection if it is closed and executes the reader in one step.
	/// </summary>
	/// <remarks>
	/// <para>
	/// <see cref="CommandBehavior.CloseConnection"/> closes the connection when the reader is disposed.
	/// When the provider fails to execute there is no reader,
	/// so the connection that was opened here is closed here (never disposed).
	/// A connection the caller had opened is left as it was.
	/// A failure to close is ignored so that it cannot replace the exception being reported.
	/// </para>
	/// <para>
	/// The two generic helpers are shared by the other packages
	/// (SqlClient passes <c>SqlCommand</c> and <c>SqlDataReader</c> and keeps its exact reader type).
	/// The execute delegate is passed in rather than a virtual call
	/// so that it can be a static lambda: no closure, no cast and no continuation.
	/// </para>
	/// </remarks>
	/// <typeparam name="TCommand">The type of the command.</typeparam>
	/// <typeparam name="TReader">The type of the reader.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="behavior">The behavior to run the reader with.</param>
	/// <param name="execute">The function that executes the reader.</param>
	/// <returns>The reader.</returns>
	// The execute delegate is passed in rather than a virtual call so that it can be a static lambda: no closure, no cast and no continuation.
	internal static TReader OpenAndExecuteReader<TCommand, TReader>(TCommand command, CommandBehavior behavior, Func<TCommand, CommandBehavior, TReader> execute)
		where TCommand : IDbCommand
	{
		if (!ConnectionExtensions.NeedsOpen(command.EnsureOpen()))
			return execute(command, behavior);

		try
		{
			return execute(command, behavior | CommandBehavior.CloseConnection);
		}
		catch
		{
			CloseOpened(command);
			throw;
		}
	}

	/// <inheritdoc cref="OpenAndExecuteReader{TCommand, TReader}(TCommand, CommandBehavior, Func{TCommand, CommandBehavior, TReader})"/>
	internal static async ValueTask<TReader> OpenAndExecuteReaderAsync<TCommand, TReader>(TCommand command, CommandBehavior behavior, Func<TCommand, CommandBehavior, CancellationToken, Task<TReader>> execute, CancellationToken cancellationToken)
		where TCommand : IDbCommand
	{
		ConnectionState prior = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (!ConnectionExtensions.NeedsOpen(prior))
			return await execute(command, behavior, cancellationToken).ConfigureAwait(false);

		try
		{
			return await execute(command, behavior | CommandBehavior.CloseConnection, cancellationToken).ConfigureAwait(false);
		}
		catch
		{
			await CloseOpenedAsync(command).ConfigureAwait(false);
			throw;
		}
	}

	internal static IDataReader OpenAndExecuteReader(this IDbCommand command, CommandBehavior behavior)
		=> OpenAndExecuteReader(command, behavior, static (c, b) => c.ExecuteReader(b));

	static ValueTask<DbDataReader> OpenAndExecuteReaderAsync(this DbCommand command, CommandBehavior behavior, CancellationToken cancellationToken)
		=> OpenAndExecuteReaderAsync(command, behavior, static (c, b, ct) => c.ExecuteReaderAsync(b, ct), cancellationToken);

	/// <summary>
	/// Closes, never disposes, the command's connection.
	/// A failure to close is ignored, so that it cannot replace the exception being reported.
	/// </summary>
	/// <param name="command">The command whose connection to close.</param>
	internal static void CloseOpened(IDbCommand command)
	{
		try
		{
			command.Connection?.Close();
		}
		catch
		{
			// Intentionally ignored; see above.
		}
	}

#if NETSTANDARD2_0
	/// <inheritdoc cref="CloseOpened(IDbCommand)"/>
	internal static ValueTask CloseOpenedAsync(IDbCommand command)
	{
		CloseOpened(command);
		return default;
	}
#else
	/// <inheritdoc cref="CloseOpened(IDbCommand)"/>
	internal static async ValueTask CloseOpenedAsync(IDbCommand command)
	{
		try
		{
			if (command.Connection is DbConnection connection)
				await connection.CloseAsync().ConfigureAwait(false);
			else
				command.Connection?.Close();
		}
		catch
		{
			// Intentionally ignored; see above.
		}
	}
#endif
	#endregion

	/// <inheritdoc cref="ExecuteReaderAsync(IDbCommand, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask<IDataReader> ExecuteReaderAsync(this IDbCommand command,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync(command, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader, calling <see cref="DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
	/// if the <paramref name="command"/> is derived from <see cref="DbCommand"/>;
	/// otherwise it calls <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/>.
	/// </summary>
	/// <remarks>
	/// Unlike the other members, this one does not open the connection:
	/// it must already be open, and the caller owns and disposes the reader.
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is the reader.
	/// </returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command']/*" />
	public static async ValueTask<IDataReader> ExecuteReaderAsync(this IDbCommand command,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command is DbCommand c
			? await c.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false)
			: command.ExecuteReader(behavior);
	}

	/// <inheritdoc cref="ExecuteReaderAsync(DbCommand, Action{DbDataReader}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask ExecuteReaderAsync(this DbCommand command,
		Action<DbDataReader> handler,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync(command, handler, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader and passes it to a handler.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-handler']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static async ValueTask ExecuteReaderAsync(this DbCommand command,
		Action<DbDataReader> handler,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

#if NETSTANDARD2_0
		using DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		handler(reader);
	}

	/// <inheritdoc cref="ExecuteReaderAsync(IDbCommand, Func{IDataReader, ValueTask}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask ExecuteReaderAsync(this IDbCommand command,
		Func<IDataReader, ValueTask> handler,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync(command, handler, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader and passes it to an asynchronous handler.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-handler']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static async ValueTask ExecuteReaderAsync(this IDbCommand command,
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		if (command is DbCommand c)
		{
			await c.ExecuteReaderAsync(reader => handler(reader), behavior, cancellationToken).ConfigureAwait(false);
			return;
		}

		using IDataReader reader = command.OpenAndExecuteReader(behavior);
		await handler(reader).ConfigureAwait(false);
	}

	/// <inheritdoc cref="ExecuteReaderAsync(DbCommand, Func{DbDataReader, ValueTask}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask ExecuteReaderAsync(this DbCommand command,
		Func<DbDataReader, ValueTask> handler,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync(command, handler, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader and passes it to an asynchronous handler.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-handler']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static async ValueTask ExecuteReaderAsync(this DbCommand command,
		Func<DbDataReader, ValueTask> handler,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

#if NETSTANDARD2_0
		using DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		await handler(reader).ConfigureAwait(false);
	}

	/// <inheritdoc cref="ExecuteReaderAsync{T}(DbCommand, Func{DbDataReader, T}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
		Func<DbDataReader, T> transform,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync<T>(command, transform, CommandBehavior.Default, cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader and returns the result of a transform function applied to it.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-transform']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is the result of the transform.
	/// </returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
		Func<DbDataReader, T> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

#if NETSTANDARD2_0
		using DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		return transform(reader);
	}

	/// <inheritdoc cref="ExecuteReaderAsync{T}(IDbCommand, Func{IDataReader, ValueTask{T}}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask<T> ExecuteReaderAsync<T>(this IDbCommand command,
		Func<IDataReader, ValueTask<T>> transform,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync<T>(command, transform, CommandBehavior.Default, cancellationToken);

	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-transform']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <inheritdoc cref="ExecuteReaderAsync{T}(DbCommand, Func{DbDataReader, T}, CommandBehavior, CancellationToken)" path="/*[not(self::param)]"/>
	public static ValueTask<T> ExecuteReaderAsync<T>(this IDbCommand command,
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command is DbCommand c
			? ExecuteReaderAsync(c, reader => transform(reader), behavior, cancellationToken)
			: ExecuteReaderAsyncCore(command, transform, behavior);

		static async ValueTask<T> ExecuteReaderAsyncCore(IDbCommand command, Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior)
		{
			using IDataReader reader = command.OpenAndExecuteReader(behavior);
			return await transform(reader).ConfigureAwait(false);
		}
	}

	/// <inheritdoc cref="ExecuteReaderAsync{T}(DbCommand, Func{DbDataReader, ValueTask{T}}, CommandBehavior, CancellationToken)" path="/*[not(self::param[@name='behavior'])]"/>
	public static ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
		Func<DbDataReader, ValueTask<T>> transform,
		CancellationToken cancellationToken = default)
		=> ExecuteReaderAsync<T>(command, transform, CommandBehavior.Default, cancellationToken);

	/// <inheritdoc cref="ExecuteReaderAsync{T}(DbCommand, Func{DbDataReader, T}, CommandBehavior, CancellationToken)"/>
	public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
		Func<DbDataReader, ValueTask<T>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

#if NETSTANDARD2_0
		using DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
#else
		DbDataReader reader = await command.OpenAndExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		return await transform(reader).ConfigureAwait(false);
	}

	internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
	{
		return command is null
			? throw new ArgumentNullException(nameof(command))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: IterateReaderInternalCore(command, behavior, transform);

		static IEnumerable<T> IterateReaderInternalCore(IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			using IDataReader reader = command.OpenAndExecuteReader(behavior);
			while (reader.Read())
				yield return transform(reader);
		}
	}

}
