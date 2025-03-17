using System.Data.Common;
using System.Runtime.CompilerServices;

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
		return command.Connection!.EnsureOpenAsync(cancellationToken);
	}
	#endregion

	/// <param name="command">The <see cref="SqlCommand"/> to generate a reader from.</param>
	/// <param name="handler">The handler function for the <see cref="SqlDataReader"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <inheritdoc cref="CommandExtensions.ExecuteReader(IDbCommand, Action{IDataReader}, CommandBehavior)"/>
	public static void ExecuteReader(this SqlCommand command, Action<SqlDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		ConnectionState state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using SqlDataReader reader = command.ExecuteReader(behavior);
		handler(reader);
	}

	/// <param name="command">The <see cref="SqlCommand"/> to generate a reader from.</param>
	/// <param name="handler">The handler function for the <see cref="SqlDataReader"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <inheritdoc cref="CommandExtensions.ExecuteReaderAsync(DbCommand, Action{DbDataReader}, CommandBehavior, CancellationToken)"/>
	public static async ValueTask ExecuteReaderAsync(this SqlCommand command,
		Func<SqlDataReader, ValueTask> handler,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		ConnectionState state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using SqlDataReader reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
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

		ConnectionState state = await command
			.EnsureOpenAsync(cancellationToken)
			.ConfigureAwait(false);

		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using SqlDataReader reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		handler(reader);
	}

	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="transform">The transform function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <inheritdoc cref="CommandExtensions.ExecuteReaderAsync{T}(DbCommand, Func{DbDataReader, ValueTask{T}}, CommandBehavior, CancellationToken)"/>/>
	public static async ValueTask<T> ExecuteReaderAsync<T>(this SqlCommand command,
		Func<SqlDataReader, T> transform,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		ConnectionState state = await command
			.EnsureOpenAsync(cancellationToken)
			.ConfigureAwait(false);

		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using SqlDataReader reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		return transform(reader);
	}
}
