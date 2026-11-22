namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <summary>
	/// Returns <see langword="true"/> if a connection found in this state has to be opened by us, so whoever opens it
	/// closes it again afterwards.
	/// </summary>
	/// <param name="state">The state the connection was found in.</param>
	/// <returns>
	/// <see langword="true"/> if the connection is closed, still connecting, or broken;
	/// otherwise, <see langword="false"/>.
	/// </returns>
	/// <remarks>
	/// <see cref="ConnectionState"/> is a <see cref="FlagsAttribute">[Flags]</see>
	/// enum: <see cref="ConnectionState.Closed"/> = 0, <see cref="ConnectionState.Open"/> = 1,
	/// <see cref="ConnectionState.Connecting"/> = 2, <see cref="ConnectionState.Executing"/> = 4,
	/// <see cref="ConnectionState.Fetching"/> = 8, <see cref="ConnectionState.Broken"/> = 16.
	/// An open connection that is busy is <c>Open | Executing</c> or <c>Open | Fetching</c>,
	/// which is why the <see cref="ConnectionState.Open"/> <em>bit</em> is tested and the state is never compared for equality.
	/// <see cref="ConnectionState.Closed"/> is zero, so it has no bit: it is found as "the Open bit is not set".
	/// </remarks>
	internal static bool NeedsOpen(ConnectionState state)
		// Open bit not set: the connection is closed (or still connecting), so it has to be opened.
		=> (state & ConnectionState.Open) == 0
		// Broken bit set: the connection was open and broke; ADO.NET says it may be closed and re-opened,
		// so it is closed first, then opened.
		|| IsBroken(state);

	/// <summary>
	/// Returns <see langword="true"/> if the <see cref="ConnectionState.Broken"/> bit of the state is set.
	/// </summary>
	/// <param name="state">The state of the connection.</param>
	/// <returns>
	/// <see langword="true"/> if the connection is broken; otherwise, <see langword="false"/>.
	/// </returns>
	/// <remarks>
	/// Kept in one place so that the check that closes a broken connection first
	/// and the one that decides it needs opening cannot disagree.
	/// </remarks>
	static bool IsBroken(ConnectionState state)
		=> (state & ConnectionState.Broken) != 0;

	/// <summary>
	/// Opens the connection if it is not open (a broken connection is closed first)
	/// and returns the prior state, so you can close it afterwards if it was not open.
	/// </summary>
	/// <param name="connection">The connection to open.</param>
	/// <returns>The state the connection was in before this call.</returns>
	/// <remarks>
	/// A connection that is already open is left as it is, whatever it is doing.
	/// Nothing here closes it afterwards; use
	/// <see cref="EnsureOpenScope{TConnection}(TConnection)"/> for a scope that does.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
	public static ConnectionState EnsureOpen(this IDbConnection connection)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		Contract.EndContractBlock();

		ConnectionState state = connection.State;

		// Already open: nothing to do, and nobody should close it.
		if (!NeedsOpen(state)) return state;

		// Broken: it is closed first, then opened.
		if (IsBroken(state))
			connection.Close();

		connection.Open();
		return state;
	}

	/// <summary>
	/// Asynchronously opens the connection if it is not open (a broken connection is closed first)
	/// and returns the prior state, so you can close it afterwards if it was not open.
	/// </summary>
	/// <param name="connection">The connection to open.</param>
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation; its result is the
	/// state the connection was in before this call.
	/// </returns>
	/// <remarks>
	/// Always awaits with <c>ConfigureAwait(false)</c>: the caller decides whether to return to its context.
	/// If the token is canceled after the connection has been opened, the connection is closed again
	/// before the exception is thrown.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	public static async ValueTask<ConnectionState> EnsureOpenAsync(
		this DbConnection connection,
		CancellationToken cancellationToken = default)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();

		ConnectionState state = connection.State;

		// Already open: nothing to do, and nobody should close it.
		if (!NeedsOpen(state)) return state;

		// Broken: it is closed first, then opened.
		if (IsBroken(state))
			connection.Close();

		await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

		// ConnectionState.Closed is zero (it has no bit), so compare the state itself.
		if (cancellationToken.IsCancellationRequested && connection.State != ConnectionState.Closed)
		{
			connection.Close(); // Fake finally...
			cancellationToken.ThrowIfCancellationRequested();
		}

		return state;
	}

	/// <summary>
	/// Opens the connection if it is not open (a broken connection is closed first)
	/// and returns the prior state, asynchronously when the connection is a <see cref="DbConnection"/>.
	/// </summary>
	/// <remarks>
	/// A connection that is not a <see cref="DbConnection"/> is opened synchronously by
	/// <see cref="EnsureOpen(IDbConnection)"/>, after the token has been checked.
	/// </remarks>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	public static async ValueTask<ConnectionState> EnsureOpenAsync(
		this IDbConnection connection,
		CancellationToken cancellationToken = default)
	{
		if (connection is DbConnection c)
			return await c.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);

		cancellationToken.ThrowIfCancellationRequested();
		return connection.EnsureOpen();
	}

	/// <summary>
	/// Returns the behavior to execute a reader with: <paramref name="behavior"/>, plus
	/// <see cref="CommandBehavior.CloseConnection"/> when a connection found in <paramref name="priorState"/> had to be opened.
	/// </summary>
	/// <param name="priorState">The state the connection was found in.</param>
	/// <param name="behavior">The behavior to start from.</param>
	/// <returns>The behavior to pass to the command.</returns>
	/// <remarks>
	/// A reader of a connection that needed opening closes the connection when the reader does,
	/// also for the callers that hold no scope.
	/// </remarks>
	internal static CommandBehavior ReaderBehavior(ConnectionState priorState, CommandBehavior behavior)
		=> NeedsOpen(priorState) ? behavior | CommandBehavior.CloseConnection : behavior;

	/// <summary>
	/// Opens the connection if it is not open (a broken connection is closed first) and
	/// returns a <see cref="ConnectionScope{TConnection}"/> that puts it back when disposed: a
	/// connection this call opened is closed again, and one that was already open is left open.
	/// </summary>
	/// <remarks>
	/// The same rule as <see cref="EnsureOpen(IDbConnection)"/>, which returns the prior state
	/// for a caller that wants to do the closing itself; the scope does it, without a
	/// <see langword="try"/>/<see langword="finally"/>. It is safe to call on a connection that is already open.
	/// You already hold the connection, so the scope is usually only there to be disposed:
	/// <c>using var _ = connection.EnsureOpenScope();</c>. Prefer
	/// <see cref="EnsureOpenScopeAsync{TConnection}(TConnection, CancellationToken)"/>.
	/// </remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <param name="connection">The connection to open.</param>
	/// <returns>The connection and the state it was found in; <see cref="ConnectionScope{TConnection}.Dispose"/> closes it again if the scope opened it.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
	public static ConnectionScope<TConnection> EnsureOpenScope<TConnection>(this TConnection connection)
		where TConnection : IDbConnection
		=> new(connection, connection.EnsureOpen());

	/// <summary>
	/// Asynchronously opens the connection if it is not open (a broken connection is closed first) and
	/// returns a <see cref="ConnectionScope{TConnection}"/> that puts it back when disposed: a
	/// connection this call opened is closed again, and one that was already open is left open.
	/// </summary>
	/// <remarks>
	/// <code language="csharp">
	/// await using var _ = await connection.EnsureOpenScopeAsync(cancellationToken);
	/// </code>
	/// You already hold the connection, so the scope is usually only there to be disposed; keep it
	/// (<c>var scope = ...</c>, then <c>var (sql, was) = scope;</c>) when you also want the state the
	/// connection was found in. The same rule as <see cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>,
	/// which returns the prior state for a caller that wants to do the closing itself. It opens with
	/// <see cref="DbConnection.OpenAsync(CancellationToken)"/> when the connection is a
	/// <see cref="DbConnection"/>, and always awaits with <c>ConfigureAwait(false)</c>.
	/// The scope's <see cref="ConnectionScope{TConnection}.DisposeAsync"/> closes with <c>CloseAsync</c>
	/// where the target has it (not netstandard2.0, which closes synchronously). If the token
	/// is canceled the connection it opened is closed again and no scope is returned.
	/// </remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <param name="connection">The connection to open.</param>
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <returns>The connection and the state it was found in; <see cref="ConnectionScope{TConnection}.DisposeAsync"/> closes it again if the scope opened it.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	public static async ValueTask<ConnectionScope<TConnection>> EnsureOpenScopeAsync<TConnection>(
		this TConnection connection,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
		=> new(
			connection,
			await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false));

	/// <summary>
	/// Opens the connection by calling <see cref="DbConnection.OpenAsync(CancellationToken)"/> if it is a
	/// <see cref="DbConnection"/>, and <see cref="IDbConnection.Open()"/> otherwise.
	/// </summary>
	/// <param name="connection">The connection to open.</param>
	/// <param name="cancellationToken">A token that cancels opening a <see cref="DbConnection"/>. It is ignored for any other connection.</param>
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <remarks>
	/// Unlike <see cref="EnsureOpenAsync(IDbConnection, CancellationToken)"/>, this method does not check the
	/// state of the connection first, and does not return it.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>. It is thrown by the call, not through the returned task.</exception>
	public static ValueTask OpenAsync(
		this IDbConnection connection,
		CancellationToken cancellationToken = default)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		Contract.EndContractBlock();

		return OpenCoreAsync(connection, cancellationToken);

		static async ValueTask OpenCoreAsync(IDbConnection connection, CancellationToken cancellationToken)
		{
			if (connection is DbConnection c)
				await c.OpenAsync(cancellationToken).ConfigureAwait(false);
			else
				connection.Open();
		}
	}
}
