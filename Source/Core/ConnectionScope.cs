namespace Open.Database.Extensions;

/// <summary>
/// A connection that was opened if it needed to be, and the state it was found in,
/// so that disposing the scope puts it back: a connection this call opened is closed
/// again, and one that was already open is left open.
/// </summary>
/// <remarks>
/// <para>
/// Made by <see cref="ConnectionExtensions.EnsureOpenScope{TConnection}(TConnection)"/> and
/// <see cref="ConnectionExtensions.EnsureOpenScopeAsync{TConnection}(TConnection, CancellationToken)"/>.
/// It is what <see cref="ConnectionExtensions.EnsureOpen(IDbConnection)"/> and its
/// async twin return the state for, without the <c>try</c>/<c>finally</c>. You already
/// hold the connection, so the scope is usually only there to be disposed:
/// </para>
/// <code>
/// await using var _ = await connection.EnsureOpenScopeAsync(cancellationToken);
/// // ... use the connection you already hold.
/// </code>
/// <para>
/// Keep the scope when you also want the state the connection was found in. The scope keeps
/// the type of the connection you called it on (a <c>SqlConnection</c> stays a
/// <c>SqlConnection</c>), and deconstructs:
/// </para>
/// <code>
/// await using var scope = await connection.EnsureOpenScopeAsync(cancellationToken);
/// var (sql, was) = scope;
/// </code>
/// <para>
/// Disposing closes the connection only when this call opened it: the connection was
/// closed, or broken (a broken connection is closed and opened again, so it is closed
/// again afterwards; the broken state is not restored). It never disposes the
/// connection: that is the caller's. <see cref="DisposeAsync"/> calls
/// <c>CloseAsync</c> for a <see cref="DbConnection"/> where the target has it
/// (not netstandard2.0, where it closes synchronously).
/// </para>
/// <para>
/// Dispose the value you were given exactly once, and do not copy it: a readonly struct
/// cannot remember that it was disposed, so a second dispose of a copy closes the
/// connection again if someone opened it in between. Deconstruct a scope you hold, never the
/// call itself: <c>var (c, s) = connection.EnsureOpenScope();</c> drops the scope, and the
/// connection it opened stays open.
/// </para>
/// <para>
/// A <see langword="default"/> scope holds nothing: disposing it does nothing, and
/// reading <see cref="Connection"/> (or deconstructing it) throws
/// <see cref="InvalidOperationException"/>.
/// </para>
/// </remarks>
/// <typeparam name="TConnection">The type of the connection, as the caller holds it.</typeparam>
public readonly struct ConnectionScope<TConnection> : IDisposable, IAsyncDisposable
	where TConnection : IDbConnection
{
	readonly TConnection? _connection;

	internal ConnectionScope(TConnection connection, ConnectionState priorState)
	{
		_connection = connection;
		PriorState = priorState;
	}

	/// <summary>The connection, open for as long as the scope has not been disposed.</summary>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> scope.</exception>
	public TConnection Connection
		=> _connection ?? throw new InvalidOperationException(
			"This scope was not created by EnsureOpenScope(); a default value has no connection.");

	/// <summary>
	/// The state the connection was in when the scope was made (not its state now):
	/// <see cref="ConnectionState.Closed"/>, or <see cref="ConnectionState.Open"/> (possibly
	/// with <see cref="ConnectionState.Executing"/> or <see cref="ConnectionState.Fetching"/>),
	/// or with <see cref="ConnectionState.Broken"/> set.
	/// </summary>
	public ConnectionState PriorState { get; }

	/// <summary>Splits the scope into the connection and the state it was found in.</summary>
	/// <param name="connection">The connection.</param>
	/// <param name="priorState">The state it was found in.</param>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> scope.</exception>
	public void Deconstruct(out TConnection connection, out ConnectionState priorState)
	{
		connection = Connection;
		priorState = PriorState;
	}

	/// <summary>
	/// Closes the connection if this call opened it; leaves it open if it was already open.
	/// Never disposes the connection.
	/// </summary>
	public void Dispose()
	{
		if (_connection is not null && ConnectionExtensions.NeedsOpen(PriorState))
			_connection.Close();
	}

	/// <summary>
	/// As <see cref="Dispose"/>, awaiting <c>CloseAsync</c> when the
	/// connection is a <see cref="DbConnection"/> (on netstandard2.0, which has no
	/// <c>CloseAsync</c>, it calls <c>Close</c>).
	/// </summary>
	public ValueTask DisposeAsync()
	{
		if (_connection is null || !ConnectionExtensions.NeedsOpen(PriorState))
			return default;

#if NETSTANDARD2_0
		// No CloseAsync here: the same fallback as the command extensions.
		_connection.Close();
		return default;
#else
		if (_connection is DbConnection connection)
			return new ValueTask(connection.CloseAsync());

		_connection.Close();
		return default;
#endif
	}
}
