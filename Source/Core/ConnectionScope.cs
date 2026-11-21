namespace Open.Database.Extensions;

// How a scope lets go of its connection when it is disposed. Chosen by whatever made it.
enum ScopeEnd : byte
{
	// A connection the caller already holds: close it if the scope opened it.
	Close = 0,

	// A connection a factory created: dispose it.
	Dispose,

	// A connection a pool lent: give it back; the pool decides what that means.
	Give,
}

/// <summary>
/// A connection that was opened if it needed to be, the state it was found in, and what
/// to do with it when the scope is disposed.
/// </summary>
/// <remarks>
/// <para>
/// One type for every source of a connection: the scope carries how it ends. What
/// disposing does depends on what made it:
/// </para>
/// <list type="table">
/// <listheader>
/// <term>Made by</term>
/// <description>The connection came from; disposing the scope</description>
/// </listheader>
/// <item>
/// <term><see cref="ConnectionExtensions.EnsureOpenScope{TConnection}(TConnection)"/> /
/// <see cref="ConnectionExtensions.EnsureOpenScopeAsync{TConnection}(TConnection, CancellationToken)"/></term>
/// <description>You already hold it; closes it if the scope opened it, and leaves it open if it was already open.
/// Never disposes it.</description>
/// </item>
/// <item>
/// <term><see cref="ConnectionExtensions.OpenScope(IDbConnectionFactory)"/> /
/// <see cref="ConnectionExtensions.OpenScopeAsync(IDbConnectionFactory, CancellationToken)"/> (on a factory)</term>
/// <description>The factory created it, so it is yours; disposes it (<c>DisposeAsync</c> when the
/// connection has one), as <c>factory.Open(...)</c> does.</description>
/// </item>
/// <item>
/// <term><see cref="ConnectionExtensions.OpenScope(IDbConnectionPool)"/> /
/// <see cref="ConnectionExtensions.OpenScopeAsync(IDbConnectionPool, CancellationToken)"/> (on a pool)</term>
/// <description>The pool lent it; gives it back with <see cref="IDbConnectionPool.Give(IDbConnection)"/>, as
/// <c>pool.Open(...)</c> does. What giving back means (close it, keep it, dispose it) is the pool's.</description>
/// </item>
/// </list>
/// <para>
/// When you already hold the connection the scope is usually only there to be disposed:
/// </para>
/// <code>
/// await using var _ = await connection.EnsureOpenScopeAsync(cancellationToken);
/// // ... use the connection you already hold.
/// </code>
/// <para>
/// When the connection came from a factory or a pool the scope is how you get it:
/// </para>
/// <code>
/// await using var scope = await factory.OpenScopeAsync(cancellationToken);
/// var connection = scope.Connection;
/// </code>
/// <para>
/// The scope keeps the type of the connection (a <c>SqlConnection</c> stays a
/// <c>SqlConnection</c>), and deconstructs into the connection and the state it was found in:
/// <c>var (sql, was) = scope;</c>.
/// </para>
/// <para>
/// A connection that was broken is closed and opened again, so a scope that closes it closes it
/// again afterwards; the broken state is not restored. <see cref="DisposeAsync"/> calls
/// <c>CloseAsync</c> for a <see cref="DbConnection"/> where the target has it (not netstandard2.0,
/// where it closes synchronously).
/// </para>
/// <para>
/// Dispose the value you were given exactly once, and do not copy it: a readonly struct
/// cannot remember that it was disposed, so a second dispose of a copy disposes a factory's
/// connection again, gives a pool's connection back again, or closes a connection that someone
/// opened in between. Deconstruct a scope you hold, never the call itself:
/// <c>var (c, s) = connection.EnsureOpenScope();</c> drops the scope, and the connection it opened
/// stays open.
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
	readonly ScopeEnd _end;
	readonly IDbConnectionPool? _pool;

	// The caller's own connection: closed again if the scope opened it.
	internal ConnectionScope(TConnection connection, ConnectionState priorState)
	{
		_connection = connection;
		PriorState = priorState;
	}

	// A connection a factory created: disposed.
	internal ConnectionScope(
		TConnection connection,
		ConnectionState priorState,
		ScopeEnd end)
		: this(connection, priorState)
		=> _end = end;

	// A connection a pool lent: given back to that pool.
	internal ConnectionScope(
		TConnection connection,
		ConnectionState priorState,
		IDbConnectionPool pool)
		: this(connection, priorState)
	{
		_end = ScopeEnd.Give;
		_pool = pool;
	}

	/// <summary>The connection, open for as long as the scope has not been disposed.</summary>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> scope.</exception>
	public TConnection Connection
		=> _connection ?? throw new InvalidOperationException(
			"This scope was not created by an OpenScope or EnsureOpenScope call; a default value has no connection.");

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
	/// Lets go of the connection the way its source calls for: closes it if the scope opened it
	/// (a connection the caller held), disposes it (a factory's), or gives it back to the pool
	/// (a pool's). See the table in the remarks of the type.
	/// </summary>
	public void Dispose()
	{
		if (_connection is null) return;

		switch (_end)
		{
			case ScopeEnd.Dispose:
				_connection.Dispose();
				break;

			case ScopeEnd.Give:
				_pool!.Give(_connection);
				break;

			default:
				if (ConnectionExtensions.NeedsOpen(PriorState))
					_connection.Close();

				break;
		}
	}

	/// <summary>
	/// As <see cref="Dispose"/>, asynchronously where the connection can: <c>DisposeAsync</c> for a
	/// factory's connection that has one, and <c>CloseAsync</c> for a caller's
	/// <see cref="DbConnection"/> (on netstandard2.0, which has no <c>CloseAsync</c>, it calls
	/// <c>Close</c>). Giving a connection back to a pool is synchronous.
	/// </summary>
	public ValueTask DisposeAsync()
	{
		if (_connection is null) return default;

		switch (_end)
		{
			case ScopeEnd.Dispose:
				if (_connection is IAsyncDisposable disposable)
					return disposable.DisposeAsync();

				_connection.Dispose();
				return default;

			case ScopeEnd.Give:
				_pool!.Give(_connection);
				return default;

			default:
				if (!ConnectionExtensions.NeedsOpen(PriorState))
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
}
