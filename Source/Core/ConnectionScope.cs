namespace Open.Database.Extensions;

/// <summary>
/// How a scope lets go of its connection when it is disposed. Chosen by whatever made the scope.
/// </summary>
enum ScopeEnd : byte
{
	/// <summary>
	/// A connection the caller already holds: close it if the scope opened it.
	/// </summary>
	Close = 0,

	/// <summary>
	/// A connection a factory created: dispose it.
	/// </summary>
	Dispose,

	/// <summary>
	/// A connection rented from a pool: give it back; the pool decides what that means.
	/// </summary>
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
/// <term><see cref="ConnectionExtensions.EnsureOpenScope(IDbConnectionFactory)"/> /
/// <see cref="ConnectionExtensions.EnsureOpenScopeAsync(IDbConnectionFactory, CancellationToken)"/> (on a factory)</term>
/// <description>The factory created it, so it is yours; disposes it: <c>Dispose</c>, as
/// <c>factory.Open(...)</c> does, and <c>DisposeAsync</c> from <see cref="DisposeAsync"/> when the connection has one.</description>
/// </item>
/// <item>
/// <term><see cref="ConnectionExtensions.EnsureOpenScope(IDbConnectionPool)"/> /
/// <see cref="ConnectionExtensions.EnsureOpenScopeAsync(IDbConnectionPool, CancellationToken)"/> (on a pool)</term>
/// <description>The pool leased it; gives it back with <see cref="IDbConnectionPool.Give(IDbConnection)"/>, as
/// <c>pool.Open(...)</c> does. What giving back means (close it, keep it, dispose it) is the pool's.</description>
/// </item>
/// </list>
/// <para>
/// When you already hold the connection the scope is usually only there to be disposed:
/// </para>
/// <code language="csharp">
/// await using var _ = await connection.EnsureOpenScopeAsync(cancellationToken);
/// // ... use the connection you already hold.
/// </code>
/// <para>
/// When the connection came from a factory or a pool the scope is how you get it:
/// </para>
/// <code language="csharp">
/// await using var scope = await factory.EnsureOpenScopeAsync(cancellationToken);
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

	/// <summary>
	/// Creates a scope for the caller's own connection: closed again if the scope opened it.
	/// </summary>
	/// <param name="connection">The connection the caller holds.</param>
	/// <param name="priorState">The state the connection was found in.</param>
	internal ConnectionScope(TConnection connection, ConnectionState priorState)
	{
		_connection = connection;
		PriorState = priorState;
	}

	/// <summary>
	/// Creates a scope that ends the way <paramref name="end"/> says.
	/// Used for a connection a factory created, which is disposed.
	/// </summary>
	/// <param name="connection">The connection.</param>
	/// <param name="priorState">The state the connection was found in.</param>
	/// <param name="end">How the scope lets go of the connection.</param>
	internal ConnectionScope(
		TConnection connection,
		ConnectionState priorState,
		ScopeEnd end)
		: this(connection, priorState)
		=> _end = end;

	/// <summary>
	/// Creates a scope for a connection rented from a pool: given back to that pool.
	/// </summary>
	/// <param name="connection">The connection.</param>
	/// <param name="priorState">The state the connection was found in.</param>
	/// <param name="pool">The pool the connection was taken from.</param>
	internal ConnectionScope(
		TConnection connection,
		ConnectionState priorState,
		IDbConnectionPool pool)
		: this(connection, priorState)
	{
		_end = ScopeEnd.Give;
		_pool = pool;
	}

	/// <summary>Gets the connection, open for as long as the scope has not been disposed.</summary>
	/// <value>The connection the scope holds.</value>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> scope.</exception>
	public TConnection Connection
		=> _connection ?? throw new InvalidOperationException(
			"This scope was not created by an EnsureOpenScope call; a default value has no connection.");

	/// <summary>
	/// Gets the state the connection was in when the scope was made (not its state now):
	/// for example <see cref="ConnectionState.Closed"/>, or <see cref="ConnectionState.Open"/> (possibly
	/// with <see cref="ConnectionState.Executing"/> or <see cref="ConnectionState.Fetching"/>),
	/// or with <see cref="ConnectionState.Broken"/> set.
	/// </summary>
	/// <value>The state the connection was in when the scope was made.</value>
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
	/// <returns>A task that represents the asynchronous dispose operation.</returns>
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

	/// <summary>
	/// The behavior to execute a reader with: <paramref name="behavior"/>, plus
	/// <see cref="CommandBehavior.CloseConnection"/> when this scope opened the connection.
	/// </summary>
	/// <remarks>
	/// A reader that outlives the scope still closes what the scope opened, because the connection
	/// closes when the reader does:
	/// <code language="csharp">
	/// var scope = await connection.EnsureOpenScopeAsync(cancellationToken).ConfigureAwait(false);
	/// await using var disposal = scope.ConfigureAwait(false);
	/// await using var reader = await command
	///     .ExecuteReaderAsync(scope.ReaderBehavior(), cancellationToken)
	///     .ConfigureAwait(false);
	/// </code>
	/// A connection that was already open is left open: nothing is added. A <see langword="default"/> scope
	/// opened nothing, so it adds nothing either.
	/// </remarks>
	/// <param name="behavior">The behavior to start from. The default is <see cref="CommandBehavior.Default"/>.</param>
	/// <returns>The behavior to pass to the command.</returns>
	public CommandBehavior ReaderBehavior(CommandBehavior behavior = CommandBehavior.Default)
		=> _connection is not null
			? ConnectionExtensions.ReaderBehavior(PriorState, behavior)
			: behavior;

	/// <summary>
	/// Configures how the disposal of this scope is awaited by <see langword="await"/> <see langword="using"/>, without boxing the scope.
	/// </summary>
	/// <remarks>
	/// Library code takes the scope first, then hands it to <see langword="await"/> <see langword="using"/> on its own line:
	/// <code language="csharp">
	/// var scope = await connection.EnsureOpenScopeAsync(cancellationToken).ConfigureAwait(false);
	/// await using var disposal = scope.ConfigureAwait(false);
	/// </code>
	/// The scope is a struct, so the <see cref="IAsyncDisposable"/> extension of the same name would box
	/// it; this instance method is chosen instead, and allocates nothing. It exists only for that:
	/// it holds no library setting, and the library never returns to a caller's context (it always passes
	/// <see langword="false"/>). Whether to return to a context is the caller's choice at their own call site,
	/// as with <see cref="Task.ConfigureAwait(bool)"/>.
	/// </remarks>
	/// <param name="continueOnCapturedContext">
	/// <see langword="true"/> to continue on the captured context after the connection has been
	/// closed, disposed or given back; otherwise <see langword="false"/>.
	/// </param>
	/// <returns>A value that disposes the scope asynchronously, configured as asked.</returns>
	public ConfiguredConnectionScope<TConnection> ConfigureAwait(bool continueOnCapturedContext)
		=> new(this, continueOnCapturedContext);
}

/// <summary>
/// A <see cref="ConnectionScope{TConnection}"/> whose disposal is awaited as configured by
/// <see cref="ConnectionScope{TConnection}.ConfigureAwait(bool)"/>. It exists only so that
/// <see langword="await"/> <see langword="using"/> of the scope allocates nothing.
/// </summary>
/// <remarks>
/// The scope is a struct, and the BCL <c>IAsyncDisposable.ConfigureAwait</c> extension would box it; this
/// type is what the scope's own <c>ConfigureAwait</c> returns instead. It holds no library setting, and the
/// library never returns to a caller's context: ODX always passes <see langword="false"/>, and whether to
/// return to a context is the caller's choice at their own call site, as with <see cref="Task.ConfigureAwait(bool)"/>.
/// </remarks>
/// <typeparam name="TConnection">The type of the connection, as the caller holds it.</typeparam>
public readonly struct ConfiguredConnectionScope<TConnection>
	where TConnection : IDbConnection
{
	readonly ConnectionScope<TConnection> _scope;
	readonly bool _continueOnCapturedContext;

	/// <summary>
	/// Creates the configured scope that <see cref="ConnectionScope{TConnection}.ConfigureAwait(bool)"/> returns.
	/// </summary>
	/// <param name="scope">The scope to dispose.</param>
	/// <param name="continueOnCapturedContext">
	/// <see langword="true"/> to continue on the captured context after the disposal; otherwise <see langword="false"/>.
	/// </param>
	internal ConfiguredConnectionScope(
		ConnectionScope<TConnection> scope,
		bool continueOnCapturedContext)
	{
		_scope = scope;
		_continueOnCapturedContext = continueOnCapturedContext;
	}

	/// <summary>Disposes the scope as <see cref="ConnectionScope{TConnection}.DisposeAsync"/> does, awaited as configured.</summary>
	/// <returns>An awaitable that completes when the scope has been disposed.</returns>
	public ConfiguredValueTaskAwaitable DisposeAsync()
		=> _scope.DisposeAsync().ConfigureAwait(_continueOnCapturedContext);
}
