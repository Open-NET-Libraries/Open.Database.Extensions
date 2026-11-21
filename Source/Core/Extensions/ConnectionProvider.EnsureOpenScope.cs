namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <summary>
	/// Wraps a connection in a scope that ends the way its source calls for.
	/// </summary>
	/// <typeparam name="TConnection">The type of the connection.</typeparam>
	/// <param name="connection">The connection to wrap.</param>
	/// <param name="priorState">The state the connection was found in.</param>
	/// <param name="pool">The pool the connection was taken from, or <see langword="null"/> when a factory created it.</param>
	/// <returns>
	/// A scope that gives the connection back to <paramref name="pool"/> when it is disposed,
	/// or disposes the connection when there is no pool.
	/// </returns>
	static ConnectionScope<TConnection> Seal<TConnection>(
		TConnection connection,
		ConnectionState priorState,
		IDbConnectionPool? pool)
		where TConnection : IDbConnection
		=> pool is null
		? new(connection, priorState, ScopeEnd.Dispose)
		: new(connection, priorState, pool);

	/// <summary>
	/// Opens a connection that is not the caller's, and releases it if that fails.
	/// </summary>
	/// <typeparam name="TConnection">The type of the connection.</typeparam>
	/// <param name="connection">The connection to open.</param>
	/// <param name="pool">The pool the connection was taken from, or <see langword="null"/> when a factory created it.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <remarks>
	/// Releasing means giving the connection back to <paramref name="pool"/>, or disposing it when there is no pool.
	/// The exception that made opening fail is rethrown after that.
	/// </remarks>
	internal static ConnectionScope<TConnection> OpenOwned<TConnection>(
		TConnection connection,
		IDbConnectionPool? pool)
		where TConnection : IDbConnection
	{
		try
		{
			// EnsureOpen in case the source has its own pooling.
			return Seal(connection, connection.EnsureOpen(), pool);
		}
		catch
		{
			Seal(connection, default, pool).Dispose();
			throw;
		}
	}

	/// <summary>
	/// Asynchronously opens a connection that is not the caller's, and releases it if that fails or is canceled.
	/// </summary>
	/// <typeparam name="TConnection">The type of the connection.</typeparam>
	/// <param name="connection">The connection to open.</param>
	/// <param name="pool">The pool the connection was taken from, or <see langword="null"/> when a factory created it.</param>
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <inheritdoc cref="OpenOwned{TConnection}(TConnection, IDbConnectionPool)" path="/*[not(self::summary) and not(self::param) and not(self::typeparam)]"/>
	internal static async ValueTask<ConnectionScope<TConnection>> OpenOwnedAsync<TConnection>(
		TConnection connection,
		IDbConnectionPool? pool,
		CancellationToken cancellationToken)
		where TConnection : IDbConnection
	{
		try
		{
			// EnsureOpen in case the source has its own pooling.
			return Seal(
				connection,
				await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false),
				pool);
		}
		catch
		{
			await Seal(connection, default, pool).DisposeAsync().ConfigureAwait(false);
			throw;
		}
	}

	/// <summary>
	/// Takes a connection from the pool without opening it (the <c>Using</c> forms).
	/// </summary>
	/// <param name="connectionPool">The pool to take the connection from.</param>
	/// <returns>A scope that holds the connection and the state it is in, and gives it back when it is disposed.</returns>
	/// <remarks>
	/// The state the connection is in is recorded; it is never opened or closed here.
	/// </remarks>
	internal static ConnectionScope<IDbConnection> Rent(this IDbConnectionPool connectionPool)
	{
		IDbConnection connection = connectionPool.Take();
		return new(connection, connection.State, connectionPool);
	}

	/// <typeparam name="TConnection">The type of the connection.</typeparam>
	/// <inheritdoc cref="Rent(IDbConnectionPool)"/>
	internal static ConnectionScope<TConnection> Rent<TConnection>(
		this IDbConnectionPool<TConnection> connectionPool)
		where TConnection : IDbConnection
	{
		TConnection connection = connectionPool.Take();
		return new(connection, connection.State, connectionPool);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that disposes it when the scope is disposed.
	/// </summary>
	/// <remarks>
	/// The scope form of <see cref="Open{T}(IDbConnectionFactory, Func{IDbConnection, T})"/>: the same
	/// create, open if needed, dispose, without the callback. Prefer
	/// <see cref="EnsureOpenScopeAsync(IDbConnectionFactory, CancellationToken)"/>. If opening throws the
	/// connection is disposed before the exception propagates.
	/// </remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static ConnectionScope<IDbConnection> EnsureOpenScope(
		this IDbConnectionFactory connectionFactory)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		Contract.EndContractBlock();

		return OpenOwned(connectionFactory.Create(), null);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that disposes it when the scope is disposed.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <inheritdoc cref="EnsureOpenScope(IDbConnectionFactory)" path="/remarks"/>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static ConnectionScope<TConnection> EnsureOpenScope<TConnection>(
		this IDbConnectionFactory<TConnection> connectionFactory)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		Contract.EndContractBlock();

		return OpenOwned(connectionFactory.Create(), null);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that disposes it when the scope is disposed.
	/// </summary>
	/// <remarks>
	/// <code language="csharp">
	/// await using var scope = await factory.EnsureOpenScopeAsync(cancellationToken);
	/// var connection = scope.Connection;
	/// </code>
	/// The scope form of <see cref="OpenAsync{T}(IDbConnectionFactory, Func{IDbConnection, ValueTask{T}}, CancellationToken)"/>.
	/// The scope's <see cref="ConnectionScope{TConnection}.DisposeAsync"/> calls <c>DisposeAsync</c> on a connection
	/// that has one. If opening throws or <paramref name="cancellationToken"/> is canceled, the connection is
	/// disposed before the exception propagates.
	/// </remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static async ValueTask<ConnectionScope<IDbConnection>> EnsureOpenScopeAsync(
		this IDbConnectionFactory connectionFactory,
		CancellationToken cancellationToken = default)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		Contract.EndContractBlock();

		return await OpenOwnedAsync(connectionFactory.Create(), null, cancellationToken)
			.ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that disposes it when the scope is disposed.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <inheritdoc cref="EnsureOpenScopeAsync(IDbConnectionFactory, CancellationToken)" path="/remarks"/>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static async ValueTask<ConnectionScope<TConnection>> EnsureOpenScopeAsync<TConnection>(
		this IDbConnectionFactory<TConnection> connectionFactory,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		Contract.EndContractBlock();

		return await OpenOwnedAsync(connectionFactory.Create(), null, cancellationToken)
			.ConfigureAwait(false);
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that gives it back to the pool when the scope is disposed.
	/// </summary>
	/// <remarks>
	/// The scope form of <see cref="Open{T}(IDbConnectionPool, Func{IDbConnection, ConnectionState, T})"/>: the same
	/// take, open if needed, give back, without the callback. Giving back is the pool's: it may close the
	/// connection, keep it, or dispose it. Prefer <see cref="EnsureOpenScopeAsync(IDbConnectionPool, CancellationToken)"/>.
	/// If opening throws the connection is given back before the exception propagates.
	/// </remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-pool']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static ConnectionScope<IDbConnection> EnsureOpenScope(this IDbConnectionPool connectionPool)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		Contract.EndContractBlock();

		return OpenOwned(connectionPool.Take(), connectionPool);
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that gives it back to the pool when the scope is disposed.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <inheritdoc cref="EnsureOpenScope(IDbConnectionPool)" path="/remarks"/>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-pool']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static ConnectionScope<TConnection> EnsureOpenScope<TConnection>(
		this IDbConnectionPool<TConnection> connectionPool)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		Contract.EndContractBlock();

		return OpenOwned(connectionPool.Take(), connectionPool);
	}

	/// <summary>
	/// Takes a connection from the pool, asynchronously opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that gives it back to the pool when the scope is disposed.
	/// </summary>
	/// <remarks>
	/// <code language="csharp">
	/// await using var scope = await pool.EnsureOpenScopeAsync(cancellationToken);
	/// var connection = scope.Connection;
	/// </code>
	/// The scope form of <see cref="OpenAsync{T}(IDbConnectionPool, Func{IDbConnection, ConnectionState, ValueTask{T}}, CancellationToken)"/>.
	/// Giving back is the pool's, and is synchronous. If opening throws or <paramref name="cancellationToken"/> is
	/// canceled, the connection is given back before the exception propagates.
	/// </remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-pool-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static async ValueTask<ConnectionScope<IDbConnection>> EnsureOpenScopeAsync(
		this IDbConnectionPool connectionPool,
		CancellationToken cancellationToken = default)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		Contract.EndContractBlock();

		return await OpenOwnedAsync(connectionPool.Take(), connectionPool, cancellationToken)
			.ConfigureAwait(false);
	}

	/// <summary>
	/// Takes a connection from the pool, asynchronously opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that gives it back to the pool when the scope is disposed.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <inheritdoc cref="EnsureOpenScopeAsync(IDbConnectionPool, CancellationToken)" path="/remarks"/>
	/// <include file="Connections.docs.xml" path="docs/members[@name='scope-pool-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-scope']/*" />
	public static async ValueTask<ConnectionScope<TConnection>> EnsureOpenScopeAsync<TConnection>(
		this IDbConnectionPool<TConnection> connectionPool,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		Contract.EndContractBlock();

		return await OpenOwnedAsync(connectionPool.Take(), connectionPool, cancellationToken)
			.ConfigureAwait(false);
	}
}
