namespace Open.Database.Extensions;

// EnsureOpenScope / EnsureOpenScopeAsync: the factory and pool forms of Open / OpenAsync, handing back the
// connection and how it ends instead of taking a callback. They follow the callbacks exactly:
// a factory's connection is created, opened if needed, and disposed; a pool's is taken, opened
// if needed, and given back (the pool decides what that means). If the open throws or is
// cancelled the connection is released the same way before the exception goes on.
public static partial class ConnectionExtensions
{
	static ConnectionScope<TConnection> Seal<TConnection>(
		TConnection connection,
		ConnectionState priorState,
		IDbConnectionPool? pool)
		where TConnection : IDbConnection
		=> pool is null
		? new(connection, priorState, ScopeEnd.Dispose)
		: new(connection, priorState, pool);

	// Opens a connection that is not the caller's, and releases it if that fails.
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

	// A connection rented from a pool and not opened (the Using forms): the state it is in is
	// recorded, it is never opened or closed here, and it is given back when disposed.
	internal static ConnectionScope<IDbConnection> Rent(this IDbConnectionPool connectionPool)
	{
		IDbConnection connection = connectionPool.Take();
		return new(connection, connection.State, connectionPool);
	}

	internal static ConnectionScope<TConnection> Rent<TConnection>(this IDbConnectionPool<TConnection> connectionPool)
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
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>.</exception>
	public static ConnectionScope<IDbConnection> EnsureOpenScope(this IDbConnectionFactory connectionFactory)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		Contract.EndContractBlock();

		return OpenOwned(connectionFactory.Create(), null);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, and returns a
	/// <see cref="ConnectionScope{TConnection}"/> that disposes it when the scope is disposed.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <inheritdoc cref="EnsureOpenScope(IDbConnectionFactory)" path="/remarks"/>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>.</exception>
	public static ConnectionScope<TConnection> EnsureOpenScope<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory)
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
	/// <code>
	/// await using var scope = await factory.EnsureOpenScopeAsync(cancellationToken);
	/// var connection = scope.Connection;
	/// </code>
	/// The scope form of <see cref="OpenAsync{T}(IDbConnectionFactory, Func{IDbConnection, ValueTask{T}}, CancellationToken)"/>.
	/// The scope's <see cref="ConnectionScope{TConnection}.DisposeAsync"/> calls <c>DisposeAsync</c> on a connection
	/// that has one. If opening throws or <paramref name="cancellationToken"/> is cancelled, the connection is
	/// disposed before the exception propagates.
	/// </remarks>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
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
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <inheritdoc cref="EnsureOpenScopeAsync(IDbConnectionFactory, CancellationToken)" path="/remarks"/>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
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
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> is <see langword="null"/>.</exception>
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
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <inheritdoc cref="EnsureOpenScope(IDbConnectionPool)" path="/remarks"/>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> is <see langword="null"/>.</exception>
	public static ConnectionScope<TConnection> EnsureOpenScope<TConnection>(this IDbConnectionPool<TConnection> connectionPool)
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
	/// <code>
	/// await using var scope = await pool.EnsureOpenScopeAsync(cancellationToken);
	/// var connection = scope.Connection;
	/// </code>
	/// The scope form of <see cref="OpenAsync{T}(IDbConnectionPool, Func{IDbConnection, ConnectionState, ValueTask{T}}, CancellationToken)"/>.
	/// Giving back is the pool's, and is synchronous. If opening throws or <paramref name="cancellationToken"/> is
	/// cancelled, the connection is given back before the exception propagates.
	/// </remarks>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
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
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <inheritdoc cref="EnsureOpenScopeAsync(IDbConnectionPool, CancellationToken)" path="/remarks"/>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>The connection, and the state it was in before it was opened.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
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
