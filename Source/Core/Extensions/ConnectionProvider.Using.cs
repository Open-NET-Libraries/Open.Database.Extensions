namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <summary>
	/// Creates a connection from the factory, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static T Using<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, T> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		return action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Using(this IDbConnectionFactory connectionFactory, Action<IDbConnection> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static T Using<TConnection, T>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, T> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		return action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Using<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory, Action<TConnection> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		action(conn);
	}

	/// <summary>
	/// Takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static T Using<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, T> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var lease = connectionPool.Rent();
		return action(lease.Connection);
	}

	/// <summary>
	/// Takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static void Using(this IDbConnectionPool connectionPool, Action<IDbConnection> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var lease = connectionPool.Rent();
		action(lease.Connection);
	}

	/// <summary>
	/// Takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static T Using<TConnection, T>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, T> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var lease = connectionPool.Rent();
		return action(lease.Connection);
	}

	/// <summary>
	/// Takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static void Using<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Action<TConnection> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var lease = connectionPool.Rent();
		action(lease.Connection);
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static T Using<TConnection, T>(this Func<TConnection> connectionFactory, Func<TConnection, T> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		return action(conn);
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Using<TConnection>(this Func<TConnection> connectionFactory, Action<TConnection> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask<T>> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask UsingAsync(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<TConnection, T>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, ValueTask<T>> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask UsingAsync<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, ValueTask> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ValueTask<T>> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var lease = connectionPool.Rent();
		await using var leaseDisposal = lease.ConfigureAwait(false);
		return await action(lease.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask UsingAsync(this IDbConnectionPool connectionPool, Func<IDbConnection, ValueTask> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var lease = connectionPool.Rent();
		await using var leaseDisposal = lease.ConfigureAwait(false);
		await action(lease.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<TConnection, T>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ValueTask<T>> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var lease = connectionPool.Rent();
		await using var leaseDisposal = lease.ConfigureAwait(false);
		return await action(lease.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask UsingAsync<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ValueTask> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var lease = connectionPool.Rent();
		await using var leaseDisposal = lease.ConfigureAwait(false);
		await action(lease.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<TConnection, T>(this Func<TConnection> connectionFactory, Func<TConnection, ValueTask<T>> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask UsingAsync<TConnection>(this Func<TConnection> connectionFactory, Func<TConnection, ValueTask> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		await action(conn).ConfigureAwait(false);
	}
}
