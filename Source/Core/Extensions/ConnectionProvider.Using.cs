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
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static T Using<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, T> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory.Create();
		return action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Using<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Action<TConn> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory.Create();
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

		IDbConnection conn = connectionPool.Take();
		try
		{
			return action(conn);
		}
		finally
		{
			connectionPool.Give(conn);
		}
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

		IDbConnection conn = connectionPool.Take();
		try
		{
			action(conn);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static T Using<TConn, T>(this IDbConnectionPool<TConn> connectionPool, Func<TConn, T> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConn conn = connectionPool.Take();
		try
		{
			return action(conn);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static void Using<TConn>(this IDbConnectionPool<TConn> connectionPool, Action<TConn> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConn conn = connectionPool.Take();
		try
		{
			action(conn);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static T Using<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, T> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory();
		return action(conn);
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Using<TConn>(this Func<TConn> connectionFactory, Action<TConn> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory();
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
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, ValueTask<T>> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory.Create();
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask UsingAsync<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, ValueTask> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory.Create();
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

		IDbConnection conn = connectionPool.Take();
		try
		{
			return await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
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

		IDbConnection conn = connectionPool.Take();
		try
		{
			await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<TConn, T>(this IDbConnectionPool<TConn> connectionPool, Func<TConn, ValueTask<T>> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConn conn = connectionPool.Take();
		try
		{
			return await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, invokes the action, and then gives the connection back to the pool. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask UsingAsync<TConn>(this IDbConnectionPool<TConn> connectionPool, Func<TConn, ValueTask> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConn conn = connectionPool.Take();
		try
		{
			await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> UsingAsync<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, ValueTask<T>> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory();
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Invokes the function to create a connection, invokes the asynchronous action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask UsingAsync<TConn>(this Func<TConn> connectionFactory, Func<TConn, ValueTask> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConn conn = connectionFactory();
		await action(conn).ConfigureAwait(false);
	}
}
