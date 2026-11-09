namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	/// <seealso cref="ConnectionExtensions.EnsureOpen(IDbConnection)"/>
	public static T Open<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, T> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
		return action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Open(this IDbConnectionFactory connectionFactory, Action<IDbConnection> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
		action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
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
	public static T Open<TConnection, T>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, T> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
		return action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Open<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory, Action<TConnection> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
		action(conn);
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static T Open<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, T> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		IDbConnection conn = connectionPool.Take();
		try
		{
			return action(conn, conn.EnsureOpen());
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static void Open(this IDbConnectionPool connectionPool, Action<IDbConnection, ConnectionState> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		IDbConnection conn = connectionPool.Take();
		try
		{
			action(conn, conn.EnsureOpen());
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <returns>The value returned by the action.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static T Open<TConnection, T>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ConnectionState, T> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConnection conn = connectionPool.Take();
		try
		{
			return action(conn, conn.EnsureOpen());
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static void Open<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Action<TConnection, ConnectionState> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConnection conn = connectionPool.Take();
		try
		{
			action(conn, conn.EnsureOpen());
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Invokes the function to create a connection, opens it if needed, invokes the action, and then disposes the connection.
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
	public static T Open<TConnection, T>(this Func<TConnection> connectionFactory, Func<TConnection, T> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
		return action(conn);
	}

	/// <summary>
	/// Invokes the function to create a connection, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The action to execute. It receives the connection.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static void Open<TConnection>(
		this Func<TConnection> connectionFactory,
		Action<TConnection> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
		action(conn);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> OpenAsync<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask<T>> action, CancellationToken cancellationToken = default)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		// Use EnsureOpen in case the connection factory implementation has it's own pooling.
		await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask OpenAsync(
		this IDbConnectionFactory connectionFactory,
		Func<IDbConnection, ValueTask> action,
		CancellationToken cancellationToken = default)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using IDbConnection conn = connectionFactory.Create();
		// Use EnsureOpen in case the connection factory implementation has it's own pooling.
		await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> OpenAsync<TConnection, T>(
		this IDbConnectionFactory<TConnection> connectionFactory,
		Func<TConnection, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		// Use EnsureOpen in case the connection factory implementation has it's own pooling.
		await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to create the connection from.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask OpenAsync<TConnection>(
		this IDbConnectionFactory<TConnection> connectionFactory,
		Func<TConnection, ValueTask> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory.Create();
		// Use EnsureOpen in case the connection factory implementation has it's own pooling.
		await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask<T> OpenAsync<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, ValueTask<T>> action, CancellationToken cancellationToken = default)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		IDbConnection conn = connectionPool.Take();
		try
		{
			return await action(conn,
				await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false))
				.ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask OpenAsync(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, ValueTask> action, CancellationToken cancellationToken = default)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		IDbConnection conn = connectionPool.Take();
		try
		{
			await action(conn,
				await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false))
				.ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask<T> OpenAsync<TConnection, T>(
		this IDbConnectionPool<TConnection> connectionPool,
		Func<TConnection, ConnectionState, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConnection conn = connectionPool.Take();
		try
		{
			return await action(conn,
				await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false))
				.ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionPool">The connection pool to take the connection from.</param>
	/// <param name="action">The action to execute. It receives the connection and the <see cref="ConnectionState"/> the connection was in before it was opened.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been given back to the pool.
	/// </remarks>
	public static async ValueTask OpenAsync<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ConnectionState, ValueTask> action, CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		TConnection conn = connectionPool.Take();
		try
		{
			await action(conn,
				await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false))
				.ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <summary>
	/// Invokes the function to create a connection, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that produces the value returned by the action after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask<T> OpenAsync<TConnection, T>(
		this Func<TConnection> connectionFactory,
		Func<TConnection, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		// Use EnsureOpen in case the connection factory implementation has it's own pooling.
		await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		return await action(conn).ConfigureAwait(false);
	}

	/// <summary>
	/// Invokes the function to create a connection, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates the connection.</param>
	/// <param name="action">The asynchronous action to execute. It receives the connection.</param>
	/// <param name="cancellationToken">A token that cancels opening the connection.</param>
	/// <returns>A task that completes after the action has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while the connection was being opened.</exception>
	/// <remarks>
	/// Useful for single-line operations. Exceptions thrown by the action propagate after the connection has been disposed.
	/// </remarks>
	public static async ValueTask OpenAsync<TConnection>(
		this Func<TConnection> connectionFactory,
		Func<TConnection, ValueTask> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using TConnection conn = connectionFactory();
		// Use EnsureOpen in case the connection factory implementation has it's own pooling.
		await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		await action(conn).ConfigureAwait(false);
	}
}
