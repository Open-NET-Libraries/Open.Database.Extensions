namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	/// <seealso cref="EnsureOpen(IDbConnection)"/>
	public static T Open<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, T> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionFactory.EnsureOpenScope();
		return action(scope.Connection);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static void Open(this IDbConnectionFactory connectionFactory, Action<IDbConnection> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionFactory.EnsureOpenScope();
		action(scope.Connection);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static T Open<TConnection, T>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, T> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionFactory.EnsureOpenScope();
		return action(scope.Connection);
	}

	/// <summary>
	/// Creates a connection from the factory, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static void Open<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory, Action<TConnection> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionFactory.EnsureOpenScope();
		action(scope.Connection);
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static T Open<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, T> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionPool.EnsureOpenScope();
		return action(scope.Connection, scope.PriorState);
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static void Open(this IDbConnectionPool connectionPool, Action<IDbConnection, ConnectionState> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionPool.EnsureOpenScope();
		action(scope.Connection, scope.PriorState);
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static T Open<TConnection, T>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ConnectionState, T> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionPool.EnsureOpenScope();
		return action(scope.Connection, scope.PriorState);
	}

	/// <summary>
	/// Takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static void Open<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Action<TConnection, ConnectionState> action)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = connectionPool.EnsureOpenScope();
		action(scope.Connection, scope.PriorState);
	}

	/// <summary>
	/// Invokes the function to create a connection, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static T Open<TConnection, T>(this Func<TConnection> connectionFactory, Func<TConnection, T> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = OpenOwned(connectionFactory(), null);
		return action(scope.Connection);
	}

	/// <summary>
	/// Invokes the function to create a connection, opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static void Open<TConnection>(
		this Func<TConnection> connectionFactory,
		Action<TConnection> action)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var scope = OpenOwned(connectionFactory(), null);
		action(scope.Connection);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static async ValueTask<T> OpenAsync<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask<T>> action, CancellationToken cancellationToken = default)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionFactory
			.EnsureOpenScopeAsync(cancellationToken)
			.ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		return await action(scope.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static async ValueTask OpenAsync(
		this IDbConnectionFactory connectionFactory,
		Func<IDbConnection, ValueTask> action,
		CancellationToken cancellationToken = default)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionFactory
			.EnsureOpenScopeAsync(cancellationToken)
			.ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		await action(scope.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static async ValueTask<T> OpenAsync<TConnection, T>(
		this IDbConnectionFactory<TConnection> connectionFactory,
		Func<TConnection, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionFactory
			.EnsureOpenScopeAsync(cancellationToken)
			.ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		return await action(scope.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Creates a connection from the factory, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static async ValueTask OpenAsync<TConnection>(
		this IDbConnectionFactory<TConnection> connectionFactory,
		Func<TConnection, ValueTask> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionFactory
			.EnsureOpenScopeAsync(cancellationToken)
			.ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		await action(scope.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static async ValueTask<T> OpenAsync<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, ValueTask<T>> action, CancellationToken cancellationToken = default)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionPool.EnsureOpenScopeAsync(cancellationToken).ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		return await action(scope.Connection, scope.PriorState).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static async ValueTask OpenAsync(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, ValueTask> action, CancellationToken cancellationToken = default)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionPool.EnsureOpenScopeAsync(cancellationToken).ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		await action(scope.Connection, scope.PriorState).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static async ValueTask<T> OpenAsync<TConnection, T>(
		this IDbConnectionPool<TConnection> connectionPool,
		Func<TConnection, ConnectionState, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionPool.EnsureOpenScopeAsync(cancellationToken).ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		return await action(scope.Connection, scope.PriorState).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously takes a connection from the pool, opens it if needed, invokes the action with the connection and the state it was in before being opened, and then gives the connection back to the pool.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-state-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-given-back']/*" />
	public static async ValueTask OpenAsync<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ConnectionState, ValueTask> action, CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await connectionPool.EnsureOpenScopeAsync(cancellationToken).ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		await action(scope.Connection, scope.PriorState).ConfigureAwait(false);
	}

	/// <summary>
	/// Invokes the function to create a connection, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static async ValueTask<T> OpenAsync<TConnection, T>(
		this Func<TConnection> connectionFactory,
		Func<TConnection, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await OpenOwnedAsync(connectionFactory(), null, cancellationToken).ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		return await action(scope.Connection).ConfigureAwait(false);
	}

	/// <summary>
	/// Invokes the function to create a connection, asynchronously opens it if needed, invokes the action, and then disposes the connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='cancel-open']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='open-disposed']/*" />
	public static async ValueTask OpenAsync<TConnection>(
		this Func<TConnection> connectionFactory,
		Func<TConnection, ValueTask> action,
		CancellationToken cancellationToken = default)
		where TConnection : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var scope = await OpenOwnedAsync(connectionFactory(), null, cancellationToken).ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);
		await action(scope.Connection).ConfigureAwait(false);
	}
}
