namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <summary>
	/// Creates a connection from the factory, invokes the action, and then disposes the connection. The connection is not opened by this method.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='factory-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='pool-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-given-back']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tconnection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='func-async']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='using-disposed']/*" />
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
