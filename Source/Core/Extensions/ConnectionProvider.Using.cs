using System;
using System.Data;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;

namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for acquiring and operating on different connection factories.
/// </summary>
public static partial class ConnectionExtensions
{
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static T Using<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, T> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		return action(conn);
	}

	/// <summary>
	/// Generates a connection and executes the action within a using statement.
	/// </summary>
	/// <remarks>Useful for single-line operations.</remarks>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <param name="action">The action to execute.</param>
	public static void Using(this IDbConnectionFactory connectionFactory, Action<IDbConnection> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		action(conn);
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static T Using<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, T> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		return action(conn);
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static void Using<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Action<TConn> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		action(conn);
	}

	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionPool, Action{IDbConnection})"/>
	public static T Using<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, T> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
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
	/// Acquires a connection from the pool, returning it after the action is complete.
	/// <remarks>Useful for single-line operations.</remarks>
	/// </summary>
	/// <param name="connectionPool">The connection pool to acquire connections from.</param>
	/// <param name="action">The action to execute.</param>
	public static void Using(this IDbConnectionPool connectionPool, Action<IDbConnection> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
		try
		{
			action(conn);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionPool, Action{IDbConnection})"/>
	public static T Using<TConn, T>(this IDbConnectionPool<TConn> connectionPool, Func<TConn, T> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
		try
		{
			return action(conn);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <inheritdoc cref="Using(IDbConnectionPool, Action{IDbConnection})"/>
	public static void Using<TConn>(this IDbConnectionPool<TConn> connectionPool, Action<TConn> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
		try
		{
			action(conn);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static T Using<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, T> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory();
		return action(conn);
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static void Using<TConn>(this Func<TConn> connectionFactory, Action<TConn> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory();
		action(conn);
	}

	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static async ValueTask<T> UsingAsync<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask<T>> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		return await action(conn).ConfigureAwait(false);
	}

	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static async ValueTask UsingAsync(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask> action)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		await action(conn).ConfigureAwait(false);
	}

	/// <inheritdoc cref="Using{T}(IDbConnectionFactory, Func{IDbConnection, T})"/>
	public static async ValueTask<T> UsingAsync<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, ValueTask<T>> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		return await action(conn).ConfigureAwait(false);
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static async ValueTask UsingAsync<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, ValueTask> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory.Create();
		await action(conn).ConfigureAwait(false);
	}

	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionPool, Action{IDbConnection})"/>
	public static async ValueTask<T> UsingAsync<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ValueTask<T>> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
		try
		{
			return await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <inheritdoc cref="Using(IDbConnectionPool, Action{IDbConnection})"/>
	public static async ValueTask UsingAsync(this IDbConnectionPool connectionPool, Func<IDbConnection, ValueTask> action)
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
		try
		{
			await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <returns>The value from the action.</returns>
	/// <inheritdoc cref="Using(IDbConnectionPool, Action{IDbConnection})"/>
	public static async ValueTask<T> UsingAsync<TConn, T>(this IDbConnectionPool<TConn> connectionPool, Func<TConn, ValueTask<T>> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
		try
		{
			return await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <inheritdoc cref="Using(IDbConnectionPool, Action{IDbConnection})"/>
	public static async ValueTask UsingAsync<TConn>(this IDbConnectionPool<TConn> connectionPool, Func<TConn, ValueTask> action)
		where TConn : IDbConnection
	{
		if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		var conn = connectionPool.Take();
		try
		{
			await action(conn).ConfigureAwait(false);
		}
		finally
		{
			connectionPool.Give(conn);
		}
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <typeparam name="T">The type returned from the action.</typeparam>
	/// <inheritdoc cref="Using{T}(IDbConnectionFactory, Func{IDbConnection, T})"/>
	public static async ValueTask<T> UsingAsync<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, ValueTask<T>> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory();
		return await action(conn).ConfigureAwait(false);
	}

	/// <typeparam name="TConn">The connection type.</typeparam>
	/// <inheritdoc cref="Using(IDbConnectionFactory, Action{IDbConnection})"/>
	public static async ValueTask UsingAsync<TConn>(this Func<TConn> connectionFactory, Func<TConn, ValueTask> action)
		where TConn : IDbConnection
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		using var conn = connectionFactory();
		await action(conn).ConfigureAwait(false);
	}
}
