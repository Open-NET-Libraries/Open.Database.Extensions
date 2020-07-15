using System;
using System.Data;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Core non-DB-specific extensions for acquiring and operating on different connection factories.
	/// </summary>
	public static partial class ConnectionExtensions
	{
		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static T Open<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, T> action)
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
			return action(conn);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static void Open(this IDbConnectionFactory connectionFactory, Action<IDbConnection> action)
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
			action(conn);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static T Open<TConnection, T>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, T> action)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
			return action(conn);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static void Open<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory, Action<TConnection> action)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
			action(conn);
		}

		/// <summary>
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static T Open<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, T> action)
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
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
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static void Open(this IDbConnectionPool connectionPool, Action<IDbConnection, ConnectionState> action)
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
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
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static T Open<TConnection, T>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ConnectionState, T> action)
			where TConnection : IDbConnection
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
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
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static void Open<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Action<TConnection, ConnectionState> action)
			where TConnection : IDbConnection
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
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
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static T Open<TConnection, T>(this Func<TConnection> connectionFactory, Func<TConnection, T> action)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory();
			conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
			return action(conn);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static void Open<TConnection>(this Func<TConnection> connectionFactory, Action<TConnection> action)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory();
			conn.EnsureOpen(); // Use EnsureOpen in case the connection factory implementation has it's own pooling.
			action(conn);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The value from the action.</returns>
		public static async ValueTask<T> OpenAsync<T>(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask<T>> action, CancellationToken cancellationToken = default)
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			// Use EnsureOpen in case the connection factory implementation has it's own pooling.
			await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true);
			return await action(conn).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static async ValueTask OpenAsync(this IDbConnectionFactory connectionFactory, Func<IDbConnection, ValueTask> action, CancellationToken cancellationToken = default)
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			// Use EnsureOpen in case the connection factory implementation has it's own pooling.
			await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true);
			await action(conn).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The value from the action.</returns>
		public static async ValueTask<T> OpenAsync<TConnection, T>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, ValueTask<T>> action, CancellationToken cancellationToken = default)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			// Use EnsureOpen in case the connection factory implementation has it's own pooling.
			await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true);
			return await action(conn).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static async ValueTask OpenAsync<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory, Func<TConnection, ValueTask> action, CancellationToken cancellationToken = default)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory.Create();
			// Use EnsureOpen in case the connection factory implementation has it's own pooling.
			await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true);
			await action(conn).ConfigureAwait(false);
		}

		/// <summary>
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The value from the action.</returns>
		public static async ValueTask<T> OpenAsync<T>(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, ValueTask<T>> action, CancellationToken cancellationToken = default)
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
			try
			{
				return await action(conn,
					await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true))
					.ConfigureAwait(false);
			}
			finally
			{
				connectionPool.Give(conn);
			}
		}

		/// <summary>
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static async ValueTask OpenAsync(this IDbConnectionPool connectionPool, Func<IDbConnection, ConnectionState, ValueTask> action, CancellationToken cancellationToken = default)
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
			try
			{
				await action(conn,
					await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true))
					.ConfigureAwait(false);
			}
			finally
			{
				connectionPool.Give(conn);
			}
		}

		/// <summary>
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The value from the action.</returns>
		public static async ValueTask<T> OpenAsync<TConnection, T>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ConnectionState, ValueTask<T>> action, CancellationToken cancellationToken = default)
			where TConnection : IDbConnection
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
			try
			{
				return await action(conn,
					await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true))
					.ConfigureAwait(false);
			}
			finally
			{
				connectionPool.Give(conn);
			}
		}

		/// <summary>
		/// Acquires a connection from the pool. Ensures it's open. Invokes the action.
		/// Ensures the connection is returned to the pool when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="connectionPool">The connection pool to acquire connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static async ValueTask OpenAsync<TConnection>(this IDbConnectionPool<TConnection> connectionPool, Func<TConnection, ConnectionState, ValueTask> action, CancellationToken cancellationToken = default)
			where TConnection : IDbConnection
		{
			if (connectionPool is null) throw new ArgumentNullException(nameof(connectionPool));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			var conn = connectionPool.Take();
			try
			{
				await action(conn,
					await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true))
					.ConfigureAwait(false);
			}
			finally
			{
				connectionPool.Give(conn);
			}
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The value from the action.</returns>
		public static async ValueTask<T> OpenAsync<TConnection, T>(this Func<TConnection> connectionFactory, Func<TConnection, ValueTask<T>> action, CancellationToken cancellationToken = default)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory();
			// Use EnsureOpen in case the connection factory implementation has it's own pooling.
			await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true);
			return await action(conn).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a connection. Ensures it's open. Invokes the action.
		/// Ensures the connection is disposed of when the action is complete.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static async ValueTask OpenAsync<TConnection>(this Func<TConnection> connectionFactory, Func<TConnection, ValueTask> action, CancellationToken cancellationToken = default)
			where TConnection : IDbConnection
		{
			if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			using var conn = connectionFactory();
			// Use EnsureOpen in case the connection factory implementation has it's own pooling.
			await conn.EnsureOpenAsync(cancellationToken).ConfigureAwait(true);
			await action(conn).ConfigureAwait(false);
		}
	}
}
