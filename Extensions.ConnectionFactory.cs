using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	public static partial class Extensions
    {
		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static T Using<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, T> action)
			where TConn : IDbConnection
		{
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
			{
				return action(conn);
			}
		}

		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static void Using<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Action<TConn> action)
			where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
			{
				action(conn);
			}
		}

		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static T Using<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, T> action)
			where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
			{
				return action(conn);
			}
		}

		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static void Using<TConn>(this Func<TConn> connectionFactory, Action<TConn> action)
			where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
			{
				action(conn);
			}
		}

		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static async Task<T> UsingAsync<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, Task<T>> action)
			where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
			{
				return await action(conn).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static async Task UsingAsync<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, Task> action)
			where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
			{
				await action(conn).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The type returned from the action.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		/// <returns>The value from the action.</returns>
		public static async Task<T> UsingAsync<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, Task<T>> action)
			where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
			{
				return await action(conn).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Generates a connection and executes the action within a using statement.
		/// Useful for single-line operations.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <param name="connectionFactory">The connection factory to generate connections from.</param>
		/// <param name="action">The action to execute.</param>
		public static async Task UsingAsync<TConn>(this Func<TConn> connectionFactory, Func<TConn, Task> action)
			where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
			{
				await action(conn).ConfigureAwait(false);
			}
		}


        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <typeparam name="T">The type returned from the action.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        /// <returns>The value from the action.</returns>
        public static T Open<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, T> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
            {
                conn.Open();
                return action(conn);
            }
        }

        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        public static void Open<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Action<TConn> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
            {
                conn.Open();
                action(conn);
            }
        }

        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <typeparam name="T">The type returned from the action.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        /// <returns>The value from the action.</returns>
        public static T Open<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, T> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
            {
                conn.Open();
                return action(conn);
            }
        }

        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        public static void Open<TConn>(this Func<TConn> connectionFactory, Action<TConn> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
            {
                conn.Open();
                action(conn);
            }
        }

        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <typeparam name="T">The type returned from the action.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        /// <returns>The value from the action.</returns>
        public static async Task<T> OpenAsync<TConn, T>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, Task<T>> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
            {
                var c = conn as DbConnection;
                if (c == null) conn.Open();
                else await c.OpenAsync();
                return await action(conn).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        public static async Task OpenAsync<TConn>(this IDbConnectionFactory<TConn> connectionFactory, Func<TConn, Task> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory.Create())
            {
                var c = conn as DbConnection;
                if (c == null) conn.Open();
                else await c.OpenAsync();
                await action(conn).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <typeparam name="T">The type returned from the action.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        /// <returns>The value from the action.</returns>
        public static async Task<T> OpenAsync<TConn, T>(this Func<TConn> connectionFactory, Func<TConn, Task<T>> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
            {
                var c = conn as DbConnection;
                if (c == null) conn.Open();
                else await c.OpenAsync();
                return await action(conn).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Generates a connection, opens it, and executes the action within a using statement.
        /// Useful for single-line operations.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="connectionFactory">The connection factory to generate connections from.</param>
        /// <param name="action">The action to execute.</param>
        public static async Task OpenAsync<TConn>(this Func<TConn> connectionFactory, Func<TConn, Task> action)
            where TConn : IDbConnection
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            Contract.EndContractBlock();

            using (var conn = connectionFactory())
            {
                var c = conn as DbConnection;
                if (c == null) conn.Open();
                else await c.OpenAsync();
                await action(conn).ConfigureAwait(false);
            }
        }



        /// <summary>
        /// Creates an ExpressiveDbCommand for subsequent configuration and execution.
        /// </summary>
        /// <param name="target">The connection to execute the command on.</param>
        /// <param name="command">The command text or stored procedure name to use.</param>
        /// <param name="type">The command type.</param>
        /// <returns>The resultant ExpressiveDbCommand.</returns>
        public static ExpressiveDbCommand Command(
			this DbConnection target,
			string command,
			CommandType type = CommandType.Text)
			=> new ExpressiveDbCommand(target, null, type, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command(
			this DbTransaction target,
			string command,
			CommandType type = CommandType.Text)
			=> new ExpressiveDbCommand(target.Connection, target, type, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this DbConnection target,
			string command)
			=> new ExpressiveDbCommand(target, null, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this DbTransaction target,
			string command)
			=> new ExpressiveDbCommand(target.Connection, target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command(
			this IDbConnectionFactory<DbConnection> target,
			string command,
			CommandType type = CommandType.Text)
			=> new ExpressiveDbCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this IDbConnectionFactory<DbConnection> target,
			string command)
			=> new ExpressiveDbCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command(
			this Func<DbConnection> target,
			string command,
			CommandType type = CommandType.Text)
			=> Command(new DbConnectionFactory(target), command, type);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this Func<DbConnection> target,
			string command)
			=> StoredProcedure(new DbConnectionFactory(target), command);


	}
}
