
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.SqlClient
{
	/// <summary>
	/// SqlClient extensions for building a command and retrieving data using best practices.
	/// </summary>
	public static class Extensions
	{

		/// <summary>
		/// Shortcut for adding command a typed (non-input) parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="type">The SqlDbType of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameterType(this SqlCommand target, string name, SqlDbType type)
		{
			var c = target.CreateParameter();
			c.ParameterName = name;
			c.SqlDbType = type;
			target.Parameters.Add(c);
			return c;
		}

		/// <summary>
		/// Shortcut for adding command a typed (non-input) parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="type">The SqlDbType of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameterType(this IDbCommand target, string name, SqlDbType type)
		{
			return AddParameterType((SqlCommand)target, name, type);
		}

		/// <summary>
		/// Shortcut for creating an SqlCommand from any SqlConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		public static SqlCommand CreateCommand(this SqlConnection connection,
			CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		{
			var command = connection.CreateCommand();
			command.CommandType = type;
			command.CommandText = commandText;
			command.CommandTimeout = secondsTimeout;

			return command;
		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async Task<List<T>> ToListAsync<T>(this SqlDataReader reader,
			Func<IDataRecord, T> transform)
		{
			var list = new List<T>();
			while (await reader.ReadAsync()) list.Add(transform(reader));
			return list;
		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The SqlCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async Task<List<T>> ToListAsync<T>(this SqlCommand command,
			Func<IDataRecord, T> transform)
		{
			using (var reader = await command.ExecuteReaderAsync())
				return await reader.ToListAsync(transform);
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		public static async Task ToTargetBlockAsync<T>(this SqlDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform)
		{
			Task<bool> lastSend = Task.FromResult(true);
			while (target.IsStillAlive()
				&& await reader.ReadAsync()
				&& await lastSend)
			{
				// Allows for a premtive read before waiting for the next send.
				lastSend = target.SendAsync(transform(reader));
			}
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		public static async Task ToTargetBlockAsync<T>(this SqlCommand command,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform)
		{
			if (target.IsStillAlive())
			{
				using (var reader = await command.ExecuteReaderAsync())
				{
					if (target.IsStillAlive())
						await reader.ToTargetBlockAsync(target, transform);
				}
			}
		}

		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="token">Optional cancellatio token.</param>
		public static async Task IterateReaderAsync(this SqlCommand command, Action<IDataRecord> handler, CancellationToken? token = null)
		{
			using (var reader = await command.ExecuteReaderAsync())
			{
				if (token.HasValue)
				{
					var t = token.Value;
					while (!t.IsCancellationRequested && await reader.ReadAsync())
						handler(reader);
				}
				else
				{
					while (await reader.ReadAsync())
						handler(reader);
				}
			}
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="predicate">The hanlder function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public static async Task IterateReaderAsyncWhile(this SqlCommand command, Func<IDataRecord, bool> predicate, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = await command.ExecuteReaderAsync(behavior))
				while (await reader.ReadAsync() && predicate(reader)) { }
		}
		
		/// <summary>
		/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand Command(
			this IDbConnectionFactory<SqlConnection> target,
			string command, CommandType type = CommandType.Text)
		{
			return new ExpressiveSqlCommand(target, type, command);
		}

		/// <summary>
		/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand StoredProcedure(
			this IDbConnectionFactory<SqlConnection> target,
			string command)
		{
			return new ExpressiveSqlCommand(target, CommandType.StoredProcedure, command);
		}
	}
}
