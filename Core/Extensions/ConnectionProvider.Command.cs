using System;
using System.Data;
using System.Data.Common;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Core non-DB-specific extensions for acquiring and operating on different connection factories.
	/// </summary>
	public static partial class ConnectionExtensions
	{
		/// <summary>
		/// Creates an ExpressiveCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveCommand.</returns>
		public static ExpressiveCommand Command(
			this IDbConnection target,
			string command, CommandType type = CommandType.Text)
			=> new ExpressiveCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveCommand.</returns>
		public static ExpressiveCommand Command(
			this IDbTransaction target,
			string command, CommandType type = CommandType.Text)
			=> new ExpressiveCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command(
			this DbConnection target,
			string command, CommandType type = CommandType.Text)
			=> new ExpressiveDbCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command(
			this DbTransaction target,
			string command, CommandType type = CommandType.Text)
			=> new ExpressiveDbCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveCommand.</returns>
		public static ExpressiveCommand StoredProcedure(
			this IDbConnection target,
			string command)
			=> new ExpressiveCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveCommand.</returns>
		public static ExpressiveCommand StoredProcedure(
			this IDbTransaction target,
			string command)
			=> new ExpressiveCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this DbConnection target,
			string command)
			=> new ExpressiveDbCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this DbTransaction target,
			string command)
			=> new ExpressiveDbCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveCommand.</returns>
		public static ExpressiveCommand Command(
			this IDbConnectionFactory target,
			string command,
			CommandType type = CommandType.Text)
			=> new ExpressiveCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveCommand.</returns>
		public static ExpressiveCommand StoredProcedure(
			this IDbConnectionFactory target,
			string command)
			=> new ExpressiveCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command<TConnection>(
			this IDbConnectionFactory<TConnection> target,
			string command,
			CommandType type = CommandType.Text)
			where TConnection : DbConnection
			=> new ExpressiveDbCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <typeparam name="TConnection">The connection type.</typeparam>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure<TConnection>(
			this IDbConnectionFactory<TConnection> target,
			string command)
			where TConnection : DbConnection
			=> new ExpressiveDbCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveCommand Command(
			this Func<IDbConnection> target,
			string command,
			CommandType type = CommandType.Text)
			=> Command(new DbConnectionFactory(target), command, type);

		/// <summary>
		/// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveCommand StoredProcedure(
			this Func<IDbConnection> target,
			string command)
			=> StoredProcedure(new DbConnectionFactory(target), command);

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command<TConnection>(
			this Func<TConnection> target,
			string command,
			CommandType type = CommandType.Text)
			where TConnection : DbConnection
			=> Command(DbConnectionFactory.Create(target), command, type);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure<TConnection>(
			this Func<TConnection> target,
			string command)
			where TConnection : DbConnection
			=> StoredProcedure(DbConnectionFactory.Create(target), command);
	}
}
