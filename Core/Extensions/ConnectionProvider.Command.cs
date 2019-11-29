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
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command(
			this DbConnection target,
			string command, CommandType type = CommandType.Text)
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
			string command, CommandType type = CommandType.Text)
			=> new ExpressiveDbCommand((target ?? throw new ArgumentNullException(nameof(target))).Connection, target, type, command);

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
			=> new ExpressiveDbCommand((target ?? throw new ArgumentNullException(nameof(target))).Connection, target, CommandType.StoredProcedure, command);

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
