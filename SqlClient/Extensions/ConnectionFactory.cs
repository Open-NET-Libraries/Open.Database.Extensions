using System;
using System.Data;
using System.Data.SqlClient;
// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace Open.Database.Extensions.SqlClient
{
	public static class ConnectionFactoryExtensions
	{
		/// <summary>
		/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand Command(
			this SqlConnection target,
			string command, CommandType type = CommandType.Text)
			=> new ExpressiveSqlCommand(target, null, type, command);

		/// <summary>
		/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand Command(
			this SqlTransaction target,
			string command, CommandType type = CommandType.Text)
			=> new ExpressiveSqlCommand((target ?? throw new ArgumentNullException(nameof(target))).Connection, target, type, command);

		/// <summary>
		/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand StoredProcedure(
			this SqlConnection target,
			string command)
			=> new ExpressiveSqlCommand(target, null, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The transaction to execute the command on.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand StoredProcedure(
			this SqlTransaction target,
			string command)
			=> new ExpressiveSqlCommand((target ?? throw new ArgumentNullException(nameof(target))).Connection, target, CommandType.StoredProcedure, command);

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
			=> new ExpressiveSqlCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand StoredProcedure(
			this IDbConnectionFactory<SqlConnection> target,
			string command)
			=> new ExpressiveSqlCommand(target, CommandType.StoredProcedure, command);

		/// <summary>
		/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand Command(
			this Func<SqlConnection> target,
			string command, CommandType type = CommandType.Text)
			=> Command(new DbConnectionFactory<SqlConnection>(target), command, type);

		/// <summary>
		/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveSqlCommand.</returns>
		public static ExpressiveSqlCommand StoredProcedure(
			this Func<SqlConnection> target,
			string command)
			=> StoredProcedure(new DbConnectionFactory<SqlConnection>(target), command);

	}
}
