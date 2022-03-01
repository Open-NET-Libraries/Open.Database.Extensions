using System;
using System.Data;
using System.Data.SqlClient;

namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for acquiring and operating on different connection factories.
/// </summary>
public static partial class SqlConnectionExtensions
{
	/// <summary>
	/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
	/// </summary>
	/// <param name="connection">The connection to execute the command on.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand Command(
		this SqlConnection connection,
		string command, CommandType type = CommandType.Text)
		=> new(connection, type, command);

	/// <summary>
	/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
	/// </summary>
	/// <param name="transaction">The transaction to execute the command on.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand Command(
		this SqlTransaction transaction,
		string command, CommandType type = CommandType.Text)
		=> new(transaction, type, command);

	/// <summary>
	/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
	/// </summary>
	/// <param name="connection">The connection to execute the command on.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand StoredProcedure(
		this SqlConnection connection,
		string procedureName)
		=> new(connection, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
	/// </summary>
	/// <param name="transaction">The transaction to execute the command on.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand StoredProcedure(
		this SqlTransaction transaction,
		string procedureName)
		=> new(transaction, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand Command(
		this IDbConnectionFactory<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand StoredProcedure(
		this IDbConnectionFactory<SqlConnection> connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection pool to take connections from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand Command(
		this IDbConnectionPool<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection pool to take connections from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand StoredProcedure(
		this IDbConnectionPool<SqlConnection> connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate a commands from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand Command(
		this Func<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> Command(DbConnectionFactory.Create(connectionSource), command, type);

	/// <summary>
	/// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate a commands from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <returns>The resultant ExpressiveSqlCommand.</returns>
	public static ExpressiveSqlCommand StoredProcedure(
		this Func<SqlConnection> connectionSource,
		string procedureName)
		=> StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);
}
