using System;
using System.Data;
using System.Data.Common;

namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for acquiring and operating on different connection factories.
/// </summary>
public static partial class ConnectionExtensions
{
	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connection">The connection to execute the command on.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	public static ExpressiveCommand Command(
		this IDbConnection connection,
		string command, CommandType type = CommandType.Text)
		=> new(connection, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="transaction">The transaction to execute the command on.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	public static ExpressiveCommand Command(
		this IDbTransaction transaction,
		string command, CommandType type = CommandType.Text)
		=> new(transaction, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <inheritdoc cref="Command(IDbConnection, string, CommandType)"/>
	public static ExpressiveDbCommand Command(
	   this DbConnection connection,
	   string command, CommandType type = CommandType.Text)
	   => new(connection, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="transaction">The transaction to execute the command on.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	public static ExpressiveDbCommand Command(
		this DbTransaction transaction,
		string command, CommandType type = CommandType.Text)
		=> new(transaction, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connection">The connection to execute the command on.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	public static ExpressiveCommand StoredProcedure(
		this IDbConnection connection,
		string procedureName)
		=> new(connection, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="transaction">The transaction to execute the command on.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	public static ExpressiveCommand StoredProcedure(
		this IDbTransaction transaction,
		string procedureName)
		=> new(transaction, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <inheritdoc cref="StoredProcedure(IDbConnection, string)"/>
	public static ExpressiveDbCommand StoredProcedure(
		this DbConnection connection,
		string procedureName)
		=> new(connection, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <inheritdoc cref="StoredProcedure(IDbTransaction, string)"/>
	public static ExpressiveDbCommand StoredProcedure(
		this DbTransaction transaction,
		string procedureName)
		=> new(transaction, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	public static ExpressiveCommand Command(
		this IDbConnectionFactory connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	public static ExpressiveCommand StoredProcedure(
		this IDbConnectionFactory connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection pool to take connections from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	public static ExpressiveCommand Command(
		this IDbConnectionPool connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection pool to take connections from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	public static ExpressiveCommand StoredProcedure(
		this IDbConnectionPool connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	public static ExpressiveDbCommand Command<TConnection>(
		this IDbConnectionFactory<TConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		where TConnection : DbConnection
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <returns>The resultant ExpressiveDbCommand.</returns>
	public static ExpressiveDbCommand StoredProcedure<TConnection>(
		this IDbConnectionFactory<TConnection> connectionSource,
		string procedureName)
		where TConnection : DbConnection
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The connection pool to take connections from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	public static ExpressiveDbCommand Command<TConnection>(
		this IDbConnectionPool<TConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		where TConnection : DbConnection
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The connection pool to take connections from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	public static ExpressiveDbCommand StoredProcedure<TConnection>(
		this IDbConnectionPool<TConnection> connectionSource,
		string procedureName)
		where TConnection : DbConnection
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate a commands from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	public static ExpressiveCommand Command(
		this Func<IDbConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> Command(DbConnectionFactory.Create(connectionSource), command, type);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <param name="connectionSource">The connection factory to generate a commands from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	public static ExpressiveCommand StoredProcedure(
		this Func<IDbConnection> connectionSource,
		string procedureName)
		=> StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> for subsequent configuration and execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The connection factory to generate a commands from.</param>
	/// <param name="command">The command text or stored procedure name to use.</param>
	/// <param name="type">The command type. Default = CommandType.Text.</param>
	public static ExpressiveDbCommand Command<TConnection>(
		this Func<TConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		where TConnection : DbConnection
		=> Command(DbConnectionFactory.Create(connectionSource), command, type);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> with command type set to <see cref="CommandType.StoredProcedure"/> for subsequent configuration and execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The connection factory to generate a commands from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	public static ExpressiveDbCommand StoredProcedure<TConnection>(
		this Func<TConnection> connectionSource,
		string procedureName)
		where TConnection : DbConnection
		=> StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);
}
