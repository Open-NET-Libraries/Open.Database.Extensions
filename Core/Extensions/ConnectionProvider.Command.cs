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
    /// Creates an ExpressiveCommand for subsequent configuration and execution.
    /// </summary>
    /// <param name="connection">The connection to execute the command on.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand Command(
        this IDbConnection connection,
        string command, CommandType type = CommandType.Text)
        => new(connection, type, command);

    /// <summary>
    /// Creates an ExpressiveCommand for subsequent configuration and execution.
    /// </summary>
    /// <param name="transaction">The transaction to execute the command on.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand Command(
        this IDbTransaction transaction,
        string command, CommandType type = CommandType.Text)
        => new(transaction, type, command);

    /// <summary>
    /// Creates an ExpressiveDbCommand for subsequent configuration and execution.
    /// </summary>
    /// <param name="connection">The connection to execute the command on.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand Command(
        this DbConnection connection,
        string command, CommandType type = CommandType.Text)
        => new(connection, type, command);

    /// <summary>
    /// Creates an ExpressiveDbCommand for subsequent configuration and execution.
    /// </summary>
    /// <param name="transaction">The transaction to execute the command on.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand Command(
        this DbTransaction transaction,
        string command, CommandType type = CommandType.Text)
        => new(transaction, type, command);

    /// <summary>
    /// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <param name="connection">The connection to execute the command on.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand StoredProcedure(
        this IDbConnection connection,
        string procedureName)
        => new(connection, CommandType.StoredProcedure, procedureName);

    /// <summary>
    /// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <param name="transaction">The transaction to execute the command on.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand StoredProcedure(
        this IDbTransaction transaction,
        string procedureName)
        => new(transaction, CommandType.StoredProcedure, procedureName);

    /// <summary>
    /// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <param name="connection">The connection to execute the command on.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand StoredProcedure(
        this DbConnection connection,
        string procedureName)
        => new(connection, CommandType.StoredProcedure, procedureName);

    /// <summary>
    /// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <param name="transaction">The transaction to execute the command on.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand StoredProcedure(
        this DbTransaction transaction,
        string procedureName)
        => new(transaction, CommandType.StoredProcedure, procedureName);

    /// <summary>
    /// Creates an ExpressiveCommand for subsequent configuration and execution.
    /// </summary>
    /// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand Command(
        this IDbConnectionFactory connectionSource,
        string command,
        CommandType type = CommandType.Text)
        => new(connectionSource, type, command);

    /// <summary>
    /// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand StoredProcedure(
        this IDbConnectionFactory connectionSource,
        string procedureName)
        => new(connectionSource, CommandType.StoredProcedure, procedureName);

    /// <summary>
    /// Creates an ExpressiveCommand for subsequent configuration and execution.
    /// </summary>
    /// <param name="connectionSource">The connection pool to take connections from.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand Command(
        this IDbConnectionPool connectionSource,
        string command,
        CommandType type = CommandType.Text)
        => new(connectionSource, type, command);

    /// <summary>
    /// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <param name="connectionSource">The connection pool to take connections from.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveCommand.</returns>
    public static ExpressiveCommand StoredProcedure(
        this IDbConnectionPool connectionSource,
        string procedureName)
        => new(connectionSource, CommandType.StoredProcedure, procedureName);

    /// <summary>
    /// Creates an ExpressiveDbCommand for subsequent configuration and execution.
    /// </summary>
    /// <typeparam name="TConnection">The connection type.</typeparam>
    /// <param name="connectionSource">The connection factory to generate connections and subsequently commands from.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand Command<TConnection>(
        this IDbConnectionFactory<TConnection> connectionSource,
        string command,
        CommandType type = CommandType.Text)
        where TConnection : DbConnection
        => new(connectionSource, type, command);

    /// <summary>
    /// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
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
    /// Creates an ExpressiveDbCommand for subsequent configuration and execution.
    /// </summary>
    /// <typeparam name="TConnection">The connection type.</typeparam>
    /// <param name="connectionSource">The connection pool to take connections from.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand Command<TConnection>(
        this IDbConnectionPool<TConnection> connectionSource,
        string command,
        CommandType type = CommandType.Text)
        where TConnection : DbConnection
        => new(connectionSource, type, command);

    /// <summary>
    /// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <typeparam name="TConnection">The connection type.</typeparam>
    /// <param name="connectionSource">The connection pool to take connections from.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand StoredProcedure<TConnection>(
        this IDbConnectionPool<TConnection> connectionSource,
        string procedureName)
        where TConnection : DbConnection
        => new(connectionSource, CommandType.StoredProcedure, procedureName);

    /// <summary>
    /// Creates an ExpressiveCommand for subsequent configuration and execution.
    /// </summary>
    /// <param name="connectionSource">The connection factory to generate a commands from.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveCommand Command(
        this Func<IDbConnection> connectionSource,
        string command,
        CommandType type = CommandType.Text)
        => Command(DbConnectionFactory.Create(connectionSource), command, type);

    /// <summary>
    /// Creates an ExpressiveCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <param name="connectionSource">The connection factory to generate a commands from.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveCommand StoredProcedure(
        this Func<IDbConnection> connectionSource,
        string procedureName)
        => StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);

    /// <summary>
    /// Creates an ExpressiveDbCommand for subsequent configuration and execution.
    /// </summary>
    /// <typeparam name="TConnection">The connection type.</typeparam>
    /// <param name="connectionSource">The connection factory to generate a commands from.</param>
    /// <param name="command">The command text or stored procedure name to use.</param>
    /// <param name="type">The command type. Default = CommandType.Text.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand Command<TConnection>(
        this Func<TConnection> connectionSource,
        string command,
        CommandType type = CommandType.Text)
        where TConnection : DbConnection
        => Command(DbConnectionFactory.Create(connectionSource), command, type);

    /// <summary>
    /// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
    /// </summary>
    /// <typeparam name="TConnection">The connection type.</typeparam>
    /// <param name="connectionSource">The connection factory to generate a commands from.</param>
    /// <param name="procedureName">The stored procedure name to use.</param>
    /// <returns>The resultant ExpressiveDbCommand.</returns>
    public static ExpressiveDbCommand StoredProcedure<TConnection>(
        this Func<TConnection> connectionSource,
        string procedureName)
        where TConnection : DbConnection
        => StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);
}
