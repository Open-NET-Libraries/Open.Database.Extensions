using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;

namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

	/// <summary>
	/// Shortcut for creating an <see cref="IDbCommand"/> from any <see cref="IDbConnection"/>.
	/// </summary>
	/// <param name="connection">The connection to create a command from.</param>
	/// <param name="type">The command type.  <see cref="CommandType.Text"/>, <see cref="CommandType.StoredProcedure"/>, or <see cref="CommandType.TableDirect"/>.</param>
	/// <param name="commandText">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	public static IDbCommand CreateCommand(this IDbConnection connection,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		Contract.EndContractBlock();

		var command = connection.CreateCommand();
		command.CommandType = type;
		command.CommandText = commandText;
		command.CommandTimeout = secondsTimeout;
		return command;
	}

	/// <summary>
	/// Shortcut for creating a text <see cref="IDbCommand"/> from any <see cref="IDbConnection"/>.
	/// </summary>
	/// <inheritdoc cref=" CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static IDbCommand CreateTextCommand(this IDbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Shortcut for creating a stored procedure <see cref="IDbCommand"/> from any <see cref="IDbConnection"/>.
	/// </summary>
	/// <inheritdoc cref=" CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static IDbCommand CreateStoredProcedureCommand(this IDbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, commandText, secondsTimeout);

	/// <summary>
	/// Shortcut for creating an <see cref="DbCommand"/> from any <see cref="DbConnection"/>.
	/// </summary>
	/// <inheritdoc cref=" CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static DbCommand CreateCommand(this DbConnection connection,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		Contract.EndContractBlock();

		var command = connection.CreateCommand();
		command.CommandType = type;
		command.CommandText = commandText;
		command.CommandTimeout = secondsTimeout;

		return command;
	}

	/// <summary>
	/// Shortcut for creating a text <see cref="DbCommand"/> from any <see cref="DbConnection"/>.
	/// </summary>
	/// <inheritdoc cref=" CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static DbCommand CreateTextCommand(this DbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Shortcut for creating a stored procedure <see cref="DbCommand"/> from any <see cref="DbConnection"/>.s
	/// </summary>
	/// <inheritdoc cref=" CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static DbCommand CreateStoredProcedureCommand(this DbConnection connection,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
