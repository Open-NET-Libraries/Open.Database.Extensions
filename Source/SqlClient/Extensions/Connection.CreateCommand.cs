﻿namespace Open.Database.Extensions;

/// <summary>
/// Extensions for SqlConnections.
/// </summary>
public static partial class SqlConnectionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

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
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		Contract.EndContractBlock();

		SqlCommand command = connection.CreateCommand();
		command.CommandType = type;
		command.CommandText = commandText;
		command.CommandTimeout = secondsTimeout;

		return command;
	}

	/// <summary>
	/// Shortcut for creating an text SqlCommand from any SqlConnection.
	/// </summary>
	/// <param name="connection">The connection to create a command from.</param>
	/// <param name="commandText">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static SqlCommand CreateTextCommand(this SqlConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Shortcut for creating a stored procedure SqlCommand from any SqlConnection.
	/// </summary>
	/// <param name="connection">The connection to create a command from.</param>
	/// <param name="procedureName">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static SqlCommand CreateStoredProcedureCommand(this SqlConnection connection,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
