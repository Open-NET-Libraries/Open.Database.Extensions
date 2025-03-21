﻿namespace Open.Database.Extensions;

public static partial class SqlTransactionExtensions
{
	/// <summary>
	/// Shortcut for creating an SqlCommand from any SqlTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
	/// <param name="commandText">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static SqlCommand CreateCommand(this SqlTransaction transaction,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (transaction is null) throw new ArgumentNullException(nameof(transaction));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException("Command is empty or whitespace.", nameof(commandText));
		Contract.EndContractBlock();

		SqlCommand command = transaction
			.Connection
			.CreateCommand(type, commandText, secondsTimeout);

		command.Transaction = transaction;
		return command;
	}

	/// <summary>
	/// Shortcut for creating a text SqlCommand from any SqlTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="procedureName">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static SqlCommand CreateTextCommand(this SqlTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.Text, procedureName, secondsTimeout);

	/// <summary>
	/// Shortcut for creating a stored procedure SqlCommand from any SqlTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="procedureName">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static SqlCommand CreateStoredProcedureCommand(this SqlTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
