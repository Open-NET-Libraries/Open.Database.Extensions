namespace Open.Database.Extensions;

public static partial class SqlTransactionExtensions
{
	/// <summary>
	/// Creates a <see cref="SqlCommand"/> enlisted in a transaction, on the transaction's connection, with the command type, text, and timeout already set.
	/// </summary>
	/// <param name="transaction">The transaction to create the command from.</param>
	/// <param name="type">The command type: <see cref="CommandType.Text"/>, <see cref="CommandType.StoredProcedure"/>, or <see cref="CommandType.TableDirect"/>.</param>
	/// <param name="commandText">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="SqlCommand"/> that the caller owns and disposes.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="commandText"/> is empty or white space.</exception>
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
	/// Creates a text <see cref="SqlCommand"/> enlisted in a transaction.
	/// </summary>
	/// <param name="transaction">The transaction to create the command from.</param>
	/// <param name="commandText">The command text to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="SqlCommand"/> that the caller owns and disposes.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="commandText"/> is empty or white space.</exception>
	public static SqlCommand CreateTextCommand(this SqlTransaction transaction,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="SqlCommand"/> enlisted in a transaction.
	/// </summary>
	/// <param name="transaction">The transaction to create the command from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="SqlCommand"/> that the caller owns and disposes.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static SqlCommand CreateStoredProcedureCommand(this SqlTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
