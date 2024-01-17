namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for database transactions.
/// </summary>
public static partial class TransactionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

	/// <summary>
	/// Shortcut for creating an IDbCommand from any IDbTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
	/// <param name="commandText">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static IDbCommand CreateCommand(this IDbTransaction transaction,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (transaction is null) throw new ArgumentNullException(nameof(transaction));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		if (transaction.Connection is null) throw new InvalidOperationException("Transaction has no connection.");
		Contract.EndContractBlock();

		var command = transaction.Connection.CreateCommand(type, commandText, secondsTimeout);
		command.Transaction = transaction;
		return command;
	}

	/// <summary>
	/// Shortcut for creating a text IDbCommand from any IDbTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="commandText">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static IDbCommand CreateTextCommand(this IDbTransaction transaction,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Shortcut for creating a stored procedure IDbCommand from any IDbTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="procedureName">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static IDbCommand CreateStoredProcedureCommand(this IDbTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);

	/// <summary>
	/// Shortcut for creating an DbCommand from any DbTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
	/// <param name="commandText">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static DbCommand CreateCommand(this DbTransaction transaction,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (transaction is null) throw new ArgumentNullException(nameof(transaction));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		if (transaction.Connection is null) throw new InvalidOperationException("Transaction has no connection.");
		Contract.EndContractBlock();

		var command = transaction.Connection.CreateCommand(type, commandText, secondsTimeout);
		command.Transaction = transaction;
		return command;
	}

	/// <summary>
	/// Shortcut for creating a text DbCommand from any DbTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="commandText">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static DbCommand CreateTextCommand(this DbTransaction transaction,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Shortcut for creating a stored procedure DbCommand from any DbTransaction.
	/// </summary>
	/// <param name="transaction">The transaction to create a command from.</param>
	/// <param name="procedureName">The command text or stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
	/// <returns>The created SqlCommand.</returns>
	public static DbCommand CreateStoredProcedureCommand(this DbTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
