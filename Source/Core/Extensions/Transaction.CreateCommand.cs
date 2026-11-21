namespace Open.Database.Extensions;

/// <summary>
/// Extension methods for database transactions that do not depend on a specific database provider.
/// </summary>
public static partial class TransactionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

	/// <summary>
	/// Creates an <see cref="IDbCommand"/> on the transaction's connection, enlisted in the transaction, with the command type, text, and timeout already set.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-transaction']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-text']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <returns>A new <see cref="IDbCommand"/> that the caller owns and disposes. Its <see cref="IDbCommand.Transaction"/> is <paramref name="transaction"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The transaction has no connection.</exception>
	public static IDbCommand CreateCommand(this IDbTransaction transaction,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (transaction is null) throw new ArgumentNullException(nameof(transaction));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		if (transaction.Connection is null) throw new InvalidOperationException("Transaction has no connection.");
		Contract.EndContractBlock();

		IDbCommand command = transaction.Connection.CreateCommand(type, commandText, secondsTimeout);
		command.Transaction = transaction;
		return command;
	}

	/// <summary>
	/// Creates a text <see cref="IDbCommand"/> on the transaction's connection, enlisted in the transaction.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-transaction']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-text-command']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <returns>A new <see cref="IDbCommand"/> that the caller owns and disposes. Its <see cref="IDbCommand.Transaction"/> is <paramref name="transaction"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The transaction has no connection.</exception>
	public static IDbCommand CreateTextCommand(this IDbTransaction transaction,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="IDbCommand"/> on the transaction's connection, enlisted in the transaction.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-transaction']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <returns>A new <see cref="IDbCommand"/> that the caller owns and disposes. Its <see cref="IDbCommand.Transaction"/> is <paramref name="transaction"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The transaction has no connection.</exception>
	public static IDbCommand CreateStoredProcedureCommand(this IDbTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);

	/// <summary>
	/// Creates a <see cref="DbCommand"/> on the transaction's connection, enlisted in the transaction, with the command type, text, and timeout already set.
	/// </summary>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. Its <see cref="DbCommand.Transaction"/> is <paramref name="transaction"/>.</returns>
	/// <inheritdoc cref="CreateCommand(IDbTransaction, CommandType, string, int)"/>
	public static DbCommand CreateCommand(this DbTransaction transaction,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (transaction is null) throw new ArgumentNullException(nameof(transaction));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		if (transaction.Connection is null) throw new InvalidOperationException("Transaction has no connection.");
		Contract.EndContractBlock();

		DbCommand command = transaction.Connection.CreateCommand(type, commandText, secondsTimeout);
		command.Transaction = transaction;
		return command;
	}

	/// <summary>
	/// Creates a text <see cref="DbCommand"/> on the transaction's connection, enlisted in the transaction.
	/// </summary>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. Its <see cref="DbCommand.Transaction"/> is <paramref name="transaction"/>.</returns>
	/// <inheritdoc cref="CreateTextCommand(IDbTransaction, string, int)"/>
	public static DbCommand CreateTextCommand(this DbTransaction transaction,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="DbCommand"/> on the transaction's connection, enlisted in the transaction.
	/// </summary>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. Its <see cref="DbCommand.Transaction"/> is <paramref name="transaction"/>.</returns>
	/// <inheritdoc cref="CreateStoredProcedureCommand(IDbTransaction, string, int)"/>
	public static DbCommand CreateStoredProcedureCommand(this DbTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
