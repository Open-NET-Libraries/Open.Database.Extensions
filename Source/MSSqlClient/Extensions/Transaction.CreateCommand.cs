namespace Open.Database.Extensions;

public static partial class SqlTransactionExtensions
{
	/// <summary>
	/// Creates a <see cref="SqlCommand"/> on the transaction's connection, enlisted in the transaction, with the command type, text, and timeout already set.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-transaction']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-text']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-returns-tx']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
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

		// The caller owns the command only once it is returned: a provider that rejects the transaction must not leave it undisposed.
		try
		{
			command.Transaction = transaction;
			return command;
		}
		catch
		{
			command.Dispose();
			throw;
		}
	}

	/// <summary>
	/// Creates a text <see cref="SqlCommand"/> on the transaction's connection, enlisted in the transaction.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-transaction']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-text-command']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-returns-tx']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	public static SqlCommand CreateTextCommand(this SqlTransaction transaction,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="SqlCommand"/> on the transaction's connection, enlisted in the transaction.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-transaction']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-returns-tx']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static SqlCommand CreateStoredProcedureCommand(this SqlTransaction transaction,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> transaction.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
