namespace Open.Database.Extensions;

public static partial class SqlConnectionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

	/// <summary>
	/// Creates a <see cref="SqlCommand"/> from a connection with the command type, text, and timeout already set.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-connection']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-text']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-returns']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	public static SqlCommand CreateCommand(this SqlConnection connection,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		Contract.EndContractBlock();

		SqlCommand command = connection.CreateCommand();

		// A command that cannot be set up is never returned, so nobody else can dispose it.
		try
		{
			command.CommandType = type;
			command.CommandText = commandText;
			command.CommandTimeout = secondsTimeout;
		}
		catch
		{
			command.Dispose();
			throw;
		}

		return command;
	}

	/// <summary>
	/// Creates a text <see cref="SqlCommand"/> from a connection.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-connection']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-text-command']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-returns']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	public static SqlCommand CreateTextCommand(this SqlConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="SqlCommand"/> from a connection.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-connection']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-returns']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='create-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static SqlCommand CreateStoredProcedureCommand(this SqlConnection connection,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
