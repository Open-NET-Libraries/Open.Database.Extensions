namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

	/// <summary>
	/// Creates an <see cref="IDbCommand"/> from a connection with the command type, text, and timeout already set.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-connection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-text']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <returns>A new <see cref="IDbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	public static IDbCommand CreateCommand(this IDbConnection connection,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		Contract.EndContractBlock();

		IDbCommand command = connection.CreateCommand();

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
	/// Creates a text <see cref="IDbCommand"/> from a connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-connection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-text-command']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <returns>A new <see cref="IDbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	public static IDbCommand CreateTextCommand(this IDbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="IDbCommand"/> from a connection.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-connection']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-timeout']/*" />
	/// <returns>A new <see cref="IDbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <include file="Connections.docs.xml" path="docs/members[@name='create-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static IDbCommand CreateStoredProcedureCommand(this IDbConnection connection,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);

	/// <summary>
	/// Creates a <see cref="DbCommand"/> from a <see cref="DbConnection"/> with the command type, text, and timeout already set.
	/// </summary>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <inheritdoc cref="CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static DbCommand CreateCommand(this DbConnection connection,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		Contract.EndContractBlock();

		DbCommand command = connection.CreateCommand();

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
	/// Creates a text <see cref="DbCommand"/> from a <see cref="DbConnection"/>.
	/// </summary>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <inheritdoc cref="CreateTextCommand(IDbConnection, string, int)"/>
	public static DbCommand CreateTextCommand(this DbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="DbCommand"/> from a <see cref="DbConnection"/>.
	/// </summary>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <inheritdoc cref="CreateStoredProcedureCommand(IDbConnection, string, int)"/>
	public static DbCommand CreateStoredProcedureCommand(this DbConnection connection,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
