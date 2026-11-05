namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

	/// <summary>
	/// Creates an <see cref="IDbCommand"/> from a connection with the command type, text, and timeout already set.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="type">The command type: <see cref="CommandType.Text"/>, <see cref="CommandType.StoredProcedure"/>, or <see cref="CommandType.TableDirect"/>.</param>
	/// <param name="commandText">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="IDbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <remarks>
	/// The connection is not opened by this method; open it (see <see cref="EnsureOpen(IDbConnection)"/>) before executing the command.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="commandText"/> is empty or white space.</exception>
	public static IDbCommand CreateCommand(this IDbConnection connection,
		CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (commandText is null) throw new ArgumentNullException(nameof(commandText));
		if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException(EmptyOrWhiteSpace, nameof(commandText));
		Contract.EndContractBlock();

		IDbCommand command = connection.CreateCommand();
		command.CommandType = type;
		command.CommandText = commandText;
		command.CommandTimeout = secondsTimeout;
		return command;
	}

	/// <summary>
	/// Creates a text <see cref="IDbCommand"/> from a connection.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="commandText">The command text to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <inheritdoc cref="CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static IDbCommand CreateTextCommand(this IDbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="IDbCommand"/> from a connection.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="commandText">The stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <inheritdoc cref="CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static IDbCommand CreateStoredProcedureCommand(this IDbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, commandText, secondsTimeout);

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
		command.CommandType = type;
		command.CommandText = commandText;
		command.CommandTimeout = secondsTimeout;

		return command;
	}

	/// <summary>
	/// Creates a text <see cref="DbCommand"/> from a <see cref="DbConnection"/>.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="commandText">The command text to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <inheritdoc cref="CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static DbCommand CreateTextCommand(this DbConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="DbCommand"/> from a <see cref="DbConnection"/>.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="DbCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <inheritdoc cref="CreateCommand(IDbConnection, CommandType, string, int)"/>
	public static DbCommand CreateStoredProcedureCommand(this DbConnection connection,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
