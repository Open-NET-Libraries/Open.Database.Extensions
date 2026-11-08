namespace Open.Database.Extensions;

/// <summary>
/// Extensions for SqlConnections.
/// </summary>
public static partial class SqlConnectionExtensions
{
	const string EmptyOrWhiteSpace = "Command is empty or whitespace.";

	/// <summary>
	/// Creates a <see cref="SqlCommand"/> from a connection with the command type, text, and timeout already set.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="type">The command type: <see cref="CommandType.Text"/>, <see cref="CommandType.StoredProcedure"/>, or <see cref="CommandType.TableDirect"/>.</param>
	/// <param name="commandText">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="SqlCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <remarks>
	/// The connection is not opened by this method; open it (see <see cref="ConnectionExtensions.EnsureOpen(IDbConnection)"/>) before executing the command.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="commandText"/> is empty or white space.</exception>
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
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="commandText">The command text to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="SqlCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="commandText"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="commandText"/> is empty or white space.</exception>
	public static SqlCommand CreateTextCommand(this SqlConnection connection,
		string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

	/// <summary>
	/// Creates a stored procedure <see cref="SqlCommand"/> from a connection.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="procedureName">The stored procedure name to use.</param>
	/// <param name="secondsTimeout">The number of seconds to wait before the command times out. The default is 60 seconds.</param>
	/// <returns>A new <see cref="SqlCommand"/> that the caller owns and disposes. It is not associated with a transaction.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static SqlCommand CreateStoredProcedureCommand(this SqlConnection connection,
		string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);
}
