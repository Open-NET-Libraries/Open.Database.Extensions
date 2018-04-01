using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace Open.Database.Extensions.SqlClient
{
	public static partial class Extensions
	{

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
			var command = connection.CreateCommand();
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
			var command = transaction.Connection.CreateCommand(CommandType.StoredProcedure, commandText, secondsTimeout);
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
}
