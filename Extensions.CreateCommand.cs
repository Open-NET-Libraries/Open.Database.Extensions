using System.Data;
using System.Data.Common;

namespace Open.Database.Extensions
{
	public static partial class Extensions
    {
		/// <summary>
		/// Shortcut for creating an IDbCommand from any IDbConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		public static IDbCommand CreateCommand(this IDbConnection connection,
			CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		{
			var command = connection.CreateCommand();
			command.CommandType = type;
			command.CommandText = commandText;
			command.CommandTimeout = secondsTimeout;

			return command;
		}

		/// <summary>
		/// Shortcut for creating a text IDbCommand from any IDbConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		public static IDbCommand CreateTextCommand(this IDbConnection connection,
			string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
			=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

		/// <summary>
		/// Shortcut for creating an IDbCommand from any IDbConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		public static IDbCommand CreateStoredProcedureCommand(this IDbConnection connection,
			string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
			=> connection.CreateCommand(CommandType.StoredProcedure, commandText, secondsTimeout);

		/// <summary>
		/// Shortcut for creating an DbCommand from any DbConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		public static DbCommand CreateCommand(this DbConnection connection,
			CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		{
			var command = connection.CreateCommand();
			command.CommandType = type;
			command.CommandText = commandText;
			command.CommandTimeout = secondsTimeout;

			return command;
		}

		/// <summary>
		/// Shortcut for creating a text DbCommand from any DbConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		public static DbCommand CreateTextCommand(this DbConnection connection,
			string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
			=> connection.CreateCommand(CommandType.Text, commandText, secondsTimeout);

		/// <summary>
		/// Shortcut for creating a stored procedure DbCommand from any DbConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="procedureName">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		public static DbCommand CreateStoredProcedureCommand(this DbConnection connection,
			string procedureName, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
			=> connection.CreateCommand(CommandType.StoredProcedure, procedureName, secondsTimeout);

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
			var command = transaction.Connection.CreateCommand(CommandType.StoredProcedure, commandText, secondsTimeout);
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
			var command = transaction.Connection.CreateCommand(CommandType.StoredProcedure, commandText, secondsTimeout);
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
}
