using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace Open.Database.Extensions.SqlClient
{
	public static class ConnectionExtensions
	{

		/// <summary>
		/// Shortcut for creating an SqlCommand from any SqlConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "No different that if manually inserted.")]
		public static SqlCommand CreateCommand(this SqlConnection connection,
			CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		{
			if (connection is null) throw new ArgumentNullException(nameof(connection));
			if (commandText is null) throw new ArgumentNullException(nameof(commandText));
			Contract.EndContractBlock();

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
			if (transaction is null) throw new System.ArgumentNullException(nameof(transaction));
			Contract.EndContractBlock();

			var command = transaction
				.Connection
				.CreateCommand(type, commandText, secondsTimeout);

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
