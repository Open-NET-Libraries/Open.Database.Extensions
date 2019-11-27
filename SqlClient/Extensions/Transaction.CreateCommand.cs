using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace Open.Database.Extensions.SqlClient
{
	public static partial class SqlTransactionExtensions
	{
		/// <summary>
		/// Shortcut for creating an SqlCommand from any SqlTransaction.
		/// </summary>
		/// <param name="transaction">The transaction to create a command from.</param>
		/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created SqlCommand.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Globalization", "CA1303:Do not pass literals as localized parameters", Justification = "Locally only used once.")]
		public static SqlCommand CreateCommand(this SqlTransaction transaction,
			CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		{
			if (transaction is null) throw new ArgumentNullException(nameof(transaction));
			if (commandText is null) throw new ArgumentNullException(nameof(commandText));
			if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentException("Command is empty or whitespace.", nameof(commandText));
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
