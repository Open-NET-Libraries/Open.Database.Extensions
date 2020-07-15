using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace Open.Database.Extensions
{
	// NOTE: This is simply a copy/paste of th IDb and Db extensions but replacing types with their Sql versions.
	// Why?  To ensure the Sql types are propagated through the type flow.

	/// <summary>
	/// System.Data.SqlClient specific extensions for database transactions.
	/// </summary>
	public static partial class SqlTransactionExtensions
	{
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value returned from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
		{
			if (connection is null) throw new ArgumentNullException(nameof(connection));
			if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
			Contract.EndContractBlock();

			cancellationToken.ThrowIfCancellationRequested();

			var success = false;
			SqlTransaction? transaction = null;

			connection.EnsureOpen();
			cancellationToken.ThrowIfCancellationRequested();

			try
			{
				transaction = connection.BeginTransaction(isolationLevel);
				var result = conditionalAction(transaction);
				cancellationToken.ThrowIfCancellationRequested();
				success = result.Commit;
				return result;
			}
			finally
			{
				if (transaction != null) // Just in case acquiring a transaction fails.
				{
					if (success) transaction.Commit();
					else transaction.Rollback();
				}
			}
		}

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the conditional action returns true, and the optional cancellation token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			Func<SqlTransaction, bool> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionConditional(
				isolationLevel, cancellationToken, t => (conditionalAction(t), true)).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			Func<SqlTransaction, T> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionConditional(
				isolationLevel, cancellationToken, t => (true, action(t))).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static void ExecuteTransaction(
			this SqlConnection connection,
			Action<SqlTransaction> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransaction(t => { action(t); return true; }, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
		{
			if (connection is null) throw new ArgumentNullException(nameof(connection));
			if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
			Contract.EndContractBlock();

			cancellationToken.ThrowIfCancellationRequested();

			var success = false;
			SqlTransaction? transaction = null;

			// Only await if needed...
			if (connection.State != ConnectionState.Open)
			{
				await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(true); // If the task is cancelled, awaiting will throw.
				cancellationToken.ThrowIfCancellationRequested();
			}

			try
			{
				transaction = connection.BeginTransaction(isolationLevel);
				var result = await conditionalAction(transaction).ConfigureAwait(false); // If the task is cancelled, awaiting will throw.
				cancellationToken.ThrowIfCancellationRequested();
				success = result.Commit;
				return result;
			}
			finally
			{
				if (transaction != null) // Just in case acquiring a transaction fails.
				{
					if (success) transaction.Commit();
					else transaction.Rollback();
				}
			}
		}

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> (await connection.ExecuteTransactionConditionalAsync(
				async t => (await conditionalAction(t).ConfigureAwait(false), true), isolationLevel, cancellationToken).ConfigureAwait(false)).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			Func<SqlTransaction, ValueTask<T>> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> (await connection.ExecuteTransactionConditionalAsync(async t => (true, await action(t).ConfigureAwait(false)), isolationLevel, cancellationToken).ConfigureAwait(false)).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static async ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			Func<SqlTransaction, ValueTask> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> await connection.ExecuteTransactionAsync(async c => { await action(c).ConfigureAwait(false); return true; }, isolationLevel, cancellationToken).ConfigureAwait(false);

		#region Overloads


		#region No optional params
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, T> action)
			=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static void ExecuteTransaction(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Action<SqlTransaction> action)
			=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<T>> action)
			=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask> action)
			=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

		#endregion

		#region Optional Token
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value returned from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, bool> conditionalAction,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, T> action,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static void ExecuteTransaction(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Action<SqlTransaction> action,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask<T>> action,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask> action,
			CancellationToken cancellationToken = default)
			=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);
		#endregion

		#region Unspecified Isolation Level
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the conditional action returns true, and the optional cancellation token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, T> action)
			=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static void ExecuteTransaction(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Action<SqlTransaction> action)
			=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<T>> action)
			=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask> action)
			=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

		#endregion

		#endregion
	}

}
