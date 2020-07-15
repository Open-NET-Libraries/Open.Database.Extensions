using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	public static partial class TransactionExtensions
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
			this DbConnection connection,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
		{
			if (connection is null) throw new ArgumentNullException(nameof(connection));
			if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
			Contract.EndContractBlock();

			cancellationToken.ThrowIfCancellationRequested();

			var success = false;
			DbTransaction? transaction = null;

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
			this DbConnection connection,
			Func<DbTransaction, bool> conditionalAction,
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
			this DbConnection connection,
			Func<DbTransaction, T> action,
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
			this DbConnection connection,
			Action<DbTransaction> action,
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
			this DbConnection connection,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
		{
			if (connection is null) throw new ArgumentNullException(nameof(connection));
			if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
			Contract.EndContractBlock();

			cancellationToken.ThrowIfCancellationRequested();

			var success = false;
			DbTransaction? transaction = null;

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
			this DbConnection connection,
			Func<DbTransaction, ValueTask<bool>> conditionalAction,
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
			this DbConnection connection,
			Func<DbTransaction, ValueTask<T>> action,
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
			this DbConnection connection,
			Func<DbTransaction, ValueTask> action,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<DbTransaction, bool> conditionalAction)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<DbTransaction, T> action)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Action<DbTransaction> action)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask<bool>> conditionalAction)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask<T>> action)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask> action)
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, bool> conditionalAction,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, T> action,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Action<DbTransaction> action,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask<bool>> conditionalAction,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask<T>> action,
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
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask> action,
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
			this DbConnection connection,
			CancellationToken cancellationToken,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
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
			this DbConnection connection,
			CancellationToken cancellationToken,
			Func<DbTransaction, bool> conditionalAction)
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
			this DbConnection connection,
			CancellationToken cancellationToken,
			Func<DbTransaction, T> action)
			=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static void ExecuteTransaction(
			this DbConnection connection,
			CancellationToken cancellationToken,
			Action<DbTransaction> action)
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
			this DbConnection connection,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
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
			this DbConnection connection,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask<bool>> conditionalAction)
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
			this DbConnection connection,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask<T>> action)
			=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
		public static ValueTask ExecuteTransactionAsync(
			this DbConnection connection,
			CancellationToken cancellationToken,
			Func<DbTransaction, ValueTask> action)
			=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

		#endregion

		#endregion
	}
}
