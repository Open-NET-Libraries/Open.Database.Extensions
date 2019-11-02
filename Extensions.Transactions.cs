using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions
{
	public static partial class Extensions
	{
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
		{
			if (conditionalAction == null) throw new ArgumentNullException(nameof(conditionalAction));
			Contract.EndContractBlock();

			token.ThrowIfCancellationRequested();

			var success = false;
			DbTransaction? transaction = null;

			connection.EnsureOpen();
			token.ThrowIfCancellationRequested();

			try
			{
				transaction = connection.BeginTransaction(isolationLevel);
				var result = conditionalAction(transaction);
				token.ThrowIfCancellationRequested();
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
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Func<DbTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(
				isolationLevel, token, t => (conditionalAction(t), true)).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Func<DbTransaction, T> action)
			=> connection.ExecuteTransactionConditional(
				isolationLevel, token, t => (true, action(t))).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static void ExecuteTransaction(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Action<DbTransaction> action)
			=> connection.ExecuteTransaction(isolationLevel, token, t => { action(t); return true; });

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
		{
			if (conditionalAction == null) throw new ArgumentNullException(nameof(conditionalAction));
			Contract.EndContractBlock();

			token.ThrowIfCancellationRequested();

			var success = false;
			DbTransaction? transaction = null;

			// Only await if needed...
			if (connection.State != ConnectionState.Open)
			{
				await connection.EnsureOpenAsync(token); // If the task is cancelled, awaiting will throw.
				token.ThrowIfCancellationRequested();
			}

			try
			{
				transaction = connection.BeginTransaction(isolationLevel);
				var result = await conditionalAction(transaction).ConfigureAwait(false); // If the task is cancelled, awaiting will throw.
				token.ThrowIfCancellationRequested();
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
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<bool> ExecuteTransactionConditionalAsync(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Func<DbTransaction, ValueTask<bool>> conditionalAction)
			=> (await connection.ExecuteTransactionConditionalAsync(
				isolationLevel, token, async t => (await conditionalAction(t), true))).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<T> ExecuteTransactionAsync<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Func<DbTransaction, ValueTask<T>> action)
			=> (await connection.ExecuteTransactionConditionalAsync(isolationLevel, token, async t => (true, await action(t))).ConfigureAwait(false)).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static async ValueTask ExecuteTransactionAsync(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken token,
			Func<DbTransaction, ValueTask> action)
			=> await connection.ExecuteTransactionAsync(isolationLevel, token, async c => { await action(c); return true; });

		#region Overloads

		#region No Token
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(isolationLevel, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(isolationLevel, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, T> action)
			=> connection.ExecuteTransaction(isolationLevel, default, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static void ExecuteTransaction(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Action<DbTransaction> action)
			=> connection.ExecuteTransaction(isolationLevel, default, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(isolationLevel, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(isolationLevel, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask<T>> action)
			=> connection.ExecuteTransactionAsync(isolationLevel, default, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static ValueTask ExecuteTransactionAsync(
			this DbConnection connection,
			IsolationLevel isolationLevel,
			Func<DbTransaction, ValueTask> action)
			=> connection.ExecuteTransactionAsync(isolationLevel, default, action);

		#endregion

		#region Unspecified Isolation Level
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this DbConnection connection,
			CancellationToken token,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(IsolationLevel.Unspecified, token, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the conditional action returns true, and the optional cancellation token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this DbConnection connection,
			CancellationToken token,
			Func<DbTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(IsolationLevel.Unspecified, token, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this DbConnection connection,
			CancellationToken token,
			Func<DbTransaction, T> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, token, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static void ExecuteTransaction(
			this DbConnection connection,
			CancellationToken token,
			Action<DbTransaction> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, token, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this DbConnection connection,
			CancellationToken token,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, token, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this DbConnection connection,
			CancellationToken token,
			Func<DbTransaction, ValueTask<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, token, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this DbConnection connection,
			CancellationToken token,
			Func<DbTransaction, ValueTask<T>> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, token, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static ValueTask ExecuteTransactionAsync(
			this DbConnection connection,
			CancellationToken token,
			Func<DbTransaction, ValueTask> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, token, action);

		#endregion

		#region Defaults Only
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this DbConnection connection,
			Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(IsolationLevel.Unspecified, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this DbConnection connection,
			Func<DbTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(IsolationLevel.Unspecified, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this DbConnection connection,
			Func<DbTransaction, T> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, default, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static void ExecuteTransaction(
			this DbConnection connection,
			Action<DbTransaction> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, default, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this DbConnection connection,
			Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this DbConnection connection,
			Func<DbTransaction, ValueTask<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, default, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this DbConnection connection,
			Func<DbTransaction, ValueTask<T>> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, default, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static ValueTask ExecuteTransactionAsync(
			this DbConnection connection,
			Func<DbTransaction, ValueTask> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, default, action);

		#endregion

		#endregion
	}
}
