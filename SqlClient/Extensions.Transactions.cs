using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions.SqlClient
{
    // NOTE: This is simply a copy/paste of th IDb and Db extensions but replacing types with their Sql versions.
    // Why?  To ensure the Sql types are propagated through the type flow.

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
		/// <returns>The value retured from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
		{
            if (conditionalAction == null) throw new ArgumentNullException(nameof(conditionalAction));
            Contract.EndContractBlock();

            var t = token ?? CancellationToken.None;
			t.ThrowIfCancellationRequested();

			var success = false;
			SqlTransaction transaction = null;

			connection.EnsureOpen();
			t.ThrowIfCancellationRequested();

			try
			{
				transaction = connection.BeginTransaction(isolationLevel);
				var result = conditionalAction(transaction);
				t.ThrowIfCancellationRequested();
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
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Func<SqlTransaction, bool> conditionalAction)
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
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Func<SqlTransaction, T> action)
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
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Action<SqlTransaction> action)
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
		public static async Task<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Func<SqlTransaction, Task<(bool Commit, T Value)>> conditionalAction)
        {
            if (conditionalAction == null) throw new ArgumentNullException(nameof(conditionalAction));
            Contract.EndContractBlock();

            var t = token ?? CancellationToken.None;
			t.ThrowIfCancellationRequested();

			var success = false;
			SqlTransaction transaction = null;

			// Only await if needed...
			var state = await connection.EnsureOpenAsync(t); // If the task is cancelled, awaiting will throw.

			try
			{
				t.ThrowIfCancellationRequested();

				transaction = connection.BeginTransaction(isolationLevel);
				var result = await conditionalAction(transaction).ConfigureAwait(false); // If the task is cancelled, awaiting will throw.
				t.ThrowIfCancellationRequested();
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
				if (state == ConnectionState.Closed)
					connection.Close();
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
		public static async Task<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Func<SqlTransaction, Task<bool>> conditionalAction)
			=> (await connection.ExecuteTransactionConditionalAsync(
				isolationLevel, token, async t => (await conditionalAction(t).ConfigureAwait(false), true)).ConfigureAwait(false)).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async Task<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Func<SqlTransaction, Task<T>> action)
			=> (await connection.ExecuteTransactionConditionalAsync(isolationLevel, token, async t => (true, await action(t).ConfigureAwait(false))).ConfigureAwait(false)).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static Task ExecuteTransactionAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken? token,
			Func<SqlTransaction, Task> action)
			=> connection.ExecuteTransactionAsync(isolationLevel, token, async c => { await action(c).ConfigureAwait(false); return true; });

		#region Overloads

		#region No Token
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value retured from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(isolationLevel, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(isolationLevel, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, T> action)
			=> connection.ExecuteTransaction(isolationLevel, null, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static void ExecuteTransaction(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Action<SqlTransaction> action)
			=> connection.ExecuteTransaction(isolationLevel, null, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, Task<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(isolationLevel, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, Task<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(isolationLevel, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, Task<T>> action)
			=> connection.ExecuteTransactionAsync(isolationLevel, null, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static Task ExecuteTransactionAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, Task> action)
			=> connection.ExecuteTransactionAsync(isolationLevel, null, action);

		#endregion

		#region Unspecified Isolation Level
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value retured from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			CancellationToken? token,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(IsolationLevel.Unspecified, token, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the conditional action returns true, and the optional cancellation token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			CancellationToken? token,
			Func<SqlTransaction, bool> conditionalAction)
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
			this SqlConnection connection,
			CancellationToken? token,
			Func<SqlTransaction, T> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, token, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static void ExecuteTransaction(
			this SqlConnection connection,
			CancellationToken? token,
			Action<SqlTransaction> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, token, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			CancellationToken? token,
			Func<SqlTransaction, Task<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, token, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			CancellationToken? token,
			Func<SqlTransaction, Task<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, token, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			CancellationToken? token,
			Func<SqlTransaction, Task<T>> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, token, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static Task ExecuteTransactionAsync(
			this SqlConnection connection,
			CancellationToken? token,
			Func<SqlTransaction, Task> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, token, action);

		#endregion

		#region Defaults Only
		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value retured from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> connection.ExecuteTransactionConditional(IsolationLevel.Unspecified, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			Func<SqlTransaction, bool> conditionalAction)
			=> connection.ExecuteTransactionConditional(IsolationLevel.Unspecified, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			Func<SqlTransaction, T> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, null, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static void ExecuteTransaction(
			this SqlConnection connection,
			Action<SqlTransaction> action)
			=> connection.ExecuteTransaction(IsolationLevel.Unspecified, null, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			Func<SqlTransaction, Task<(bool Commit, T Value)>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			Func<SqlTransaction, Task<bool>> conditionalAction)
			=> connection.ExecuteTransactionConditionalAsync(IsolationLevel.Unspecified, null, conditionalAction);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static Task<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			Func<SqlTransaction, Task<T>> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, null, action);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static Task ExecuteTransactionAsync(
			this SqlConnection connection,
			Func<SqlTransaction, Task> action)
			=> connection.ExecuteTransactionAsync(IsolationLevel.Unspecified, null, action);

		#endregion

		#endregion
	}

}
