using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	public static partial class Extensions
	{

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value retured from the conditional action.</returns>
		public static (bool Commit, T Value) ExecuteTransactionConditional<TConn, T>(
			this TConn connection, Func<TConn, (bool Commit, T Value)> conditionalAction, CancellationToken? token = null)
			where TConn : DbConnection
		{
			var t = token ?? CancellationToken.None;
			t.ThrowIfCancellationRequested();

			var success = false;
			IDbTransaction transaction = null;

			connection.EnsureOpen();
			t.ThrowIfCancellationRequested();

			try
			{
				transaction = connection.BeginTransaction();
				var result = conditionalAction(connection);
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
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional actio returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional<TConn>(
			this TConn connection, Func<TConn, bool> conditionalAction, CancellationToken? token = null)
			where TConn : DbConnection
			=> connection.ExecuteTransactionConditional(c => (conditionalAction(c), true), token).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<TConn, T>(
			this TConn connection, Func<TConn, T> action, CancellationToken? token = null)
			where TConn : DbConnection
			=> connection.ExecuteTransactionConditional(c => (true, action(c)), token).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static void ExecuteTransaction<TConn>(
			this TConn connection, Action<TConn> action, CancellationToken? token = null)
			where TConn : DbConnection
			=> connection.ExecuteTransaction(c => { action(c); return true; }, token);


		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async Task<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<TConn, T>(
			this TConn connection, Func<TConn, Task<(bool Commit, T Value)>> conditionalAction, CancellationToken? token = null)
			where TConn : DbConnection
		{
			var t = token ?? CancellationToken.None;
			t.ThrowIfCancellationRequested();

			var success = false;
			IDbTransaction transaction = null;

			await connection.EnsureOpenAsync(t); // If the task is cancelled, awaiting will throw.
			t.ThrowIfCancellationRequested();

			try
			{
				transaction = connection.BeginTransaction();
				var result = await conditionalAction(connection); // If the task is cancelled, awaiting will throw.
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
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async Task<bool> ExecuteTransactionConditionalAsync<TConn, T>(
			this TConn connection, Func<TConn, Task<bool>> conditionalAction, CancellationToken? token = null)
			where TConn : DbConnection
			=> (await connection.ExecuteTransactionConditionalAsync(async c => (await conditionalAction(c), true), token)).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async Task<T> ExecuteTransactionAsync<TConn, T>(
			this TConn connection, Func<TConn, Task<T>> action, CancellationToken? token = null)
			where TConn : DbConnection
			=> (await connection.ExecuteTransactionConditionalAsync(async c => (true, await action(c)), token)).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="TConn">The connection type.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="token">A optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static Task ExecuteTransactionAsync<TConn>(
			this TConn connection, Func<TConn, Task> action, CancellationToken? token = null)
			where TConn : DbConnection
			=> connection.ExecuteTransactionAsync(async c => { await action(c); return true; }, token);

	}
}
