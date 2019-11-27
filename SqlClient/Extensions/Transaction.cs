using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace Open.Database.Extensions.SqlClient
{
	// NOTE: This is simply a copy/paste of th IDb and Db extensions but replacing types with their Sql versions.
	// Why?  To ensure the Sql types are propagated through the type flow.

	public static partial class SqlTransactionExtensions
	{

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
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
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>True if committed.</returns>
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			Func<SqlTransaction, bool> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> ExecuteTransactionConditional(connection,
				t => (conditionalAction(t), true),
				isolationLevel, cancellationToken).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the action.</returns>
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			Func<SqlTransaction, T> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> ExecuteTransactionConditional(connection,
				t => (true, action(t)),
				isolationLevel, cancellationToken).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static void ExecuteTransaction(
			this SqlConnection connection,
			Action<SqlTransaction> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> ExecuteTransaction(connection,
				t => { action(t); return true; },
				isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
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
			var state = await connection.EnsureOpenAsync(cancellationToken); // If the task is cancelled, awaiting will throw.

			try
			{
				cancellationToken.ThrowIfCancellationRequested();

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
				if (state == ConnectionState.Closed)
					connection.Close();
			}
		}

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> (await ExecuteTransactionConditionalAsync(connection,
				async t => (await conditionalAction(t).ConfigureAwait(false), true),
				isolationLevel, cancellationToken).ConfigureAwait(false)).Commit;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		/// <returns>The value of the awaited action.</returns>
		public static async ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			Func<SqlTransaction, ValueTask<T>> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> (await ExecuteTransactionConditionalAsync(connection,
				async t => (true, await action(t)),
				isolationLevel, cancellationToken)).Value;

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">An optional token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
		public static async ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			Func<SqlTransaction, ValueTask> action,
			IsolationLevel isolationLevel = IsolationLevel.Unspecified,
			CancellationToken cancellationToken = default)
			=> await ExecuteTransactionAsync(connection,
				async c => { await action(c); return true; },
				isolationLevel,
				cancellationToken);


		#region Overloads

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> ExecuteTransactionConditional(connection, conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, bool> conditionalAction)
			=> ExecuteTransactionConditional(connection, conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, T> action)
			=> ExecuteTransaction(connection, action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static void ExecuteTransaction(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Action<SqlTransaction> action)
			=> ExecuteTransaction(connection, action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> ExecuteTransactionConditionalAsync(connection, conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction)
			=> ExecuteTransactionConditionalAsync(connection, conditionalAction, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<T>> action)
			=> ExecuteTransactionAsync(connection, action, isolationLevel, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask> action)
			=> ExecuteTransactionAsync(connection, action, isolationLevel, cancellationToken);

		#region Unspecified Isolation Level

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value returned from the conditional action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> ExecuteTransactionConditional(connection, conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the conditional action returns true, and the optional cancellation token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>True if committed.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static bool ExecuteTransactionConditional(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, bool> conditionalAction)
			=> ExecuteTransactionConditional(connection, conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static T ExecuteTransaction<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, T> action)
			=> ExecuteTransaction(connection, action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static void ExecuteTransaction(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Action<SqlTransaction> action)
			=> ExecuteTransaction(connection, action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> ExecuteTransactionConditionalAsync(connection, conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions, the value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction)
			=> ExecuteTransactionConditionalAsync(connection, conditionalAction, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask<T>> action)
			=> ExecuteTransactionAsync(connection, action, IsolationLevel.Unspecified, cancellationToken);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="cancellationToken">The token that if cancelled may cause this transaction to be aborted or rolled-back.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Provided for aesthetic convenience.")]
		public static ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			CancellationToken cancellationToken,
			Func<SqlTransaction, ValueTask> action)
			=> ExecuteTransactionAsync(connection, action, IsolationLevel.Unspecified, cancellationToken);

		#endregion

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
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
			=> ExecuteTransactionConditional(connection, conditionalAction, isolationLevel);

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
			=> ExecuteTransactionConditional(connection, conditionalAction, isolationLevel);

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
			=> ExecuteTransaction(connection, action, isolationLevel);

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
			=> ExecuteTransaction(connection, action, isolationLevel);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
			=> ExecuteTransactionConditionalAsync(connection, conditionalAction, isolationLevel);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<bool> ExecuteTransactionConditionalAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask<bool>> conditionalAction)
			=> ExecuteTransactionConditionalAsync(connection, conditionalAction, isolationLevel);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <typeparam name="T">The value returned from the action.</typeparam>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		/// <returns>The value of the awaited action.</returns>
		public static ValueTask<T> ExecuteTransactionAsync<T>(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask<T>> action)
			=> ExecuteTransactionAsync(connection, action, isolationLevel);

		/// <summary>
		/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="isolationLevel">The isolation level for the transaction.</param>
		/// <param name="action">The handler to execute while a transaction is pending.</param>
		public static ValueTask ExecuteTransactionAsync(
			this SqlConnection connection,
			IsolationLevel isolationLevel,
			Func<SqlTransaction, ValueTask> action)
			=> ExecuteTransactionAsync(connection, action, isolationLevel);

		#endregion

		#endregion
	}

}
