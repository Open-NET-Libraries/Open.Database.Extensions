using static Open.Database.Extensions.SqlTransactionExtensions;

namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking transaction helpers on a <see cref="SqlConnection"/>: they hold the calling thread until the transaction ends.
/// Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// </summary>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
[Obsolete(SqlClientObsolete._message)]
public static class SyncSqlTransactionExtensions
{
	/// <summary>
	/// Begins a transaction before executing the action.
	/// Commits if there are no exceptions, the 'Commit' value from the action is true and the optional cancellation token has not been cancelled.
	/// Otherwise rolls-back the transaction.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The connection does not need to be opened beforehand.
	/// If it is not open, it is opened first and then closed (not disposed) when the transaction completes, even if it fails.
	/// A connection that was already open is left open.
	/// A failure while closing is ignored, as by then the transaction has already been committed or rolled back.
	/// To keep a connection open across several operations, open it before calling.
	/// </para>
	/// <para>
	/// A transaction that is not committed is rolled back by disposing it, which is the standard ADO.NET pattern.
	/// The .NET documentation for <see cref="System.Data.Common.DbTransaction.Dispose()"/> notes that this is provider specific.
	/// The transaction is always disposed before the connection is closed,
	/// as some providers only roll back on dispose while the connection is still open.
	/// The action should not commit or roll back the transaction itself; completion is handled here.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The value returned from the action.</typeparam>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="conditionalAction">
	/// The handler to execute while a transaction is pending.
	/// Returning a 'Commit' value of true signals to commit the transaction.
	/// </param>
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

		// Remember the prior state so a connection that is opened here is closed again here.
		ConnectionState priorState = connection.EnsureOpen();
		try
		{
			cancellationToken.ThrowIfCancellationRequested();

			// Disposing the transaction (at the end of this block, before the connection is closed)
			// is what rolls back anything that was not committed.
			using SqlTransaction transaction = connection.BeginTransaction(isolationLevel);
			(bool Commit, T Value) result = conditionalAction(transaction);
			cancellationToken.ThrowIfCancellationRequested();

			if (result.Commit) transaction.Commit();
			return result;
		}
		finally
		{
			RestoreConnection(connection, priorState);
		}
	}

	/// <summary>
	/// Begins a transaction before executing the action.
	/// Commits if there are no exceptions, the conditional action returns true, and the optional cancellation token is not cancelled.
	/// Otherwise rolls-back the transaction.
	/// </summary>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning true signals to commit the transaction.</param>
	/// <param name="isolationLevel">The isolation level for the transaction.</param>
	/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
	/// <returns>True if committed.</returns>
	public static bool ExecuteTransactionConditional(
		this SqlConnection connection,
		Func<SqlTransaction, bool> conditionalAction,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
		Contract.EndContractBlock();

		return connection.ExecuteTransactionConditional(
			isolationLevel, cancellationToken,
			t => (conditionalAction(t), true)).Commit;
	}

	/// <summary>
	/// Begins a transaction before executing the action.
	/// Commits if there are no exceptions and the optional provided token is not cancelled.
	/// Otherwise rolls-back the transaction.
	/// </summary>
	/// <typeparam name="T">The value returned from the action.</typeparam>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="action">The handler to execute while a transaction is pending.</param>
	/// <param name="isolationLevel">The isolation level for the transaction.</param>
	/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
	/// <returns>The value of the action.</returns>
	public static T ExecuteTransaction<T>(
		this SqlConnection connection,
		Func<SqlTransaction, T> action,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		return connection.ExecuteTransactionConditional(
			isolationLevel, cancellationToken,
			t => (true, action(t))).Value;
	}

	/// <summary>
	/// Begins a transaction before executing the action.
	/// Commits if there are no exceptions and the optional provided token is not cancelled.
	/// Otherwise rolls-back the transaction.
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
	{
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		connection.ExecuteTransaction(
			t => { action(t); return true; },
			isolationLevel, cancellationToken);
	}

	/// <inheritdoc cref="ExecuteTransactionConditional{T}(SqlConnection, Func{SqlTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this SqlConnection connection,
		Func<SqlTransaction, (bool Commit, T Value)> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional(SqlConnection, Func{SqlTransaction, bool}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static bool ExecuteTransactionConditional(
		this SqlConnection connection,
		Func<SqlTransaction, bool> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction{T}(SqlConnection, Func{SqlTransaction, T}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static T ExecuteTransaction<T>(
		this SqlConnection connection,
		Func<SqlTransaction, T> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(SqlConnection, Action{SqlTransaction}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static void ExecuteTransaction(
		this SqlConnection connection,
		Action<SqlTransaction> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions and 'Commit' value from the action is true.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <typeparam name="T">The value returned from the action.</typeparam>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="isolationLevel">The isolation level for the transaction.</param>
	/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
	/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
	/// <returns>The value returned from the conditional action.</returns>
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransactionConditional(SqlConnection, Func{SqlTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static bool ExecuteTransactionConditional(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, bool> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransaction{T}(SqlConnection, Func{SqlTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static T ExecuteTransaction<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, T> action)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransaction(SqlConnection, Action{SqlTransaction}, IsolationLevel, CancellationToken)"/>
	public static void ExecuteTransaction(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Action<SqlTransaction> action)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

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
}
