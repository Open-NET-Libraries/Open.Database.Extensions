using static Open.Database.Extensions.SqlTransactionExtensions;

namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking transaction helpers on a <see cref="SqlConnection"/>: they hold the calling thread until the transaction ends.
/// Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// </summary>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
public static class SyncSqlTransactionExtensions
{
	/// <summary>
	/// Runs the action in a transaction on the connection, committing it unless the action throws, its <c>Commit</c> value is <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The handler to execute while the transaction is pending.
	/// Returning a <c>Commit</c> value of <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns>
	/// The value returned by <paramref name="conditionalAction"/>.
	/// Its <c>Commit</c> value is <see langword="true"/> if the transaction was committed.
	/// </returns>
	/// <remarks>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>.</exception>
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
	/// Runs the action in a transaction on the connection, committing it unless the action throws, returns <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The handler to execute while the transaction is pending.
	/// Returning <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns><see langword="true"/> if the transaction was committed; otherwise, <see langword="false"/>.</returns>
	/// <remarks>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>.</exception>
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
	/// Runs the action in a transaction on the connection, committing it unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The handler to execute while the transaction is pending.</param>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <remarks>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
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
	/// Runs the action in a transaction on the connection, committing it unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The handler to execute while the transaction is pending.</param>
	/// <remarks>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
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

	/// <inheritdoc cref="ExecuteTransactionConditional{T}(SqlConnection, Func{SqlTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, (bool Commit, T Value)> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional(SqlConnection, Func{SqlTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static bool ExecuteTransactionConditional(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, bool> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction{T}(SqlConnection, Func{SqlTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static T ExecuteTransaction<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, T> action)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(SqlConnection, Action{SqlTransaction}, IsolationLevel, CancellationToken)"/>
	public static void ExecuteTransaction(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Action<SqlTransaction> action)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional{T}(SqlConnection, Func{SqlTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, (bool Commit, T Value)> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional(SqlConnection, Func{SqlTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static bool ExecuteTransactionConditional(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, bool> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction{T}(SqlConnection, Func{SqlTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static T ExecuteTransaction<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, T> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(SqlConnection, Action{SqlTransaction}, IsolationLevel, CancellationToken)"/>
	public static void ExecuteTransaction(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Action<SqlTransaction> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);
}
