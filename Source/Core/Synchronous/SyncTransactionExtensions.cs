using static Open.Database.Extensions.TransactionExtensions;

namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking transaction helpers on a <see cref="DbConnection"/>: they hold the calling thread until the transaction ends.
/// </summary>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
public static class SyncTransactionExtensions
{
	/// <summary>
	/// Begins a transaction on the connection, invokes the action, and commits it when the action's <c>Commit</c> value is <see langword="true"/>,
	/// unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The handler to execute while the transaction is pending.
	/// Returning a <c>Commit</c> value of <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns>
	/// The value returned by <paramref name="conditionalAction"/>.
	/// Its <c>Commit</c> value is <see langword="true"/> if the transaction was committed.
	/// </returns>
	/// <remarks>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>.</exception>
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

		// Remember the prior state so a connection that is opened here is closed again here.
		ConnectionState priorState = connection.EnsureOpen();
		try
		{
			cancellationToken.ThrowIfCancellationRequested();

			// Disposing the transaction (at the end of this block, before the connection is closed)
			// is what rolls back anything that was not committed.
			using DbTransaction transaction = connection.BeginTransaction(isolationLevel);
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
	/// Begins a transaction on the connection, invokes the action, and commits it when the action returns <see langword="true"/>,
	/// unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The handler to execute while the transaction is pending.
	/// Returning <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns><see langword="true"/> if the transaction was committed; otherwise, <see langword="false"/>.</returns>
	/// <remarks>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>.</exception>
	public static bool ExecuteTransactionConditional(
		this DbConnection connection,
		Func<DbTransaction, bool> conditionalAction,
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
	/// Begins a transaction on the connection, invokes the action, and commits it
	/// unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The handler to execute while the transaction is pending.</param>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <remarks>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	public static T ExecuteTransaction<T>(
		this DbConnection connection,
		Func<DbTransaction, T> action,
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
	/// Begins a transaction on the connection, invokes the action, and commits it
	/// unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The handler to execute while the transaction is pending.</param>
	/// <remarks>
	/// <include file="../Extensions/Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	public static void ExecuteTransaction(
		this DbConnection connection,
		Action<DbTransaction> action,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		connection.ExecuteTransaction(
			t => { action(t); return true; },
			isolationLevel, cancellationToken);
	}

	/// <inheritdoc cref="ExecuteTransactionConditional{T}(DbConnection, Func{DbTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this DbConnection connection,
		Func<DbTransaction, (bool Commit, T Value)> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, Func{DbTransaction, bool}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static bool ExecuteTransactionConditional(
		this DbConnection connection,
		Func<DbTransaction, bool> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, Func{DbTransaction, T}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static T ExecuteTransaction<T>(
		this DbConnection connection,
		Func<DbTransaction, T> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(DbConnection, Action{DbTransaction}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static void ExecuteTransaction(
		this DbConnection connection,
		Action<DbTransaction> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional{T}(DbConnection, Func{DbTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, Func{DbTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static bool ExecuteTransactionConditional(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, bool> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, Func{DbTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static T ExecuteTransaction<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, T> action)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(DbConnection, Action{DbTransaction}, IsolationLevel, CancellationToken)"/>
	public static void ExecuteTransaction(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Action<DbTransaction> action)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional{T}(DbConnection, Func{DbTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, (bool Commit, T Value)> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, Func{DbTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static bool ExecuteTransactionConditional(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, bool> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, Func{DbTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static T ExecuteTransaction<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, T> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(DbConnection, Action{DbTransaction}, IsolationLevel, CancellationToken)"/>
	public static void ExecuteTransaction(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Action<DbTransaction> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);
}
