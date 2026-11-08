namespace Open.Database.Extensions;

[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
public static partial class TransactionExtensions
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
	/// The .NET documentation for <see cref="DbTransaction.Dispose()"/> notes that this is provider specific.
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

	/// <inheritdoc cref="ExecuteTransactionConditional{T}(DbConnection, Func{DbTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		Func<DbTransaction, CancellationToken, ValueTask<(bool Commit, T Value)>> conditionalAction,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
		Contract.EndContractBlock();

		// Remember the prior state so a connection that is opened here is closed again here.
		ConnectionState priorState = await connection.EnsureOpenAsync(cancellationToken);
		try
		{
			// Disposing the transaction (at the end of this block, before the connection is closed)
			// is what rolls back anything that was not committed.
#if NETSTANDARD2_0
			using DbTransaction transaction = connection.BeginTransaction(isolationLevel);
#else
			await using DbTransaction transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken);
#endif
			(bool Commit, T Value) result = await conditionalAction(transaction, cancellationToken);

			// Same contract as the synchronous path: a cancelled token never commits.
			cancellationToken.ThrowIfCancellationRequested();

			if (result.Commit)
			{
				// The commit is the point of no return: the token was checked just above, and cancelling
				// part-way through would leave the outcome unknown.
#if NETSTANDARD2_0
				transaction.Commit();
#else
				await transaction.CommitAsync(CancellationToken.None);
#endif
			}

			return result;
		}
		finally
		{
			await RestoreConnectionAsync(connection, priorState);
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
	/// Begins a transaction before executing the action.
	/// Commits if there are no exceptions and the optional provided token is not cancelled.
	/// Otherwise rolls-back the transaction.
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
	{
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		connection.ExecuteTransaction(
			t => { action(t); return true; },
			isolationLevel, cancellationToken);
	}

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <typeparam name="T">The value returned from the action.</typeparam>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
	/// <param name="isolationLevel">The isolation level for the transaction.</param>
	/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
	/// <returns>The value of the awaited action.</returns>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
		Contract.EndContractBlock();

		return connection.ExecuteTransactionConditionalAsync(
			(db, _) => conditionalAction(db),
			isolationLevel, cancellationToken);
	}

	/// <summary>
	/// Begins a transaction before executing the action.
	/// Commits if there are no exceptions, the 'Commit' value from the action is true, and the optional provided token is not cancelled.
	/// Otherwise rolls-back the transaction.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, Func{DbTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		Func<DbTransaction, ValueTask<bool>> conditionalAction,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
		Contract.EndContractBlock();

		return (await connection
			.ExecuteTransactionConditionalAsync(
				async t => (await conditionalAction(t).ConfigureAwait(false), true),
				isolationLevel, cancellationToken)
			.ConfigureAwait(false)).Commit;
	}

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, Func{DbTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		Func<DbTransaction, ValueTask<T>> action,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		return (await connection
			.ExecuteTransactionConditionalAsync(
				async t => (true, await action(t).ConfigureAwait(false)),
				isolationLevel, cancellationToken)
			.ConfigureAwait(false)).Value;
	}

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions and the optional provided token is not cancelled.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransaction(DbConnection, Action{DbTransaction}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask ExecuteTransactionAsync(
		this DbConnection connection,
		Func<DbTransaction, ValueTask> action,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (action is null) throw new ArgumentNullException(nameof(action));
		Contract.EndContractBlock();

		await connection
			.ExecuteTransactionAsync(
				async c => { await action(c).ConfigureAwait(false); return true; },
				isolationLevel, cancellationToken)
			.ConfigureAwait(false);
	}

	#region Restoring the connection

	// Closes (never disposes) a connection that was not open when the operation began, so the caller's
	// connection is left the way it was found. By the time this runs the transaction has already been disposed:
	// that ordering matters because some providers (e.g. Microsoft.Data.Sqlite, MySqlConnector) only roll back
	// an uncommitted transaction on dispose while the connection is still open.
	// A failure to close is deliberately ignored. By then the transaction has already been committed or rolled back,
	// so it must neither turn a completed operation into an error nor replace the exception of a failed one.
	// Only a connection that was already Open belongs to the caller. One that was Closed, or Broken and therefore
	// repaired by EnsureOpen, is a connection this operation opened, so it is closed again here.
	static void RestoreConnection(DbConnection connection, ConnectionState priorState)
	{
		if (priorState.HasFlag(ConnectionState.Open))
			return;

		try
		{
			connection.Close();
		}
		catch
		{
			// Intentionally ignored; see above.
		}
	}

#if NETSTANDARD2_0
	static ValueTask RestoreConnectionAsync(DbConnection connection, ConnectionState priorState)
	{
		RestoreConnection(connection, priorState);
		return default;
	}
#else
	static async ValueTask RestoreConnectionAsync(DbConnection connection, ConnectionState priorState)
	{
		if (priorState.HasFlag(ConnectionState.Open))
			return;

		try
		{
			await connection.CloseAsync().ConfigureAwait(false);
		}
		catch
		{
			// Intentionally ignored; see above.
		}
	}
#endif

	#endregion

	#region Cancellation token in place of the isolation level

	// The isolation level is almost always left unspecified, so a token may be passed right after the action.
	// None of these have optional parameters, which is what keeps them from competing with the overloads that do.
	// They carry the lowest priority so that a bare `default` argument, which has always bound to IsolationLevel,
	// keeps doing so. A real CancellationToken can only match these, so it is unaffected.

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

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(DbConnection, Func{DbTransaction, CancellationToken, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		Func<DbTransaction, CancellationToken, ValueTask<(bool Commit, T Value)>> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(DbConnection, Func{DbTransaction, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync(DbConnection, Func{DbTransaction, ValueTask{bool}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		Func<DbTransaction, ValueTask<bool>> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync{T}(DbConnection, Func{DbTransaction, ValueTask{T}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		Func<DbTransaction, ValueTask<T>> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync(DbConnection, Func{DbTransaction, ValueTask}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask ExecuteTransactionAsync(
		this DbConnection connection,
		Func<DbTransaction, ValueTask> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	#endregion

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
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions and the conditional action returns true.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, Func{DbTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static bool ExecuteTransactionConditional(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, bool> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, Func{DbTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static T ExecuteTransaction<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, T> action)
		=> connection.ExecuteTransaction(action, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransaction(DbConnection, Action{DbTransaction}, IsolationLevel, CancellationToken)"/>
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
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions and the value from the action is true.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, Func{DbTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<bool>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, Func{DbTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<T>> action)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	/// <summary>
	/// Begins a transaction before executing the action.  Commits if there are no exceptions.  Otherwise rolls-back the transaction.
	/// </summary>
	/// <inheritdoc cref="ExecuteTransaction(DbConnection, Action{DbTransaction}, IsolationLevel, CancellationToken)"/>
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
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, IsolationLevel, Func{DbTransaction, bool}, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, ValueTask<bool>> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, IsolationLevel, Func{DbTransaction, T}, CancellationToken)"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(DbConnection, IsolationLevel, Action{DbTransaction}, CancellationToken)"/>
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
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<bool>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, CancellationToken, Func{DbTransaction, T})"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<T>> action)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(DbConnection, CancellationToken, Action{DbTransaction})"/>
	public static ValueTask ExecuteTransactionAsync(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask> action)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	#endregion

	#endregion
}
