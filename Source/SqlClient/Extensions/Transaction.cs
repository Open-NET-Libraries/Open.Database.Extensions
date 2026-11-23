namespace Open.Database.Extensions;

// NOTE: This is simply a copy of the IDb and Db extensions but replacing types with their Sql versions.
// Why?  To ensure the Sql types are propagated through the type flow.
// Keep it in sync with Source/Core/Extensions/Transaction.cs; only the type names and the TFM seams differ.

/// <summary>
/// System.Data.SqlClient specific extensions for database transactions.
/// </summary>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
[Obsolete(SqlClientObsolete._message)]
public static partial class SqlTransactionExtensions
{
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransactionConditional{T}(SqlConnection, Func{SqlTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this SqlConnection connection,
		Func<SqlTransaction, CancellationToken, ValueTask<(bool Commit, T Value)>> conditionalAction,
		IsolationLevel isolationLevel = IsolationLevel.Unspecified,
		CancellationToken cancellationToken = default)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		if (conditionalAction is null) throw new ArgumentNullException(nameof(conditionalAction));
		Contract.EndContractBlock();

		// Remember the prior state so a connection that is opened here is closed again here.
		ConnectionState priorState = await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		try
		{
			// Disposing the transaction (at the end of this block, before the connection is closed)
			// is what rolls back anything that was not committed.
			// BeginTransaction rather than BeginTransactionAsync: the latter returns a plain DbTransaction
			// and this class exists to keep the SqlTransaction type flowing.
#if NETSTANDARD2_0
			using SqlTransaction transaction = connection.BeginTransaction(isolationLevel);
#else
			SqlTransaction transaction = connection.BeginTransaction(isolationLevel);
			await using var transactionDisposal = transaction.ConfigureAwait(false);
#endif
			(bool Commit, T Value) result = await conditionalAction(transaction, cancellationToken).ConfigureAwait(false);

			// Same contract as the synchronous path: a cancelled token never commits.
			cancellationToken.ThrowIfCancellationRequested();

			if (result.Commit)
			{
				// The commit is the point of no return: the token was checked just above, and cancelling
				// part-way through would leave the outcome unknown.
#if NETSTANDARD2_0
				transaction.Commit();
#else
				await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
#endif
			}

			return result;
		}
		finally
		{
			await RestoreConnectionAsync(connection, priorState).ConfigureAwait(false);
		}
	}

	/// <summary>
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws, its <c>Commit</c> value is <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <typeparam name="T">The value returned from the action.</typeparam>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
	/// <param name="isolationLevel">The isolation level for the transaction.</param>
	/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
	/// <returns>The value of the awaited action.</returns>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
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
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws, returns <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransactionConditional(SqlConnection, Func{SqlTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask<bool> ExecuteTransactionConditionalAsync(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask<bool>> conditionalAction,
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
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransaction{T}(SqlConnection, Func{SqlTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask<T> ExecuteTransactionAsync<T>(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask<T>> action,
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
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransaction(SqlConnection, Action{SqlTransaction}, IsolationLevel, CancellationToken)"/>
	public static async ValueTask ExecuteTransactionAsync(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask> action,
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
	internal static void RestoreConnection(SqlConnection connection, ConnectionState priorState)
	{
		if ((priorState & (ConnectionState.Open | ConnectionState.Broken)) == ConnectionState.Open)
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
	static ValueTask RestoreConnectionAsync(SqlConnection connection, ConnectionState priorState)
	{
		RestoreConnection(connection, priorState);
		return default;
	}
#else
	static async ValueTask RestoreConnectionAsync(SqlConnection connection, ConnectionState priorState)
	{
		if ((priorState & (ConnectionState.Open | ConnectionState.Broken)) == ConnectionState.Open)
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

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(SqlConnection, Func{SqlTransaction, CancellationToken, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this SqlConnection connection,
		Func<SqlTransaction, CancellationToken, ValueTask<(bool Commit, T Value)>> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(SqlConnection, Func{SqlTransaction, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync(SqlConnection, Func{SqlTransaction, ValueTask{bool}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask<bool>> conditionalAction,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync{T}(SqlConnection, Func{SqlTransaction, ValueTask{T}}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask<T>> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync(SqlConnection, Func{SqlTransaction, ValueTask}, IsolationLevel, CancellationToken)"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask ExecuteTransactionAsync(
		this SqlConnection connection,
		Func<SqlTransaction, ValueTask> action,
		CancellationToken cancellationToken)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	#endregion

	#region Overloads

	#region No optional params
	/// <summary>
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws, its <c>Commit</c> value is <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <typeparam name="T">The value returned from the action.</typeparam>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="isolationLevel">The isolation level for the transaction.</param>
	/// <param name="cancellationToken">A token that if cancelled will cause this transaction to be aborted or rolled-back.</param>
	/// <param name="conditionalAction">The handler to execute while a transaction is pending. Returning a 'Commit' value of true signals to commit the transaction.</param>
	/// <returns>The value of the awaited action.</returns>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws, returns <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransactionConditional(SqlConnection, Func{SqlTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, ValueTask<bool>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <summary>
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransaction{T}(SqlConnection, Func{SqlTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, ValueTask<T>> action)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	/// <summary>
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransaction(SqlConnection, Action{SqlTransaction}, IsolationLevel, CancellationToken)"/>
	public static ValueTask ExecuteTransactionAsync(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, ValueTask> action)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	#endregion

	#region Optional Token
	/// <summary>
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws, its <c>Commit</c> value is <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
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
	/// Asynchronously runs the action in a transaction on the connection, committing it unless the action throws, returns <see langword="false"/>, or <paramref name="cancellationToken"/> is canceled.
	/// </summary>
	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransactionConditional(SqlConnection, IsolationLevel, Func{SqlTransaction, bool}, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, ValueTask<bool>> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <returns>The value of the awaited action.</returns>
	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransaction{T}(SqlConnection, IsolationLevel, Func{SqlTransaction, T}, CancellationToken)"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="Synchronous.SyncSqlTransactionExtensions.ExecuteTransaction(SqlConnection, IsolationLevel, Action{SqlTransaction}, CancellationToken)"/>
	public static ValueTask ExecuteTransactionAsync(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, ValueTask> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);
	#endregion

	#endregion
}
