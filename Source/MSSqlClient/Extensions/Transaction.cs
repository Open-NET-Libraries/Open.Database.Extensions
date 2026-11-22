namespace Open.Database.Extensions;

// Keep it in sync with Source/Core/Extensions/Transaction.cs; only the type names and the TFM seams differ.

/// <summary>
/// Provides Microsoft.Data.SqlClient specific extension methods that run work in a database transaction on a <see cref="SqlConnection"/>.
/// </summary>
/// <remarks>
/// These are a copy of the <see cref="TransactionExtensions"/> methods for <see cref="System.Data.Common.DbConnection"/>, with the types replaced by their Sql versions.
/// The copy exists so that the <see cref="SqlTransaction"/> type is propagated through the type flow: the action receives a <see cref="SqlTransaction"/>, not a plain transaction.
/// </remarks>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
public static partial class SqlTransactionExtensions
{
	/// <summary>
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, its <c>Commit</c> value is <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The asynchronous handler to execute while the transaction is pending.
	/// It receives the transaction and <paramref name="cancellationToken"/>.
	/// Returning a <c>Commit</c> value of <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns>
	/// A task that represents the asynchronous operation; its result is the value
	/// returned by <paramref name="conditionalAction"/>.
	/// Its <c>Commit</c> value is <see langword="true"/> if the transaction was committed.
	/// </returns>
	/// <remarks>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
#if NET472
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
#if NET472
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, its <c>Commit</c> value is <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The asynchronous handler to execute while the transaction is pending.
	/// Returning a <c>Commit</c> value of <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns>
	/// A task that represents the asynchronous operation; its result is the value
	/// returned by <paramref name="conditionalAction"/>.
	/// Its <c>Commit</c> value is <see langword="true"/> if the transaction was committed.
	/// </returns>
	/// <remarks>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="conditionalAction"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, returns <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The asynchronous handler to execute while the transaction is pending.
	/// Returning <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns>
	/// A task that represents the asynchronous operation; its result is
	/// <see langword="true"/> if the transaction was committed; otherwise, <see langword="false"/>.
	/// </returns>
	/// <remarks>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw and
	/// <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The asynchronous handler to execute while the transaction is pending.</param>
	/// <returns>
	/// A task that represents the asynchronous operation; its result is the value
	/// returned by <paramref name="action"/>.
	/// </returns>
	/// <remarks>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw and
	/// <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The asynchronous handler to execute while the transaction is pending.</param>
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <remarks>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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

	/// <summary>
	/// Closes (never disposes) a connection that was not open when the operation began,
	/// so the caller's connection is left the way it was found.
	/// </summary>
	/// <param name="connection">The connection the operation ran on.</param>
	/// <param name="priorState">The state the connection was in before the operation opened it.</param>
	/// <remarks>
	/// <para>
	/// By the time this runs the transaction has already been disposed: that ordering matters
	/// because some providers (for example Microsoft.Data.Sqlite and MySqlConnector) only roll back
	/// an uncommitted transaction on dispose while the connection is still open.
	/// </para>
	/// <para>
	/// A failure to close is deliberately ignored. By then the transaction has already been committed
	/// or rolled back, so it must neither turn a completed operation into an error nor replace the
	/// exception of a failed one.
	/// </para>
	/// <para>
	/// Only a connection that was already <see cref="ConnectionState.Open"/> belongs to the caller.
	/// One that was closed, or broken and therefore repaired by
	/// <see cref="ConnectionExtensions.EnsureOpen(IDbConnection)"/>, is a connection this operation
	/// opened, so it is closed again here.
	/// </para>
	/// </remarks>
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

#if NET472
	/// <summary>
	/// Closes the connection as <see cref="RestoreConnection(SqlConnection, ConnectionState)"/> does.
	/// The .NET Framework target has no <c>CloseAsync</c>, so it closes synchronously.
	/// </summary>
	/// <inheritdoc cref="RestoreConnection(SqlConnection, ConnectionState)"/>
	static ValueTask RestoreConnectionAsync(SqlConnection connection, ConnectionState priorState)
	{
		RestoreConnection(connection, priorState);
		return default;
	}
#else
	/// <summary>
	/// Asynchronously closes the connection as <see cref="RestoreConnection(SqlConnection, ConnectionState)"/> does.
	/// </summary>
	/// <inheritdoc cref="RestoreConnection(SqlConnection, ConnectionState)"/>
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
	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(SqlConnection, Func{SqlTransaction, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync(SqlConnection, Func{SqlTransaction, ValueTask{bool}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<SqlTransaction, ValueTask<bool>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	#endregion

	#region Optional Token
	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(SqlConnection, Func{SqlTransaction, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync(SqlConnection, Func{SqlTransaction, ValueTask{bool}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this SqlConnection connection,
		IsolationLevel isolationLevel,
		Func<SqlTransaction, ValueTask<bool>> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	#endregion

	#endregion
}
