namespace Open.Database.Extensions;

[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload for easier consumption.")]
public static partial class TransactionExtensions
{
	/// <summary>
	/// Begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, its <c>Commit</c> value is <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The handler to execute while the transaction is pending.
	/// Returning a <c>Commit</c> value of <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns>
	/// The value returned by <paramref name="conditionalAction"/>.
	/// Its <c>Commit</c> value is <see langword="true"/> if the transaction was committed.
	/// </returns>
	/// <remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, its <c>Commit</c> value is <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
		ConnectionState priorState = await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		try
		{
			// Disposing the transaction (at the end of this block, before the connection is closed)
			// is what rolls back anything that was not committed.
#if NETSTANDARD2_0
			using DbTransaction transaction = connection.BeginTransaction(isolationLevel);
#else
			DbTransaction transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
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
	/// Begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, returns <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The handler to execute while the transaction is pending.
	/// Returning <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns><see langword="true"/> if the transaction was committed; otherwise, <see langword="false"/>.</returns>
	/// <remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
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
	/// Begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw and
	/// <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The handler to execute while the transaction is pending.</param>
	/// <include file="Connections.docs.xml" path="docs/members[@name='returns-value']/*" />
	/// <remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
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
	/// Begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw and
	/// <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The handler to execute while the transaction is pending.</param>
	/// <remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
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

	/// <summary>
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, its <c>Commit</c> value is <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
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
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="conditionalAction"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw, returns <see langword="true"/>,
	/// and <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="conditionalAction">
	/// The asynchronous handler to execute while the transaction is pending.
	/// Returning <see langword="true"/> signals to commit the transaction.
	/// </param>
	/// <returns>
	/// A task that represents the asynchronous operation; its result is
	/// <see langword="true"/> if the transaction was committed; otherwise, <see langword="false"/>.
	/// </returns>
	/// <remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="conditionalAction"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <example>
	/// The transaction is committed only if exactly one row was deleted:
	/// <code language="csharp">
	/// bool committed = await connection.ExecuteTransactionConditionalAsync(
	///     async transaction =>
	///     {
	///         int rows = await transaction
	///             .Command("DELETE FROM Posts WHERE Id = @id AND AuthorId = @authorId")
	///             .AddParam("id", postId)
	///             .AddParam("authorId", authorId)
	///             .ExecuteNonQueryAsync(cancellationToken);
	///         return rows == 1;
	///     },
	///     cancellationToken);
	/// </code>
	/// </example>
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw and
	/// <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tresult']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The asynchronous handler to execute while the transaction is pending.</param>
	/// <returns>
	/// A task that represents the asynchronous operation; its result is the value
	/// returned by <paramref name="action"/>.
	/// </returns>
	/// <remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously begins a transaction on the connection and invokes the action.
	/// The transaction is committed if the action does not throw and
	/// <paramref name="cancellationToken"/> has not been canceled; otherwise it is rolled back.
	/// </summary>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-common']/*" />
	/// <param name="action">The asynchronous handler to execute while the transaction is pending.</param>
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <remarks>
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-paras']/*" />
	/// <include file="Connections.docs.xml" path="docs/members[@name='tx-async-para']/*" />
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="action"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <example>
	/// Both statements are committed together, or neither is:
	/// <code language="csharp">
	/// await connection.ExecuteTransactionAsync(
	///     async transaction =>
	///     {
	///         await transaction
	///             .Command("DELETE FROM Posts WHERE Id = @id")
	///             .AddParam("id", postId)
	///             .ExecuteNonQueryAsync(cancellationToken);
	///         await transaction
	///             .Command("UPDATE Authors SET PostCount = PostCount - 1 WHERE Id = @authorId")
	///             .AddParam("authorId", authorId)
	///             .ExecuteNonQueryAsync(cancellationToken);
	///     },
	///     cancellationToken);
	/// </code>
	/// </example>
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
	static void RestoreConnection(DbConnection connection, ConnectionState priorState)
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
	/// <summary>
	/// Closes the connection as <see cref="RestoreConnection(DbConnection, ConnectionState)"/> does.
	/// netstandard2.0 has no <c>CloseAsync</c>, so it closes synchronously.
	/// </summary>
	/// <inheritdoc cref="RestoreConnection(DbConnection, ConnectionState)"/>
	static ValueTask RestoreConnectionAsync(DbConnection connection, ConnectionState priorState)
	{
		RestoreConnection(connection, priorState);
		return default;
	}
#else
	/// <summary>
	/// Asynchronously closes the connection as <see cref="RestoreConnection(DbConnection, ConnectionState)"/> does.
	/// </summary>
	/// <inheritdoc cref="RestoreConnection(DbConnection, ConnectionState)"/>
	static async ValueTask RestoreConnectionAsync(DbConnection connection, ConnectionState priorState)
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

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(DbConnection, Func{DbTransaction, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync(DbConnection, Func{DbTransaction, ValueTask{bool}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<bool>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync{T}(DbConnection, Func{DbTransaction, ValueTask{T}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<T>> action)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync(DbConnection, Func{DbTransaction, ValueTask}, IsolationLevel, CancellationToken)"/>
	public static ValueTask ExecuteTransactionAsync(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask> action)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	#endregion

	#region Optional Token
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

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(DbConnection, Func{DbTransaction, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync(DbConnection, Func{DbTransaction, ValueTask{bool}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, ValueTask<bool>> conditionalAction,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync{T}(DbConnection, Func{DbTransaction, ValueTask{T}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, ValueTask<T>> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync(DbConnection, Func{DbTransaction, ValueTask}, IsolationLevel, CancellationToken)"/>
	public static ValueTask ExecuteTransactionAsync(
		this DbConnection connection,
		IsolationLevel isolationLevel,
		Func<DbTransaction, ValueTask> action,
		CancellationToken cancellationToken = default)
		=> connection.ExecuteTransactionAsync(action, isolationLevel, cancellationToken);
	#endregion

	#region Unspecified Isolation Level
	/// <inheritdoc cref="ExecuteTransactionConditional{T}(DbConnection, Func{DbTransaction, ValueTuple{bool, T}}, IsolationLevel, CancellationToken)"/>
	public static (bool Commit, T Value) ExecuteTransactionConditional<T>(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, (bool Commit, T Value)> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditional(DbConnection, Func{DbTransaction, bool}, IsolationLevel, CancellationToken)"/>
	public static bool ExecuteTransactionConditional(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, bool> conditionalAction)
		=> connection.ExecuteTransactionConditional(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction{T}(DbConnection, Func{DbTransaction, T}, IsolationLevel, CancellationToken)"/>
	public static T ExecuteTransaction<T>(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, T> action)
		=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransaction(DbConnection, Action{DbTransaction}, IsolationLevel, CancellationToken)"/>
	public static void ExecuteTransaction(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Action<DbTransaction> action)
		=> connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync{T}(DbConnection, Func{DbTransaction, ValueTask{ValueTuple{bool, T}}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<(bool Commit, T Value)> ExecuteTransactionConditionalAsync<T>(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<(bool Commit, T Value)>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionConditionalAsync(DbConnection, Func{DbTransaction, ValueTask{bool}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<bool> ExecuteTransactionConditionalAsync(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<bool>> conditionalAction)
		=> connection.ExecuteTransactionConditionalAsync(conditionalAction, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync{T}(DbConnection, Func{DbTransaction, ValueTask{T}}, IsolationLevel, CancellationToken)"/>
	public static ValueTask<T> ExecuteTransactionAsync<T>(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask<T>> action)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	/// <inheritdoc cref="ExecuteTransactionAsync(DbConnection, Func{DbTransaction, ValueTask}, IsolationLevel, CancellationToken)"/>
	public static ValueTask ExecuteTransactionAsync(
		this DbConnection connection,
		CancellationToken cancellationToken,
		Func<DbTransaction, ValueTask> action)
		=> connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cancellationToken);

	#endregion

	#endregion
}
