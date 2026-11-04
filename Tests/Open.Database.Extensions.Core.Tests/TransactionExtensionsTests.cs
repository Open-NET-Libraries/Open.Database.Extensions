using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Behavior of the ExecuteTransaction* extensions: connection open/close ownership, commit/rollback rules,
// cancellation, and argument validation. Everything runs against FakeDbConnection, which logs each call in order.
[ExcludeFromCodeCoverage]
public static class TransactionExtensionsTests
{
	static readonly string[] Success = ["open", "begin", "commit", "tx-dispose", "close"];
	static readonly string[] RolledBack = ["open", "begin", "tx-dispose:rollback", "close"];

	static InvalidOperationException Boom() => new("boom");

	#region Connection ownership: close what we opened

	[Fact]
	public static void ExecuteTransaction_ClosedConnection_ClosesItAfterTheTransactionIsDisposed()
	{
		var c = new FakeDbConnection();

		c.ExecuteTransaction(_ => { });

		Assert.Equal(Success, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_ClosedConnection_ClosesItAfterTheTransactionIsDisposed()
	{
		var c = new FakeDbConnection();

		await c.ExecuteTransactionAsync(_ => default);

		Assert.Equal(Success, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_OpenConnection_LeavesItOpen()
	{
		var c = new FakeDbConnection();
		c.Open();
		c.Log.Clear();

		c.ExecuteTransaction(_ => { });

		Assert.Equal(["begin", "commit", "tx-dispose"], c.Log);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_OpenConnection_LeavesItOpen()
	{
		var c = new FakeDbConnection();
		c.Open();
		c.Log.Clear();

		await c.ExecuteTransactionAsync(_ => default);

		Assert.Equal(["begin", "commit", "tx-dispose"], c.Log);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_ActionThrows_RollsBackClosesAndPropagates()
	{
		var c = new FakeDbConnection();
		Action<DbTransaction> action = _ => throw Boom();

		var ex = Assert.Throws<InvalidOperationException>(() => c.ExecuteTransaction(action));

		Assert.Equal("boom", ex.Message);
		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_ActionThrows_RollsBackClosesAndPropagates()
	{
		var c = new FakeDbConnection();
		Func<DbTransaction, ValueTask> action = _ => throw Boom();

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await c.ExecuteTransactionAsync(action));

		Assert.Equal("boom", ex.Message);
		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_BeginTransactionThrows_StillClosesTheConnectionItOpened()
	{
		var c = new FakeDbConnection { BeginException = Boom() };

		Assert.Throws<InvalidOperationException>(() => c.ExecuteTransaction(_ => { }));

		Assert.Equal(["open", "begin", "close"], c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_BeginTransactionThrows_StillClosesTheConnectionItOpened()
	{
		var c = new FakeDbConnection { BeginException = Boom() };

		await Assert.ThrowsAsync<InvalidOperationException>(async () => await c.ExecuteTransactionAsync(_ => default));

		Assert.Equal(["open", "begin", "close"], c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_CloseFails_DoesNotHideTheActionsException()
	{
		var c = new FakeDbConnection { CloseException = new TimeoutException("close failed") };
		Action<DbTransaction> action = _ => throw Boom();

		var ex = Assert.Throws<InvalidOperationException>(() => c.ExecuteTransaction(action));

		Assert.Equal("boom", ex.Message);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_CloseFails_DoesNotHideTheActionsException()
	{
		var c = new FakeDbConnection { CloseException = new TimeoutException("close failed") };
		Func<DbTransaction, ValueTask> action = _ => throw Boom();

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await c.ExecuteTransactionAsync(action));

		Assert.Equal("boom", ex.Message);
	}

	[Fact]
	public static void ExecuteTransaction_CloseFailsAfterSuccess_ReportsTheFailure()
	{
		var c = new FakeDbConnection { CloseException = new TimeoutException("close failed") };

		Assert.Throws<TimeoutException>(() => c.ExecuteTransaction(_ => { }));
	}

	[Fact]
	public static void ExecuteTransaction_BrokenConnection_IsRepairedAndTreatedAsTheCallers()
	{
		// Same rule as the rest of the library: only a connection that was Closed on entry is closed afterwards.
		var c = new FakeDbConnection();
		c.SetState(ConnectionState.Broken);

		c.ExecuteTransaction(_ => { });

		Assert.Equal(["close", "open", "begin", "commit", "tx-dispose"], c.Log);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	#endregion

	#region ConnectionBehavior.LeaveOpen

	[Fact]
	public static void ExecuteTransaction_LeaveOpen_KeepsTheConnectionItOpened()
	{
		var c = new FakeDbConnection();

		c.ExecuteTransaction(_ => { }, ConnectionBehavior.LeaveOpen);

		Assert.Equal(["open", "begin", "commit", "tx-dispose"], c.Log);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static void ExecuteTransactionOfT_LeaveOpen_KeepsTheConnectionAndReturnsTheValue()
	{
		var c = new FakeDbConnection();

		int value = c.ExecuteTransaction(_ => 42, ConnectionBehavior.LeaveOpen);

		Assert.Equal(42, value);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static void ExecuteTransactionConditional_LeaveOpen_KeepsTheConnection()
	{
		var c = new FakeDbConnection();

		var result = c.ExecuteTransactionConditional(_ => (true, "ok"), ConnectionBehavior.LeaveOpen);

		Assert.Equal((true, "ok"), result);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_LeaveOpen_KeepsTheConnectionItOpened()
	{
		var c = new FakeDbConnection();

		await c.ExecuteTransactionAsync(_ => default, ConnectionBehavior.LeaveOpen);

		Assert.Equal(["open", "begin", "commit", "tx-dispose"], c.Log);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsyncOfT_LeaveOpen_KeepsTheConnectionAndReturnsTheValue()
	{
		var c = new FakeDbConnection();

		int value = await c.ExecuteTransactionAsync(_ => new ValueTask<int>(7), ConnectionBehavior.LeaveOpen);

		Assert.Equal(7, value);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionConditionalAsync_LeaveOpen_KeepsTheConnection()
	{
		var c = new FakeDbConnection();

		var result = await c.ExecuteTransactionConditionalAsync(
			(_, _) => new ValueTask<(bool Commit, int Value)>((true, 5)),
			ConnectionBehavior.LeaveOpen);

		Assert.Equal((true, 5), result);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_RestoreState_IsTheDefault()
	{
		var explicitly = new FakeDbConnection();
		var implicitly = new FakeDbConnection();

		explicitly.ExecuteTransaction(_ => { }, ConnectionBehavior.RestoreState);
		implicitly.ExecuteTransaction(_ => { });

		Assert.Equal(implicitly.Log, explicitly.Log);
		Assert.Equal(ConnectionState.Closed, explicitly.State);
	}

	[Fact]
	public static void ExecuteTransaction_LiteralZero_StillBindsToIsolationLevel()
	{
		// Source compatibility: adding the ConnectionBehavior overloads must not make a literal 0 ambiguous.
		var c = new FakeDbConnection();

		c.ExecuteTransaction(_ => { }, 0);

		Assert.Equal((IsolationLevel)0, c.LastTransaction!.IsolationLevel);
	}

	#endregion

	#region Commit and rollback

	[Fact]
	public static void ExecuteTransactionConditional_CommitFalse_RollsBackByDisposeAndReturnsFalse()
	{
		var c = new FakeDbConnection();

		bool committed = c.ExecuteTransactionConditional(_ => false);

		Assert.False(committed);
		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(0, c.LastTransaction!.CommitCalls);
	}

	[Fact]
	public static void ExecuteTransactionOfT_ReturnsTheActionsValueAndCommits()
	{
		var c = new FakeDbConnection();

		int value = c.ExecuteTransaction(_ => 42);

		Assert.Equal(42, value);
		Assert.Equal(1, c.LastTransaction!.CommitCalls);
	}

	[Fact]
	public static void ExecuteTransaction_PassesTheIsolationLevelToTheProvider()
	{
		var c = new FakeDbConnection();

		c.ExecuteTransaction(_ => { }, IsolationLevel.Serializable);

		Assert.Equal(IsolationLevel.Serializable, c.LastTransaction!.IsolationLevel);
	}

	[Fact]
	public static async Task ExecuteTransactionConditionalAsync_CommitFalse_DoesNotCommit()
	{
		var c = new FakeDbConnection();

		bool committed = await c.ExecuteTransactionConditionalAsync(_ => new ValueTask<bool>(false));

		Assert.False(committed);
		Assert.Equal(RolledBack, c.Log);
	}

	#endregion

	#region Cancellation

	[Fact]
	public static void ExecuteTransaction_TokenAlreadyCancelled_DoesNothing()
	{
		var c = new FakeDbConnection();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransaction(_ => { }, cancellationToken: cts.Token));

		Assert.Empty(c.Log);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_TokenAlreadyCancelled_DoesNothing()
	{
		var c = new FakeDbConnection();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await c.ExecuteTransactionAsync(_ => default, cancellationToken: cts.Token));

		Assert.Empty(c.Log);
	}

	[Fact]
	public static void ExecuteTransaction_TokenCancelledByAction_DoesNotCommitAndClosesTheConnection()
	{
		var c = new FakeDbConnection();
		using var cts = new CancellationTokenSource();

		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransaction(_ => cts.Cancel(), cancellationToken: cts.Token));

		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_TokenCancelledByAction_DoesNotCommitAndClosesTheConnection()
	{
		// Used to commit: the async path never looked at the token again after the action returned.
		var c = new FakeDbConnection();
		using var cts = new CancellationTokenSource();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await c.ExecuteTransactionAsync(
				_ => { cts.Cancel(); return default; },
				cancellationToken: cts.Token));

		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	#endregion

	#region Argument validation: a null delegate must fail fast, before anything is opened or begun

	static void AssertRejected(FakeDbConnection c, Action call, string paramName)
	{
		var ex = Assert.Throws<ArgumentNullException>(call);
		Assert.Equal(paramName, ex.ParamName);
		Assert.Empty(c.Log);
	}

	static async Task AssertRejectedAsync(FakeDbConnection c, Func<Task> call, string paramName)
	{
		var ex = await Assert.ThrowsAsync<ArgumentNullException>(call);
		Assert.Equal(paramName, ex.ParamName);
		Assert.Empty(c.Log);
	}

	[Fact]
	public static void NullDelegate_SyncEntryPoints_ThrowArgumentNullException_BeforeOpening()
	{
		var c = new FakeDbConnection();

		AssertRejected(c, () => c.ExecuteTransactionConditional((Func<DbTransaction, (bool Commit, int Value)>)null!), "conditionalAction");
		AssertRejected(c, () => c.ExecuteTransactionConditional((Func<DbTransaction, bool>)null!), "conditionalAction");
		AssertRejected(c, () => c.ExecuteTransaction((Func<DbTransaction, int>)null!), "action");
		AssertRejected(c, () => c.ExecuteTransaction((Action<DbTransaction>)null!), "action");
	}

	[Fact]
	public static void NullDelegate_ConnectionBehaviorOverloads_ThrowArgumentNullException_BeforeOpening()
	{
		var c = new FakeDbConnection();
		const ConnectionBehavior b = ConnectionBehavior.RestoreState;

		AssertRejected(c, () => c.ExecuteTransactionConditional((Func<DbTransaction, (bool Commit, int Value)>)null!, b), "conditionalAction");
		AssertRejected(c, () => c.ExecuteTransaction((Func<DbTransaction, int>)null!, b), "action");
		AssertRejected(c, () => c.ExecuteTransaction((Action<DbTransaction>)null!, b), "action");
	}

	[Fact]
	public static void NullDelegate_ForwardingOverloads_ThrowArgumentNullException_BeforeOpening()
	{
		var c = new FakeDbConnection();

		AssertRejected(c, () => c.ExecuteTransaction(IsolationLevel.ReadCommitted, CancellationToken.None, (Action<DbTransaction>)null!), "action");
		AssertRejected(c, () => c.ExecuteTransaction(CancellationToken.None, (Func<DbTransaction, int>)null!), "action");
		AssertRejected(c, () => c.ExecuteTransactionConditional(IsolationLevel.ReadCommitted, (Func<DbTransaction, bool>)null!), "conditionalAction");
	}

	[Fact]
	public static async Task NullDelegate_AsyncEntryPoints_ThrowArgumentNullException_BeforeOpening()
	{
		var c = new FakeDbConnection();

		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionConditionalAsync((Func<DbTransaction, CancellationToken, ValueTask<(bool Commit, int Value)>>)null!), "conditionalAction");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionConditionalAsync((Func<DbTransaction, ValueTask<(bool Commit, int Value)>>)null!), "conditionalAction");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionConditionalAsync((Func<DbTransaction, ValueTask<bool>>)null!), "conditionalAction");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionAsync((Func<DbTransaction, ValueTask<int>>)null!), "action");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionAsync((Func<DbTransaction, ValueTask>)null!), "action");
	}

	[Fact]
	public static async Task NullDelegate_AsyncConnectionBehaviorOverloads_ThrowArgumentNullException_BeforeOpening()
	{
		var c = new FakeDbConnection();
		const ConnectionBehavior b = ConnectionBehavior.RestoreState;

		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionConditionalAsync((Func<DbTransaction, CancellationToken, ValueTask<(bool Commit, int Value)>>)null!, b), "conditionalAction");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionAsync((Func<DbTransaction, ValueTask<int>>)null!, b), "action");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionAsync((Func<DbTransaction, ValueTask>)null!, b), "action");
	}

	[Fact]
	public static void NullConnection_Throws()
	{
		DbConnection connection = null!;

		var ex = Assert.Throws<ArgumentNullException>(() => connection.ExecuteTransaction(_ => { }));

		Assert.Equal("connection", ex.ParamName);
	}

	[Fact]
	public static async Task NullConnection_Async_Throws()
	{
		DbConnection connection = null!;

		var ex = await Assert.ThrowsAsync<ArgumentNullException>(async () => await connection.ExecuteTransactionAsync(_ => default));

		Assert.Equal("connection", ex.ParamName);
	}

	#endregion

	#region Cancellation token in place of the isolation level

	[Fact]
	public static void ExecuteTransaction_TokenAfterAction_UsesUnspecifiedIsolation()
	{
		var c = new FakeDbConnection();

		c.ExecuteTransaction(_ => { }, CancellationToken.None);

		Assert.Equal(Success, c.Log);
		Assert.Equal(IsolationLevel.Unspecified, c.LastTransaction!.IsolationLevel);
	}

	// A token that is already cancelled must stop every shape before anything is opened.
	// That only happens if each overload actually hands the token on.
	[Fact]
	public static void TokenAfterAction_SyncShapes_PassTheTokenOn()
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		CancellationToken ct = cts.Token;
		var c = new FakeDbConnection();

		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransactionConditional(_ => (true, 1), ct));
		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransactionConditional(_ => true, ct));
		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransaction(_ => 1, ct));
		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransaction(_ => { }, ct));

		Assert.Empty(c.Log);
	}

	[Fact]
	public static async Task TokenAfterAction_AsyncShapes_PassTheTokenOn()
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		CancellationToken ct = cts.Token;
		var c = new FakeDbConnection();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionConditionalAsync((_, _) => new ValueTask<(bool Commit, int Value)>((true, 1)), ct));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionConditionalAsync(_ => new ValueTask<(bool Commit, int Value)>((true, 1)), ct));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionConditionalAsync(_ => new ValueTask<bool>(true), ct));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionAsync(_ => new ValueTask<int>(1), ct));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionAsync(_ => default, ct));

		Assert.Empty(c.Log);
	}

	[Fact]
	public static void TokenAfterConnectionBehavior_SyncShapes_PassTheTokenOn()
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		CancellationToken ct = cts.Token;
		const ConnectionBehavior b = ConnectionBehavior.LeaveOpen;
		var c = new FakeDbConnection();

		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransactionConditional(_ => (true, 1), b, ct));
		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransaction(_ => 1, b, ct));
		Assert.Throws<OperationCanceledException>(() => c.ExecuteTransaction(_ => { }, b, ct));

		Assert.Empty(c.Log);
	}

	[Fact]
	public static async Task TokenAfterConnectionBehavior_AsyncShapes_PassTheTokenOn()
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		CancellationToken ct = cts.Token;
		const ConnectionBehavior b = ConnectionBehavior.LeaveOpen;
		var c = new FakeDbConnection();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionConditionalAsync((_, _) => new ValueTask<(bool Commit, int Value)>((true, 1)), b, ct));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionAsync(_ => new ValueTask<int>(1), b, ct));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await c.ExecuteTransactionAsync(_ => default, b, ct));

		Assert.Empty(c.Log);
	}

	[Fact]
	public static void ExecuteTransaction_LiteralDefault_StillBindsToIsolationLevel()
	{
		// Source compatibility: this compiled (as IsolationLevel) before the token overloads were added.
		var c = new FakeDbConnection();

		c.ExecuteTransaction(_ => { }, default);

		Assert.Equal((IsolationLevel)0, c.LastTransaction!.IsolationLevel);
	}

	#endregion
}
