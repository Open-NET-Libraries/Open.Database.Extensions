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

		c.ExecuteTransaction(_ => { }, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(Success, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_ClosedConnection_ClosesItAfterTheTransactionIsDisposed()
	{
		var c = new FakeDbConnection();

		await c.ExecuteTransactionAsync(_ => default, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(Success, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_OpenConnection_LeavesItOpen()
	{
		var c = new FakeDbConnection();
		c.Open();
		c.Log.Clear();

		c.ExecuteTransaction(_ => { }, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(["begin", "commit", "tx-dispose"], c.Log);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_OpenConnection_LeavesItOpen()
	{
		var c = new FakeDbConnection();
		c.Open();
		c.Log.Clear();

		await c.ExecuteTransactionAsync(_ => default, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(["begin", "commit", "tx-dispose"], c.Log);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_ActionThrows_RollsBackClosesAndPropagates()
	{
		var c = new FakeDbConnection();
		static void action(DbTransaction _) => throw Boom();

		var ex = Assert.Throws<InvalidOperationException>(() => c.ExecuteTransaction(action, cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal("boom", ex.Message);
		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_ActionThrows_RollsBackClosesAndPropagates()
	{
		var c = new FakeDbConnection();
		static ValueTask action(DbTransaction _) => throw Boom();

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await c.ExecuteTransactionAsync(action, cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal("boom", ex.Message);
		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_BeginTransactionThrows_StillClosesTheConnectionItOpened()
	{
		var c = new FakeDbConnection { BeginException = Boom() };

		Assert.Throws<InvalidOperationException>(() => c.ExecuteTransaction(_ => { }, cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal(["open", "begin", "close"], c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_BeginTransactionThrows_StillClosesTheConnectionItOpened()
	{
		var c = new FakeDbConnection { BeginException = Boom() };

		await Assert.ThrowsAsync<InvalidOperationException>(async () => await c.ExecuteTransactionAsync(_ => default, cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal(["open", "begin", "close"], c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static void ExecuteTransaction_CloseFails_DoesNotHideTheActionsException()
	{
		var c = new FakeDbConnection { CloseException = new TimeoutException("close failed") };
		static void action(DbTransaction _) => throw Boom();

		var ex = Assert.Throws<InvalidOperationException>(() => c.ExecuteTransaction(action, cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal("boom", ex.Message);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_CloseFails_DoesNotHideTheActionsException()
	{
		var c = new FakeDbConnection { CloseException = new TimeoutException("close failed") };
		static ValueTask action(DbTransaction _) => throw Boom();

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await c.ExecuteTransactionAsync(action, cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal("boom", ex.Message);
	}

	[Fact]
	public static void ExecuteTransaction_CloseFailsAfterCommit_StillReturnsNormally()
	{
		// The work is already committed; a failure to close must not make the caller think it failed.
		var c = new FakeDbConnection { CloseException = new TimeoutException("close failed") };

		int value = c.ExecuteTransaction(_ => 42, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(42, value);
		Assert.Equal(1, c.LastTransaction!.CommitCalls);
	}

	[Fact]
	public static async Task ExecuteTransactionAsync_CloseFailsAfterCommit_StillReturnsNormally()
	{
		var c = new FakeDbConnection { CloseException = new TimeoutException("close failed") };

		int value = await c.ExecuteTransactionAsync(_ => new ValueTask<int>(42), cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(42, value);
		Assert.Equal(1, c.LastTransaction!.CommitCalls);
	}

	[Fact]
	public static void ExecuteTransaction_BrokenConnection_IsRepairedThenClosedAgain()
	{
		// A broken connection was not open, so the connection that gets (re)opened here is closed here.
		var c = new FakeDbConnection();
		c.SetState(ConnectionState.Broken);

		c.ExecuteTransaction(_ => { }, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(["close", "open", "begin", "commit", "tx-dispose", "close"], c.Log);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	#endregion

	#region Commit and rollback

	[Fact]
	public static void ExecuteTransactionConditional_CommitFalse_RollsBackByDisposeAndReturnsFalse()
	{
		var c = new FakeDbConnection();

		bool committed = c.ExecuteTransactionConditional(_ => false, cancellationToken: TestContext.Current.CancellationToken);

		Assert.False(committed);
		Assert.Equal(RolledBack, c.Log);
		Assert.Equal(0, c.LastTransaction!.CommitCalls);
	}

	[Fact]
	public static void ExecuteTransactionOfT_ReturnsTheActionsValueAndCommits()
	{
		var c = new FakeDbConnection();

		int value = c.ExecuteTransaction(_ => 42, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(42, value);
		Assert.Equal(1, c.LastTransaction!.CommitCalls);
	}

	[Fact]
	public static void ExecuteTransaction_PassesTheIsolationLevelToTheProvider()
	{
		var c = new FakeDbConnection();

		c.ExecuteTransaction(_ => { }, IsolationLevel.Serializable, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(IsolationLevel.Serializable, c.LastTransaction!.IsolationLevel);
	}

	[Fact]
	public static async Task ExecuteTransactionConditionalAsync_CommitFalse_DoesNotCommit()
	{
		var c = new FakeDbConnection();

		bool committed = await c.ExecuteTransactionConditionalAsync(_ => new ValueTask<bool>(false), cancellationToken: TestContext.Current.CancellationToken);

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
		AssertRejected(c, () => c.ExecuteTransactionConditional(null!), "conditionalAction");
		AssertRejected(c, () => c.ExecuteTransaction((Func<DbTransaction, int>)null!), "action");
		AssertRejected(c, () => c.ExecuteTransaction(null!), "action");
	}

	[Fact]
	public static void NullDelegate_ForwardingOverloads_ThrowArgumentNullException_BeforeOpening()
	{
		var c = new FakeDbConnection();

		AssertRejected(c, () => c.ExecuteTransaction(IsolationLevel.ReadCommitted, CancellationToken.None, null!), "action");
		AssertRejected(c, () => c.ExecuteTransaction(CancellationToken.None, (Func<DbTransaction, int>)null!), "action");
		AssertRejected(c, () => c.ExecuteTransactionConditional(IsolationLevel.ReadCommitted, null!), "conditionalAction");
	}

	[Fact]
	public static async Task NullDelegate_AsyncEntryPoints_ThrowArgumentNullException_BeforeOpening()
	{
		var c = new FakeDbConnection();

		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionConditionalAsync((Func<DbTransaction, CancellationToken, ValueTask<(bool Commit, int Value)>>)null!), "conditionalAction");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionConditionalAsync((Func<DbTransaction, ValueTask<(bool Commit, int Value)>>)null!), "conditionalAction");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionConditionalAsync(null!), "conditionalAction");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionAsync((Func<DbTransaction, ValueTask<int>>)null!), "action");
		await AssertRejectedAsync(c, async () => await c.ExecuteTransactionAsync(null!), "action");
	}

	[Fact]
	public static void NullConnection_Throws()
	{
		DbConnection connection = null!;

		var ex = Assert.Throws<ArgumentNullException>(() => connection.ExecuteTransaction(_ => { }, cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal("connection", ex.ParamName);
	}

	[Fact]
	public static async Task NullConnection_Async_Throws()
	{
		DbConnection connection = null!;

		var ex = await Assert.ThrowsAsync<ArgumentNullException>(async () => await connection.ExecuteTransactionAsync(_ => default, cancellationToken: TestContext.Current.CancellationToken));

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
	public static void ExecuteTransaction_LiteralDefault_StillBindsToIsolationLevel()
	{
		// Source compatibility: this compiled (as IsolationLevel) before the token overloads were added.
		var c = new FakeDbConnection();

		// Deliberately the two-argument form (no token): this test guards how a bare `default` binds.
#pragma warning disable xUnit1051
		c.ExecuteTransaction(_ => { }, default);
#pragma warning restore xUnit1051

		Assert.Equal((IsolationLevel)0, c.LastTransaction!.IsolationLevel);
	}

	#endregion
}
