using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A library must not resume on the caller's SynchronizationContext. Each case makes exactly ONE awaited operation
// genuinely asynchronous (a gate the harness opens only after the call has returned an incomplete task) and runs
// the call under a context that counts Post/Send. An await in the library that lacks ConfigureAwait(false) posts back.
[ExcludeFromCodeCoverage]
public static class ConfigureAwaitTests
{
	sealed class CountingContext : SynchronizationContext
	{
		int _posts;
		public int Posts => _posts;
		public override void Post(SendOrPostCallback d, object? state) { Interlocked.Increment(ref _posts); ThreadPool.QueueUserWorkItem(_ => d(state)); }
		public override void Send(SendOrPostCallback d, object? state) { Interlocked.Increment(ref _posts); d(state); }
	}

	sealed class GatedConnection(string gatedOperation) : DbConnection
	{
		ConnectionState _state;
		public int Gated { get; private set; }
		public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public Task Operation(string name)
		{
			if (name != gatedOperation) return Task.CompletedTask;
			Gated++;
			return Gate.Task;
		}

		public override async Task OpenAsync(CancellationToken cancellationToken) { await Operation("open").ConfigureAwait(false); _state = ConnectionState.Open; }
		public override async Task CloseAsync() { await Operation("close").ConfigureAwait(false); _state = ConnectionState.Closed; }
		protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
		{ await Operation("begin").ConfigureAwait(false); return new GatedTransaction(this); }
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new GatedTransaction(this);
		public override void Open() => _state = ConnectionState.Open;
		public override void Close() => _state = ConnectionState.Closed;
		[AllowNull] public override string ConnectionString { get; set; } = "";
		public override string Database => "";
		public override string DataSource => "";
		public override string ServerVersion => "";
		public override ConnectionState State => _state;
		public override void ChangeDatabase(string databaseName) { }
		protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
	}

	sealed class GatedTransaction(GatedConnection connection) : DbTransaction
	{
		public override IsolationLevel IsolationLevel => IsolationLevel.Unspecified;
		protected override DbConnection? DbConnection => connection;
		public override void Commit() { }
		public override void Rollback() { }
		public override Task CommitAsync(CancellationToken cancellationToken) => connection.Operation("commit");
		public override ValueTask DisposeAsync() => new(connection.Operation("dispose"));
	}

	// Runs the call on a thread whose context counts posts; the gate is opened after the call has returned.
	static async Task<(int Posts, int Gated)> Run(string gatedOperation, Func<GatedConnection, Task> call)
	{
		var connection = new GatedConnection(gatedOperation);
		var context = new CountingContext();
		var prior = SynchronizationContext.Current;
		Task task;
		SynchronizationContext.SetSynchronizationContext(context);
		try { task = call(connection); }
		finally { SynchronizationContext.SetSynchronizationContext(prior); }

		connection.Gate.TrySetResult();
		await task;
		return (context.Posts, connection.Gated);
	}

	// A command that opens a closed connection must not resume on the caller's context after the open. The fake connection cannot
	// create or run a command, so each call ends with NotSupportedException after the open: only the posts matter.
	static async Task Swallow(Task call) { try { await call.ConfigureAwait(false); } catch (NotSupportedException) { } }

	[Fact]
	public static async Task CommandOpen_NeverPostsBackToTheCallersContext()
	{
		KeyValuePair<string, Func<GatedConnection, Task>>[] calls =
		[
			new("Core ExecuteReaderAsync", c => Swallow(new FakeDbCommand(c, () => throw new NotSupportedException()).ExecuteReaderAsync(_ => { }, CommandBehavior.Default, TestContext.Current.CancellationToken).AsTask())),
			new("ExpressiveDbCommand", c => Swallow(new ExpressiveDbCommand(c, CommandType.Text, "select 1").ExecuteAsync(_ => ValueTask.CompletedTask).AsTask())),
		];

		foreach (var call in calls)
		{
			var (posts, gated) = await Run("open", call.Value);

			Assert.True(gated == 1, $"{call.Key}: the open was never awaited, so nothing was tested.");
			Assert.True(posts == 0, $"{call.Key}: resumed on the caller's context {posts} time(s) after an asynchronous open.");
		}
	}

	static async ValueTask Act(GatedConnection c) => await c.Operation("action").ConfigureAwait(false);
	static async ValueTask<bool> ActBool(GatedConnection c) { await Act(c).ConfigureAwait(false); return true; }
	static async ValueTask<int> ActInt(GatedConnection c) { await Act(c).ConfigureAwait(false); return 1; }
	static async ValueTask<(bool Commit, int Value)> ActPair(GatedConnection c) { await Act(c).ConfigureAwait(false); return (true, 1); }

	static readonly KeyValuePair<string, Func<GatedConnection, Task>>[] Calls =
	[
		new("ExecuteTransactionConditionalAsync<T>", c => c.ExecuteTransactionConditionalAsync<int>((_, _) => ActPair(c), cancellationToken: TestContext.Current.CancellationToken).AsTask()),
		new("ExecuteTransactionConditionalAsync(bool)", c => c.ExecuteTransactionConditionalAsync(_ => ActBool(c), cancellationToken: TestContext.Current.CancellationToken).AsTask()),
		new("ExecuteTransactionAsync<T>", c => c.ExecuteTransactionAsync(_ => ActInt(c), cancellationToken: TestContext.Current.CancellationToken).AsTask()),
		new("ExecuteTransactionAsync", c => c.ExecuteTransactionAsync(_ => Act(c), cancellationToken: TestContext.Current.CancellationToken).AsTask()),
	];

	[Theory]
	[InlineData("open")]
	[InlineData("begin")]
	[InlineData("action")]
	[InlineData("commit")]
	[InlineData("dispose")]
	[InlineData("close")]
	public static async Task TransactionAsync_NeverPostsBackToTheCallersContext(string gatedOperation)
	{
		foreach (var call in Calls)
		{
			var (posts, gated) = await Run(gatedOperation, call.Value);

			Assert.True(gated == 1, $"{call.Key}: the '{gatedOperation}' operation was never awaited, so nothing was tested.");
			Assert.True(posts == 0, $"{call.Key}: resumed on the caller's context {posts} time(s) after an asynchronous '{gatedOperation}'.");
		}
	}
}
