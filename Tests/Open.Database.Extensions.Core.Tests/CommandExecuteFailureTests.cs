using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A command that is given a connection opens it only when it is closed and closes (never disposes) only what it opened.
// When the provider cannot execute the command there is no reader yet, so CloseConnection has nothing to act on:
// the call itself has to close the connection it opened, and must leave one the caller opened exactly as it was.
[ExcludeFromCodeCoverage]
public static class CommandExecuteFailureTests
{
	[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors", Justification = "Test Only")]
	sealed class ProviderFailed : Exception;

	// An IDbCommand that is not a DbCommand: the extensions take their synchronous path with it.
	sealed class OnlyDbCommand(IDbCommand inner) : IDbCommand
	{
		[AllowNull]
		public string CommandText { get => inner.CommandText; set => inner.CommandText = value; }
		public int CommandTimeout { get => inner.CommandTimeout; set => inner.CommandTimeout = value; }
		public CommandType CommandType { get => inner.CommandType; set => inner.CommandType = value; }
		public IDbConnection? Connection { get => inner.Connection; set => inner.Connection = value; }
		public IDataParameterCollection Parameters => inner.Parameters;
		public IDbTransaction? Transaction { get => inner.Transaction; set => inner.Transaction = value; }
		public UpdateRowSource UpdatedRowSource { get => inner.UpdatedRowSource; set => inner.UpdatedRowSource = value; }
		public void Cancel() => inner.Cancel();
		public IDbDataParameter CreateParameter() => inner.CreateParameter();
		public void Dispose() => inner.Dispose();
		public int ExecuteNonQuery() => inner.ExecuteNonQuery();
		public IDataReader ExecuteReader() => inner.ExecuteReader();
		public IDataReader ExecuteReader(CommandBehavior behavior) => inner.ExecuteReader(behavior);
		public object? ExecuteScalar() => inner.ExecuteScalar();
		public void Prepare() => inner.Prepare();
	}

	static readonly Dictionary<string, Func<IDbCommand, object?>> SyncCalls = new()
	{
		["ToList"] = c => c.ToList(r => r.GetValue(0)),
		["ToArray"] = c => c.ToArray(r => r.GetValue(0)),
		["ToImmutableArray"] = c => c.ToImmutableArray(r => r.GetValue(0)),
		["ToDataTable"] = c => c.ToDataTable(),
		["ToDataTables"] = c => c.ToDataTables(),
		["ExecuteReader(handler)"] = c => { c.ExecuteReader(_ => { }); return null; },
		["ExecuteReader(transform)"] = c => c.ExecuteReader(r => r.FieldCount),
		["IterateReader(selector)"] = c => c.IterateReader(r => r.GetValue(0), e => e.Count()),
		["IterateReader(handler)"] = c => { c.IterateReader(_ => { }); return null; },
		["IterateReader(behavior, handler)"] = c => { c.IterateReader(CommandBehavior.Default, _ => { }); return null; },
		["IterateReaderWhile"] = c => { c.IterateReaderWhile(_ => true); return null; },
		["First"] = c => c.First(r => r.GetValue(0)),
		["FirstOrDefault"] = c => c.FirstOrDefault(r => r.GetValue(0)),
		["Single"] = c => c.Single(r => r.GetValue(0)),
		["SingleOrDefault"] = c => c.SingleOrDefault(r => r.GetValue(0)),
		["Take"] = c => c.Take(1, r => r.GetValue(0)),
		["Skip"] = c => c.Skip(1, r => r.GetValue(0)),
		["SkipThenTake"] = c => c.SkipThenTake(1, 1, r => r.GetValue(0)),
		["FirstOrdinalResults"] = c => c.FirstOrdinalResults(),
		["FirstOrdinalResults<T>"] = c => c.FirstOrdinalResults<int>(),
		["Retrieve()"] = c => c.Retrieve(),
		["Retrieve(ordinals)"] = c => c.Retrieve([0, 1]),
		["Retrieve(names)"] = c => c.Retrieve(["id", "name"]),
	};

	static readonly Dictionary<string, Func<DbCommand, CancellationToken, Task>> AsyncCalls = new()
	{
		["ToListAsync(transform)"] = (c, t) => c.ToListAsync(r => r.GetValue(0), cancellationToken: t).AsTask(),
		["ToListAsync(behavior, transform)"] = (c, t) => c.ToListAsync(CommandBehavior.Default, r => r.GetValue(0), true, t).AsTask(),
		["ToListAsync(async transform)"] = (c, t) => c.ToListAsync(CommandBehavior.Default, r => new ValueTask<object>(r.GetValue(0)), t).AsTask(),
		["ExecuteReaderAsync(Action)"] = (c, t) => c.ExecuteReaderAsync(_ => { }, CommandBehavior.Default, t).AsTask(),
		["ExecuteReaderAsync(Func<ValueTask>)"] = (c, t) => c.ExecuteReaderAsync(_ => default, CommandBehavior.Default, t).AsTask(),
		["ExecuteReaderAsync<T>(Func<T>)"] = (c, t) => c.ExecuteReaderAsync(r => r.FieldCount, CommandBehavior.Default, t).AsTask(),
		["ExecuteReaderAsync<T>(Func<ValueTask<T>>)"] = (c, t) => c.ExecuteReaderAsync(r => new ValueTask<int>(r.FieldCount), CommandBehavior.Default, t).AsTask(),
		["ExecuteReaderAsync(IDbCommand, Func<ValueTask>)"] = (c, t) => ((IDbCommand)c).ExecuteReaderAsync(_ => default, CommandBehavior.Default, t).AsTask(),
		["ExecuteReaderAsync<T>(IDbCommand, Func<ValueTask<T>>)"] = (c, t) => ((IDbCommand)c).ExecuteReaderAsync(r => new ValueTask<int>(r.FieldCount), CommandBehavior.Default, t).AsTask(),
		["ForEachAsync(Action)"] = (c, t) => c.ForEachAsync(_ => { }, true, t).AsTask(),
		["ForEachAsync(behavior, Action)"] = (c, t) => c.ForEachAsync(_ => { }, CommandBehavior.Default, true, t).AsTask(),
		["ForEachAsync(Func<ValueTask>)"] = (c, t) => c.ForEachAsync(_ => default, true, t).AsTask(),
		["ForEachAsync(behavior, Func<ValueTask>)"] = (c, t) => c.ForEachAsync(_ => default, CommandBehavior.Default, true, t).AsTask(),
		["IterateReaderWhileAsync"] = (c, t) => c.IterateReaderWhileAsync(_ => new ValueTask<bool>(true), CommandBehavior.Default, true, t).AsTask(),
		["FirstOrdinalResultsAsync"] = (c, t) => c.FirstOrdinalResultsAsync(CommandBehavior.Default, true, t).AsTask(),
		["FirstOrdinalResultsAsync<T>"] = (c, t) => c.FirstOrdinalResultsAsync<int>(CommandBehavior.Default, true, t).AsTask(),
		["RetrieveAsync"] = (c, t) => c.RetrieveAsync(true, t).AsTask(),
	};

	public static TheoryData<string> SyncNames() => [.. SyncCalls.Keys];

	public static TheoryData<string> AsyncNames() => [.. AsyncCalls.Keys];

	// One name per overload shape, for the variants that reach the same helper branch as the full lists above.
	public static TheoryData<string> RepresentativeSyncNames() => ["ToList", "ExecuteReader(handler)", "First", "Retrieve()"];

	public static TheoryData<string> RepresentativeAsyncNames() => ["ToListAsync(transform)", "ExecuteReaderAsync(Action)", "ForEachAsync(Action)", "RetrieveAsync"];

	static (DbTally Tally, TrackedConnection Connection, DbCommand Command) Closed(Exception failure)
	{
		var tally = new DbTally(new() { ExecuteFailure = failure });
		TrackedConnection connection = tally.NewConnection();
		return (tally, connection, connection.CreateCommand());
	}

	static (DbTally Tally, TrackedConnection Connection, DbCommand Command) Open(Exception failure)
	{
		var tally = new DbTally(new() { ExecuteFailure = failure });
		TrackedConnection connection = tally.NewOpenConnection();
		return (tally, connection, connection.CreateCommand());
	}

	// The call opened the connection, so the call closes it: once, and it is never disposed.
	static void OpenedAndClosedOnce(DbTally tally, TrackedConnection connection)
	{
		Assert.Equal(1, connection.Opens);
		Assert.Equal(1, connection.Closes);
		Assert.Equal(0, connection.Disposes);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Empty(tally.Readers);
	}

	// The caller opened it: not opened, not closed, not disposed.
	static void LeftAsItWas(DbTally tally, TrackedConnection connection)
	{
		Assert.Equal(0, connection.Opens);
		Assert.Equal(0, connection.Closes);
		Assert.Equal(0, connection.Disposes);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Empty(tally.Readers);
	}

	[Theory]
	[MemberData(nameof(SyncNames))]
	public static void Sync_ProviderFails_ClosesTheConnectionItOpened(string call)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Closed(new ProviderFailed());

		Assert.Throws<ProviderFailed>(() => SyncCalls[call](command));

		OpenedAndClosedOnce(tally, connection);
	}

	[Theory]
	[MemberData(nameof(RepresentativeSyncNames))]
	public static void Sync_ProviderFails_LeavesTheCallersOpenConnectionAlone(string call)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Open(new ProviderFailed());

		Assert.Throws<ProviderFailed>(() => SyncCalls[call](command));

		LeftAsItWas(tally, connection);
	}

	[Fact]
	public static void Sync_NotADbCommand_ProviderFails_ClosesTheConnectionItOpened()
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Closed(new ProviderFailed());

		Assert.Throws<ProviderFailed>(() => new OnlyDbCommand(command).ToList(r => r.GetValue(0)));

		OpenedAndClosedOnce(tally, connection);
	}

	[Fact]
	public static async Task Async_NotADbCommand_ProviderFails_ClosesTheConnectionItOpened()
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Closed(new ProviderFailed());
		IDbCommand notDb = new OnlyDbCommand(command);

		await Assert.ThrowsAsync<ProviderFailed>(async () => await notDb.ExecuteReaderAsync(_ => default, cancellationToken: TestContext.Current.CancellationToken));
		await Assert.ThrowsAsync<ProviderFailed>(async () => await notDb.ExecuteReaderAsync(r => new ValueTask<int>(r.FieldCount), cancellationToken: TestContext.Current.CancellationToken));

		Assert.Equal(2, connection.Opens);
		Assert.Equal(2, connection.Closes);
		Assert.Equal(0, connection.Disposes);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Empty(tally.Readers);
	}

	[Theory]
	[MemberData(nameof(AsyncNames))]
	public static async Task Async_ProviderFails_ClosesTheConnectionItOpened(string call)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Closed(new ProviderFailed());

		await Assert.ThrowsAsync<ProviderFailed>(() => AsyncCalls[call](command, CancellationToken.None));

		OpenedAndClosedOnce(tally, connection);
	}

	[Theory]
	[MemberData(nameof(RepresentativeAsyncNames))]
	public static async Task Async_ProviderFails_LeavesTheCallersOpenConnectionAlone(string call)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Open(new ProviderFailed());

		await Assert.ThrowsAsync<ProviderFailed>(() => AsyncCalls[call](command, CancellationToken.None));

		LeftAsItWas(tally, connection);
	}

	// The provider reports a cancellation while it executes (the fake throws OperationCanceledException; the token passed in is not cancelled).
	[Theory]
	[MemberData(nameof(RepresentativeAsyncNames))]
	public static async Task Async_CancelledWhileExecuting_ClosesTheConnectionItOpened(string call)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Closed(new OperationCanceledException());

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AsyncCalls[call](command, CancellationToken.None));

		OpenedAndClosedOnce(tally, connection);
	}

	// A token that is already cancelled stops the call, and nothing is left open.
	[Theory]
	[MemberData(nameof(RepresentativeAsyncNames))]
	public static async Task Async_CancelledBefore_LeavesNothingOpen(string call)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Closed(new ProviderFailed());
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AsyncCalls[call](command, cts.Token));

		Assert.Equal(connection.Opens, connection.Closes);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Empty(tally.Readers);
	}

	[Theory]
	[MemberData(nameof(RepresentativeAsyncNames))]
	public static async Task Async_CancelledBefore_LeavesTheCallersOpenConnectionAlone(string call)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Open(new ProviderFailed());
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AsyncCalls[call](command, cts.Token));

		LeftAsItWas(tally, connection);
	}

	// What must not change: when the command executes, the connection the call opened is closed when the reader is disposed.
	[Theory]
	[MemberData(nameof(RepresentativeSyncNames))]
	public static void Sync_Succeeds_ClosesTheConnectionItOpenedOnce(string call)
	{
		var tally = new DbTally();
		TrackedConnection connection = tally.NewConnection();

		SyncCalls[call](connection.CreateCommand());

		Assert.Equal(1, connection.Opens);
		Assert.Equal(1, connection.Closes);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Theory]
	[MemberData(nameof(RepresentativeAsyncNames))]
	public static async Task Async_Succeeds_ClosesTheConnectionItOpenedOnce(string call)
	{
		var tally = new DbTally();
		TrackedConnection connection = tally.NewConnection();

		await AsyncCalls[call](connection.CreateCommand(), CancellationToken.None);

		Assert.Equal(1, connection.Opens);
		Assert.Equal(1, connection.Closes);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}
}
