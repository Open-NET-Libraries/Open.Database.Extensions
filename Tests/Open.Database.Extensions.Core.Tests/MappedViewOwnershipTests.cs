using System.Data.Common;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The reader view never opens, closes or disposes the caller's reader
// and never calls NextResult on it.
// The command view opens a closed connection and closes (not disposes)
// what it opened, leaves an open one open,
// and reads through the later result sets unless SingleResult() was called.
[ExcludeFromCodeCoverage]
public static class MappedViewOwnershipTests
{
	sealed record Row(int Id, string? Name);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// Counts what the library does to a reader it does not own.
	sealed class CountingReader(object?[][] rows)
		: TypedReader(Columns, rows)
	{
		public int NextResults { get; private set; }
		public int Closes { get; private set; }
		public int Disposes { get; private set; }

		public override bool NextResult()
		{
			NextResults++;
			return false;
		}

		public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
		{
			NextResults++;
			return Task.FromResult(false);
		}

		public override void Close()
			=> Closes++;

		protected override void Dispose(bool disposing)
		{
			if (disposing) Disposes++;
			base.Dispose(disposing);
		}

		public void AssertUntouched()
		{
			Assert.Equal(0, NextResults);
			Assert.Equal(0, Closes);
			Assert.Equal(0, Disposes);
		}
	}

	public static TheoryData<string> Terminals
		=> [
			"ToList", "ToArray", "FirstOrDefault", "SingleOrDefault",
			"ToDictionary", "ToHashSet", "Read", "Skip", "Take"];

	// SingleOrDefault throws on a second row, as on a MappedQuery,
	// so it reads a one-row set.
	static object?[][] RowsFor(string terminal)
		=> Rows(terminal == "SingleOrDefault" ? 1 : 3);

	static void Sync(MappedReader<Row> q, string terminal)
	{
		switch (terminal)
		{
			case "ToList": q.ToList(); break;
			case "ToArray": q.ToArray(); break;
			case "FirstOrDefault": q.FirstOrDefault(); break;
			case "SingleOrDefault": q.SingleOrDefault(); break;
			case "ToDictionary": q.ToDictionary(r => r.Id); break;
			case "ToHashSet": q.ToHashSet(); break;
			case "Read": q.Read(items => items.Count()); break;
			case "Skip": q.Skip(1).ToList(); break;
			case "Take": q.Take(1).ToList(); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	static async Task Async(MappedReader<Row> q, string terminal)
	{
		switch (terminal)
		{
			case "ToList": await q.ToListAsync(Ct); break;
			case "ToArray": await q.ToArrayAsync(Ct); break;
			case "FirstOrDefault": await q.FirstOrDefaultAsync(Ct); break;
			case "SingleOrDefault": await q.SingleOrDefaultAsync(Ct); break;
			case "ToDictionary": await q.ToDictionaryAsync(r => r.Id, null, Ct); break;
			case "ToHashSet": await q.ToHashSetAsync(null, Ct); break;
			case "Read":
				await q.ReadAsync(
					async (items, t) =>
					{
						await foreach (Row _ in items.WithCancellation(t)) { }
					},
					Ct);
				break;
			case "Skip": await q.Skip(1).ToListAsync(Ct); break;
			case "Take": await q.Take(1).ToListAsync(Ct); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	[Theory, MemberData(nameof(Terminals))]
	public static void TheReaderView_NeverCallsNextResultCloseOrDispose(
		string terminal)
	{
		using var reader = new CountingReader(RowsFor(terminal));
		Sync(reader.Map<Row>(), terminal);
		reader.AssertUntouched();
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task TheReaderView_NeverCallsNextResultCloseOrDispose_Async(
		string terminal)
	{
		await using var reader = new CountingReader(RowsFor(terminal));
		await Async(reader.Map<Row>(), terminal);
		reader.AssertUntouched();
	}

	[Fact]
	public static void TheReaderView_ReadsFromTheCurrentPosition()
	{
		using var reader = new CountingReader(Rows(3));
		Assert.True(reader.Read());

		Assert.Equal([2, 3], reader.Map<Row>().ToList().Select(r => r.Id));
		reader.AssertUntouched();
	}

	[Fact]
	public static void TheReaderView_DoesNotReadThroughALaterSet_EvenWhenItFails()
	{
		var reader = new MultiSetReader(Rows(2), 2, failAtCall: 1);

		Assert.Equal(2, reader.Map<Row>().ToList().Count);
		Assert.Equal(0, reader.NextResultCalls);
	}

	[Fact]
	public static async Task TheReaderView_DoesNotReadThroughALaterSet_Async()
	{
		var reader = new MultiSetReader(Rows(2), 2, failAtCall: 1);

		Assert.Equal(2, (await reader.Map<Row>().ToListAsync(Ct)).Count);
		Assert.Equal(0, reader.NextResultCalls);
	}

	[Fact]
	public static void TheReaderView_WorksOnAReaderThatIsNotADbDataReader()
	{
		IDataReader reader = FakeReader.Create(["Id", "Name"], [1, "a"], [2, null]);

		Assert.Equal(2, reader.Map<Row>().ToList().Count);
		reader.DidNotReceive().NextResult();
		reader.DidNotReceive().Close();
		reader.DidNotReceive().Dispose();
	}

	[Fact]
	public static async Task TheReaderView_WorksOnAReaderThatIsNotADbDataReader_Async()
	{
		IDataReader reader = FakeReader.Create(["Id", "Name"], [1, "a"], [2, null]);

		Assert.Equal(2, (await reader.Map<Row>().ToListAsync(Ct)).Count);
		reader.DidNotReceive().NextResult();
		reader.DidNotReceive().Dispose();
	}

	// ---- the command view ----

	static (FakeDbConnection Connection, FakeDbCommand Command) Command(
		ConnectionState state = ConnectionState.Closed,
		Func<DbDataReader>? reader = null)
	{
		var connection = new FakeDbConnection();
		connection.SetState(state);
		return (connection, new FakeDbCommand(connection, reader ?? (() => Reader(Rows(2)))));
	}

	[Fact]
	public static void AClosedConnection_IsOpenedThenClosed()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command();

		Assert.Equal(2, command.Map<Row>().ToList().Count);

		Assert.Equal(["open", "close"], connection.Log);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static async Task AClosedConnection_IsOpenedThenClosed_Async()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command();

		Assert.Equal(2, (await command.Map<Row>().ToListAsync(Ct)).Count);

		Assert.Equal(["open", "close"], connection.Log);
	}

	[Fact]
	public static void AnOpenConnection_IsLeftOpen()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command(ConnectionState.Open);

		Assert.Equal(2, command.Map<Row>().ToList().Count);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Empty(connection.Log);
	}

	[Fact]
	public static async Task AnOpenConnection_IsLeftOpen_Async()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command(ConnectionState.Open);

		Assert.Equal(2, (await command.Map<Row>().ToListAsync(Ct)).Count);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Empty(connection.Log);
	}

	[Fact]
	public static void AConnectionTheViewOpened_IsClosedWhenTheHandlerThrows()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command();

		Assert.Throws<InvalidOperationException>(
			() => command.Map<Row>().Read(_ => throw new InvalidOperationException()));

		Assert.Equal(["open", "close"], connection.Log);
	}

	[Fact]
	public static async Task AConnectionTheViewOpened_IsClosedWhenTheHandlerThrows_Async()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command();

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await command.Map<Row>().ReadAsync(
				(_, _) => throw new InvalidOperationException(),
				Ct));

		Assert.Equal(["open", "close"], connection.Log);
	}

	[Fact]
	public static void TheConnectionAndTheCommand_AreNeverDisposed()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command();
		command.Map<Row>().ToList();

		// Both are still usable: a second terminal runs on the same pair.
		Assert.Equal(2, command.Map<Row>().ToList().Count);
		Assert.Equal(["open", "close", "open", "close"], connection.Log);
	}

	[Fact]
	public static void ACommandWithoutAReaderThatIsADbCommand_RunsSynchronously()
	{
		var probe = new ConnectionProbe
		{
			ReaderFactory = () => FakeReader.Create(["Id", "Name"], [1, "a"]),
		};

		Assert.Single(probe.Command.Map<Row>().ToList());

		Assert.Equal("Open,ExecuteReader,DisposeReader,Close", probe.Log);
		Assert.Equal(
			CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection,
			probe.Behaviors.Single());
	}

	[Fact]
	public static async Task ACommandThatIsNotADbCommand_RunsAsyncTerminalsToo()
	{
		var probe = new ConnectionProbe(ConnectionState.Open)
		{
			ReaderFactory = () => FakeReader.Create(["Id", "Name"], [1, "a"]),
		};

		Assert.Single(await probe.Command.Map<Row>().ToListAsync(Ct));

		Assert.Equal("ExecuteReader,DisposeReader", probe.Log);
		Assert.Equal(CommandBehavior.SequentialAccess, probe.Behaviors.Single());
	}

	// ---- behaviors and read-through ----

	[Fact]
	public static void TheBehaviors_AreThoseOfAnExpressiveCommandsMappedQuery()
	{
		(_, FakeDbCommand closed) = Command();
		closed.Map<Row>().ToList();
		closed.Map<Row>().SingleResult().ToList();
		closed.Map<Row>().SingleResult().FirstOrDefault();
		closed.Map(r => r.GetInt32(0)).ToList();
		closed.Map(r => r.GetInt32(0)).SingleResult().ToList();

		Assert.Equal(
			[
				CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection,
				CommandBehavior.SingleResult | CommandBehavior.SequentialAccess
					| CommandBehavior.CloseConnection,
				CommandBehavior.SingleResult | CommandBehavior.SequentialAccess
					| CommandBehavior.SingleRow | CommandBehavior.CloseConnection,
				CommandBehavior.CloseConnection,
				CommandBehavior.SingleResult | CommandBehavior.CloseConnection,
			],
			closed.Behaviors);

		(_, FakeDbCommand open) = Command(ConnectionState.Open);
		open.Map<Row>().ToList();
		open.Map(r => r.GetInt32(0)).ToList();

		Assert.Equal(
			[CommandBehavior.SequentialAccess, CommandBehavior.Default],
			open.Behaviors);
	}

	[Fact]
	public static void TheCommandView_ReadsThroughTheLaterSets()
	{
		MultiSetReader? reader = null;
		(_, FakeDbCommand command) = Command(
			reader: () => reader = new MultiSetReader(Rows(1), 2));

		command.Map<Row>().ToList();

		Assert.Equal(3, reader!.NextResultCalls);
		Assert.Equal(0, reader.ReadsAfterNextResult);
	}

	[Fact]
	public static async Task TheCommandView_ReadsThroughTheLaterSets_Async()
	{
		MultiSetReader? reader = null;
		(_, FakeDbCommand command) = Command(
			reader: () => reader = new MultiSetReader(Rows(1), 2));

		await command.Map<Row>().ToListAsync(Ct);

		Assert.Equal(3, reader!.NextResultCalls);
		Assert.Equal(0, reader.ReadsAfterNextResult);
	}

	[Fact]
	public static void TheCommandView_ThrowsALaterFailure_AndClosesTheConnection()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command(
			reader: () => new MultiSetReader(Rows(1), 2, failAtCall: 2));

		Assert.Throws<LaterSetFailure>(() => command.Map<Row>().ToList());

		Assert.Equal(["open", "close"], connection.Log);
	}

	[Fact]
	public static async Task TheCommandView_ThrowsALaterFailure_Async()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command(
			reader: () => new MultiSetReader(Rows(1), 2, failAtCall: 2));

		await Assert.ThrowsAsync<LaterSetFailure>(
			async () => await command.Map<Row>().ToListAsync(Ct));

		Assert.Equal(["open", "close"], connection.Log);
	}

	[Fact]
	public static void SingleResult_ReadsNothingThrough()
	{
		MultiSetReader? reader = null;
		(_, FakeDbCommand command) = Command(
			reader: () => reader = new MultiSetReader(Rows(1), 2, failAtCall: 1));

		Assert.Single(command.Map<Row>().SingleResult().ToList());
		Assert.Equal(0, reader!.NextResultCalls);
	}

	[Fact]
	public static async Task SingleResult_ReadsNothingThrough_Async()
	{
		MultiSetReader? reader = null;
		(_, FakeDbCommand command) = Command(
			reader: () => reader = new MultiSetReader(Rows(1), 2, failAtCall: 1));

		Assert.Single(await command.Map<Row>().SingleResult().ToListAsync(Ct));
		Assert.Equal(0, reader!.NextResultCalls);
	}

	[Fact]
	public static async Task ACanceledToken_ThrowsBeforeAnythingOpens()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Command();
		using var cts = new CancellationTokenSource();
		await cts.CancelAsync();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await command.Map<Row>().ToListAsync(cts.Token));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Reader(Rows(1)).Map<Row>().ToListAsync(cts.Token));

		Assert.Empty(connection.Log);
	}

	// SingleResult(), reached through Skip or Take, is a no-op on a reader view:
	// it already reads only the current result set.
	[Fact]
	public static void SingleResultThroughSkipAndTake_IsANoOp_OnAReaderView()
	{
		using var reader = new CountingReader(Rows(3));

		Assert.Equal(
			[2, 3],
			reader.Map<Row>().Skip(1).SingleResult().ToList().Select(r => r.Id));
		reader.AssertUntouched();

		using var other = new CountingReader(Rows(3));
		Assert.Single(other.Map<Row>().Take(1).SingleResult().ToList());
		Assert.Single(other.Map<Row>().Skip(1).Take(1).SingleResult().ToList());
		other.AssertUntouched();
	}

	[Fact]
	public static async Task SingleResultThroughSkipAndTake_IsANoOp_OnAReaderView_Async()
	{
		await using var reader = new CountingReader(Rows(3));

		Assert.Equal(
			2,
			(await reader.Map<Row>().Skip(1).SingleResult().ToListAsync(Ct)).Count);
		reader.AssertUntouched();
	}
}
