using System.Data.Common;
using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Edge cases of Retrieve and RetrieveAsync (a reader or a command, by ordinals, by names, with or without a token):
// the rows and the columns that come back, NULLs, a column the reader lacks, cancellation before and during the read,
// and what the call leaves behind when it opened the connection (it closes it) or was given one that was open (it leaves it open).
[ExcludeFromCodeCoverage]
public static class RetrieveSurfaceTests
{
	static (DbTally Tally, TrackedConnection Connection, DbCommand Command) Command(bool open, DbScript? script = null)
	{
		var tally = new DbTally(script ?? new DbScript { Columns = Columns, Rows = TwoRows });
		TrackedConnection connection = open ? tally.NewOpenConnection() : tally.NewConnection();
		return (tally, connection, connection.CreateCommand());
	}

	static void AssertConnectionAsFound(TrackedConnection connection, bool wasOpen)
	{
		Assert.Equal(0, connection.Disposes); // a connection that was handed in is never disposed
		if (wasOpen)
		{
			Assert.Equal(ConnectionState.Open, connection.State);
			Assert.Equal(0, connection.Closes);
		}
		else
		{
			Assert.Equal(ConnectionState.Closed, connection.State);
			Assert.Equal(1, connection.Opens);
			Assert.Equal(1, connection.Closes);
		}
	}

	// ---- Retrieve over a reader ----

	[Theory]
	[InlineData(0)]
	[InlineData(3)]
	public static void Retrieve_Reader_BuffersEveryRow_AndReportsTheColumns(int rowCount)
	{
		TypedReader reader = Reader(Rows(rowCount));

		QueryResultQueue<object[]> result = reader.Retrieve();

		Assert.Equal(["Id", "Name", "Note"], result.Names.ToArray());
		Assert.Equal([0, 1, 2], result.Ordinals.ToArray());
		Assert.Equal(rowCount, result.Result.Count);
		Assert.Equal(rowCount, reader.RowsRead); // everything was read before the call returned, not on demand
		if (rowCount != 0)
			Assert.Equal([1, "n1", DBNull.Value], result.Result.Peek()); // a NULL stays DBNull
	}

	[Theory]
	[InlineData("first ordinal and others")]
	[InlineData("first name and others")]
	public static void Retrieve_Reader_SelectedColumns_ComeBackInTheOrderAsked(string variant)
	{
		TypedReader reader = Reader([1, "a", "n1"], [2, "b", "n2"]);
		QueryResultQueue<object[]> result = variant switch
		{
			"first ordinal and others" => reader.Retrieve(2, 0),
			_ => reader.Retrieve("Note", "Id"),
		};

		Assert.Equal(["Note", "Id"], result.Names.ToArray());
		Assert.Equal([2, 0], result.Ordinals.ToArray());
		Assert.Equal<object[]>([["n1", 1], ["n2", 2]], result.Result.ToArray());
	}

	[Fact]
	public static void Retrieve_Reader_UnknownColumn_NamesEveryMissingColumn_BeforeReadingARow()
	{
		TypedReader reader = Reader([1, "a", "n1"]);
		IEnumerable<string> names = ["Id", "Nope", "Zip"];

		IndexOutOfRangeException ex = Assert.Throws<IndexOutOfRangeException>(() => reader.Retrieve(names));

		Assert.Contains("Nope", ex.Message);
		Assert.Contains("Zip", ex.Message);
		Assert.Equal(0, reader.RowsRead);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task Retrieve_NoOrdinals_ReturnsAnEmptyRowForEveryRecord(bool useAsync)
	{
		TypedReader reader = Reader(TwoRows);

		QueryResultQueue<object[]> result = useAsync
			? await reader.RetrieveAsync(Array.Empty<int>(), TestContext.Current.CancellationToken)
			: reader.Retrieve(Array.Empty<int>());

		Assert.Equal(0, result.ColumnCount);
		Assert.Equal(2, result.Result.Count);
		Assert.All(result.Result, row => Assert.Empty(row));
	}

	// ---- Retrieve over a command (the library opens the connection only if it was closed, and closes what it opened) ----

	static QueryResultQueue<object[]> RetrieveVia(IDbCommand command, string variant)
	{
		IEnumerable<int> ordinals = [2, 0];
		return variant switch
		{
			"all" => command.Retrieve(),
			"ordinals" => command.Retrieve(ordinals),
			"first ordinal and others" => command.Retrieve(2, 0),
			_ => command.Retrieve("Note", "Id"),
		};
	}

	[Theory]
	[InlineData("all", "Id,Name,Note")]
	[InlineData("first ordinal and others", "Note,Id")]
	[InlineData("first name and others", "Note,Id")]
	public static void Retrieve_Command_OpensAClosedConnection_ReadsEverythingThenClosesIt(string variant, string columnNames)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Command(open: false);

		QueryResultQueue<object[]> result = RetrieveVia(command, variant);

		Assert.Equal(columnNames, string.Join(",", result.Names));
		Assert.Equal(2, result.Result.Count); // buffered, so it is usable after the connection is closed
		AssertConnectionAsFound(connection, wasOpen: false);
		Assert.All(tally.Readers, r => Assert.Equal(1, r.Disposes));
	}

	[Theory]
	[InlineData("ordinals")]
	public static void Retrieve_Command_LeavesAnOpenConnectionOpen(string variant)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Command(open: true);

		QueryResultQueue<object[]> result = RetrieveVia(command, variant);

		Assert.Equal(2, result.Result.Count);
		AssertConnectionAsFound(connection, wasOpen: true);
		Assert.All(tally.Readers, r => Assert.Equal(1, r.Disposes));
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void Retrieve_Command_AlwaysReadsSequentially_AndOnlyClosesTheConnectionItOpened(bool open)
	{
		var connection = new FakeDbConnection();
		if (open) connection.SetState(ConnectionState.Open);
		var command = new FakeDbCommand(connection, () => Reader(TwoRows));

		command.Retrieve(CommandBehavior.SingleResult);

		CommandBehavior behavior = Assert.Single(command.Behaviors);
		Assert.True(behavior.HasFlag(CommandBehavior.SequentialAccess));
		Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)); // the caller's flag is kept
		Assert.Equal(!open, behavior.HasFlag(CommandBehavior.CloseConnection));
	}

	[Fact]
	public static void Retrieve_Command_WhenAReadFailsHalfWay_DisposesTheReaderAndClosesWhatItOpened()
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Command(open: false, new DbScript { Columns = Columns, Rows = Rows(5), FailOnRead = 3 });

		Assert.Throws<InvalidOperationException>(() => command.Retrieve());

		Assert.Equal(1, Assert.Single(tally.Readers).Disposes);
		AssertConnectionAsFound(connection, wasOpen: false);
	}

	// ---- RetrieveAsync over a reader ----

	[Theory]
	[InlineData(3, true)]
	[InlineData(3, false)]
	public static async Task RetrieveAsync_Reader_BuffersEveryRow_WithOrWithoutReadAsync(int rowCount, bool useReadAsync)
	{
		TypedReader reader = Reader(Rows(rowCount));

		QueryResultQueue<object[]> result = await reader.RetrieveAsync(useReadAsync, TestContext.Current.CancellationToken);

		Assert.Equal(["Id", "Name", "Note"], result.Names.ToArray());
		Assert.Equal([0, 1, 2], result.Ordinals.ToArray());
		Assert.Equal(rowCount, result.Result.Count);
		Assert.Equal(rowCount, reader.RowsRead);
		Assert.Equal([1, "n1", DBNull.Value], result.Result.Peek());
	}

	static ValueTask<QueryResultQueue<object[]>> RetrieveAsyncVia(DbDataReader reader, string variant, CancellationToken token)
	{
		IEnumerable<string> names = ["Note", "Id"];
		return variant switch
		{
			"token" => reader.RetrieveAsync(token),
			"names, token" => reader.RetrieveAsync(names, token),
			"first ordinal and others" => reader.RetrieveAsync(2, 0),
			"first name and others" => reader.RetrieveAsync("Note", "Id"),
			_ => reader.RetrieveAsync(token, "Note", "Id"),
		};
	}

	static bool SelectsColumns(string variant) => variant != "token";

	[Theory]
	[InlineData("token")]
	[InlineData("names, token")]
	[InlineData("first ordinal and others")]
	[InlineData("first name and others")]
	[InlineData("token, first name and others")]
	public static async Task RetrieveAsync_Reader_EveryOverloadReturnsTheSameRows(string variant)
	{
		TypedReader reader = Reader([1, "a", "n1"], [2, "b", "n2"]);

		QueryResultQueue<object[]> result = await RetrieveAsyncVia(reader, variant, CancellationToken.None);

		Assert.Equal(2, result.Result.Count);
		if (SelectsColumns(variant))
		{
			Assert.Equal(["Note", "Id"], result.Names.ToArray());
			Assert.Equal<object[]>([["n1", 1], ["n2", 2]], result.Result.ToArray());
		}
		else
		{
			Assert.Equal(["Id", "Name", "Note"], result.Names.ToArray());
		}
	}

	[Theory]
	[InlineData("token")]
	public static async Task RetrieveAsync_Reader_ACancelledToken_ThrowsBeforeAnyRowIsRead(string variant)
	{
		TypedReader reader = Reader(TwoRows);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await RetrieveAsyncVia(reader, variant, cts.Token));

		Assert.Equal(0, reader.RowsRead);
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static async Task RetrieveAsync_Reader_CancelledInTheMiddle_Throws_AndReadsNoFurther(bool useReadAsync)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(5, n =>
		{
			if (n == 2) cts.Cancel();
		});

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.RetrieveAsync(useReadAsync, cts.Token));

		Assert.Equal(2, reader.Reads);
	}

	// ---- RetrieveAsync over a command ----

	static ValueTask<QueryResultQueue<object[]>> RetrieveAsyncVia(DbCommand command, string variant, CancellationToken token)
	{
		IEnumerable<int> ordinals = [2, 0];
		IEnumerable<string> names = ["Note", "Id"];
		return variant switch
		{
			"token" => command.RetrieveAsync(token),
			"ordinals, token" => command.RetrieveAsync(ordinals, token),
			_ => command.RetrieveAsync(names, token), // "names, token"
		};
	}

	[Theory]
	[InlineData("token", "Id,Name,Note")]
	[InlineData("names, token", "Note,Id")]
	public static async Task RetrieveAsync_Command_OpensAClosedConnection_ReadsEverythingThenClosesIt(string variant, string columnNames)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Command(open: false);

		QueryResultQueue<object[]> result = await RetrieveAsyncVia(command, variant, CancellationToken.None);

		Assert.Equal(columnNames, string.Join(",", result.Names));
		Assert.Equal(2, result.Result.Count);
		AssertConnectionAsFound(connection, wasOpen: false);
		Assert.All(tally.Readers, r => Assert.Equal(1, r.Disposes));
	}

	[Theory]
	[InlineData("ordinals, token")]
	public static async Task RetrieveAsync_Command_LeavesAnOpenConnectionOpen(string variant)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Command(open: true);

		QueryResultQueue<object[]> result = await RetrieveAsyncVia(command, variant, CancellationToken.None);

		Assert.Equal(2, result.Result.Count);
		AssertConnectionAsFound(connection, wasOpen: true);
		Assert.All(tally.Readers, r => Assert.Equal(1, r.Disposes));
	}

	[Theory]
	[InlineData("names, token")]
	public static async Task RetrieveAsync_Command_ACancelledToken_ThrowsAndLeavesNothingOpen(string variant)
	{
		(DbTally tally, TrackedConnection connection, DbCommand command) = Command(open: false);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await RetrieveAsyncVia(command, variant, cts.Token));

		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(connection.Opens, connection.Closes);
		Assert.Equal(0, connection.Disposes);
		Assert.All(tally.Readers, r => Assert.Equal(1, r.Disposes));
	}

	// ---- argument guards ----

	[Theory]
	[InlineData("command.Retrieve()", "command")]
	[InlineData("reader.RetrieveAsync()", "reader")]
	public static async Task Retrieve_ANullReaderOrCommand_IsRefusedByName(string call, string parameter)
	{
		ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(async () =>
		{
			switch (call)
			{
				case "command.Retrieve()": ((IDbCommand)null!).Retrieve(); break;
				case "reader.RetrieveAsync()": await ((DbDataReader)null!).RetrieveAsync(TestContext.Current.CancellationToken); break;
			}
		});

		Assert.Equal(parameter, ex.ParamName);
	}
}
