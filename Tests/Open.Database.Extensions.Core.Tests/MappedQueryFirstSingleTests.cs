using System.Data;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// First() and Single() are the throwing twins of FirstOrDefault() and
// SingleOrDefault(): the same single pass over the rows, and an
// InvalidOperationException instead of null for no rows (for Single also for
// more than one row). The error for no rows is thrown after the call has
// cleaned up, so the later result sets are read through first.
[ExcludeFromCodeCoverage]
public static class MappedQueryFirstSingleTests
{
	sealed record Person
	{
		public required int Id { get; init; }
		public string? Name { get; init; }
	}

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	const int LaterSets = 2; // a full read-through is three calls of NextResult

	static FakeExecuteReader Command(int rows)
		=> new(() => Reader(Rows(rows)));

	// A selector query asks the provider for exactly the flags the caller's
	// calls give it (a generated mapping adds SequentialAccess).
	static MappedQuery<int> Query(FakeExecuteReader command)
		=> command.Map(r => r.GetInt32(0));

	static FakeExecuteReader Sets(
		int rows,
		out Func<MultiSetReader> reader,
		int? failAt = null)
	{
		MultiSetReader? last = null;
		reader = () => last ?? throw new InvalidOperationException("never executed");
		return new FakeExecuteReader(
			() => last = new MultiSetReader(Rows(rows), LaterSets, failAt));
	}

	static FakeExecuteReader One(Type column, object? cell)
		=> new(() => TypedReader.Create(
			[("v", column)],
			[[cell]]));

	static DataTable Table(int count)
	{
		DataTable table = new();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		for (int i = 1; i <= count; i++)
		{
			table.Rows.Add(i, "n" + i);
		}

		return table;
	}

	// ---- every level: expressive command, reader, command, table ----

	sealed record Terminals(
		Func<Person> First,
		Func<Person> Single,
		Func<CancellationToken, ValueTask<Person>> FirstAsync,
		Func<CancellationToken, ValueTask<Person>> SingleAsync,
		Func<Person?> FirstOrDefault,
		Func<Person?> SingleOrDefault,
		Func<CancellationToken, ValueTask<Person?>> FirstOrDefaultAsync,
		Func<CancellationToken, ValueTask<Person?>> SingleOrDefaultAsync);

	static Terminals Of(MappedQuery<Person> q)
		=> new(
			q.First, q.Single, q.FirstAsync, q.SingleAsync,
			q.FirstOrDefault, q.SingleOrDefault,
			q.FirstOrDefaultAsync, q.SingleOrDefaultAsync);

	static Terminals Of(MappedQueryWithSkip<Person> q)
		=> new(
			q.First, q.Single, q.FirstAsync, q.SingleAsync,
			q.FirstOrDefault, q.SingleOrDefault,
			q.FirstOrDefaultAsync, q.SingleOrDefaultAsync);

	static Terminals Of(MappedQueryWithTake<Person> q)
		=> new(
			q.First, q.Single, q.FirstAsync, q.SingleAsync,
			q.FirstOrDefault, q.SingleOrDefault,
			q.FirstOrDefaultAsync, q.SingleOrDefaultAsync);

	static Terminals Of(MappedReader<Person> q)
		=> new(
			q.First, q.Single, q.FirstAsync, q.SingleAsync,
			q.FirstOrDefault, q.SingleOrDefault,
			q.FirstOrDefaultAsync, q.SingleOrDefaultAsync);

	static Terminals Of(MappedCommand<Person> q)
		=> new(
			q.First, q.Single, q.FirstAsync, q.SingleAsync,
			q.FirstOrDefault, q.SingleOrDefault,
			q.FirstOrDefaultAsync, q.SingleOrDefaultAsync);

	static Terminals Of(MappedTable<Person> q)
		=> new(
			q.First, q.Single, q.FirstAsync, q.SingleAsync,
			q.FirstOrDefault, q.SingleOrDefault,
			q.FirstOrDefaultAsync, q.SingleOrDefaultAsync);

	// The same terminals through the extensions on the interface.
	// Not an overload of Of: a struct would bind to it by mistake.
	static Terminals OfBoxed(IMappedQuery<Person> q)
		=> new(
			q.First, q.Single, q.FirstAsync, q.SingleAsync,
			q.FirstOrDefault, q.SingleOrDefault,
			q.FirstOrDefaultAsync, q.SingleOrDefaultAsync);

	// A new source every call: a reader view is spent once it has been read.
	static Terminals At(string level, int rows)
		=> level switch
		{
			"command" => Of(Command(rows).Map<Person>()),
			"reader" => Of(Reader(Rows(rows)).Map<Person>()),
			"commandView" => Of(
				new FakeDbCommand(
					new FakeDbConnection(),
					() => Reader(Rows(rows))).Map<Person>()),
			"table" => Of(Table(rows).Map<Person>()),
			_ => throw new ArgumentOutOfRangeException(nameof(level)),
		};

	public static TheoryData<string> Levels
		=> ["command", "reader", "commandView", "table"];

	static void AssertNoRows(InvalidOperationException ex, string terminal)
	{
		Assert.Contains("no elements", ex.Message);
		Assert.Contains("no rows", ex.Message);
		Assert.Contains(terminal, ex.Message);
	}

	static void AssertMoreThanOne(InvalidOperationException ex)
		=> Assert.Contains("more than one", ex.Message);

	[Theory, MemberData(nameof(Levels))]
	public static async Task OneRow_FirstAndSingleReturnIt(string level)
	{
		Assert.Equal(1, At(level, 1).First().Id);
		Assert.Equal(1, At(level, 1).Single().Id);
		Assert.Equal("n1", (await At(level, 1).FirstAsync(Ct)).Name);
		Assert.Equal("n1", (await At(level, 1).SingleAsync(Ct)).Name);
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task TwoRows_FirstReturnsTheFirst_SingleThrows(
		string level)
	{
		Assert.Equal(1, At(level, 2).First().Id);
		Assert.Equal(1, (await At(level, 2).FirstAsync(Ct)).Id);

		AssertMoreThanOne(Assert.Throws<InvalidOperationException>(
			() => At(level, 2).Single()));
		AssertMoreThanOne(await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await At(level, 2).SingleAsync(Ct)));
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task NoRows_BothThrow_AndTheMessageNamesTheTerminal(
		string level)
	{
		AssertNoRows(
			Assert.Throws<InvalidOperationException>(
				() => At(level, 0).First()),
			"First");
		AssertNoRows(
			Assert.Throws<InvalidOperationException>(
				() => At(level, 0).Single()),
			"Single");
		AssertNoRows(
			await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await At(level, 0).FirstAsync(Ct)),
			"FirstAsync");
		AssertNoRows(
			await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await At(level, 0).SingleAsync(Ct)),
			"SingleAsync");
	}

	// ---- Skip and Take ----

	[Fact]
	public static void WithSkipAndTake_FirstAndSingleSeeOnlyTheWindow()
	{
		Assert.Equal(1, Command(3).Map<int>().Take(1).Single()); // no second row is looked for
		Assert.Equal(2, Command(3).Map<int>().Skip(1).First());
		Assert.Equal(2, Command(3).Map<int>().Skip(1).Take(1).Single());
		Assert.Equal(2, Command(3).Map<int>().Skip(1).Take(2).First());
		Assert.Equal(3, Command(3).Map<int>().Skip(2).Single());

		AssertMoreThanOne(Assert.Throws<InvalidOperationException>(
			() => Command(3).Map<int>().Skip(1).Single()));
		AssertMoreThanOne(Assert.Throws<InvalidOperationException>(
			() => Command(3).Map<int>().Take(2).Single()));
		AssertNoRows(
			Assert.Throws<InvalidOperationException>(
				() => Command(3).Map<int>().Skip(3).First()),
			"First");
		AssertNoRows(
			Assert.Throws<InvalidOperationException>(
				() => Command(3).Map<int>().Skip(3).Single()),
			"Single");
		AssertNoRows(
			Assert.Throws<InvalidOperationException>(
				() => Command(3).Map<int>().Take(0).First()),
			"First");
		AssertNoRows(
			Assert.Throws<InvalidOperationException>(
				() => Command(3).Map<int>().Take(0).Single()),
			"Single");
	}

	[Fact]
	public static async Task WithSkipAndTake_FirstAndSingleSeeOnlyTheWindow_Async()
	{
		Assert.Equal(1, await Command(3).Map<int>().Take(1).SingleAsync(Ct));
		Assert.Equal(2, await Command(3).Map<int>().Skip(1).FirstAsync(Ct));
		Assert.Equal(
			2,
			await Command(3).Map<int>().Skip(1).Take(1).SingleAsync(Ct));

		AssertMoreThanOne(await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Command(3).Map<int>().Skip(1).SingleAsync(Ct)));
		AssertNoRows(
			await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await Command(3).Map<int>().Skip(3).FirstAsync(Ct)),
			"FirstAsync");
		AssertNoRows(
			await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await Command(3).Map<int>().Take(0).SingleAsync(Ct)),
			"SingleAsync");
	}

	[Fact]
	public static void First_ReadsOneRow_AndSingleReadsASecondOnlyToCheck()
	{
		TypedReader? reader = null;
		FakeExecuteReader command = new(() => reader = Reader(Rows(3)));

		Assert.Equal(1, command.Map<int>().First());
		Assert.Equal(1, reader!.RowsRead);

		Assert.Throws<InvalidOperationException>(
			() => command.Map<int>().Single());
		Assert.Equal(2, reader.RowsRead);

		Assert.Equal(1, command.Map<int>().Take(1).Single());
		Assert.Equal(1, reader.RowsRead);
	}

	// ---- a single value, and NULL ----

	[Fact]
	public static async Task ASingleValue_IsTheFirstColumn()
	{
		Assert.Equal(5, One(typeof(int), 5).Map<int>().First());
		Assert.Equal(5, One(typeof(int), 5).Map<int>().Single());
		Assert.Equal(5, await One(typeof(int), 5).Map<int>().FirstAsync(Ct));
		Assert.Equal(5, await One(typeof(int), 5).Map<int>().SingleAsync(Ct));
		Assert.Equal(1, Command(3).Map<int>().First()); // Id is the first column
	}

	[Fact]
	public static async Task ANullValue_IsARow_NotNoRows()
	{
		Assert.Equal(0, One(typeof(int), null).Map<int>().First());
		Assert.Equal(0, One(typeof(int), null).Map<int>().Single());
		Assert.Null(One(typeof(int), null).Map<int?>().First());
		Assert.Null(One(typeof(int), null).Map<int?>().Single());
		Assert.Null(One(typeof(string), null).Map<string?>().First());
		Assert.Null(One(typeof(string), null).Map<string?>().Single());

		Assert.Equal(
			0,
			await One(typeof(int), null).Map<int>().FirstAsync(Ct));
		Assert.Null(
			await One(typeof(int), null).Map<int?>().SingleAsync(Ct));
		Assert.Null(
			await One(typeof(string), null).Map<string?>().FirstAsync(Ct));
		Assert.Null(
			await One(typeof(string), null).Map<string?>().SingleAsync(Ct));
	}

	// ---- cancellation ----

	[Fact]
	public static async Task ACanceledToken_Cancels_FirstAsyncAndSingleAsync()
	{
		using var cts = new CancellationTokenSource();
		await cts.CancelAsync();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Command(1).Map<int>().FirstAsync(cts.Token));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Command(1).Map<int>().SingleAsync(cts.Token));
	}

	[Fact]
	public static async Task TheCommandsOwnToken_Cancels_Too()
	{
		using var cts = new CancellationTokenSource();
		await cts.CancelAsync();
		FakeExecuteReader command = Command(1);
		command.CancellationToken = cts.Token;

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await command.Map<int>().FirstAsync(Ct));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await command.Map<int>().SingleAsync(Ct));
	}

	[Fact]
	public static async Task TheCallsToken_ReachesTheReader()
	{
		using var cts = new CancellationTokenSource();
		FakeExecuteReader command = Command(1);

		await command.Map<int>().FirstAsync(cts.Token);
		await command.Map<int>().SingleAsync(cts.Token);

		Assert.Equal([cts.Token, cts.Token], command.ReaderTokens);
	}

	// ---- later result sets ----

	static int Run(MappedQuery<int> query, string terminal)
		=> terminal == "First" ? query.First() : query.Single();

	static async Task<int> RunAsync(
		MappedQuery<int> query,
		string terminal,
		CancellationToken token)
		=> terminal == "First"
			? await query.FirstAsync(token)
			: await query.SingleAsync(token);

	[Theory]
	[InlineData("First")]
	[InlineData("Single")]
	public static async Task ARow_ReadsThroughTheLaterSets_ThenIsReturned(
		string terminal)
	{
		FakeExecuteReader sync = Sets(1, out Func<MultiSetReader> syncReader);
		Assert.Equal(1, Run(Query(sync), terminal));
		Assert.Equal(LaterSets + 1, syncReader().NextResultCalls);
		Assert.Equal(0, syncReader().ReadsAfterNextResult);

		FakeExecuteReader viaAsync = Sets(1, out Func<MultiSetReader> asyncReader);
		Assert.Equal(1, await RunAsync(Query(viaAsync), terminal, Ct));
		Assert.Equal(LaterSets + 1, asyncReader().NextResultCalls);
		Assert.Equal(LaterSets + 1, asyncReader().AsyncCalls);
		Assert.Equal(0, asyncReader().ReadsAfterNextResult);
	}

	[Theory]
	[InlineData("First")]
	[InlineData("Single")]
	public static async Task ALaterFailure_AfterARow_IsThrown(string terminal)
	{
		FakeExecuteReader sync = Sets(
			1, out Func<MultiSetReader> syncReader, failAt: 2);
		Assert.Throws<LaterSetFailure>(() => Run(Query(sync), terminal));
		Assert.Equal(2, syncReader().NextResultCalls);

		FakeExecuteReader viaAsync = Sets(
			1, out Func<MultiSetReader> asyncReader, failAt: 1);
		await Assert.ThrowsAsync<LaterSetFailure>(
			async () => await RunAsync(Query(viaAsync), terminal, Ct));
		Assert.Equal(1, asyncReader().NextResultCalls);
	}

	[Theory]
	[InlineData("First")]
	[InlineData("Single")]
	public static async Task NoRows_ReadsThrough_ThenThrowsNoRows(
		string terminal)
	{
		FakeExecuteReader sync = Sets(0, out Func<MultiSetReader> syncReader);
		Assert.Throws<InvalidOperationException>(
			() => Run(Query(sync), terminal));
		Assert.Equal(LaterSets + 1, syncReader().NextResultCalls);

		FakeExecuteReader viaAsync = Sets(0, out Func<MultiSetReader> asyncReader);
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await RunAsync(Query(viaAsync), terminal, Ct));
		Assert.Equal(LaterSets + 1, asyncReader().NextResultCalls);
	}

	// The no-rows error waits for the cleanup, so a later failure comes first.
	[Theory]
	[InlineData("First")]
	[InlineData("Single")]
	public static async Task NoRows_WithALaterFailure_ThrowsTheLaterFailure(
		string terminal)
	{
		FakeExecuteReader sync = Sets(
			0, out Func<MultiSetReader> syncReader, failAt: 2);
		Assert.Throws<LaterSetFailure>(() => Run(Query(sync), terminal));
		Assert.Equal(2, syncReader().NextResultCalls);

		FakeExecuteReader viaAsync = Sets(
			0, out Func<MultiSetReader> asyncReader, failAt: 1);
		await Assert.ThrowsAsync<LaterSetFailure>(
			async () => await RunAsync(Query(viaAsync), terminal, Ct));
		Assert.Equal(1, asyncReader().NextResultCalls);
	}

	// Single's own error is thrown at the second row, before any read-through.
	[Fact]
	public static async Task Single_WithTwoRows_ThrowsMoreThanOne_NeverReplacedByALaterFailure()
	{
		FakeExecuteReader sync = Sets(
			2, out Func<MultiSetReader> syncReader, failAt: 1);
		AssertMoreThanOne(Assert.Throws<InvalidOperationException>(
			() => Query(sync).Single()));
		Assert.Equal(0, syncReader().NextResultCalls);

		FakeExecuteReader viaAsync = Sets(
			2, out Func<MultiSetReader> asyncReader, failAt: 1);
		AssertMoreThanOne(await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Query(viaAsync).SingleAsync(Ct)));
		Assert.Equal(0, asyncReader().NextResultCalls);
	}

	[Theory]
	[InlineData("First")]
	[InlineData("Single")]
	public static async Task SingleResult_ReadsNothingThrough(string terminal)
	{
		// One row and a failing later set: the row is returned.
		FakeExecuteReader row = Sets(
			1, out Func<MultiSetReader> rowReader, failAt: 1);
		Assert.Equal(1, Run(Query(row).SingleResult(), terminal));
		Assert.Equal(0, rowReader().NextResultCalls);

		// No row: no-rows is the only error there can be.
		FakeExecuteReader none = Sets(
			0, out Func<MultiSetReader> noneReader, failAt: 1);
		Assert.Throws<InvalidOperationException>(
			() => Run(Query(none).SingleResult(), terminal));
		Assert.Equal(0, noneReader().NextResultCalls);

		FakeExecuteReader viaAsync = Sets(
			0, out Func<MultiSetReader> asyncReader, failAt: 1);
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await RunAsync(
				Query(viaAsync).SingleResult(), terminal, Ct));
		Assert.Equal(0, asyncReader().NextResultCalls);
	}

	[Fact]
	public static async Task OnlyFirst_AsksTheProviderForOneRow_AndOnlyAfterSingleResult()
	{
		const CommandBehavior Result = CommandBehavior.SingleResult;
		const CommandBehavior ResultAndRow
			= CommandBehavior.SingleResult | CommandBehavior.SingleRow;
		FakeExecuteReader command = Command(3);

		Query(command).First(); // nothing says one result set
		Query(command).Skip(2).Single();
		Query(command).Take(1).Single();
		Query(command).SingleResult().First(); // nothing skipped: SingleRow
		Query(command).Skip(1).SingleResult().First(); // a row is skipped first
		Query(command).SingleResult().Take(1).Single(); // never SingleRow: it could hide a second row
		await Query(command).SingleResult().FirstAsync(Ct);
		await Query(command).SingleResult().Take(1).SingleAsync(Ct);

		Assert.Equal(
			[
				CommandBehavior.Default,
				CommandBehavior.Default,
				CommandBehavior.Default,
				ResultAndRow,
				Result,
				Result,
				ResultAndRow,
				Result,
			],
			command.Behaviors);
	}

	// ---- connection ownership: the throw leaves the connection as found ----

	[Fact]
	public static async Task TheCommandView_ClosesTheConnectionItOpened_WhenThereIsNoRow()
	{
		var connection = new FakeDbConnection();
		var command = new FakeDbCommand(connection, () => Reader(Rows(0)));

		Assert.Throws<InvalidOperationException>(
			() => command.Map<Person>().First());
		Assert.Equal(["open", "close"], connection.Log);
		Assert.Equal(ConnectionState.Closed, connection.State);

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await command.Map<Person>().SingleAsync(Ct));
		Assert.Equal(["open", "close", "open", "close"], connection.Log);
	}

	[Fact]
	public static async Task TheCommandView_LeavesAnOpenConnectionOpen_WhenThereIsNoRow()
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		var command = new FakeDbCommand(connection, () => Reader(Rows(0)));

		Assert.Throws<InvalidOperationException>(
			() => command.Map<Person>().Single());
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await command.Map<Person>().FirstAsync(Ct));

		Assert.Empty(connection.Log);
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Fact]
	public static async Task TheCommandView_ClosesTheConnectionItOpened_WhenSingleFindsTwo()
	{
		var connection = new FakeDbConnection();
		var command = new FakeDbCommand(connection, () => Reader(Rows(2)));

		Assert.Throws<InvalidOperationException>(
			() => command.Map<Person>().Single());
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await command.Map<Person>().SingleAsync(Ct));

		Assert.Equal(["open", "close", "open", "close"], connection.Log);
	}

	// ---- a reader that is not a DbDataReader ----

	static IDataReader Plain(params bool[] reads)
	{
		IDataReader reader = Substitute.For<IDataReader>();
		reader.Read().Returns(reads[0], reads[1..]);
		reader.NextResult().Returns(true, true, false);
		return reader;
	}

	[Fact]
	public static async Task APlainIDataReader_IsReadTheSameWay()
	{
		Assert.Equal(
			7,
			new PlainReaderCommand(() => Plain(true, false))
				.Map(_ => 7).First());
		Assert.Equal(
			7,
			new PlainReaderCommand(() => Plain(true, false))
				.Map(_ => 7).Single());
		Assert.Equal(
			7,
			await new PlainReaderCommand(() => Plain(true, true, false))
				.Map(_ => 7).FirstAsync(Ct));
		Assert.Equal(
			7,
			await new PlainReaderCommand(() => Plain(true, false))
				.Map(_ => 7).SingleAsync(Ct));

		Assert.Throws<InvalidOperationException>(
			() => new PlainReaderCommand(() => Plain(false))
				.Map(_ => 7).First());
		Assert.Throws<InvalidOperationException>(
			() => new PlainReaderCommand(() => Plain(true, true, false))
				.Map(_ => 7).Single());
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await new PlainReaderCommand(() => Plain(false))
				.Map(_ => 7).SingleAsync(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await new PlainReaderCommand(() => Plain(true, true, false))
				.Map(_ => 7).SingleAsync(Ct));
	}

	[Fact]
	public static void APlainIDataReader_NoRow_ReadsThroughBeforeItThrows()
	{
		IDataReader reader = Plain(false);

		Assert.Throws<InvalidOperationException>(
			() => new PlainReaderCommand(() => reader).Map(_ => 7).First());

		reader.Received(3).NextResult();
	}

	// ---- through IMappedQuery<T> (the extensions, for generic code) ----

	[Fact]
	public static async Task ThroughTheInterface_TheExtensionsDoTheSame()
	{
		static IMappedQuery<int> Q(int rows) => Command(rows).Map<int>();

		Assert.Equal(1, Q(2).First());
		Assert.Equal(1, Q(1).Single());
		Assert.Equal(1, await Q(2).FirstAsync(Ct));
		Assert.Equal(1, await Q(1).SingleAsync(Ct));

		Assert.Throws<InvalidOperationException>(() => Q(0).First());
		Assert.Throws<InvalidOperationException>(() => Q(0).Single());
		Assert.Throws<InvalidOperationException>(() => Q(2).Single());
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Q(0).FirstAsync(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Q(0).SingleAsync(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Q(2).SingleAsync(Ct));
	}

	[Fact]
	public static async Task ThroughTheInterface_ANullQueryIsRefused()
	{
		IMappedQuery<int> query = null!;

		static void Refused(Action call)
			=> Assert.Equal(
				"query",
				Assert.Throws<ArgumentNullException>(call).ParamName);

		static async Task RefusedAsync(Func<Task> call)
			=> Assert.Equal(
				"query",
				(await Assert.ThrowsAsync<ArgumentNullException>(call)).ParamName);

		Refused(() => query.First());
		Refused(() => query.Single());
		Refused(() => query.FirstOrDefault());
		Refused(() => query.SingleOrDefault());
		await RefusedAsync(async () => await query.FirstAsync(Ct));
		await RefusedAsync(async () => await query.SingleAsync(Ct));
		await RefusedAsync(async () => await query.FirstOrDefaultAsync(Ct));
		await RefusedAsync(async () => await query.SingleOrDefaultAsync(Ct));
	}

	// ---- through IMappedQuery<T>: the six library types, eight terminals ----

	// The extensions on the interface are the same call as the members for
	// the library types: the same no-rows message, and the later
	// result sets are read through. Only another implementation of
	// IMappedQuery<T> falls back to LINQ over its Read handler.

	public static TheoryData<string> InterfaceLevels
		=> ["query", "skip", "take", "reader", "commandView", "table"];

	// The levels that read through the later result sets.
	public static TheoryData<string> ReadThroughLevels
		=> ["query", "skip", "take", "commandView"];

	static MultiSetReader SetsReader(int rows, int? failAt)
		=> new(Rows(rows), LaterSets, failAt);

	static MappedQuery<Person> QueryOf(int rows, int? failAt)
		=> Sets(rows, out _, failAt).Map<Person>();

	static MappedCommand<Person> CommandOf(int rows, int? failAt)
		=> new FakeDbCommand(
			new FakeDbConnection(),
			() => SetsReader(rows, failAt)).Map<Person>();

	// The query as its own type, for the members.
	static Terminals MembersAt(string level, int rows, int? failAt)
		=> level switch
		{
			"query" => Of(QueryOf(rows, failAt)),
			"skip" => Of(QueryOf(rows, failAt).Skip(0)),
			"take" => Of(QueryOf(rows, failAt).Take(10)),
			"reader" => Of(SetsReader(rows, failAt).Map<Person>()),
			"commandView" => Of(CommandOf(rows, failAt)),
			"table" => Of(Table(rows).Map<Person>()),
			_ => throw new ArgumentOutOfRangeException(nameof(level)),
		};

	// The same query as an IMappedQuery<T>, for the extensions.
	static IMappedQuery<Person> BoxedAt(string level, int rows, int? failAt)
		=> level switch
		{
			"query" => QueryOf(rows, failAt),
			"skip" => QueryOf(rows, failAt).Skip(0),
			"take" => QueryOf(rows, failAt).Take(10),
			"reader" => SetsReader(rows, failAt).Map<Person>(),
			"commandView" => CommandOf(rows, failAt),
			"table" => Table(rows).Map<Person>(),
			_ => throw new ArgumentOutOfRangeException(nameof(level)),
		};

	static string Describe(object? result)
		=> result is Person person ? $"row {person.Id}" : "null";

	static string Outcome<T>(Func<T> run)
	{
		try
		{
			return Describe(run());
		}
		catch (Exception ex)
		{
			return $"{ex.GetType().Name}: {ex.Message}";
		}
	}

	static async Task<string> OutcomeAsync<T>(Func<ValueTask<T>> run)
	{
		try
		{
			return Describe(await run());
		}
		catch (Exception ex)
		{
			return $"{ex.GetType().Name}: {ex.Message}";
		}
	}

	// What each of the eight terminals did, in the order of
	// First, Single, FirstOrDefault, SingleOrDefault and their async twins.
	// A new source for every call: a reader view is spent once it is read.
	static async Task<string[]> Outcomes(Func<Terminals> fresh)
		=> [
			Outcome(() => fresh().First()),
			Outcome(() => fresh().Single()),
			Outcome(() => fresh().FirstOrDefault()),
			Outcome(() => fresh().SingleOrDefault()),
			await OutcomeAsync(() => fresh().FirstAsync(Ct)),
			await OutcomeAsync(() => fresh().SingleAsync(Ct)),
			await OutcomeAsync(() => fresh().FirstOrDefaultAsync(Ct)),
			await OutcomeAsync(() => fresh().SingleOrDefaultAsync(Ct)),
		];

	// Asserts the interface did what the members did, and returns it.
	static async Task<string[]> SameAsTheMembers(
		string level,
		int rows,
		int? failAt = null)
	{
		string[] members = await Outcomes(
			() => MembersAt(level, rows, failAt));
		string[] boxed = await Outcomes(
			() => OfBoxed(BoxedAt(level, rows, failAt)));

		Assert.Equal(members, boxed);
		return boxed;
	}

	[Theory, MemberData(nameof(InterfaceLevels))]
	public static async Task ThroughTheInterface_EveryTerminalDoesWhatTheMembersDo(
		string level)
	{
		await SameAsTheMembers(level, 0);
		await SameAsTheMembers(level, 1);
		await SameAsTheMembers(level, 2);
		await SameAsTheMembers(level, 1, failAt: 1);
		await SameAsTheMembers(level, 1, failAt: 2);
		await SameAsTheMembers(level, 0, failAt: 1);
		await SameAsTheMembers(level, 2, failAt: 1);
	}

	[Theory, MemberData(nameof(InterfaceLevels))]
	public static async Task ThroughTheInterface_NoRows_ThrowTheLibraryMessage_NamingTheTerminal(
		string level)
	{
		string[] outcomes = await SameAsTheMembers(level, 0);

		Assert.StartsWith("InvalidOperationException: ", outcomes[0]);
		Assert.Contains("no rows, and First needs one", outcomes[0]);
		Assert.Contains("no rows, and Single needs one", outcomes[1]);
		Assert.Equal("null", outcomes[2]);
		Assert.Equal("null", outcomes[3]);
		Assert.Contains("no rows, and FirstAsync needs one", outcomes[4]);
		Assert.Contains("no rows, and SingleAsync needs one", outcomes[5]);
		Assert.Equal("null", outcomes[6]);
		Assert.Equal("null", outcomes[7]);
	}

	[Theory, MemberData(nameof(InterfaceLevels))]
	public static async Task ThroughTheInterface_Rows_AreFirstSingleOrTooMany(
		string level)
	{
		Assert.All(
			await SameAsTheMembers(level, 1),
			o => Assert.Equal("row 1", o));

		string[] two = await SameAsTheMembers(level, 2);
		Assert.Equal("row 1", two[0]);
		Assert.Contains("more than one", two[1]);
		Assert.Equal("row 1", two[2]);
		Assert.Contains("more than one", two[3]);
		Assert.Equal("row 1", two[4]);
		Assert.Contains("more than one", two[5]);
		Assert.Equal("row 1", two[6]);
		Assert.Contains("more than one", two[7]);
	}

	// The failure of a later result set is thrown through the interface too:
	// the terminals stop at the row, then read the later sets through.
	[Theory, MemberData(nameof(ReadThroughLevels))]
	public static async Task ThroughTheInterface_ALaterFailure_IsThrown_ForEveryTerminal(
		string level)
	{
		foreach (int rows in new[] { 1, 0 })
		{
			foreach (int failAt in new[] { 1, 2 })
			{
				Assert.All(
					await SameAsTheMembers(level, rows, failAt),
					o => Assert.StartsWith("LaterSetFailure: ", o));
			}
		}
	}

	// A reader view is never read through, and a table has one result set.
	[Theory]
	[InlineData("reader")]
	[InlineData("table")]
	public static async Task ThroughTheInterface_AReaderViewOrATable_IsNotReadThrough(
		string level)
		=> Assert.All(
			await SameAsTheMembers(level, 1, failAt: 1),
			o => Assert.Equal("row 1", o));

	// The later sets are walked in full after the row, as for the members.
	[Fact]
	public static async Task ThroughTheInterface_ReadsThroughTheLaterSets_ThenReturns()
	{
		FakeExecuteReader sync = Sets(1, out Func<MultiSetReader> syncReader);
		IMappedQuery<Person> boxed = sync.Map<Person>();
		Assert.Equal(1, boxed.First().Id);
		Assert.Equal(LaterSets + 1, syncReader().NextResultCalls);
		Assert.Equal(0, syncReader().ReadsAfterNextResult);

		FakeExecuteReader viaAsync = Sets(1, out Func<MultiSetReader> asyncReader);
		IMappedQuery<Person> boxedAsync = viaAsync.Map<Person>();
		Assert.Equal(1, (await boxedAsync.SingleAsync(Ct)).Id);
		Assert.Equal(LaterSets + 1, asyncReader().NextResultCalls);
		Assert.Equal(LaterSets + 1, asyncReader().AsyncCalls);
		Assert.Equal(0, asyncReader().ReadsAfterNextResult);
	}

	// ---- another implementation of IMappedQuery<T>: the LINQ fallback ----

	// A tiny list-backed query that is not one of the library types.
	sealed class ListQuery<T>(params T[] rows) : IMappedQuery<T>
	{
		public int Enumerated { get; private set; }

		IEnumerable<T> Items()
		{
			foreach (T row in rows)
			{
				Enumerated++;
				yield return row;
			}
		}

		async IAsyncEnumerable<T> ItemsAsync()
		{
			foreach (T row in Items())
			{
				await Task.Yield();
				yield return row;
			}
		}

		public void Read(Action<IEnumerable<T>> handler)
			=> handler(Items());

		public TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler)
			=> handler(Items());

		public async ValueTask ReadAsync(
			Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler,
			CancellationToken cancellationToken)
			=> await handler(ItemsAsync(), cancellationToken);

		public async ValueTask<TResult> ReadAsync<TResult>(
			Func<
				IAsyncEnumerable<T>,
				CancellationToken,
				ValueTask<TResult>> handler,
			CancellationToken cancellationToken)
			=> await handler(ItemsAsync(), cancellationToken);
	}

	static Person Row(int id)
		=> new() { Id = id };

	[Fact]
	public static async Task AnotherImplementation_StillWorks_ThroughTheLinqFallback()
	{
		static IMappedQuery<Person> Q(int rows)
			=> new ListQuery<Person>(
				[.. Enumerable.Range(1, rows).Select(Row)]);

		Assert.Equal(1, Q(3).First().Id);
		Assert.Equal(1, Q(1).Single().Id);
		Assert.Equal(1, Q(3).FirstOrDefault()?.Id);
		Assert.Equal(1, Q(1).SingleOrDefault()?.Id);
		Assert.Equal(1, (await Q(3).FirstAsync(Ct)).Id);
		Assert.Equal(1, (await Q(1).SingleAsync(Ct)).Id);
		Assert.Equal(1, (await Q(3).FirstOrDefaultAsync(Ct))?.Id);
		Assert.Equal(1, (await Q(1).SingleOrDefaultAsync(Ct))?.Id);

		Assert.Null(Q(0).FirstOrDefault());
		Assert.Null(Q(0).SingleOrDefault());
		Assert.Null(await Q(0).FirstOrDefaultAsync(Ct));
		Assert.Null(await Q(0).SingleOrDefaultAsync(Ct));

		// No rows, and more than one: an InvalidOperationException, but the
		// fallback's message is LINQ's and does not name the terminal.
		Assert.Throws<InvalidOperationException>(() => Q(0).First());
		Assert.Throws<InvalidOperationException>(() => Q(0).Single());
		Assert.Throws<InvalidOperationException>(() => Q(2).Single());
		Assert.Throws<InvalidOperationException>(() => Q(2).SingleOrDefault());
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Q(0).FirstAsync(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Q(0).SingleAsync(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Q(2).SingleAsync(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Q(2).SingleOrDefaultAsync(Ct));
	}

	[Fact]
	public static void AnotherImplementation_FirstStopsAtTheFirstRow()
	{
		ListQuery<Person> three = new(Row(1), Row(2), Row(3));

		Assert.Equal(1, ((IMappedQuery<Person>)three).First().Id);
		Assert.Equal(1, three.Enumerated);
	}

	// ---- the command shortcuts ----

	[Fact]
	public static async Task TheCommandShortcuts_AreMapThenTheTerminal()
	{
		Assert.Equal(1, await Command(2).FirstAsync<int>(Ct));
		Assert.Equal(1, await Command(1).SingleAsync<int>(Ct));
		Assert.Equal(1, (await Command(2).FirstAsync<Person>(Ct)).Id);
		Assert.Equal("n1", (await Command(1).SingleAsync<Person>(Ct)).Name);

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Command(0).FirstAsync<int>(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Command(0).SingleAsync<int>(Ct));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Command(2).SingleAsync<int>(Ct));
	}

	// ---- overload resolution (compile-only) ----

#pragma warning disable xUnit1051 // The point is the forms that pass no token.
	[Fact]
	public static async Task TheNewMembers_DoNotMakeNeighbouringCallsAmbiguous()
	{
		FakeExecuteReader command = Command(2);

		// The transform-era terminals on a command still bind to themselves.
		Assert.Equal(1, command.First(r => r.GetInt32(0)));
		Assert.Throws<InvalidOperationException>(
			() => command.Single(r => r.GetInt32(0)));

		// Every form of the new ones.
		MappedQuery<int> q = command.Map<int>();
		Assert.Equal(1, q.First());
		Assert.Equal(1, await q.FirstAsync());
		Assert.Equal(1, await q.FirstAsync(Ct));
		Assert.Equal(1, await q.FirstAsync(cancellationToken: Ct));
		Assert.Equal(1, await q.Skip(0).FirstAsync());
		Assert.Equal(1, await q.Skip(0).Take(1).SingleAsync());
		Assert.Equal(1, await command.FirstAsync<int>());
		Assert.Equal(1, await command.FirstAsync<int>(Ct));
		IMappedQuery<int> boxed = q;
		Assert.Equal(1, boxed.First());
		Assert.Equal(1, await boxed.FirstAsync());
		Assert.Throws<InvalidOperationException>(() => boxed.Single());
	}
#pragma warning restore xUnit1051
}
