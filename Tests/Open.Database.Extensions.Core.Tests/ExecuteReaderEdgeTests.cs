using System.Data.Common;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Edge cases of the extensions on IExecuteReader: what the provider is asked for (the behavior flags), how much of the result is read,
// negative and zero counts, a result set loaded into a table, and the asynchronous forms over a real command with its cancellation token and connection.
[ExcludeFromCodeCoverage]
public static class ExecuteReaderEdgeTests
{
	static int Id(IDataRecord record) => record.GetInt32(0);

	static string? Name(IDataRecord record) => record.GetString(1);

	/// <summary>A command over <paramref name="rowCount"/> rows; <paramref name="rowsRead"/> says how many of them the last execution read.</summary>
	static FakeExecuteReader Command(int rowCount, out Func<int> rowsRead)
	{
		TypedReader? last = null;
		rowsRead = () => last?.RowsRead ?? 0;
		return new FakeExecuteReader(() => last = Reader(Rows(rowCount)));
	}

	// ---- First / Single ----

	[Fact]
	public static void First_OfNoRows_Throws_AndFirstOrDefault_ReturnsDefault()
	{
		FakeExecuteReader command = Command(0, out _);

		Assert.Throws<InvalidOperationException>(() => command.First(Id));
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Null(command.FirstOrDefault(Name));
		#pragma warning restore CS0618
	}

	[Theory]
	[InlineData("First")]
	public static void First_AsksTheProviderForOneRow_AndReadsOnlyThatRow(string op)
	{
		FakeExecuteReader command = Command(5, out Func<int> rowsRead);

		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		int id = op == "First" ? command.First(Id) : command.FirstOrDefault(Id);
		#pragma warning restore CS0618

		Assert.Equal(1, id);
		Assert.Equal(1, rowsRead());
		CommandBehavior behavior = Assert.Single(command.Behaviors);
		Assert.True(behavior.HasFlag(CommandBehavior.SingleRow));
		Assert.True(behavior.HasFlag(CommandBehavior.SingleResult));
	}

	[Theory]
	[InlineData("Single")]
	[InlineData("SingleOrDefault")]
	public static void Single_WithTwoRows_Throws_SoTheProviderMustNotBeLimitedToOneRow(string op)
	{
		FakeExecuteReader command = Command(2, out _);

		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Throws<InvalidOperationException>(() => op == "Single" ? command.Single(Id) : command.SingleOrDefault(Id));
		#pragma warning restore CS0618

		Assert.False(Assert.Single(command.Behaviors).HasFlag(CommandBehavior.SingleRow)); // with it, the second row could never be seen
	}

	// ---- Take / Skip / SkipThenTake ----

	[Theory]
	[InlineData("Take 2", "1,2")]
	[InlineData("SkipThenTake 1 3", "2,3,4")]
	[InlineData("Skip 9", "")]
	public static void TheSlice_IsWhatTheCountsSay_InTheOrderTheRowsCame(string call, string expectedIds)
	{
		FakeExecuteReader command = Command(5, out _);
		string[] parts = call.Split(' ');

		List<int> ids = parts[0] switch
		{
			#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
			"Take" => command.Take(int.Parse(parts[1]), Id),
			"Skip" => command.Skip(int.Parse(parts[1]), Id),
			_ => command.SkipThenTake(int.Parse(parts[1]), int.Parse(parts[2]), Id),
			#pragma warning restore CS0618
		};

		Assert.Equal(expectedIds, string.Join(",", ids));
	}

	[Fact]
	public static void Take_StopsReadingOnceItHasEnough()
	{
		FakeExecuteReader command = Command(5, out Func<int> rowsRead);

		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		command.Take(2, Id);
		#pragma warning restore CS0618

		Assert.Equal(2, rowsRead());
	}

	[Fact]
	public static void ATakeOfZero_ReturnsNothing_WithoutExecutingTheCommand()
	{
		FakeExecuteReader command = Command(5, out _);

		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Empty(command.Take(0, Id));
		Assert.Empty(command.SkipThenTake(3, 0, Id));
		#pragma warning restore CS0618
		Assert.Equal(0, command.ExecuteCount);
	}

	[Theory]
	[InlineData("Take", "count")]
	[InlineData("SkipThenTake take", "take")]
	public static void ANegativeCount_IsRefusedBeforeTheCommandRuns(string call, string parameter)
	{
		FakeExecuteReader command = Command(5, out _);

		ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
		{
			switch (call)
			{
				#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
				case "Take": command.Take(-1, Id); break;
				case "Skip": command.Skip(-1, Id); break;
				default: command.SkipThenTake(0, -1, Id); break;
				#pragma warning restore CS0618
			}
		});

		Assert.Equal(parameter, ex.ParamName);
		Assert.Equal(-1, ex.ActualValue);
		Assert.Equal(0, command.ExecuteCount);
	}

	// ---- the behavior the caller asked for is kept, and the result is always a single result set ----

	[Fact]
	public static void TheCallersBehavior_IsKept_AndSingleResultIsAdded()
	{
		FakeExecuteReader command = Command(3, out _);

		command.ToImmutableArray(Id, CommandBehavior.SequentialAccess);

		CommandBehavior behavior = Assert.Single(command.Behaviors);
		Assert.True(behavior.HasFlag(CommandBehavior.SequentialAccess));
		Assert.True(behavior.HasFlag(CommandBehavior.SingleResult));
	}

	// ---- a result set into a DataTable ----

	[Fact]
	public static void LoadTable_LoadsTheFirstResultSet_AskingTheProviderForASingleOne()
	{
		var set = new DataSet();
		DataTable table = set.Tables.Add("only");
		table.Columns.Add("Id", typeof(int));
		table.Rows.Add(1);
		table.Rows.Add(2);
		var command = new FakeExecuteReader(() => set.CreateDataReader());

		DataTable loaded = command.LoadTable();

		Assert.Equal([1, 2], loaded.Rows.Cast<DataRow>().Select(r => (int)r[0]).ToArray());
		CommandBehavior behavior = Assert.Single(command.Behaviors);
		Assert.True(behavior.HasFlag(CommandBehavior.SingleResult));
		Assert.True(behavior.HasFlag(CommandBehavior.SequentialAccess));
	}

	[Fact]
	public static void FirstOrdinalResults_ReadsEveryRowBeforeItReturns_AndTurnsNullsIntoNull()
	{
		TypedReader? reader = null;
		var command = new FakeExecuteReader(() => reader = TypedReader.Create([("Id", typeof(int))], [1], [null], [3]));

		IEnumerable<object?> values = command.FirstOrdinalResults();

		Assert.Equal(3, reader!.RowsRead); // the reader is already disposed when the caller enumerates
		Assert.Equal([1, null, 3], values.ToArray());
		Assert.Equal([1, null, 3], command.FirstOrdinalResults<int?>().ToArray());
		CommandBehavior behavior = command.Behaviors[0];
		Assert.True(behavior.HasFlag(CommandBehavior.SequentialAccess));
		Assert.True(behavior.HasFlag(CommandBehavior.SingleResult));
	}

	// ---- argument guards ----

	[Theory]
	[InlineData("First transform", "transform")]
	[InlineData("Retrieve columnNames", "columnNames")]
	public static void ANullArgument_IsRefusedBeforeTheCommandRuns(string call, string parameter)
	{
		FakeExecuteReader command = Command(1, out _);

		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
		{
			switch (call)
			{
				case "First transform": command.First((Func<IDataRecord, int>)null!); break;
				case "Retrieve columnNames": command.Retrieve((IEnumerable<string>)null!); break;
				default: ((IExecuteReader)null!).ToList(Id); break;
			}
		});

		Assert.Equal(parameter, ex.ParamName);
		Assert.Equal(0, command.ExecuteCount);
	}

	// ---- the asynchronous forms, over a real command on a tracking connection ----

	static (DbTally Tally, ExpressiveDbCommand Command) Tracked(int rowCount, Action<int>? onRead = null)
	{
		var tally = new DbTally(new DbScript { Columns = Columns, Rows = Rows(rowCount), OnRead = onRead });
		return (tally, tally.NewConnection().Command("select 1"));
	}

	[Theory]
	[InlineData(true)]
	public static async Task IterateReaderAsync_HandlesEveryRow_AndReleasesWhatItOpened(bool useAsyncRead)
	{
		(DbTally tally, ExpressiveDbCommand command) = Tracked(3);
		int handled = 0;

		await command.EnableAsyncRead(useAsyncRead).IterateReaderAsync(_ => handled++, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(3, handled);
		LeakAssert.CommandsAndReadersDisposed(tally);
		LeakAssert.ConnectionsReleased(tally, LeakSource.BareClosed, tally.Pool, 1);
	}

	[Theory]
	[InlineData(0)]
	[InlineData(2)]
	public static async Task IterateReaderAsync_CancelledByTheCommandsToken_Throws_AndReleasesWhatItOpened(int cancelAtRead)
	{
		using var cts = new CancellationTokenSource();
		if (cancelAtRead == 0) cts.Cancel();
		(DbTally tally, ExpressiveDbCommand command) = Tracked(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel();
		});
		int handled = 0;

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
#pragma warning disable xUnit1051 // The command token alone must govern this call: a live call token would link a second one.
			await command.EnableAsyncRead(true).UseCancellationToken(cts.Token).IterateReaderAsync(_ => handled++));
#pragma warning restore xUnit1051

		Assert.Equal(cancelAtRead == 0 ? 0 : 2, handled); // the rows read before the cancel were handled, and nothing was read after it
		LeakAssert.CommandsAndReadersDisposed(tally);
		LeakAssert.ConnectionsReleased(tally, LeakSource.BareClosed, tally.Pool, 1);
	}

	[Theory]
	[InlineData(true)]
	public static async Task IterateReaderWhileAsync_StopsReadingAtTheFirstFalse(bool valueTaskHandler)
	{
		(DbTally tally, ExpressiveDbCommand command) = Tracked(10);
		int seen = 0;

		if (valueTaskHandler)
			await command.EnableAsyncRead(true).IterateReaderWhileAsync(_ => new ValueTask<bool>(++seen < 3), cancellationToken: TestContext.Current.CancellationToken);
		else
			await command.EnableAsyncRead(true).IterateReaderWhileAsync(_ => ++seen < 3, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(3, seen);
		Assert.Equal(3, Assert.Single(tally.Readers).Reads);
	}
}
