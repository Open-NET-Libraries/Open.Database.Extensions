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
	[InlineData("Retrieve columnNames", "columnNames")]
	public static void ANullArgument_IsRefusedBeforeTheCommandRuns(string call, string parameter)
	{
		FakeExecuteReader command = Command(1, out _);

		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
		{
			switch (call)
			{
				case "Retrieve columnNames": command.Retrieve((IEnumerable<string>)null!); break;
#pragma warning restore CS0618
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
}
