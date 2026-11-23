#nullable enable

namespace Open.Database.Extensions.Tests;

// Edge cases of the extensions on IExecuteReader: what the provider is asked for (the behavior flags), how much of the result is read,
// negative and zero counts, a result set loaded into a table, and the asynchronous forms over a real command with its cancellation token and connection.
[ExcludeFromCodeCoverage]
public static class ExecuteReaderEdgeTests
{
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

	// ---- argument guards ----
}
