namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class RetrieveTests
{
	static List<object[]> Drain(Core.QueryResultQueue<object[]> source)
	{
		var rows = new List<object[]>();
		Queue<object[]> q = source.Result;
		while (q.Count != 0) rows.Add(q.Dequeue());
		return rows;
	}

	[Fact]
	public static void Retrieve_All_RetainsDBNull_AndReportsColumns()
	{
		IDataReader reader = FakeReader.Create(
			["Id", "Name"],
			[1, "a"],
			[2, DBNull.Value]);

		Core.QueryResultQueue<object[]> result = reader.Retrieve();

		// Names/Ordinals are ImmutableArray<T>, whose IEquatable compares the backing array by
		// reference; materialize to a plain array so xUnit compares element-by-element.
		Assert.Equal(["Id", "Name"], result.Names.ToArray());
		Assert.Equal([0, 1], result.Ordinals.ToArray());

		List<object[]> rows = Drain(result);
		Assert.Equal(2, rows.Count);
		Assert.Equal([1, "a"], rows[0]);
		Assert.Equal(2, rows[1][0]);
		Assert.Equal(DBNull.Value, rows[1][1]); // Retrieve retains DBNull (does not convert)
	}

	[Fact]
	public static void Retrieve_ByColumnNames_SelectsAndOrdersColumns()
	{
		IDataReader reader = FakeReader.Create(
			["Id", "Name", "Extra"],
			[1, "a", "x"]);

		IEnumerable<string> columns = ["Name", "Id"];
		Core.QueryResultQueue<object[]> result = reader.Retrieve(columns);

		Assert.Equal(["Name", "Id"], result.Names.ToArray());
		Assert.Equal([1, 0], result.Ordinals.ToArray());

		object[] row = Assert.Single(Drain(result));
		Assert.Equal(["a", 1], row);
	}

	[Fact]
	public static void Retrieve_ByOrdinals_SelectsColumns()
	{
		IDataReader reader = FakeReader.Create(
			["Id", "Name", "Extra"],
			[1, "a", "x"]);

		IEnumerable<int> ordinals = [2, 0];
		Core.QueryResultQueue<object[]> result = reader.Retrieve(ordinals);

		Assert.Equal(["Extra", "Id"], result.Names.ToArray());

		object[] row = Assert.Single(Drain(result));
		Assert.Equal(["x", 1], row);
	}
}
