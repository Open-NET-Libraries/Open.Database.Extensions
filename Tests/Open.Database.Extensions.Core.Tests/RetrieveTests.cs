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

		Assert.Equal(new[] { "Id", "Name" }, result.Names);
		Assert.Equal(new[] { 0, 1 }, result.Ordinals);

		List<object[]> rows = Drain(result);
		Assert.Equal(2, rows.Count);
		Assert.Equal(new object[] { 1, "a" }, rows[0]);
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

		Assert.Equal(new[] { "Name", "Id" }, result.Names);
		Assert.Equal(new[] { 1, 0 }, result.Ordinals);

		object[] row = Assert.Single(Drain(result));
		Assert.Equal(new object[] { "a", 1 }, row);
	}

	[Fact]
	public static void Retrieve_ByOrdinals_SelectsColumns()
	{
		IDataReader reader = FakeReader.Create(
			["Id", "Name", "Extra"],
			[1, "a", "x"]);

		IEnumerable<int> ordinals = [2, 0];
		Core.QueryResultQueue<object[]> result = reader.Retrieve(ordinals);

		Assert.Equal(new[] { "Extra", "Id" }, result.Names);

		object[] row = Assert.Single(Drain(result));
		Assert.Equal(new object[] { "x", 1 }, row);
	}
}
