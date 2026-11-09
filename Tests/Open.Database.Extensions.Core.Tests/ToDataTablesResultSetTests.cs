using System.Globalization;

namespace Open.Database.Extensions.Tests;

#nullable enable

// One DataTable per result set. DataTable.Load consumes a result set and moves the reader to the next one (and closes it after the last),
// so asking the reader for the next result set again skipped one and, after the last one, threw because the reader was already closed.
[ExcludeFromCodeCoverage]
public static class ToDataTablesResultSetTests
{
	/// <summary>The number of rows of each result set, in order.</summary>
	public static TheoryData<string> ResultSets() => ["0", "3", "2,1", "0,2,0", "2,1,3", "1,1,1,1"];

	static int[] Counts(string rows) => [.. rows.Split(',').Select(n => int.Parse(n, CultureInfo.InvariantCulture))];

	static DataSet DataSetOf(int[] rowCounts)
	{
		var set = new DataSet();
		for (int t = 0; t < rowCounts.Length; t++)
		{
			DataTable table = set.Tables.Add($"t{t}");
			table.Columns.Add("id", typeof(int));
			table.Columns.Add("name", typeof(string));
			for (int r = 0; r < rowCounts[t]; r++)
				table.Rows.Add(t * 100 + r, $"t{t}r{r}");
		}

		return set;
	}

	static void AssertEveryResultSet(int[] rowCounts, List<DataTable> tables)
	{
		Assert.Equal(rowCounts, tables.Select(t => t.Rows.Count));
		for (int t = 0; t < rowCounts.Length; t++)
		{
			Assert.Equal(["id", "name"], tables[t].Columns.Cast<DataColumn>().Select(c => c.ColumnName));
			for (int r = 0; r < rowCounts[t]; r++)
			{
				Assert.Equal(t * 100 + r, tables[t].Rows[r]["id"]);
				Assert.Equal($"t{t}r{r}", tables[t].Rows[r]["name"]);
			}
		}
	}

	[Theory]
	[MemberData(nameof(ResultSets))]
	public static void Reader_ReturnsOneTablePerResultSet(string rows)
	{
		int[] rowCounts = Counts(rows);
		using DataSet set = DataSetOf(rowCounts);
		using DataTableReader reader = set.CreateDataReader();

		List<DataTable> tables = reader.ToDataTables();

		AssertEveryResultSet(rowCounts, tables);
	}

	[Theory]
	[MemberData(nameof(ResultSets))]
	public static void Command_ReturnsOneTablePerResultSet(string rows)
	{
		int[] rowCounts = Counts(rows);
		using DataSet set = DataSetOf(rowCounts);
		using var connection = new FakeDbConnection();
		using var command = new FakeDbCommand(connection, () => set.CreateDataReader());

		List<DataTable> tables = command.ToDataTables();

		AssertEveryResultSet(rowCounts, tables);
	}

	[Fact]
	public static void Reader_OneDataTable_ReturnsOneTable()
	{
		using DataSet set = DataSetOf([2]);
		using DataTableReader reader = set.Tables[0].CreateDataReader();

		List<DataTable> tables = reader.ToDataTables();

		AssertEveryResultSet([2], tables);
	}
}
