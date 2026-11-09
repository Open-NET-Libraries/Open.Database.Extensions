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
	// ToDataTable loads one result set and leaves the reader on it, so the caller's NextResult goes on to the next one.
	public static TheoryData<string, bool> ResultSetsOnBothReaders()
	{
		var data = new TheoryData<string, bool>();
		foreach (string rows in new[] { "0", "3", "2,1", "0,2,0", "2,1,3", "1,1,1,1" })
		{
			data.Add(rows, false);
			data.Add(rows, true);
		}

		return data;
	}

	static IDataReader ReaderOf(DataSet set, bool notDbDataReader)
	{
		DataTableReader reader = set.CreateDataReader();
		return notDbDataReader ? new NotADbDataReader(reader) : reader;
	}

	static void Within(Action action)
		=> Assert.True(Task.Run(action).Wait(TimeSpan.FromSeconds(20)), "The call did not finish.");

	[Theory]
	[MemberData(nameof(ResultSetsOnBothReaders))]
	public static void ToDataTable_ThenNextResult_WalksEveryResultSet(string rows, bool notDbDataReader) => Within(() =>
	{
		int[] rowCounts = Counts(rows);
		using DataSet set = DataSetOf(rowCounts);
		using IDataReader reader = ReaderOf(set, notDbDataReader);

		var tables = new List<DataTable>();
		bool more;
		do
		{
			tables.Add(reader.ToDataTable());
			Assert.False(reader.IsClosed);
			more = reader.NextResult();
			Assert.Equal(tables.Count < rowCounts.Length, more);
		}
		while (more);

		AssertEveryResultSet(rowCounts, tables);
		Assert.False(reader.IsClosed);
		Assert.False(reader.NextResult()); // still no throw, still no more
		Assert.False(reader.IsClosed);
	});

	[Theory]
	[MemberData(nameof(ResultSets))]
	public static void ToDataTable_EqualsPlainLoad(string rows)
	{
		int[] rowCounts = Counts(rows);
		using DataSet set = DataSetOf(rowCounts);
		using DataTableReader plainReader = set.CreateDataReader();
		using DataTableReader reader = set.CreateDataReader();

		using var plain = new DataTable();
		plain.Load(plainReader);
		using DataTable table = reader.ToDataTable();

		Assert.Equal(plain.Columns.Cast<DataColumn>().Select(c => (c.ColumnName, c.DataType, c.AllowDBNull)), table.Columns.Cast<DataColumn>().Select(c => (c.ColumnName, c.DataType, c.AllowDBNull)));
		Assert.Equal(plain.PrimaryKey.Select(c => c.ColumnName), table.PrimaryKey.Select(c => c.ColumnName));
		Assert.Equal(plain.Rows.Cast<DataRow>().Select(r => r.ItemArray), table.Rows.Cast<DataRow>().Select(r => r.ItemArray));
	}

	[Fact]
	public static void ToDataTable_KeepsNulls()
	{
		using var source = new DataTable();
		source.Columns.Add("id", typeof(int));
		source.Columns.Add("name", typeof(string));
		source.Rows.Add(1, DBNull.Value);
		using var reader = new NotADbDataReader(source.CreateDataReader());

		using DataTable table = reader.ToDataTable();

		Assert.Equal(DBNull.Value, table.Rows[0]["name"]);
		Assert.False(reader.IsClosed);
		Assert.False(reader.NextResult());
	}

	[Fact]
	public static void ToDataTable_ResultSetWithoutColumns_DoesNotHangAndNextResultMovesOn() => Within(() =>
	{
		using DataSet set = DataSetOf([2, 1]);
		set.Tables.Add("noColumns");
		DataTable last = set.Tables.Add("last");
		last.Columns.Add("id", typeof(int));
		last.Rows.Add(7);
		using DataTableReader reader = set.CreateDataReader();

		Assert.Equal(2, reader.ToDataTable().Rows.Count);
		Assert.True(reader.NextResult());
		Assert.Equal(1, reader.ToDataTable().Rows.Count);
		Assert.True(reader.NextResult());
		using (DataTable empty = reader.ToDataTable())
			Assert.Empty(empty.Columns);
		Assert.True(reader.NextResult());
		Assert.Equal(7, reader.ToDataTable().Rows[0]["id"]);
		Assert.False(reader.NextResult());
		Assert.False(reader.IsClosed);
	});

	// An IDataReader that is not a DbDataReader, over a DataTableReader.
	sealed class NotADbDataReader(DataTableReader inner) : IDataReader
	{
		public object this[int i] => inner[i];
		public object this[string name] => inner[name];
		public int Depth => inner.Depth;
		public bool IsClosed => inner.IsClosed;
		public int RecordsAffected => inner.RecordsAffected;
		public int FieldCount => inner.FieldCount;
		public void Close() => inner.Close();
		public void Dispose() => inner.Dispose();
		public bool NextResult() => inner.NextResult();
		public bool Read() => inner.Read();
		public DataTable? GetSchemaTable() => inner.GetSchemaTable();
		public bool GetBoolean(int i) => inner.GetBoolean(i);
		public byte GetByte(int i) => inner.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length) => inner.GetBytes(i, fieldOffset, buffer, bufferOffset, length);
		public char GetChar(int i) => inner.GetChar(i);
		public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length) => inner.GetChars(i, fieldOffset, buffer, bufferOffset, length);
		public IDataReader GetData(int i) => inner.GetData(i);
		public string GetDataTypeName(int i) => inner.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => inner.GetDateTime(i);
		public decimal GetDecimal(int i) => inner.GetDecimal(i);
		public double GetDouble(int i) => inner.GetDouble(i);
		public Type GetFieldType(int i) => inner.GetFieldType(i);
		public float GetFloat(int i) => inner.GetFloat(i);
		public Guid GetGuid(int i) => inner.GetGuid(i);
		public short GetInt16(int i) => inner.GetInt16(i);
		public int GetInt32(int i) => inner.GetInt32(i);
		public long GetInt64(int i) => inner.GetInt64(i);
		public string GetName(int i) => inner.GetName(i);
		public int GetOrdinal(string name) => inner.GetOrdinal(name);
		public string GetString(int i) => inner.GetString(i);
		public object GetValue(int i) => inner.GetValue(i);
		public int GetValues(object[] values) => inner.GetValues(values);
		public bool IsDBNull(int i) => inner.IsDBNull(i);
	}

}
