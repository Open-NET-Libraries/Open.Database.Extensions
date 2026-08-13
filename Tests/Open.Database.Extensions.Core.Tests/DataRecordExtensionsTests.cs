namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class DataRecordExtensionsTests
{
	static IDataRecord PositionedRecord(IReadOnlyList<string> columns, object?[] row)
	{
		IDataReader reader = FakeReader.Create(columns, row);
		Assert.True(reader.Read());
		return reader;
	}

	static readonly string[] Columns = ["Id", "Name", "Value"];

	[Fact]
	public static void GetValues_ReturnsAllValues_RetainingDBNull()
	{
		IDataRecord record = PositionedRecord(Columns, [1, DBNull.Value, "v"]);

		object[] values = record.GetValues();

		Assert.Equal(3, values.Length);
		Assert.Equal(1, values[0]);
		Assert.Equal(DBNull.Value, values[1]); // GetValues does NOT convert DBNull
		Assert.Equal("v", values[2]);
	}

	[Fact]
	public static void ColumnNames_And_GetNames_ReturnColumnsInOrder()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		Assert.Equal(Columns, record.ColumnNames());
		Assert.Equal(Columns, record.GetNames());
	}

	[Fact]
	public static void GetNames_ByOrdinals_ReturnsSelectedNames()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		// GetNames returns ImmutableArray<string> (reference-equality IEquatable); compare as a sequence.
		Assert.Equal(["Value", "Id"], record.GetNames([2, 0]).ToArray());
	}

	[Fact]
	public static void OrdinalMapping_ReturnsNameOrdinalPairs()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		Assert.Equal(
			[("Id", 0), ("Name", 1), ("Value", 2)],
			record.OrdinalMapping());
	}

	[Fact]
	public static void OrdinalMapping_ByNames_RecoversActualCasing()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		// Requested with different casing; result reflects the reader's actual casing.
		(string Name, int Ordinal)[] map = [.. record.OrdinalMapping(["name", "id"])];

		Assert.Equal([("Name", 1), ("Id", 0)], map);
	}

	[Fact]
	public static void GetOrdinalMapping_UnknownColumn_ThrowsWithColumnName()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		var ex = Assert.Throws<IndexOutOfRangeException>(
			() => record.GetOrdinalMapping(["Id", "Missing"]));
		Assert.Contains("Missing", ex.Message);
	}

	[Fact]
	public static void GetMatchingOrdinals_IsCaseInsensitive_OrderDependsOnSort()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		// sort:false -> requested order
		Assert.Equal(
			[("Name", 1), ("Id", 0)],
			record.GetMatchingOrdinals(["name", "ID"], sort: false));

		// sort:true -> record order
		Assert.Equal(
			[("Id", 0), ("Name", 1)],
			record.GetMatchingOrdinals(["name", "ID"], sort: true));
	}

	[Fact]
	public static void GetMatchingOrdinals_IgnoresColumnsNotPresent()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		Assert.Equal(
			[("Id", 0)],
			record.GetMatchingOrdinals(["Id", "Nope"]));
	}

	[Fact]
	public static void EnumerateValues_YieldsAllValues()
	{
		IDataRecord record = PositionedRecord(Columns, [1, DBNull.Value, "v"]);

		Assert.Equal(new object[] { 1, DBNull.Value, "v" }, record.EnumerateValues());
	}

	[Fact]
	public static void EnumerateValuesFromOrdinals_SelectsByOrdinal()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		Assert.Equal(new object[] { "v", 1 }, record.EnumerateValuesFromOrdinals([2, 0]));
	}

	[Fact]
	public static void GetValuesFromOrdinals_List_ReturnsSelectedArray()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		object[] values = record.GetValuesFromOrdinals([2, 0]);

		Assert.Equal(["v", 1], values);
	}

	[Fact]
	public static void GetValuesFromOrdinals_ArrayBindsToIReadOnlyList()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		// Arrays implement IReadOnlyList<int>, so a plain array still binds the widened overload
		// (including on the netstandard2.0 shim).
		int[] ordinals = [2, 0];
		Assert.Equal(["v", 1], record.GetValuesFromOrdinals(ordinals));
	}

	[Fact]
	public static void GetValuesFromOrdinals_Span_FillsTarget()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		Span<object> target = new object[2];
		ReadOnlySpan<int> ordinals = [1, 2];
		record.GetValuesFromOrdinals(ordinals, target);

		Assert.Equal("n", target[0]);
		Assert.Equal("v", target[1]);
	}

	[Fact]
	public static void GetDataTypeNames_ReturnsTypeNamePerColumn()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		Assert.Equal(
			["Id_type", "Name_type", "Value_type"],
			record.GetDataTypeNames());
	}

	[Fact]
	public static void ToDictionary_ByColumnNames_ConvertsDBNullToNull()
	{
		IDataRecord record = PositionedRecord(Columns, [1, DBNull.Value, "v"]);

		var dict = record.ToDictionary(["Id", "Name"]);

		Assert.Equal(1, dict["Id"]);
		Assert.Null(dict["Name"]); // DBNull -> null
	}

	[Fact]
	public static void ToDictionary_ByOrdinalMapping_ConvertsDBNullToNull()
	{
		IDataRecord record = PositionedRecord(Columns, [1, DBNull.Value, "v"]);

		var mapping = new List<(string Name, int Ordinal)> { ("Name", 1), ("Value", 2) };
		var dict = record.ToDictionary(mapping);

		Assert.Null(dict["Name"]);
		Assert.Equal("v", dict["Value"]);
	}
}
