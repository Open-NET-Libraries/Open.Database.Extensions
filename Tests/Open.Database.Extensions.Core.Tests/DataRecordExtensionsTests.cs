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
			[new("Id", 0), new("Name", 1), new("Value", 2)],
			record.OrdinalMapping());
	}

	[Fact]
	public static void OrdinalMapping_ByNames_RecoversActualCasing()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		// Requested with different casing; result reflects the reader's actual casing.
		ColumnOrdinal[] map = [.. record.OrdinalMapping(["name", "id"])];

		Assert.Equal([new("Name", 1), new("Id", 0)], map);
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
	public static void GetOrdinalMapping_Mismatch_DoesNotListAColumnAskedInAnotherCasing()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		var ex = Assert.Throws<IndexOutOfRangeException>(
			() => record.GetOrdinalMapping(["id", "NAME", "Missing"]));

		Assert.Equal("Invalid columns: Missing", ex.Message);
	}

	[Fact]
	public static void MatchingOrdinals_IsCaseInsensitive_OrderDependsOnSort()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		// sort:false -> requested order
		Assert.Equal(
			[new("Name", 1), new("Id", 0)],
			record.MatchingOrdinals(["name", "ID"], sort: false).ToArray());

		// sort:true -> record order
		Assert.Equal(
			[new("Id", 0), new("Name", 1)],
			record.MatchingOrdinals(["name", "ID"], sort: true).ToArray());
	}

	[Fact]
	public static void MatchingOrdinals_IgnoresColumnsNotPresent()
	{
		IDataRecord record = PositionedRecord(Columns, [1, "n", "v"]);

		Assert.Equal(
			[new("Id", 0)],
			record.MatchingOrdinals(["Id", "Nope"]).ToArray());
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

		var mapping = new List<ColumnOrdinal> { new("Name", 1), new("Value", 2) };
		var dict = record.ToDictionary(mapping);

		Assert.Null(dict["Name"]);
		Assert.Equal("v", dict["Value"]);
	}
}
