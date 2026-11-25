namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class ColumnOrdinalTests
{
	static IDataRecord Record()
	{
		IDataReader reader = FakeReader.Create(["Id", "Title"], [1, "First"]);
		Assert.True(reader.Read());
		return reader;
	}

	[Fact]
	public static void Constructor_SetsNameAndOrdinal_AndDeconstructs()
	{
		var column = new ColumnOrdinal("Title", 1);
		(string name, int ordinal) = column;

		Assert.Equal("Title", column.Name);
		Assert.Equal(1, column.Ordinal);
		Assert.Equal("Title", name);
		Assert.Equal(1, ordinal);
	}

	[Fact]
	public static void Constructor_ANullName_ThrowsArgumentNull()
		=> Assert.Equal("name", Assert.Throws<ArgumentNullException>(() => new ColumnOrdinal(null!, 0)).ParamName);

	[Fact]
	public static void Equality_IsByOrdinalAndExactName()
	{
		var column = new ColumnOrdinal("Title", 1);
		var same = new ColumnOrdinal("Title", 1);

		Assert.True(column.Equals(same));
		Assert.True(column.Equals((object)same));
		Assert.True(column == same);
		Assert.False(column != same);
		Assert.Equal(column.GetHashCode(), same.GetHashCode());

		foreach (ColumnOrdinal other in new[] { new ColumnOrdinal("title", 1), new ColumnOrdinal("Title", 0), default })
		{
			Assert.False(column.Equals(other));
			Assert.False(column.Equals((object)other));
			Assert.False(column == other);
			Assert.True(column != other);
		}

		Assert.False(column.Equals(null));
		Assert.False(column.Equals((object)("Title", 1)));
	}

	[Fact]
	public static void Default_HasNoName_AndStillComparesAndHashes()
	{
		ColumnOrdinal column = default;
		(string name, int ordinal) = column;

		Assert.Null(name);
		Assert.Equal(0, ordinal);
		Assert.True(column == default);
		Assert.Equal(column.GetHashCode(), default(ColumnOrdinal).GetHashCode());
	}

	[Fact]
	public static void ToString_PrintsAsTheTupleDid()
	{
		Assert.Equal("(Id, 0)", new ColumnOrdinal("Id", 0).ToString());
		Assert.Equal(("Title", -1).ToString(), new ColumnOrdinal("Title", -1).ToString());
		Assert.Equal("(, 0)", default(ColumnOrdinal).ToString());
		Assert.Equal(((string?)null, 0).ToString(), default(ColumnOrdinal).ToString());
	}

	// The owner's decision: no conversion to or from a tuple, or anything else.
	[Fact]
	public static void HasNoConversionOperators()
		=> Assert.DoesNotContain(
			typeof(ColumnOrdinal).GetMethods(),
			m => m.Name is "op_Implicit" or "op_Explicit");

	[Theory]
	[InlineData("Enumerable")]
	[InlineData("List")]
	[InlineData("Span")]
	public static void ToDictionary_ADefaultEntry_ThrowsArgumentException(string overload)
	{
		IDataRecord record = Record();
		ColumnOrdinal[] columns = [new("Title", 1), default];

		// Throws<T> is exact: neither ArgumentNullException (the dictionary's null key) nor NullReferenceException.
		var ex = Assert.Throws<ArgumentException>(() => ReadWith(overload, record, columns));

		Assert.Equal("ordinalMapping", ex.ParamName);
		Assert.Contains("default ColumnOrdinal", ex.Message);
	}

	[Theory]
	[InlineData("Enumerable")]
	[InlineData("List")]
	[InlineData("Span")]
	public static void ToDictionary_ADefaultEntry_IsRefusedBeforeTheRecordIsRead(string overload)
	{
		IDataRecord record = Record();

		Assert.Throws<ArgumentException>(() => ReadWith(overload, record, [default]));

		record.DidNotReceive().GetValue(0);
		_ = record.DidNotReceive()[0];
	}

	static Dictionary<string, object?> ReadWith(string overload, IDataRecord record, ColumnOrdinal[] columns)
		=> overload switch
		{
			"Enumerable" => record.ToDictionary(columns.Select(c => c)),
			"List" => record.ToDictionary((IReadOnlyList<ColumnOrdinal>)columns),
			_ => record.ToDictionary(new ReadOnlySpan<ColumnOrdinal>(columns)),
		};

	[Fact]
	public static void ToDictionary_ReadsWhatTheMappingsReturn()
	{
		IDataRecord record = Record();

		Dictionary<string, object?> fromArray = record.ToDictionary(record.GetOrdinalMapping(["title", "id"]));
		Dictionary<string, object?> fromAll = record.ToDictionary(record.OrdinalMapping());
		Dictionary<string, object?> fromMatches = record.ToDictionary(record.MatchingOrdinals(["TITLE", "Nope"]));

		Assert.Equal(["Title", "Id"], fromArray.Keys);
		Assert.Equal("First", fromArray["Title"]);
		Assert.Equal(["Id", "Title"], fromAll.Keys);
		Assert.Equal(["Title"], fromMatches.Keys);
	}
}
