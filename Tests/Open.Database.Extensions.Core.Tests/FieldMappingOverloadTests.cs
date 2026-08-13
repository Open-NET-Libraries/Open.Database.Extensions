using Person = Open.Database.Extensions.Tests.TransformerResultsTests.Person;

#nullable enable

namespace Open.Database.Extensions.Tests;
// Assumption tests for the field-mapping overloads: every supported call shape must both
// COMPILE (proving no ambiguity) and bind to an overload that produces the correct mapping.
// [OverloadResolutionPriority] on the params-array overload is what lets the collection-expression
// forms ([("a","b")], [new("a","b"), ...]) resolve instead of being ambiguous with KeyValuePair.
[ExcludeFromCodeCoverage]
public static class FieldMappingOverloadTests
{
	static IDataReader Reader() => FakeReader.Create(["first_name", "last_name"], ["Ada", "Lovelace"]);

	static void AssertMapped(IEnumerable<Person> results)
	{
		Person p = results.Single();
		Assert.Equal("Ada", p.FirstName);
		Assert.Equal("Lovelace", p.LastName);
	}

	[Fact]
	public static void InlineTuples()
		=> AssertMapped(Reader().Results<Person>(("FirstName", "first_name"), ("LastName", "last_name")));

	[Fact]
	public static void InlineTargetTypedNew()
		=> AssertMapped(Reader().Results<Person>(new("FirstName", "first_name"), new("LastName", "last_name")));

	[Fact]
	public static void CollectionExpressionOfTuples()
		=> AssertMapped(Reader().Results<Person>([("FirstName", "first_name"), ("LastName", "last_name")]));

	[Fact]
	public static void CollectionExpressionOfNew()
		=> AssertMapped(Reader().Results<Person>([new("FirstName", "first_name"), new("LastName", "last_name")]));

	[Fact]
	public static void TupleCollection()
	{
		var overrides = new List<(string, string?)> { ("FirstName", "first_name"), ("LastName", "last_name") };
		AssertMapped(Reader().Results<Person>(overrides));
	}

	[Fact]
	public static void Dictionary()
	{
		var overrides = new Dictionary<string, string?> { ["FirstName"] = "first_name", ["LastName"] = "last_name" };
		AssertMapped(Reader().Results<Person>(overrides));
	}

	[Fact]
	public static void KeyValuePairCollection()
	{
		var overrides = new List<KeyValuePair<string, string?>> { new("FirstName", "first_name"), new("LastName", "last_name") };
		AssertMapped(Reader().Results<Person>(overrides));
	}

	[Fact]
	public static void NoOverrides_LeavesUnmatchedNull()
		=> Assert.Null(Reader().Results<Person>().Single().FirstName);

	// To<T>(DataTable, ...) uses the same params-array + [OverloadResolutionPriority] shape.
	static DataTable OneRowTable()
	{
		var table = new DataTable();
		table.Columns.Add("first_name", typeof(string));
		table.Rows.Add("Ada");
		return table;
	}

	[Fact]
	public static void To_InlineTuple()
		=> Assert.Equal("Ada", OneRowTable().To<Person>(("FirstName", "first_name")).Single().FirstName);

	[Fact]
	public static void To_CollectionExpressionOfNew()
		=> Assert.Equal("Ada", OneRowTable().To<Person>([new("FirstName", "first_name")]).Single().FirstName);

	[Fact]
	public static void To_Dictionary()
	{
		var overrides = new Dictionary<string, string?> { ["FirstName"] = "first_name" };
		Assert.Equal("Ada", OneRowTable().To<Person>(overrides).Single().FirstName);
	}
}
