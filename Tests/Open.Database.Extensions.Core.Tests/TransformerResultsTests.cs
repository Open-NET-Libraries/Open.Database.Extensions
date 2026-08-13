using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class TransformerResultsTests
{
	internal sealed class Person
	{
		public string? FirstName { get; set; }
		public string? LastName { get; set; }
		public int Age { get; set; }
	}

	static DataTable BuildTable()
	{
		var table = new DataTable();
		table.Columns.Add("FirstName", typeof(string));
		table.Columns.Add("last_name", typeof(string));
		table.Columns.Add("AGE", typeof(int));
		table.Rows.Add("John", "Doe", 30);
		table.Rows.Add("Jane", "Roe", 25);
		return table;
	}

	[Fact]
	public static void To_DefaultMapping_IsCaseInsensitive_UnmappedStayDefault()
	{
		DataTable table = BuildTable();

		List<Person> people = [.. table.To<Person>()];

		Assert.Equal(2, people.Count);
		Assert.Equal("John", people[0].FirstName);
		Assert.Equal(30, people[0].Age);   // "AGE" column matched property "Age" case-insensitively
		Assert.Null(people[0].LastName);   // "last_name" != "LastName" -> unmapped, stays default
	}

	[Fact]
	public static void To_WithFieldOverride_MapsColumn()
	{
		DataTable table = BuildTable();
		var overrides = new Dictionary<string, string?> { ["LastName"] = "last_name" };

		List<Person> people = [.. table.To<Person>(overrides)];

		Assert.Equal("Doe", people[0].LastName);
		Assert.Equal("Roe", people[1].LastName);
	}

	[Fact]
	public static void To_NullOverride_IgnoresProperty()
	{
		DataTable table = BuildTable();
		// Explicitly ignore Age even though an AGE column is present.
		var overrides = new Dictionary<string, string?> { ["Age"] = null };

		List<Person> people = [.. table.To<Person>(overrides)];

		Assert.Equal(0, people[0].Age); // ignored -> default
		Assert.Equal("John", people[0].FirstName);
	}

	[Fact]
	public static void DequeueAs_MapsBufferedRows_CaseInsensitivelyWithOverride()
	{
		// A reader-shaped buffered result (column names + queued rows).
		var queue = new Queue<object?[]>();
		queue.Enqueue(["Jane", 25]);
		queue.Enqueue(["Jack", 40]);

		var result = new Core.QueryResult<Queue<object?[]>>(
			[0, 1],
			["first_name", "AGE"],
			queue);

		var overrides = new (string Field, string? Column)[] { ("FirstName", "first_name") };
		List<Person> people = [.. result.DequeueAs<Person>(overrides)];

		Assert.Equal(2, people.Count);
		Assert.Equal("Jane", people[0].FirstName);
		Assert.Equal(25, people[0].Age);        // AGE matched Age case-insensitively
		Assert.Equal("Jack", people[1].FirstName);
		Assert.Equal(40, people[1].Age);
	}
}
