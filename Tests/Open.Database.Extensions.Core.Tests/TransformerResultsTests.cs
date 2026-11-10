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
	public static void To_DefaultMapping_IsCaseInsensitive_AndIgnoresUnderscores()
	{
		DataTable table = BuildTable();

		List<Person> people = [.. table.To<Person>()];

		Assert.Equal(2, people.Count);
		Assert.Equal("John", people[0].FirstName);
		Assert.Equal(30, people[0].Age);   // "AGE" column matched property "Age" case-insensitively
		Assert.Equal("Doe", people[0].LastName); // "last_name" matched property "LastName" with the underscore ignored
	}

	[Fact]
	public static void To_WithFieldOverride_MapsColumn()
	{
		DataTable table = BuildTable();
		var map = new RowMap<Person>([new(nameof(Person.LastName), "last_name")]);

		List<Person> people = [.. table.To(map)];

		Assert.Equal("Doe", people[0].LastName);
		Assert.Equal("Roe", people[1].LastName);
	}

	[Fact]
	public static void To_NullOverride_IgnoresProperty()
	{
		DataTable table = BuildTable();
		// Explicitly ignore Age even though an AGE column is present.
		var map = new RowMap<Person>([new(nameof(Person.Age), null)]);

		List<Person> people = [.. table.To(map)];

		Assert.Equal(0, people[0].Age); // ignored -> default
		Assert.Equal("John", people[0].FirstName);
	}

	[Fact]
	public static void DequeueAs_MapsBufferedRows_CaseInsensitivelyWithOverride()
	{
		// A reader-shaped buffered result (column names + queued rows).
		var queue = new Queue<object[]>();
		queue.Enqueue(["Jane", 25]);
		queue.Enqueue(["Jack", 40]);

		var result = new QueryResult<Queue<object[]>>(
			[0, 1],
			["first_name", "AGE"],
			queue);

		var map = new RowMap<Person>([new(nameof(Person.FirstName), "first_name")]);
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		List<Person> people = [.. result.DequeueAs(map)];
		#pragma warning restore CS0618

		Assert.Equal(2, people.Count);
		Assert.Equal("Jane", people[0].FirstName);
		Assert.Equal(25, people[0].Age);        // AGE matched Age case-insensitively
		Assert.Equal("Jack", people[1].FirstName);
		Assert.Equal(40, people[1].Age);
	}
}
