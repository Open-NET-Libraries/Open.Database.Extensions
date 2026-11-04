using Open.Database.Extensions.Core;
using System.Collections.Immutable;

namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class QueryResultTests
{
	static QueryResult<Queue<object?[]>> Rows(params object?[][] rows)
		=> new(
			ImmutableArray.Create(0, 1),
			ImmutableArray.Create("Id", "Name"),
			new Queue<object?[]>(rows));

	[Fact]
	public static void Constructor_MismatchedOrdinalAndNameCounts_Throws()
	{
		Assert.Throws<ArgumentException>(() => new QueryResult<int>(
			ImmutableArray.Create(0, 1),
			ImmutableArray.Create("only one"),
			0));
	}

	[Fact]
	public static void Constructor_EnumerableOverload_MaterializesAndCountsColumns()
	{
		var result = new QueryResult<int>(new List<int> { 3, 5 }, new List<string> { "a", "b" }, 9);

		Assert.Equal(2, result.ColumnCount);
		Assert.Equal([3, 5], result.Ordinals.ToArray());
		Assert.Equal(["a", "b"], result.Names.ToArray());
		Assert.Equal(9, result.Result);
	}

	[Fact]
	public static void Constructor_EnumerableOverload_NullSource_Throws()
	{
		Assert.Throws<ArgumentNullException>(() => new QueryResult<int>(null!, new List<string>(), 0));
		Assert.Throws<ArgumentNullException>(() => new QueryResult<int>(new List<int>(), null!, 0));
	}

	[Fact]
	public static void ImplicitConversion_ReturnsResult_AndRejectsNull()
	{
		var result = new QueryResult<string>(Array.Empty<int>(), Array.Empty<string>(), "value");

		string converted = result;
		Assert.Equal("value", converted);

		QueryResult<string> none = null!;
		Assert.Throws<ArgumentNullException>(() => { string _ = none; });
	}

	[Fact]
	public static void QueryResultCollection_RejectsNullResult_AndEnumeratesResult()
	{
		Assert.Throws<ArgumentNullException>(() => new QueryResultCollection<int, List<int>>(
			ImmutableArray<int>.Empty, ImmutableArray<string>.Empty, null!));

		var collection = new QueryResultCollection<int>(
			ImmutableArray<int>.Empty, ImmutableArray<string>.Empty, new[] { 1, 2, 3 });
		Assert.Equal([1, 2, 3], collection.ToList());
	}

	[Fact]
	public static void QueryResultQueue_EnumerableOverload_CopiesIntoAQueue()
	{
		var source = new List<int> { 1, 2 };

		var result = new QueryResultQueue<int>(new[] { 0 }, new[] { "a" }, (IEnumerable<int>)source);
		source.Add(3);

		Assert.Equal([1, 2], result.Result.ToArray());
	}

	[Fact]
	public static void AsMappedDictionaries_MapsNamesToValues_AndConvertsDBNullToNull()
	{
		var result = new QueryResult<IEnumerable<object?[]>>(
			ImmutableArray.Create(0, 1),
			ImmutableArray.Create("Id", "Name"),
			new[]
			{
				new object?[] { 1, "a" },
				new object?[] { 2, DBNull.Value },
			});

		List<Dictionary<string, object?>> dictionaries = result.AsMappedDictionaries().ToList();

		Assert.Equal(2, dictionaries.Count);
		Assert.Equal(1, dictionaries[0]["Id"]);
		Assert.Equal("a", dictionaries[0]["Name"]);
		Assert.Equal(2, dictionaries[1]["Id"]);
		Assert.True(dictionaries[1].ContainsKey("Name"));
		Assert.Null(dictionaries[1]["Name"]);
	}

	[Fact]
	public static void DequeueAsMappedDictionaries_DrainsTheQueueAsItIsEnumerated()
	{
		QueryResult<Queue<object?[]>> result = Rows(
			new object?[] { 1, "a" },
			new object?[] { 2, DBNull.Value },
			new object?[] { 3, "c" });

		IEnumerable<Dictionary<string, object?>> dictionaries = result.DequeueAsMappedDictionaries();
		Assert.Equal(3, result.Result.Count); // Nothing consumed until enumeration.

		Dictionary<string, object?> first = dictionaries.First();
		Assert.Equal(1, first["Id"]);
		Assert.Equal(2, result.Result.Count); // Only the row actually produced was dequeued.

		Assert.Null(dictionaries.First()["Name"]); // Continues where it left off, DBNull -> null.
		Assert.Single(result.Result);
	}

	[Fact]
	public static void DequeueAsMappedDictionaries_NullSource_ThrowsEagerly()
	{
		QueryResult<Queue<object?[]>> result = null!;

		// Not deferred until enumeration.
		Assert.Throws<ArgumentNullException>(() => result.DequeueAsMappedDictionaries());
	}

	[Fact]
	public static async Task DequeueAsMappedDictionaries_Task_AwaitsThenMaps()
	{
		Task<QueryResult<Queue<object?[]>>> task = Task.FromResult(Rows(new object?[] { 1, "a" }));

		IEnumerable<Dictionary<string, object?>> dictionaries = await task.DequeueAsMappedDictionaries();

		Dictionary<string, object?> only = Assert.Single(dictionaries);
		Assert.Equal("a", only["Name"]);
	}
}
