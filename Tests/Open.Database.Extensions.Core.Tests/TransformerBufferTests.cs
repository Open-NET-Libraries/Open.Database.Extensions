using Open.Database.Extensions.Core;
using System.Buffers;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// How <see cref="Transformer{T}"/> consumes buffered rows: it must hand pooled buffers back,
/// convert <see cref="DBNull"/>, and wrap property-assignment failures with the property name.
/// </summary>
[ExcludeFromCodeCoverage]
public static class TransformerBufferTests
{
	sealed class Person
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public int? Age { get; set; }
	}

	static QueryResult<Queue<object[]>> Buffered(string[] names, params object[][] rows)
		=> new(
			Enumerable.Range(0, names.Length).ToImmutableArray(),
			names.ToImmutableArray(),
			new Queue<object[]>(rows));

	[Fact]
	public static void AsDequeueingEnumerable_ReturnsEachBufferToThePoolAfterTransformingIt()
	{
		var pool = Substitute.For<ArrayPool<object>>();
		object[] first = [1, "a"];
		object[] second = [2, "b"];
		QueryResult<Queue<object[]>> results = Buffered(["Id", "Name"], first, second);

		IEnumerable<Person> people = Transformer<Person>.Create().AsDequeueingEnumerable(results, pool, clearArrays: true);

		pool.DidNotReceiveWithAnyArgs().Return(default!, default); // Deferred until enumerated.

		var list = people.ToList();

		Assert.Equal([1, 2], list.Select(p => p.Id));
		Assert.Equal(["a", "b"], list.Select(p => p.Name));
		pool.Received(1).Return(first, false); // The transformer clears the columns itself, so the pool is not asked to clear the whole array.
		pool.Received(1).Return(second, false);
		Assert.All(first, v => Assert.Null(v));
		Assert.All(second, v => Assert.Null(v));
		Assert.Empty(results.Result);
	}

	[Fact]
	public static void AsDequeueingEnumerable_ReturnsTheBufferEvenWhenTransformFails()
	{
		var pool = Substitute.For<ArrayPool<object>>();
		object[] bad = ["not an int", "a"];
		QueryResult<Queue<object[]>> results = Buffered(["Id", "Name"], bad);

		IEnumerable<Person> people = Transformer<Person>.Create().AsDequeueingEnumerable(results, pool);

		ValueMappingException ex = Assert.Throws<ValueMappingException>(() => people.ToList());

		Assert.Contains("'Id'", ex.Message);
		Assert.NotNull(ex.InnerException);
		pool.Received(1).Return(bad, false);
	}

	[Fact]
	public static void AsDequeueingEnumerable_DequeuesLazily()
	{
		var pool = Substitute.For<ArrayPool<object>>();
		QueryResult<Queue<object[]>> results = Buffered(["Id"], [1], [2], [3]);

		IEnumerable<Person> people = Transformer<Person>.Create().AsDequeueingEnumerable(results, pool);
		Assert.Equal(3, results.Result.Count);

		Person first = people.First();

		Assert.Equal(1, first.Id);
		Assert.Equal(2, results.Result.Count);
	}

	[Fact]
	public static void Transform_DBNull_BecomesNullForNullableProperties()
	{
		var pool = Substitute.For<ArrayPool<object>>();
		QueryResult<Queue<object[]>> results = Buffered(["Name", "Age"], [DBNull.Value, DBNull.Value]);

		Person person = Transformer<Person>.Create().AsDequeueingEnumerable(results, pool).Single();

		Assert.Null(person.Name);
		Assert.Null(person.Age);
	}

	[Fact]
	public static void Transform_DBNullForNonNullableProperty_LeavesTheDefault()
	{
		var pool = Substitute.For<ArrayPool<object>>();
		QueryResult<Queue<object[]>> results = Buffered(["Id"], [DBNull.Value]);

		Person person = Transformer<Person>.Create().AsDequeueingEnumerable(results, pool).Single();

		Assert.Equal(0, person.Id);
	}

	[Fact]
	public static void Transform_ColumnsWithoutAProperty_AreIgnored_AndColumnMatchIgnoresCase()
	{
		var pool = Substitute.For<ArrayPool<object>>();
		QueryResult<Queue<object[]>> results = Buffered(["extra", "NAME", "id"], ["ignored", "n", 7]);

		Person person = Transformer<Person>.Create().AsDequeueingEnumerable(results, pool).Single();

		Assert.Equal(7, person.Id);
		Assert.Equal("n", person.Name);
	}

	[Fact]
	public static void AsDequeueingEnumerable_NullArguments_Throw()
	{
		var transformer = Transformer<Person>.Create();
		QueryResult<Queue<object[]>> results = Buffered(["Id"]);

		Assert.Equal("results", Assert.Throws<ArgumentNullException>(
			() => transformer.AsDequeueingEnumerable(null!, ArrayPool<object>.Shared)).ParamName);
		Assert.Equal("arrayPool", Assert.Throws<ArgumentNullException>(
			() => transformer.AsDequeueingEnumerable(results, null!)).ParamName);
	}

	[Fact]
	public static void Results_DataTable_ClearTable_BuffersRowsThenClearsTheSource()
	{
		DataTable table = NewTable();

		var people = Transformer<Person>.Create().Results(table, clearTable: true).ToList();

		Assert.Empty(table.Rows);
		Assert.Equal([1, 2], people.Select(p => p.Id));
		Assert.Equal(["a", "b"], people.Select(p => p.Name));
	}

	[Fact]
	public static void Results_DataTable_NoClear_LeavesTheSourceIntact()
	{
		DataTable table = NewTable();

		var people = Transformer<Person>.Create().Results(table, clearTable: false).ToList();

		Assert.Equal(2, table.Rows.Count);
		Assert.Equal(2, people.Count);
	}

	static DataTable NewTable()
	{
		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		table.Columns.Add("Unmapped", typeof(string));
		table.Rows.Add(1, "a", "x");
		table.Rows.Add(2, "b", "y");
		return table;
	}
}
