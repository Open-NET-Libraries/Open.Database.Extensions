namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class DataReaderExtensionsTests
{
	static readonly string[] IdName = ["Id", "Name"];
	static readonly string[] IdNameExtra = ["Id", "Name", "Extra"];

	[Fact]
	public static void AsEnumerable_All_YieldsRowArrays()
	{
		IDataReader reader = FakeReader.Create(IdName, [1, "a"], [2, "b"]);

		List<object[]> rows = [.. reader.AsEnumerable()];

		Assert.Equal(2, rows.Count);
		Assert.Equal([1, "a"], rows[0]);
		Assert.Equal([2, "b"], rows[1]);
	}

	[Fact]
	public static void AsEnumerable_Ordinals_ProjectsSelectedColumns()
	{
		IDataReader reader = FakeReader.Create(IdNameExtra, [1, "a", "x"]);

		IEnumerable<int> ordinals = [2, 0];
		object[] row = Assert.Single([.. reader.AsEnumerable(ordinals)]);

		Assert.Equal(["x", 1], row);
	}

	[Fact]
	public static void AsEnumerable_ParamsOrdinals_ProjectsSelectedColumns()
	{
		IDataReader reader = FakeReader.Create(IdNameExtra, [1, "a", "x"]);

		// n = 0, others = 2  ->  ordinals [0, 2]
		object[] row = Assert.Single([.. reader.AsEnumerable(0, 2)]);

		Assert.Equal([1, "x"], row);
	}

	[Fact]
	public static void Select_TransformsEachRecord()
	{
		IDataReader reader = FakeReader.Create(["Id"], [1], [2], [3]);

		List<int> ids = [.. reader.Select(r => (int)r.GetValue(0))];

		Assert.Equal([1, 2, 3], ids);
	}

	[Fact]
	public static void ForEach_InvokesHandlerPerRow()
	{
		IDataReader reader = FakeReader.Create(["Id"], [1], [2], [4]);

		int sum = 0;
		reader.ForEach(r => sum += (int)r.GetValue(0));

		Assert.Equal(7, sum);
	}

	[Fact]
	public static void ToList_ToArray_ToImmutableArray_ProduceSameSequence()
	{
		static int First(IDataRecord r) => (int)r.GetValue(0);

		Assert.Equal([1, 2], FakeReader.Create(["Id"], [1], [2]).ToList(First));
		Assert.Equal([1, 2], FakeReader.Create(["Id"], [1], [2]).ToArray(First));
		// ImmutableArray<T> implements IEquatable<T> (reference equality), so compare as a sequence.
		Assert.Equal([1, 2], FakeReader.Create(["Id"], [1], [2]).ToImmutableArray(First).ToArray());
	}

	[Fact]
	public static void FirstOrdinalResults_ReadsFirstColumn_ConvertsDBNull()
	{
		IDataReader reader = FakeReader.Create(IdName, [1, "a"], [DBNull.Value, "b"]);

		List<object?> firsts = [.. reader.FirstOrdinalResults()];

		Assert.Equal(2, firsts.Count);
		Assert.Equal(1, firsts[0]);
		Assert.Null(firsts[1]); // DBNull -> null
	}
}
