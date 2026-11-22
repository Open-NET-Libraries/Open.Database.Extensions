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
		reader.ForEach(r => sum += (int)r.GetValue(0), cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(7, sum);
	}

}
