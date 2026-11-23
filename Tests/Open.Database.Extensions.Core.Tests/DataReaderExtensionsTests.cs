namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class DataReaderExtensionsTests
{
	static readonly string[] IdName = ["Id", "Name"];

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

	static object?[][] IdRows(int count)
	{
		var rows = new object?[count][];
		for (int i = 0; i < count; i++) rows[i] = [i];
		return rows;
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(5)]
	[InlineData(2000)]
	public static async Task ToListAsync_ReadsEveryRow_InOrder_IntoAListOfExactlyThatLength(int count)
	{
		System.Data.Common.DbDataReader reader = FakeReader.CreateDb(["Id"], IdRows(count));

		List<int> list = await reader.ToListAsync(
			static r => new ValueTask<int>((int)r.GetValue(0)),
			TestContext.Current.CancellationToken);

		Assert.Equal(Enumerable.Range(0, count), list);
		Assert.Equal(count, list.Capacity);
	}

#if !DEBUG
	// The rows are collected in pooled chunks and copied once, so a call allocates the list and not the growth of one.
	[Fact]
	public static async Task ToListAsync_AllocatesLessThanAListThatGrows()
	{
		const int Count = 2000;
		object?[][] rows = IdRows(Count);
		string[] columns = ["Id"];

		async Task<long> Pooled()
		{
			System.Data.Common.DbDataReader reader = FakeReader.CreateDb(columns, rows);
			long before = GC.GetAllocatedBytesForCurrentThread();
			_ = await reader.ToListAsync(static r => new ValueTask<int>((int)r.GetValue(0)), TestContext.Current.CancellationToken);
			return GC.GetAllocatedBytesForCurrentThread() - before;
		}

		async Task<long> Growing()
		{
			System.Data.Common.DbDataReader reader = FakeReader.CreateDb(columns, rows);
			long before = GC.GetAllocatedBytesForCurrentThread();
			var list = new List<int>();
			while (await reader.ReadAsync(TestContext.Current.CancellationToken)) list.Add((int)reader.GetValue(0));
			return GC.GetAllocatedBytesForCurrentThread() - before;
		}

		// Warm up, then take the smallest of several runs of each.
		for (int i = 0; i < 20; i++)
		{
			await Pooled();
			await Growing();
		}

		long pooled = long.MaxValue, growing = long.MaxValue;
		for (int i = 0; i < 10; i++)
		{
			pooled = Math.Min(pooled, await Pooled());
			growing = Math.Min(growing, await Growing());
		}

		Assert.True(pooled < growing, $"pooled {pooled} B, growing list {growing} B.");
	}
#endif
}
