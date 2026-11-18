using System.Reflection;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The chunk sizes are internal static fields a benchmark sets by
// reflection. They are process-wide, so every test class that reads rows
// through the buffer terminals is in the one "ChunkSizing" collection
// (xUnit runs a collection's classes one after the other): a changed size
// here can never reach a read elsewhere.
[ExcludeFromCodeCoverage]
[Collection("ChunkSizing")]
public static class ChunkSizingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly Type Sizing = typeof(PooledBuffer<>).Assembly
		.GetType("Open.Database.Extensions.Core.ChunkSizing")!;

	const BindingFlags Internal
		= BindingFlags.Static | BindingFlags.NonPublic;

	static FieldInfo Field(string name)
		=> Sizing.GetField(name, Internal)!;

	static async Task<T> WithSizes<T>(
		int first,
		int streamMax,
		Func<Task<T>> body)
	{
		FieldInfo firstRows = Field("FirstRows");
		FieldInfo streamMaxRows = Field("StreamMaxRows");
		object? oldFirst = firstRows.GetValue(null);
		object? oldMax = streamMaxRows.GetValue(null);
		firstRows.SetValue(null, first);
		streamMaxRows.SetValue(null, streamMax);
		try
		{
			return await body();
		}
		finally
		{
			firstRows.SetValue(null, oldFirst);
			streamMaxRows.SetValue(null, oldMax);
		}
	}

	static MappedCommand<ChannelRow> Rows(int n)
		=> new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(n))
			.Map<ChannelRow>();

	static async Task<List<int>> StreamSizes(int rows)
	{
		List<int> sizes = [];
		await foreach (var chunk in Rows(rows)
			.ToAsyncBufferStream(clearOnReturn: false, Ct))
		{
			sizes.Add(chunk.Length);
		}

		return sizes;
	}

	[Fact]
	public static void TheDefaults_AreTheShippedSizes()
	{
		Assert.Equal(16, Field("FirstRows").GetValue(null));
		Assert.Equal(4096, Field("StreamMaxRows").GetValue(null));
	}

	[Fact]
	public static async Task TheDefaultSizes_AreUnchanged()
	{
		Assert.Equal([16, 32, 52], await StreamSizes(100));
		Assert.Equal(
			[16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 824],
			await StreamSizes(9000));

		// The buffer: 16, 32, 64 ... and the indexer agrees.
		await using var buffer = Rows(100).ToAsyncBuffer(
			clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		foreach (int i in new[] { 0, 15, 16, 47, 48, 99 })
			Assert.Equal(i + 1, items[i].Id);
	}

	[Fact]
	public static async Task TheKnobs_ChangeTheSizes_AndTheIndexerStillAgrees()
	{
		// First 4, stream stops doubling at 32: 4, 8, 16, 32, 32, ...
		List<int> sizes = await WithSizes(4, 32, () => StreamSizes(100));
		Assert.Equal([4, 8, 16, 32, 32, 8], sizes);

		// The buffer doubles from 4 without a stop: 4, 8, 16, 32, 64.
		int[] checkedAt = [0, 3, 4, 11, 12, 27, 28, 59, 60, 99];
		await WithSizes(4, 32, async () =>
		{
			await using var buffer = Rows(100).ToAsyncBuffer(
				clearOnDispose: false, Ct);
			PooledArray<ChannelRow> items = await buffer.Items.Within();
			Assert.Equal(100, items.Count);
			foreach (int i in checkedAt) Assert.Equal(i + 1, items[i].Id);

			Assert.Equal(
				Enumerable.Range(1, 100),
				items.Select(r => r.Id));
			return 0;
		});
	}

	[Fact]
	public static async Task AKnobThatDoesNotFit_IsIgnored()
	{
		// 3 is not a power of two, and 5 is not one either: both are
		// ignored and the defaults are used.
		List<int> sizes = await WithSizes(3, 5, () => StreamSizes(100));

		Assert.Equal([16, 32, 52], sizes);

		// And the knobs were put back.
		Assert.Equal(16, Field("FirstRows").GetValue(null));
	}
}
