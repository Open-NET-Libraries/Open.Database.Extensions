using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The chunk sizes are a configuration object (ChunkSizes): the shipped
// values are ChunkSizes.Default, and a test gives a query its own through a
// ChunkConfig, so nothing is global and nothing is set and put back.
[ExcludeFromCodeCoverage]
public static class ChunkSizingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static MappedCommand<ChannelRow> Rows(int n, ChunkSizes? sizes = null)
	{
		MappedCommand<ChannelRow> view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(n))
			.Map<ChannelRow>();

		return sizes is null
			? view
			: view.WithConfig(
				new ChunkConfig<ChannelRow>(
					null,
					PoolClearing.Resolve<ChannelRow>(null),
					sizes));
	}

	static async Task<List<int>> StreamSizes(int rows, ChunkSizes? sizes = null)
	{
		List<int> chunks = [];
		await foreach (var chunk in Rows(rows, sizes).ToAsyncBufferStream(Ct))
			chunks.Add(chunk.Length);

		return chunks;
	}

	[Fact]
	public static void TheDefaults_AreTheShippedSizes()
	{
		ChunkSizes sizes = ChunkSizes.Default;
		Assert.Equal(16, sizes.FirstRows);
		Assert.Equal(4096, sizes.StreamMaxRows);
		Assert.Equal(16, sizes.CollectFirstRows);
		Assert.Equal(1, sizes.CollectShift);
		Assert.Equal(8192, sizes.CollectMaxRows);
	}

	[Fact]
	public static async Task TheDefaultSizes_AreUnchanged()
	{
		Assert.Equal([16, 32, 52], await StreamSizes(100));
		Assert.Equal(
			[16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 824],
			await StreamSizes(9000));

		// The buffer: 16, 32, 64 ... and the indexer agrees.
		await using var buffer = Rows(100).ToAsyncBuffer(Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		foreach (int i in new[] { 0, 15, 16, 47, 48, 99 })
			Assert.Equal(i + 1, items[i].Id);
	}

	[Fact]
	public static async Task TheSizes_ChangeTheChunks_AndTheIndexerStillAgrees()
	{
		// First 4, stream stops doubling at 32: 4, 8, 16, 32, 32, ...
		var sizes = new ChunkSizes(firstRows: 4, streamMaxRows: 32);
		Assert.Equal([4, 8, 16, 32, 32, 8], await StreamSizes(100, sizes));

		// The buffer doubles from 4 without a stop: 4, 8, 16, 32, 64.
		int[] checkedAt = [0, 3, 4, 11, 12, 27, 28, 59, 60, 99];
		await using var buffer = Rows(100, sizes).ToAsyncBuffer(Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		Assert.Equal(100, items.Count);
		foreach (int i in checkedAt) Assert.Equal(i + 1, items[i].Id);

		Assert.Equal(
			Enumerable.Range(1, 100),
			items.Select(r => r.Id));
	}

	[Theory]
	[InlineData(1)]
	[InlineData(2)]
	public static async Task TheSmallestFirstSizes_KeepTheIndexerAndTheEnumeratorRight(
		int first)
	{
		var sizes = new ChunkSizes(firstRows: first);
		await using var buffer = Rows(100, sizes).ToAsyncBuffer(Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		Assert.Equal(100, items.Count);
		for (int i = 0; i < 100; ++i)
			Assert.Equal(i + 1, items[i].Id);

		List<int> viaForeach = [];
		foreach (ChannelRow row in items) viaForeach.Add(row.Id);

		Assert.Equal(Enumerable.Range(1, 100), viaForeach);
	}

	[Fact]
	public static async Task AStreamWithNoDoubling_HasEqualChunks()
	{
		// StreamMaxRows equal to FirstRows: every chunk is the same size.
		var sizes = new ChunkSizes(firstRows: 2, streamMaxRows: 2);

		Assert.Equal([2, 2, 2, 2, 2], await StreamSizes(10, sizes));
	}

	[Fact]
	public static async Task AValueThatDoesNotFit_IsIgnored()
	{
		// 3 is not a power of two, and 5 is not one either: both are
		// ignored and the defaults are used.
		var sizes = new ChunkSizes(firstRows: 3, streamMaxRows: 5);

		Assert.Equal(16, sizes.FirstRows);
		Assert.Equal(4096, sizes.StreamMaxRows);
		Assert.Equal([16, 32, 52], await StreamSizes(100, sizes));
	}

	[Theory]
	[InlineData(16, 1, 8192, 16, 32)]
	[InlineData(32, 1, 8192, 4096, 8192)]
	[InlineData(4096, 1, 8192, 8192, 8192)]
	[InlineData(16, 2, 8192, 16, 64)]
	[InlineData(1024, 2, 16_384, 4096, 16_384)]
	public static void TheCollectSchedule_GrowsByTheShift_UpToTheMax(
		int first,
		int shift,
		int max,
		int from,
		int expected)
	{
		var sizes = new ChunkSizes(
			collectFirstRows: first,
			collectShift: shift,
			collectMaxRows: max);

		Assert.Equal(first, sizes.CollectFirstRows);
		Assert.Equal(expected, sizes.NextCollectRows(from));
	}

	[Fact]
	public static void ACollectShiftOrMaxThatDoesNotFit_IsIgnored()
	{
		var sizes = new ChunkSizes(collectShift: 9, collectMaxRows: 1000);

		Assert.Equal(1, sizes.CollectShift);
		Assert.Equal(8192, sizes.CollectMaxRows);
	}
}
