namespace Open.Database.Extensions.Tests;

#nullable enable

// The legacy reader extensions (ResultsBuffered, ResultsBufferedAsync) read the
// reader's CURRENT result set into a queue: they never move to a later set and
// never dispose the reader, and the async one reads with ReadAsync only when asked.
// They are built over the same query core as everything else (Views.ReaderCore):
// this pins that wiring.
[ExcludeFromCodeCoverage]
public static class ResultsBufferedWiringTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static ChannelTestReader WithLaterSet()
		=> new(3, laterSets: 1, failAtNextResult: 1);

	[Fact]
	public static void ResultsBuffered_ReadsTheCurrentSetOnly_AndDoesNotDispose()
	{
		ChannelTestReader reader = WithLaterSet();

		PoolRow[] rows = [.. reader.ResultsBuffered<PoolRow>(null)];

		Assert.Equal([1, 2, 3], rows.Select(r => r.Id));
		Assert.Equal(0, reader.NextResultCalls);
		Assert.Equal(0, reader.Disposes);
		Assert.True(reader.SyncReads > 0);
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static async Task ResultsBufferedAsync_ReadsTheCurrentSetOnly_AndHonoursUseReadAsync(
		bool useReadAsync)
	{
		ChannelTestReader reader = WithLaterSet();

		PoolRow[] rows = [.. await reader.ResultsBufferedAsync<PoolRow>(null, useReadAsync, Ct)];

		Assert.Equal([1, 2, 3], rows.Select(r => r.Id));
		Assert.Equal(0, reader.NextResultCalls);
		Assert.Equal(0, reader.AsyncNextResults);
		Assert.Equal(0, reader.Disposes);

		// ReadAsync only when asked for; otherwise the rows are read with Read.
		if (useReadAsync)
		{
			Assert.True(reader.AsyncReads > 0);
			Assert.Equal(0, reader.SyncReads);
		}
		else
		{
			Assert.Equal(0, reader.AsyncReads);
			Assert.True(reader.SyncReads > 0);
		}
	}
}
