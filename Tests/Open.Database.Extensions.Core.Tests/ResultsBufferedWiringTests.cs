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
}
