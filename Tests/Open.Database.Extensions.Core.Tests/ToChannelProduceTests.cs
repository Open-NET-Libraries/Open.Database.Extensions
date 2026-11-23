using static Open.Database.Extensions.Tests.ChannelFixtures;

namespace Open.Database.Extensions.Tests;

// ToChannelReader, ToChannel and ToChannel(out count): the package makes
// the channel (unbounded, single writer, started at the call, always
// completed) over Core's buffered stream.
[ExcludeFromCodeCoverage]
public static class ToChannelProduceTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public static TheoryData<string> AllViews => ViewNames;

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task ToChannelReader_DeliversEveryRow_InOrder_ThenCompletes(
		string view)
	{
		DbTally tally = Tally(40);

		ChannelReader<ChannelRow> reader = View(view, tally)
			.ToChannelReader(Ct);

		(List<int> rows, Exception? error) = await ChannelWait.Collect(reader);

		Assert.Null(error);
		Assert.Equal(ChannelWait.Ids(40), rows);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task ToChannel_ReturnsTheChannel_AndCompletesIt(string view)
	{
		DbTally tally = Tally(12);

		Channel<ChannelRow> channel = View(view, tally).ToChannel(Ct);

		(List<int> rows, Exception? error) = await ChannelWait.Collect(channel.Reader);
		await channel.Reader.Completion.Within();

		Assert.Null(error);
		Assert.Equal(ChannelWait.Ids(12), rows);
	}

	[Fact]
	public static async Task AnEmptyResult_CompletesWithNoRows()
	{
		DbTally tally = Tally(0);

		(List<int> rows, Exception? error) = await ChannelWait.Collect(
			Read(tally).ToChannelReader(Ct));

		Assert.Null(error);
		Assert.Empty(rows);
	}

	[Fact]
	public static async Task TheCount_IsTheRowsWritten()
	{
		DbTally tally = Tally(25);

		Channel<ChannelRow> channel = Read(tally)
			.ToChannel(out Task<long> readCount, Ct);

		Assert.Equal(25, await readCount.Within());
		Assert.Equal(25, await readCount.Within());
		(List<int> rows, Exception? _) = await ChannelWait.Collect(channel.Reader);
		Assert.Equal(25, rows.Count);
	}

	[Fact]
	public static async Task TheSingleReaderForms_Deliver()
	{
		DbTally tally = Tally(7);

		(List<int> a, Exception? _) = await ChannelWait.Collect(
			Read(tally).ToChannelReader(true, Ct));
		Channel<ChannelRow> channel = Read(tally)
			.ToChannel(out Task<long> count, true, Ct);
		(List<int> b, Exception? _) = await ChannelWait.Collect(channel.Reader);

		Assert.Equal(ChannelWait.Ids(7), a);
		Assert.Equal(ChannelWait.Ids(7), b);
		Assert.Equal(7, await count.Within());
	}

	[Fact]
	public static async Task TheRead_StartsAtTheCall_WithoutAFirstRead()
	{
		DbTally tally = Tally(10);

		Channel<ChannelRow> channel = Read(tally)
			.ToChannel(out Task<long> readCount, Ct);

		// Nobody reads, yet the read runs to the end and the channel is closed.
		Assert.Equal(10, await readCount.Within());
		Assert.False(channel.Writer.TryWrite(new(0)));
		Assert.Equal(10, channel.Reader.Count);
	}

	[Fact]
	public static async Task TheConnection_IsClosed_WhateverTheConsumerDoes()
	{
		DbTally tally = Tally(500);

		// The consumer never reads a row.
		Channel<ChannelRow> channel = Read(tally).ToChannel(Ct);

		await ChannelWait.Until(
			() => tally.Connections.Count == 1
				&& tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
		Assert.Equal(1, tally.Readers[0].Disposes);
		GC.KeepAlive(channel);
	}

	[Fact]
	public static async Task ARowFailure_DeliversTheEarlierRows_ThenFailsTheChannel_AndTheCount()
	{
		DbTally tally = Tally(20, failOnRead: 9);

		Channel<ChannelRow> channel = Read(tally)
			.ToChannel(out Task<long> readCount, Ct);

		(List<int> rows, Exception? error) = await ChannelWait.Collect(channel.Reader);

		Assert.NotNull(error);
		Assert.Contains("failed while reading", Messages(error));
		Assert.Equal(ChannelWait.Ids(rows.Count), rows);
		Assert.InRange(rows.Count, 0, 8);
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => readCount.Within());
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task ACancel_CompletesTheChannelWithIt_NotACleanEnd()
	{
		using CancellationTokenSource cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		DbTally tally = Tally(
			100_000,
			onRead: n => { if (n == 50) cts.Cancel(); });

		Channel<ChannelRow> channel = Read(tally)
			.ToChannel(out Task<long> readCount, cts.Token);

		(List<int> _, Exception? error) = await ChannelWait.Collect(channel.Reader);

		Assert.NotNull(error);
		Assert.True(
			error is OperationCanceledException
			|| error.InnerException is OperationCanceledException,
			error.ToString());
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => readCount.Within());
		await ChannelWait.Until(
			() => tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
	}

	[Fact]
	public static void ATokenAlreadyCanceled_ThrowsAtTheCall_AndStartsNothing()
	{
		DbTally tally = Tally(5);
		using CancellationTokenSource cts = new();
		cts.Cancel();
		MappedQuery<ChannelRow> query = Read(tally);

		Assert.Throws<OperationCanceledException>(
			() => query.ToChannelReader(cts.Token));
		Assert.Throws<OperationCanceledException>(
			() => query.ToChannel(cts.Token));
		Assert.Throws<OperationCanceledException>(
			() => query.ToChannel(out _, cts.Token));
		Assert.Empty(tally.Connections);

		// No stream was made either: the guard comes first.
		FakeSource source = new(Rows(3));
		Assert.Throws<OperationCanceledException>(
			() => source.ToChannelReader(cts.Token));
		Assert.Equal(0, source.ChunkStreams);
	}

	[Fact]
	public static void ADefaultView_ThrowsAtTheCall()
	{
		IMappedQuery<ChannelRow> view = default(MappedQuery<ChannelRow>);

		Assert.Throws<InvalidOperationException>(
			() => view.ToChannelReader(Ct));
	}

	[Fact]
	public static void ANullSource_ThrowsAtTheCall()
	{
		IMappedQuery<ChannelRow> source = null;

		Assert.Throws<ArgumentNullException>(
			() => source.ToChannelReader(Ct));
		Assert.Throws<ArgumentNullException>(
			() => source.ToChannel(out _, Ct));
		Assert.Throws<ArgumentNullException>(
			() => source.ToBoundedChannel(4, Ct));
	}

	[Fact]
	public static async Task SkipAndTake_Apply()
	{
		DbTally tally = Tally(10);

		(List<int> rows, Exception? _) = await ChannelWait.Collect(
			Read(tally).Skip(2).Take(3).ToChannelReader(Ct));

		Assert.Equal([3, 4, 5], rows);
	}

	[Fact]
	public static async Task AsyncReads_ReadsWithReadAsync()
	{
		DbTally tally = Tally(6);

		(List<int> rows, Exception? _) = await ChannelWait.Collect(
			Read(tally).AsyncReads().ToChannelReader(Ct));

		Assert.Equal(ChannelWait.Ids(6), rows);
	}

	[Fact]
	public static async Task AUsersOwnSource_Works()
	{
		FakeSource source = new(Rows(9));

		(List<int> rows, Exception? _) = await ChannelWait.Collect(
			source.ToChannelReader(Ct));

		Assert.Equal(ChannelWait.Ids(9), rows);
		Assert.Equal(1, source.ChunkStreams);
	}

	[Fact]
	public static async Task Bounded_DeliversEveryRow_AWaitingConsumerGetsThemAll()
	{
		DbTally tally = Tally(60);

		ChannelReader<ChannelRow> reader = Read(tally)
			.ToBoundedChannelReader(2, Ct);

		Assert.Equal(ChannelWait.Ids(60), await Drain(reader, 60));
		await reader.Completion.Within();
	}

	[Fact]
	public static async Task Bounded_DoesNotHoldTheConnection()
	{
		DbTally tally = Tally(300);

		// Capacity 1 and nobody reads: the read still finishes and the
		// connection closes; the rows wait in the internal queue.
		Channel<ChannelRow> channel = Read(tally).ToBoundedChannel(1, Ct);

		await ChannelWait.Until(
			() => tally.Connections.Count == 1
				&& tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
		Assert.True(tally.Readers[0].Reads >= 300);

		Assert.Equal(ChannelWait.Ids(300), await Drain(channel.Reader, 300));
	}

	[Theory]
	[InlineData(0)]
	[InlineData(-1)]
	public static void Bounded_ACapacityBelowOne_ThrowsAtTheCall(int capacity)
		=> Assert.Throws<ArgumentOutOfRangeException>(
			() => Read(Tally(1)).ToBoundedChannelReader(capacity, Ct));

	[Fact]
	public static void ThePackage_HasNoDependencyOnOpenChannelExtensions()
		=> Assert.DoesNotContain(
			typeof(ChannelDbExtensions).Assembly.GetReferencedAssemblies(),
			static name => name.Name == "Open.ChannelExtensions");

	static string Messages(Exception ex)
	{
		List<string> all = [];
		for (Exception? e = ex; e is not null; e = e.InnerException)
			all.Add(e.Message);

		return string.Join(" | ", all);
	}
}
