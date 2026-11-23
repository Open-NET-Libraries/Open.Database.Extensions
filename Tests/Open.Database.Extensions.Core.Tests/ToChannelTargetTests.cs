using static Open.Database.Extensions.Tests.ChannelFixtures;

namespace Open.Database.Extensions.Tests;

// ToChannel(channel | writer [, end]): the caller owns the channel, the
// package writes only, the channel stays open unless asked, the task is
// the done signal. Rows come from Core's buffered stream.
[ExcludeFromCodeCoverage]
public static class ToChannelTargetTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public static TheoryData<string> AllViews => ViewNames;

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task KeepOpen_IsTheDefault_AndPassesShareOneChannel(
		string view)
	{
		DbTally tally = Tally(5);
		Channel<ChannelRow> channel = Unbounded();

		long first = await View(view, tally).ToChannel(channel, Ct);
		long second = await View(view, tally).ToChannel(channel.Writer, Ct);

		Assert.Equal(5, first);
		Assert.Equal(5, second);
		Assert.False(channel.Reader.Completion.IsCompleted);
		Assert.True(channel.Writer.TryWrite(new(99)));
		Assert.Equal(
			[1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 99],
			await Drain(channel.Reader, 11));
	}

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task Complete_ClosesTheChannel_AfterTheLastRow(
		string view)
	{
		DbTally tally = Tally(8);
		Channel<ChannelRow> channel = Unbounded();

		long count = await View(view, tally)
			.ToChannel(channel, complete: true, Ct);

		Assert.Equal(8, count);
		Assert.False(channel.Writer.TryWrite(new(0)));
		(List<int> rows, Exception? error) = await ChannelWait.Collect(channel.Reader);
		Assert.Null(error);
		Assert.Equal(ChannelWait.Ids(8), rows);
	}

	[Fact]
	public static async Task TheWriterForms_Behave_LikeTheChannelForms()
	{
		DbTally tally = Tally(6);
		Channel<ChannelRow> channel = Unbounded();

		await Read(tally).ToChannel(channel.Writer, complete: true, Ct);

		Assert.False(channel.Writer.TryWrite(new(0)));
		Assert.Equal(6, channel.Reader.Count);
	}

	[Fact]
	public static async Task TheTask_CompletesAfterTheConnectionIsClosed()
	{
		DbTally tally = Tally(30);
		Channel<ChannelRow> channel = Unbounded();

		await Read(tally).ToChannel(channel, complete: true, Ct);

		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
		Assert.False(channel.Writer.TryWrite(new(0)));
	}

	[Fact]
	public static async Task AFullBoundedChannel_WaitsForRoom_AndDropsNothing()
	{
		DbTally tally = Tally(40);
		Channel<ChannelRow> channel = Bounded(1);

		ValueTask<long> pass = Read(tally).ToChannel(channel, Ct);
		Task<long> task = pass.AsTask();

		List<int> rows = await Drain(channel.Reader, 40);

		Assert.Equal(ChannelWait.Ids(40), rows);
		Assert.Equal(40, await task.Within());
	}

	[Fact]
	public static async Task ItWaitsForRoom_BeforeTheCommandRuns()
	{
		DbTally tally = Tally(5);
		Channel<ChannelRow> channel = Bounded(1);
		Assert.True(channel.Writer.TryWrite(new(0)));

		Task<long> task = Read(tally).ToChannel(channel, Ct).AsTask();

		// No room: no command, no connection, however long we wait.
		await Task.Delay(300, Ct);
		Assert.False(task.IsCompleted);
		Assert.Empty(tally.Connections);
		Assert.Empty(tally.Commands);

		// Room appears: the pass runs.
		Assert.Equal(0, (await channel.Reader.ReadAsync(Ct)).Id);
		Assert.Equal([1, 2, 3, 4, 5], await Drain(channel.Reader, 5));
		Assert.Equal(5, await task.Within());
	}

	[Fact]
	public static async Task AClosedTarget_StopsThePass_WithoutAnException()
	{
		DbTally tally = Tally(100_000);
		Channel<ChannelRow> channel = Bounded(2);

		Task<long> task = Read(tally).ToChannel(channel, Ct).AsTask();

		// Wait until the pass is blocked on the full channel, then close it.
		await ChannelWait.Until(
			() => channel.Reader.Count == 2 && tally.Readers.Count == 1,
			"the channel to fill");
		channel.Writer.TryComplete();

		long count = await task.Within();

		Assert.InRange(count, 2, 99_999);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task ATargetClosed_BeforeThePass_ReturnsZero_AndReadsNothing()
	{
		DbTally tally = Tally(10);
		Channel<ChannelRow> channel = Unbounded();
		channel.Writer.TryComplete();

		long count = await Read(tally).ToChannel(channel, Ct);

		Assert.Equal(0, count);
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static async Task ATargetCompletedWithAnError_ThrowsThatError()
	{
		DbTally tally = Tally(10);
		Channel<ChannelRow> channel = Unbounded();
		InvalidOperationException owner = new("the owner's error");
		channel.Writer.TryComplete(owner);

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(
			async () => await Read(tally).ToChannel(channel, Ct));

		Assert.True(
			ReferenceEquals(thrown, owner) || ReferenceEquals(thrown.InnerException, owner),
			thrown.ToString());
	}

	[Fact]
	public static async Task ACancel_UnderKeepOpen_FaultsTheTask_AndLeavesTheChannelAlone()
	{
		using CancellationTokenSource cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		DbTally tally = Tally(
			100_000,
			onRead: n => { if (n == 50) cts.Cancel(); });
		Channel<ChannelRow> channel = Unbounded();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Read(tally).ToChannel(channel, cts.Token));

		Assert.False(channel.Reader.Completion.IsCompleted);
		Assert.True(channel.Writer.TryWrite(new(-1)));
		Assert.Equal(
			ConnectionState.Closed,
			tally.Connections[0].State);
	}

	[Fact]
	public static async Task ACancel_UnderComplete_CompletesTheChannelWithIt()
	{
		using CancellationTokenSource cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		DbTally tally = Tally(
			100_000,
			onRead: n => { if (n == 50) cts.Cancel(); });
		Channel<ChannelRow> channel = Unbounded();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Read(tally)
				.ToChannel(channel, complete: true, cts.Token));

		(List<int> _, Exception? error) = await ChannelWait.Collect(channel.Reader);
		Assert.NotNull(error);
		Assert.True(
			error is OperationCanceledException
			|| error.InnerException is OperationCanceledException,
			error.ToString());
	}

	[Fact]
	public static async Task AFailure_UnderKeepOpen_ComesBackThroughTheTask_ChannelOpen()
	{
		DbTally tally = Tally(20, failOnRead: 8);
		Channel<ChannelRow> channel = Unbounded();

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Read(tally).ToChannel(channel, Ct));

		Assert.False(channel.Reader.Completion.IsCompleted);
		Assert.True(channel.Writer.TryWrite(new(-1)));
	}

	[Fact]
	public static async Task AFailure_UnderComplete_AlsoCompletesTheChannelWithIt()
	{
		DbTally tally = Tally(20, failOnRead: 8);
		Channel<ChannelRow> channel = Unbounded();

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Read(tally)
				.ToChannel(channel, complete: true, Ct));

		(List<int> rows, Exception? error) = await ChannelWait.Collect(channel.Reader);
		Assert.NotNull(error);
		Assert.Equal(ChannelWait.Ids(rows.Count), rows);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void ATokenAlreadyCanceled_ThrowsAtTheCall_AndTouchesNothing(
		bool complete)
	{
		DbTally tally = Tally(5);
		Channel<ChannelRow> channel = Unbounded();
		using CancellationTokenSource cts = new();
		cts.Cancel();
		MappedQuery<ChannelRow> query = Read(tally);

		Assert.Throws<OperationCanceledException>(
			() => Drop(query.ToChannel(
				channel,
				complete,
				cts.Token)));

		Assert.False(channel.Reader.Completion.IsCompleted);
		Assert.Empty(tally.Connections);

		// No stream was made either: the guard comes first.
		FakeSource source = new(Rows(3));
		Assert.Throws<OperationCanceledException>(
			() => Drop(source.ToChannel(
				channel, complete: true, cts.Token)));
		Assert.Equal(0, source.ChunkStreams);
	}

	[Fact]
	public static void TheGuards_ThrowAtTheCall()
	{
		DbTally tally = Tally(1);
		MappedQuery<ChannelRow> query = Read(tally);
		Channel<ChannelRow> channel = Unbounded();
		IMappedQuery<ChannelRow> none = null;

		Assert.Throws<ArgumentNullException>(
			() => Drop(none.ToChannel(channel, Ct)));
		Assert.Throws<ArgumentNullException>(
			() => Drop(query.ToChannel(null, Ct)));
		Assert.Throws<ArgumentNullException>(
			() => Drop(query.ToChannel((ChannelWriter<ChannelRow>)null, Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(default(MappedQuery<ChannelRow>)
				.ToChannel(channel, Ct)));
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static async Task AUsersOwnSource_Works()
	{
		FakeSource source = new(Rows(10));
		Channel<ChannelRow> channel = Unbounded();

		Assert.Equal(10, await source.ToChannel(channel, complete: true, Ct));

		(List<int> rows, Exception? _) = await ChannelWait.Collect(channel.Reader);
		Assert.Equal(ChannelWait.Ids(10), rows);
	}

	[Fact]
	public static async Task ManyRows_ThroughAOneSlotChannel_ArriveOnce_InOrder()
	{
		DbTally tally = Tally(20_000);
		Channel<ChannelRow> channel = Bounded(1);

		Task<long> task = Read(tally).ToChannel(channel, Ct).AsTask();

		Assert.Equal(ChannelWait.Ids(20_000), await Drain(channel.Reader, 20_000));
		Assert.Equal(20_000, await task.Within());
	}

	[Fact]
	public static async Task ConcurrentPasses_IntoOneSharedChannel_WriteEveryRowOnce()
	{
		const int Passes = 16;
		const int Rows = 500;
		Channel<ChannelRow> channel = Unbounded();

		long[] counts = await Task.WhenAll(
			Enumerable.Range(0, Passes).Select(
				_ => Read(Tally(Rows)).ToChannel(channel, Ct).AsTask()))
			.Within();

		Assert.All(counts, static c => Assert.Equal(Rows, c));
		List<int> ids = await Drain(channel.Reader, Passes * Rows);
		Assert.Equal(Passes * Rows, ids.Count);
		Assert.All(
			ids.GroupBy(static id => id),
			static g => Assert.Equal(Passes, g.Count()));
		Assert.False(channel.Reader.Completion.IsCompleted);
	}

	public static TheoryData<string> Starts
		=> ["buffered", "bufferedFullFirst", "streamingFullFirst"];

	// Every await of the package must be ConfigureAwait(false): one that
	// is not posts its continuation back to the context it started under.
	// The context is current only while the pass starts, so this sees
	// the first suspension of each path: the first wait for room (a full
	// channel at the start) and the first read of the stream.
	[Theory]
	[MemberData(nameof(Starts))]
	public static async Task TheCalls_NeverPostBackToTheCallersContext(
		string start)
	{
		CountingContext context = new();
		SynchronizationContext? previous = SynchronizationContext.Current;
		DbTally tally = Tally(200);
		Channel<ChannelRow> channel = Bounded(1);
		bool full = start != "buffered";
		if (full) Assert.True(channel.Writer.TryWrite(new(0)));

		Task<long> task;
		SynchronizationContext.SetSynchronizationContext(context);
		try
		{
			task = start == "streamingFullFirst"
				? Read(tally)
					.ToChannelHoldingConnection(channel, Long, complete: true, Ct)
					.AsTask()
				: Read(tally)
					.ToChannel(channel, complete: true, Ct)
					.AsTask();
		}
		finally
		{
			SynchronizationContext.SetSynchronizationContext(previous);
		}

		if (full) _ = await channel.Reader.ReadAsync(Ct);
		Assert.Equal(200, (await Drain(channel.Reader, 200)).Count);
		Assert.Equal(200, await task.Within());
		Assert.Equal(0, context.Posts);
	}

	static readonly TimeSpan Long = TimeSpan.FromSeconds(30);

	sealed class CountingContext : SynchronizationContext
	{
		int _posts;
		public int Posts => Volatile.Read(ref _posts);

		public override void Post(SendOrPostCallback d, object? state)
		{
			Interlocked.Increment(ref _posts);
			base.Post(d, state);
		}
	}
}
