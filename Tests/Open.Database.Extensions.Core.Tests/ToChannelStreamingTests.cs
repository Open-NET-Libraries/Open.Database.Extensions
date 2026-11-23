using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.ChannelFixtures;

namespace Open.Database.Extensions.Tests;

// ToChannelHoldingConnection(channel | writer, stallTimeout [, end]):
// rows pulled one at a time from Core's Stream into the write loop. A full
// target holds the reader and the connection open; the stall timeout is
// Core's, and the loop only learns of an expiry through the internal
// signal while it waits for room.
[ExcludeFromCodeCoverage]
public static class ToChannelStreamingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly TimeSpan Long = TimeSpan.FromSeconds(30);
	static readonly TimeSpan Short = TimeSpan.FromMilliseconds(200);

	public static TheoryData<string> AllViews => ViewNames;

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task DeliversEveryRow_InOrder_AndLeavesTheChannelOpen(
		string view)
	{
		DbTally tally = Tally(12);
		Channel<ChannelRow> channel = Unbounded();

		long count = await View(view, tally)
			.ToChannelHoldingConnection(channel, Long, Ct);

		Assert.Equal(12, count);
		Assert.False(channel.Reader.Completion.IsCompleted);
		Assert.Equal(ChannelWait.Ids(12), await Drain(channel.Reader, 12));
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task Complete_ClosesTheChannel_AfterTheLastRow(
		string view)
	{
		DbTally tally = Tally(5);
		Channel<ChannelRow> channel = Unbounded();

		await View(view, tally)
			.ToChannelHoldingConnection(channel.Writer, Long, complete: true, Ct);

		(List<int> rows, Exception? error) = await ChannelWait.Collect(channel.Reader);
		Assert.Null(error);
		Assert.Equal(ChannelWait.Ids(5), rows);
	}

	[Fact]
	public static async Task AFullChannel_HoldsTheReaderOpen_UntilTheConsumerTakesRows()
	{
		DbTally tally = Tally(50);
		Channel<ChannelRow> channel = Bounded(1);

		Task<long> task = Read(tally)
			.ToChannelHoldingConnection(channel, Long, Ct)
			.AsTask();

		// Backpressure reaches the reader: it stops a few rows in
		// and the connection stays open.
		await ChannelWait.Until(
			() => tally.Readers.Count == 1 && tally.Readers[0].Reads >= 3,
			"the read to start");
		await Task.Delay(300, Ct);
		int reads = tally.Readers[0].Reads;
		Assert.InRange(reads, 3, 10);
		Assert.Equal(ConnectionState.Open, tally.Connections[0].State);
		Assert.False(task.IsCompleted);

		// The consumer drains: the read finishes and the connection closes.
		Assert.Equal(ChannelWait.Ids(50), await Drain(channel.Reader, 50));
		Assert.Equal(50, await task.Within());
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task ItWaitsForRoom_BeforeTheCommandRuns()
	{
		DbTally tally = Tally(3);
		Channel<ChannelRow> channel = Bounded(1);
		Assert.True(channel.Writer.TryWrite(new(0)));

		Task<long> task = Read(tally)
			.ToChannelHoldingConnection(channel, Short, Ct)
			.AsTask();

		// The first wait is not timed and holds nothing.
		await Task.Delay(600, Ct);
		Assert.False(task.IsCompleted);
		Assert.Empty(tally.Connections);

		_ = await channel.Reader.ReadAsync(Ct);
		Assert.Equal([1, 2, 3], await Drain(channel.Reader, 3));
		Assert.Equal(3, await task.Within());
	}

	[Fact]
	public static async Task AStalledTarget_TripsTheTimeout_AndClosesTheConnection()
	{
		DbTally tally = Tally(50);
		Channel<ChannelRow> channel = Bounded(1);

		TimeoutException error = await Assert.ThrowsAsync<TimeoutException>(
			async () => await Read(tally)
				.ToChannelHoldingConnection(channel, Short, Ct)
				.AsTask()
				.Within());

		Assert.Contains(Short.ToString(), error.Message);
		await ChannelWait.Until(
			() => tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
		Assert.Equal(1, tally.Readers[0].Disposes);
	}

	[Fact]
	public static async Task AStall_UnderComplete_CompletesTheChannelWithTheTimeout()
	{
		DbTally tally = Tally(50);
		Channel<ChannelRow> channel = Bounded(1);

		await Assert.ThrowsAsync<TimeoutException>(
			async () => await Read(tally)
				.ToChannelHoldingConnection(channel, Short, complete: true, Ct)
				.AsTask()
				.Within());

		// The rows written before the stall are still there,
		// then the channel ends with the timeout.
		Assert.Equal(1, (await channel.Reader.ReadAsync(Ct)).Id);
		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(
			async () => await channel.Reader.ReadAsync(Ct));
		Assert.True(
			thrown is TimeoutException || thrown.InnerException is TimeoutException,
			thrown.ToString());
	}

	[Fact]
	public static async Task AStall_UnderKeepOpen_LeavesTheChannelWritable()
	{
		DbTally tally = Tally(50);
		Channel<ChannelRow> channel = Bounded(1);

		await Assert.ThrowsAsync<TimeoutException>(
			async () => await Read(tally)
				.ToChannelHoldingConnection(channel, Short, Ct)
				.AsTask()
				.Within());

		Assert.False(channel.Reader.Completion.IsCompleted);
		_ = await channel.Reader.ReadAsync(Ct);
		Assert.True(channel.Writer.TryWrite(new(99)));
	}

	[Fact]
	public static async Task AMovingConsumer_DoesNotTripTheTimeout()
	{
		DbTally tally = Tally(30);
		Channel<ChannelRow> channel = Bounded(1);

		Task<long> task = Read(tally)
			.ToChannelHoldingConnection(channel, TimeSpan.FromMilliseconds(400), Ct)
			.AsTask();

		// One row every 40 ms: well over the pass's length against the
		// timeout, but never a standstill of the timeout.
		List<int> rows = [];
		while (rows.Count < 30)
		{
			await Task.Delay(40, Ct);
			rows.Add((await channel.Reader.ReadAsync(Ct)).Id);
		}

		Assert.Equal(ChannelWait.Ids(30), rows);
		Assert.Equal(30, await task.Within());
	}

	[Theory]
	[InlineData(-1)]
	[InlineData(3650 * 24 * 3600)]
	public static async Task AnInfiniteTimeout_OrAHugeOne_WaitsForThe_Consumer(
		int seconds)
	{
		// -1 stands for Timeout.InfiniteTimeSpan; the other is 10 years.
		TimeSpan stall = seconds < 0
			? Timeout.InfiniteTimeSpan
			: TimeSpan.FromSeconds(seconds);
		DbTally tally = Tally(10);
		Channel<ChannelRow> channel = Bounded(1);

		Task<long> task = Read(tally)
			.ToChannelHoldingConnection(channel, stall, Ct)
			.AsTask();

		await Task.Delay(500, Ct);
		Assert.False(task.IsCompleted);

		Assert.Equal(ChannelWait.Ids(10), await Drain(channel.Reader, 10));
		Assert.Equal(10, await task.Within());
	}

	[Fact]
	public static async Task ACallerCancel_WhileWaiting_IsACancel_NotATimeout()
	{
		DbTally tally = Tally(50);
		Channel<ChannelRow> channel = Bounded(1);
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);

		Task<long> task = Read(tally)
			.ToChannelHoldingConnection(channel, Long, cts.Token)
			.AsTask();
		await ChannelWait.Until(
			() => tally.Readers.Count == 1 && tally.Readers[0].Reads >= 3,
			"the pass to be waiting");

		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => task.Within());
		await ChannelWait.Until(
			() => tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
	}

	[Fact]
	public static async Task AClosedTarget_StopsThePass_WithoutAnException()
	{
		DbTally tally = Tally(100_000);
		Channel<ChannelRow> channel = Bounded(2);

		Task<long> task = Read(tally)
			.ToChannelHoldingConnection(channel, Long, Ct)
			.AsTask();
		await ChannelWait.Until(
			() => channel.Reader.Count == 2 && tally.Readers.Count == 1,
			"the channel to fill");

		channel.Writer.TryComplete();

		Assert.InRange(await task.Within(), 2, 99_999);
		await ChannelWait.Until(
			() => tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
	}

	[Fact]
	public static async Task AFailure_ComesBackThroughTheTask()
	{
		DbTally tally = Tally(20, failOnRead: 6);
		Channel<ChannelRow> channel = Unbounded();

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Read(tally)
				.ToChannelHoldingConnection(channel, Long, Ct));

		Assert.False(channel.Reader.Completion.IsCompleted);
	}

	[Fact]
	public static void ATokenAlreadyCanceled_ThrowsAtTheCall()
	{
		DbTally tally = Tally(5);
		using CancellationTokenSource cts = new();
		cts.Cancel();

		Assert.Throws<OperationCanceledException>(
			() => Drop(Read(tally).ToChannelHoldingConnection(
				Unbounded(), Long, complete: true, cts.Token)));
		Assert.Empty(tally.Connections);

		// No stream was made either: the guard comes first.
		FakeSource source = new(Rows(3));
		Assert.Throws<OperationCanceledException>(
			() => Drop(source.ToChannelHoldingConnection(
				Unbounded(), Long, cts.Token)));
		Assert.Null(source.StallTimeout);
	}

	[Theory]
	[InlineData(0)]
	[InlineData(-1000)]
	[InlineData(-2)]
	public static void ABadTimeout_ThrowsAtTheCall_BeforeTheTargetIsTouched(
		int milliseconds)
	{
		DbTally tally = Tally(5);
		Channel<ChannelRow> channel = Unbounded();

		Assert.Throws<ArgumentOutOfRangeException>(
			() => Drop(Read(tally).ToChannelHoldingConnection(
				channel,
				TimeSpan.FromMilliseconds(milliseconds),
				complete: true,
				Ct)));
		Assert.False(channel.Reader.Completion.IsCompleted);
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static void TheGuards_ThrowAtTheCall()
	{
		MappedQuery<ChannelRow> query = Read(Tally(1));
		IMappedQuery<ChannelRow> none = null;

		Assert.Throws<ArgumentNullException>(
			() => Drop(none.ToChannelHoldingConnection(Unbounded(), Long, Ct)));
		Assert.Throws<ArgumentNullException>(
			() => Drop(query.ToChannelHoldingConnection(null, Long, Ct)));
		Assert.Throws<ArgumentNullException>(
			() => Drop(query.ToChannelHoldingConnection((ChannelWriter<ChannelRow>)null, Long, Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(default(MappedQuery<ChannelRow>)
				.ToChannelHoldingConnection(Unbounded(), Long, Ct)));
	}

	// The feed adds no timing of its own: the timeout and the token
	// reach Stream untouched, on a source that is not ours.
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task TheTimeoutAndTheToken_ReachStream_Untouched(
		bool infinite)
	{
		TimeSpan stall = infinite ? Timeout.InfiniteTimeSpan : Long;
		FakeSource source = new(Rows(6));
		using CancellationTokenSource cts = new();

		await source.ToChannelHoldingConnection(Unbounded(), stall, cts.Token);

		Assert.Equal(stall, source.StallTimeout);
		Assert.Equal(cts.Token, source.StreamToken);
	}

	// A stream without a stall signal is waited on with the caller's own
	// token: nothing is linked, so cancelling it is a plain cancel.
	[Fact]
	public static async Task AStreamWithNoSignal_IsWaitedOnWithTheCallersToken()
	{
		FakeSource source = new(Rows(10));
		Channel<ChannelRow> channel = Bounded(1);
		using CancellationTokenSource cts = new();

		Task<long> task = source
			.ToChannelHoldingConnection(channel, Timeout.InfiniteTimeSpan, cts.Token)
			.AsTask();
		await Task.Delay(300, Ct);
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => task.Within());
	}

	// The signal: while the target is full the loop is not pulling, so
	// it learns of an expiry only through the stream's Expired token, and
	// fails with the stream's own exception, without waiting for room.
	[Fact]
	public static async Task AnExpiry_WhileTheTargetIsFull_FailsThePassPromptly()
	{
		SignalStream stream = new(Rows(10));
		FakeSource source = new(Rows(10), (_, _) => stream);
		Channel<ChannelRow> channel = Bounded(1);

		Task<long> task = source
			.ToChannelHoldingConnection(channel, Long, complete: true, Ct)
			.AsTask();
		await ChannelWait.Until(
			() => stream.Pulled >= 2,
			"the loop to be waiting for room");

		// Nobody ever reads the channel.
		stream.Expire();

		TimeoutException error = await Assert.ThrowsAsync<TimeoutException>(
			() => task.Within());
		Assert.Same(stream.Exception, error);
		// Drained, the channel's own completion shows the error.
		while (channel.Reader.TryRead(out _))
		{
		}

		Assert.True(channel.Reader.Completion.IsFaulted);
	}

	sealed class SignalStream(IReadOnlyList<ChannelRow> rows)
		: IAsyncEnumerable<ChannelRow>, IStallSignal
	{
		readonly CancellationTokenSource _expired = new();
		int _pulled;

		public int Pulled => Volatile.Read(ref _pulled);

		public TimeoutException Exception { get; } = new("stalled");

		public CancellationToken Expired => _expired.Token;

		public void Expire() => _expired.Cancel();

		public TimeoutException ToException() => Exception;

		public async IAsyncEnumerator<ChannelRow> GetAsyncEnumerator(
			CancellationToken cancellationToken = default)
		{
			foreach (ChannelRow row in rows)
			{
				await Task.Yield();
				Interlocked.Increment(ref _pulled);
				yield return row;
			}
		}
	}
}
