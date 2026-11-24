using System.Threading.Tasks.Dataflow;
using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.BlockFixtures;
using static Open.Database.Extensions.Tests.ChannelFixtures;

namespace Open.Database.Extensions.Tests;

// ToTargetBlockHoldingConnection(target, stallTimeout [, end]): rows pulled
// one at a time from Core's Stream. A full block holds the reader and the
// connection open; the stall timeout is Core's, and the loop only learns of
// an expiry through the internal signal while it waits for the block.
[ExcludeFromCodeCoverage]
public static class ToTargetBlockStreamingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly TimeSpan Long = TimeSpan.FromSeconds(30);
	static readonly TimeSpan Short = TimeSpan.FromMilliseconds(200);

	public static TheoryData<string> AllViews => ViewNames;

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task DeliversEveryRow_InOrder_AndLeavesTheBlockOpen(
		string view)
	{
		DbTally tally = Tally(12);
		BufferBlock<ChannelRow> block = NewBlock();

		long count = await View(view, tally)
			.ToTargetBlockHoldingConnection(block, Long, Ct);

		Assert.Equal(12, count);
		Assert.False(block.Completion.IsCompleted);
		Assert.Equal(ChannelWait.Ids(12), await Receive(block, 12));
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task Complete_CompletesTheBlock_AfterTheLastRow(
		string view)
	{
		DbTally tally = Tally(5);
		BufferBlock<ChannelRow> block = NewBlock();

		await View(view, tally)
			.ToTargetBlockHoldingConnection(block, Long, complete: true, Ct);

		(List<int> rows, Exception? error) = await Collect(block);
		Assert.Null(error);
		Assert.Equal(ChannelWait.Ids(5), rows);
	}

	[Fact]
	public static async Task AFullBlock_HoldsTheReaderOpen_UntilTheConsumerTakesRows()
	{
		DbTally tally = Tally(50);
		BufferBlock<ChannelRow> block = NewBounded(1);

		Task<long> task = Read(tally)
			.ToTargetBlockHoldingConnection(block, Long, Ct)
			.AsTask();

		// Backpressure reaches the reader: it stops a few rows in
		// and the connection stays open.
		await ChannelWait.Until(
			() => tally.Readers.Count == 1 && tally.Readers[0].Reads >= 3,
			"the read to start");
		await Task.Delay(300, Ct);
		Assert.InRange(tally.Readers[0].Reads, 3, 10);
		Assert.Equal(ConnectionState.Open, tally.Connections[0].State);
		Assert.False(task.IsCompleted);

		Assert.Equal(ChannelWait.Ids(50), await Receive(block, 50));
		Assert.Equal(50, await task.Within());
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task AFinishedBlock_ReturnsZero_BeforeTheCommandRuns()
	{
		DbTally tally = Tally(3);
		BufferBlock<ChannelRow> block = NewBlock();
		block.Complete();
		await block.Completion.Within();

		Assert.Equal(
			0,
			await Read(tally).ToTargetBlockHoldingConnection(block, Short, Ct));
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static async Task AStalledTarget_TripsTheTimeout_AndClosesTheConnection()
	{
		DbTally tally = Tally(50);
		BufferBlock<ChannelRow> block = NewBounded(1);

		TimeoutException error = await Assert.ThrowsAsync<TimeoutException>(
			async () => await Read(tally)
				.ToTargetBlockHoldingConnection(block, Short, Ct)
				.AsTask()
				.Within());

		Assert.Contains(Short.ToString(), error.Message);
		await ChannelWait.Until(
			() => tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
		Assert.Equal(1, tally.Readers[0].Disposes);
	}

	[Fact]
	public static async Task AStall_UnderComplete_FaultsTheBlockWithTheTimeout()
	{
		DbTally tally = Tally(50);
		BufferBlock<ChannelRow> block = NewBounded(1);

		await Assert.ThrowsAsync<TimeoutException>(
			async () => await Read(tally)
				.ToTargetBlockHoldingConnection(block, Short, complete: true, Ct)
				.AsTask()
				.Within());

		(_, Exception? error) = await Collect(block);
		TimeoutException timeout = Assert.IsType<TimeoutException>(error);
		// The stall's own exception, not the harness's 30 second limit.
		Assert.Contains(Short.ToString(), timeout.Message);
	}

	[Fact]
	public static async Task AStall_UnderKeepOpen_LeavesTheBlockOpen()
	{
		DbTally tally = Tally(50);
		BufferBlock<ChannelRow> block = NewBounded(1);

		await Assert.ThrowsAsync<TimeoutException>(
			async () => await Read(tally)
				.ToTargetBlockHoldingConnection(block, Short, Ct)
				.AsTask()
				.Within());

		Assert.False(block.Completion.IsCompleted);
		_ = await block.ReceiveAsync(Ct).Within();
		Assert.True(await block.SendAsync(new(99), Ct).Within());
	}

	[Fact]
	public static async Task AMovingConsumer_DoesNotTripTheTimeout()
	{
		DbTally tally = Tally(30);
		BufferBlock<ChannelRow> block = NewBounded(1);

		Task<long> task = Read(tally)
			.ToTargetBlockHoldingConnection(
				block, TimeSpan.FromMilliseconds(4000), Ct)
			.AsTask();

		List<int> rows = [];
		while (rows.Count < 30)
		{
			await Task.Delay(150, Ct);
			rows.Add((await ChannelWait.ReceiveRow(block, task)).Id);
		}

		Assert.Equal(ChannelWait.Ids(30), rows);
		Assert.Equal(30, await task.Within());
	}

	[Theory]
	[InlineData(-1)]
	[InlineData(3650 * 24 * 3600)]
	public static async Task AnInfiniteTimeout_OrAHugeOne_WaitsForTheConsumer(
		int seconds)
	{
		// -1 stands for Timeout.InfiniteTimeSpan; the other is 10 years.
		TimeSpan stall = seconds < 0
			? Timeout.InfiniteTimeSpan
			: TimeSpan.FromSeconds(seconds);
		DbTally tally = Tally(10);
		BufferBlock<ChannelRow> block = NewBounded(1);

		Task<long> task = Read(tally)
			.ToTargetBlockHoldingConnection(block, stall, Ct)
			.AsTask();

		await Task.Delay(500, Ct);
		Assert.False(task.IsCompleted);

		Assert.Equal(ChannelWait.Ids(10), await Receive(block, 10));
		Assert.Equal(10, await task.Within());
	}

	[Fact]
	public static async Task ACallerCancel_WhileWaiting_IsACancel_NotATimeout()
	{
		DbTally tally = Tally(50);
		BufferBlock<ChannelRow> block = NewBounded(1);
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);

		Task<long> task = Read(tally)
			.ToTargetBlockHoldingConnection(block, Long, cts.Token)
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
		BufferBlock<ChannelRow> block = NewBounded(2);

		Task<long> task = Read(tally)
			.ToTargetBlockHoldingConnection(block, Long, Ct)
			.AsTask();
		await ChannelWait.Until(
			() => block.Count == 2 && tally.Readers.Count == 1,
			"the block to fill");

		block.Complete();

		Assert.InRange(await task.Within(), 2, 99_999);
		await ChannelWait.Until(
			() => tally.Connections[0].ClosedAfterOpen,
			"the connection to close");
	}

	[Fact]
	public static async Task AFailure_ComesBackThroughTheTask()
	{
		DbTally tally = Tally(20, failOnRead: 6);
		BufferBlock<ChannelRow> block = NewBlock();

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Read(tally)
				.ToTargetBlockHoldingConnection(block, Long, Ct));

		Assert.False(block.Completion.IsCompleted);
	}

	[Fact]
	public static void ATokenAlreadyCanceled_ThrowsAtTheCall()
	{
		DbTally tally = Tally(5);
		using CancellationTokenSource cts = new();
		cts.Cancel();
		FakeSource source = new(Rows(3));
		BufferBlock<ChannelRow> block = NewBlock();

		Assert.Throws<OperationCanceledException>(
			() => Drop(Read(tally).ToTargetBlockHoldingConnection(
				block, Long, complete: true, cts.Token)));
		Assert.Throws<OperationCanceledException>(
			() => Drop(source.ToTargetBlockHoldingConnection(
				block, Long, cts.Token)));
		Assert.Empty(tally.Connections);
		Assert.Null(source.StallTimeout);
		Assert.False(block.Completion.IsCompleted);
	}

	[Theory]
	[InlineData(0)]
	[InlineData(-1000)]
	[InlineData(-2)]
	public static void ABadTimeout_ThrowsAtTheCall_BeforeTheTargetIsTouched(
		int milliseconds)
	{
		DbTally tally = Tally(5);
		BufferBlock<ChannelRow> block = NewBlock();

		Assert.Throws<ArgumentOutOfRangeException>(
			() => Drop(Read(tally).ToTargetBlockHoldingConnection(
				block,
				TimeSpan.FromMilliseconds(milliseconds),
				complete: true,
				Ct)));
		Assert.False(block.Completion.IsCompleted);
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static void TheGuards_ThrowAtTheCall()
	{
		MappedQuery<ChannelRow> query = Read(Tally(1));
		IMappedQuery<ChannelRow> none = null;

		Assert.Throws<ArgumentNullException>(
			() => Drop(none.ToTargetBlockHoldingConnection(NewBlock(), Long, Ct)));
		Assert.Throws<ArgumentNullException>(
			() => Drop(query.ToTargetBlockHoldingConnection(null, Long, Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(default(MappedQuery<ChannelRow>)
				.ToTargetBlockHoldingConnection(NewBlock(), Long, Ct)));
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

		await source.ToTargetBlockHoldingConnection(NewBlock(), stall, cts.Token);

		Assert.Equal(stall, source.StallTimeout);
		Assert.Equal(cts.Token, source.StreamToken);
	}

	// While the block is full the loop is not pulling, so it learns of an
	// expiry only through the stream's Expired token, and fails with the
	// stream's own exception, without waiting for room.
	[Fact]
	public static async Task AnExpiry_WhileTheTargetIsFull_FailsThePassPromptly()
	{
		SignalStream stream = new(Rows(10));
		FakeSource source = new(Rows(10), (_, _) => stream);
		BufferBlock<ChannelRow> block = NewBounded(1);

		Task<long> task = source
			.ToTargetBlockHoldingConnection(block, Long, complete: true, Ct)
			.AsTask();
		await ChannelWait.Until(
			() => stream.Pulled >= 2,
			"the loop to be waiting for room");

		// Nobody ever receives from the block.
		stream.Expire();

		TimeoutException error = await Assert.ThrowsAsync<TimeoutException>(
			() => task.Within());
		Assert.Same(stream.Exception, error);
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
