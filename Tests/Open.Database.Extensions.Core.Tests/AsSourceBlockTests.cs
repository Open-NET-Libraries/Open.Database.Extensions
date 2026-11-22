using System.Threading.Tasks.Dataflow;
using static Open.Database.Extensions.Tests.BlockFixtures;
using static Open.Database.Extensions.Tests.ChannelFixtures;

namespace Open.Database.Extensions.Tests;

// AsSourceBlock: the package makes the block
// (started at the call, always finished) over Core's buffered stream.
[ExcludeFromCodeCoverage]
public static class AsSourceBlockTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public static TheoryData<string> AllViews => ViewNames;

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task DeliversEveryRow_InOrder_ThenCompletes(string view)
	{
		DbTally tally = Tally(40);

		IReceivableSourceBlock<ChannelRow> block = View(view, tally)
			.AsSourceBlock(Ct);

		(List<int> rows, Exception? error) = await Collect(block);

		Assert.Null(error);
		Assert.Equal(ChannelWait.Ids(40), rows);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task AnEmptyResult_CompletesWithNoRows()
	{
		(List<int> rows, Exception? error) = await Collect(
			Read(Tally(0)).AsSourceBlock(Ct));

		Assert.Null(error);
		Assert.Empty(rows);
	}

	[Fact]
	public static async Task TheRead_StartsAtTheCall_AndTheConnectionClosesWhateverTheConsumerDoes()
	{
		DbTally tally = Tally(500);

		// The consumer never receives a row.
		IReceivableSourceBlock<ChannelRow> block = Read(tally).AsSourceBlock(Ct);

		await ChannelWait.Until(
			() => tally.Connections.Count == 1
				&& tally.Connections[0].State == ConnectionState.Closed,
			"the connection to close");
		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(ChannelWait.Ids(500), await Receive(block, 500));
	}

	[Fact]
	public static async Task ARowFailure_FaultsTheBlockWithIt()
	{
		DbTally tally = Tally(20, failOnRead: 9);

		IReceivableSourceBlock<ChannelRow> block = Read(tally).AsSourceBlock(Ct);

		(_, Exception? error) = await Collect(block);

		Assert.IsType<InvalidOperationException>(error);
		Assert.Contains("failed while reading", error.Message);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task ACancel_FaultsTheBlockWithIt_NotACleanEnd()
	{
		using CancellationTokenSource cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		DbTally tally = Tally(
			100_000,
			onRead: n => { if (n == 50) cts.Cancel(); });

		IReceivableSourceBlock<ChannelRow> block = Read(tally)
			.AsSourceBlock(cts.Token);

		(_, Exception? error) = await Collect(block);

		Assert.IsType<OperationCanceledException>(error, exactMatch: false);
		await ChannelWait.Until(
			() => tally.Connections[0].State == ConnectionState.Closed,
			"the connection to close");
	}

	[Fact]
	public static void ATokenAlreadyCanceled_ThrowsAtTheCall_AndStartsNothing()
	{
		DbTally tally = Tally(5);
		using CancellationTokenSource cts = new();
		cts.Cancel();
		MappedQuery<ChannelRow> query = Read(tally);
		FakeSource source = new(Rows(3));

		Assert.Throws<OperationCanceledException>(
			() => query.AsSourceBlock(cts.Token));
		Assert.Throws<OperationCanceledException>(
			() => source.AsSourceBlock(new DataflowBlockOptions(), cts.Token));
		Assert.Empty(tally.Connections);
		Assert.Equal(0, source.ChunkStreams);
	}

	[Fact]
	public static void TheGuards_ThrowAtTheCall()
	{
		IMappedQuery<ChannelRow> none = null!;
		IMappedQuery<ChannelRow> view = default(MappedQuery<ChannelRow>);

		Assert.Throws<ArgumentNullException>(
			() => none.AsSourceBlock(Ct));
		Assert.Throws<ArgumentNullException>(
			() => none.AsSourceBlock(new DataflowBlockOptions(), Ct));
		Assert.Throws<ArgumentNullException>(
			() => Read(Tally(1)).AsSourceBlock(null!, Ct));
		Assert.Throws<InvalidOperationException>(
			() => view.AsSourceBlock(Ct));
	}

	[Fact]
	public static async Task SkipAndTake_Apply()
	{
		(List<int> rows, _) = await Collect(
			Read(Tally(10)).Skip(2).Take(3).AsSourceBlock(Ct));

		Assert.Equal([3, 4, 5], rows);
	}

	[Fact]
	public static async Task AsyncReads_Deliver()
	{
		(List<int> rows, _) = await Collect(
			Read(Tally(6)).AsyncReads().AsSourceBlock(Ct));

		Assert.Equal(ChannelWait.Ids(6), rows);
	}

	[Fact]
	public static async Task AUsersOwnSource_Works()
	{
		FakeSource source = new(Rows(9));

		(List<int> rows, _) = await Collect(source.AsSourceBlock(Ct));

		Assert.Equal(ChannelWait.Ids(9), rows);
		Assert.Equal(1, source.ChunkStreams);
	}

	[Fact]
	public static async Task Bounded_DeliversEveryRow_ToAConsumerThatWaits()
	{
		DbTally tally = Tally(60);

		IReceivableSourceBlock<ChannelRow> block = Read(tally)
			.AsSourceBlock(Options(2), Ct);

		Assert.Equal(ChannelWait.Ids(60), await Receive(block, 60));
		await block.Completion.Within();
	}

	[Fact]
	public static async Task Bounded_DoesNotHoldTheConnection()
	{
		DbTally tally = Tally(300);

		// Capacity 1 and nobody receives: the read still finishes and
		// the connection closes; the rows wait in the internal queue.
		IReceivableSourceBlock<ChannelRow> block = Read(tally)
			.AsSourceBlock(Options(1), Ct);

		await ChannelWait.Until(
			() => tally.Connections.Count == 1
				&& tally.Connections[0].State == ConnectionState.Closed,
			"the connection to close");
		Assert.Equal(ChannelWait.Ids(300), await Receive(block, 300));
	}

	// The options reach the block: its scheduler runs its tasks, and its
	// bound holds the rows while nobody receives.
	[Fact]
	public static async Task TheOptions_ReachTheBlock()
	{
		CountingScheduler scheduler = new();
		DbTally tally = Tally(30);

		IReceivableSourceBlock<ChannelRow> block = Read(tally).AsSourceBlock(
			new DataflowBlockOptions { TaskScheduler = scheduler, BoundedCapacity = 4 },
			Ct);

		// The read finishes (the connection is free) while the block holds four.
		await ChannelWait.Until(
			() => tally.Connections.Count == 1
				&& tally.Connections[0].State == ConnectionState.Closed,
			"the connection to close");
		await ChannelWait.Until(
			() => ((BufferBlock<ChannelRow>)block).Count == 4,
			"the block to hold its bound");
		await Task.Delay(200, Ct);
		Assert.Equal(4, ((BufferBlock<ChannelRow>)block).Count);

		Assert.Equal(ChannelWait.Ids(30), await Receive(block, 30));
		Assert.True(scheduler.Queued > 0);
	}

	sealed class CountingScheduler : TaskScheduler
	{
		int _queued;
		public int Queued => Volatile.Read(ref _queued);

		protected override void QueueTask(Task task)
		{
			Interlocked.Increment(ref _queued);
			ThreadPool.UnsafeQueueUserWorkItem(
				_ => TryExecuteTask(task),
				null);
		}

		protected override bool TryExecuteTaskInline(
			Task task,
			bool taskWasPreviouslyQueued)
			=> false;

		protected override IEnumerable<Task> GetScheduledTasks()
			=> [];
	}

	static DataflowBlockOptions Options(int capacity)
		=> new() { BoundedCapacity = capacity };
}
