using System.Threading.Tasks.Dataflow;
using static Open.Database.Extensions.Tests.BlockFixtures;
using static Open.Database.Extensions.Tests.ChannelFixtures;

namespace Open.Database.Extensions.Tests;

// ToTargetBlock(target [, end]): the caller owns the block, the package
// writes only (SendAsync, never Post), the block stays open unless asked,
// the task is the done signal. Rows come from Core's buffered stream.
[ExcludeFromCodeCoverage]
public static class ToTargetBlockTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public static TheoryData<string> AllViews => ViewNames;

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task KeepOpen_IsTheDefault_AndPassesShareOneBlock(
		string view)
	{
		DbTally tally = Tally(5);
		BufferBlock<ChannelRow> block = NewBlock();

		long first = await View(view, tally).ToTargetBlock(block, Ct);
		long second = await View(view, tally).ToTargetBlock(block, Ct);

		Assert.Equal(5, first);
		Assert.Equal(5, second);
		Assert.False(block.Completion.IsCompleted);
		Assert.True(block.Post(new(99)));
		Assert.Equal(
			[1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 99],
			await Receive(block, 11));
	}

	[Theory]
	[MemberData(nameof(AllViews))]
	public static async Task Complete_CompletesTheBlock_AfterTheLastRow(
		string view)
	{
		DbTally tally = Tally(8);
		BufferBlock<ChannelRow> block = NewBlock();

		long count = await View(view, tally)
			.ToTargetBlock(block, complete: true, Ct);

		Assert.Equal(8, count);
		Assert.True(IsClosed(block));
		(List<int> rows, Exception? error) = await Collect(block);
		Assert.Null(error);
		Assert.Equal(ChannelWait.Ids(8), rows);
	}

	[Fact]
	public static async Task TheTask_CompletesAfterTheConnectionIsClosed()
	{
		DbTally tally = Tally(30);
		BufferBlock<ChannelRow> block = NewBlock();

		await Read(tally).ToTargetBlock(block, complete: true, Ct);

		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
		Assert.True(IsClosed(block));
	}

	[Fact]
	public static async Task AFullBoundedBlock_WaitsForRoom_AndDropsNothing()
	{
		DbTally tally = Tally(40);
		BufferBlock<ChannelRow> block = NewBounded(1);

		Task<long> task = Read(tally).ToTargetBlock(block, Ct).AsTask();

		Assert.Equal(ChannelWait.Ids(40), await Receive(block, 40));
		Assert.Equal(40, await task.Within());
	}

	[Fact]
	public static async Task ATargetAlreadyCompleted_ReturnsZero_AndRunsNoCommand()
	{
		DbTally tally = Tally(10);
		BufferBlock<ChannelRow> block = NewBlock();
		block.Complete();
		await block.Completion.Within();

		long count = await Read(tally).ToTargetBlock(block, Ct);

		Assert.Equal(0, count);
		Assert.Empty(tally.Connections);
		Assert.Empty(tally.Commands);
	}

	[Fact]
	public static async Task AFaultedTarget_ReturnsZero_AndRunsNoCommand()
	{
		DbTally tally = Tally(10);
		BufferBlock<ChannelRow> block = NewBlock();
		Fault(block, new InvalidOperationException("faulted"));
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => block.Completion);

		long count = await Read(tally).ToTargetBlock(block, Ct);

		Assert.Equal(0, count);
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static async Task ATargetClosedMidPass_StopsThePass_WithoutAnException()
	{
		DbTally tally = Tally(100_000);
		BufferBlock<ChannelRow> block = NewBounded(2);

		Task<long> task = Read(tally).ToTargetBlock(block, Ct).AsTask();
		await ChannelWait.Until(
			() => block.Count == 2 && tally.Readers.Count == 1,
			"the block to fill");

		block.Complete();

		Assert.InRange(await task.Within(), 2, 99_999);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task ACancel_UnderKeepOpen_FaultsTheTask_AndLeavesTheBlockAlone()
	{
		using CancellationTokenSource cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		DbTally tally = Tally(
			100_000,
			onRead: n => { if (n == 50) cts.Cancel(); });
		BufferBlock<ChannelRow> block = NewBlock();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Read(tally).ToTargetBlock(block, cts.Token));

		Assert.False(block.Completion.IsCompleted);
		Assert.True(block.Post(new(-1)));
		await ChannelWait.Until(
			() => tally.Connections[0].State == ConnectionState.Closed,
			"the connection to close");
	}

	[Fact]
	public static async Task ACancel_UnderComplete_FaultsTheBlockWithIt()
	{
		using CancellationTokenSource cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		DbTally tally = Tally(
			100_000,
			onRead: n => { if (n == 50) cts.Cancel(); });
		BufferBlock<ChannelRow> block = NewBlock();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Read(tally)
				.ToTargetBlock(block, complete: true, cts.Token));

		(_, Exception? error) = await Collect(block);
		Assert.IsType<OperationCanceledException>(error, exactMatch: false);
	}

	[Fact]
	public static async Task AFailure_UnderKeepOpen_ComesBackThroughTheTask_BlockOpen()
	{
		DbTally tally = Tally(20, failOnRead: 8);
		BufferBlock<ChannelRow> block = NewBlock();

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Read(tally).ToTargetBlock(block, Ct));

		Assert.False(block.Completion.IsCompleted);
		Assert.True(block.Post(new(-1)));
	}

	[Fact]
	public static async Task AFailure_UnderComplete_AlsoFaultsTheBlockWithIt()
	{
		DbTally tally = Tally(20, failOnRead: 8);
		BufferBlock<ChannelRow> block = NewBlock();

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Read(tally)
				.ToTargetBlock(block, complete: true, Ct));

		(_, Exception? error) = await Collect(block);
		Assert.IsType<InvalidOperationException>(error);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void ATokenAlreadyCanceled_ThrowsAtTheCall_AndTouchesNothing(
		bool complete)
	{
		DbTally tally = Tally(5);
		BufferBlock<ChannelRow> block = NewBlock();
		using CancellationTokenSource cts = new();
		cts.Cancel();
		FakeSource source = new(Rows(3));

		Assert.Throws<OperationCanceledException>(
			() => Drop(Read(tally).ToTargetBlock(block, complete, cts.Token)));
		Assert.Throws<OperationCanceledException>(
			() => Drop(source.ToTargetBlock(block, complete, cts.Token)));

		Assert.False(block.Completion.IsCompleted);
		Assert.Empty(tally.Connections);
		Assert.Equal(0, source.ChunkStreams);
	}

	[Fact]
	public static void TheGuards_ThrowAtTheCall()
	{
		DbTally tally = Tally(1);
		MappedQuery<ChannelRow> query = Read(tally);
		BufferBlock<ChannelRow> block = NewBlock();
		IMappedQuery<ChannelRow> none = null;

		Assert.Throws<ArgumentNullException>(
			() => Drop(none.ToTargetBlock(block, Ct)));
		Assert.Throws<ArgumentNullException>(
			() => Drop(query.ToTargetBlock(null, Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(default(MappedQuery<ChannelRow>)
				.ToTargetBlock(block, Ct)));
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static async Task AUsersOwnSource_Works()
	{
		FakeSource source = new(Rows(10));
		BufferBlock<ChannelRow> block = NewBlock();

		Assert.Equal(10, await source.ToTargetBlock(block, complete: true, Ct));

		(List<int> rows, _) = await Collect(block);
		Assert.Equal(ChannelWait.Ids(10), rows);
	}

	[Fact]
	public static async Task ManyRows_ThroughAOneSlotBlock_ArriveOnce_InOrder()
	{
		DbTally tally = Tally(20_000);
		BufferBlock<ChannelRow> block = NewBounded(1);

		Task<long> task = Read(tally).ToTargetBlock(block, Ct).AsTask();

		Assert.Equal(ChannelWait.Ids(20_000), await Receive(block, 20_000));
		Assert.Equal(20_000, await task.Within());
	}

	[Fact]
	public static async Task ConcurrentPasses_IntoOneSharedBlock_SendEveryRowOnce()
	{
		const int Passes = 16;
		const int Rows = 500;
		BufferBlock<ChannelRow> block = NewBlock();

		long[] counts = await Task.WhenAll(
			Enumerable.Range(0, Passes).Select(
				_ => Read(Tally(Rows)).ToTargetBlock(block, Ct).AsTask()))
			.Within();

		Assert.All(counts, static c => Assert.Equal(Rows, c));
		List<int> ids = await Receive(block, Passes * Rows);
		Assert.All(
			ids.GroupBy(static id => id),
			static g => Assert.Equal(Passes, g.Count()));
		Assert.False(block.Completion.IsCompleted);
	}

	public static TheoryData<string> Starts
		=> ["buffered", "bufferedFullFirst", "streamingFullFirst"];

	// Every await of the package must be ConfigureAwait(false): one that
	// is not posts its continuation back to the context it started under.
	// The context is current only while the pass starts, so this sees
	// the first suspension of each path.
	[Theory]
	[MemberData(nameof(Starts))]
	public static async Task TheCalls_NeverPostBackToTheCallersContext(
		string start)
	{
		CountingContext context = new();
		SynchronizationContext? previous = SynchronizationContext.Current;
		DbTally tally = Tally(200);
		BufferBlock<ChannelRow> block = NewBounded(1);
		bool full = start != "buffered";
		if (full) Assert.True(block.Post(new(0)));

		Task<long> task;
		SynchronizationContext.SetSynchronizationContext(context);
		try
		{
			task = start == "streamingFullFirst"
				? Read(tally)
					.ToTargetBlockHoldingConnection(
						block,
						TimeSpan.FromSeconds(30),
						complete: true,
						Ct)
					.AsTask()
				: Read(tally)
					.ToTargetBlock(block, complete: true, Ct)
					.AsTask();
		}
		finally
		{
			SynchronizationContext.SetSynchronizationContext(previous);
		}

		if (full) _ = await block.ReceiveAsync(Ct);
		Assert.Equal(200, (await Receive(block, 200)).Count);
		Assert.Equal(200, await task.Within());
		Assert.Equal(0, context.Posts);
	}

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
