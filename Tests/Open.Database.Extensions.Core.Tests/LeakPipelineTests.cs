using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The Channel and Dataflow pipelines run a reader on behalf of the caller, often in the background, so they are the place where a reader, a
// command and a connection can be held for as long as somebody forgets about them. For both sinks (a ChannelWriter and a dataflow block), both
// shapes (raw rows straight to the target, and rows mapped to a model through the two-stage pipeline) and three connection sources:
//   - when the producer has written every row, it ends and everything is released;
//   - when the reader fails, the producer fails with it and everything is released;
//   - when the target is full and the call is cancelled, the producer ends with the cancellation and everything is released;
//   - when the consumer simply walks away from a full target and nothing is cancelled, the producer waits (and holds the reader) until the
//     caller completes the target, which ends it and releases everything. That wait is the one deliberate hold: the reader is not
//     released on its own, because a full bounded target is a consumer that is only slow, not gone. (Asserted below, so a change to it is a decision.)
//   - an unbounded target never makes the producer wait: with nobody reading it, the producer still reads everything and releases everything.
[ExcludeFromCodeCoverage]
public static class LeakPipelineTests
{
	sealed class Row
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	public enum Shape
	{
		/// <summary>Rows as object arrays, written straight to the target.</summary>
		Raw,

		/// <summary>Rows mapped to a model through the two-stage pipeline (an internal buffer of 1024 rows sits between the reader and the target).</summary>
		Mapped,
	}

	public enum Sink
	{
		Channel,
		Block,
	}

	// More rows than the internal buffer of the mapped pipeline, so that with a full target the reader is still open.
	const int Many = 3000;

	static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

	static object?[][] Rows(int count) => [.. Enumerable.Range(0, count).Select(i => new object?[] { i, "n" })];

	public static TheoryData<LeakSource, Shape, Sink> Cases { get; } = BuildCases();

	static TheoryData<LeakSource, Shape, Sink> BuildCases()
	{
		var data = new TheoryData<LeakSource, Shape, Sink>();
		foreach (LeakSource source in new[] { LeakSource.Factory, LeakSource.BareClosed, LeakSource.BareOpen })
		{
			foreach (Shape shape in Enum.GetValues<Shape>())
			{
				foreach (Sink sink in Enum.GetValues<Sink>())
					data.Add(source, shape, sink);
			}
		}

		return data;
	}

	// What a scenario holds on to: the running producer and a way for the caller to close the target.
	sealed record Pipe(Task<long> Producer, Action CloseTarget);

	static Pipe Start(ExpressiveDbCommand command, Shape shape, Sink sink, int capacity)
	{
		switch (shape, sink)
		{
			case (Shape.Raw, Sink.Channel):
				Channel<object[]> rawChannel = Make<object[]>(capacity);
				return new(command.ToChannelAsync(rawChannel.Writer, true).AsTask(), () => rawChannel.Writer.TryComplete());
			case (Shape.Mapped, Sink.Channel):
				Channel<Row> mappedChannel = Make<Row>(capacity);
				return new(command.ToChannelAsync<Row>(mappedChannel.Writer, true).AsTask(), () => mappedChannel.Writer.TryComplete());
			case (Shape.Raw, _):
				var rawBlock = new BufferBlock<object[]>(Options(capacity));
				return new(command.ToTargetBlockAsync(rawBlock, true).AsTask(), () => ((IDataflowBlock)rawBlock).Fault(ConsumerGone()));
			default:
				var mappedBlock = new BufferBlock<Row>(Options(capacity));
				return new(command.ToTargetBlockAsync<Row>(mappedBlock, true).AsTask(), () => ((IDataflowBlock)mappedBlock).Fault(ConsumerGone()));
		}
	}

	// A block ends when it is faulted (a block that is only completed first delivers what it holds, and nobody is receiving).
	static InvalidOperationException ConsumerGone() => new("The consumer is gone.");

	static Channel<T> Make<T>(int capacity)
		=> capacity <= 0
			? System.Threading.Channels.Channel.CreateUnbounded<T>()
			: System.Threading.Channels.Channel.CreateBounded<T>(capacity);

	static DataflowBlockOptions Options(int capacity)
		=> capacity <= 0 ? new() : new() { BoundedCapacity = capacity };

	static async Task Eventually(Func<bool> condition, string what, CancellationToken token)
	{
		DateTime until = DateTime.UtcNow + Bound;
		while (!condition())
		{
			if (DateTime.UtcNow > until) Assert.Fail("Did not happen within " + Bound + ": " + what);
			await Task.Delay(5, token);
		}
	}

	// The producer has read as much as it is going to: its reader has not been read from for a while.
	static async Task WaitUntilBlocked(DbTally tally, CancellationToken token)
	{
		await Eventually(() => tally.Readers.Count == 1 && tally.Readers[0].Reads >= 2, "the producer to start reading", token);
		int last = -1;
		while (tally.Readers[0].Reads != last)
		{
			last = tally.Readers[0].Reads;
			await Task.Delay(100, token);
		}
	}

	static void AssertReleased(DbTally tally, LeakSource source)
	{
		Assert.Single(tally.Commands);
		Assert.Single(tally.Readers);
		LeakAssert.CommandsAndReadersDisposed(tally);
		LeakAssert.ConnectionsReleased(tally, source, tally.Pool, 1);
	}

	static async Task EndsWithin(Task task, CancellationToken token)
	{
		try
		{
			await task.WaitAsync(Bound, token);
		}
		catch (TimeoutException)
		{
			Assert.Fail("The producer did not end within " + Bound + ".");
		}
		catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
		{
			// Ending with an error or a cancellation is an ending: what the scenario asserts is the release.
		}
	}

	[Theory]
	[MemberData(nameof(Cases))]
	public static async Task ProducerReadsEverything_AndEverythingIsReleased(LeakSource source, Shape shape, Sink sink)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		var tally = new DbTally(new() { Rows = Rows(20) });
		Pipe pipe = Start(tally.Command(source, tally.Pool), shape, sink, capacity: 0);

		Assert.Equal(20, await pipe.Producer.WaitAsync(Bound, token));

		AssertReleased(tally, source);
	}

	[Theory]
	[MemberData(nameof(Cases))]
	public static async Task ReaderFails_TheProducerFails_AndEverythingIsReleased(LeakSource source, Shape shape, Sink sink)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		var tally = new DbTally(new() { Rows = Rows(20), FailOnRead = 5 });
		Pipe pipe = Start(tally.Command(source, tally.Pool), shape, sink, capacity: 0);

		await Assert.ThrowsAnyAsync<Exception>(() => pipe.Producer.WaitAsync(Bound, token));

		AssertReleased(tally, source);
	}

	[Theory]
	[MemberData(nameof(Cases))]
	public static async Task TargetFull_Cancelled_ProducerEnds_AndEverythingIsReleased(LeakSource source, Shape shape, Sink sink)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
		var tally = new DbTally(new() { Rows = Rows(Many) });
		Pipe pipe = Start(tally.Command(source, tally.Pool).UseCancellationToken(cts.Token), shape, sink, capacity: 1);

		await WaitUntilBlocked(tally, token);
		Assert.False(pipe.Producer.IsCompleted);

		await cts.CancelAsync();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pipe.Producer.WaitAsync(Bound, token));

		AssertReleased(tally, source);
	}

	[Theory]
	[MemberData(nameof(Cases))]
	public static async Task TargetFull_ConsumerAbandoned_ProducerHoldsTheReader_UntilTheCallerClosesTheTarget(LeakSource source, Shape shape, Sink sink)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		var tally = new DbTally(new() { Rows = Rows(Many) });
		Pipe pipe = Start(tally.Command(source, tally.Pool), shape, sink, capacity: 1);

		// Nobody reads and nothing is cancelled: the producer waits and the reader stays open. This is the deliberate hold.
		await WaitUntilBlocked(tally, token);
		Assert.False(pipe.Producer.IsCompleted);
		Assert.Equal(0, tally.Readers[0].Disposes);

		// Closing the target (a block: faulting it) is the caller's way out, and it is enough: the producer ends and releases everything.
		pipe.CloseTarget();
		await EndsWithin(pipe.Producer, token);

		AssertReleased(tally, source);
	}

	// The overloads that create the target never wait for a consumer, so a consumer that never shows up costs memory (the rows) but holds no reader.
	[Theory]
	[InlineData(LeakSource.Factory, "ToChannel")]
	[InlineData(LeakSource.BareClosed, "ToChannel")]
	[InlineData(LeakSource.BareOpen, "ToChannel")]
	[InlineData(LeakSource.Factory, "ToChannelAsync")]
	[InlineData(LeakSource.BareClosed, "ToChannelAsync")]
	[InlineData(LeakSource.BareOpen, "ToChannelAsync")]
	[InlineData(LeakSource.Factory, "AsSourceBlock")]
	[InlineData(LeakSource.BareClosed, "AsSourceBlock")]
	[InlineData(LeakSource.BareOpen, "AsSourceBlock")]
	[InlineData(LeakSource.Factory, "AsSourceBlockAsync")]
	[InlineData(LeakSource.BareClosed, "AsSourceBlockAsync")]
	[InlineData(LeakSource.BareOpen, "AsSourceBlockAsync")]
	public static async Task CreatedTarget_NobodyEverReads_TheReaderIsStillReleased(LeakSource source, string overload)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		var tally = new DbTally(new() { Rows = Rows(Many) });
		ExpressiveDbCommand command = tally.Command(source, tally.Pool);

		object target = overload switch
		{
			"ToChannel" => command.ToChannel<Row>(true),
			"ToChannelAsync" => command.ToChannelAsync<Row>(true),
			"AsSourceBlock" => command.AsSourceBlock<Row>(),
			_ => command.AsSourceBlockAsync<Row>(),
		};

		await Eventually(() => tally.Readers.Count == 1 && tally.Readers[0].Disposes == 1, "the reader to be disposed", token);
		AssertReleased(tally, source);
		GC.KeepAlive(target);
	}

	// The limit, spelled out: a bounded block that the caller only COMPLETES is not enough to release a mapped pipeline. The block still holds a row that nobody
	// receives, so it never reports that it has completed, and the pipeline (which stops writing when the target has completed) keeps waiting for room and keeps the
	// reader open. Receiving the held row (the block then completes) or cancelling the call are the ways out; faulting a block that was already completed changes nothing. Asserted so that changing it is a decision.
	[Fact]
	public static async Task MappedBlock_CompletedButNotDrained_TheProducerKeepsTheReader_UntilTheHeldRowIsReceived()
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		var tally = new DbTally(new() { Rows = Rows(Many) });
		var block = new BufferBlock<Row>(Options(1));
		Task<long> producer = tally.Command(LeakSource.BareClosed, tally.Pool).ToTargetBlockAsync<Row>(block, true).AsTask();

		await WaitUntilBlocked(tally, token);
		block.Complete();
		await Task.Delay(500, token);

		Assert.False(producer.IsCompleted);
		Assert.Equal(0, tally.Readers[0].Disposes);

		Assert.True(block.TryReceive(out _));
		await EndsWithin(producer, token);
		AssertReleased(tally, LeakSource.BareClosed);
	}
}
