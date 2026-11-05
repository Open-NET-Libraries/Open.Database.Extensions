using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A bounded Dataflow target (capacity 10) behind a slow consumer must receive every row the call reports as read,
// and must not be completed until the last of those rows has been accepted.
//
// The mapped ToTargetBlock<T>/ToTargetBlockAsync<T> overloads read into an internal bounded TransformBlock
// (stage 1) which is LinkTo'd to the caller's target (stage 2). The command-level overloads must not complete the
// TARGET while the TransformBlock still holds rows.
//
// NOTE: Post() declining when a bounded block is full is intended behavior in the synchronous paths, so the
// synchronous 5000-row cases can legitimately read fewer than 5000 rows. Those cases therefore assert against
// what the call REPORTED as produced (not against 5000); the "all 5000" assertion applies only where a decline
// cannot happen (async paths use SendAsync; sync paths with fewer rows than the 1024 transform buffer).
[ExcludeFromCodeCoverage]
public static class ToTargetBlockMappedCompletionTests
{
	const int TargetCapacity = 10;
	const int TransformBuffer = 1024;
	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
	static readonly long ConsumerTicks = Stopwatch.Frequency / 10_000; // ~100 microseconds per item

	static readonly string[] Columns = ["Id"];
	static readonly (string Field, string? Column)[] Overrides = [("Id", "Id")];

	sealed class Row
	{
		public int Id { get; set; }
	}

	public enum Kind
	{
		DbCommandInterfaceSync,
		DbCommandAsync,
		ExecuteReaderSync,
		ExecuteReaderAsync,
	}

	public enum TargetKind
	{
		ActionBlock,
		BufferBlock,
	}

	static bool IsAsync(Kind kind) => kind is Kind.DbCommandAsync or Kind.ExecuteReaderAsync;

	public static TheoryData<Kind, bool, TargetKind, int> MappedShapes { get; } = BuildMappedShapes();

	static TheoryData<Kind, bool, TargetKind, int> BuildMappedShapes()
	{
		var data = new TheoryData<Kind, bool, TargetKind, int>();
		foreach (Kind kind in Enum.GetValues<Kind>())
			foreach (bool overrides in new[] { false, true })
				foreach (TargetKind target in Enum.GetValues<TargetKind>())
					foreach (int count in new[] { 1000, 5000 })
						data.Add(kind, overrides, target, count);
		return data;
	}

	public static TheoryData<Kind, TargetKind> UnmappedShapes { get; } = BuildUnmappedShapes();

	static TheoryData<Kind, TargetKind> BuildUnmappedShapes()
	{
		var data = new TheoryData<Kind, TargetKind>();
		foreach (Kind kind in Enum.GetValues<Kind>())
			foreach (TargetKind target in Enum.GetValues<TargetKind>())
				data.Add(kind, target);
		return data;
	}

	public static TheoryData<TargetKind> Targets { get; } = [.. Enum.GetValues<TargetKind>()];

	static object?[][] Rows(int count)
	{
		var rows = new object?[count][];
		for (int i = 0; i < count; i++) rows[i] = [i];
		return rows;
	}

	static void Burn()
	{
		long end = Stopwatch.GetTimestamp() + ConsumerTicks;
		while (Stopwatch.GetTimestamp() < end) Thread.SpinWait(20);
	}

	// ---- spy: wraps a real block, records how many messages it had accepted when Complete() was called, and by whom ----

	sealed class SpyTarget<T>(ITargetBlock<T> inner) : ITargetBlock<T>
	{
		readonly object _sync = new();
		int _accepted;

		public int Accepted => Volatile.Read(ref _accepted);
		public int? AcceptedAtComplete { get; private set; }
		public string? CompletedBy { get; private set; }

		public Task Completion => inner.Completion;

		public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
		{
			// Hand the inner block a proxy so any later Reserve/Consume of a postponed message names the spy as the target.
			ISourceBlock<T>? proxy = source is null ? null : new SourceProxy(source, this);
			DataflowMessageStatus status = inner.OfferMessage(messageHeader, messageValue, proxy, consumeToAccept);

			// Accepted with consumeToAccept == true was already counted by the proxy's ConsumeMessage.
			if (status == DataflowMessageStatus.Accepted && !consumeToAccept)
				Interlocked.Increment(ref _accepted);

			return status;
		}

		public void Complete()
		{
			lock (_sync)
			{
				if (AcceptedAtComplete is null)
				{
					AcceptedAtComplete = Accepted;
					CompletedBy = string.Join(" <- ", new StackTrace(1, false)
						.GetFrames()
						.Take(8)
						.Select(f => f.GetMethod() is { } m ? $"{m.DeclaringType?.Name}.{m.Name}" : "?"));
				}
			}

			inner.Complete();
		}

		public void Fault(Exception exception) => inner.Fault(exception);

		sealed class SourceProxy(ISourceBlock<T> real, SpyTarget<T> spy) : ISourceBlock<T>
		{
			public Task Completion => real.Completion;
			public void Complete() => real.Complete();
			public void Fault(Exception exception) => real.Fault(exception);

			public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
				=> real.LinkTo(target, linkOptions);

			public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed)
			{
				T? value = real.ConsumeMessage(messageHeader, spy, out messageConsumed);
				if (messageConsumed) Interlocked.Increment(ref spy._accepted);
				return value;
			}

			public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
				=> real.ReserveMessage(messageHeader, spy);

			public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
				=> real.ReleaseReservation(messageHeader, spy);
		}
	}

	// ---- harness: bounded real target (capacity 10) + slow consumer, wrapped in the spy ----

	sealed class Harness<T>
	{
		int _consumed;

		public int Consumed => Volatile.Read(ref _consumed);
		public SpyTarget<T> Target { get; }
		public Task Drained { get; }

		public Harness(TargetKind kind, CancellationToken cancellationToken)
		{
			if (kind == TargetKind.ActionBlock)
			{
				var block = new ActionBlock<T>(
					_ =>
					{
						Burn();
						Interlocked.Increment(ref _consumed);
					},
					new ExecutionDataflowBlockOptions { BoundedCapacity = TargetCapacity });
				Target = new SpyTarget<T>(block);
				Drained = block.Completion;
			}
			else
			{
				var block = new BufferBlock<T>(new DataflowBlockOptions { BoundedCapacity = TargetCapacity });
				Target = new SpyTarget<T>(block);
				Drained = Task.Run(async () =>
				{
					await foreach (T _ in block.ReceiveAllAsync(cancellationToken))
					{
						Burn();
						Interlocked.Increment(ref _consumed);
					}
				}, cancellationToken);
			}
		}
	}

	sealed record Outcome(string Label, long Produced, int Consumed, int? AcceptedAtComplete, string? CompletedBy, bool Drained)
	{
		public override string ToString()
			=> $"{Label}: produced={Produced} consumed={Consumed} acceptedAtComplete={AcceptedAtComplete?.ToString() ?? "never"} drained={Drained} completedBy=[{CompletedBy}]";
	}

	static async Task<Outcome> RunAsync<T>(string label, TargetKind targetKind, Func<ITargetBlock<T>, ValueTask<long>> call)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<T>(targetKind, ct);

		long produced = await Task.Run(async () => await call(harness.Target).ConfigureAwait(false), ct).WaitAsync(Timeout, ct);

		bool drained = true;
		try
		{
			await harness.Drained.WaitAsync(Timeout, ct);
		}
		catch (TimeoutException)
		{
			drained = false;
		}

		var outcome = new Outcome(label, produced, harness.Consumed, harness.Target.AcceptedAtComplete, harness.Target.CompletedBy, drained);
		TestContext.Current.TestOutputHelper?.WriteLine(outcome.ToString());
		return outcome;
	}

	// ---- command builders ----

	static Func<ITargetBlock<Row>, bool, ValueTask<long>> Mapped(Kind kind, bool overrides, object?[][] rows)
	{
		switch (kind)
		{
			case Kind.DbCommandInterfaceSync:
			{
				var command = Substitute.For<IDbCommand>();
				var connection = Substitute.For<IDbConnection>();
				command.Connection.Returns(connection);
				connection.State.Returns(ConnectionState.Open);
				command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ => FakeReader.Create(Columns, rows));
				return (t, c) => ValueTask.FromResult(overrides ? command.ToTargetBlock(t, c, Overrides) : command.ToTargetBlock(t, c));
			}

			case Kind.DbCommandAsync:
			{
				var connection = new FakeDbConnection();
				connection.SetState(ConnectionState.Open);
				var command = new FakeDbCommand(connection, () => FakeReader.CreateDb(Columns, rows));
				return (t, c) => overrides ? command.ToTargetBlockAsync(t, c, Overrides) : command.ToTargetBlockAsync(t, c);
			}

			case Kind.ExecuteReaderSync:
			{
				IExecuteReader command = new FakeExecuteReader(Columns, rows);
				return (t, c) => ValueTask.FromResult(overrides ? command.ToTargetBlock(t, c, Overrides) : command.ToTargetBlock(t, c));
			}

			default:
			{
				IExecuteReader command = new FakeExecuteReader(Columns, rows);
				return (t, c) => overrides ? command.ToTargetBlockAsync(t, c, Overrides) : command.ToTargetBlockAsync(t, c);
			}
		}
	}

	static Func<ITargetBlock<object[]>, bool, ValueTask<long>> Unmapped(Kind kind, object?[][] rows)
	{
		switch (kind)
		{
			case Kind.DbCommandInterfaceSync:
			{
				var command = Substitute.For<IDbCommand>();
				var connection = Substitute.For<IDbConnection>();
				command.Connection.Returns(connection);
				connection.State.Returns(ConnectionState.Open);
				command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ => FakeReader.Create(Columns, rows));
				return (t, c) => ValueTask.FromResult(command.ToTargetBlock(t, c));
			}

			case Kind.DbCommandAsync:
			{
				var connection = new FakeDbConnection();
				connection.SetState(ConnectionState.Open);
				var command = new FakeDbCommand(connection, () => FakeReader.CreateDb(Columns, rows));
				return (t, c) => command.ToTargetBlockAsync(t, c);
			}

			case Kind.ExecuteReaderSync:
			{
				IExecuteReader command = new FakeExecuteReader(Columns, rows);
				return (t, c) => ValueTask.FromResult(command.ToTargetBlock(t, c));
			}

			default:
			{
				IExecuteReader command = new FakeExecuteReader(Columns, rows);
				return (t, c) => command.ToTargetBlockAsync(t, c);
			}
		}
	}

	// Shared assertions.
	// ordering:  nothing the call reported as read may be lost, and the target must be completed only after the last
	//            of those rows was accepted by it.
	// allRows:   where a decline cannot happen, the call must also have read every row.
	static void AssertDelivered(Outcome o, int expectedRows, bool declineIsPossible)
	{
		string message = o.ToString();
		var checks = new List<Action>
		{
			() => Assert.True(o.Drained, "target never completed/drained within the timeout. " + message),
			() => Assert.True(o.Consumed == o.Produced, $"rows lost: the call reported {o.Produced} read but the consumer got {o.Consumed}. " + message),
			() => Assert.True(o.AcceptedAtComplete == o.Produced, $"target was completed after {o.AcceptedAtComplete?.ToString() ?? "no"} rows were accepted, before the last of {o.Produced}. " + message),
		};

		if (!declineIsPossible)
			checks.Add(() => Assert.True(o.Produced == expectedRows && o.Consumed == expectedRows, $"expected all {expectedRows} rows delivered. " + message));

		Assert.Multiple([.. checks]);
	}

	// ---- the mapped paths under test ----

	[Theory]
	[MemberData(nameof(MappedShapes))]
	public static async Task Mapped_CompleteTrue_BoundedSlowTarget_DeliversEveryRow_AndCompletesOnlyAfterTheLast(Kind kind, bool overrides, TargetKind target, int count)
	{
		var call = Mapped(kind, overrides, Rows(count));
		Outcome outcome = await RunAsync<Row>($"mapped {kind} overrides={overrides} target={target} rows={count}", target, t => call(t, true));

		// Post() may legitimately decline once the 1024-row transform buffer fills (sync paths); fewer rows than that cannot.
		bool declineIsPossible = !IsAsync(kind) && count >= TransformBuffer;
		AssertDelivered(outcome, count, declineIsPossible);
	}

	// ---- controls: the non-mapped (object[]) path, same target and consumer ----

	[Theory]
	[MemberData(nameof(UnmappedShapes))]
	public static async Task Unmapped_CompleteTrue_BoundedSlowTarget_DeliversEveryRow_AndCompletesOnlyAfterTheLast(Kind kind, TargetKind target)
	{
		const int count = 5000;
		var call = Unmapped(kind, Rows(count));
		Outcome outcome = await RunAsync<object[]>($"unmapped {kind} target={target} rows={count}", target, t => call(t, true));

		// Sync Post() into a capacity-10 target declines almost at once (intended); async SendAsync waits for room.
		AssertDelivered(outcome, count, declineIsPossible: !IsAsync(kind));
	}

	// ---- controls: the reader-level call with complete: true (completion comes from the TransformBlock) ----

	[Theory]
	[MemberData(nameof(Targets))]
	public static async Task ReaderLevelMapped_CompleteTrue_Sync_BoundedSlowTarget_DeliversEveryRow(TargetKind target)
	{
		const int count = 1000;
		IDataReader reader = FakeReader.Create(Columns, Rows(count));
		Outcome outcome = await RunAsync<Row>($"reader-level sync target={target} rows={count}", target, t => ValueTask.FromResult(reader.ToTargetBlock(t, true)));
		AssertDelivered(outcome, count, declineIsPossible: false);
	}

	[Theory]
	[MemberData(nameof(Targets))]
	public static async Task ReaderLevelMapped_CompleteTrue_Async_BoundedSlowTarget_DeliversEveryRow(TargetKind target)
	{
		const int count = 5000;
		var reader = FakeReader.CreateDb(Columns, Rows(count));
		Outcome outcome = await RunAsync<Row>($"reader-level async target={target} rows={count}", target, t => reader.ToTargetBlockAsync(t, true));
		AssertDelivered(outcome, count, declineIsPossible: false);
	}
}
