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

	sealed class SpyTarget<T>(ITargetBlock<T> inner, Action<string>? log = null) : ITargetBlock<T>
	{
		readonly object _sync = new();
		int _accepted;

		public int Accepted => Volatile.Read(ref _accepted);
		public int? AcceptedAtComplete { get; private set; }
		public int? AcceptedAtFault { get; private set; }
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

			log?.Invoke("target-completed");
			inner.Complete();
		}

		public void Fault(Exception exception)
		{
			lock (_sync) AcceptedAtFault ??= Accepted;
			log?.Invoke("target-faulted");
			inner.Fault(exception);
		}

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

		public Harness(TargetKind kind, CancellationToken cancellationToken, Action<string>? log = null)
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
				Target = new SpyTarget<T>(block, log);
				Drained = block.Completion;
			}
			else
			{
				var block = new BufferBlock<T>(new DataflowBlockOptions { BoundedCapacity = TargetCapacity });
				Target = new SpyTarget<T>(block, log);
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

	static async Task<Outcome> RunAsync<T>(string label, TargetKind targetKind, Func<ITargetBlock<T>, ValueTask<long>> call, Action<string>? log = null)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<T>(targetKind, ct, log);

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

	// An IExecuteReader whose readers come from a factory (which may throw) and which logs when the reader was disposed.
	sealed class ScriptedCommand(Func<IDataReader> open, Action<string>? log) : IExecuteReader
	{
		public CancellationToken CancellationToken => default;

		public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			IDataReader reader = open();
			try { handler(reader); }
			finally { reader.Dispose(); log?.Invoke("reader-disposed"); }
		}

		public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			IDataReader reader = open();
			try { return transform(reader); }
			finally { reader.Dispose(); log?.Invoke("reader-disposed"); }
		}

		public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			IDataReader reader = open();
			try { await handler(reader).ConfigureAwait(false); }
			finally { reader.Dispose(); log?.Invoke("reader-disposed"); }
		}

		public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			IDataReader reader = open();
			try { return await transform(reader).ConfigureAwait(false); }
			finally { reader.Dispose(); log?.Invoke("reader-disposed"); }
		}
	}

	static Func<ITargetBlock<Row>, bool, ValueTask<long>> Mapped(
		Kind kind, bool overrides, object?[][] rows,
		Exception? failure = null, Action<string>? log = null,
		IEnumerable<(string Field, string? Column)>? overrideValues = null)
	{
		IEnumerable<(string Field, string? Column)> ov = overrideValues ?? Overrides;
		switch (kind)
		{
			case Kind.DbCommandInterfaceSync:
			{
				var command = Substitute.For<IDbCommand>();
				var connection = Substitute.For<IDbConnection>();
				command.Connection.Returns(connection);
				connection.State.Returns(ConnectionState.Open);
				command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ =>
				{
					if (failure is not null) throw failure;
					IDataReader reader = FakeReader.Create(Columns, rows);
					reader.When(r => r.Dispose()).Do(x => log?.Invoke("reader-disposed"));
					return reader;
				});
				return (t, c) => ValueTask.FromResult(overrides ? command.ToTargetBlock(t, c, ov) : command.ToTargetBlock(t, c));
			}

			case Kind.DbCommandAsync:
			{
				var connection = new FakeDbConnection();
				connection.SetState(ConnectionState.Open);
				var command = new FakeDbCommand(connection, () => failure is null ? FakeReader.CreateDb(Columns, rows) : throw failure);
				return (t, c) => overrides ? command.ToTargetBlockAsync(t, c, ov) : command.ToTargetBlockAsync(t, c);
			}

			case Kind.ExecuteReaderSync:
			{
				IExecuteReader command = new ScriptedCommand(() => failure is null ? FakeReader.CreateDb(Columns, rows) : throw failure, log);
				return (t, c) => ValueTask.FromResult(overrides ? command.ToTargetBlock(t, c, ov) : command.ToTargetBlock(t, c));
			}

			default:
			{
				IExecuteReader command = new ScriptedCommand(() => failure is null ? FakeReader.CreateDb(Columns, rows) : throw failure, log);
				return (t, c) => overrides ? command.ToTargetBlockAsync(t, c, ov) : command.ToTargetBlockAsync(t, c);
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

	// ---- how the completion signal behaves: failures, preconditions, complete: false, ordering against the reader ----

	public static TheoryData<Kind, bool, TargetKind> ShapesWithTarget { get; } = BuildShapesWithTarget();

	static TheoryData<Kind, bool, TargetKind> BuildShapesWithTarget()
	{
		var data = new TheoryData<Kind, bool, TargetKind>();
		foreach (Kind kind in Enum.GetValues<Kind>())
			foreach (bool overrides in new[] { false, true })
				foreach (TargetKind target in Enum.GetValues<TargetKind>())
					data.Add(kind, overrides, target);
		return data;
	}

	public static TheoryData<Kind, bool> Shapes { get; } = BuildShapes();

	static TheoryData<Kind, bool> BuildShapes()
	{
		var data = new TheoryData<Kind, bool>();
		foreach (Kind kind in Enum.GetValues<Kind>())
			foreach (bool overrides in new[] { false, true })
				data.Add(kind, overrides);
		return data;
	}

	public enum Precondition
	{
		NullDbCommand,
		NullDbCommandAsync,
		NullExecuteReader,
		NullExecuteReaderAsync,
		DbCommandWithoutConnectionAsync,
	}

	public static TheoryData<Precondition, bool> PreconditionShapes { get; } = BuildPreconditionShapes();

	static TheoryData<Precondition, bool> BuildPreconditionShapes()
	{
		var data = new TheoryData<Precondition, bool>();
		foreach (Precondition p in Enum.GetValues<Precondition>())
			foreach (bool overrides in new[] { false, true })
				data.Add(p, overrides);
		return data;
	}

	static Func<ITargetBlock<Row>, ValueTask<long>> PreconditionCall(Precondition precondition, bool overrides)
	{
		switch (precondition)
		{
			case Precondition.NullDbCommand:
				return t => ValueTask.FromResult(overrides
					? DataflowExtensions.ToTargetBlock<Row>((IDbCommand)null!, t, true, Overrides)
					: DataflowExtensions.ToTargetBlock<Row>((IDbCommand)null!, t, true));

			case Precondition.NullDbCommandAsync:
				return t => overrides
					? DataflowExtensions.ToTargetBlockAsync<Row>((IDbCommand)null!, t, true, Overrides)
					: DataflowExtensions.ToTargetBlockAsync<Row>((IDbCommand)null!, t, true);

			case Precondition.NullExecuteReader:
				return t => ValueTask.FromResult(overrides
					? DataflowExtensions.ToTargetBlock<Row>((IExecuteReader)null!, t, true, Overrides)
					: DataflowExtensions.ToTargetBlock<Row>((IExecuteReader)null!, t, true));

			case Precondition.NullExecuteReaderAsync:
				return t => overrides
					? DataflowExtensions.ToTargetBlockAsync<Row>((IExecuteReader)null!, t, true, Overrides)
					: DataflowExtensions.ToTargetBlockAsync<Row>((IExecuteReader)null!, t, true);

			default:
			{
				var command = Substitute.For<IDbCommand>();
				command.Connection.Returns((IDbConnection)null!);
				return t => overrides
					? command.ToTargetBlockAsync(t, true, Overrides)
					: command.ToTargetBlockAsync(t, true);
			}
		}
	}

	static void AssertUntouched<T>(SpyTarget<T> target, string because)
		=> Assert.Multiple(
			() => Assert.False(target.Completion.IsCompleted, because),
			() => Assert.Null(target.AcceptedAtComplete),
			() => Assert.Null(target.AcceptedAtFault));

	// The command fails before any reader exists.  Whatever the pipeline has or hasn't built by then,
	// complete: true means the consumer must see the failure (not hang), carrying the original exception.
	[Theory]
	[MemberData(nameof(ShapesWithTarget))]
	public static async Task Mapped_ExecutionFails_CompleteTrue_FaultsTheTarget(Kind kind, bool overrides, TargetKind target)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var failure = new InvalidOperationException("boom");
		var harness = new Harness<Row>(target, ct);
		var call = Mapped(kind, overrides, Rows(3), failure);

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await call(harness.Target, true));
		var faulted = await Assert.ThrowsAsync<InvalidOperationException>(async () => await harness.Target.Completion.WaitAsync(Timeout, ct));

		Assert.Multiple(
			() => Assert.Same(failure, thrown),
			() => Assert.Same(failure, faulted),
			() => Assert.Equal(0, harness.Target.AcceptedAtFault),
			() => Assert.Null(harness.Target.AcceptedAtComplete));
	}

	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_ExecutionFails_CompleteFalse_LeavesTheTargetOpen(Kind kind, bool overrides)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var failure = new InvalidOperationException("boom");
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct);
		var call = Mapped(kind, overrides, Rows(3), failure);

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await call(harness.Target, false));

		Assert.Same(failure, thrown);
		AssertUntouched(harness.Target, "complete: false must not complete or fault the target, even on failure");
		Assert.True(DataflowBlock.Post(harness.Target, new Row()), "the target must still accept rows");
	}

	// The reader fails after 500 good rows (a row that throws when read).  Every row read before the failure must
	// reach the target first; only then does the target learn of the failure.  Nothing may hang.
	[Theory]
	[MemberData(nameof(ShapesWithTarget))]
	public static async Task Mapped_ReaderFailsMidStream_CompleteTrue_DeliversTheRowsReadThenFaultsTheTarget(Kind kind, bool overrides, TargetKind target)
	{
		const int good = 500;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<Row>(target, ct);
		object?[][] rows = [.. Rows(good), null!];
		var call = Mapped(kind, overrides, rows);

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call(harness.Target, true).AsTask().WaitAsync(Timeout, ct));
		Exception faulted = await Assert.ThrowsAnyAsync<Exception>(async () => await harness.Target.Completion.WaitAsync(Timeout, ct));

		Assert.Multiple(
			() => Assert.Same(thrown, faulted),
			() => Assert.Equal(good, harness.Target.AcceptedAtFault), // every row read before the failure was accepted first
			() => Assert.Null(harness.Target.AcceptedAtComplete));
	}

	// Preconditions are the caller's mistakes, not a failed operation: they throw as they always did and
	// neither complete nor fault the target (and no pipeline is built to leave running).
	[Theory]
	[MemberData(nameof(PreconditionShapes))]
	public static async Task Mapped_PreconditionFailure_DoesNotCompleteOrFaultTheTarget(Precondition precondition, bool overrides)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct);
		var call = PreconditionCall(precondition, overrides);

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call(harness.Target));

		Assert.IsType(precondition == Precondition.DbCommandWithoutConnectionAsync ? typeof(InvalidOperationException) : typeof(ArgumentNullException), thrown);
		AssertUntouched(harness.Target, "a precondition failure must not complete or fault the target");
		Assert.True(DataflowBlock.Post(harness.Target, new Row()), "the target must still accept rows");
	}

	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_NullTarget_Throws(Kind kind, bool overrides)
	{
		var call = Mapped(kind, overrides, Rows(3));
		var thrown = await Assert.ThrowsAsync<ArgumentNullException>(async () => await call(null!, true));
		Assert.Equal("target", thrown.ParamName);
	}

	// complete: false means "leave my target open".  The rows still all arrive (the second stage runs behind the call by design).
	[Theory]
	[MemberData(nameof(ShapesWithTarget))]
	public static async Task Mapped_CompleteFalse_DeliversEveryRow_AndLeavesTheTargetOpen(Kind kind, bool overrides, TargetKind target)
	{
		const int count = 1000;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<Row>(target, ct);
		var call = Mapped(kind, overrides, Rows(count));

		long produced = await Task.Run(async () => await call(harness.Target, false), ct).WaitAsync(Timeout, ct);

		var watch = Stopwatch.StartNew();
		while (harness.Consumed < count && watch.Elapsed < Timeout)
			await Task.Delay(5, ct);

		Assert.Multiple(
			() => Assert.Equal(count, produced),
			() => Assert.Equal(count, harness.Consumed));
		AssertUntouched(harness.Target, "complete: false must leave the target open");
		Assert.True(DataflowBlock.Post(harness.Target, new Row()), "the target must still accept rows");

		harness.Target.Complete();
		await harness.Drained.WaitAsync(Timeout, ct);
	}

	// The target is completed by the pipeline draining, never by the call itself: not before the reader is disposed.
	public static TheoryData<Kind, bool> DisposalShapes { get; } = BuildDisposalShapes();

	static TheoryData<Kind, bool> BuildDisposalShapes()
	{
		var data = new TheoryData<Kind, bool>();
		foreach (Kind kind in new[] { Kind.DbCommandInterfaceSync, Kind.ExecuteReaderSync, Kind.ExecuteReaderAsync })
			foreach (bool overrides in new[] { false, true })
				data.Add(kind, overrides);
		return data;
	}

	[Theory]
	[MemberData(nameof(DisposalShapes))]
	public static async Task Mapped_CompleteTrue_CompletesTheTargetOnlyAfterTheReaderIsDisposed(Kind kind, bool overrides)
	{
		const int count = 1000;
		var log = new System.Collections.Concurrent.ConcurrentQueue<string>();
		var call = Mapped(kind, overrides, Rows(count), log: log.Enqueue);

		Outcome outcome = await RunAsync<Row>($"disposal {kind} overrides={overrides}", TargetKind.ActionBlock, t => call(t, true), log.Enqueue);

		Assert.Multiple(
			() => Assert.Equal(count, outcome.Consumed),
			() => Assert.Equal(["reader-disposed", "target-completed"], log.ToArray()));
	}

	// A field override the model can't honor fails while the mapper is being built.  That is an operation failure
	// (the reader is already open), so with complete: true the target is faulted rather than left waiting.
	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_FieldOverrideCannotBeBound_CompleteTrue_FaultsTheTarget(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct);
		var call = Mapped(kind, true, Rows(3), overrideValues: new (string Field, string? Column)[] { ("NoSuchProperty", "Id") });

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call(harness.Target, true));
		Exception faulted = await Assert.ThrowsAnyAsync<Exception>(async () => await harness.Target.Completion.WaitAsync(Timeout, ct));

		Assert.Same(thrown, faulted);
	}

	public static TheoryData<Kind> Kinds { get; } = [.. Enum.GetValues<Kind>()];
}
