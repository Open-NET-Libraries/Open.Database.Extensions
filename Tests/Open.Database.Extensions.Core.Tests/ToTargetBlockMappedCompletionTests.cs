using System.Collections;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The retention tests measure process-wide memory, so this class runs alone, after the parallel collections.
[CollectionDefinition("Dataflow mapped completion", DisableParallelization = true)]
public sealed class DataflowMappedCompletionCollection;

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
[Collection("Dataflow mapped completion")]
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
				// Count first: the block may complete (and complete the target) the moment the consume returns.
				Interlocked.Increment(ref spy._accepted);
				T? value = real.ConsumeMessage(messageHeader, spy, out messageConsumed);
				if (!messageConsumed) Interlocked.Decrement(ref spy._accepted);
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
		public System.Collections.Concurrent.ConcurrentQueue<T> Received { get; } = new();

		public int Consumed => Volatile.Read(ref _consumed);
		public SpyTarget<T> Target { get; }
		public Task Drained { get; }

		public Harness(TargetKind kind, CancellationToken cancellationToken, Action<string>? log = null, Task? gate = null)
		{
			if (kind == TargetKind.ActionBlock)
			{
				var block = new ActionBlock<T>(
					item =>
					{
						gate?.Wait(); // A parked consumer: nothing is consumed until the gate opens.
						Burn();
						Received.Enqueue(item);
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
					if (gate is not null) await gate.ConfigureAwait(false); // A parked consumer.
					await foreach (T item in block.ReceiveAllAsync(cancellationToken))
					{
						Burn();
						Received.Enqueue(item);
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

	static async ValueTask ReleaseAsync(IDataReader reader, Action<string>? log)
	{
		if (reader is IAsyncDisposable d) await d.DisposeAsync().ConfigureAwait(false);
		else reader.Dispose();
		log?.Invoke("reader-disposed");
	}

	// An IExecuteReader whose readers come from a factory (which may throw) and which logs when the reader was released.
	sealed class ScriptedCommand(Func<IDataReader> open, Action<string>? log, CancellationToken token = default) : IExecuteReader
	{
		public CancellationToken CancellationToken => token;

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
			finally { await ReleaseAsync(reader, log).ConfigureAwait(false); }
		}

		public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			IDataReader reader = open();
			try { return await transform(reader).ConfigureAwait(false); }
			finally { await ReleaseAsync(reader, log).ConfigureAwait(false); }
		}
	}

	// A hand-written reader whose Dispose and DisposeAsync both block until the test lets them finish.
	// "Entered" completes when the owner starts releasing it; "reader-released" is logged when the release finishes.
	sealed class GatedReader(string[] columns, object?[][] rows, Action<string> log, int failReadAt = -1, int stallAt = -1, Func<Exception>? cancelException = null) : DbDataReader
	{
		readonly TaskCompletionSource<bool> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
		readonly TaskCompletionSource<bool> _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
		int _index = -1;
		int _released;

		public Task Entered => _entered.Task;
		public void Release() => _gate.TrySetResult(true);
		readonly TaskCompletionSource<bool> _stalled = new(TaskCreationOptions.RunContinuationsAsynchronously);
		readonly TaskCompletionSource<bool> _cancelSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

		/// <summary>Completes when a read has parked at <c>stallAt</c>.</summary>
		public Task Stalled => _stalled.Task;

		/// <summary>The provider noticing a cancellation: unblocks the parked read, which then throws.</summary>
		public void Cancel() => _cancelSignal.TrySetResult(true);

		void Released()
		{
			if (Interlocked.Exchange(ref _released, 1) == 0) log("reader-released");
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				_entered.TrySetResult(true);
				_gate.Task.Wait();
				Released();
			}

			base.Dispose(disposing);
		}

		public override async ValueTask DisposeAsync()
		{
			_entered.TrySetResult(true);
			await _gate.Task.ConfigureAwait(false);
			Released();
		}

		public override bool Read()
		{
			if (failReadAt >= 0 && _index + 1 == failReadAt) throw new InvalidOperationException("the reader failed");
			if (stallAt >= 0 && _index + 1 == stallAt)
			{
				_stalled.TrySetResult(true);
				_cancelSignal.Task.Wait();
				throw (cancelException ?? (() => new OperationCanceledException()))();
			}

			return ++_index < rows.Length;
		}

		public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
		{
			if (stallAt >= 0 && _index + 1 == stallAt)
			{
				_stalled.TrySetResult(true);
				using CancellationTokenRegistration registration = cancellationToken.Register(s => ((TaskCompletionSource<bool>)s!).TrySetResult(true), _cancelSignal);
				await _cancelSignal.Task.ConfigureAwait(false);
				throw (cancelException ?? (() => new OperationCanceledException(cancellationToken)))();
			}

			return Read();
		}

		public override int FieldCount => columns.Length;
		public override bool HasRows => rows.Length > 0;
		public override bool IsClosed => false;
		public override int RecordsAffected => -1;
		public override int Depth => 0;
		public override object this[int ordinal] => GetValue(ordinal);
		public override object this[string name] => GetValue(GetOrdinal(name));
		public override object GetValue(int ordinal) => rows[_index][ordinal] ?? DBNull.Value;

		public override int GetValues(object[] values)
		{
			object?[] row = rows[_index];
			int n = Math.Min(values.Length, row.Length);
			for (int i = 0; i < n; i++) values[i] = row[i] ?? DBNull.Value;
			return n;
		}

		public override string GetName(int ordinal) => columns[ordinal];

		public override int GetOrdinal(string name)
		{
			int i = Array.FindIndex(columns, c => string.Equals(c, name, StringComparison.OrdinalIgnoreCase));
			return i >= 0 ? i : throw new IndexOutOfRangeException(name);
		}

		public override string GetDataTypeName(int ordinal) => "gated";
		public override Type GetFieldType(int ordinal) => typeof(object);
		public override bool IsDBNull(int ordinal) => GetValue(ordinal) is DBNull;
		public override bool NextResult() => false;
		public override void Close() { }
		public override IEnumerator GetEnumerator() => throw new NotSupportedException();
		public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
		public override byte GetByte(int ordinal) => throw new NotSupportedException();
		public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
		public override char GetChar(int ordinal) => throw new NotSupportedException();
		public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
		public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
		public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
		public override double GetDouble(int ordinal) => throw new NotSupportedException();
		public override float GetFloat(int ordinal) => throw new NotSupportedException();
		public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
		public override short GetInt16(int ordinal) => throw new NotSupportedException();
		public override int GetInt32(int ordinal) => throw new NotSupportedException();
		public override long GetInt64(int ordinal) => throw new NotSupportedException();
		public override string GetString(int ordinal) => throw new NotSupportedException();
	}

	static Func<ITargetBlock<TRow>, bool, ValueTask<long>> Mapped<TRow>(
		Kind kind, bool overrides, object?[][] rows,
		Exception? failure = null, Action<string>? log = null,
		IEnumerable<(string Field, string? Column)>? overrideValues = null,
		string[]? columns = null, Func<DbDataReader>? readerFactory = null, CancellationToken? callToken = null)
		where TRow : new()
	{
		IEnumerable<(string Field, string? Column)> ov = overrideValues ?? Overrides;
		string[] cols = columns ?? Columns;
		DbDataReader NewReader() => failure is not null
			? throw failure
			: readerFactory is not null ? readerFactory() : FakeReader.CreateDb(cols, rows);

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
					if (readerFactory is not null) return readerFactory();
					IDataReader reader = FakeReader.Create(cols, rows);
					reader.When(r => r.Dispose()).Do(x => log?.Invoke("reader-disposed"));
					return reader;
				});
				return (t, c) => ValueTask.FromResult(overrides ? command.ToTargetBlock(t, c, ov) : command.ToTargetBlock(t, c));
			}

			case Kind.DbCommandAsync:
			{
				var connection = new FakeDbConnection();
				connection.SetState(ConnectionState.Open);
				var command = new FakeDbCommand(connection, NewReader);
				return (t, c) => overrides ? command.ToTargetBlockAsync(t, c, ov, callToken ?? default) : command.ToTargetBlockAsync(t, c, callToken ?? default);
			}

			case Kind.ExecuteReaderSync:
			{
				IExecuteReader command = new ScriptedCommand(() => NewReader(), log, callToken ?? default);
				return (t, c) => ValueTask.FromResult(overrides ? command.ToTargetBlock(t, c, ov) : command.ToTargetBlock(t, c));
			}

			default:
			{
				IExecuteReader command = new ScriptedCommand(() => NewReader(), log, callToken ?? default);
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
		var call = Mapped<Row>(kind, overrides, Rows(count));
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
		var call = Mapped<Row>(kind, overrides, Rows(3), failure);

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
		var call = Mapped<Row>(kind, overrides, Rows(3), failure);

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
		var call = Mapped<Row>(kind, overrides, rows);

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
		if (precondition != Precondition.DbCommandWithoutConnectionAsync)
			Assert.Equal("command", ((ArgumentNullException)thrown).ParamName);
		AssertUntouched(harness.Target, "a precondition failure must not complete or fault the target");
		Assert.True(DataflowBlock.Post(harness.Target, new Row()), "the target must still accept rows");
	}

	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_NullTarget_Throws(Kind kind, bool overrides)
	{
		var call = Mapped<Row>(kind, overrides, Rows(3));
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
		var call = Mapped<Row>(kind, overrides, Rows(count));

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
		var call = Mapped<Row>(kind, overrides, Rows(count), log: log.Enqueue);

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
		var call = Mapped<Row>(kind, true, Rows(3), overrideValues: new (string Field, string? Column)[] { ("NoSuchProperty", "Id") });

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call(harness.Target, true));
		Exception faulted = await Assert.ThrowsAnyAsync<Exception>(async () => await harness.Target.Completion.WaitAsync(Timeout, ct));

		Assert.Same(thrown, faulted);
	}

	// The reader-level call shares the same pipeline: a read failure after 500 good rows delivers them, then faults the target
	// (complete: true), or leaves the target open (complete: false).
	public static TheoryData<bool, TargetKind> ReaderLevelShapes { get; } = BuildReaderLevelShapes();

	static TheoryData<bool, TargetKind> BuildReaderLevelShapes()
	{
		var data = new TheoryData<bool, TargetKind>();
		foreach (bool useAsync in new[] { false, true })
			foreach (TargetKind target in Enum.GetValues<TargetKind>())
				data.Add(useAsync, target);
		return data;
	}

	static async Task ReadFailingReader(bool useAsync, SpyTarget<Row> target, bool complete)
	{
		object?[][] rows = [.. Rows(500), null!];
		if (useAsync) await FakeReader.CreateDb(Columns, rows).ToTargetBlockAsync(target, complete);
		else FakeReader.Create(Columns, rows).ToTargetBlock(target, complete);
	}

	[Theory]
	[MemberData(nameof(ReaderLevelShapes))]
	public static async Task ReaderLevelMapped_ReaderFailsMidStream_CompleteTrue_DeliversTheRowsReadThenFaultsTheTarget(bool useAsync, TargetKind target)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<Row>(target, ct);

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await ReadFailingReader(useAsync, harness.Target, true));
		Exception faulted = await Assert.ThrowsAnyAsync<Exception>(async () => await harness.Target.Completion.WaitAsync(Timeout, ct));

		Assert.Multiple(
			() => Assert.Same(thrown, faulted),
			() => Assert.Equal(500, harness.Target.AcceptedAtFault),
			() => Assert.Null(harness.Target.AcceptedAtComplete));
	}

	[Theory]
	[MemberData(nameof(ReaderLevelShapes))]
	public static async Task ReaderLevelMapped_ReaderFailsMidStream_CompleteFalse_LeavesTheTargetOpen(bool useAsync, TargetKind target)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<Row>(target, ct);

		await Assert.ThrowsAnyAsync<Exception>(async () => await ReadFailingReader(useAsync, harness.Target, false));

		// The rows read before the failure still flow to the (open) target.
		var watch = Stopwatch.StartNew();
		while (harness.Consumed < 500 && watch.Elapsed < Timeout)
			await Task.Delay(5, ct);

		Assert.Equal(500, harness.Consumed);
		AssertUntouched(harness.Target, "complete: false must leave the target open, even when reading fails");

		harness.Target.Complete();
		await harness.Drained.WaitAsync(Timeout, ct);
	}

	public static TheoryData<Kind> Kinds { get; } = [.. Enum.GetValues<Kind>()];

	// ---- column alignment: columns of the reader that the model does not map must not shift the ones it does ----

	sealed class Person
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	public enum Layout
	{
		AllMapped,
		UnmappedBefore,
		UnmappedBetween,
		UnmappedAfter,
		UnmappedEverywhere,
	}

	static (string[] Columns, object?[][] Rows) PersonRows(Layout layout, int count)
	{
		string[] columns = layout switch
		{
			Layout.AllMapped => ["Id", "Name"],
			Layout.UnmappedBefore => ["Extra", "Id", "Name"],
			Layout.UnmappedBetween => ["Id", "Extra", "Name"],
			Layout.UnmappedAfter => ["Id", "Name", "Extra"],
			_ => ["Extra1", "Id", "Extra2", "Name", "Extra3"],
		};

		var rows = new object?[count][];
		for (int i = 0; i < count; i++)
		{
			int id = i + 1;
			rows[i] = [.. columns.Select(c => c switch
			{
				"Id" => (object?)id,
				"Name" => "name" + id,
				_ => "extra text, not a number",
			})];
		}

		return (columns, rows);
	}

	public static TheoryData<Kind, bool, Layout> AlignmentShapes { get; } = BuildAlignmentShapes();

	static TheoryData<Kind, bool, Layout> BuildAlignmentShapes()
	{
		var data = new TheoryData<Kind, bool, Layout>();
		foreach (Kind kind in Enum.GetValues<Kind>())
			foreach (bool overrides in new[] { false, true })
				foreach (Layout layout in Enum.GetValues<Layout>())
					data.Add(kind, overrides, layout);
		return data;
	}

	public static TheoryData<bool, Layout> ReaderLevelAlignmentShapes { get; } = BuildReaderLevelAlignmentShapes();

	static TheoryData<bool, Layout> BuildReaderLevelAlignmentShapes()
	{
		var data = new TheoryData<bool, Layout>();
		foreach (bool useAsync in new[] { false, true })
			foreach (Layout layout in Enum.GetValues<Layout>())
				data.Add(useAsync, layout);
		return data;
	}

	static void AssertPeople(Harness<Person> harness, int count)
		=> Assert.Multiple(
			() => Assert.Equal(Enumerable.Range(1, count), harness.Received.Select(p => p.Id)),
			() => Assert.Equal(Enumerable.Range(1, count).Select(i => "name" + i), harness.Received.Select(p => p.Name)));

	[Theory]
	[MemberData(nameof(AlignmentShapes))]
	public static async Task Mapped_UnmappedColumns_DoNotShiftTheMappedOnes(Kind kind, bool overrides, Layout layout)
	{
		const int count = 5;
		CancellationToken ct = TestContext.Current.CancellationToken;
		(string[] columns, object?[][] rows) = PersonRows(layout, count);
		var harness = new Harness<Person>(TargetKind.ActionBlock, ct);
		var call = Mapped<Person>(kind, overrides, rows, columns: columns);

		long produced = await Task.Run(async () => await call(harness.Target, true), ct).WaitAsync(Timeout, ct);
		await harness.Drained.WaitAsync(Timeout, ct);

		Assert.Equal(count, produced);
		AssertPeople(harness, count);
	}

	[Theory]
	[MemberData(nameof(ReaderLevelAlignmentShapes))]
	public static async Task ReaderLevelMapped_UnmappedColumns_DoNotShiftTheMappedOnes(bool useAsync, Layout layout)
	{
		const int count = 5;
		CancellationToken ct = TestContext.Current.CancellationToken;
		(string[] columns, object?[][] rows) = PersonRows(layout, count);
		var harness = new Harness<Person>(TargetKind.ActionBlock, ct);

		long produced = useAsync
			? await FakeReader.CreateDb(columns, rows).ToTargetBlockAsync(harness.Target, true, ct)
			: FakeReader.Create(columns, rows).ToTargetBlock(harness.Target, true);
		await harness.Drained.WaitAsync(Timeout, ct);

		Assert.Equal(count, produced);
		AssertPeople(harness, count);
	}

	// ---- the target hears nothing until the reader has been released (success, mapping failure, reader failure) ----

	public enum Scenario
	{
		Success,
		MappingFailure,
		ReaderFailure,
	}

	public static TheoryData<Kind, Scenario> GatedShapes { get; } = BuildGatedShapes();

	static TheoryData<Kind, Scenario> BuildGatedShapes()
	{
		var data = new TheoryData<Kind, Scenario>();
		foreach (Kind kind in Enum.GetValues<Kind>())
			foreach (Scenario scenario in Enum.GetValues<Scenario>())
				data.Add(kind, scenario);
		return data;
	}

	// The reader's Dispose/DisposeAsync are held shut by the test.  While they are, the pipeline may have drained every row
	// and the mapper may have faulted, yet the target must be neither completed nor faulted: the reader (and its connection)
	// is still held.  Only once the release has finished may the target hear about the end.
	[Theory]
	[MemberData(nameof(GatedShapes))]
	public static async Task Mapped_CompleteTrue_NeverCompletesOrFaultsTheTargetBeforeTheReaderIsReleased(Kind kind, Scenario scenario)
	{
		const int count = 200;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var log = new System.Collections.Concurrent.ConcurrentQueue<string>();
		object?[][] rows = Rows(count);
		if (scenario == Scenario.MappingFailure) rows[0] = ["not a number"];
		var reader = new GatedReader(Columns, rows, log.Enqueue, failReadAt: scenario == Scenario.ReaderFailure ? 100 : -1);
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct, log.Enqueue);
		var call = Mapped<Row>(kind, false, rows, readerFactory: () => reader);

		Task<long> running = Task.Run(async () => await call(harness.Target, true), ct);

		await reader.Entered.WaitAsync(Timeout, ct); // The call is done with the reader and is releasing it.
		if (scenario == Scenario.Success)
		{
			var watch = Stopwatch.StartNew();
			while (harness.Consumed < count && watch.Elapsed < Timeout)
				await Task.Delay(5, ct);
		}

		// Give a premature completion or fault every chance to happen.
		await Task.WhenAny(harness.Target.Completion, Task.Delay(500, ct));
		Assert.False(harness.Target.Completion.IsCompleted, $"{scenario}: the target was completed or faulted while the reader was still held");

		reader.Release();

		if (scenario == Scenario.ReaderFailure) await Assert.ThrowsAnyAsync<Exception>(async () => await running.WaitAsync(Timeout, ct));
		else await running.WaitAsync(Timeout, ct);

		if (scenario == Scenario.Success)
		{
			await harness.Target.Completion.WaitAsync(Timeout, ct);
			Assert.Equal(["reader-released", "target-completed"], log.ToArray());
		}
		else
		{
			await Assert.ThrowsAnyAsync<Exception>(async () => await harness.Target.Completion.WaitAsync(Timeout, ct));
			Assert.Equal(["reader-released", "target-faulted"], log.ToArray());
		}
	}

	// ---- completion must not depend on the scheduler the caller happens to run on ----

	[Fact]
	public static async Task Mapped_CompleteTrue_CallerBlockedOnAnExclusiveScheduler_StillCompletesTheTarget()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var pair = new ConcurrentExclusiveSchedulerPair();
		var target = new ActionBlock<Row>(_ => { });

		// The caller runs on the exclusive scheduler and then blocks that scheduler's thread waiting for the target.
		bool completed = await Task.Factory.StartNew(() =>
		{
			IExecuteReader command = new FakeExecuteReader(Columns, Rows(10));
			command.ToTargetBlock(target, true);
			return target.Completion.Wait(TimeSpan.FromSeconds(10));
		}, ct, TaskCreationOptions.None, pair.ExclusiveScheduler).WaitAsync(Timeout, ct);

		Assert.True(completed, "the target was never completed: its completion was queued on the caller's blocked scheduler");
	}

	// ---- one exception shape, whichever stage failed ----

	static async Task<AggregateException> FaultOfAsync(Task completion)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		try
		{
			await completion.WaitAsync(Timeout, ct);
		}
		catch (TimeoutException)
		{
			throw;
		}
		catch
		{
			// Inspected below.
		}

		return Assert.IsType<AggregateException>(completion.Exception);
	}

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_MappingFailure_FaultsTheTargetWithTheSameShapeAsAReaderFailure(Kind kind)
	{
		const int count = 20_000;
		CancellationToken ct = TestContext.Current.CancellationToken;

		// A row that cannot be mapped (the second stage fails)...
		object?[][] badRows = Rows(count);
		badRows[0] = ["not a number"];
		var mappingHarness = new Harness<Row>(TargetKind.ActionBlock, ct);
		long produced = await Task.Run(async () => await Mapped<Row>(kind, false, badRows)(mappingHarness.Target, true), ct).WaitAsync(TimeSpan.FromSeconds(10), ct); // An async writer that is not stopped hangs here.
		AggregateException mapping = await FaultOfAsync(mappingHarness.Target.Completion);

		// ...and a reader that fails (the first stage fails).
		var readerHarness = new Harness<Row>(TargetKind.ActionBlock, ct);
		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () =>
			await Mapped<Row>(kind, false, [.. Rows(10), null!])(readerHarness.Target, true));
		AggregateException reader = await FaultOfAsync(readerHarness.Target.Completion);

		Assert.Multiple(
			() => Assert.True(produced <= TransformBuffer + 64, $"the writer should have stopped within one block of the mapper failing on row 0, but it wrote {produced} of {count} rows"),
			() => Assert.Single(mapping.InnerExceptions),
			() => Assert.IsType<InvalidOperationException>(mapping.InnerExceptions[0]), // "Unable to set value of property..." directly, not wrapped again
			() => Assert.Single(reader.InnerExceptions),
			() => Assert.Same(thrown, reader.InnerExceptions[0]));
	}

	// A target that throws from Complete() must not take anything down, nor leave a faulted task unobserved.
	sealed class ThrowingTarget<T>(ITargetBlock<T> inner) : ITargetBlock<T>
	{
		int _completeCalls;
		public int CompleteCalls => Volatile.Read(ref _completeCalls);
		public Task Completion => inner.Completion;

		public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
			=> inner.OfferMessage(messageHeader, messageValue, source, consumeToAccept);

		public void Complete()
		{
			Interlocked.Increment(ref _completeCalls);
			throw new InvalidOperationException("dfx-target-complete-threw");
		}

		public void Fault(Exception exception) => inner.Fault(exception);
	}

	[Fact]
	public static async Task Mapped_TargetThatThrowsFromComplete_IsHandledAndNothingIsLeftUnobserved()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var unobserved = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
		void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs e)
		{
			foreach (Exception inner in e.Exception.Flatten().InnerExceptions)
				if (inner.Message == "dfx-target-complete-threw") unobserved.Enqueue(inner);
		}

		TaskScheduler.UnobservedTaskException += OnUnobserved;
		try
		{
			var target = new ThrowingTarget<Row>(new ActionBlock<Row>(_ => { }));
			IExecuteReader command = new FakeExecuteReader(Columns, Rows(10));

			await Task.Run(() => command.ToTargetBlock(target, true), ct); // The caller is not told: it has no say in the target's Complete().

			var watch = Stopwatch.StartNew();
			while (target.CompleteCalls == 0 && watch.Elapsed < Timeout)
				await Task.Delay(5, ct);

			for (int i = 0; i < 3; i++)
			{
				GC.Collect();
				GC.WaitForPendingFinalizers();
			}

			Assert.Equal(1, target.CompleteCalls);
			Assert.Empty(unobserved);
		}
		finally
		{
			TaskScheduler.UnobservedTaskException -= OnUnobserved;
		}
	}

	// ---- a target closed under a running pipeline must not leave the (async) writer, and its reader, hanging ----

	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_TargetClosedMidStream_StopsTheWriter_AndReleasesTheReader(Kind kind, bool complete)
	{
		const int count = 20_000;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var shortTimeout = TimeSpan.FromSeconds(10);
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct);
		var call = Mapped<Row>(kind, false, Rows(count));

		Task<long> running = Task.Run(async () => await call(harness.Target, complete), ct);

		var watch = Stopwatch.StartNew();
		while (harness.Consumed < 20 && watch.Elapsed < Timeout)
			await Task.Delay(5, ct);

		harness.Target.Complete(); // The consumer closes the target while rows are still being read.

		long produced = await running.WaitAsync(shortTimeout, ct); // Hangs forever if the writer is left waiting on a block that can no longer deliver.
		await harness.Drained.WaitAsync(Timeout, ct);

		Assert.True(produced < count, $"the writer should have stopped, but it wrote all {produced} rows");
	}

	// ---- a block cancelled through its options is a failure of the pipeline, not a clean end ----

	[Fact]
	public static async Task Pipeline_BlockCancelled_FaultsTheTargetInsteadOfCompletingIt()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		using var cancellation = new CancellationTokenSource();
		cancellation.Cancel();
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct);
		var options = new ExecutionDataflowBlockOptions { CancellationToken = cancellation.Token, BoundedCapacity = 1024 };

		Dataflow.Transformer<Row>.Create().PipeResultsTo(FakeReader.Create(Columns, Rows(10)), harness.Target, true, options);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await harness.Target.Completion.WaitAsync(Timeout, ct));
		Assert.Null(harness.Target.AcceptedAtComplete);
	}

	// ---- a finished call leaves nothing behind on a long-lived target ----

	public enum RetentionKind
	{
		ReaderLevelSync,
		ReaderLevelAsync,
		ExecuteReaderSync,
		ExecuteReaderAsync,
		DbCommandAsync,
	}

	public static TheoryData<RetentionKind> RetentionKinds { get; } = [.. Enum.GetValues<RetentionKind>()];

	static async Task SettleAsync(CancellationToken ct)
	{
		await Task.Delay(200, ct); // Let the last pipelines drain.
		for (int i = 0; i < 3; i++)
		{
			GC.Collect();
			GC.WaitForPendingFinalizers();
		}
	}

	// complete: false against one open target for the lifetime of the test, thousands of calls.  Whatever a call registers
	// on the target (or on anything the target keeps alive) must be gone when the call's pipeline has finished.
	[Theory]
	[MemberData(nameof(RetentionKinds))]
	public static async Task Mapped_CompleteFalse_RetainsNothingPerCallOnALongLivedTarget(RetentionKind kind)
	{
		const int warmUp = 300;
		const int calls = 4000;
		const double allowedBytesPerCall = 500;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var target = new ActionBlock<Row>(_ => { }); // Open, unbounded, never completed.
		object?[][] rows = Rows(3);

		DbDataReader NewReader()
		{
			var reader = new GatedReader(Columns, rows, _ => { });
			reader.Release(); // Not held on release.
			return reader;
		}

		Func<Task> oneCall;
		switch (kind)
		{
			case RetentionKind.ReaderLevelSync:
				oneCall = () =>
				{
					NewReader().ToTargetBlock(target, false);
					return Task.CompletedTask;
				};
				break;

			case RetentionKind.ReaderLevelAsync:
				oneCall = async () => await NewReader().ToTargetBlockAsync(target, false, ct);
				break;

			default:
			{
				Kind commandKind = kind switch
				{
					RetentionKind.ExecuteReaderSync => Kind.ExecuteReaderSync,
					RetentionKind.ExecuteReaderAsync => Kind.ExecuteReaderAsync,
					_ => Kind.DbCommandAsync,
				};
				var call = Mapped<Row>(commandKind, false, rows, readerFactory: NewReader);
				oneCall = async () => await call(target, false);
				break;
			}
		}

		for (int i = 0; i < warmUp; i++) await oneCall();
		await SettleAsync(ct);
		long before = GC.GetTotalMemory(true);

		for (int i = 0; i < calls; i++) await oneCall();
		await SettleAsync(ct);
		long after = GC.GetTotalMemory(true);
		GC.KeepAlive(target);

		double perCall = (double)(after - before) / calls;
		TestContext.Current.TestOutputHelper?.WriteLine($"retention {kind}: {perCall:F0} B/call over {calls} calls");
		Assert.True(perCall < allowedBytesPerCall, $"{kind}: {perCall:F0} bytes retained per call on a long-lived target (allowed {allowedBytesPerCall:F0})");
	}

	// ---- a provider that surfaces its own exception when the read is cancelled: the pipeline's stop is a quiet end ----

	public static TheoryData<Kind> AsyncKinds { get; } = [Kind.DbCommandAsync, Kind.ExecuteReaderAsync];

	[Theory]
	[MemberData(nameof(AsyncKinds))]
	public static async Task Mapped_MappingFailure_ProviderThrowsItsOwnExceptionWhenTheReadIsCancelled_EndsQuietly(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var log = new System.Collections.Concurrent.ConcurrentQueue<string>();
		object?[][] rows = Rows(200);
		rows[0] = ["not a number"];
		var provider = new InvalidOperationException("provider: the command was cancelled");
		var reader = new GatedReader(Columns, rows, log.Enqueue, stallAt: 5, cancelException: () => provider);
		reader.Release();
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct, log.Enqueue);
		var call = Mapped<Row>(kind, false, rows, readerFactory: () => reader);

		// The writer parks in a read; the mapper fails on row 0, the pipeline stops the writer by cancelling that read, and the
		// provider answers with an exception of its own instead of an OperationCanceledException.
		long produced = await Task.Run(async () => await call(harness.Target, true), ct).WaitAsync(Timeout, ct);
		AggregateException faulted = await FaultOfAsync(harness.Target.Completion);

		Assert.Multiple(
			() => Assert.InRange(produced, 0, 5), // At most the rows read before the stall.
			() => Assert.Single(faulted.InnerExceptions),
			() => Assert.NotSame(provider, faulted.InnerExceptions[0]), // The target heard about the mapping failure...
			() => Assert.IsType<InvalidOperationException>(faulted.InnerExceptions[0]),
			() => Assert.Equal(["reader-released", "target-faulted"], log.ToArray()));
	}

	// ---- the caller cancels while the reader is stalled in a read ----

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_CallerCancelsWhileTheReaderIsStalled_ReleasesTheReaderThenFaultsTheTargetWithTheCancellation(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		using var cancellation = new CancellationTokenSource();
		var log = new System.Collections.Concurrent.ConcurrentQueue<string>();
		object?[][] rows = Rows(200);
		var reader = new GatedReader(Columns, rows, log.Enqueue, stallAt: 50, cancelException: () => new OperationCanceledException(cancellation.Token));
		reader.Release(); // The release itself is not held: this test is about the cancelled read.
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct, log.Enqueue);
		var call = Mapped<Row>(kind, false, rows, readerFactory: () => reader, callToken: cancellation.Token);

		Task<long> running = Task.Run(async () => await call(harness.Target, true), ct);

		await reader.Stalled.WaitAsync(Timeout, ct); // The writer is parked in the read of row 50.
		cancellation.Cancel();
		reader.Cancel(); // The synchronous overloads take no token: the provider's own cancellation unblocks their read.

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await running.WaitAsync(Timeout, ct));
		AggregateException faulted = await FaultOfAsync(harness.Target.Completion);

		Assert.Multiple(
			() => Assert.Single(faulted.InnerExceptions),
			() => Assert.IsAssignableFrom<OperationCanceledException>(faulted.InnerExceptions[0]),
			() => Assert.Equal(["reader-released", "target-faulted"], log.ToArray()),
			() => Assert.Null(harness.Target.AcceptedAtComplete),
			() => Assert.Equal(50, harness.Target.AcceptedAtFault)); // The 50 rows read before the cancellation were delivered first.
	}

	// ---- get in, get out: the reader is released as soon as it has been read, not held while a slow consumer drains ----

	// The consumer is parked behind a gate, the result is larger than the target's capacity but smaller than the 1024-row
	// buffer.  The call must finish and the reader must already be released while most rows are still undelivered;
	// only when the gate opens do the rows arrive, and the target then completes, after the reader's release.
	[Theory]
	[MemberData(nameof(ShapesWithTarget))]
	public static async Task Mapped_AParkedConsumer_DoesNotHoldTheReader(Kind kind, bool overrides, TargetKind target)
	{
		const int count = 300;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var log = new System.Collections.Concurrent.ConcurrentQueue<string>();
		var consumerGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		object?[][] rows = Rows(count);
		var reader = new GatedReader(Columns, rows, log.Enqueue);
		reader.Release(); // The reader's release itself is not held: this test is about the consumer.
		var harness = new Harness<Row>(target, ct, log.Enqueue, consumerGate.Task);
		var call = Mapped<Row>(kind, overrides, rows, readerFactory: () => reader);

		try
		{
			// The positive condition is the call finishing (an event, with a generous bound only to turn a hang into a failure).
			long produced = await Task.Run(async () => await call(harness.Target, true), ct).WaitAsync(TimeSpan.FromSeconds(8), ct);

			// The consumer is parked, so none of the following can change until the gate opens: no sleeps needed.
			string[] atReturn = log.ToArray();
			int accepted = harness.Target.Accepted;
			int consumed = harness.Consumed;
			bool completed = harness.Target.Completion.IsCompleted;

			Assert.Multiple(
				() => Assert.Equal(count, produced), // The reader was read to the end...
				() => Assert.Equal(["reader-released"], atReturn), // ...and is already released,
				() => Assert.True(accepted < count, $"{accepted} of {count} rows accepted by the target: it should still be behind"), // while rows are still undelivered,
				() => Assert.Equal(0, consumed),
				() => Assert.False(completed, "the target cannot complete while rows are undelivered"));

			consumerGate.TrySetResult(true);
			await harness.Drained.WaitAsync(Timeout, ct);

			Assert.Multiple(
				() => Assert.Equal(count, harness.Consumed),
				() => Assert.Equal(["reader-released", "target-completed"], log.ToArray()));
		}
		finally
		{
			consumerGate.TrySetResult(true); // Never leave a consumer parked, even when an assertion fails.
		}
	}

	// The reader-level calls validate their arguments once, by name.
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ReaderLevelMapped_NullArguments_ThrowArgumentNullExceptionNamingTheParameter(bool useAsync)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var harness = new Harness<Row>(TargetKind.ActionBlock, ct);
		IDataReader reader = FakeReader.Create(Columns, Rows(3));

		var nullReader = await Assert.ThrowsAsync<ArgumentNullException>(async () =>
		{
			if (useAsync) await DataflowExtensions.ToTargetBlockAsync<Row>((IDataReader)null!, harness.Target, true, ct);
			else DataflowExtensions.ToTargetBlock<Row>((IDataReader)null!, harness.Target, true);
		});
		var nullTarget = await Assert.ThrowsAsync<ArgumentNullException>(async () =>
		{
			if (useAsync) await DataflowExtensions.ToTargetBlockAsync<Row>(reader, null!, true, ct);
			else DataflowExtensions.ToTargetBlock<Row>(reader, null!, true);
		});

		Assert.Multiple(
			() => Assert.Equal("reader", nullReader.ParamName),
			() => Assert.Equal("target", nullTarget.ParamName),
			() => AssertUntouched(harness.Target, "an argument failure must not complete or fault the target"));
	}
}
