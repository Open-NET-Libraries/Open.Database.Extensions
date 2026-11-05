using System.Collections;
using System.Data.Common;
using BoundedChannel = System.Threading.Channels.Channel;
using Kind = Open.Database.Extensions.Tests.ToChannelMappedCompletionTests.Kind;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The mapped ToChannel<T>/ToChannelAsync<T> overloads are a two stage pipeline: stage 1 reads the database into a
// bounded internal buffer as fast as it can, stage 2 maps the buffered rows into the caller's target at the consumer's
// pace. Completion flows downstream: the command-level core completes the buffer once the reader has been released
// (or with the error that ended the read) and, after the last buffered row has been delivered, stage 2 completes the
// target. These tests pin that structure: ordering, early release of the reader, no hang when stage 2 stops, rows read
// before a failure are still delivered, and the preconditions leave the target alone.
[ExcludeFromCodeCoverage]
public static partial class ToChannelPipelineTests
{
	const int TargetCapacity = 10;
	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
	static readonly TimeSpan Prompt = TimeSpan.FromSeconds(10);

	static readonly string[] Columns = ["Id"];
	static readonly (string Field, string? Column)[] Overrides = [("Id", "Id")];

	sealed class Row
	{
		/// <summary>Invoked each time a record is mapped into a row.</summary>
		public static Action? OnMapped { get; set; }

		public int Id
		{
			get;
			set
			{
				field = value;
				OnMapped?.Invoke();
			}
		}
	}

	public static TheoryData<Kind> Kinds { get; } = [.. Enum.GetValues<Kind>()];

	public static TheoryData<Kind, bool> KindsAndFlag { get; } = Combine();

	public static TheoryData<Kind, bool> Shapes { get; } = Combine();

	static TheoryData<Kind, bool> Combine()
	{
		var data = new TheoryData<Kind, bool>();
		foreach (Kind kind in Enum.GetValues<Kind>())
		{
			data.Add(kind, false);
			data.Add(kind, true);
		}

		return data;
	}

	// 0 = unmapped, 1 = mapped, 2 = mapped with field mapping overrides.
	public static TheoryData<Kind, int> Modes { get; } = BuildModes();

	static TheoryData<Kind, int> BuildModes()
	{
		var data = new TheoryData<Kind, int>();
		foreach (Kind kind in Enum.GetValues<Kind>())
		{
			for (int mode = 0; mode < 3; mode++)
				data.Add(kind, mode);
		}

		return data;
	}

	public static TheoryData<bool, bool, bool> ReaderLevelShapes { get; } = BuildReaderLevelShapes();

	static TheoryData<bool, bool, bool> BuildReaderLevelShapes()
	{
		var data = new TheoryData<bool, bool, bool>();
		foreach (bool useAsync in new[] { false, true })
		{
			foreach (bool complete in new[] { false, true })
			{
				foreach (bool overrides in new[] { false, true })
					data.Add(useAsync, complete, overrides);
			}
		}

		return data;
	}

	static object?[][] Rows(int count, object? value = null)
	{
		object?[][] rows = new object?[count][];
		for (int i = 0; i < count; i++) rows[i] = [value ?? i];
		return rows;
	}

	// ---- test doubles ----

	/// <summary>Wraps a fake reader to record when it is released, to fail after a number of rows, and to misbehave on demand.</summary>
	sealed class SpyReader(DbDataReader inner, int failAfterRows, Exception? failure) : DbDataReader
	{
		readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
		int _reads;
		int _rowsRead;
		Exception? _armed;

		/// <summary>Completes once the reader has been released.</summary>
		public Task Disposed => _disposed.Task;
		public bool IsDisposed => _disposed.Task.IsCompleted;

		/// <summary>Completes as soon as releasing the reader begins (it may then be held up by <see cref="DisposeGate"/>).</summary>
		public TaskCompletionSource DisposeStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		/// <summary>When set, releasing the reader blocks until this completes.</summary>
		public TaskCompletionSource? DisposeGate { get; set; }

		/// <summary>The number of rows the reader has handed out.</summary>
		public int RowsRead => Volatile.Read(ref _rowsRead);

		/// <summary>Makes every following read fail with the given error.</summary>
		public void ArmFailure(Exception error) => Volatile.Write(ref _armed, error);

		/// <summary>Runs before each asynchronous read with the number of rows handed out so far.</summary>
		public Func<int, CancellationToken, Task>? BeforeReadAsync { get; set; }

		/// <summary>Runs after each read with the number of rows handed out so far and whether the read found a row.</summary>
		public Action<int, bool>? AfterRead { get; set; }

		public override bool Read()
		{
			if (Volatile.Read(ref _armed) is { } armed) throw armed;
			if (failure is not null && _reads++ == failAfterRows) throw failure;
			bool more = inner.Read();
			if (more) Interlocked.Increment(ref _rowsRead);
			AfterRead?.Invoke(RowsRead, more);
			return more;
		}

		public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();
			if (BeforeReadAsync is { } hook) await hook(RowsRead, cancellationToken).ConfigureAwait(false);
			return Read();
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				DisposeStarted.TrySetResult();
				DisposeGate?.Task.Wait(Timeout);
				_disposed.TrySetResult();
			}

			base.Dispose(disposing);
		}

		public override int Depth => 0;
		public override int FieldCount => inner.FieldCount;
		public override bool HasRows => true;
		public override bool IsClosed => IsDisposed;
		public override int RecordsAffected => -1;
		public override object this[int ordinal] => inner[ordinal];
		public override object this[string name] => inner[name];
		public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
		public override string GetName(int ordinal) => inner.GetName(ordinal);
		public override int GetOrdinal(string name) => inner.GetOrdinal(name);
		public override object GetValue(int ordinal) => inner.GetValue(ordinal);
		public override int GetValues(object[] values) => inner.GetValues(values);
		public override bool IsDBNull(int ordinal) => inner.GetValue(ordinal) is DBNull;
		public override bool NextResult() => false;
		public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
		public override byte GetByte(int ordinal) => throw new NotSupportedException();
		public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
		public override char GetChar(int ordinal) => throw new NotSupportedException();
		public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
		public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
		public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
		public override double GetDouble(int ordinal) => throw new NotSupportedException();
		public override IEnumerator GetEnumerator() => throw new NotSupportedException();
		public override Type GetFieldType(int ordinal) => throw new NotSupportedException();
		public override float GetFloat(int ordinal) => throw new NotSupportedException();
		public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
		public override short GetInt16(int ordinal) => throw new NotSupportedException();
		public override int GetInt32(int ordinal) => throw new NotSupportedException();
		public override long GetInt64(int ordinal) => throw new NotSupportedException();
		public override string GetString(int ordinal) => throw new NotSupportedException();
	}

	/// <summary>Counts executions and hands out the reader that was created last.</summary>
	sealed class Probe
	{
		readonly TaskCompletionSource<SpyReader> _created = new(TaskCreationOptions.RunContinuationsAsynchronously);
		int _executions;

		public int Executions => Volatile.Read(ref _executions);
		public SpyReader Reader => _created.Task.Result;
		public Task<SpyReader> Created => _created.Task;

		public Func<DbDataReader> Factory(
			object?[][] rows, int failAfterRows = -1, Exception? failure = null,
			Action<SpyReader>? configure = null, IReadOnlyList<string>? columns = null)
			=> () =>
			{
				Interlocked.Increment(ref _executions);
				var reader = new SpyReader(FakeReader.CreateDb(columns ?? Columns, rows), failAfterRows, failure);
				configure?.Invoke(reader);
				_created.TrySetResult(reader);
				return reader;
			};
	}

	/// <summary>Reports (just before delegating) that something is completing the target.</summary>
	sealed class HookWriter<T>(ChannelWriter<T> inner, Action onComplete) : ChannelWriter<T>
	{
		public override bool TryWrite(T item) => inner.TryWrite(item);

		public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
			=> inner.WaitToWriteAsync(cancellationToken);

		public override bool TryComplete(Exception? error = null)
		{
			onComplete();
			return inner.TryComplete(error);
		}
	}

	sealed class ScriptedCommand(Func<DbDataReader> factory, bool useAsyncRead, CancellationToken token) : IExecuteReaderAsync
	{
		public bool UseAsyncRead => useAsyncRead;
		public CancellationToken CancellationToken => token;

		public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			using DbDataReader reader = factory();
			handler(reader);
		}

		public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			using DbDataReader reader = factory();
			return transform(reader);
		}

		public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			await using DbDataReader reader = factory();
			await handler(reader).ConfigureAwait(false);
		}

		public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReader(reader => transform(reader, state), behavior);

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReaderAsync(reader => transform(reader, state), behavior);

		public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			await using DbDataReader reader = factory();
			return await transform(reader).ConfigureAwait(false);
		}
	}

	static (IDbCommand Command, IDbConnection Connection) NewDbCommand(Func<DbDataReader> factory, ConnectionState state)
	{
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		command.Connection.Returns(connection);
		connection.State.Returns(state);
		command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ => factory());
		return (command, connection);
	}

	static FakeDbCommand NewFakeDbCommand(Func<DbDataReader> factory, ConnectionState state)
	{
		var connection = new FakeDbConnection();
		connection.SetState(state);
		return new FakeDbCommand(connection, factory);
	}

	/// <summary>The unmapped ToChannel call (single stage) for the given command type.</summary>
	static Func<ChannelWriter<object[]>, bool, ValueTask<long>> Unmapped(Kind kind, Func<DbDataReader> factory)
	{
		switch (kind)
		{
			case Kind.ExecuteReader:
				{
					IExecuteReader command = new ScriptedCommand(factory, false, default);
					return (w, c) => command.ToChannel(w, c);
				}

			case Kind.ExecuteReaderAsync:
			case Kind.ExecuteReaderAsyncUsingAsyncRead:
				{
					IExecuteReaderAsync command = new ScriptedCommand(factory, kind == Kind.ExecuteReaderAsyncUsingAsyncRead, default);
					return (w, c) => command.ToChannelAsync(w, c);
				}

			case Kind.DbCommandInterface:
				{
					(IDbCommand command, _) = NewDbCommand(factory, ConnectionState.Open);
					return (w, c) => command.ToChannel(w, c);
				}

			default:
				{
					FakeDbCommand command = NewFakeDbCommand(factory, ConnectionState.Open);
					return (w, c) => command.ToChannelAsync(w, c);
				}
		}
	}

	static async Task<(List<T> Items, Exception? Error)> Drain<T>(ChannelReader<T> reader, CancellationToken cancellationToken, int spin = 0)
	{
		var items = new List<T>();
		try
		{
			await foreach (T item in reader.ReadAllAsync(cancellationToken))
			{
				items.Add(item);
				if (spin > 0) Thread.SpinWait(spin);
			}

			return (items, null);
		}
		catch (Exception ex)
		{
			return (items, ex);
		}
	}

	static void AssertInOrder(IReadOnlyList<Row> rows, int expectedCount)
	{
		Assert.Equal(expectedCount, rows.Count);
		for (int i = 0; i < rows.Count; i++)
			Assert.Equal(i, rows[i].Id); // Also proves the mapper knew the column names before the first row arrived.
	}

	// ---- ordering: completion follows the release of the reader ----

	static async Task<bool?> RunOrdering<TItem>(Func<ChannelWriter<TItem>, bool, ValueTask<long>> call, Probe probe, CancellationToken ct)
	{
		const int Count = 300;
		var channel = BoundedChannel.CreateBounded<TItem>(TargetCapacity);
		bool? disposedAtComplete = null;
		var writer = new HookWriter<TItem>(channel.Writer, () =>
		{
			if (probe.Created.IsCompleted) disposedAtComplete ??= probe.Reader.IsDisposed;
		});

		Task<(List<TItem> Items, Exception? Error)> consumer = Task.Run(() => Drain(channel.Reader, ct, 200), ct);
		long produced = await call(writer, true).AsTask().WaitAsync(Timeout, ct);
		(List<TItem> items, Exception? error) = await consumer.WaitAsync(Timeout, ct);

		Assert.Null(error);
		Assert.Equal(Count, produced);
		Assert.Equal(Count, items.Count);
		return disposedAtComplete;
	}

	[Theory]
	[MemberData(nameof(Modes))]
	public static async Task TheTarget_IsCompleted_OnlyAfterTheReaderWasReleased(Kind kind, int mode)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		Func<DbDataReader> factory = probe.Factory(Rows(300));

		bool? disposedAtComplete = mode == 0
			? await RunOrdering(Unmapped(kind, factory), probe, ct)
			: await RunOrdering(Mapped<Row>(kind, mode == 2, factory, ct), probe, ct);

		Assert.True(disposedAtComplete, "the reader must be released before anything completes the target");
	}

	// ---- the mission: get in, get out ----

	// The reader (and with it the connection) is held only while the database is being read into the buffer. A slow
	// consumer must not keep it open: with the consumer parked, the reader is already released and the call has
	// already returned, while rows are still undelivered. Everything is delivered when the consumer resumes, and only
	// then is the target completed.
	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_AParkedConsumer_DoesNotHoldTheReader(Kind kind, bool overrides)
	{
		const int Count = 300;
		const int TakenBeforeParking = 3;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var received = new List<Row>();

		var consumer = Task.Run(async () =>
		{
			await foreach (Row row in channel.Reader.ReadAllAsync(ct))
			{
				received.Add(row);
				if (received.Count != TakenBeforeParking) continue;
				parked.TrySetResult();
				await gate.Task;
			}
		}, ct);

		Task<long> call = Mapped<Row>(kind, overrides, probe.Factory(Rows(Count)), ct)(channel.Writer, true).AsTask();

		await parked.Task.WaitAsync(Prompt, ct);
		SpyReader reader = await probe.Created.WaitAsync(Prompt, ct);
		await reader.Disposed.WaitAsync(Prompt, ct); // Released while the consumer is parked...
		long produced = await call.WaitAsync(Prompt, ct); // ...and the call is done too, it was never waiting for the consumer.

		Assert.Multiple(
			() => Assert.Equal(Count, produced),
			() => Assert.Equal(TakenBeforeParking, received.Count), // Still parked: most of the rows are undelivered.
			() => Assert.False(channel.Reader.Completion.IsCompleted, "nothing may be completed while rows are undelivered"));

		gate.SetResult();
		await consumer.WaitAsync(Timeout, ct);
		await channel.Reader.Completion.WaitAsync(Timeout, ct);

		AssertInOrder(received, Count);
	}

	// ---- stage 2 stops: stage 1 must never be left blocked on a buffer nothing drains ----

	[Theory]
	[MemberData(nameof(KindsAndFlag))]
	public static async Task Mapped_WhenMappingFails_TheCallEndsPromptly_AndTheReaderIsReleased(Kind kind, bool complete)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(5000, "not a number")), ct);

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call(channel.Writer, complete).AsTask().WaitAsync(Prompt, ct));

		Assert.IsNotType<TimeoutException>(thrown); // It hung.
		Assert.IsNotType<OperationCanceledException>(thrown, exactMatch: false); // The cause is reported, not the cancellation it triggered.
		await probe.Reader.Disposed.WaitAsync(Prompt, ct);

		if (complete)
		{
			await Assert.ThrowsAnyAsync<Exception>(async () => await channel.Reader.Completion.WaitAsync(Prompt, ct));
			Assert.True(channel.Reader.Completion.IsFaulted, "the target must end faulted, not cleanly");
		}
		else
		{
			Assert.True(channel.Writer.TryWrite(new Row()), "complete: false must leave the target open");
		}
	}

	[Theory]
	[MemberData(nameof(KindsAndFlag))]
	public static async Task Mapped_WhenTheConsumerClosesTheTargetEarly_TheCallEndsPromptly_AndTheReaderIsReleased(Kind kind, bool complete)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(5000)), ct);

		var consumer = Task.Run(async () =>
		{
			int taken = 0;
			await foreach (Row _ in channel.Reader.ReadAllAsync(ct))
			{
				if (++taken < 3) continue;
				channel.Writer.TryComplete();
				break;
			}
		}, ct);

		Task<long> task = call(channel.Writer, complete).AsTask();
		await consumer.WaitAsync(Prompt, ct);

		Task finished = await Task.WhenAny(task, Task.Delay(Prompt, ct));
		Assert.Same(task, finished); // Otherwise stage 1 is blocked on a full buffer for good.
		try { await task; } catch { /* Ending with a failure is fine, hanging is not. */ }

		await probe.Reader.Disposed.WaitAsync(Prompt, ct);
	}

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_WhenTheCallerCancels_TheCallEndsPromptly_AndTheTargetEnds(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(5000)), cts.Token);

		Task<long> task = call(channel.Writer, true).AsTask();
		await probe.Created.WaitAsync(Prompt, ct);
		await Task.Delay(100, ct); // Nobody consumes: both stages end up waiting.
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task.WaitAsync(Prompt, ct));
		// A consumer that shows up late gets whatever was already delivered, and then the end of the stream (here: the cancellation).
		(_, Exception? ended) = await Drain(channel.Reader, ct).WaitAsync(Prompt, ct);
		Assert.NotNull(ended);
		await probe.Reader.Disposed.WaitAsync(Prompt, ct);
	}

	[Theory]
	[MemberData(nameof(ReaderLevelShapes))]
	public static async Task ReaderLevel_WhenMappingFails_TheCallEndsPromptly_AndTheTargetIsFaulted(bool useAsync, bool complete, bool overrides)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var reader = (SpyReader)probe.Factory(Rows(5000, "not a number"))();

		Func<ValueTask<long>> call = (useAsync, overrides) switch
		{
			(false, false) => () => reader.ToChannel(channel.Writer, complete, ct),
			(false, true) => () => reader.ToChannel(channel.Writer, complete, Overrides, ct),
			(true, false) => () => reader.ToChannelAsync(channel.Writer, complete, ct),
			(true, true) => () => reader.ToChannelAsync(channel.Writer, complete, Overrides, ct),
		};

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call().AsTask().WaitAsync(Prompt, ct));
		Assert.IsNotType<TimeoutException>(thrown);
		Assert.IsNotType<OperationCanceledException>(thrown, exactMatch: false);

		if (complete)
		{
			await Assert.ThrowsAnyAsync<Exception>(async () => await channel.Reader.Completion.WaitAsync(Prompt, ct));
			Assert.True(channel.Reader.Completion.IsFaulted);
		}
		else
		{
			Assert.True(channel.Writer.TryWrite(new Row()));
		}

		Assert.False(reader.IsDisposed, "the caller's reader belongs to the caller");
	}

	// ---- reader level: the caller owns the reader ----

	[Theory]
	[MemberData(nameof(ReaderLevelShapes))]
	public static async Task ReaderLevel_DeliversEverything_AndLeavesTheReaderToTheCaller(bool useAsync, bool complete, bool overrides)
	{
		const int Count = 300;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var reader = (SpyReader)probe.Factory(Rows(Count))();

		Task<(List<Row> Items, Exception? Error)> consumer = complete
			? Task.Run(() => Drain(channel.Reader, ct, 200), ct)
			: Task.Run(async () =>
			{
				var items = new List<Row>();
				for (int i = 0; i < Count; i++) items.Add(await channel.Reader.ReadAsync(ct));
				return (items, (Exception?)null);
			}, ct);

		ValueTask<long> call = (useAsync, overrides) switch
		{
			(false, false) => reader.ToChannel(channel.Writer, complete, ct),
			(false, true) => reader.ToChannel(channel.Writer, complete, Overrides, ct),
			(true, false) => reader.ToChannelAsync(channel.Writer, complete, ct),
			(true, true) => reader.ToChannelAsync(channel.Writer, complete, Overrides, ct),
		};

		long produced = await call.AsTask().WaitAsync(Timeout, ct);
		(List<Row> items, Exception? error) = await consumer.WaitAsync(Timeout, ct);

		Assert.Equal(Count, produced);
		Assert.Null(error);
		AssertInOrder(items, Count);
		Assert.False(reader.IsDisposed, "the caller's reader belongs to the caller");
		if (complete)
			await channel.Reader.Completion.WaitAsync(Timeout, ct);
		else
			Assert.True(channel.Writer.TryWrite(new Row()), "complete: false must leave the target open");
	}

	[Fact]
	public static void ReaderLevel_NullArguments_Throw()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var reader = (SpyReader)new Probe().Factory(Rows(1))();

		Assert.Multiple(
			() => Assert.Throws<ArgumentNullException>(() => ((IDataReader)null!).ToChannel(channel.Writer, true, ct)),
			() => Assert.Throws<ArgumentNullException>(() => reader.ToChannel<Row>(null!, true, ct)),
			() => Assert.Throws<ArgumentNullException>(() => ((DbDataReader)null!).ToChannelAsync(channel.Writer, true, ct)),
			() => Assert.Throws<ArgumentNullException>(() => reader.ToChannelAsync<Row>(null!, true, ct)));
		Assert.False(channel.Reader.Completion.IsCompleted);
	}

	// ---- a failure while reading: what was read before it is still delivered ----

	// More rows than the buffer holds (1024) were read before the failure, and the consumer is slow, so a good many
	// are still in the buffer when it happens. Every one of them must reach the consumer, and only then does the target
	// end with the database error.
	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_WhenTheReaderFailsMidStream_EveryRowReadBeforeItIsDelivered_ThenTheTargetFaults(Kind kind, bool overrides)
	{
		const int FailAfter = 3000;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var failure = new InvalidOperationException("db");
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, overrides, probe.Factory(Rows(5000), FailAfter, failure), ct);

		Task<(List<Row> Items, Exception? Error)> consumer = Task.Run(() => Drain(channel.Reader, ct, 300), ct);

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await call(channel.Writer, true).AsTask().WaitAsync(Timeout, ct));
		(List<Row> items, Exception? error) = await consumer.WaitAsync(Timeout, ct);

		Assert.Same(failure, thrown);
		Assert.Same(failure, error);
		AssertInOrder(items, FailAfter);
		Assert.True(probe.Reader.IsDisposed);
	}

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_WhenTheReaderFailsMidStream_CompleteFalse_EveryRowReadBeforeItIsDelivered_AndTheTargetStaysOpen(Kind kind)
	{
		const int FailAfter = 3000;
		CancellationToken ct = TestContext.Current.CancellationToken;
		var failure = new InvalidOperationException("db");
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(5000), FailAfter, failure), ct);

		var items = new List<Row>();
		var consumer = Task.Run(async () =>
		{
			for (int i = 0; i < FailAfter; i++)
			{
				items.Add(await channel.Reader.ReadAsync(ct));
				Thread.SpinWait(300);
			}
		}, ct);

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await call(channel.Writer, false).AsTask().WaitAsync(Timeout, ct));
		await consumer.WaitAsync(Timeout, ct);

		Assert.Same(failure, thrown);
		AssertInOrder(items, FailAfter);
		Assert.True(channel.Writer.TryWrite(new Row()), "complete: false must leave the target open");
	}

	// ---- preconditions: nothing is completed, nothing is started ----

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_WhenTheTargetIsAlreadyClosed_Throws_WithoutExecutingTheCommand(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		channel.Writer.TryComplete();
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(3)), ct, ConnectionState.Closed);

		await Assert.ThrowsAsync<ChannelClosedException>(async () => await call(channel.Writer, true));

		Assert.Equal(0, probe.Executions);
	}

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_WhenAlreadyCancelled_Throws_LeavesTheTargetAlone_AndDoesNotExecute(Kind kind)
	{
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(3)), new CancellationToken(true), ConnectionState.Closed);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await call(channel.Writer, true));

		Assert.Equal(0, probe.Executions);
		Assert.False(channel.Reader.Completion.IsCompleted, "a call that never started must not complete the target");
	}

	[Fact]
	public static async Task Mapped_InvalidArguments_Throw_AndLeaveTheTargetAlone()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		Func<DbDataReader> factory = probe.Factory(Rows(3));
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		ChannelWriter<Row> writer = channel.Writer;

		(IDbCommand dbCommand, IDbConnection dbConnection) = NewDbCommand(factory, ConnectionState.Open);
		FakeDbCommand fakeCommand = NewFakeDbCommand(factory, ConnectionState.Open);
		IExecuteReader executeReader = new ScriptedCommand(factory, false, default);
		IExecuteReaderAsync executeReaderAsync = new ScriptedCommand(factory, false, default);

		await Assert.ThrowsAsync<ArgumentNullException>(async () => await ((IDbCommand)null!).ToChannel(writer, true, ct));
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await ((DbCommand)null!).ToChannelAsync(writer, true, ct));
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await ((IExecuteReader)null!).ToChannel(writer, true));
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await ((IExecuteReaderAsync)null!).ToChannelAsync(writer, true));
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await dbCommand.ToChannel<Row>(null!, true, ct));
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await fakeCommand.ToChannelAsync<Row>(null!, true, ct));
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await executeReader.ToChannel<Row>(null!, true));
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await executeReaderAsync.ToChannelAsync<Row>(null!, true));

		dbCommand.Connection.Returns((IDbConnection?)null);
		fakeCommand.Connection = null;
		await Assert.ThrowsAsync<InvalidOperationException>(async () => await dbCommand.ToChannel(writer, true, ct));
		await Assert.ThrowsAsync<InvalidOperationException>(async () => await fakeCommand.ToChannelAsync(writer, true, ct));

		Assert.Equal(0, probe.Executions);
		Assert.False(channel.Reader.Completion.IsCompleted, "a call that never started must not complete the target");
		GC.KeepAlive(dbConnection);
	}
}
