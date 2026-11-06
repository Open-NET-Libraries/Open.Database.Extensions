using System.Data.Common;
using BoundedChannel = System.Threading.Channels.Channel;
using Kind = Open.Database.Extensions.Tests.ToChannelMappedCompletionTests.Kind;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Deterministic pins for the structure of the pipeline. None of these depend on load or timing luck: the reader's release,
// the target's capacity and the consumer are all held by gates, so a correct implementation cannot fail and a broken one cannot pass.
public static partial class ToChannelPipelineTests
{
	/// <summary>A target whose writes can be admitted by permits, and which records how and when it was completed.</summary>
	sealed class GateWriter<T>(ChannelWriter<T> inner, bool gated = false) : ChannelWriter<T>
	{
		readonly SemaphoreSlim _permits = new(gated ? 0 : 1_000_000_000, int.MaxValue);
		int _completeCalls;
		int _attempts;

		public int CompleteCalls => Volatile.Read(ref _completeCalls);
		public Exception? CompletedWith { get; private set; }
		public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public void Allow(int count) => _permits.Release(count);
		public void AllowAll() => _permits.Release(1_000_000_000);

		public override bool TryWrite(T item)
		{
			Interlocked.Increment(ref _attempts);
			if (!_permits.Wait(0)) return false;
			if (inner.TryWrite(item)) return true;
			_permits.Release();
			return false;
		}

		public override async ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
		{
			// Asking whether the target is open for writing before there is anything to write (as some of the overloads do before
			// they even start the command) is not a write.
			if (Volatile.Read(ref _attempts) > 0)
			{
				await _permits.WaitAsync(cancellationToken).ConfigureAwait(false);
				_permits.Release();
			}

			return await inner.WaitToWriteAsync(cancellationToken).ConfigureAwait(false);
		}

		public override bool TryComplete(Exception? error = null)
		{
			Interlocked.Increment(ref _completeCalls);
			CompletedWith ??= error;
			Completed.TrySetResult();
			return inner.TryComplete(error);
		}
	}

	/// <summary>The mapped ToChannel call for the given command type and overload shape.</summary>
	static Func<ChannelWriter<T>, bool, ValueTask<long>> Mapped<T>(
		Kind kind, bool overrides, Func<DbDataReader> factory,
		CancellationToken token, ConnectionState state = ConnectionState.Open,
		RowMap<T>? mapping = null)
		where T : new()
	{
		RowMap<T> map = mapping ?? new([new("Id", "Id")]);
		switch (kind)
		{
			case Kind.ExecuteReader:
				{
					IExecuteReader command = new ScriptedCommand(factory, false, token);
					return (w, c) => overrides ? command.ToChannel(w, c, map) : command.ToChannel(w, c);
				}

			case Kind.ExecuteReaderAsync:
			case Kind.ExecuteReaderAsyncUsingAsyncRead:
				{
					IExecuteReaderAsync command = new ScriptedCommand(factory, kind == Kind.ExecuteReaderAsyncUsingAsyncRead, token);
					return (w, c) => overrides ? command.ToChannelAsync(w, c, map) : command.ToChannelAsync(w, c);
				}

			case Kind.DbCommandInterface:
				{
					(IDbCommand command, _) = NewDbCommand(factory, state);
					return (w, c) => overrides ? command.ToChannel(w, c, map, token) : command.ToChannel(w, c, token);
				}

			default:
				{
					FakeDbCommand command = NewFakeDbCommand(factory, state);
					return (w, c) => overrides ? command.ToChannelAsync(w, c, map, token) : command.ToChannelAsync(w, c, token);
				}
		}
	}

	/// <summary>Waits until the value has stopped changing for a good while (something downstream has backed up to it).</summary>
	static async Task WaitUntilStable(Func<int> read, CancellationToken cancellationToken)
	{
		int last = -1;
		int stableFor = 0;
		for (int i = 0; i < 400; i++)
		{
			await Task.Delay(50, cancellationToken);
			int now = read();
			if (now == last && now > 0)
			{
				if (++stableFor >= 6) return;
			}
			else
			{
				stableFor = 0;
				last = now;
			}
		}

		throw new TimeoutException("The value never settled.");
	}

	// ---- rows already read are never lost ----

	// Open.ChannelExtensions' WriteAll starts writing a row without awaiting it and reads the next row first; when that read throws, the
	// write that is still waiting for room is abandoned. With a full buffer that is the last row read, lost without any error.
	// Here the target admits nothing, so the buffer is full and stage 1 is stalled; then exactly one slot is opened and the next read
	// fails. Every row the reader handed out must still reach the target, followed by the reader's own error.
	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_WhenTheReaderFailsWhileTheBufferIsFull_EveryRowItHandedOutIsStillDelivered(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var failure = new InvalidOperationException("db");
		var probe = new Probe();
		var channel = BoundedChannel.CreateUnbounded<Row>();
		var target = new GateWriter<Row>(channel.Writer, gated: true);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(5000)), ct);

		Task<(List<Row> Items, Exception? Error)> consumer = Task.Run(() => Drain(channel.Reader, ct), ct);
		Task<long> task = Task.Run(() => call(target, true).AsTask(), ct); // Off this thread: a gated release must not block the test itself.
		SpyReader reader = await probe.Created.WaitAsync(Prompt, ct);

		await WaitUntilStable(() => reader.RowsRead, ct);
		int stalledAt = reader.RowsRead;
		Assert.True(stalledAt > 1024, "the buffer should have filled up"); // Otherwise this isn't testing what it is meant to.

		reader.ArmFailure(failure);
		target.Allow(1); // One slot opens up in the buffer, stage 1 moves on and runs into the failure.

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await task.WaitAsync(Prompt, ct));
		Assert.Same(failure, thrown);
		Assert.Equal(0, target.CompleteCalls); // The target is still waiting for the rows in the buffer.

		target.AllowAll();
		(List<Row> items, Exception? error) = await consumer.WaitAsync(Timeout, ct);

		Assert.Same(failure, error);
		AssertInOrder(items, reader.RowsRead);
		Assert.True(reader.RowsRead >= stalledAt);
	}

	// ---- the target is never completed before the reader has been released ----

	public enum Ending
	{
		Success,
		MappingError,
		CallerCancel,
		ConsumerClosesTarget,
		ReaderFailsMidStream,
	}

	public static TheoryData<Kind, Ending> ReleaseOrderCases { get; } = BuildReleaseOrderCases();

	static TheoryData<Kind, Ending> BuildReleaseOrderCases()
	{
		var data = new TheoryData<Kind, Ending>();
		foreach (Kind kind in Enum.GetValues<Kind>())
		{
			foreach (Ending ending in Enum.GetValues<Ending>())
				data.Add(kind, ending);
		}

		return data;
	}

	// The reader's release is held up by a gate. Whatever ended the pipeline - normally, a mapping error, the caller cancelling, the
	// consumer closing the target, the database failing - nothing may complete the target until the reader has really been released.
	[Theory]
	[MemberData(nameof(ReleaseOrderCases))]
	public static async Task TheTarget_IsNotCompleted_BeforeTheReaderHasBeenReleased(Kind kind, Ending ending)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var failure = new InvalidOperationException("db");
		var probe = new Probe();
		var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var allDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		object?[][] rows = ending switch
		{
			Ending.Success => Rows(300),
			Ending.MappingError => Rows(5000, "not a number"),
			_ => Rows(5000),
		};
		int expected = ending switch { Ending.Success => 300, Ending.ReaderFailsMidStream => 3000, _ => -1 };

		Func<DbDataReader> factory = probe.Factory(
			rows,
			ending == Ending.ReaderFailsMidStream ? 3000 : -1,
			ending == Ending.ReaderFailsMidStream ? failure : null,
			reader => reader.DisposeGate = release);
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var target = new GateWriter<Row>(channel.Writer, gated: ending == Ending.CallerCancel); // Gated: everything backs up to the reader.

		Task<(List<Row> Items, Exception? Error)> consumer = Task.Run(async () =>
		{
			var items = new List<Row>();
			try
			{
				await foreach (Row row in channel.Reader.ReadAllAsync(ct))
				{
					items.Add(row);
					if (items.Count == expected) allDelivered.TrySetResult();
					if (ending != Ending.ConsumerClosesTarget || items.Count < 3) continue;

					channel.Writer.TryComplete();
					break;
				}

				return (items, (Exception?)null);
			}
			catch (Exception ex)
			{
				return (items, ex);
			}
		}, ct);

		Task<long> task = Task.Run(() => Mapped<Row>(kind, false, factory, cts.Token)(target, true).AsTask(), ct); // Off this thread: a gated release must not block the test itself.
		SpyReader reader = await probe.Created.WaitAsync(Prompt, ct);

		if (ending == Ending.CallerCancel)
		{
			await WaitUntilStable(() => reader.RowsRead, ct);
			cts.Cancel();
		}

		await reader.DisposeStarted.Task.WaitAsync(Prompt, ct); // Stage 1 is done: the reader is being released, and held.
		if (expected > 0) await allDelivered.Task.WaitAsync(Prompt, ct);
		await Task.Delay(300, ct); // Whatever might (wrongly) complete the target has had its chance by now.

		Assert.Multiple(
			() => Assert.Equal(0, target.CompleteCalls),
			() => Assert.False(reader.IsDisposed));

		release.SetResult();
		await reader.Disposed.WaitAsync(Prompt, ct);
		await target.Completed.Task.WaitAsync(Prompt, ct);

		Exception? thrown = null;
		try
		{
			await task.WaitAsync(Prompt, ct);
		}
		catch (Exception ex)
		{
			thrown = ex;
		}

		(List<Row> delivered, _) = await consumer.WaitAsync(Prompt, ct);
		switch (ending)
		{
			case Ending.Success:
				Assert.Null(thrown);
				Assert.Null(target.CompletedWith);
				AssertInOrder(delivered, 300);
				break;

			case Ending.MappingError:
				Assert.Contains("Unable to set value", thrown?.Message);
				Assert.Contains("Unable to set value", target.CompletedWith?.Message);
				break;

			case Ending.CallerCancel:
				Assert.IsType<OperationCanceledException>(thrown, exactMatch: false);
				Assert.IsType<OperationCanceledException>(target.CompletedWith, exactMatch: false);
				break;

			case Ending.ConsumerClosesTarget:
				Assert.IsType<ChannelClosedException>(thrown);
				break;

			default:
				Assert.Same(failure, thrown);
				Assert.Same(failure, target.CompletedWith);
				AssertInOrder(delivered, 3000);
				break;
		}
	}

	// ---- stopping stage 1 ----

	// A reader with a column that isn't mapped, so records are read by ordinal, with the stopping token handed down to every read.
	static readonly string[] ExtraThenId = ["Extra", "Id"];

	static object?[][] RowsWithAnExtraColumn(int count)
		=> [.. Enumerable.Range(0, count).Select(i => new object?[] { "junk", i })];

	// Stage 2 fails (here: when it is told to) while stage 1 is inside the provider, which can report the cancellation as
	// anything (here an ObjectDisposedException). The caller must be told why the pipeline stopped, not that.
	[Theory]
	[InlineData(Kind.ExecuteReaderAsyncUsingAsyncRead)]
	[InlineData(Kind.DbCommand)]
	public static async Task Mapped_WhenStageOneStopsWithAnyErrorBecauseStageTwoFailed_TheMappingFailureIsReported(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var insideProvider = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(RowsWithAnExtraColumn(5000), configure: reader => reader.BeforeReadAsync = async (rows, token) =>
		{
			if (rows < 3) return;

			insideProvider.TrySetResult();
			try
			{
				await Task.Delay(-1, token);
			}
			catch (OperationCanceledException)
			{
				throw new ObjectDisposedException("reader");
			}
		}, columns: ExtraThenId), ct);

		Row.OnMapped = () =>
		{
			insideProvider.Task.Wait(Prompt);
			throw new FormatException("cannot map");
		};

		try
		{
			Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call(channel.Writer, true).AsTask().WaitAsync(Prompt, ct));

			Assert.IsNotType<TimeoutException>(thrown);
			Assert.IsNotType<ObjectDisposedException>(thrown);
			Assert.Contains("Unable to set value", thrown.Message);
		}
		finally
		{
			Row.OnMapped = null;
		}
	}

	// A callback registered on the token stage 1 observes may throw when that token is cancelled. That must not keep the target
	// from ending.
	[Theory]
	[InlineData(Kind.ExecuteReaderAsyncUsingAsyncRead)]
	[InlineData(Kind.DbCommand)]
	public static async Task Mapped_WhenCancellingStageOneThrows_TheTargetStillEnds(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var registered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(RowsWithAnExtraColumn(5000), configure: reader => reader.BeforeReadAsync = async (rows, token) =>
		{
			if (rows < 3) return;

			token.Register(() => throw new FormatException("a callback that throws"));
			registered.TrySetResult();
			await Task.Delay(-1, token);
		}, columns: ExtraThenId), ct);

		Row.OnMapped = () =>
		{
			registered.Task.Wait(Prompt);
			throw new FormatException("cannot map");
		};

		try
		{
			Task<(List<Row> Items, Exception? Error)> consumer = Task.Run(() => Drain(channel.Reader, ct), ct);
			await Assert.ThrowsAnyAsync<Exception>(async () => await call(channel.Writer, true).AsTask().WaitAsync(Prompt, ct));
			(_, Exception? error) = await consumer.WaitAsync(Prompt, ct);

			Assert.Contains("Unable to set value", error?.Message);
		}
		finally
		{
			Row.OnMapped = null;
		}
	}

	// ---- the point of the exercise: get in, get out ----

	// Mapping is the second stage's job. If it ran inline on the thread that is reading, the reader would stay open for the
	// time to read plus the time to map every row. Even with a consumer that keeps up, no mapping may run inside a write of stage 1.
	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapping_NeverRunsInline_InsideTheWritesOfTheReadingStage(Kind kind)
	{
		const int Count = 3000;
		CancellationToken ct = TestContext.Current.CancellationToken;
		int mapped = 0;
		int inline = 0;

		Row.OnMapped = () =>
		{
			if (Interlocked.Increment(ref mapped) % 10 != 0) return;

			// A continuation that runs synchronously inside the channel write that woke it has that write on the stack.
			string stack = new System.Diagnostics.StackTrace(false).ToString();
			if (stack.Contains("TryWrite") || stack.Contains("WriteAsync"))
				Interlocked.Increment(ref inline);
		};

		try
		{
			var probe = new Probe();
			var channel = BoundedChannel.CreateUnbounded<Row>();
			var call = Mapped<Row>(kind, false, probe.Factory(Rows(Count)), ct);

			Task<(List<Row> Items, Exception? Error)> consumer = Task.Run(() => Drain(channel.Reader, ct), ct);
			await call(channel.Writer, true).AsTask().WaitAsync(Timeout, ct);
			(List<Row> items, Exception? error) = await consumer.WaitAsync(Timeout, ct);

			Assert.Null(error);
			AssertInOrder(items, Count);
		}
		finally
		{
			Row.OnMapped = null;
		}

		Assert.True(mapped >= Count);
		Assert.Equal(0, inline);
	}
}
