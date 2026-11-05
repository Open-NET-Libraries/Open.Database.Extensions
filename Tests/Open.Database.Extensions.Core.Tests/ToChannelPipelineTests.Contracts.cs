using System.Data.Common;
using System.Threading.Channels;
using BoundedChannel = System.Threading.Channels.Channel;
using Kind = Open.Database.Extensions.Tests.ToChannelMappedCompletionTests.Kind;

#nullable enable

namespace Open.Database.Extensions.Tests;

// What a caller can rely on at the edges: which failures are preconditions (the target is left alone) and which are failures of
// the operation (a target that is to be completed ends faulted), what the parameters are called, and how cancellation reads.
public static partial class ToChannelPipelineTests
{
	static readonly (string Field, string? Column)[] Unbindable = [("NoSuchProperty", "Id")];

	// ---- a field override that can't be bound ----

	// Master faulted the target here too (measured: with complete: true the target ended faulted with the exception, with
	// complete: false it stayed open, and the call threw). It is a failure of the operation, not a precondition.
	[Theory]
	[MemberData(nameof(KindsAndFlag))]
	public static async Task Mapped_FieldOverrideCannotBeBound_IsAFailure_TheTargetIsFaultedWhenComplete(Kind kind, bool complete)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var target = new GateWriter<Row>(channel.Writer);
		var call = Mapped<Row>(kind, true, probe.Factory(Rows(3)), ct, mapping: Unbindable);

		Exception thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await call(target, complete).AsTask().WaitAsync(Prompt, ct));

		Assert.IsType<KeyNotFoundException>(thrown);
		Assert.Equal(0, probe.Executions); // Nothing ran, so there is no reader that the target could be completed ahead of.
		if (complete)
		{
			Exception faulted = await Assert.ThrowsAnyAsync<Exception>(async () => await channel.Reader.Completion.WaitAsync(Prompt, ct));
			Assert.Same(thrown, faulted);
			Assert.Equal(1, target.CompleteCalls);
		}
		else
		{
			Assert.Equal(0, target.CompleteCalls);
			Assert.True(channel.Writer.TryWrite(new Row()), "complete: false must leave the target open");
		}
	}

	// ... while a precondition that fails first still leaves the target alone.
	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_FieldOverrideCannotBeBound_ButTheTargetIsClosed_ReportsTheClosedTarget(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var target = new GateWriter<Row>(channel.Writer);
		channel.Writer.TryComplete();
		var call = Mapped<Row>(kind, true, probe.Factory(Rows(3)), ct, ConnectionState.Closed, Unbindable);

		await Assert.ThrowsAsync<ChannelClosedException>(async () => await call(target, true));

		Assert.Equal(0, target.CompleteCalls);
		Assert.Equal(0, probe.Executions);
	}

	// The reader-level calls have no command: they throw at once and never touch the target (as they did).
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void ReaderLevel_FieldOverrideCannotBeBound_ThrowsAtOnce_AndLeavesTheTargetAlone(bool useAsync)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		SpyReader reader = (SpyReader)new Probe().Factory(Rows(1))();
		Func<ValueTask<long>> call = useAsync
			? () => reader.ToChannelAsync(channel.Writer, true, Unbindable, ct)
			: () => reader.ToChannel(channel.Writer, true, Unbindable, ct);

		Assert.Throws<KeyNotFoundException>(() => { _ = call(); });

		Assert.False(channel.Reader.Completion.IsCompleted);
		Assert.False(reader.IsDisposed);
	}

	// ---- parameter names ----

	[Fact]
	public static async Task Mapped_NullArguments_NameTheParameter()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		Func<DbDataReader> factory = new Probe().Factory(Rows(1));
		ChannelWriter<Row> writer = BoundedChannel.CreateBounded<Row>(TargetCapacity).Writer;
		(IDbCommand dbCommand, _) = NewDbCommand(factory, ConnectionState.Open);
		FakeDbCommand fakeCommand = NewFakeDbCommand(factory, ConnectionState.Open);
		IExecuteReader executeReader = new ScriptedCommand(factory, false, ct);
		IExecuteReaderAsync executeReaderAsync = new ScriptedCommand(factory, false, ct);

		static async Task Names(string expected, Func<ValueTask<long>> call)
		{
			var thrown = await Assert.ThrowsAsync<ArgumentNullException>(async () => await call());
			Assert.Equal(expected, thrown.ParamName);
		}

		// No command.
		await Names("command", () => ((IDbCommand)null!).ToChannel(writer, true, ct));
		await Names("command", () => ((IDbCommand)null!).ToChannel(writer, true, Overrides, ct));
		await Names("command", () => ((DbCommand)null!).ToChannelAsync(writer, true, ct));
		await Names("command", () => ((DbCommand)null!).ToChannelAsync(writer, true, Overrides, ct));
		await Names("command", () => ((IExecuteReader)null!).ToChannel(writer, true));
		await Names("command", () => ((IExecuteReader)null!).ToChannel(writer, true, Overrides));
		await Names("command", () => ((IExecuteReaderAsync)null!).ToChannelAsync(writer, true));
		await Names("command", () => ((IExecuteReaderAsync)null!).ToChannelAsync(writer, true, Overrides));

		// No target.
		await Names("target", () => dbCommand.ToChannel<Row>(null!, true, ct));
		await Names("target", () => dbCommand.ToChannel<Row>(null!, true, Overrides, ct));
		await Names("target", () => fakeCommand.ToChannelAsync<Row>(null!, true, ct));
		await Names("target", () => fakeCommand.ToChannelAsync<Row>(null!, true, Overrides, ct));
		await Names("target", () => executeReader.ToChannel<Row>(null!, true));
		await Names("target", () => executeReader.ToChannel<Row>(null!, true, Overrides));
		await Names("target", () => executeReaderAsync.ToChannelAsync<Row>(null!, true));
		await Names("target", () => executeReaderAsync.ToChannelAsync<Row>(null!, true, Overrides));
	}

	// ---- cancellation reads the same whichever way the records are read ----

	// The token is cancelled as the reader reports that there is nothing left to read, with every record already handed over:
	// the read is complete, and the call says so.
	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_CancelledAsTheReaderReportsTheEnd_ReturnsTheCount(Kind kind)
	{
		const int Count = 50;
		CancellationToken ct = TestContext.Current.CancellationToken;
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		Func<DbDataReader> factory = probe.Factory(Rows(Count), configure: reader => reader.AfterRead = (_, more) =>
		{
			if (more) return;

			delivered.Task.Wait(Prompt); // Every record has reached the consumer...
			cts.Cancel(); // ...when the token is cancelled, as the reader says there are no more.
		});
		var call = Mapped<Row>(kind, false, factory, cts.Token);

		Task consumer = Task.Run(async () =>
		{
			int received = 0;
			try
			{
				await foreach (Row _ in channel.Reader.ReadAllAsync(ct))
					if (++received == Count) delivered.TrySetResult();
			}
			catch (OperationCanceledException)
			{
				// The consumer may or may not see the end of the stream as the cancellation, depending on when it looks.
			}
		}, ct);

		long produced = await Task.Run(() => call(channel.Writer, true).AsTask(), ct).WaitAsync(Prompt, ct);
		await consumer.WaitAsync(Prompt, ct);

		Assert.Equal(Count, produced);
	}

	// Cancelled while records remain: the call ends with the cancellation.
	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_CancelledWhileRecordsRemain_IsCancelled(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(300), configure: reader => reader.AfterRead = (rows, more) =>
		{
			if (more && rows == 10) cts.Cancel();
		}), cts.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await Task.Run(() => call(channel.Writer, true).AsTask(), ct).WaitAsync(Prompt, ct));
		await probe.Reader.Disposed.WaitAsync(Prompt, ct);
	}

	// A token that is cancelled from the start: records remain, so the call is cancelled and the target ends.
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ReaderLevel_AlreadyCancelled_IsCancelled_AndTheTargetEnds(bool useAsync)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		SpyReader reader = (SpyReader)new Probe().Factory(Rows(50))();
		var cancelled = new CancellationToken(true);

		Func<ValueTask<long>> call = useAsync
			? () => reader.ToChannelAsync<Row>(channel.Writer, true, cancelled)
			: () => reader.ToChannel<Row>(channel.Writer, true, cancelled);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await call().AsTask().WaitAsync(Prompt, ct));
		(_, Exception? ended) = await Drain(channel.Reader, ct).WaitAsync(Prompt, ct);

		Assert.NotNull(ended);
		Assert.False(reader.IsDisposed, "the caller's reader belongs to the caller");
	}

	// With the connection already open nothing gates on the target, so the command may start; either way the call is cancelled,
	// the target ends, and a reader that was opened is released.
	[Theory]
	[InlineData(Kind.DbCommandInterface)]
	[InlineData(Kind.DbCommand)]
	public static async Task Mapped_AlreadyCancelled_WithAnOpenConnection_IsCancelled_AndTheTargetEnds(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped<Row>(kind, false, probe.Factory(Rows(3)), new CancellationToken(true));

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await call(channel.Writer, true).AsTask().WaitAsync(Prompt, ct));
		(_, Exception? ended) = await Drain(channel.Reader, ct).WaitAsync(Prompt, ct);

		Assert.NotNull(ended);
		if (probe.Executions > 0)
			await probe.Reader.Disposed.WaitAsync(Prompt, ct);
	}
	[Fact]
	public static void ReaderLevel_NullArguments_NameTheParameter()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		ChannelWriter<Row> writer = BoundedChannel.CreateBounded<Row>(TargetCapacity).Writer;
		SpyReader reader = (SpyReader)new Probe().Factory(Rows(1))();

		static void Names(string expected, Action call)
		{
			var thrown = Assert.Throws<ArgumentNullException>(call);
			Assert.Equal(expected, thrown.ParamName);
		}

		Names("reader", () => ((IDataReader)null!).ToChannel<Row>(writer, true, ct));
		Names("reader", () => ((IDataReader)null!).ToChannel(writer, true, Overrides, ct));
		Names("reader", () => ((DbDataReader)null!).ToChannelAsync<Row>(writer, true, ct));
		Names("reader", () => ((DbDataReader)null!).ToChannelAsync(writer, true, Overrides, ct));
		Names("target", () => reader.ToChannel<Row>(null!, true, ct));
		Names("target", () => reader.ToChannel<Row>(null!, true, Overrides, ct));
		Names("target", () => reader.ToChannelAsync<Row>(null!, true, ct));
		Names("target", () => reader.ToChannelAsync<Row>(null!, true, Overrides, ct));
	}

	// No overrides is no overrides, however it is spelled.
	[Fact]
	public static async Task Mapped_NullFieldMappingOverrides_MeanNone()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var none = (IEnumerable<(string Field, string? Column)>)null!;
		Func<DbDataReader> factory = new Probe().Factory(Rows(3));
		(IDbCommand dbCommand, _) = NewDbCommand(factory, ConnectionState.Open);
		FakeDbCommand fakeCommand = NewFakeDbCommand(factory, ConnectionState.Open);
		IExecuteReader executeReader = new ScriptedCommand(factory, false, ct);
		IExecuteReaderAsync executeReaderAsync = new ScriptedCommand(factory, false, ct);
		ChannelWriter<Row> writer = BoundedChannel.CreateUnbounded<Row>().Writer;

		Assert.Equal(3, await dbCommand.ToChannel(writer, false, none, ct));
		Assert.Equal(3, await fakeCommand.ToChannelAsync(writer, false, none, ct));
		Assert.Equal(3, await executeReader.ToChannel(writer, false, none));
		Assert.Equal(3, await executeReaderAsync.ToChannelAsync(writer, false, none));
	}

	// Whatever is wrong with the overrides, the answer is never a NullReferenceException.
	[Fact]
	public static async Task Mapped_ANullFieldInAnOverride_IsNotANullReference()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var oddly = new (string Field, string? Column)[] { (null!, "Id") };
		Func<DbDataReader> factory = new Probe().Factory(Rows(3));
		(IDbCommand dbCommand, _) = NewDbCommand(factory, ConnectionState.Open);
		IExecuteReader executeReader = new ScriptedCommand(factory, false, ct);
		ChannelWriter<Row> writer = BoundedChannel.CreateUnbounded<Row>().Writer;

		Exception? thrown = null;
		try
		{
			await dbCommand.ToChannel(writer, false, oddly, ct);
			await executeReader.ToChannel(writer, false, oddly);
		}
		catch (Exception ex)
		{
			thrown = ex;
		}

		Assert.IsNotType<NullReferenceException>(thrown);
	}
}
