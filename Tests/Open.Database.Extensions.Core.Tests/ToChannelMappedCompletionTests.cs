using System.Data.Common;
using System.Threading.Channels;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A bounded channel with the default FullMode (Wait) never drops a write: the producer waits for room.
// So whatever the producer reads from the database must reach the consumer, however slow the consumer is, and
// the target must not be completed until everything has been written to it.
//
// The mapped ToChannel<T>/ToChannelAsync<T> overloads read the database into a buffer (stage 1) and a second stage
// maps the buffered rows into the target. These tests use only default channel options (no drop mode) and a
// consumer much slower than the reader, for every command type and both overload shapes (with/without overrides).
[ExcludeFromCodeCoverage]
public static class ToChannelMappedCompletionTests
{
	const int Count = 5000;
	const int TargetCapacity = 10;
	const int SpinPerItem = 500;
	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

	static readonly string[] Columns = ["Id"];
	static readonly (string Field, string? Column)[] Overrides = [("Id", "Id")];

	sealed class Row
	{
		public int Id { get; set; }
	}

	public enum Kind
	{
		ExecuteReader,
		ExecuteReaderAsync,
		ExecuteReaderAsyncUsingAsyncRead,
		DbCommandInterface,
		DbCommand,
	}

	public static TheoryData<Kind, bool> Shapes { get; } = BuildShapes();

	static TheoryData<Kind, bool> BuildShapes()
	{
		var data = new TheoryData<Kind, bool>();
		foreach (Kind kind in Enum.GetValues<Kind>())
		{
			data.Add(kind, false);
			data.Add(kind, true);
		}

		return data;
	}

	public static TheoryData<Kind> Kinds { get; } = [.. Enum.GetValues<Kind>()];

	static object?[][] Rows(int count, object? value = null)
	{
		var rows = new object?[count][];
		for (int i = 0; i < count; i++) rows[i] = [value ?? i];
		return rows;
	}

	// An IExecuteReaderAsync that counts nothing and can be told to fail instead of producing a reader.
	sealed class ScriptedCommand(object?[][] rows, Exception? failure, bool useAsyncRead) : IExecuteReaderAsync
	{
		public bool UseAsyncRead => useAsyncRead;
		public CancellationToken CancellationToken => default;

		DbDataReader Begin()
		{
			if (failure is not null) throw failure;
			return FakeReader.CreateDb(Columns, rows);
		}

		public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			using DbDataReader reader = Begin();
			handler(reader);
		}

		public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			using DbDataReader reader = Begin();
			return transform(reader);
		}

		public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			await using DbDataReader reader = Begin();
			await handler(reader).ConfigureAwait(false);
		}

		public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			await using DbDataReader reader = Begin();
			return await transform(reader).ConfigureAwait(false);
		}
	}

	/// <summary>Builds the mapped ToChannel call for the given command type and overload shape.</summary>
	static Func<ChannelWriter<Row>, bool, ValueTask<long>> Mapped(Kind kind, bool overrides, object?[][] rows, Exception? failure = null)
	{
		switch (kind)
		{
			case Kind.ExecuteReader:
			{
				IExecuteReader command = new ScriptedCommand(rows, failure, false);
				return (w, c) => overrides ? command.ToChannel(w, c, Overrides) : command.ToChannel(w, c);
			}

			case Kind.ExecuteReaderAsync:
			case Kind.ExecuteReaderAsyncUsingAsyncRead:
			{
				IExecuteReaderAsync command = new ScriptedCommand(rows, failure, kind == Kind.ExecuteReaderAsyncUsingAsyncRead);
				return (w, c) => overrides ? command.ToChannelAsync(w, c, Overrides) : command.ToChannelAsync(w, c);
			}

			case Kind.DbCommandInterface:
			{
				var command = Substitute.For<IDbCommand>();
				var connection = Substitute.For<IDbConnection>();
				command.Connection.Returns(connection);
				connection.State.Returns(ConnectionState.Open);
				command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ => failure is null
					? FakeReader.Create(Columns, rows)
					: throw failure);
				return (w, c) => overrides ? command.ToChannel(w, c, Overrides) : command.ToChannel(w, c);
			}

			default:
			{
				var connection = new FakeDbConnection();
				connection.SetState(ConnectionState.Open);
				var command = new FakeDbCommand(connection, () => failure is null
					? FakeReader.CreateDb(Columns, rows)
					: throw failure);
				return (w, c) => overrides ? command.ToChannelAsync(w, c, Overrides) : command.ToChannelAsync(w, c);
			}
		}
	}

	static async Task<int> SlowConsumeAll<T>(ChannelReader<T> reader, CancellationToken cancellationToken)
	{
		int received = 0;
		await foreach (T _ in reader.ReadAllAsync(cancellationToken))
		{
			received++;
			Thread.SpinWait(SpinPerItem);
		}

		return received;
	}

	// Records how many rows had been written to the target at the moment something completed it.
	sealed class SpyWriter<T>(ChannelWriter<T> inner) : ChannelWriter<T>
	{
		int _written;
		public int Written => Volatile.Read(ref _written);
		public int? WrittenAtComplete { get; private set; }

		public override bool TryWrite(T item)
		{
			bool ok = inner.TryWrite(item);
			if (ok) Interlocked.Increment(ref _written);
			return ok;
		}

		public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
			=> inner.WaitToWriteAsync(cancellationToken);

		public override bool TryComplete(Exception? error = null)
		{
			bool completed = inner.TryComplete(error);

			// Once completed, further writes fail, so Written is now the exact number written before completion.
			WrittenAtComplete ??= Written;
			return completed;
		}
	}

	// ---- control: the unmapped (single stage) path with the same channel and consumer loses nothing ----

	[Fact]
	public static async Task Unmapped_BoundedChannel_SlowConsumer_DeliversEveryRow()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var channel = System.Threading.Channels.Channel.CreateBounded<object[]>(TargetCapacity);
		IExecuteReader cmd = new FakeExecuteReader(Columns, Rows(Count));

		Task<int> consumer = Task.Run(() => SlowConsumeAll(channel.Reader, ct), ct);
		long produced = await cmd.ToChannel(channel.Writer, true).AsTask().WaitAsync(Timeout, ct);
		int received = await consumer.WaitAsync(Timeout, ct);

		Assert.Multiple(
			() => Assert.Equal(Count, produced),
			() => Assert.Equal(Count, received));
	}

	// ---- the mapped paths ----

	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_CompleteTrue_SlowConsumer_DeliversEveryRow_AndCompletesOnlyAfterTheLast(Kind kind, bool overrides)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var channel = System.Threading.Channels.Channel.CreateBounded<Row>(TargetCapacity);
		var spy = new SpyWriter<Row>(channel.Writer);
		var call = Mapped(kind, overrides, Rows(Count));

		Task<int> consumer = Task.Run(() => SlowConsumeAll(channel.Reader, ct), ct);
		long produced = await call(spy, true).AsTask().WaitAsync(Timeout, ct);
		int received = await consumer.WaitAsync(Timeout, ct);

		Assert.Multiple(
			() => Assert.Equal(Count, produced), // The reader read them all...
			() => Assert.Equal(Count, received), // ...so the consumer must get them all...
			() => Assert.Equal(Count, spy.WrittenAtComplete)); // ...and nothing may complete the target before the last row is in.
	}

	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task Mapped_CompleteFalse_DeliversEveryRow_AndLeavesTheTargetOpen(Kind kind, bool overrides)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var channel = System.Threading.Channels.Channel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped(kind, overrides, Rows(Count));

		// Without completion there is no end of stream to read to, so read exactly the rows expected.
		Task<int> consumer = Task.Run(async () =>
		{
			for (int i = 0; i < Count; i++)
			{
				await channel.Reader.ReadAsync(ct);
				Thread.SpinWait(SpinPerItem);
			}

			return Count;
		}, ct);

		long produced = await call(channel.Writer, false).AsTask().WaitAsync(Timeout, ct);
		int received = await consumer.WaitAsync(Timeout, ct);

		Assert.Multiple(
			() => Assert.Equal(Count, produced),
			() => Assert.Equal(Count, received),
			() => Assert.True(channel.Writer.TryWrite(new Row()), "complete: false must leave the target open"));
	}

	// ---- failures ----

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_ExecutionFails_CompleteTrue_FaultsTheTarget(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var failure = new InvalidOperationException("boom");
		var channel = System.Threading.Channels.Channel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped(kind, false, Rows(3), failure);

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await call(channel.Writer, true));
		var faulted = await Assert.ThrowsAsync<InvalidOperationException>(async () => await channel.Reader.Completion.WaitAsync(Timeout, ct));

		Assert.Same(failure, thrown);
		Assert.Same(failure, faulted);
	}

	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_ExecutionFails_CompleteFalse_LeavesTheTargetOpen(Kind kind)
	{
		var failure = new InvalidOperationException("boom");
		var channel = System.Threading.Channels.Channel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped(kind, false, Rows(3), failure);

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await call(channel.Writer, false));

		Assert.Same(failure, thrown);
		Assert.True(channel.Writer.TryWrite(new Row()), "complete: false must not complete the target, even on failure");
	}

	// A row that cannot be mapped fails the second stage. With complete: true the consumer must see that failure
	// as the end of the stream rather than waiting forever (or seeing a clean end after missing rows).
	[Theory]
	[MemberData(nameof(Kinds))]
	public static async Task Mapped_MappingFails_CompleteTrue_FaultsTheTarget(Kind kind)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var channel = System.Threading.Channels.Channel.CreateBounded<Row>(TargetCapacity);
		var call = Mapped(kind, false, Rows(3, "not a number"));

		Task<int> consumer = Task.Run(() => SlowConsumeAll(channel.Reader, ct), ct);
		try
		{
			await call(channel.Writer, true).AsTask().WaitAsync(Timeout, ct);
		}
		catch (InvalidOperationException)
		{
			// The database is read on one task while the rows are mapped on another: if the mapping fails before the
			// reading has finished, reading is stopped and the call reports that failure. Either way the target must end faulted.
		}

		await Assert.ThrowsAnyAsync<Exception>(async () => await consumer.WaitAsync(Timeout, ct));
		Assert.True(channel.Reader.Completion.IsFaulted, "the target must end faulted, not cleanly");
	}
}
