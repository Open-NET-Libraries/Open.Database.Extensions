#nullable enable

namespace Open.Database.Extensions.Tests;

// What "complete" means for the command-level ToChannel producers, checked against each of the four
// ToChannelCore/ToChannelAsyncCore copies:
//   - the command must be executed exactly ONCE (the reader drains the result into the channel and stops);
//   - complete: false must leave the writer OPEN (the caller will write more or complete it later);
//   - complete: true must complete the writer after everything has been written.
[ExcludeFromCodeCoverage]
public static class ToChannelCompleteFlagTests
{
	static readonly string[] Columns = ["Id"];
	static readonly object?[][] ThreeRows = [[1], [2], [3]];

	// A writer is open iff a write is still accepted. (Reader.Completion only completes once drained, so it
	// can't tell "completed" from "completed but not yet read".)
	static bool WriterIsOpen(Channel<object[]> channel)
	{
		bool open = channel.Writer.TryWrite([99]);
		return open;
	}

	static async Task<int> DrainAfterCompleting(Channel<object[]> channel, CancellationToken cancellationToken)
	{
		channel.Writer.TryComplete();
		int count = 0;
		await foreach (object[] _ in channel.Reader.ReadAllAsync(cancellationToken)) count++;
		return count;
	}

	// ---- 1. IExecuteReader (ToChannelCore) ----

	[Fact]
	public static async Task IExecuteReader_CompleteFalse_ExecutesOnce_Delivers3Rows_LeavesWriterOpen()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		var cmd = new FakeExecuteReader(Columns, ThreeRows);

		long n = await cmd.ToChannel(
			channel.Writer,
			false,
			cancellationToken: TestContext.Current.CancellationToken);

		bool open = WriterIsOpen(channel); // This write adds a 4th item when the writer is open.
		int delivered = await DrainAfterCompleting(channel, TestContext.Current.CancellationToken);

		Assert.Multiple(
			() => Assert.Equal(1, cmd.ExecuteCount),
			() => Assert.Equal(3, n),
			() => Assert.True(open, "complete: false must leave the writer open"),
			() => Assert.Equal(4, delivered)); // 3 rows + the one the test writes to prove the writer is open
	}

	[Fact]
	public static async Task IExecuteReader_CompleteTrue_ExecutesOnce_Delivers3Rows_CompletesWriter()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		var cmd = new FakeExecuteReader(Columns, ThreeRows);

		long n = await cmd.ToChannel(
			channel.Writer,
			true,
			cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(1, cmd.ExecuteCount);
		Assert.Equal(3, n);
		Assert.False(WriterIsOpen(channel), "complete: true must complete the writer");
		Assert.Equal(3, await DrainAfterCompleting(channel, TestContext.Current.CancellationToken));
	}

	// ---- 2. IExecuteReaderAsync (ToChannelAsyncCore) ----

	sealed class AsyncCommand(FakeExecuteReader inner) : IExecuteReaderAsync
	{
		public bool UseAsyncRead => false;
		public CancellationToken CancellationToken => inner.CancellationToken;
		public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReader(handler, behavior);
		public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReader(transform, behavior);
		public ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReaderAsync(handler, behavior);
		public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReader(reader => transform(reader, state), behavior);

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReaderAsync(reader => transform(reader, state), behavior);

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken)
			=> ExecuteReaderAsync(state, transform, behavior);

		public ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReaderAsync(transform, behavior);
	}

	[Fact]
	public static async Task IExecuteReaderAsync_CompleteFalse_ExecutesOnce_Delivers3Rows_LeavesWriterOpen()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		var inner = new FakeExecuteReader(Columns, ThreeRows);

		long n = await new AsyncCommand(inner)
			.ToChannelAsync(
				channel.Writer,
				false,
				cancellationToken: TestContext.Current.CancellationToken);

		bool open = WriterIsOpen(channel);
		int delivered = await DrainAfterCompleting(channel, TestContext.Current.CancellationToken);

		Assert.Multiple(
			() => Assert.Equal(1, inner.ExecuteCount),
			() => Assert.Equal(3, n),
			() => Assert.True(open, "complete: false must leave the writer open"),
			() => Assert.Equal(4, delivered)); // 3 rows + the one the test writes to prove the writer is open
	}

	[Fact]
	public static async Task IExecuteReaderAsync_CompleteTrue_ExecutesOnce_CompletesWriter()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		var inner = new FakeExecuteReader(Columns, ThreeRows);

		long n = await new AsyncCommand(inner)
			.ToChannelAsync(
				channel.Writer,
				true,
				cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(1, inner.ExecuteCount);
		Assert.Equal(3, n);
		Assert.False(WriterIsOpen(channel));
	}

	// ---- 3. IDbCommand (ToChannelCore) ----

	static (IDbCommand Command, Func<int> Executions) CountingDbCommand()
	{
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		int executions = 0;
		command.Connection.Returns(connection);
		connection.State.Returns(ConnectionState.Open);
		command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ =>
		{
			executions++;
			return FakeReader.Create(Columns, ThreeRows);
		});
		return (command, () => executions);
	}

	[Fact]
	public static async Task IDbCommand_CompleteFalse_ExecutesOnce_Delivers3Rows_LeavesWriterOpen()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		(IDbCommand command, Func<int> executions) = CountingDbCommand();

		long n = await command.ToChannel(channel.Writer, false, TestContext.Current.CancellationToken);

		bool open = WriterIsOpen(channel);
		int delivered = await DrainAfterCompleting(channel, TestContext.Current.CancellationToken);

		Assert.Multiple(
			() => Assert.Equal(1, executions()),
			() => Assert.Equal(3, n),
			() => Assert.True(open, "complete: false must leave the writer open"),
			() => Assert.Equal(4, delivered)); // 3 rows + the one the test writes to prove the writer is open
	}

	[Fact]
	public static async Task IDbCommand_CompleteTrue_ExecutesOnce_CompletesWriter()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		(IDbCommand command, Func<int> executions) = CountingDbCommand();

		long n = await command.ToChannel(channel.Writer, true, TestContext.Current.CancellationToken);

		Assert.Equal(1, executions());
		Assert.Equal(3, n);
		Assert.False(WriterIsOpen(channel));
	}

	// ---- 4. DbCommand (ToChannelAsyncCore) ----

	static FakeDbCommand NewDbCommand()
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		return new FakeDbCommand(connection, () => FakeReader.CreateDb(Columns, ThreeRows));
	}

	[Fact]
	public static async Task DbCommand_CompleteFalse_ExecutesOnce_Delivers3Rows_LeavesWriterOpen()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		FakeDbCommand command = NewDbCommand();

		long n = await command.ToChannelAsync(channel.Writer, false, TestContext.Current.CancellationToken);

		bool open = WriterIsOpen(channel);
		int delivered = await DrainAfterCompleting(channel, TestContext.Current.CancellationToken);

		Assert.Multiple(
			() => Assert.Single(command.Behaviors),
			() => Assert.Equal(3, n),
			() => Assert.True(open, "complete: false must leave the writer open"),
			() => Assert.Equal(4, delivered)); // 3 rows + the one the test writes to prove the writer is open
	}

	[Fact]
	public static async Task DbCommand_CompleteTrue_ExecutesOnce_CompletesWriter()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		FakeDbCommand command = NewDbCommand();

		long n = await command.ToChannelAsync(channel.Writer, true, TestContext.Current.CancellationToken);

		Assert.Single(command.Behaviors);
		Assert.Equal(3, n);
		Assert.False(WriterIsOpen(channel));
	}
}
