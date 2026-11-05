using System.Data.Common;
using System.Threading.Channels;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Argument checks, the closed-connection wait, a closed writer, and failures, for each of the four
// ToChannelCore/ToChannelAsyncCore copies (one per command type). Together with ToChannelCompleteFlagTests these
// cover every line and branch of those methods.
[ExcludeFromCodeCoverage]
public static class ToChannelCoreEdgeTests
{
	static readonly string[] Columns = ["Id"];
	static readonly object?[][] ThreeRows = [[1], [2], [3]];

	public enum Kind { ExecuteReader, ExecuteReaderAsync, DbCommandInterface, DbCommand }

	/// <summary>One command of a given kind, plus a way to run its raw ToChannel and count how often it executed.</summary>
	sealed class Subject(Func<ChannelWriter<object[]>, bool, ValueTask<long>> run, Func<int> executions)
	{
		public ValueTask<long> Run(ChannelWriter<object[]> writer, bool complete) => run(writer, complete);
		public int Executions => executions();
	}

	// An IExecuteReaderAsync that can be told to fail; it counts executions and hands out a three-row reader.
	sealed class ScriptedCommand(Exception? failure) : IExecuteReaderAsync
	{
		public int Executions;
		public bool UseAsyncRead => false;
		public CancellationToken CancellationToken => default;

		DbDataReader Begin()
		{
			Executions++;
			if (failure is not null) throw failure;
			return FakeReader.CreateDb(Columns, ThreeRows);
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

		public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReader<TResult>(reader => transform(reader, state), behavior);

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReaderAsync<TResult>(reader => transform(reader, state), behavior);

		public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			await using DbDataReader reader = Begin();
			return await transform(reader).ConfigureAwait(false);
		}
	}

	static Subject Build(Kind kind, Exception? failure = null, bool connectionOpen = true, bool hasConnection = true)
	{
		switch (kind)
		{
			case Kind.ExecuteReader:
			{
				var command = new ScriptedCommand(failure);
				return new Subject((w, c) => ((IExecuteReader)command).ToChannel(w, c), () => command.Executions);
			}

			case Kind.ExecuteReaderAsync:
			{
				var command = new ScriptedCommand(failure);
				return new Subject((w, c) => command.ToChannelAsync(w, c), () => command.Executions);
			}

			case Kind.DbCommandInterface:
			{
				var command = Substitute.For<IDbCommand>();
				var connection = Substitute.For<IDbConnection>();
				int executions = 0;
				command.Connection.Returns(hasConnection ? connection : null);
				connection.State.Returns(connectionOpen ? ConnectionState.Open : ConnectionState.Closed);
				command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ =>
				{
					executions++;
					if (failure is not null) throw failure;
					return FakeReader.Create(Columns, ThreeRows);
				});
				return new Subject((w, c) => command.ToChannel(w, c), () => executions);
			}

			default:
			{
				var connection = new FakeDbConnection();
				connection.SetState(connectionOpen ? ConnectionState.Open : ConnectionState.Closed);
				var command = new FakeDbCommand(hasConnection ? connection : null!, () => failure is null
					? FakeReader.CreateDb(Columns, ThreeRows)
					: throw failure);
				return new Subject((w, c) => command.ToChannelAsync(w, c), () => command.Behaviors.Count);
			}
		}
	}

	static Channel<object[]> NewChannel() => System.Threading.Channels.Channel.CreateUnbounded<object[]>();

	// ---- argument checks ----

	[Theory]
	[InlineData(Kind.ExecuteReader)]
	[InlineData(Kind.ExecuteReaderAsync)]
	[InlineData(Kind.DbCommandInterface)]
	[InlineData(Kind.DbCommand)]
	public static async Task NullCommand_Throws(Kind kind)
	{
		ChannelWriter<object[]> writer = NewChannel().Writer;
		Func<Task> act = kind switch
		{
			Kind.ExecuteReader => async () => await ((IExecuteReader)null!).ToChannel(writer, true),
			Kind.ExecuteReaderAsync => async () => await ((IExecuteReaderAsync)null!).ToChannelAsync(writer, true),
			Kind.DbCommandInterface => async () => await ((IDbCommand)null!).ToChannel(writer, true),
			_ => async () => await ((DbCommand)null!).ToChannelAsync(writer, true),
		};

		var ex = await Assert.ThrowsAsync<ArgumentNullException>(act);
		Assert.Equal("command", ex.ParamName);
	}

	[Theory]
	[InlineData(Kind.ExecuteReader)]
	[InlineData(Kind.ExecuteReaderAsync)]
	[InlineData(Kind.DbCommandInterface)]
	[InlineData(Kind.DbCommand)]
	public static async Task NullWriter_Throws_AndNeverExecutes(Kind kind)
	{
		Subject subject = Build(kind);

		var ex = await Assert.ThrowsAsync<ArgumentNullException>(async () => await subject.Run(null!, true));

		Assert.Equal("writer", ex.ParamName);
		Assert.Equal(0, subject.Executions);
	}

	[Theory]
	[InlineData(Kind.DbCommandInterface)]
	[InlineData(Kind.DbCommand)]
	public static async Task CommandWithoutConnection_Throws_AndNeverExecutes(Kind kind)
	{
		Subject subject = Build(kind, hasConnection: false);

		await Assert.ThrowsAsync<InvalidOperationException>(async () => await subject.Run(NewChannel().Writer, true));

		Assert.Equal(0, subject.Executions);
	}

	// ---- the writer / connection gate ----

	[Theory]
	[InlineData(Kind.ExecuteReader, true)]
	[InlineData(Kind.ExecuteReaderAsync, true)]
	[InlineData(Kind.DbCommandInterface, false)] // These two only check the writer up front when the connection is not already open.
	[InlineData(Kind.DbCommand, false)]
	public static async Task ClosedWriter_IsRejectedBeforeAnythingExecutes(Kind kind, bool connectionOpen)
	{
		Subject subject = Build(kind, connectionOpen: connectionOpen);
		Channel<object[]> channel = NewChannel();
		channel.Writer.Complete();

		await Assert.ThrowsAsync<ChannelClosedException>(async () => await subject.Run(channel.Writer, true));

		Assert.Equal(0, subject.Executions);
	}

	[Theory]
	[InlineData(Kind.DbCommandInterface)]
	[InlineData(Kind.DbCommand)]
	public static async Task ClosedConnection_WaitsForTheWriter_ThenExecutesOnce(Kind kind)
	{
		Subject subject = Build(kind, connectionOpen: false);
		Channel<object[]> channel = NewChannel();

		long n = await subject.Run(channel.Writer, true);

		Assert.Equal(3, n);
		Assert.Equal(1, subject.Executions);
		Assert.False(channel.Writer.TryWrite([99]), "complete: true completes the writer");
	}

	// ---- failures ----

	[Theory]
	[InlineData(Kind.ExecuteReader)]
	[InlineData(Kind.ExecuteReaderAsync)]
	[InlineData(Kind.DbCommandInterface)]
	[InlineData(Kind.DbCommand)]
	public static async Task Failure_WithComplete_PropagatesAndFaultsTheChannel(Kind kind)
	{
		var failure = new InvalidOperationException("boom");
		Subject subject = Build(kind, failure);
		Channel<object[]> channel = NewChannel();

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await subject.Run(channel.Writer, true));

		Assert.Same(failure, thrown);
		Assert.Equal(1, subject.Executions);
		var faulted = await Assert.ThrowsAsync<InvalidOperationException>(async () => await channel.Reader.Completion);
		Assert.Same(failure, faulted);
	}

	[Theory]
	[InlineData(Kind.ExecuteReader)]
	[InlineData(Kind.ExecuteReaderAsync)]
	[InlineData(Kind.DbCommandInterface)]
	[InlineData(Kind.DbCommand)]
	public static async Task Failure_WithoutComplete_PropagatesAndLeavesTheWriterOpen(Kind kind)
	{
		var failure = new InvalidOperationException("boom");
		Subject subject = Build(kind, failure);
		Channel<object[]> channel = NewChannel();

		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await subject.Run(channel.Writer, false));

		Assert.Same(failure, thrown);
		Assert.Equal(1, subject.Executions);
		Assert.True(channel.Writer.TryWrite([99]), "complete: false must not complete the writer, even on failure");
	}
}
