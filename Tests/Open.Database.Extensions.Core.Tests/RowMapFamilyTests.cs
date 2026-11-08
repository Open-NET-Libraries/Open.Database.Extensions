using Open.Database.Extensions.Core;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;
using Item = Open.Database.Extensions.Tests.RowMapTests.Item;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Every override-taking public method through each receiver, through a map.
// The columns are renamed, so a wrapper that swaps Field and Column, or does not forward its map, fails here.
[ExcludeFromCodeCoverage]
public static class RowMapFamilyTests
{
	[Fact]
	public static void ExecuteReaderResults_WithoutAMap_ReadsByColumnName()
	{
		var command = new FakeExecuteReader(["Id", "Name"], [1, "one"]);

		Assert.Equal(1, command.Results<Item>().Single().Id);
		Assert.Equal(1, command.Results((RowMap<Item>?)null).Single().Id);
	}

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

	static object?[][] Rows => [RowMapTests.Row(RowMapTests.Renamed, 1), RowMapTests.Row(RowMapTests.Renamed, 2)];

	// ---- receivers ----
	static IDataReader Reader() => FakeReader.Create(RowMapTests.Renamed, Rows);
	static DbDataReader DbReader() => FakeReader.CreateDb(RowMapTests.Renamed, Rows);
	static FakeExecuteReader Exec() => RowMapTests.Cmd(RowMapTests.Renamed, 2);

	[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
	static AsyncCommand Async() => new(Exec());

	static IDbCommand Command()
	{
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		command.Connection.Returns(connection);
		connection.State.Returns(ConnectionState.Open);
		command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ => FakeReader.Create(RowMapTests.Renamed, Rows));
		return command;
	}

	static FakeDbCommand DbCmd()
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		return new FakeDbCommand(connection, () => FakeReader.CreateDb(RowMapTests.Renamed, Rows));
	}

	static DataTable Table()
	{
		var table = new DataTable();
		foreach (string name in RowMapTests.Renamed) table.Columns.Add(name, name == "item_id" ? typeof(int) : name == "item_total" ? typeof(decimal) : typeof(string));
		foreach (object?[] row in Rows) table.Rows.Add([.. row.Select(v => v ?? DBNull.Value)]);
		return table;
	}

	static QueryResult<Queue<object?[]>> Queued()
		=> new([0, 1, 2, 3], [.. RowMapTests.Renamed], new Queue<object?[]>(Rows));

	// ---- draining what each family produces ----
	static async Task<List<Item>> Drain(ChannelReader<Item> reader)
	{
		var items = new List<Item>();
		await foreach (Item item in reader.ReadAllAsync()) items.Add(item);
		return items;
	}

	static async Task<List<Item>> ToWriter(Func<ChannelWriter<Item>, ValueTask<long>> run)
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<Item>();
		Assert.Equal(2, await run(channel.Writer));
		return await Drain(channel.Reader);
	}

	static async Task<List<Item>> Receive(IReceivableSourceBlock<Item> block, int count = 2)
	{
		var items = new List<Item>();
		for (int i = 0; i < count; i++) items.Add(await block.ReceiveAsync(TimeSpan.FromSeconds(10)));
		return items;
	}

	static async Task<List<Item>> ToBlock(Func<BufferBlock<Item>, ValueTask<long>> run)
	{
		var block = new BufferBlock<Item>();
		Assert.Equal(2, await run(block));
		return await Receive(block);
	}

	static async Task<List<Item>> Enumerate(IAsyncEnumerable<Item> source)
	{
		var items = new List<Item>();
		await foreach (Item item in source) items.Add(item);
		return items;
	}

	static Task<List<Item>> Now(IEnumerable<Item> items) => Task.FromResult(items.ToList());

	// name -> the call, through a map
	static readonly Dictionary<string, Func<RowMap<Item>, Task<List<Item>>>> Cases = new()
	{
		// Core
		["Map(IExecuteReader)"] = m => Now(Exec().Map(m).ToList()),
		["Results(IDataReader)"] = m => Now(Reader().Results(m)),
		["ResultsBuffered(IDataReader)"] = m => Now(Reader().ResultsBuffered(m)),
		["Results(IDbCommand)"] = m => Now(Command().Results(m)),
		["Results(IExecuteReader)"] = m => Now(Exec().Results(m)),
		["ResultsAsync(DbDataReader)"] = m => Enumerate(DbReader().ResultsAsync(m)),
		["ResultsBufferedAsync(DbDataReader)"] = async m => (await DbReader().ResultsBufferedAsync(m)).ToList(),
		["ResultsAsync(DbCommand)"] = async m => (await DbCmd().ResultsAsync(m)).ToList(),
		["To(DataTable)"] = m => Now(Table().To(m)),
		["DequeueAs(QueryResult)"] = m => Now(Queued().DequeueAs(m)),
		["DequeueAs(Task)"] = async m => (await Task.FromResult(Queued()).DequeueAs(m)).ToList(),
		["DequeueAs(ValueTask)"] = async m => (await new ValueTask<QueryResult<Queue<object?[]>>>(Queued()).DequeueAs(m)).ToList(),

		// Channel: a new channel
		["ToChannel(IDataReader)"] = m => Drain(Reader().ToChannel(true, m)),
		["ToChannel(IDbCommand)"] = m => Drain(Command().ToChannel(true, m)),
		["ToChannel(IExecuteReader)"] = m => Drain(Exec().ToChannel(true, m)),
		["ToChannelAsync(DbDataReader)"] = m => Drain(DbReader().ToChannelAsync(true, m)),
		["ToChannelAsync(DbCommand)"] = m => Drain(DbCmd().ToChannelAsync(true, m)),
		["ToChannelAsync(IExecuteReaderAsync)"] = m => Drain(Async().ToChannelAsync(true, m)),

		// Channel: the caller's writer
		["ToChannel(writer, IDataReader)"] = m => ToWriter(w => Reader().ToChannel(w, true, m)),
		["ToChannel(writer, IDbCommand)"] = m => ToWriter(w => Command().ToChannel(w, true, m)),
		["ToChannel(writer, IExecuteReader)"] = m => ToWriter(w => Exec().ToChannel(w, true, m)),
		["ToChannelAsync(writer, DbDataReader)"] = m => ToWriter(w => DbReader().ToChannelAsync(w, true, m)),
		["ToChannelAsync(writer, DbCommand)"] = m => ToWriter(w => DbCmd().ToChannelAsync(w, true, m)),
		["ToChannelAsync(writer, IExecuteReaderAsync)"] = m => ToWriter(w => Async().ToChannelAsync(w, true, m)),

		// Dataflow: into a block
		["ToTargetBlock(IDataReader)"] = m => ToBlock(b => new ValueTask<long>(Reader().ToTargetBlock(b, true, m))),
		["ToTargetBlock(IDbCommand)"] = m => ToBlock(b => new ValueTask<long>(Command().ToTargetBlock(b, true, m))),
		["ToTargetBlock(IExecuteReader)"] = m => ToBlock(b => new ValueTask<long>(Exec().ToTargetBlock(b, true, m))),
		["ToTargetBlockAsync(IDataReader)"] = m => ToBlock(b => Reader().ToTargetBlockAsync(b, true, m)),
		["ToTargetBlockAsync(IDbCommand)"] = m => ToBlock(b => Command().ToTargetBlockAsync(b, true, m)),
		["ToTargetBlockAsync(IExecuteReader)"] = m => ToBlock(b => Exec().ToTargetBlockAsync(b, true, m)),

		// Dataflow: a new source block
		["AsSourceBlock(IDataReader)"] = m => Receive(Reader().AsSourceBlock(m)),
		["AsSourceBlock(IDbCommand)"] = m => Receive(Command().AsSourceBlock(m)),
		["AsSourceBlock(IExecuteReader)"] = m => Receive(Exec().AsSourceBlock(m)),
		["AsSourceBlockAsync(IDataReader)"] = m => Receive(Reader().AsSourceBlockAsync(m)),
		["AsSourceBlockAsync(IDbCommand)"] = m => Receive(Command().AsSourceBlockAsync(m)),
		["AsSourceBlockAsync(IExecuteReader)"] = m => Receive(Exec().AsSourceBlockAsync(m)),
	};

	public static TheoryData<string> Names => [.. Cases.Keys];

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task RenamedColumns_AreMappedThroughTheOverload(string name)
		=> RowMapTests.AssertItems(await Cases[name](RowMapTests.Plain), 2);
}
