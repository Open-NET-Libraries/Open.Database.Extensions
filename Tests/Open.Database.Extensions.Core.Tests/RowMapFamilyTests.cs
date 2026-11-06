using System.Data.Common;
using System.Threading.Tasks.Dataflow;
using Open.Database.Extensions.Core;
using Item = Open.Database.Extensions.Tests.RowMapTests.Item;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Every override-taking public method through each receiver, once with a map and once with the tuple convenience overload.
// The columns are renamed, so a wrapper that swaps Field and Column, or does not forward its map, fails here.
[ExcludeFromCodeCoverage]
public static class RowMapFamilyTests
{
	[Fact]
	public static void ExecuteReaderResults_WithoutAMap_ReadsByColumnName()
	{
		var command = new FakeExecuteReader(["Id", "Name"], [1, "one"]);

		Assert.Equal(1, command.Results<RowMapTests.Item>().Single().Id);
		Assert.Equal(1, command.Results<RowMapTests.Item>(null).Single().Id);
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

		public ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReaderAsync(transform, behavior);
	}

	static readonly (string, string?)[] Tuples =
		[(nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")];

	static object?[][] Rows => [RowMapTests.Row(RowMapTests.Renamed, 1), RowMapTests.Row(RowMapTests.Renamed, 2)];

	// ---- receivers ----
	static IDataReader Reader() => FakeReader.Create(RowMapTests.Renamed, Rows);
	static DbDataReader DbReader() => FakeReader.CreateDb(RowMapTests.Renamed, Rows);
	static FakeExecuteReader Exec() => RowMapTests.Cmd(RowMapTests.Renamed, 2);
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

	static DbCommand DbCmd()
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

	// name -> (through a map, through the tuple convenience overload)
	static readonly Dictionary<string, (Func<RowMap<Item>, Task<List<Item>>> ViaMap, Func<(string, string?)[], Task<List<Item>>> ViaTuples)> Cases = new()
	{
		// Core
		["Map(IExecuteReader)"] = (m => Now(Exec().Map<Item>(m).ToList()), t => Now(Exec().Map<Item>(t).ToList())),
		["Results(IDataReader)"] = (m => Now(Reader().Results<Item>(m)), t => Now(Reader().Results<Item>(t))),
		["ResultsBuffered(IDataReader)"] = (m => Now(Reader().ResultsBuffered<Item>(m)), t => Now(Reader().ResultsBuffered<Item>(t))),
		["Results(IDbCommand)"] = (m => Now(Command().Results<Item>(m)), t => Now(Command().Results<Item>(t))),
		["Results(IExecuteReader)"] = (m => Now(Exec().Results<Item>(m)), t => Now(Exec().Results<Item>(t))),
		["ResultsAsync(DbDataReader)"] = (m => Enumerate(DbReader().ResultsAsync<Item>(m)), t => Enumerate(DbReader().ResultsAsync<Item>(t))),
		["ResultsBufferedAsync(DbDataReader)"] = (async m => (await DbReader().ResultsBufferedAsync<Item>(m)).ToList(), async t => (await DbReader().ResultsBufferedAsync<Item>(t)).ToList()),
		["ResultsAsync(DbCommand)"] = (async m => (await DbCmd().ResultsAsync<Item>(m)).ToList(), async t => (await DbCmd().ResultsAsync<Item>(t)).ToList()),
		["To(DataTable)"] = (m => Now(Table().To<Item>(m)), t => Now(Table().To<Item>(t))),
		["DequeueAs(QueryResult)"] = (m => Now(Queued().DequeueAs<Item>(m)), t => Now(Queued().DequeueAs<Item>(t))),
		["DequeueAs(Task)"] = (async m => (await Task.FromResult(Queued()).DequeueAs<Item>(m)).ToList(), async t => (await Task.FromResult(Queued()).DequeueAs<Item>(t)).ToList()),
		["DequeueAs(ValueTask)"] = (async m => (await new ValueTask<QueryResult<Queue<object?[]>>>(Queued()).DequeueAs<Item>(m)).ToList(), async t => (await new ValueTask<QueryResult<Queue<object?[]>>>(Queued()).DequeueAs<Item>(t)).ToList()),

		// Channel: a new channel
		["ToChannel(IDataReader)"] = (m => Drain(Reader().ToChannel<Item>(true, m)), t => Drain(Reader().ToChannel<Item>(true, t))),
		["ToChannel(IDbCommand)"] = (m => Drain(Command().ToChannel<Item>(true, m)), t => Drain(Command().ToChannel<Item>(true, t))),
		["ToChannel(IExecuteReader)"] = (m => Drain(Exec().ToChannel<Item>(true, m)), t => Drain(Exec().ToChannel<Item>(true, t))),
		["ToChannelAsync(DbDataReader)"] = (m => Drain(DbReader().ToChannelAsync<Item>(true, m)), t => Drain(DbReader().ToChannelAsync<Item>(true, t))),
		["ToChannelAsync(DbCommand)"] = (m => Drain(DbCmd().ToChannelAsync<Item>(true, m)), t => Drain(DbCmd().ToChannelAsync<Item>(true, t))),
		["ToChannelAsync(IExecuteReaderAsync)"] = (m => Drain(Async().ToChannelAsync<Item>(true, m)), t => Drain(Async().ToChannelAsync<Item>(true, t))),

		// Channel: the caller's writer
		["ToChannel(writer, IDataReader)"] = (m => ToWriter(w => Reader().ToChannel(w, true, m)), t => ToWriter(w => Reader().ToChannel(w, true, t))),
		["ToChannel(writer, IDbCommand)"] = (m => ToWriter(w => Command().ToChannel(w, true, m)), t => ToWriter(w => Command().ToChannel(w, true, t))),
		["ToChannel(writer, IExecuteReader)"] = (m => ToWriter(w => Exec().ToChannel(w, true, m)), t => ToWriter(w => Exec().ToChannel(w, true, t))),
		["ToChannelAsync(writer, DbDataReader)"] = (m => ToWriter(w => DbReader().ToChannelAsync(w, true, m)), t => ToWriter(w => DbReader().ToChannelAsync(w, true, t))),
		["ToChannelAsync(writer, DbCommand)"] = (m => ToWriter(w => DbCmd().ToChannelAsync(w, true, m)), t => ToWriter(w => DbCmd().ToChannelAsync(w, true, t))),
		["ToChannelAsync(writer, IExecuteReaderAsync)"] = (m => ToWriter(w => Async().ToChannelAsync(w, true, m)), t => ToWriter(w => Async().ToChannelAsync(w, true, t))),

		// Dataflow: into a block
		["ToTargetBlock(IDataReader)"] = (m => ToBlock(b => new ValueTask<long>(Reader().ToTargetBlock(b, true, m))), t => ToBlock(b => new ValueTask<long>(Reader().ToTargetBlock(b, true, t)))),
		["ToTargetBlock(IDbCommand)"] = (m => ToBlock(b => new ValueTask<long>(Command().ToTargetBlock(b, true, m))), t => ToBlock(b => new ValueTask<long>(Command().ToTargetBlock(b, true, t)))),
		["ToTargetBlock(IExecuteReader)"] = (m => ToBlock(b => new ValueTask<long>(Exec().ToTargetBlock(b, true, m))), t => ToBlock(b => new ValueTask<long>(Exec().ToTargetBlock(b, true, t)))),
		["ToTargetBlockAsync(IDataReader)"] = (m => ToBlock(b => Reader().ToTargetBlockAsync(b, true, m)), t => ToBlock(b => Reader().ToTargetBlockAsync(b, true, t))),
		["ToTargetBlockAsync(IDbCommand)"] = (m => ToBlock(b => Command().ToTargetBlockAsync(b, true, m)), t => ToBlock(b => Command().ToTargetBlockAsync(b, true, t))),
		["ToTargetBlockAsync(IExecuteReader)"] = (m => ToBlock(b => Exec().ToTargetBlockAsync(b, true, m)), t => ToBlock(b => Exec().ToTargetBlockAsync(b, true, t))),

		// Dataflow: a new source block
		["AsSourceBlock(IDataReader)"] = (m => Receive(Reader().AsSourceBlock<Item>(m)), t => Receive(Reader().AsSourceBlock<Item>(t))),
		["AsSourceBlock(IDbCommand)"] = (m => Receive(Command().AsSourceBlock<Item>(m)), t => Receive(Command().AsSourceBlock<Item>(t))),
		["AsSourceBlock(IExecuteReader)"] = (m => Receive(Exec().AsSourceBlock<Item>(m)), t => Receive(Exec().AsSourceBlock<Item>(t))),
		["AsSourceBlockAsync(IDataReader)"] = (m => Receive(Reader().AsSourceBlockAsync<Item>(m)), t => Receive(Reader().AsSourceBlockAsync<Item>(t))),
		["AsSourceBlockAsync(IDbCommand)"] = (m => Receive(Command().AsSourceBlockAsync<Item>(m)), t => Receive(Command().AsSourceBlockAsync<Item>(t))),
		["AsSourceBlockAsync(IExecuteReader)"] = (m => Receive(Exec().AsSourceBlockAsync<Item>(m)), t => Receive(Exec().AsSourceBlockAsync<Item>(t))),
	};

	public static TheoryData<string> Names => [.. Cases.Keys];

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task RenamedColumns_AreMappedThroughTheOverload_WithAMapAndWithTuples(string name)
	{
		var (viaMap, viaTuples) = Cases[name];
		RowMapTests.AssertItems(await viaMap(RowMapTests.Plain), 2);
		RowMapTests.AssertItems(await viaTuples(Tuples), 2);
	}
}
