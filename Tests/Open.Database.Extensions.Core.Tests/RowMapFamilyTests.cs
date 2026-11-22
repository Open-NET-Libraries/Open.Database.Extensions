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

	};

	public static TheoryData<string> Names => [.. Cases.Keys];

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task RenamedColumns_AreMappedThroughTheOverload(string name)
		=> RowMapTests.AssertItems(await Cases[name](RowMapTests.Plain), 2);
}
