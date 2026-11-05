using System.Threading.Tasks.Dataflow;
using System.Data;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Benchmarks;

// The features that sit beside Map<T>: DataTable loading, transactions and channels.
// Dapper has no direct counterpart for these, so the Dapper side is the idiomatic hand-written equivalent.

/// <summary>DataTable and transaction scenarios run on SQLite because both need a real provider (schema table, real transactions).</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class SqliteExtrasBenchmarks
{
	const string Sql = "select Id, Title, Score from items";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	SqliteConnection _conn = null!;

	[GlobalSetup]
	public void Setup()
	{
		_conn = new SqliteConnection("Data Source=:memory:");
		_conn.Open();
		_conn.Execute("create table items (Id integer not null, Title text not null, Score real not null)");
		using var tx = _conn.BeginTransaction();
		for (int i = 1; i <= Rows; i++)
			_conn.Execute("insert into items values (@Id,@Title,@Score)", new { Id = i, Title = "Item " + i, Score = i * 1.5 }, tx);
		tx.Commit();
	}

	[GlobalCleanup]
	public void Cleanup() => _conn.Dispose();

	[BenchmarkCategory("DataTable"), Benchmark(Baseline = true)]
	public DataTable DataTable_Dapper()
	{
		var table = new DataTable();
		using var reader = _conn.ExecuteReader(Sql);
		table.Load(reader);
		return table;
	}

	[BenchmarkCategory("DataTable"), Benchmark]
	public DataTable DataTable_ODE() => _conn.Command(Sql).ExecuteReader(reader => reader.ToDataTable());

	[BenchmarkCategory("Transaction"), Benchmark(Baseline = true)]
	public int Transaction_Dapper()
	{
		using var tx = _conn.BeginTransaction();
		int n = _conn.Execute("update items set Score = Score where Id = @id", new { id = 1 }, tx);
		tx.Commit();
		return n;
	}

	[BenchmarkCategory("Transaction"), Benchmark]
	public int Transaction_ODE()
		=> _conn.ExecuteTransaction(tx => tx.Command("update items set Score = Score where Id = @id").AddParam("id", 1).ExecuteNonQuery());
}

[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class ChannelBenchmarks
{
	const string Sql = "select * from posts";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	BenchConnection _conn = null!;

	[GlobalSetup]
	public void Setup() => _conn = new BenchConnection(BenchTable.Posts(Rows));

	// Produce every row into a channel and drain it: the shape ToChannel is for.
	[BenchmarkCategory("Channel"), Benchmark(Baseline = true)]
	public async Task<long> Channel_Dapper()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<Post>(new UnboundedChannelOptions { SingleReader = true });
		var producer = Task.Run(async () =>
		{
			await foreach (Post p in _conn.QueryUnbufferedAsync<Post>(Sql))
				await channel.Writer.WriteAsync(p);
			channel.Writer.Complete();
		});

		long n = 0;
		await foreach (Post _ in channel.Reader.ReadAllAsync()) n++;
		await producer;
		return n;
	}

	[BenchmarkCategory("Channel"), Benchmark]
	public async Task<long> Channel_ODE()
	{
		long n = 0;
		await foreach (Post _ in _conn.Command(Sql).ToChannel<Post>(true).ReadAllAsync()) n++;
		return n;
	}
}

[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class DataflowBenchmarks
{
	const string Sql = "select * from posts";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	BenchConnection _conn = null!;

	[GlobalSetup]
	public void Setup() => _conn = new BenchConnection(BenchTable.Posts(Rows));

	// Fill a BufferBlock with every mapped row and drain it.
	[BenchmarkCategory("Dataflow"), Benchmark(Baseline = true)]
	public async Task<long> Dataflow_Dapper()
	{
		var block = new BufferBlock<Post>();
		foreach (Post p in await _conn.QueryAsync<Post>(Sql))
			await block.SendAsync(p);
		block.Complete();

		long n = 0;
		while (await block.OutputAvailableAsync())
			while (block.TryReceive(null, out _)) n++;
		return n;
	}

	[BenchmarkCategory("Dataflow"), Benchmark]
	public async Task<long> Dataflow_ODE()
	{
		var block = new BufferBlock<Post>();
		await _conn.Command(Sql).ExecuteReaderAsync(reader => reader.ToTargetBlock(block, true));

		long n = 0;
		while (await block.OutputAvailableAsync())
			while (block.TryReceive(null, out _)) n++;
		return n;
	}
}
