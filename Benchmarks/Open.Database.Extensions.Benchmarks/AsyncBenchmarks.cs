using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;

namespace Open.Database.Extensions.Benchmarks;

[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class AsyncBenchmarks
{
	const string Sql = "select * from posts";
	const string ParamSql = "select * from posts where id > @id";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	BenchConnection _conn = null!;

	[GlobalSetup]
	public void Setup() => _conn = new BenchConnection(BenchTable.Posts(Rows));

	[BenchmarkCategory("ListAsync"), Benchmark(Baseline = true)]
	public async Task<List<Post>> ListAsync_Dapper() => (await _conn.QueryAsync<Post>(Sql)).AsList();

	[BenchmarkCategory("ListAsync"), Benchmark]
	public async Task<List<Post>> ListAsync_ODE() => await _conn.Command(Sql).Map<Post>().ToListAsync();

	[BenchmarkCategory("ArrayAsync"), Benchmark(Baseline = true)]
	public async Task<Post[]> ArrayAsync_Dapper() => (await _conn.QueryAsync<Post>(Sql)).ToArray();

	[BenchmarkCategory("ArrayAsync"), Benchmark]
	public async Task<Post[]> ArrayAsync_ODE() => await _conn.Command(Sql).Map<Post>().ToArrayAsync();

	[BenchmarkCategory("FirstAsync"), Benchmark(Baseline = true)]
	public async Task<Post?> FirstAsync_Dapper() => await _conn.QueryFirstOrDefaultAsync<Post>(Sql);

	[BenchmarkCategory("FirstAsync"), Benchmark]
	public async Task<Post?> FirstAsync_ODE() => await _conn.Command(Sql).Map<Post>().FirstOrDefaultAsync();

	[BenchmarkCategory("ParamsAsync"), Benchmark(Baseline = true)]
	public async Task<List<Post>> ParamsAsync_Dapper() => (await _conn.QueryAsync<Post>(ParamSql, new { id = 0 })).AsList();

	[BenchmarkCategory("ParamsAsync"), Benchmark]
	public async Task<List<Post>> ParamsAsync_ODE() => await _conn.Command(ParamSql).AddParam("id", 0).Map<Post>().ToListAsync();

	[BenchmarkCategory("ScalarAsync"), Benchmark(Baseline = true)]
	public async Task<int> ScalarAsync_Dapper() => await _conn.ExecuteScalarAsync<int>(Sql);

	[BenchmarkCategory("ScalarAsync"), Benchmark]
	public async Task<int> ScalarAsync_ODE() => await _conn.Command(Sql).ExecuteScalarAsync<int>();

	[BenchmarkCategory("ExecuteAsync"), Benchmark(Baseline = true)]
	public async Task<int> ExecuteAsync_Dapper() => await _conn.ExecuteAsync(Sql);

	[BenchmarkCategory("ExecuteAsync"), Benchmark]
	public async Task<int> ExecuteAsync_ODE() => await _conn.Command(Sql).ExecuteNonQueryAsync();
}
