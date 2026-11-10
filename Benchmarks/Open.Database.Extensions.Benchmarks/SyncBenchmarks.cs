using Open.Database.Extensions.Core;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;

namespace Open.Database.Extensions.Benchmarks;

// Each category compares the same job done the Dapper way (the baseline) and the ODE way.
// Ratio < 1 means ODE is faster. The allocation columns are the tie-breaker.
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class SyncBenchmarks
{
	const string Sql = "select * from posts";
	const string ParamSql = "select * from posts where id > @id";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	BenchConnection _conn = null!, _one = null!;

	[GlobalSetup]
	public void Setup()
	{
		_conn = new BenchConnection(BenchTable.Posts(Rows));
		_one = new BenchConnection(BenchTable.Posts(1));
	}

	// ---- results ----------------------------------------------------------------
	[BenchmarkCategory("List"), Benchmark(Baseline = true)]
	public List<Post> List_Dapper() => _conn.Query<Post>(Sql).AsList();

	[BenchmarkCategory("List"), Benchmark]
	public List<Post> List_ODE() => _conn.Command(Sql).Map<Post>().ToList();

	[BenchmarkCategory("Array"), Benchmark(Baseline = true)]
	public Post[] Array_Dapper() => _conn.Query<Post>(Sql).ToArray();

	[BenchmarkCategory("Array"), Benchmark]
	public Post[] Array_ODE() => _conn.Command(Sql).Map<Post>().ToArray();

	[BenchmarkCategory("First"), Benchmark(Baseline = true)]
	public Post? First_Dapper() => _conn.QueryFirstOrDefault<Post>(Sql);

	[BenchmarkCategory("First"), Benchmark]
	public Post? First_ODE() => _conn.Command(Sql).Map<Post>().FirstOrDefault();

	[BenchmarkCategory("Stream"), Benchmark(Baseline = true)]
	public long Stream_Dapper()
	{
		long sum = 0;
		foreach (Post p in _conn.Query<Post>(Sql, buffered: false)) sum += p.Id;
		return sum;
	}

	[BenchmarkCategory("Stream"), Benchmark]
	public long Stream_ODE() => _conn.Command(Sql).Map<Post>().Read(static items =>
	{
		long sum = 0;
		foreach (Post p in items) sum += p.Id;
		return sum;
	});

	[BenchmarkCategory("Params"), Benchmark(Baseline = true)]
	public List<Post> Params_Dapper() => _conn.Query<Post>(ParamSql, new { id = 0 }).AsList();

	[BenchmarkCategory("Params"), Benchmark]
	public List<Post> Params_ODE() => _conn.Command(ParamSql).AddParam("id", 0).Map<Post>().ToList();

	[BenchmarkCategory("Single"), Benchmark(Baseline = true)]
	public Post? Single_Dapper() => _one.QuerySingleOrDefault<Post>(Sql);

	[BenchmarkCategory("Single"), Benchmark]
	public Post? Single_ODE() => _one.Command(Sql).Map<Post>().SingleOrDefault();

	[BenchmarkCategory("Column"), Benchmark(Baseline = true)]
	public List<int> Column_Dapper() => _conn.Query<int>(Sql).AsList();

	[BenchmarkCategory("Column"), Benchmark]
	public List<int> Column_ODE() => _conn.Command(Sql).FirstOrdinalResults<int>().ToList();

	// Rows without a model class: Dapper's dynamic rows versus ODE's object[] rows.
	[BenchmarkCategory("Untyped"), Benchmark(Baseline = true)]
	public int Untyped_Dapper() => _conn.Query(Sql).AsList().Count;

	[BenchmarkCategory("Untyped"), Benchmark]
	public int Untyped_ODE() => _conn.Command(Sql).Retrieve().Result.Count;

	// ODE's buffer-then-map route: Retrieve() copies object[] rows, DequeueAs<T>() maps them as they are drained.
	[BenchmarkCategory("QueryResult"), Benchmark(Baseline = true)]
	public List<Post> QueryResult_Dapper() => _conn.Query<Post>(Sql).AsList();

	[BenchmarkCategory("QueryResult"), Benchmark]
#pragma warning disable CS0618 // measures the deprecated Retrieve + DequeueAs route until 12
	public List<Post> QueryResult_ODE() => _conn.Command(Sql).Retrieve().DequeueAs<Post>().ToList();
#pragma warning restore CS0618

	[BenchmarkCategory("Dictionary"), Benchmark(Baseline = true)]
	public Dictionary<int, Post> Dictionary_Dapper() => _conn.Query<Post>(Sql).ToDictionary(p => p.Id);

	[BenchmarkCategory("Dictionary"), Benchmark]
	public Dictionary<int, Post> Dictionary_ODE() => _conn.Command(Sql).Map<Post>().ToDictionary(p => p.Id);

	// A hand-written projection, the way both libraries are used when a model class is not wanted.
	[BenchmarkCategory("Selector"), Benchmark(Baseline = true)]
	public List<(int, string)> Selector_Dapper()
	{
		var list = new List<(int, string)>();
		using var reader = _conn.ExecuteReader(Sql);
		while (reader.Read()) list.Add((reader.GetInt32(0), reader.GetString(1)));
		return list;
	}

	[BenchmarkCategory("Selector"), Benchmark]
	public List<(int, string)> Selector_ODE() => _conn.Command(Sql).Map(static r => (r.GetInt32(0), r.GetString(1))).ToList();

	// ---- the basics ----------------------------------------------------------------
	[BenchmarkCategory("Scalar"), Benchmark(Baseline = true)]
	public int Scalar_Dapper() => _conn.ExecuteScalar<int>(Sql);

	[BenchmarkCategory("Scalar"), Benchmark]
	public int Scalar_ODE() => _conn.Command(Sql).ExecuteScalar<int>();

	[BenchmarkCategory("Execute"), Benchmark(Baseline = true)]
	public int Execute_Dapper() => _conn.Execute(Sql);

	[BenchmarkCategory("Execute"), Benchmark]
	public int Execute_ODE() => _conn.Command(Sql).ExecuteNonQuery();
}
