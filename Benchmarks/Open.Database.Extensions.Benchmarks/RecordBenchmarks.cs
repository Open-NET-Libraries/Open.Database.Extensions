using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>The 12-column <see cref="Post"/> as a positional record: both libraries build it through its constructor.</summary>
public sealed record PostRecord(int Id, string Title, string? Body, DateTime Created, decimal Price, bool IsPublished, int? ViewCount, double Score, Guid Key, short Rank, long Big, string? Tag);

// A positional record read through its constructor: Dapper (the baseline) and Open.Database.Extensions on the same rows.
// Ratio < 1 means ODE is faster. The allocation columns are the tie-breaker.
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class RecordBenchmarks
{
	const string Sql = "select * from posts";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	BenchConnection _conn = null!;

	[GlobalSetup]
	public void Setup()
	{
		_conn = new BenchConnection(BenchTable.Posts(Rows));

		// Both libraries must return the same records, or the comparison measures nothing.
		List<PostRecord> dapper = RecordList_Dapper();
		List<PostRecord> ode = RecordList_ODE();
		if (dapper.Count != Rows || !dapper.SequenceEqual(ode))
			throw new InvalidOperationException("Dapper and Open.Database.Extensions returned different records.");
	}

	[BenchmarkCategory("RecordList"), Benchmark(Baseline = true)]
	public List<PostRecord> RecordList_Dapper() => _conn.Query<PostRecord>(Sql).AsList();

	[BenchmarkCategory("RecordList"), Benchmark]
	public List<PostRecord> RecordList_ODE() => _conn.Command(Sql).Map<PostRecord>().ToList();

	[BenchmarkCategory("RecordFirst"), Benchmark(Baseline = true)]
	public PostRecord? RecordFirst_Dapper() => _conn.QueryFirstOrDefault<PostRecord>(Sql);

	[BenchmarkCategory("RecordFirst"), Benchmark]
	public PostRecord? RecordFirst_ODE() => _conn.Command(Sql).Map<PostRecord>().FirstOrDefault();
}
