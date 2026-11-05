using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Benchmarks;

// The same comparison against a real provider. SQLite is dynamically typed (integers arrive as long,
// dates/decimals/Guids as text, bool as integer), so this exercises the conversion paths the in-memory
// provider (which reports exact CLR types) never takes.
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class SqliteBenchmarks
{
	const string Sql = "select * from posts";
	const string ParamSql = "select * from posts where id > @id";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	SqliteConnection _conn = null!;

	[GlobalSetup]
	public void Setup()
	{
		_conn = new SqliteConnection("Data Source=:memory:");
		_conn.Open();
		_conn.Execute("""
			create table posts (
				Id integer not null, Title text not null, Body text null, Created text not null, Price text not null,
				IsPublished integer not null, ViewCount integer null, Score real not null, Key text not null,
				Rank integer not null, Big integer not null, Tag text null)
			""");

		using var tx = _conn.BeginTransaction();
		for (int i = 1; i <= Rows; i++)
		{
			_conn.Execute(
				"insert into posts values (@Id,@Title,@Body,@Created,@Price,@IsPublished,@ViewCount,@Score,@Key,@Rank,@Big,@Tag)",
				new
				{
					Id = i, Title = "SqlitePost number " + i, Body = i % 3 == 0 ? null : "Lorem ipsum " + i,
					Created = new DateTime(2024, 1, 1).AddMinutes(i), Price = 10m + i, IsPublished = i % 2 == 0,
					ViewCount = i % 4 == 0 ? (int?)null : i * 3, Score = i * 1.5, Key = Guid.NewGuid().ToString(),
					Rank = (short)(i % 100), Big = (long)i << 20, Tag = i % 2 == 0 ? null : "t" + (i % 7),
				}, tx);
		}

		tx.Commit();

		// Both libraries must produce identical objects, or the timings compare different work.
		var expected = List_Dapper();
		var actual = List_ODE();
		if (expected.Count != actual.Count || !expected.Zip(actual, Same).All(x => x))
			throw new InvalidOperationException("ODE and Dapper mapped the SQLite rows differently.");
	}

	static bool Same(SqlitePost a, SqlitePost b)
		=> a.Id == b.Id && a.Title == b.Title && a.Body == b.Body && a.Created == b.Created && a.Price == b.Price
			&& a.IsPublished == b.IsPublished && a.ViewCount == b.ViewCount && a.Score == b.Score && a.Key == b.Key
			&& a.Rank == b.Rank && a.Big == b.Big && a.Tag == b.Tag;

	[GlobalCleanup]
	public void Cleanup() => _conn.Dispose();

	[BenchmarkCategory("List"), Benchmark(Baseline = true)]
	public List<SqlitePost> List_Dapper() => _conn.Query<SqlitePost>(Sql).AsList();

	[BenchmarkCategory("List"), Benchmark]
	public List<SqlitePost> List_ODE() => _conn.Command(Sql).Map<SqlitePost>().ToList();

	[BenchmarkCategory("First"), Benchmark(Baseline = true)]
	public SqlitePost? First_Dapper() => _conn.QueryFirstOrDefault<SqlitePost>(Sql);

	[BenchmarkCategory("First"), Benchmark]
	public SqlitePost? First_ODE() => _conn.Command(Sql).Map<SqlitePost>().FirstOrDefault();

	[BenchmarkCategory("Params"), Benchmark(Baseline = true)]
	public List<SqlitePost> Params_Dapper() => _conn.Query<SqlitePost>(ParamSql, new { id = 0 }).AsList();

	[BenchmarkCategory("Params"), Benchmark]
	public List<SqlitePost> Params_ODE() => _conn.Command(ParamSql).AddParam("id", 0).Map<SqlitePost>().ToList();

	[BenchmarkCategory("Scalar"), Benchmark(Baseline = true)]
	public long Scalar_Dapper() => _conn.ExecuteScalar<long>("select count(*) from posts");

	[BenchmarkCategory("Scalar"), Benchmark]
	public long Scalar_ODE() => _conn.Command("select count(*) from posts").ExecuteScalar<long>();

	[BenchmarkCategory("ListAsync"), Benchmark(Baseline = true)]
	public async Task<List<SqlitePost>> ListAsync_Dapper() => (await _conn.QueryAsync<SqlitePost>(Sql)).AsList();

	[BenchmarkCategory("ListAsync"), Benchmark]
	public async Task<List<SqlitePost>> ListAsync_ODE() => await _conn.Command(Sql).Map<SqlitePost>().ToListAsync();
}

/// <summary>Post as SQLite delivers it: the key is text (Dapper cannot convert text to Guid, so both libraries read a string).</summary>
public class SqlitePost
{
	public int Id { get; set; }
	public string Title { get; set; } = "";
	public string? Body { get; set; }
	public DateTime Created { get; set; }
	public decimal Price { get; set; }
	public bool IsPublished { get; set; }
	public int? ViewCount { get; set; }
	public double Score { get; set; }
	public string Key { get; set; } = "";
	public short Rank { get; set; }
	public long Big { get; set; }
	public string? Tag { get; set; }
}
