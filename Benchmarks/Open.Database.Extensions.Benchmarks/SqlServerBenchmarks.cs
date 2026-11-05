using System.Data;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Benchmarks;

// The same comparison against SQL Server (LocalDB), a statically typed provider: column types match the
// model exactly, so ODE's typed (unboxed) path is taken for every column. Skipped when LocalDB is unavailable.
// A throwaway database is created in Setup and dropped in Cleanup.
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class SqlServerBenchmarks
{
	const string Server = @"Server=(localdb)\MSSQLLocalDB;Integrated Security=true;Encrypt=false;TrustServerCertificate=true;";
	const string Sql = "select * from posts";
	const string ParamSql = "select * from posts where Id > @id";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	string _database = null!;
	SqlConnection _conn = null!;

	[GlobalSetup]
	public void Setup()
	{
		_database = "OdeBench_" + Guid.NewGuid().ToString("N");
		using (var master = new SqlConnection(Server))
		{
			master.Open();
			master.Execute($"create database [{_database}]");
		}

		_conn = new SqlConnection(Server + $"Database={_database};");
		_conn.Open();
		_conn.Execute("""
			create table posts (
				Id int not null, Title nvarchar(100) not null, Body nvarchar(max) null, Created datetime2 not null,
				Price decimal(18,2) not null, IsPublished bit not null, ViewCount int null, Score float not null,
				[Key] uniqueidentifier not null, [Rank] smallint not null, Big bigint not null, Tag nvarchar(20) null)
			""");

		var table = new DataTable();
		foreach ((string name, Type type) in new[]
		{
			("Id", typeof(int)), ("Title", typeof(string)), ("Body", typeof(string)), ("Created", typeof(DateTime)),
			("Price", typeof(decimal)), ("IsPublished", typeof(bool)), ("ViewCount", typeof(int)), ("Score", typeof(double)),
			("Key", typeof(Guid)), ("Rank", typeof(short)), ("Big", typeof(long)), ("Tag", typeof(string)),
		})
			table.Columns.Add(name, type);

		for (int i = 1; i <= Rows; i++)
			table.Rows.Add(i, "Post number " + i, i % 3 == 0 ? DBNull.Value : "Lorem ipsum " + i, new DateTime(2024, 1, 1).AddMinutes(i),
				10m + i, i % 2 == 0, i % 4 == 0 ? DBNull.Value : i * 3, i * 1.5, Guid.NewGuid(), (short)(i % 100), (long)i << 20,
				i % 2 == 0 ? DBNull.Value : "t" + (i % 7));

		using (var bulk = new SqlBulkCopy(_conn) { DestinationTableName = "posts" })
		{
			foreach (DataColumn c in table.Columns) bulk.ColumnMappings.Add(c.ColumnName, c.ColumnName);
			bulk.WriteToServer(table);
		}

		// Both libraries must produce identical objects, or the timings compare different work.
		var expected = List_Dapper();
		var actual = List_ODE();
		if (expected.Count != actual.Count || !expected.Zip(actual, Same).All(x => x))
			throw new InvalidOperationException("ODE and Dapper mapped the SQL Server rows differently.");
	}

	[GlobalCleanup]
	public void Cleanup()
	{
		_conn.Dispose();
		SqlConnection.ClearAllPools();
		using var master = new SqlConnection(Server);
		master.Open();
		master.Execute($"alter database [{_database}] set single_user with rollback immediate; drop database [{_database}]");
	}

	static bool Same(Post a, Post b)
		=> a.Id == b.Id && a.Title == b.Title && a.Body == b.Body && a.Created == b.Created && a.Price == b.Price
			&& a.IsPublished == b.IsPublished && a.ViewCount == b.ViewCount && a.Score == b.Score && a.Key == b.Key
			&& a.Rank == b.Rank && a.Big == b.Big && a.Tag == b.Tag;

	[BenchmarkCategory("List"), Benchmark(Baseline = true)]
	public List<Post> List_Dapper() => _conn.Query<Post>(Sql).AsList();

	[BenchmarkCategory("List"), Benchmark]
	public List<Post> List_ODE() => _conn.Command(Sql).Map<Post>().ToList();

	[BenchmarkCategory("First"), Benchmark(Baseline = true)]
	public Post? First_Dapper() => _conn.QueryFirstOrDefault<Post>(Sql);

	[BenchmarkCategory("First"), Benchmark]
	public Post? First_ODE() => _conn.Command(Sql).Map<Post>().FirstOrDefault();

	[BenchmarkCategory("Params"), Benchmark(Baseline = true)]
	public List<Post> Params_Dapper() => _conn.Query<Post>(ParamSql, new { id = 0 }).AsList();

	[BenchmarkCategory("Params"), Benchmark]
	public List<Post> Params_ODE() => _conn.Command(ParamSql).AddParam("id", 0).Map<Post>().ToList();

	[BenchmarkCategory("Scalar"), Benchmark(Baseline = true)]
	public int Scalar_Dapper() => _conn.ExecuteScalar<int>("select count(*) from posts");

	[BenchmarkCategory("Scalar"), Benchmark]
	public int Scalar_ODE() => _conn.Command("select count(*) from posts").ExecuteScalar<int>();

	// A non-query with 8 parameters (7 values and the key); it rewrites the same row each time.
	const string NonQuery8Sql = "update posts set Title = @title, Body = @body, Tag = @tag, ViewCount = @views, [Rank] = @rank, IsPublished = @pub, Big = @big where Id = @id";

	[BenchmarkCategory("NonQuery8"), Benchmark(Baseline = true)]
	public int NonQuery8_Dapper() => _conn.Execute(NonQuery8Sql, new { title = "t", body = "b", tag = "x", views = 3, rank = 4, pub = 1, big = 5L, id = 1 });

	[BenchmarkCategory("NonQuery8"), Benchmark]
	public int NonQuery8_ODE() => _conn.Command(NonQuery8Sql)
		.AddParam("title", "t").AddParam("body", "b").AddParam("tag", "x").AddParam("views", 3).AddParam("rank", 4).AddParam("pub", 1).AddParam("big", 5L).AddParam("id", 1)
		.ExecuteNonQuery();

	[BenchmarkCategory("ListAsync"), Benchmark(Baseline = true)]
	public async Task<List<Post>> ListAsync_Dapper() => (await _conn.QueryAsync<Post>(Sql)).AsList();

	[BenchmarkCategory("ListAsync"), Benchmark]
	public async Task<List<Post>> ListAsync_ODE() => await _conn.Command(Sql).Map<Post>().ToListAsync();
}
