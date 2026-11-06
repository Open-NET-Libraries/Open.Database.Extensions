using System.Data;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Benchmarks;

// The renamed-column comparison against SQL Server (LocalDB): the same rows in a table whose every column is
// snake_case, read through a predefined RowMap<T>, through per-call overrides, and through Dapper with a type map set once.
// A throwaway database is created in Setup and dropped in Cleanup.
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class SqlServerRenamedColumnBenchmarks
{
	const string Server = @"Server=(localdb)\MSSQLLocalDB;Integrated Security=true;Encrypt=false;TrustServerCertificate=true;";
	const string Sql = "select * from posts";
	const string RenamedSql = "select * from posts_renamed";

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
		_conn.Execute("""
			create table posts_renamed (
				post_id int not null, post_title nvarchar(100) not null, post_body nvarchar(max) null, post_created datetime2 not null,
				post_price decimal(18,2) not null, post_is_published bit not null, post_view_count int null, post_score float not null,
				post_key uniqueidentifier not null, post_rank smallint not null, post_big bigint not null, post_tag nvarchar(20) null)
			""");

		var table = new DataTable();
		foreach ((string name, Type type) in new[]
		{
			("c0", typeof(int)), ("c1", typeof(string)), ("c2", typeof(string)), ("c3", typeof(DateTime)),
			("c4", typeof(decimal)), ("c5", typeof(bool)), ("c6", typeof(int)), ("c7", typeof(double)),
			("c8", typeof(Guid)), ("c9", typeof(short)), ("c10", typeof(long)), ("c11", typeof(string)),
		})
			table.Columns.Add(name, type);

		for (int i = 1; i <= Rows; i++)
			table.Rows.Add(i, "Post number " + i, i % 3 == 0 ? DBNull.Value : "Lorem ipsum " + i, new DateTime(2024, 1, 1).AddMinutes(i),
				10m + i, i % 2 == 0, i % 4 == 0 ? DBNull.Value : i * 3, i * 1.5, Guid.NewGuid(), (short)(i % 100), (long)i << 20,
				i % 2 == 0 ? DBNull.Value : "t" + (i % 7));

		// Both tables get the same rows (the same Guids too), by ordinal.
		foreach (string destination in new[] { "posts", "posts_renamed" })
		{
			using var bulk = new SqlBulkCopy(_conn) { DestinationTableName = destination };
			for (int c = 0; c < table.Columns.Count; c++) bulk.ColumnMappings.Add(c, c);
			bulk.WriteToServer(table);
		}

		RenamedColumns.ConfigureDapper();

		// Every approach must produce the same objects, or the numbers compare different work.
		List<Post> expected = List_ShortNames();
		if (!List_SameNames().Zip(expected, RenamedColumns.Same).All(x => x))
			throw new InvalidOperationException("The same-name read mapped the rows differently.");
		foreach (List<RenamedPost> actual in new[] { List_RowMap(), List_PerCallOverrides(), List_DapperTypeMap() })
			if (expected.Count != actual.Count || !expected.Zip(actual, RenamedColumns.Same).All(x => x))
				throw new InvalidOperationException("A renamed-column read mapped the rows differently from the matching-name read.");
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

	// The control: Map<T>() by name over the very same result set (same column names, so the provider allocates the same).
	[BenchmarkCategory("List"), Benchmark(Baseline = true)]
	public List<SnakePost> List_SameNames() => _conn.Command(RenamedSql).Map<SnakePost>().ToList();

	// Map<T>() by name over the short-named table: shorter column names, so the provider allocates a little less.
	[BenchmarkCategory("List"), Benchmark]
	public List<Post> List_ShortNames() => _conn.Command(Sql).Map<Post>().ToList();

	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_RowMap() => _conn.Command(RenamedSql).Map<RenamedPost>(RenamedColumns.Map).ToList();

	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_PerCallOverrides() => _conn.Command(RenamedSql).Map<RenamedPost>(
		(nameof(RenamedPost.Id), "post_id"), (nameof(RenamedPost.Title), "post_title"), (nameof(RenamedPost.Body), "post_body"), (nameof(RenamedPost.Created), "post_created"),
		(nameof(RenamedPost.Price), "post_price"), (nameof(RenamedPost.IsPublished), "post_is_published"), (nameof(RenamedPost.ViewCount), "post_view_count"), (nameof(RenamedPost.Score), "post_score"),
		(nameof(RenamedPost.Key), "post_key"), (nameof(RenamedPost.Rank), "post_rank"), (nameof(RenamedPost.Big), "post_big"), (nameof(RenamedPost.Tag), "post_tag")).ToList();

	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_DapperTypeMap() => _conn.Query<RenamedPost>(RenamedSql).AsList();

	[BenchmarkCategory("First"), Benchmark(Baseline = true)]
	public SnakePost? First_SameNames() => _conn.Command(RenamedSql).Map<SnakePost>().FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public Post? First_ShortNames() => _conn.Command(Sql).Map<Post>().FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_RowMap() => _conn.Command(RenamedSql).Map<RenamedPost>(RenamedColumns.Map).FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_PerCallOverrides() => _conn.Command(RenamedSql).Map<RenamedPost>(
		(nameof(RenamedPost.Id), "post_id"), (nameof(RenamedPost.Title), "post_title"), (nameof(RenamedPost.Body), "post_body"), (nameof(RenamedPost.Created), "post_created"),
		(nameof(RenamedPost.Price), "post_price"), (nameof(RenamedPost.IsPublished), "post_is_published"), (nameof(RenamedPost.ViewCount), "post_view_count"), (nameof(RenamedPost.Score), "post_score"),
		(nameof(RenamedPost.Key), "post_key"), (nameof(RenamedPost.Rank), "post_rank"), (nameof(RenamedPost.Big), "post_big"), (nameof(RenamedPost.Tag), "post_tag")).FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_DapperTypeMap() => _conn.QueryFirstOrDefault<RenamedPost>(RenamedSql);
}
