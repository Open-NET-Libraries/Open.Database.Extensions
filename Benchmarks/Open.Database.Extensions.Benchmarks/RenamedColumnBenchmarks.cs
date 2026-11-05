using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dapper;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>The 12-property <see cref="Post"/> model, read from a query whose every column is named differently from its property.</summary>
public sealed class RenamedPost : Post;

/// <summary>The same columns as <see cref="RenamedPost"/> reads, but a model whose property names are the column names, so <c>Map&lt;T&gt;()</c> matches by name.</summary>
public sealed class SnakePost
{
	public int post_id { get; set; }
	public string post_title { get; set; } = "";
	public string? post_body { get; set; }
	public DateTime post_created { get; set; }
	public decimal post_price { get; set; }
	public bool post_is_published { get; set; }
	public int? post_view_count { get; set; }
	public double post_score { get; set; }
	public Guid post_key { get; set; }
	public short post_rank { get; set; }
	public long post_big { get; set; }
	public string? post_tag { get; set; }
}

/// <summary>The one mapping of <see cref="RenamedPost"/>, defined once for every scenario that reads it.</summary>
public static class RenamedColumns
{
	/// <summary>Property, column; in the order of the columns <see cref="BenchTable.Posts"/> produces.</summary>
	public static readonly (string Field, string? Column)[] Pairs =
	[
		("Id", "post_id"), ("Title", "post_title"), ("Body", "post_body"), ("Created", "post_created"),
		("Price", "post_price"), ("IsPublished", "post_is_published"), ("ViewCount", "post_view_count"), ("Score", "post_score"),
		("Key", "post_key"), ("Rank", "post_rank"), ("Big", "post_big"), ("Tag", "post_tag"),
	];

	public static readonly string[] Names = [.. Pairs.Select(p => p.Column!)];

	/// <summary>ODE: built once, passed to every query.</summary>
	public static readonly RowMap<RenamedPost> Map = new(Pairs);

	static bool _dapperConfigured;

	/// <summary>Dapper's equivalent: a type map set once per type (global state, as Dapper requires).</summary>
	public static void ConfigureDapper()
	{
		if (_dapperConfigured) return;
		Dictionary<string, string> propertyByColumn = Pairs.ToDictionary(p => p.Column!, p => p.Field, StringComparer.OrdinalIgnoreCase);
		SqlMapper.SetTypeMap(typeof(RenamedPost), new CustomPropertyTypeMap(typeof(RenamedPost), (type, column) => type.GetProperty(propertyByColumn[column])!));
		_dapperConfigured = true;
	}

	public static bool Same(SnakePost a, Post b)
		=> a.post_id == b.Id && a.post_title == b.Title && a.post_body == b.Body && a.post_created == b.Created && a.post_price == b.Price
			&& a.post_is_published == b.IsPublished && a.post_view_count == b.ViewCount && a.post_score == b.Score && a.post_key == b.Key
			&& a.post_rank == b.Rank && a.post_big == b.Big && a.post_tag == b.Tag;

	public static bool Same(Post a, Post b)
		=> a.Id == b.Id && a.Title == b.Title && a.Body == b.Body && a.Created == b.Created && a.Price == b.Price
			&& a.IsPublished == b.IsPublished && a.ViewCount == b.ViewCount && a.Score == b.Score && a.Key == b.Key
			&& a.Rank == b.Rank && a.Big == b.Big && a.Tag == b.Tag;
}

// Criterion 2 of the declared-mapping work: with every column named differently from its property, a read through a
// predefined RowMap<T> should cost what Map<T>() costs on matching names (the baseline), and no more than Dapper
// with an equivalent type map set once. Per-call overrides are the "before".
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class RenamedColumnBenchmarks
{
	const string Sql = "select * from posts";

	[Params(1, 100, 1000)]
	public int Rows { get; set; }

	BenchConnection _matching = null!, _renamed = null!;

	[GlobalSetup]
	public void Setup()
	{
		BenchTable table = BenchTable.Posts(Rows);
		_matching = new BenchConnection(table);
		_renamed = new BenchConnection(table.WithNames(RenamedColumns.Names));
		RenamedColumns.ConfigureDapper();

		// Every approach must produce the same objects, or the numbers compare different work.
		List<Post> expected = List_ShortNames();
		if (!List_SameNames().Zip(expected, RenamedColumns.Same).All(x => x))
			throw new InvalidOperationException("The same-name read mapped the rows differently.");
		foreach (List<RenamedPost> actual in new[] { List_RowMap(), List_PerCallOverrides(), List_DapperTypeMap() })
			if (expected.Count != actual.Count || !expected.Zip(actual, RenamedColumns.Same).All(x => x))
				throw new InvalidOperationException("A renamed-column read mapped the rows differently from the matching-name read.");
	}

	// The control: Map<T>() by name over the very same result set (same column names, so the provider allocates the same).
	[BenchmarkCategory("List"), Benchmark(Baseline = true)]
	public List<SnakePost> List_SameNames() => _renamed.Command(Sql).Map<SnakePost>().ToList();

	// Map<T>() by name over the short-named table: shorter column names, so the provider allocates a little less.
	[BenchmarkCategory("List"), Benchmark]
	public List<Post> List_ShortNames() => _matching.Command(Sql).Map<Post>().ToList();

	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_RowMap() => _renamed.Command(Sql).Map<RenamedPost>(RenamedColumns.Map).ToList();

	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_PerCallOverrides() => _renamed.Command(Sql).Map<RenamedPost>(
		("Id", "post_id"), ("Title", "post_title"), ("Body", "post_body"), ("Created", "post_created"),
		("Price", "post_price"), ("IsPublished", "post_is_published"), ("ViewCount", "post_view_count"), ("Score", "post_score"),
		("Key", "post_key"), ("Rank", "post_rank"), ("Big", "post_big"), ("Tag", "post_tag")).ToList();

	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_DapperTypeMap() => _renamed.Query<RenamedPost>(Sql).AsList();

	[BenchmarkCategory("First"), Benchmark(Baseline = true)]
	public SnakePost? First_SameNames() => _renamed.Command(Sql).Map<SnakePost>().FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public Post? First_ShortNames() => _matching.Command(Sql).Map<Post>().FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_RowMap() => _renamed.Command(Sql).Map<RenamedPost>(RenamedColumns.Map).FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_PerCallOverrides() => _renamed.Command(Sql).Map<RenamedPost>(
		("Id", "post_id"), ("Title", "post_title"), ("Body", "post_body"), ("Created", "post_created"),
		("Price", "post_price"), ("IsPublished", "post_is_published"), ("ViewCount", "post_view_count"), ("Score", "post_score"),
		("Key", "post_key"), ("Rank", "post_rank"), ("Big", "post_big"), ("Tag", "post_tag")).FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_DapperTypeMap() => _renamed.QueryFirstOrDefault<RenamedPost>(Sql);
}
