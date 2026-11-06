using System.Collections.Immutable;
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
	public static readonly ImmutableArray<KeyValuePair<string, string?>> Pairs =
	[
		new(nameof(Post.Id), "post_id"), new(nameof(Post.Title), "post_title"), new(nameof(Post.Body), "post_body"), new(nameof(Post.Created), "post_created"),
		new(nameof(Post.Price), "post_price"), new(nameof(Post.IsPublished), "post_is_published"), new(nameof(Post.ViewCount), "post_view_count"), new(nameof(Post.Score), "post_score"),
		new(nameof(Post.Key), "post_key"), new(nameof(Post.Rank), "post_rank"), new(nameof(Post.Big), "post_big"), new(nameof(Post.Tag), "post_tag"),
	];

	public static readonly string[] Names = [.. Pairs.Select(p => p.Value!)];

	/// <summary>The same mapping as tuples, for the tuple overloads. They convert it to pairs, build a new map and compile the row reader on every call.</summary>
	public static readonly (string Field, string? Column)[] Tuples = [.. Pairs.Select(p => (p.Key, p.Value))];

	/// <summary>ODE: built once (the implicit conversion from the pairs), passed to every query.</summary>
	public static readonly RowMap<RenamedPost> Map = new(Pairs);

	static bool _dapperConfigured;

	/// <summary>Dapper's equivalent: a type map set once per type (global state, as Dapper requires).</summary>
	public static void ConfigureDapper()
	{
		if (_dapperConfigured) return;
		Dictionary<string, string> propertyByColumn = Pairs.ToDictionary(p => p.Value!, p => p.Key, StringComparer.OrdinalIgnoreCase);
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

// With every column named differently from its property, a read through a declared RowMap<T> should cost what Map<T>() costs
// on matching names (the baseline), and no more than Dapper with an equivalent type map set once. The tuple overload is the
// one-off path: it builds a new map and compiles the row reader on every call, so it is here to show what declaring a map saves.
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
		foreach (List<RenamedPost> actual in new[] { List_RowMap(), List_TupleOverload(), List_DapperTypeMap() })
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

	// The one-off path: builds a new map and compiles the row reader on every call.
	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_TupleOverload() => _renamed.Command(Sql).Map<RenamedPost>(RenamedColumns.Tuples).ToList();

	[BenchmarkCategory("List"), Benchmark]
	public List<RenamedPost> List_DapperTypeMap() => _renamed.Query<RenamedPost>(Sql).AsList();

	[BenchmarkCategory("First"), Benchmark(Baseline = true)]
	public SnakePost? First_SameNames() => _renamed.Command(Sql).Map<SnakePost>().FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public Post? First_ShortNames() => _matching.Command(Sql).Map<Post>().FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_RowMap() => _renamed.Command(Sql).Map<RenamedPost>(RenamedColumns.Map).FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_TupleOverload() => _renamed.Command(Sql).Map<RenamedPost>(RenamedColumns.Tuples).FirstOrDefault();

	[BenchmarkCategory("First"), Benchmark]
	public RenamedPost? First_DapperTypeMap() => _renamed.QueryFirstOrDefault<RenamedPost>(Sql);
}
