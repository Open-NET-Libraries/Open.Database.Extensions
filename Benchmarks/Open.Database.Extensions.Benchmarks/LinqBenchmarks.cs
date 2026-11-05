using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ZLinq;

namespace Open.Database.Extensions.Benchmarks;

// Does ZLinq earn a place in the library? These are the in-memory collection operations ODE performs around
// a query (building lookups from column/override lists, materializing keyed collections, slicing).
// Each category compares System.Linq (the baseline), ZLinq, and the plain loop ODE uses.
// A dependency is only worth taking if it beats the plain loop; this is the evidence.
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class LinqBenchmarks
{
	[Params(12, 1000)]
	public int Count { get; set; }

	List<Post> _posts = null!;
	KeyValuePair<string, string?>[] _overrides = null!;
	string[] _columns = null!;

	[GlobalSetup]
	public void Setup()
	{
		_posts = Enumerable.Range(0, Count).Select(i => new Post { Id = i, Title = "t" + i }).ToList();
		_columns = Enumerable.Range(0, Count).Select(i => "Column" + i).ToArray();
		_overrides = _columns.Select(c => new KeyValuePair<string, string?>(c, i(c))).ToArray();
		static string? i(string c) => c.Length % 5 == 0 ? null : c + "x";
	}

	// ---- keyed collection from rows ----
	[BenchmarkCategory("ToDictionary"), Benchmark(Baseline = true)]
	public Dictionary<int, Post> ToDictionary_Linq() => _posts.ToDictionary(p => p.Id);

	[BenchmarkCategory("ToDictionary"), Benchmark]
	public Dictionary<int, Post> ToDictionary_ZLinq() => _posts.AsValueEnumerable().ToDictionary(p => p.Id);

	[BenchmarkCategory("ToDictionary"), Benchmark]
	public Dictionary<int, Post> ToDictionary_Loop()
	{
		var d = new Dictionary<int, Post>();
		foreach (Post p in _posts) d.Add(p.Id, p);
		return d;
	}

	// ---- slicing ----
	[BenchmarkCategory("SkipTake"), Benchmark(Baseline = true)]
	public int SkipTake_Linq()
	{
		int sum = 0;
		foreach (Post p in _posts.Skip(2).Take(Count / 2)) sum += p.Id;
		return sum;
	}

	[BenchmarkCategory("SkipTake"), Benchmark]
	public int SkipTake_ZLinq()
	{
		int sum = 0;
		foreach (Post p in _posts.AsValueEnumerable().Skip(2).Take(Count / 2)) sum += p.Id;
		return sum;
	}

	[BenchmarkCategory("SkipTake"), Benchmark]
	public int SkipTake_Loop()
	{
		int sum = 0, end = Math.Min(_posts.Count, 2 + Count / 2);
		for (int i = 2; i < end; i++) sum += _posts[i].Id;
		return sum;
	}

	// ---- override list → lookup (what a field-mapping override does per query) ----
	[BenchmarkCategory("Overrides"), Benchmark(Baseline = true)]
	public Dictionary<string, string> Overrides_Linq()
		=> _overrides.Where(o => o.Value is not null).ToDictionary(o => o.Key, o => o.Value!, StringComparer.OrdinalIgnoreCase);

	[BenchmarkCategory("Overrides"), Benchmark]
	public Dictionary<string, string> Overrides_ZLinq()
		=> _overrides.AsValueEnumerable().Where(o => o.Value is not null).ToDictionary(o => o.Key, o => o.Value!, StringComparer.OrdinalIgnoreCase);

	[BenchmarkCategory("Overrides"), Benchmark]
	public Dictionary<string, string> Overrides_Loop()
	{
		var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		foreach (KeyValuePair<string, string?> o in _overrides)
			if (o.Value is not null) d.Add(o.Key, o.Value);
		return d;
	}

	// ---- projecting names to an array ----
	[BenchmarkCategory("Names"), Benchmark(Baseline = true)]
	public int[] Names_Linq() => _columns.Select(c => c.Length).ToArray();

	[BenchmarkCategory("Names"), Benchmark]
	public int[] Names_ZLinq() => _columns.AsValueEnumerable().Select(c => c.Length).ToArray();

	[BenchmarkCategory("Names"), Benchmark]
	public int[] Names_Loop()
	{
		var r = new int[_columns.Length];
		for (int i = 0; i < r.Length; i++) r[i] = _columns[i].Length;
		return r;
	}
}
