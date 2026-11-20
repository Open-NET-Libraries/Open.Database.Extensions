using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Open.Database.Extensions.Benchmarks;

// What it costs to find the names of K derived arguments in a SQL text that is built at run time (a new string on every call):
// the scan that allocates nothing (what runs now) against reading the text into a set of names and matching in that (what a text
// that is not remembered cost before), for texts of about 200 and 2,000 characters and one and four arguments.
// Written, not yet run: the numbers are for the Bench lane.
[MemoryDiagnoser]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1102:Make class static", Justification = "BenchmarkDotNet generates a class that derives from the benchmark class, so it cannot be static.")]
public class TextNameScanBenchmarks
{
	// The internals are reached through delegates (the benchmark project cannot see them), which cost the same for both arms.
	static readonly Type Cache = typeof(DataRecordExtensions).Assembly.GetType("Open.Database.Extensions.TextNameCache", throwOnError: true)!;

	static readonly Func<string, string, string?> Scan
		= Cache.GetMethod("Scan", BindingFlags.Static | BindingFlags.NonPublic)!.CreateDelegate<Func<string, string, string?>>();

	// The parsed way: read the text into a set of names and match each argument in it, as the set of a remembered text does.
	static readonly Func<string, string, string?> Parse
		= Cache.GetMethod("Slow", BindingFlags.Static | BindingFlags.NonPublic)!.CreateDelegate<Func<string, string, string?>>();

	static readonly string[] Expressions = ["p0", "p1", "p2", "p3"];

	[Params(200, 2000)]
	public int Length { get; set; }

	[Params(1, 4)]
	public int Arguments { get; set; }

	string _text = "";

	[GlobalSetup]
	public void Setup()
	{
		var sql = new System.Text.StringBuilder("select ");
		for (int i = 0; sql.Length < Length - 80; i++) sql.Append("column_").Append(i).Append(", ");
		sql.Append("id from posts where @p0 > 0");
		for (int i = 1; i < Arguments; i++) sql.Append(" and @p").Append(i).Append(" > 0");
		_text = sql.ToString();

		// Both arms must find the same names.
		for (int i = 0; i < Arguments; i++)
		{
			if (Scan(_text, Expressions[i]) != Parse(_text, Expressions[i]))
				throw new InvalidOperationException("The scan and the parse disagree.");
		}
	}

	// The text is a fresh string on every call, as when a query is put together at run time.
	[Benchmark(Baseline = true)]
	public int ParsedEveryCall()
	{
		string text = new(_text.AsSpan());
		int found = 0;
		for (int i = 0; i < Arguments; i++)
		{
			if (Parse(text, Expressions[i]) is not null) found++;
		}

		return found;
	}

	[Benchmark]
	public int ScannedEveryCall()
	{
		string text = new(_text.AsSpan());
		int found = 0;
		for (int i = 0; i < Arguments; i++)
		{
			if (Scan(text, Expressions[i]) is not null) found++;
		}

		return found;
	}
}
