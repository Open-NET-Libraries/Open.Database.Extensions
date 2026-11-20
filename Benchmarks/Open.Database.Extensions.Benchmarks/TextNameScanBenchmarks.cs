using System.Collections.Immutable;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;

namespace Open.Database.Extensions.Benchmarks;

// What it costs to find the names of K derived arguments in a SQL text that is built at run time (a new string on every call), for texts of about
// 200 and 2,000 characters, one and four arguments, on one thread and on 8 threads that all run the same arm at the same time:
//   ParsedEveryCall      the text is read into a set of names and each argument is matched in it (what an unremembered text cost before the content table);
//   ScannedEveryCall     the scan alone;
//   MasterPathEveryCall  the whole path as merged in PR 185 (a port of that TextNameCache, prototype below): the table of texts by object, then the table by
//                        content under a lock, the set of names found there, each argument matched in it;
//   NewPathEveryCall     the whole path of this change: the table by object, string.IsInterned and the reference check, then the scan of each argument,
//                        reached through the same reflection delegates as the scan;
//   IsInternedOnly       only string.IsInterned(text), over a prebuilt array of distinct string objects with the same characters (nothing is allocated),
//                        so that the question whether the runtime's intern table is a point of contention at 8 threads is isolated.
// Every arm except the last builds a new string for each call (the same allocation in each arm). Every arm uses the same scheme for threads: a Parallel.For with
// one iteration per thread, each iteration making the same fixed number of calls; one thread runs the calls directly. The time is the wall time of one
// invoke (Reads calls on each thread) divided by Reads, and the bytes include the same cost of starting the threads for every arm.
// Written, not yet run: the numbers are for the Bench lane.
[MemoryDiagnoser]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1102:Make class static", Justification = "BenchmarkDotNet generates a class that derives from the benchmark class, so it cannot be static.")]
public class TextNameScanBenchmarks
{
	const int Reads = 100;

	// The internals are reached through delegates (the benchmark project cannot see them), which cost the same for every arm.
	static readonly Assembly Core = typeof(DataRecordExtensions).Assembly;
	static readonly Type Cache = Core.GetType("Open.Database.Extensions.TextNameCache", throwOnError: true)!;
	static readonly Type SqlText = Core.GetType("Open.Database.Extensions.SqlTextParameters", throwOnError: true)!;
	static readonly Type NameSet = Core.GetType("Open.Database.Extensions.NameSet", throwOnError: true)!;

	static readonly Func<string, string, string?> Scan
		= Cache.GetMethod("Scan", BindingFlags.Static | BindingFlags.NonPublic)!.CreateDelegate<Func<string, string, string?>>();

	// The new path's lookup: null for a text that is not interned (the text is then scanned).
	static readonly Func<string, object?> Get
		= Cache.GetMethod("Get", BindingFlags.Static | BindingFlags.NonPublic)!.CreateDelegate<Func<string, object?>>();

	// The parsed way: read the text into a set of names and match each argument in it, as the set of a remembered text does.
	static readonly Func<string, string, string?> Parse
		= Cache.GetMethod("Slow", BindingFlags.Static | BindingFlags.NonPublic)!.CreateDelegate<Func<string, string, string?>>();

	// What the prototype of the merged path needs of the internals: the tokenizer, a set made from its names, and the match in a set.
	static readonly Func<string, ImmutableArray<string>> Tokens
		= SqlText.GetMethod("Parse", BindingFlags.Static | BindingFlags.NonPublic)!.CreateDelegate<Func<string, ImmutableArray<string>>>();

	static readonly Func<ImmutableArray<string>, object> MakeSet = BuildMakeSet();

	static readonly Func<object, string, string, string?> MatchIn = BuildMatchIn();

	// Compiled expressions may reach non-public members.
	static Func<ImmutableArray<string>, object> BuildMakeSet()
	{
		ParameterExpression names = Expression.Parameter(typeof(ImmutableArray<string>));
		ConstructorInfo constructor = NameSet.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)[0];
		NewExpression made = constructor.GetParameters().Length == 1
			? Expression.New(constructor, names)
			: Expression.New(constructor, names, Expression.Constant(false));
		return Expression.Lambda<Func<ImmutableArray<string>, object>>(Expression.Convert(made, typeof(object)), names).Compile();
	}

	static Func<object, string, string, string?> BuildMatchIn()
	{
		ParameterExpression set = Expression.Parameter(typeof(object));
		ParameterExpression expression = Expression.Parameter(typeof(string));
		ParameterExpression procedure = Expression.Parameter(typeof(string));
		MethodInfo match = NameSet.GetMethod("Match", BindingFlags.Instance | BindingFlags.NonPublic)!;
		return Expression.Lambda<Func<object, string, string, string?>>(
			Expression.Call(Expression.Convert(set, NameSet), match, expression, procedure),
			set,
			expression,
			procedure).Compile();
	}

	// A port of the TextNameCache of PR 185, for the benchmark only.
	static class MasterPath
	{
		const int MaxTexts = 1024;
		const int MaxLength = 4096;

		static readonly ConditionalWeakTable<string, object> ByReference = new();
		static readonly Dictionary<string, object> ByContent = new(StringComparer.Ordinal);

		public static object Get(string text)
		{
			if (ByReference.TryGetValue(text, out object? known)) return known;

			if (text.Length > MaxLength)
				return ByReference.GetValue(text, static t => MakeSet(Tokens(t)));

			lock (ByContent)
			{
				if (ByContent.TryGetValue(text, out known)) return known;
			}

			return Remember(text);
		}

		static object Remember(string text)
		{
			object made = MakeSet(Tokens(text));
			lock (ByContent)
			{
				if (ByContent.Count >= MaxTexts) ByContent.Clear();
				ByContent[text] = made;
			}

			return ByReference.GetValue(text, _ => made);
		}
	}

	static readonly string[] Expressions = ["p0", "p1", "p2", "p3"];

	[Params(200, 2000)]
	public int Length { get; set; }

	[Params(1, 4)]
	public int Arguments { get; set; }

	[Params(1, 8)]
	public int Threads { get; set; }

	string _text = "";
	string[] _copies = [];

	[GlobalSetup]
	public void Setup()
	{
		var sql = new System.Text.StringBuilder("select ");
		for (int i = 0; sql.Length < Length - 80; i++) sql.Append("column_").Append(i).Append(", ");
		sql.Append("id from posts where @p0 > 0");
		for (int i = 1; i < Arguments; i++) sql.Append(" and @p").Append(i).Append(" > 0");
		_text = sql.ToString();

		// Distinct objects with the same characters, none of them interned, for the arm that only asks string.IsInterned.
		_copies = [.. Enumerable.Range(0, 256).Select(_ => new string(_text.AsSpan()))];

		// Every path must find the same names, and the new path must take the scan.
		string built = new(_text.AsSpan());
		if (Get(built) is not null) throw new InvalidOperationException("A built text must not be interned.");
		for (int i = 0; i < Arguments; i++)
		{
			string? scanned = Scan(built, Expressions[i]);
			if (scanned != Parse(built, Expressions[i]) || scanned != MatchIn(MasterPath.Get(built), Expressions[i], built))
				throw new InvalidOperationException("The paths disagree.");
		}
	}

	// One thread runs the calls directly; more run one iteration each of a Parallel.For, every iteration making the same number of calls.
	int Run(Func<int> calls)
	{
		if (Threads == 1) return calls();

		int total = 0;
		Parallel.For(
			0,
			Threads,
			new ParallelOptions { MaxDegreeOfParallelism = Threads },
			_ => Interlocked.Add(ref total, calls()));
		return total;
	}

	int Resolve(Func<string, string, string?> path)
	{
		int found = 0;
		for (int call = 0; call < Reads; call++)
		{
			// The text is a fresh string on every call, as when a query is put together at run time.
			string text = new(_text.AsSpan());
			for (int i = 0; i < Arguments; i++)
			{
				if (path(text, Expressions[i]) is not null) found++;
			}
		}

		return found;
	}

	static string? ParsedPath(string text, string expression) => Parse(text, expression);

	static string? ScanPath(string text, string expression) => Scan(text, expression);

	static string? MasterPathMatch(string text, string expression) => MatchIn(MasterPath.Get(text), expression, text);

	// A text that is interned would be remembered by its object; any other is scanned.
	static string? NewPathMatch(string text, string expression)
		=> Get(text) is { } set
			? MatchIn(set, expression, text)
			: Scan(text, expression);

	[Benchmark(Baseline = true, OperationsPerInvoke = Reads)]
	public int ParsedEveryCall() => Run(() => Resolve(ParsedPath));

	[Benchmark(OperationsPerInvoke = Reads)]
	public int ScannedEveryCall() => Run(() => Resolve(ScanPath));

	[Benchmark(OperationsPerInvoke = Reads)]
	public int MasterPathEveryCall() => Run(() => Resolve(MasterPathMatch));

	[Benchmark(OperationsPerInvoke = Reads)]
	public int NewPathEveryCall() => Run(() => Resolve(NewPathMatch));

	[Benchmark(OperationsPerInvoke = Reads)]
	public int IsInternedOnly() => Run(() =>
	{
		string[] copies = _copies;
		int interned = 0;
		for (int call = 0; call < Reads; call++)
		{
			if (string.IsInterned(copies[call & (copies.Length - 1)]) is not null) interned++;
		}

		return interned;
	});
}
