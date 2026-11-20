using System.Collections.Frozen;
using BenchmarkDotNet.Attributes;

namespace Open.Database.Extensions.Benchmarks;

// How the answers a NameSet remembers (the real name each argument expression matched) are read on every execution, for the expressions
// of 1, 4 and 16 call sites, on one thread and on 8 threads that share one set. The three read paths are prototypes copied from the read logic of NameSet:
//   Locked      today: a lock and a Dictionary, written under the same lock on a miss;
//   Frozen      a FrozenDictionary snapshot published with Volatile, rebuilt under a lock only on a miss;
//   Snapshot    the same with a plain Dictionary that is never changed after it is published.
// Each invoke reads 1,000 answers on each thread: the time is the wall time of that, the bytes include the same cost of starting the threads for all three.
// Written, not yet run: the numbers are for the Bench lane.
[MemoryDiagnoser]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1102:Make class static", Justification = "BenchmarkDotNet generates a class that derives from the benchmark class, so it cannot be static.")]
public class NameSetMatchBenchmarks
{
	const int Reads = 1000;

	// What a miss works out; the benchmark only reads what is already remembered, so this runs while the sets are filled.
	static string? Find(string expression) => expression.Length % 5 == 0 ? null : "@" + expression;

	sealed class Locked
	{
		readonly Dictionary<string, string?> _matches = new(StringComparer.Ordinal);

		public string? Match(string expression)
		{
			lock (_matches)
			{
				if (_matches.TryGetValue(expression, out string? remembered)) return remembered;
			}

			string? found = Find(expression);
			lock (_matches)
			{
				_matches[expression] = found;
			}

			return found;
		}
	}

	sealed class Frozen
	{
		readonly Lock _gate = new();
		FrozenDictionary<string, string?> _snapshot = FrozenDictionary<string, string?>.Empty;

		public string? Match(string expression)
			=> Volatile.Read(ref _snapshot).TryGetValue(expression, out string? remembered)
				? remembered
				: Remember(expression);

		string? Remember(string expression)
		{
			string? found = Find(expression);
			lock (_gate)
			{
				FrozenDictionary<string, string?> current = _snapshot;
				var next = new Dictionary<string, string?>(current, StringComparer.Ordinal) { [expression] = found };
				Volatile.Write(ref _snapshot, next.ToFrozenDictionary(StringComparer.Ordinal));
			}

			return found;
		}
	}

	sealed class Snapshot
	{
		readonly Lock _gate = new();
		Dictionary<string, string?> _snapshot = new(StringComparer.Ordinal);

		public string? Match(string expression)
			=> Volatile.Read(ref _snapshot).TryGetValue(expression, out string? remembered)
				? remembered
				: Remember(expression);

		string? Remember(string expression)
		{
			string? found = Find(expression);
			lock (_gate)
			{
				var next = new Dictionary<string, string?>(_snapshot, StringComparer.Ordinal) { [expression] = found };
				Volatile.Write(ref _snapshot, next);
			}

			return found;
		}
	}

	// The keys are read round robin, so the count is a power of two.
	[Params(1, 4, 16)]
	public int Keys { get; set; }

	[Params(1, 8)]
	public int Threads { get; set; }

	string[] _expressions = [];
	Locked _locked = new();
	Frozen _frozen = new();
	Snapshot _snapshot = new();

	[GlobalSetup]
	public void Setup()
	{
		_expressions = [.. Enumerable.Range(0, Keys).Select(i => "argument" + i)];
		_locked = new();
		_frozen = new();
		_snapshot = new();

		_lockedMatch = _locked.Match;
		_frozenMatch = _frozen.Match;
		_snapshotMatch = _snapshot.Match;

		// Every key is remembered before the first read is timed.
		foreach (string expression in _expressions)
		{
			_locked.Match(expression);
			_frozen.Match(expression);
			_snapshot.Match(expression);
		}
	}

	int Run(Func<string, string?> match)
	{
		if (Threads == 1) return Read(match);

		var tasks = new Task<int>[Threads];
		for (int t = 0; t < tasks.Length; t++) tasks[t] = Task.Factory.StartNew(() => Read(match), TaskCreationOptions.LongRunning);
		Task.WaitAll(tasks);
		return tasks[0].Result;
	}

	int Read(Func<string, string?> match)
	{
		string[] expressions = _expressions;
		int mask = expressions.Length - 1;
		int found = 0;
		for (int i = 0; i < Reads; i++)
		{
			if (match(expressions[i & mask]) is not null) found++;
		}

		return found;
	}

	Func<string, string?>? _lockedMatch;
	Func<string, string?>? _frozenMatch;
	Func<string, string?>? _snapshotMatch;

	[Benchmark(Baseline = true)]
	public int LockedDictionary() => Run(_lockedMatch!);

	[Benchmark]
	public int FrozenSnapshot() => Run(_frozenMatch!);

	[Benchmark]
	public int DictionarySnapshot() => Run(_snapshotMatch!);
}
