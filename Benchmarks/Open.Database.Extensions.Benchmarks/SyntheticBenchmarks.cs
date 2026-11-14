using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// Which parameter values a run uses. The defaults are the whole catalog; a staged run narrows them on the command line
/// (<c>--widths 1,5,20 --rows 1,1000 --row-cost 0 --extra-sets 0 --next-result-cost 0</c>, read by Program and passed on through these variables).
/// </summary>
public static class SyntheticParams
{
	public const string WidthsVariable = "ODX_SYN_WIDTHS", RowsVariable = "ODX_SYN_ROWS", RowCostVariable = "ODX_SYN_ROWCOST",
		ExtraSetsVariable = "ODX_SYN_EXTRASETS", NextResultCostVariable = "ODX_SYN_NRCOST";

	public static IEnumerable<int> Widths => Read(WidthsVariable, SyntheticCatalog.Widths);
	public static IEnumerable<int> Rows => Read(RowsVariable, SyntheticCatalog.RowCounts);
	public static IEnumerable<int> RowCosts => Read(RowCostVariable, [SyntheticOptions.DefaultRowCostIterations]);
	public static IEnumerable<int> ExtraSets => Read(ExtraSetsVariable, [0]);
	public static IEnumerable<int> NextResultCosts => Read(NextResultCostVariable, [SyntheticOptions.DefaultNextResultCostIterations]);

	/// <summary>Maps a command-line option to its variable; returns false when the option is not one of ours.</summary>
	public static bool TryGetVariable(string option, out string variable)
	{
		variable = option switch
		{
			"--widths" => WidthsVariable,
			"--rows" => RowsVariable,
			"--row-cost" => RowCostVariable,
			"--extra-sets" => ExtraSetsVariable,
			"--next-result-cost" => NextResultCostVariable,
			_ => "",
		};
		return variable.Length > 0;
	}

	public static int[] Parse(string? list, int[] fallback)
	{
		if (string.IsNullOrWhiteSpace(list)) return fallback;
		var values = new List<int>();
		foreach (string part in list.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
			values.Add(int.Parse(part, System.Globalization.CultureInfo.InvariantCulture));
		return values.Count == 0 ? fallback : [.. values];
	}

	static int[] Read(string variable, int[] fallback) => Parse(Environment.GetEnvironmentVariable(variable), fallback);
}

/// <summary>
/// Adds the reader-open and drain columns. Total time is BenchmarkDotNet's own Mean. The numbers come from the recorder in this process,
/// so they exist only for an in-process run (<c>--inprocess</c>, as bench-guard.ps1 runs); out of process the cells say so.
/// </summary>
public sealed class SyntheticConfig : ManualConfig
{
	public SyntheticConfig()
	{
		Add(DefaultConfig.Instance);
		AddColumn(new OpenTimeColumn("ReaderOpen", "Reader-open: from ExecuteReader until the reader is closed, per operation, median over the last readers; the drain is inside it", static s => s.MedianOpenUs),
			new OpenTimeColumn("Drain", "Time inside NextResult/NextResultAsync, per operation, median over the last readers", static s => s.MedianDrainUs));
	}
}

sealed class OpenTimeColumn(string name, string legend, Func<OpenTimeSummary, double> pick) : IColumn
{
	public string Id => "Synthetic." + name;
	public string ColumnName => name + " (us)";
	public string Legend => legend;
	public bool AlwaysShow => true;
	public ColumnCategory Category => ColumnCategory.Custom;
	public int PriorityInCategory => 0;
	public bool IsNumeric => true;
	public UnitType UnitType => UnitType.Time;
	public bool IsAvailable(Summary summary) => true;
	public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
	public string GetValue(Summary summary, BenchmarkCase benchmarkCase) => GetValue(summary, benchmarkCase, SummaryStyle.Default);

	public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
		=> SyntheticBenchmarks.Results.TryGetValue(SyntheticBenchmarks.Key(benchmarkCase.Descriptor.WorkloadMethod.Name, benchmarkCase.Parameters.Items.Select(static p => (p.Name, p.Value))), out var s)
			? pick(s).ToString("F2", System.Globalization.CultureInfo.InvariantCulture)
			: "n/a (not in-process)";
}

/// <summary>
/// V0, V0b, V1 and V2, sync and async where the path differs, over the synthetic provider.
/// Run it in process (<c>--inprocess</c>) to get the ReaderOpen and Drain columns; BenchmarkDotNet's Mean is the total.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SyntheticConfig))]
public class SyntheticBenchmarks
{
	/// <summary>The recorder numbers of every case run in this process, by <see cref="Key"/>. Filled when a case is cleaned up.</summary>
	public static readonly ConcurrentDictionary<string, OpenTimeSummary> Results = new();

	public static string Key(string method, IEnumerable<(string Name, object Value)> parameters)
		=> method + "|" + string.Join(",", parameters.OrderBy(static p => p.Name, StringComparer.Ordinal).Select(static p => p.Name + "=" + p.Value));

	[ParamsSource(nameof(WidthsValues), Priority = 0)]
	public int Width { get; set; }

	[ParamsSource(nameof(RowsValues), Priority = 1)]
	public int Rows { get; set; }

	[ParamsSource(nameof(RowCostsValues), Priority = 2)]
	public int RowCost { get; set; }

	[ParamsSource(nameof(ExtraSetsValues), Priority = 3)]
	public int ExtraSets { get; set; }

	[ParamsSource(nameof(NextResultCostsValues), Priority = 4)]
	public int NextResultCost { get; set; }

	public static IEnumerable<int> WidthsValues => SyntheticParams.Widths;
	public static IEnumerable<int> RowsValues => SyntheticParams.Rows;
	public static IEnumerable<int> RowCostsValues => SyntheticParams.RowCosts;
	public static IEnumerable<int> ExtraSetsValues => SyntheticParams.ExtraSets;
	public static IEnumerable<int> NextResultCostsValues => SyntheticParams.NextResultCosts;

	SyntheticConnection _conn = null!;
	Func<System.Data.IDataRecord, SynRow> _hand = null!;
	string _label = "";

	[GlobalSetup]
	public void Setup()
	{
		_conn = new SyntheticConnection(
			SyntheticTable.Create(Width, Rows),
			new SyntheticOptions { RowCostIterations = RowCost, ExtraResultSets = ExtraSets, NextResultCostIterations = NextResultCost });
		_hand = SyntheticVariants.Hand(Width);
	}

	// The label is the benchmark method that ran, so the recorder's numbers can be filed under the case the column asks about.
	[GlobalCleanup]
	public void Cleanup()
	{
		var summary = _conn.Options.Recorder.Summary();
		if (_label.Length == 0 || summary.Readers == 0) return;
		var key = Key(_label, [(nameof(Width), Width), (nameof(Rows), Rows), (nameof(RowCost), RowCost), (nameof(ExtraSets), ExtraSets), (nameof(NextResultCost), NextResultCost)]);
		Results[key] = summary;
		Console.WriteLine($"// SYNTHETIC {key} {summary}");
	}

	[Benchmark]
	public List<object[]> V0_Sync() { _label = nameof(V0_Sync); return SyntheticVariants.V0(_conn); }

	[Benchmark]
	public ValueTask<List<object[]>> V0_Async() { _label = nameof(V0_Async); return SyntheticVariants.V0Async(_conn); }

	[Benchmark]
	public List<SynRow> V0b_Sync() { _label = nameof(V0b_Sync); return SyntheticVariants.V0b(_conn); }

	[Benchmark]
	public ValueTask<List<SynRow>> V0b_Async() { _label = nameof(V0b_Async); return SyntheticVariants.V0bAsync(_conn); }

	[Benchmark(Baseline = true)]
	public List<SynRow> V1_Sync() { _label = nameof(V1_Sync); return SyntheticVariants.V1(_conn); }

	[Benchmark]
	public ValueTask<List<SynRow>> V1_Async() { _label = nameof(V1_Async); return SyntheticVariants.V1Async(_conn); }

	[Benchmark]
	public ValueTask<List<SynRow>> V1_AsyncRead() { _label = nameof(V1_AsyncRead); return SyntheticVariants.V1AsyncRead(_conn); }

	[Benchmark]
	public List<SynRow> V2_Sync() { _label = nameof(V2_Sync); return SyntheticVariants.V2(_conn, _hand); }

	[Benchmark]
	public ValueTask<List<SynRow>> V2_Async() { _label = nameof(V2_Async); return SyntheticVariants.V2Async(_conn, _hand); }
}
