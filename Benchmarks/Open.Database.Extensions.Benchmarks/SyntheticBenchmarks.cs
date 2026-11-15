using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
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
		ExtraSetsVariable = "ODX_SYN_EXTRASETS", NextResultCostVariable = "ODX_SYN_NRCOST", PaceVariable = "ODX_SYN_PACE",
		BufferRowsVariable = "ODX_SYN_BUFFER_ROWS", RefillUsVariable = "ODX_SYN_REFILL_US", PressureVariable = "ODX_SYN_PRESSURE";

	public static IEnumerable<int> Widths => Read(WidthsVariable, SyntheticCatalog.Widths);
	public static IEnumerable<int> Rows => Read(RowsVariable, SyntheticCatalog.RowCounts);
	public static IEnumerable<int> RowCosts => Read(RowCostVariable, [SyntheticOptions.DefaultRowCostIterations]);
	public static IEnumerable<int> ExtraSets => Read(ExtraSetsVariable, [0]);
	public static IEnumerable<int> NextResultCosts => Read(NextResultCostVariable, [SyntheticOptions.DefaultNextResultCostIterations]);
	// Consumer pace: Spin iterations per item, paid only by the variants that run caller code while the reader is open or stream. 0 = off.
	public static IEnumerable<int> Paces => Read(PaceVariable, [0]);
	// BS-SA: a buffer refill every N rows (0 = off), the refill time in microseconds, and K busy thread-pool items (0 = off; the word auto means twice the processor count).
	public static IEnumerable<int> BufferRows => Read(BufferRowsVariable, [SyntheticOptions.DefaultBufferRows]);
	public static IEnumerable<int> RefillUs => Read(RefillUsVariable, [SyntheticOptions.DefaultRefillCostUs]);
	public static IEnumerable<int> Pressures => ReadPressure(Environment.GetEnvironmentVariable(PressureVariable));

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
			"--pace" => PaceVariable,
			"--buffer-rows" => BufferRowsVariable,
			"--refill-us" => RefillUsVariable,
			"--pressure" => PressureVariable,
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

	static int[] ReadPressure(string? list)
	{
		if (string.IsNullOrWhiteSpace(list)) return [0];
		return [.. list.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(ThreadPoolPressure.Parse)];
	}
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
		AddColumn(new OpenTimeColumn("ReaderOpen", "Reader-open: from ExecuteReader until the reader is closed, summed over the readers of one operation. MEAN per operation over the iterations BenchmarkDotNet's Mean is made of (WorkloadActual minus the outliers it removed); the drain is inside it", static s => s.OpenUs),
			new OpenTimeColumn("Drain", "Time inside NextResult/NextResultAsync, per operation: MEAN over the same iterations as ReaderOpen", static s => s.DrainUs));
	}
}

sealed class OpenTimeColumn(string name, string legend, Func<MeasuredOpenSummary, double> pick) : IColumn
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

	// Three decimals: the check script compares this with Mean, and a case of 100 ns would lose 5% to two.
	public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
	{
		if (!SyntheticBenchmarks.Results.TryGetValue(SyntheticBenchmarks.Key(benchmarkCase.Descriptor.WorkloadMethod.Name, benchmarkCase.Parameters.Items.Select(static p => (p.Name, p.Value))), out var recorded))
			return "n/a (not in-process)";
		if (summary[benchmarkCase] is not { } report) return "n/a (no report)";
		var actual = report.AllMeasurements.Where(static m => m.Is(IterationMode.Workload, IterationStage.Actual)).ToList();
		return MeasuredOpenTime.TrySummarize(recorded, actual, report.GetResultRuns(), out var s, out string reason)
			? pick(s).ToString("F3", System.Globalization.CultureInfo.InvariantCulture)
			: "n/a (" + reason + ")";
	}
}

/// <summary>
/// V0, V0b, V1 and V2, sync and async where the path differs, over the synthetic provider.
/// Run it in process (<c>--inprocess</c>) to get the ReaderOpen and Drain columns; BenchmarkDotNet's Mean is the total.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SyntheticConfig))]
public class SyntheticBenchmarks
{
	/// <summary>The WorkloadActual iterations the recorder saw for every case run in this process, by <see cref="Key"/>. Filled when a case is cleaned up.</summary>
	public static readonly ConcurrentDictionary<string, MeasuredIteration[]> Results = new();

	/// <summary>Files the recorder's iterations under the case (appended, so a second launch of the same case adds to the first) and logs a line.</summary>
	internal static void FileResults(string key, SyntheticRecorder recorder)
	{
		SyntheticIterationListener.Detach(recorder);
		var iterations = recorder.MeasuredIterations;
		Results.AddOrUpdate(key, iterations, (_, earlier) => [.. earlier, .. iterations]);
		var all = MeasuredOpenTime.MeanOfAll(iterations);
		Console.WriteLine($"// SYNTHETIC {key} {recorder.Summary()} | WorkloadActual iterations={all.Iterations}, per operation, mean over all of them (outliers not yet removed): reader-open={all.OpenUs:F3}us drain={all.DrainUs:F3}us");
	}

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

	/// <summary>Consumer pace: Spin iterations per item for the variants that run caller code while the reader is open or stream (V3, V3s, V4a, V4b, V6c and their Dapper twins). 0 = off.</summary>
	[ParamsSource(nameof(PacesValues), Priority = 5)]
	public int Pace { get; set; }

	/// <summary>BS-SA: the provider needs a refill every this many rows (0 = never). A sync Read blocks for the refill; ReadAsync returns an incomplete Task finished from another thread.</summary>
	[ParamsSource(nameof(BufferRowsValues), Priority = 6)]
	public int BufferRows { get; set; }

	/// <summary>BS-SA: microseconds one refill takes.</summary>
	[ParamsSource(nameof(RefillUsValues), Priority = 7)]
	public int RefillUs { get; set; }

	/// <summary>BS-SA: busy thread-pool items kept running while the case is measured (0 = none). ODX_SYN_PRESSURE=auto is twice the processor count.</summary>
	[ParamsSource(nameof(PressuresValues), Priority = 8)]
	public int Pressure { get; set; }

	public static IEnumerable<int> WidthsValues => SyntheticParams.Widths;
	public static IEnumerable<int> RowsValues => SyntheticParams.Rows;
	public static IEnumerable<int> RowCostsValues => SyntheticParams.RowCosts;
	public static IEnumerable<int> ExtraSetsValues => SyntheticParams.ExtraSets;
	public static IEnumerable<int> NextResultCostsValues => SyntheticParams.NextResultCosts;
	public static IEnumerable<int> PacesValues => SyntheticParams.Paces;
	public static IEnumerable<int> BufferRowsValues => SyntheticParams.BufferRows;
	public static IEnumerable<int> RefillUsValues => SyntheticParams.RefillUs;
	public static IEnumerable<int> PressuresValues => SyntheticParams.Pressures;

	// The connection of this case; --verify drives the benchmark methods through Setup and reads its recorder.
	internal SyntheticConnection Connection { get; private set; } = null!;
	Func<System.Data.IDataRecord, SynRow> _hand = null!;
	Func<System.Data.IDataRecord, SynItem> _handM = null!;
	Hands _hands = null!;
	Func<SyntheticConnection, System.Collections.IList> _k = null!;
	string _label = "";

	[GlobalSetup]
	public void Setup()
	{
		Connection = new SyntheticConnection(
			SyntheticTable.Create(Width, Rows),
			new SyntheticOptions { RowCostIterations = RowCost, ExtraResultSets = ExtraSets, NextResultCostIterations = NextResultCost, BufferRows = BufferRows, RefillCostUs = RefillUs });
		_hand = SyntheticVariants.Hand(Width);
		_handM = SyntheticVariantsB.HandMassaged(Width);
		_hands = Hands.For(Width);
		_k = SyntheticVariantsC.K(Width);
		SyntheticIterationListener.Attach(Connection.Options.Recorder);
		ThreadPoolPressure.Start(Pressure); // BS-SA: busy before the first iteration; Cleanup stops it. Pressure 0 does nothing.
	}

	// The label is the benchmark method that ran, so the recorder's numbers can be filed under the case the column asks about.
	[GlobalCleanup]
	public void Cleanup()
	{
		ThreadPoolPressure.Stop();
		if (_label.Length == 0) return;
		var key = Key(_label, [(nameof(Width), Width), (nameof(Rows), Rows), (nameof(RowCost), RowCost), (nameof(ExtraSets), ExtraSets), (nameof(NextResultCost), NextResultCost), (nameof(Pace), Pace), (nameof(BufferRows), BufferRows), (nameof(RefillUs), RefillUs), (nameof(Pressure), Pressure)]);
		FileResults(key, Connection.Options.Recorder);
		if (_label.StartsWith("V9a", StringComparison.Ordinal))
			Console.WriteLine($"// SYNTHETIC {key} BufferBytes={SyntheticVariantsC.LastBufferBytes} per operation (rented array lengths x element size, plus the null bitmaps; ArrayPool memory, so NOT in Allocated); pool tally rented={PoolTally.Rented} returned={PoolTally.Returned}");
	}

	[Benchmark]
	public List<object[]> V0_Sync() { _label = nameof(V0_Sync); return SyntheticVariants.V0(Connection); }

	[Benchmark]
	public ValueTask<List<object[]>> V0_Async() { _label = nameof(V0_Async); return SyntheticVariants.V0Async(Connection); }

	[Benchmark]
	public List<SynRow> V0b_Sync() { _label = nameof(V0b_Sync); return SyntheticVariants.V0b(Connection); }

	[Benchmark]
	public ValueTask<List<SynRow>> V0b_Async() { _label = nameof(V0b_Async); return SyntheticVariants.V0bAsync(Connection); }

	[Benchmark(Baseline = true)]
	public List<SynRow> V1_Sync() { _label = nameof(V1_Sync); return SyntheticVariants.V1(Connection); }

	[Benchmark]
	public ValueTask<List<SynRow>> V1_Async() { _label = nameof(V1_Async); return SyntheticVariants.V1Async(Connection); }

	[Benchmark]
	public ValueTask<List<SynRow>> V1_AsyncRead() { _label = nameof(V1_AsyncRead); return SyntheticVariants.V1AsyncRead(Connection); }

	[Benchmark]
	public List<SynRow> V2_Sync() { _label = nameof(V2_Sync); return SyntheticVariants.V2(Connection, _hand); }

	[Benchmark]
	public ValueTask<List<SynRow>> V2_Async() { _label = nameof(V2_Async); return SyntheticVariants.V2Async(Connection, _hand); }

	// ---- V3 to V6 (BS-B): the domain object is SynItem; Pace applies to V3, V3s, V4a, V4b, V6c only ----------------------------------------------------------------
	[Benchmark]
	public List<SynItem> V3_Sync() { _label = nameof(V3_Sync); return SyntheticVariantsB.V3(Connection, Pace); }

	[Benchmark]
	public ValueTask<List<SynItem>> V3_Async() { _label = nameof(V3_Async); return SyntheticVariantsB.V3Async(Connection, Pace); }

	[Benchmark]
	public ValueTask<List<SynItem>> V3s_Async() { _label = nameof(V3s_Async); return SyntheticVariantsB.V3s(Connection, Pace); }

	[Benchmark]
	public List<SynItem> V4a_Sync() { _label = nameof(V4a_Sync); return SyntheticVariantsB.V4a(Connection, Width, Pace); }

	[Benchmark]
	public ValueTask<List<SynItem>> V4a_Async() { _label = nameof(V4a_Async); return SyntheticVariantsB.V4aAsync(Connection, Width, Pace); }

	[Benchmark]
	public List<SynItem> V4b_Sync() { _label = nameof(V4b_Sync); return SyntheticVariantsB.V4b(Connection, Width, Pace); }

	[Benchmark]
	public ValueTask<List<SynItem>> V4b_Async() { _label = nameof(V4b_Async); return SyntheticVariantsB.V4bAsync(Connection, Width, Pace); }

	[Benchmark]
	public List<SynItem> V5_Sync() { _label = nameof(V5_Sync); return SyntheticVariantsB.V5(Connection); }

	[Benchmark]
	public ValueTask<List<SynItem>> V5_Async() { _label = nameof(V5_Async); return SyntheticVariantsB.V5Async(Connection); }

	[Benchmark]
	public List<SynItem> V6a_Sync() { _label = nameof(V6a_Sync); return SyntheticVariantsB.V6a(Connection, _handM); }

	[Benchmark]
	public ValueTask<List<SynItem>> V6a_Async() { _label = nameof(V6a_Async); return SyntheticVariantsB.V6aAsync(Connection, _handM); }

	// V6b: RESERVED for DS1a. No method.

	[Benchmark]
	public List<SynItem> V6c_Sync() { _label = nameof(V6c_Sync); return SyntheticVariantsB.V6c(Connection, Width, Pace); }

	[Benchmark]
	public ValueTask<List<SynItem>> V6c_Async() { _label = nameof(V6c_Async); return SyntheticVariantsB.V6cAsync(Connection, Width, Pace); }

	[Benchmark]
	public List<SynItem> V6d_Sync() { _label = nameof(V6d_Sync); return SyntheticVariantsB.V6d(Connection, Width); }

	[Benchmark]
	public ValueTask<List<SynItem>> V6d_Async() { _label = nameof(V6d_Async); return SyntheticVariantsB.V6dAsync(Connection, Width); }

	// ---- BS-C: the DS1a candidates (SyntheticVariantsC). V6b-F, V6b-E, V6b-K, V9a-on, V9b, V9d-M produce the massaged SynItem; V9c, V9d, V9e and V9a-off produce SynRow like V1 ----------------
	[Benchmark]
	public List<SynRow> V1_Fresh_Sync() { _label = nameof(V1_Fresh_Sync); return SyntheticVariantsC.V1Fresh(Connection); }

	[Benchmark]
	public List<SynRow> V9c_Db_Sync() { _label = nameof(V9c_Db_Sync); return SyntheticVariantsC.V9cDb(Connection, _hands); }

	[Benchmark]
	public List<SynRow> V9c_Syn_Sync() { _label = nameof(V9c_Syn_Sync); return SyntheticVariantsC.V9cSyn(Connection, _hands); }

	[Benchmark]
	public List<SynRow> V9d_Sync() { _label = nameof(V9d_Sync); return SyntheticVariantsC.V9d(Connection, _hands); }

	[Benchmark]
	public List<SynItem> V9d_M_Sync() { _label = nameof(V9d_M_Sync); return SyntheticVariantsC.V9dM(Connection, _hands); }

	[Benchmark]
	public List<SynRow> V9e_Sync() { _label = nameof(V9e_Sync); return SyntheticVariantsC.V9e(Connection, _hands); }

	[Benchmark]
	public List<SynItem> V6b_F_Sync() { _label = nameof(V6b_F_Sync); return SyntheticVariantsC.V6bF(Connection, _hands); }

	[Benchmark]
	public List<SynItem> V6b_E_Sync() { _label = nameof(V6b_E_Sync); return SyntheticVariantsC.V6bE(Connection, _hands); }

	[Benchmark]
	public System.Collections.IList V6b_K_Sync() { _label = nameof(V6b_K_Sync); return _k(Connection); }

	[Benchmark]
	public List<SynItem> V9a_On_Sync() { _label = nameof(V9a_On_Sync); return SyntheticVariantsC.V9aOn(Connection, _hands); }

	[Benchmark]
	public List<SynRow> V9a_Off_Sync() { _label = nameof(V9a_Off_Sync); return SyntheticVariantsC.V9aOff(Connection, _hands); }

	[Benchmark]
	public List<SynItem> V9b_Sync() { _label = nameof(V9b_Sync); return SyntheticVariantsC.V9b(Connection, Width); }

	// ---- V7: Dapper, the shapes it can do (SyntheticDapper lists the ones it cannot) ----------------------------------------------------------------
	[Benchmark]
	public List<SynRow> V7_V1_Sync() { _label = nameof(V7_V1_Sync); return SyntheticDapper.V1(Connection); }

	[Benchmark]
	public Task<List<SynRow>> V7_V1_Async() { _label = nameof(V7_V1_Async); return SyntheticDapper.V1Async(Connection); }

	[Benchmark]
	public List<SynItem> V7_V3_Sync() { _label = nameof(V7_V3_Sync); return SyntheticDapper.V3(Connection, Pace); }

	[Benchmark]
	public Task<List<SynItem>> V7_V3s_Async() { _label = nameof(V7_V3s_Async); return SyntheticDapper.V3s(Connection, Pace); }

	[Benchmark]
	public List<SynItem> V7_V5_Sync() { _label = nameof(V7_V5_Sync); return SyntheticDapper.V5(Connection); }

	[Benchmark]
	public Task<List<SynItem>> V7_V5_Async() { _label = nameof(V7_V5_Async); return SyntheticDapper.V5Async(Connection); }

	[Benchmark]
	public List<SynItem> V7_V6c_Sync() { _label = nameof(V7_V6c_Sync); return SyntheticDapper.V6c(Connection, Width, Pace); }

	[Benchmark]
	public List<SynItem> V7_V6d_Sync() { _label = nameof(V7_V6d_Sync); return SyntheticDapper.V6d(Connection, Width); }

	[Benchmark]
	public Task<List<SynItem>> V7_V6d_Async() { _label = nameof(V7_V6d_Async); return SyntheticDapper.V6dAsync(Connection, Width); }
}

/// <summary>
/// V8: the scalar (1 row x 1 column) variants, library and Dapper, over a width-1, 1-row table. No parameters (the shape is fixed).
/// Run in process for the ReaderOpen and Drain columns, as for <see cref="SyntheticBenchmarks"/>.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SyntheticConfig))]
public class SyntheticScalarBenchmarks
{
	SyntheticConnection _conn = null!;
	string _label = "";

	[GlobalSetup]
	public void Setup()
	{
		_conn = new SyntheticConnection(SyntheticTable.Create(1, 1), new SyntheticOptions());
		SyntheticIterationListener.Attach(_conn.Options.Recorder);
	}

	[GlobalCleanup]
	public void Cleanup()
	{
		if (_label.Length == 0) return;
		SyntheticBenchmarks.FileResults(SyntheticBenchmarks.Key(_label, []), _conn.Options.Recorder);
	}

	[Benchmark(Baseline = true)]
	public int V8a_Sync() { _label = nameof(V8a_Sync); return SyntheticVariantsB.V8a(_conn); }

	[Benchmark]
	public ValueTask<int> V8a_Async() { _label = nameof(V8a_Async); return SyntheticVariantsB.V8aAsync(_conn); }

	[Benchmark]
	public int V8b_Sync() { _label = nameof(V8b_Sync); return SyntheticVariantsB.V8b(_conn); }

	[Benchmark]
	public ValueTask<int> V8b_Async() { _label = nameof(V8b_Async); return SyntheticVariantsB.V8bAsync(_conn); }

	[Benchmark]
	public int V8c_Sync() { _label = nameof(V8c_Sync); return SyntheticVariantsB.V8c(_conn); }

	[Benchmark]
	public ValueTask<int> V8c_Async() { _label = nameof(V8c_Async); return SyntheticVariantsB.V8cAsync(_conn); }

	[Benchmark]
	public int V7_V8a_Sync() { _label = nameof(V7_V8a_Sync); return SyntheticDapper.V8a(_conn); }

	[Benchmark]
	public Task<int> V7_V8a_Async() { _label = nameof(V7_V8a_Async); return SyntheticDapper.V8aAsync(_conn); }

	[Benchmark]
	public int V7_V8b_Sync() { _label = nameof(V7_V8b_Sync); return SyntheticDapper.V8b(_conn); }

	[Benchmark]
	public Task<int> V7_V8b_Async() { _label = nameof(V7_V8b_Async); return SyntheticDapper.V8bAsync(_conn); }
}

/// <summary>
/// The DateTimeOffset scenario (DS1a 3.1 point 4), outside the width ladder: a three-column table (Id, Title, When), parameter Rows only.
/// <c>Dto_*</c> read When as a DateTimeOffset (no typed getter exists, so the plan reads it by GetValue, which boxes); <c>Dt_V1</c> is the control with a DateTime.
/// Run in process for the ReaderOpen and Drain columns, as for <see cref="SyntheticBenchmarks"/>.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SyntheticConfig))]
public class SyntheticShapeBenchmarks
{
	[ParamsSource(nameof(RowsValues))]
	public int Rows { get; set; }

	public static IEnumerable<int> RowsValues => SyntheticParams.Rows;

	SyntheticConnection _dto = null!, _dt = null!;
	string _label = "";

	[GlobalSetup]
	public void Setup()
	{
		_dto = new SyntheticConnection(SyntheticTable.Create(SyntheticCatalog.DateTimeOffsetColumns, Rows), new SyntheticOptions());
		_dt = new SyntheticConnection(SyntheticTable.Create(SyntheticCatalog.DateTimeColumns, Rows), new SyntheticOptions { Recorder = _dto.Options.Recorder });
		SyntheticIterationListener.Attach(_dto.Options.Recorder);
	}

	[GlobalCleanup]
	public void Cleanup()
	{
		if (_label.Length == 0) return;
		SyntheticBenchmarks.FileResults(SyntheticBenchmarks.Key(_label, [(nameof(Rows), Rows)]), _dto.Options.Recorder);
	}

	[Benchmark]
	public List<DtoRow> Dto_V1_Sync() { _label = nameof(Dto_V1_Sync); return SyntheticVariantsC.DtoV1(_dto); }

	[Benchmark]
	public List<DtRow> Dt_V1_Sync() { _label = nameof(Dt_V1_Sync); return SyntheticVariantsC.DtV1(_dt); }

	[Benchmark]
	public List<DtoRow> Dto_HandBoxed_Sync() { _label = nameof(Dto_HandBoxed_Sync); return SyntheticVariantsC.DtoHandBoxed(_dto); }

	[Benchmark]
	public List<DtoRow> Dto_HandTyped_Sync() { _label = nameof(Dto_HandTyped_Sync); return SyntheticVariantsC.DtoHandTyped(_dto); }
}

/// <summary>
/// Item 11: the one-off <c>Map&lt;T&gt;(pair)</c> and <c>Map&lt;T&gt;(params pairs)</c> overloads build a map on every call; a declared map is built once.
/// One row, width 3, no parameters. OneOff_* use the params overload with three pairs; Declared_* use a RowMap field. List and First of each.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(SyntheticConfig))]
public class OneOffOverloadBenchmarks
{
	public const int Width = 3, Rows = 1;

	SyntheticConnection _conn = null!;
	string _label = "";

	[GlobalSetup]
	public void Setup()
	{
		_conn = new SyntheticConnection(SyntheticTable.Create(Width, Rows), new SyntheticOptions());
		SyntheticIterationListener.Attach(_conn.Options.Recorder);
	}

	[GlobalCleanup]
	public void Cleanup()
	{
		if (_label.Length == 0) return;
		SyntheticBenchmarks.FileResults(SyntheticBenchmarks.Key(_label, []), _conn.Options.Recorder);
	}

	[Benchmark]
	public List<SynRow> OneOff_List_Sync() { _label = nameof(OneOff_List_Sync); return SyntheticVariantsC.OneOffList(_conn); }

	[Benchmark]
	public SynRow OneOff_First_Sync() { _label = nameof(OneOff_First_Sync); return SyntheticVariantsC.OneOffFirst(_conn); }

	[Benchmark]
	public List<SynRow> Declared_List_Sync() { _label = nameof(Declared_List_Sync); return SyntheticVariantsC.DeclaredList(_conn); }

	[Benchmark]
	public SynRow Declared_First_Sync() { _label = nameof(Declared_First_Sync); return SyntheticVariantsC.DeclaredFirst(_conn); }
}
