using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Open.Database.Extensions.Benchmarks;

// --verify-rp (BS-RP): every real-provider variant produces equal final objects, per provider, through the benchmark class's own Setup.
//   dotnet run -c Release -f net11.0 --project Benchmarks/Open.Database.Extensions.Benchmarks -- --verify-rp --scratch <folder> [--sqlite-dir <folder>] [--connection "<string>"]
// For each provider (SQL Server LocalDB, a SQLite file), width 5 and 20, rows 1 and 100 (and 10,000, for the timing table): every [Benchmark] method is invoked and its
// final objects are compared with an oracle built from the seed data (SynRow.FromTable; V3's SynItem through SynItem.From); each variant must also record exactly
// one reader per operation, with a reader-open time that is above 0 and not above the operation's own wall time.
// When LocalDB is absent (or CREATE DATABASE is refused) the SQLite part still runs and the report says LOUDLY that SQL Server was skipped and is NOT verified.
// Not a benchmark: the timing table is a stopwatch over a few operations, indicative only (it is the basis of the run-time estimate in BS-RP.md).
public static partial class SyntheticChecks
{
	static readonly int[] RpWidths = [5, 20];
	static readonly int[] RpRows = [1, 100, 10_000];

	sealed record RpTiming(string Provider, int Width, int Rows, string Method, double WallUs, double OpenUs, double BytesPerOp, int Ops);

	sealed record RpSetup(string Provider, int Width, int Rows, double SetupSeconds, double CleanupSeconds);

	public static int VerifyRealProviders()
	{
		if (RealProviderParams.Get(RealProviderParams.ScratchVariable) is null && RealProviderParams.Get(RealProviderParams.SqliteDirVariable) is null)
		{
			Console.WriteLine("--verify-rp: pass --scratch <folder> (the LocalDB database files, and the SQLite file unless --sqlite-dir is given), a folder not on drive C:.");
			return 2;
		}

		var report = new Report();
		var timings = new List<RpTiming>();
		var setup = new List<RpSetup>();
		string? skipped = null;
		Console.WriteLine("== real providers: every variant against the oracle built from the seed data; one reader per operation; reader-open inside the operation ==");
		foreach (string provider in new[] { RealProviderParams.SqlServer, RealProviderParams.Sqlite })
		{
			foreach (int width in RpWidths)
			{
				foreach (int rows in RpRows)
				{
					if (provider == RealProviderParams.SqlServer && skipped is not null) continue;
					string? reason = VerifyRpCell(report, provider, width, rows, timings, setup);
					if (reason is not null) skipped = reason;
				}
			}
		}

		Console.WriteLine();
		PrintRpTimings(timings, setup);

		// Nothing may be left behind in the scratch folders.
		foreach (string variable in new[] { RealProviderParams.ScratchVariable, RealProviderParams.SqliteDirVariable })
		{
			if (RealProviderParams.Get(variable) is not { } folder || !Directory.Exists(folder)) continue;
			var left = Directory.EnumerateFiles(folder, "*").Where(static f => Path.GetFileName(f).StartsWith("rp-", StringComparison.Ordinal) || Path.GetFileName(f).StartsWith("ode_rp_", StringComparison.Ordinal)).ToList();
			report.Check(left.Count == 0, $"no test file left in {variable}'s folder ({left.Count} found: {string.Join(", ", left.Select(Path.GetFileName))})");
		}

		Console.WriteLine();
		if (skipped is not null)
		{
			Console.WriteLine("**************************************************************************************");
			Console.WriteLine("*** SQL SERVER (LOCALDB) WAS SKIPPED AND IS NOT VERIFIED. Only SQLite was checked.   ***");
			Console.WriteLine("**************************************************************************************");
			Console.WriteLine("reason: " + skipped);
		}

		string scope = skipped is null ? "both providers" : "SQLite ONLY, SQL SERVER NOT VERIFIED (skipped, see above)";
		Console.WriteLine(report.Failures == 0 ? $"VERIFY-RP PASS ({report.Checks} checks; {scope})" : $"VERIFY-RP FAIL ({report.Failures} of {report.Checks} checks failed; {scope})");
		return report.Failures == 0 ? 0 : 1;
	}

	// Returns the reason when the provider cannot be used (the caller decides what that means); null when the cell ran.
	static string? VerifyRpCell(Report report, string provider, int width, int rows, List<RpTiming> timings, List<RpSetup> setup)
	{
		string where = $"{provider} width={width} rows={rows}";
		var bench = new RealProviderBenchmarks { Provider = provider, Width = width, Rows = rows };
		var clock = Stopwatch.StartNew();
		try
		{
			bench.Setup();
		}
		catch (RealProviderUnavailableException ex) when (provider == RealProviderParams.SqlServer)
		{
			Console.WriteLine($"!!! {where}: SQL SERVER SKIPPED: {ex.Message}");
			return ex.Message;
		}
		catch (Exception ex)
		{
			report.Fail($"{where}: Setup threw {ex.GetType().Name}: {ex.Message}");
			return null;
		}

		double setupSeconds = clock.Elapsed.TotalSeconds;
		try
		{
			var table = bench.Case.Table;
			var expectedRows = new List<SynRow>(rows);
			var expectedItems = new List<SynItem>(rows);
			for (int i = 0; i < rows; i++)
			{
				expectedRows.Add(SynRow.FromTable(table, i));
				expectedItems.Add(SynItem.From(expectedRows[i]));
			}

			var recorder = bench.Case.Recorder;
			int ops = rows switch { <= 1 => 50, <= 100 => 30, _ => 5 };
			int invoked = 0;
			foreach (var method in typeof(RealProviderBenchmarks).GetMethods().Where(static m => m.GetCustomAttributes(typeof(BenchmarkAttribute), false).Length > 0))
			{
				recorder.Reset();
				object? result;
				try { result = method.Invoke(bench, null); }
				catch (Exception ex)
				{
					report.Fail($"{where} {method.Name} threw {(ex.InnerException ?? ex).GetType().Name}: {(ex.InnerException ?? ex).Message}");
					continue;
				}

				invoked++;
				switch (result)
				{
					case List<SynRow> typed:
						Compare(report, method.Name, where, expectedRows, typed);
						break;
					case List<SynItem> domain:
						CompareItems(report, method.Name, where, expectedItems, domain);
						break;
					default:
						report.Fail($"{where} {method.Name}: unexpected result {result?.GetType().Name ?? "null"}");
						break;
				}

				report.Check(recorder.Readers == 1, $"{method.Name} {where}: {recorder.Readers} readers recorded for one operation, expected 1");

				// The recorded reader-open time is inside the operation: above 0 and not above the wall time per operation.
				recorder.Reset();
				long allocated0 = GC.GetAllocatedBytesForCurrentThread();
				long t0 = Stopwatch.GetTimestamp();
				for (int k = 0; k < ops; k++) method.Invoke(bench, null);
				double wallUs = (Stopwatch.GetTimestamp() - t0) * 1_000_000d / Stopwatch.Frequency / ops;
				double bytes = (GC.GetAllocatedBytesForCurrentThread() - allocated0) / (double)ops;
				var summary = recorder.Summary();
				report.Check(summary.Readers == ops, $"{method.Name} {where}: {summary.Readers} readers over {ops} operations");
				report.Check(summary.MeanOpenUs > 0 && summary.MeanOpenUs <= wallUs, $"{method.Name} {where}: reader-open {summary.MeanOpenUs:F1} us must be above 0 and not above the operation's wall time {wallUs:F1} us");
				timings.Add(new RpTiming(provider, width, rows, method.Name, wallUs, summary.MeanOpenUs, bytes, ops));
			}

			Console.WriteLine($"  ok  {where}: {invoked} benchmark methods through Setup, output equal to the oracle ({rows} rows); data: {bench.Case.Describe}");
		}
		finally
		{
			var cleanup = Stopwatch.StartNew();
			try
			{
				bench.Cleanup();
			}
			catch (Exception ex)
			{
				report.Fail($"{where}: Cleanup threw {ex.GetType().Name}: {ex.Message}");
			}

			report.Check(bench.Case.FilesGone, $"{where}: the test files are gone after Cleanup");
			setup.Add(new RpSetup(provider, width, rows, setupSeconds, cleanup.Elapsed.TotalSeconds));
		}

		return null;
	}

	static void PrintRpTimings(List<RpTiming> timings, List<RpSetup> setup)
	{
		var inv = CultureInfo.InvariantCulture;
		Console.WriteLine("== timing table (stopwatch over a few operations, one process, NOT a benchmark): wall time per operation, reader-open, allocated bytes per operation ==");
		foreach (var group in timings.GroupBy(static t => (t.Provider, t.Width, t.Rows)))
		{
			Console.WriteLine($"  {group.Key.Provider} width={group.Key.Width} rows={group.Key.Rows} ({group.First().Ops} operations per method)");
			foreach (var t in group) Console.WriteLine($"      {t.Method,-20} wall {t.WallUs.ToString("F1", inv),10} us   reader-open {t.OpenUs.ToString("F1", inv),10} us   alloc {t.BytesPerOp.ToString("F0", inv),10} B");
		}

		Console.WriteLine("== setup and cleanup seconds per case (create the data and drop it; BenchmarkDotNet pays them once per benchmark case) ==");
		foreach (var s in setup) Console.WriteLine($"  {s.Provider,-9} width={s.Width,2} rows={s.Rows,6}: setup {s.SetupSeconds.ToString("F2", inv)} s, cleanup {s.CleanupSeconds.ToString("F2", inv)} s");
		foreach (var group in setup.GroupBy(static s => s.Provider))
			Console.WriteLine($"  {group.Key}: mean setup {group.Average(static s => s.SetupSeconds).ToString("F2", inv)} s, mean cleanup {group.Average(static s => s.CleanupSeconds).ToString("F2", inv)} s, max (setup + cleanup) {group.Max(static s => s.SetupSeconds + s.CleanupSeconds).ToString("F2", inv)} s");
	}
}
