using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

// BS-SA, IN6: the real check of "sync Read against ReadAsync" (a console mode, NOT BenchmarkDotNet).
//   dotnet run -c Release -f net11.0 --project Benchmarks/Open.Database.Extensions.Benchmarks -- --in6 --scratch <folder> [options]
// Three read paths through ODX Map<T>():  sync       = sync execute + sync Read (ToList)
//                                         async+Read = async execute + sync Read (ToListAsync, the library's default for async)
//                                         EnableAsyncRead = async execute + ReadAsync (EnableAsyncRead().ToListAsync)
// over SQL Server (LocalDB, SqlClient) and a SQLite FILE database, each with thread-pool pressure off and on, plus the calibration of the
// synthetic provider's costs (per-row and per-NextResult time on LocalDB). Reader-open is measured by the timing wrappers (In6Timing.cs):
// from the entry of ExecuteReader to the close of the reader, the same clock as the synthetic recorder.

/// <summary>A row with only an Id and one large string, for the case where a row crosses packets.</summary>
public sealed class In6BigRow
{
	public int Id { get; set; }
	public string? Body { get; set; }
}

/// <summary>The catalog row as SQLite delivers it: the two Guid columns arrive as text, so they are strings here (as in SqlitePost).</summary>
public sealed class In6LiteRow
{
	public int Id { get; set; }
	public DateTime Created { get; set; }
	public string? Title { get; set; }
	public decimal Price { get; set; }
	public int? Qty { get; set; }
	public bool IsActive { get; set; }
	public string? Key { get; set; }
	public double Score { get; set; }
	public string? Note { get; set; }
	public byte[]? Blob { get; set; }
	public long Big { get; set; }
	public string? Body { get; set; }
	public decimal? Amount { get; set; }
	public DateTime? Stamp { get; set; }
	public string? Tag { get; set; }
	public double Ratio { get; set; }
	public bool? Flag { get; set; }
	public string? Link { get; set; }
	public long Count { get; set; }
	public string? Summary { get; set; }
}

/// <summary>What one path of one cell measured: per operation, over the measured operations (the warm-up operations are not in it).</summary>
public sealed record In6Summary(
	string Path, int Ops, double OpenMedianUs, double OpenMinUs, double OpenMaxUs, double TotalMeanUs, double TotalMedianUs,
	double AllocThreadBytes, double AllocProcessBytes, double ReadAsyncCalls, double ReadAsyncIncomplete, double ResumedOnOtherThread,
	double OtherAsyncCalls, double OtherAsyncIncomplete, bool OneReaderPerOperation, double WaitSumMedianUs = 0, double WaitMaxUs = 0, WaitPositions? Positions = null);

sealed class In6Settings
{
	public const string DefaultSqlServer = @"Server=(localdb)\MSSQLLocalDB;Integrated Security=true;Encrypt=False";
	public static readonly string[] Parts = ["sqlserver", "sqlite", "calibration"];
	public int Rows = 10_000, Warmup = 5, Ops = 20;
	public string Pressure = "auto";
	public string SqlServer = DefaultSqlServer;
	public string? Scratch, SqlTcp, PasswordFile, Affinity, Priority;
	public long AffinityMask;
	public ProcessPriorityClass? PriorityClass;
	public bool DryRun, Help;
	public int OpsS3 = -1;
	public string PressureS3 = "both";
	public bool S3PressureOff => PressureS3 == "off";
	public HashSet<string> Scenarios = new(["S1", "S2", "S3"], StringComparer.OrdinalIgnoreCase);
	public int S3Ops => OpsS3 > 0 ? OpsS3 : Ops;
	public int Processors = Environment.ProcessorCount;
	public bool Tcp => SqlTcp is not null;
	public string SqlLabel => Tcp ? "SqlServerTcp" : "SqlServer";
	public HashSet<string> Only = new(Parts, StringComparer.OrdinalIgnoreCase);
	public int K => Pressure.Trim().Equals("auto", StringComparison.OrdinalIgnoreCase) ? Processors * 2 : ThreadPoolPressure.Parse(Pressure);
}

public static class In6Probe
{
	const string Usage = """
		--in6: sync Read against ReadAsync on real providers (SqlClient on LocalDB, Microsoft.Data.Sqlite on a file). Not a BenchmarkDotNet run.

		  --in6 --help                 this text
		  --in6 --dry-run              print the scenarios, the number of operations and the settings; opens no connection, creates nothing
		  --in6 --scratch <folder>     run everything; (LocalDB mode and SQLite) the folder (never on drive C:) holds the probe's SQL Server database files (ode_in6_<guid>.mdf and _log.ldf) and the SQLite file; all deleted afterwards
		  --only sqlserver,sqlite,calibration   run only these parts (default: all three; every part needs --scratch, except the SQL Server parts with --sql-tcp)
		  --pressure auto|N            K busy thread-pool items for the "pressure on" cells (default auto = processor count x 2)
		  --rows 10000 --warmup 5 --ops 20      rows per scenario, warm-up and measured operations per cell (per path)
		  --scenarios S1,S2,S3         which SQL Server scenarios run (default all three; S1 width 5, S2 width 20, S3 about 8 KB per row)
		  --ops-s3 N                   measured operations for S3 only (default: the value of --ops)
		  --pressure-s3 both|off       with off, S3 runs only its pressure-off cells (S1 and S2 still run both); default both
		  --sql-tcp <host,port>        run the SQL Server part against a server over TCP (for example 127.0.0.1,14333) as user sa, TrustServerCertificate, default packet size, instead of LocalDB;
		                               needs --sql-password-file; the database is created without a FILENAME clause (the server's default location); --scratch is then needed only for SQLite
		  --sql-password-file <path>   a file that holds only the password of sa; read at run time, never printed, never on a command line
		  --affinity <hexmask>         set this process's affinity at start (for example 0xF00000), then print the processor count (K = auto uses it)
		  --priority belownormal       set this process's priority class at start
		  --connection "<string>"      the SQL Server connection string, without a database (default: LocalDB, integrated security, Encrypt=False); the probe connects to master, creates its own database and drops it

		Test data: a dedicated SQL Server database ode_in6_<guid>, ordinary tables, dropped at the end (SINGLE_USER, DROP DATABASE). LocalDB mode: its files are under --scratch (128 MB data, growth 64 MB; 64 MB log, growth 64 MB) and are checked to be gone;
		with --sql-tcp: no FILENAME clause (the server's default location, inside the container) and sys.master_files must show 0 files after the drop;
		if CREATE DATABASE fails the SQL Server part stops (no fallback to tempdb or the default location) and the SQLite part still runs. The SQLite file is under --scratch too, deleted afterwards.
		""";

	public static int Run(string[] args)
	{
		try
		{
			return RunCore(args);
		}
		catch (Exception ex)
		{
			Console.WriteLine("--in6 FAILED: " + ex.GetType().Name + ": " + Scrub(ex.Message));
			return 1;
		}
	}

	static int RunCore(string[] args)
	{
		In6Settings s;
		try
		{
			s = Parse(args);
		}
		catch (Exception ex) when (ex is ArgumentException or FormatException)
		{
			Console.WriteLine("--in6: " + Scrub(ex.Message));
			Console.WriteLine(Usage);
			return 2;
		}

		if (s.Help)
		{
			Console.WriteLine(Usage);
			return 0;
		}

		if (s.DryRun)
		{
			DryRun(s);
			return 0;
		}

		if (s.Scratch is null && (s.Only.Contains("sqlite") || (!s.Tcp && (s.Only.Contains("sqlserver") || s.Only.Contains("calibration")))))
		{
			Console.WriteLine("--in6: --scratch <folder> is required (a folder not on drive C:) for the SQLite file and, in LocalDB mode, for the SQL Server database files. Only --sql-tcp with --only sqlserver,calibration can do without it.");
			return 2;
		}

		ApplyProcessSettings(s);
		Console.WriteLine("runtime: " + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription + ", processors " + s.Processors + ", pressure K = " + s.K
			+ (s.Tcp ? ", SQL Server over TCP " + s.SqlTcp + " as sa" : ", SQL Server on LocalDB"));
		Console.WriteLine(SettingsText(s));
		int failures = 0;
		if (s.Only.Contains("sqlserver") || s.Only.Contains("calibration")) failures += RunSqlServer(s);
		if (s.Only.Contains("sqlite")) failures += RunSqlite(s);
		Console.WriteLine(failures == 0 ? "IN6 DONE" : $"IN6 DONE WITH {failures} PART(S) SKIPPED OR FAILED (see the lines above)");
		return failures == 0 ? 0 : 1;
	}

	[System.Runtime.Versioning.SupportedOSPlatform("windows")]
	[System.Runtime.Versioning.SupportedOSPlatform("linux")]
	static void SetAffinity(Process process, long mask) => process.ProcessorAffinity = (IntPtr)mask;

	static long GetAffinity(Process process)
		=> OperatingSystem.IsWindows() || OperatingSystem.IsLinux() ? (long)process.ProcessorAffinity : 0;

	static string SettingsText(In6Settings s)
		=> $"settings: rows {s.Rows}, warm-up {s.Warmup} + measured {s.Ops} operations per cell (S3: {s.S3Ops}), SQL Server scenarios {string.Join(",", s.Scenarios.Order())}, S3 pressure {s.PressureS3}, pressure K = {s.K} (processors {s.Processors}), parts: {string.Join(",", s.Only.Order())}";

	static void ApplyProcessSettings(In6Settings s)
	{
		if (s.PriorityClass is null && s.AffinityMask == 0) return;
		var process = Process.GetCurrentProcess();
		if (s.PriorityClass is { } priority) process.PriorityClass = priority;
		if (s.AffinityMask != 0)
		{
			if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux()) SetAffinity(process, s.AffinityMask);
			else throw new PlatformNotSupportedException("--affinity needs Windows or Linux");
			s.Processors = System.Numerics.BitOperations.PopCount((ulong)s.AffinityMask);
			// The pool's minimum worker count was set at start from the full processor count; bring it in line with the affinity so that pressure K = processors x 2 starves the pool as it would in a process started with that affinity.
			ThreadPool.GetMinThreads(out _, out int io);
			ThreadPool.SetMinThreads(s.Processors, io);
		}

		ThreadPool.GetMinThreads(out int workers, out _);
		Console.WriteLine($"process: priority {process.PriorityClass}, affinity 0x{GetAffinity(process):X}, processors {s.Processors} (Environment.ProcessorCount reads {Environment.ProcessorCount}), thread-pool minimum worker threads {workers}");
	}

	static In6Settings Parse(string[] args)
	{
		var s = new In6Settings();
		for (int i = 0; i < args.Length; i++)
		{
			string Value() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"{args[i]} needs a value");
			switch (args[i])
			{
				case "--in6": break;
				case "--help": s.Help = true; break;
				case "--dry-run": s.DryRun = true; break;
				case "--scratch": s.Scratch = Value(); break;
				case "--connection": s.SqlServer = Value(); break;
				case "--scenarios":
					s.Scenarios = new(Value().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries), StringComparer.OrdinalIgnoreCase);
					if (s.Scenarios.Count == 0 || s.Scenarios.Any(static x => !(x.Equals("S1", StringComparison.OrdinalIgnoreCase) || x.Equals("S2", StringComparison.OrdinalIgnoreCase) || x.Equals("S3", StringComparison.OrdinalIgnoreCase))))
						throw new ArgumentException("--scenarios takes a comma list of S1, S2, S3");
					s.Scenarios = new(s.Scenarios.Select(static x => x.ToUpperInvariant()), StringComparer.OrdinalIgnoreCase);
					break;
				case "--pressure-s3":
					s.PressureS3 = Value().Trim().ToLowerInvariant();
					if (s.PressureS3 is not ("both" or "off")) throw new ArgumentException("--pressure-s3 takes both or off");
					break;
				case "--ops-s3": s.OpsS3 = int.Parse(Value(), CultureInfo.InvariantCulture); break;
				case "--sql-tcp": s.SqlTcp = Value(); break;
				case "--sql-password-file": s.PasswordFile = Value(); break;
				case "--affinity": s.Affinity = Value(); break;
				case "--priority": s.Priority = Value(); break;
				case "--pressure": s.Pressure = Value(); break;
				case "--rows": s.Rows = int.Parse(Value(), CultureInfo.InvariantCulture); break;
				case "--warmup": s.Warmup = int.Parse(Value(), CultureInfo.InvariantCulture); break;
				case "--ops": s.Ops = int.Parse(Value(), CultureInfo.InvariantCulture); break;
				case "--only":
					s.Only = new(Value().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries), StringComparer.OrdinalIgnoreCase);
					foreach (string part in s.Only)
					{
						if (!In6Settings.Parts.Contains(part, StringComparer.OrdinalIgnoreCase)) throw new ArgumentException("--only takes sqlserver, sqlite, calibration; got " + part);
					}

					break;
				default: throw new ArgumentException("unknown option " + args[i]);
			}
		}

		if (s.SqlTcp is not null)
		{
			string[] parts = s.SqlTcp.Split(',', StringSplitOptions.TrimEntries);
			if (parts.Length != 2 || parts[0].Length == 0 || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int port) || port is < 1 or > 65535)
				throw new ArgumentException("--sql-tcp takes <host,port>, for example 127.0.0.1,14333");
			if (s.PasswordFile is null) throw new ArgumentException("--sql-tcp needs --sql-password-file <path> (a file that holds only the password)");
			if (s.SqlServer != In6Settings.DefaultSqlServer) throw new ArgumentException("--sql-tcp and --connection cannot be combined");
			s.SqlTcp = parts[0] + "," + parts[1];
		}
		else if (s.PasswordFile is not null)
		{
			throw new ArgumentException("--sql-password-file is only used with --sql-tcp");
		}

		if (s.Affinity is not null)
		{
			string hex = s.Affinity.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? s.Affinity[2..] : s.Affinity;
			if (!long.TryParse(hex, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out long mask) || mask <= 0) throw new ArgumentException("--affinity takes a non-zero hexadecimal mask, for example 0xF00000");
			s.AffinityMask = mask;
			s.Processors = System.Numerics.BitOperations.PopCount((ulong)mask);
		}

		if (s.Priority is not null)
		{
			if (!Enum.TryParse(s.Priority, ignoreCase: true, out ProcessPriorityClass priority) || priority is ProcessPriorityClass.High or ProcessPriorityClass.RealTime || !Enum.IsDefined(priority))
				throw new ArgumentException("--priority takes belownormal, normal, abovenormal or idle");
			s.PriorityClass = priority;
		}

		if (s.Rows < 2 || s.Warmup < 0 || s.Ops < 1 || (s.OpsS3 != -1 && s.OpsS3 < 1)) throw new ArgumentException("--rows needs at least 2, --warmup at least 0, --ops and --ops-s3 at least 1");
		if (s.Scratch is not null)
		{
			string full = Path.GetFullPath(s.Scratch);
			if (full.StartsWith("C:", StringComparison.OrdinalIgnoreCase)) throw new ArgumentException("--scratch must not be on drive C:");
			s.Scratch = full;
		}

		return s;
	}

	static void DryRun(In6Settings s)
	{
		int k = s.K, perPath = s.Warmup + s.Ops;
		Console.WriteLine("IN6 dry run: nothing is opened or created.");
		Console.WriteLine(SettingsText(s));
		Console.WriteLine($"process: affinity {(s.AffinityMask == 0 ? "unchanged" : "0x" + s.AffinityMask.ToString("X", CultureInfo.InvariantCulture) + " (" + System.Numerics.BitOperations.PopCount((ulong)s.AffinityMask) + " processors; K = auto uses that count)")}, priority {(s.PriorityClass?.ToString() ?? "unchanged")} (applied at the start of a real run, not in a dry run)");
		Console.WriteLine("read paths: sync (sync execute + Read) | async+Read (async execute + Read) | EnableAsyncRead (async execute + ReadAsync); all through Map<T>().ToList[Async]()");
		if (s.Tcp)
		{
			string[] hostPort = s.SqlTcp!.Split(',');
			Console.WriteLine($"SQL Server mode: TCP, host {hostPort[0]}, port {hostPort[1]}, user sa, TrustServerCertificate, default packet size, password read from the --sql-password-file at run time (never printed); the connection string is never printed");
			Console.WriteLine("SQL Server database: ode_in6_<guid>, created with CREATE DATABASE WITHOUT a FILENAME clause (the server's default data location, inside the container; whoever started the container removes it); ordinary tables w20 and big; at the end SINGLE_USER WITH ROLLBACK IMMEDIATE, DROP DATABASE, and the files are counted in sys.master_files (0 expected after the drop)");
			Console.WriteLine("the log shows the server version and, for the probe's own connection, net_transport, protocol_type and net_packet_size from sys.dm_exec_connections; lines are labelled SqlServerTcp");
		}
		else
		{
			Console.WriteLine($"SQL Server connection (to master): {s.SqlServer}");
			Console.WriteLine(s.Scratch is null
				? "SQL Server database and files: (--scratch not given)"
				: $"SQL Server database: ode_in6_<guid>, created with CREATE DATABASE; data file {Path.Combine(s.Scratch, "ode_in6_<guid>.mdf")} ({DataSize}), log file {Path.Combine(s.Scratch, "ode_in6_<guid>_log.ldf")} ({LogSize}); at the end SINGLE_USER WITH ROLLBACK IMMEDIATE, DROP DATABASE, and the two files are checked");
		}

		Console.WriteLine("if CREATE DATABASE fails: STOP message, non-zero exit, no fallback to tempdb or the default location (the SQLite part still runs)");
		Console.WriteLine($"SQLite file: {(s.Scratch is null ? "(--scratch not given; needed for the SQLite part)" : Path.Combine(s.Scratch, "in6-<guid>.db"))}");
		int cells = 0;
		long operations = 0;
		if (s.Only.Contains("sqlserver"))
		{
			Console.WriteLine("scenarios, SQL Server (SqlClient, ordinary tables w20 and big in the probe's own database, dropped with it):");
			foreach (var (id, text) in SqlServerScenarios(s.Rows))
			{
				if (!s.Scenarios.Contains(id)) continue;
				Console.WriteLine($"  {id}: {text}, {s.Warmup} warm-up + {(id == "S3" ? s.S3Ops : s.Ops)} measured operations per path{(id == "S3" && s.S3PressureOff ? ", pressure off only" : "")}");
				int pressureCells = id == "S3" && s.S3PressureOff ? 1 : 2;
				cells += pressureCells;
				operations += pressureCells * 3L * (s.Warmup + (id == "S3" ? s.S3Ops : s.Ops));
			}

			Console.WriteLine("before S1: rtt = 50 warm-up + 200 timed SELECT 1 round trips (ExecuteScalar) on one open connection, one line: rtt (SELECT 1 round trip, ms): median, min, max (n=200)");
		}

		if (s.Only.Contains("sqlite"))
		{
			Console.WriteLine("scenarios, SQLite (Microsoft.Data.Sqlite, a file database, deleted afterwards):");
			foreach (var (id, text) in SqliteScenarios(s.Rows)) Console.WriteLine($"  {id}: {text}");
			cells += 2 * 2;
			operations += 4L * 3 * perPath;
		}

		Console.WriteLine($"each scenario runs with pressure off and on (K = {k}{(s.S3PressureOff ? "; S3: off only" : "")}); a cell is one scenario x one pressure setting x 3 paths x (warm-up + measured operations, as listed)");
		Console.WriteLine($"cells: {cells}, operations: {operations}");
		if (s.Only.Contains("calibration"))
		{
			Console.WriteLine("calibration (SQL Server, sync Read, no pressure): per-row time = (time of the full result - time of 1 row) / (rows - 1), for width 5 and 20; per-NextResult time for k = 0..4 extra empty result sets");
			Console.WriteLine($"  operations: {2 * 2 * perPath} for the per-row part, {5 * perPath} for the NextResult part");
		}

		Console.WriteLine("output per path: reader-open median/min/max (ms), total mean (ms), allocated (KB, calling thread and whole process), ReadAsync calls, incomplete ReadAsync, continuations resumed on another thread, wait ms");
		Console.WriteLine("wait ms: sum = per operation, the time between the return of an incomplete ReadAsync and the completion of its task, summed over the operation; the cell shows the MEDIAN of that sum over the measured operations (the same statistic as the open-time column); max = the longest single wait over all measured operations; n/a = no ReadAsync was called (sync Read). The wait ends when the task completes, before its continuation gets a thread. An incomplete read also allocates the timing object and a continuation (a few hundred bytes), which is inside the allocation fields; a completed read allocates nothing for it");
		Console.WriteLine("wait rows: for every INCOMPLETE ReadAsync the row index = the number of rows already read in that operation when the call was made (0-based: the call that asks for row 100 has index 100), pooled over the measured operations of the cell; first50 = the 50 smallest distinct indices, smallest first, with (xN) when the same index was seen N times across operations; hist10 = counts per bucket of the rows 0..rows-1 of the scenario (bucket k covers rows from k*rows/10 up to, not including, (k+1)*rows/10); 'over N ops' = the operations pooled; n/a = no ReadAsync was called. Recording uses preallocated arrays: it allocates nothing");
		Console.WriteLine("IN6 DRY RUN DONE");
	}

	static IEnumerable<(string Id, string Text)> SqlServerScenarios(int rows)
	{
		yield return ("S1", $"{rows} rows, width 5 (Id, Created, Title, Price, Qty) into SynRow");
		yield return ("S2", $"{rows} rows, width 20 (the whole catalog row) into SynRow");
		yield return ("S3", $"{rows} rows, Id + nvarchar(max) of 4000 characters (about 8 KB per row, so rows cross packets) into In6BigRow");
	}

	static IEnumerable<(string Id, string Text)> SqliteScenarios(int rows)
	{
		yield return ("L1", $"{rows} rows, width 5 (Id, Created, Title, Price, Qty) into In6LiteRow");
		yield return ("L2", $"{rows} rows, width 20 into In6LiteRow");
	}

	// ---- the measuring engine (shared with --selftest-in6) ----------------------------------------------------------------
	static double ToUs(long ticks) => ticks * 1_000_000d / Stopwatch.Frequency;

	static double Median(double[] values)
	{
		var copy = (double[])values.Clone();
		Array.Sort(copy);
		return copy.Length % 2 == 1 ? copy[copy.Length / 2] : (copy[(copy.Length / 2) - 1] + copy[copy.Length / 2]) / 2;
	}

	internal static In6Summary Measure(string path, In6Stats stats, Func<int> operation, int expectedRows, int warmup, int ops)
	{
		var open = new double[ops];
		var total = new double[ops];
		var waitSum = new double[ops];
		var positions = new WaitPositions(expectedRows) { Operations = ops };
		double waitMax = 0;
		double allocThread = 0, allocProcess = 0, readAsync = 0, incomplete = 0, resumed = 0, otherAsync = 0, otherIncomplete = 0;
		bool oneReader = true;
		for (int i = 0; i < warmup + ops; i++)
		{
			stats.Reset();
			stats.Positions = i < warmup ? null : positions;
			long thread0 = GC.GetAllocatedBytesForCurrentThread(), process0 = GC.GetTotalAllocatedBytes(false);
			long t0 = Stopwatch.GetTimestamp();
			int rows = operation();
			long t1 = Stopwatch.GetTimestamp();
			long thread1 = GC.GetAllocatedBytesForCurrentThread(), process1 = GC.GetTotalAllocatedBytes(false);
			if (rows != expectedRows) throw new InvalidOperationException($"{path}: read {rows} rows, expected {expectedRows}");
			if (i < warmup) continue;
			int j = i - warmup;
			open[j] = ToUs(stats.OpenTicks);
			total[j] = ToUs(t1 - t0);
			waitSum[j] = ToUs(stats.WaitTicks);
			waitMax = Math.Max(waitMax, ToUs(stats.MaxWaitTicks));
			allocThread += thread1 - thread0;
			allocProcess += process1 - process0;
			readAsync += stats.ReadAsyncCalls;
			incomplete += stats.ReadAsyncIncomplete;
			resumed += stats.ResumedOnOtherThread;
			otherAsync += stats.OtherAsyncCalls;
			otherIncomplete += stats.OtherAsyncIncomplete;
			if (stats.Readers != 1) oneReader = false;
		}

		return new In6Summary(path, ops, Median(open), open.Min(), open.Max(), total.Average(), Median(total), allocThread / ops, allocProcess / ops,
			readAsync / ops, incomplete / ops, resumed / ops, otherAsync / ops, otherIncomplete / ops, oneReader, Median(waitSum), waitMax, positions);
	}

	// The three paths of one scenario over one (wrapped) connection. T is the row type.
	internal static List<In6Summary> RunPaths<T>(TimedConnection timed, In6Stats stats, string sql, int expectedRows, int warmup, int ops, Action<string, Exception>? failed = null)
		where T : class
	{
		var paths = new (string Name, Func<int> Operation)[]
		{
			("sync", () => timed.Command(sql).Map<T>().ToList().Count),
			("async+Read", () => timed.Command(sql).Map<T>().ToListAsync().AsTask().GetAwaiter().GetResult().Count),
			("EnableAsyncRead", () => timed.Command(sql).EnableAsyncRead().Map<T>().ToListAsync().AsTask().GetAwaiter().GetResult().Count),
		};

		var results = new List<In6Summary>();
		foreach (var (name, operation) in paths)
		{
			try
			{
				results.Add(Measure(name, stats, operation, expectedRows, warmup, ops));
			}
			catch (Exception ex)
			{
				if (failed is null) throw;
				failed(name, ex);
			}
		}

		return results;
	}

	static string Ms(double us) => (us / 1000d).ToString("F3", CultureInfo.InvariantCulture);
	static string Kb(double bytes) => (bytes / 1024d).ToString("F0", CultureInfo.InvariantCulture);
	static string N(double value) => value.ToString("0.#", CultureInfo.InvariantCulture);

	static string Line(string provider, string scenario, string pressure, In6Summary r)
		=> $"{provider,-9} {scenario,-3} {pressure,-9} {r.Path,-16} open ms med {Ms(r.OpenMedianUs),9} min {Ms(r.OpenMinUs),9} max {Ms(r.OpenMaxUs),9} | total ms mean {Ms(r.TotalMeanUs),9} | alloc KB thread {Kb(r.AllocThreadBytes),7} process {Kb(r.AllocProcessBytes),7} | ReadAsync {N(r.ReadAsyncCalls),7} incomplete {N(r.ReadAsyncIncomplete),6} other-thread {N(r.ResumedOnOtherThread),6}"
			+ (r.ReadAsyncCalls > 0 ? $" wait ms sum {Ms(r.WaitSumMedianUs)} max {Ms(r.WaitMaxUs)}" : " wait ms n/a")
			+ (r.ReadAsyncCalls > 0 && r.Positions is { } spots ? " wait rows " + spots.Format() : " wait rows n/a")
			+ (r.OtherAsyncCalls > 0 ? $" | other async {N(r.OtherAsyncCalls)}/{N(r.OtherAsyncIncomplete)} incomplete" : "")
			+ (r.OneReaderPerOperation ? "" : " | WARNING: not one closed reader per operation");

	static void RunCell<T>(In6Settings s, string provider, string scenario, DbConnection connection, string sql, int expectedRows, int ops, int[] pressures)
		where T : class
	{
		var stats = new In6Stats();
		var timed = new TimedConnection(connection, stats);
		foreach (int k in pressures)
		{
			string label = k == 0 ? "off" : "on K=" + k;
			try
			{
				if (k > 0) ThreadPoolPressure.Start(k);
				foreach (var r in RunPaths<T>(timed, stats, sql, expectedRows, s.Warmup, ops, (name, ex) => Console.WriteLine($"{provider,-9} {scenario,-3} {label,-9} {name,-16} FAILED: {ex.GetType().Name}: {Scrub(ex.Message)}")))
					Console.WriteLine(Line(provider, scenario, label, r));
			}
			catch (Exception ex)
			{
				Console.WriteLine($"{provider,-9} {scenario,-3} {label,-9} FAILED: {ex.GetType().Name}: {Scrub(ex.Message)}");
			}
			finally
			{
				ThreadPoolPressure.Stop();
			}
		}
	}

	// ---- test data ----------------------------------------------------------------
	internal static object CellValue(SyntheticTable table, int column, int row)
		=> table.Nulls[column] is { } nulls && nulls[row] ? DBNull.Value : table.Columns[column].GetValue(row)!;

	internal static DataTable ToDataTable(SyntheticTable table)
	{
		var data = new DataTable();
		for (int c = 0; c < table.Width; c++) data.Columns.Add(table.Names[c], table.Types[c]);
		var values = new object[table.Width];
		for (int i = 0; i < table.Rows; i++)
		{
			for (int c = 0; c < table.Width; c++) values[c] = CellValue(table, c, i);
			data.Rows.Add(values);
		}

		return data;
	}

	internal static void Exec(DbConnection connection, string sql)
	{
		using var command = connection.CreateCommand();
		command.CommandText = sql;
		command.CommandTimeout = 300;
		command.ExecuteNonQuery();
	}

	// ---- SQL Server ----------------------------------------------------------------
	// LocalDB mode: a dedicated database whose data and log files live in the scratch folder (never on drive C:, never tempdb, never the default location):
	// initial 128 MB data (growth 64 MB) and 64 MB log (growth 64 MB), so the 80 MB big table fits without many growth events.
	// TCP mode (--sql-tcp): the same database name and ordinary tables, created WITHOUT a FILENAME clause, so its files are in the server's own default
	// data location (inside the container); the server is removed by whoever started it.
	const string DataSize = "size = 128MB, filegrowth = 64MB", LogSize = "size = 64MB, filegrowth = 64MB";

	// Everything that must never be printed: the password and every connection string built from it.
	static readonly List<string> Secrets = [];
	static string? _password;

	static string ReadPassword(In6Settings s)
	{
		string text = File.ReadAllText(s.PasswordFile!).Trim();
		if (text.Length == 0) throw new InvalidOperationException("the password file is empty");
		Secrets.Add(text);
		return text;
	}

	/// <summary>Removes the password and the connection strings from a text that is about to be printed.</summary>
	internal static string Scrub(string text)
	{
		foreach (string secret in Secrets)
		{
			if (secret.Length > 0) text = text.Replace(secret, "***", StringComparison.Ordinal);
		}

		return System.Text.RegularExpressions.Regex.Replace(text, @"(?i)\b(password|pwd)\s*=\s*[^;]*", "$1=***");
	}

	static string Connection(In6Settings s, string database)
	{
		if (!s.Tcp) return new SqlConnectionStringBuilder(s.SqlServer) { InitialCatalog = database, Pooling = false }.ConnectionString;
		_password ??= ReadPassword(s);
		string text = new SqlConnectionStringBuilder { DataSource = "tcp:" + s.SqlTcp, UserID = "sa", Password = _password, TrustServerCertificate = true, InitialCatalog = database, Pooling = false }.ConnectionString;
		if (!Secrets.Contains(text)) Secrets.Add(text);
		return text;
	}

	internal static string Quote(string path) => path.Replace("'", "''", StringComparison.Ordinal);

	static int RunSqlServer(In6Settings s)
	{
		string name = "ode_in6_" + Guid.NewGuid().ToString("N")[..12];
		string? dataFile = s.Tcp ? null : Path.Combine(s.Scratch!, name + ".mdf"), logFile = s.Tcp ? null : Path.Combine(s.Scratch!, name + "_log.ldf");
		SqlConnection master;
		try
		{
			master = new SqlConnection(Connection(s, "master"));
			master.Open();
		}
		catch (Exception ex)
		{
			Console.WriteLine($"SQL SERVER SKIPPED: could not open the connection to master: {ex.GetType().Name}: {Scrub(ex.Message)}");
			return 1;
		}

		using (master)
		{
			try
			{
				if (s.Tcp)
				{
					Exec(master, $"create database [{name}]");
				}
				else
				{
					Directory.CreateDirectory(s.Scratch!);
					Exec(master, $"create database [{name}] on (name = N'{name}', filename = N'{Quote(dataFile!)}', {DataSize}) log on (name = N'{name}_log', filename = N'{Quote(logFile!)}', {LogSize})");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"STOP: CREATE DATABASE on the {(s.Tcp ? "server's default location" : "scratch folder")} failed: {ex.GetType().Name}: {Scrub(ex.Message)}");
				Console.WriteLine("The SQL Server part (S1 to S3 and the calibration) is not run: there is no fallback to tempdb or to another location. The SQLite part is independent and still runs.");
				try { Exec(master, $"if db_id(N'{name}') is not null drop database [{name}]"); }
				catch (Exception cleanup) { Console.WriteLine($"cleanup after the failed CREATE DATABASE also failed: {Scrub(cleanup.Message)}"); }

				return 1;
			}

			Console.WriteLine(s.Tcp
				? $"SQL Server (TCP): database {name} created without a FILENAME clause (the server's default data location, inside the server's host or container)"
				: $"SQL Server: database {name} created; data file {dataFile} ({DataSize}), log file {logFile} ({LogSize})");
			int databaseId = 0, filesBefore = 0;
			try
			{
				databaseId = (int)ScalarLong(master, $"select db_id(N'{name}')");
				filesBefore = (int)ScalarLong(master, $"select count(*) from sys.master_files where database_id = {databaseId}");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"could not read the new database's files from sys.master_files: {ex.GetType().Name}: {Scrub(ex.Message)}");
			}

			try
			{
				return RunInDatabase(s, name);
			}
			catch (Exception ex)
			{
				Console.WriteLine($"SQL SERVER PART FAILED: {ex.GetType().Name}: {Scrub(ex.Message)}");
				return 1;
			}
			finally
			{
				SqlConnection.ClearAllPools();
				try
				{
					Exec(master, $"alter database [{name}] set single_user with rollback immediate");
					Exec(master, $"drop database [{name}]");
				}
				catch (Exception ex)
				{
					Console.WriteLine($"could not drop database {name}: {ex.GetType().Name}: {Scrub(ex.Message)}");
				}

				if (s.Tcp)
				{
					try
					{
						long after = ScalarLong(master, $"select count(*) from sys.master_files where database_id = {databaseId}");
						Console.WriteLine($"SQL Server (TCP): database {name} dropped; files in sys.master_files: {filesBefore} before the drop, {after} after (must be 0)");
					}
					catch (Exception ex)
					{
						Console.WriteLine($"could not count the dropped database's files: {ex.GetType().Name}: {Scrub(ex.Message)}");
					}
				}
				else
				{
					Console.WriteLine($"SQL Server: database {name} dropped; data file still exists: {File.Exists(dataFile)}, log file still exists: {File.Exists(logFile)} (both must be False)");
				}
			}
		}
	}

	// 50 warm-up and 200 timed SELECT 1 round trips on the probe's own open connection, before S1.
	static void PrintRtt(DbConnection connection)
	{
		const int Warm = 50, Timed = 200;
		try
		{
			using var command = connection.CreateCommand();
			command.CommandText = "select 1";
			for (int i = 0; i < Warm; i++) command.ExecuteScalar();
			var times = new double[Timed];
			for (int i = 0; i < Timed; i++)
			{
				long t0 = Stopwatch.GetTimestamp();
				command.ExecuteScalar();
				times[i] = ToUs(Stopwatch.GetTimestamp() - t0);
			}

			Console.WriteLine($"rtt (SELECT 1 round trip, ms): median {Ms(Median(times))} min {Ms(times.Min())} max {Ms(times.Max())} (n={Timed})");
		}
		catch (Exception ex)
		{
			Console.WriteLine($"could not measure the round trip time: {ex.GetType().Name}: {Scrub(ex.Message)}");
		}
	}

	static void PrintTransport(SqlConnection connection)
	{
		try
		{
			using var command = connection.CreateCommand();
			command.CommandText = "select @@version, net_transport, protocol_type, net_packet_size, encrypt_option from sys.dm_exec_connections where session_id = @@spid";
			using var reader = command.ExecuteReader();
			if (reader.Read())
			{
				string version = Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)!.Split('\n')[0].Trim();
				Console.WriteLine($"SQL Server version: {version}");
				Console.WriteLine($"this connection (sys.dm_exec_connections): net_transport {reader.GetValue(1)}, protocol_type {reader.GetValue(2)}, net_packet_size {reader.GetValue(3)}, encrypt_option {reader.GetValue(4)}; client PacketSize {connection.PacketSize}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"could not read sys.dm_exec_connections: {ex.GetType().Name}: {Scrub(ex.Message)}");
		}
	}

	static int RunInDatabase(In6Settings s, string database)
	{
		const string T20 = "w20", TBig = "big";
		using var connection = new SqlConnection(Connection(s, database));
		connection.Open();
		Console.WriteLine(s.Tcp
			? $"SQL Server (TCP): host and port {s.SqlTcp}, user sa, database {connection.Database}, server version {connection.ServerVersion}; ordinary tables {T20} and {TBig}"
			: $"SQL Server: {connection.DataSource}, database {connection.Database}, server version {connection.ServerVersion}; ordinary tables {T20} and {TBig}");
		if (s.Tcp) PrintTransport(connection);
		PrintRtt(connection);
		Exec(connection, $"""
			create table [{T20}] (Id int not null primary key, Created datetime2 not null, Title nvarchar(50) not null, Price decimal(18,2) not null,
				Qty int null, IsActive bit not null, [Key] uniqueidentifier not null, Score float not null, Note nvarchar(50) null,
				Blob varbinary(16) not null, Big bigint not null, Body nvarchar(200) not null, Amount decimal(18,3) null, Stamp datetime2 null,
				Tag nvarchar(20) not null, Ratio float not null, Flag bit null, Link uniqueidentifier null, [Count] bigint not null, Summary nvarchar(200) null)
			""");
		using (var bulk = new SqlBulkCopy(connection) { DestinationTableName = $"[{T20}]", BulkCopyTimeout = 300, BatchSize = 5000 })
			bulk.WriteToServer(ToDataTable(SyntheticTable.Create(20, s.Rows)));

		if (s.Only.Contains("sqlserver"))
		{
			if (s.Scenarios.Contains("S3"))
			{
				Exec(connection, $"create table [{TBig}] (Id int not null primary key, Body nvarchar(max) not null)");
				Exec(connection, $"with n as (select top ({s.Rows}) row_number() over (order by (select null)) as i from sys.all_objects a cross join sys.all_objects b) insert into [{TBig}] (Id, Body) select i, replicate(cast(N'x' as nvarchar(max)), 4000) from n");
				Console.WriteLine($"data ready: {s.Rows} rows in each table; {ScalarLong(connection, $"select sum(datalength(Body)) from [{TBig}]") / s.Rows} bytes of Body per row in {TBig}");
			}
			else
			{
				Console.WriteLine($"data ready: {s.Rows} rows in {T20}");
			}

			Console.WriteLine("-- SQL Server, three paths, pressure off and on --");
			if (s.Scenarios.Contains("S1")) RunCell<SynRow>(s, s.SqlLabel, "S1", connection, $"select Id, Created, Title, Price, Qty from [{T20}]", s.Rows, s.Ops, [0, s.K]);
			if (s.Scenarios.Contains("S2")) RunCell<SynRow>(s, s.SqlLabel, "S2", connection, $"select * from [{T20}]", s.Rows, s.Ops, [0, s.K]);
			if (s.Scenarios.Contains("S3")) RunCell<In6BigRow>(s, s.SqlLabel, "S3", connection, $"select Id, Body from [{TBig}]", s.Rows, s.S3Ops, s.S3PressureOff ? [0] : [0, s.K]);
		}

		if (s.Only.Contains("calibration")) Calibrate(s, connection, T20);
		return 0;
	}

	static long ScalarLong(DbConnection connection, string sql)
	{
		using var command = connection.CreateCommand();
		command.CommandText = sql;
		return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
	}

	// ---- calibration ----------------------------------------------------------------
	sealed class NextResultTimes
	{
		public int Calls;
		public long Ticks;
	}

	static void Calibrate(In6Settings s, SqlConnection connection, string table)
	{
		Console.WriteLine("-- calibration: SQL Server, sync Read, no pressure --");
		var stats = new In6Stats();
		var timed = new TimedConnection(connection, stats);
		foreach (var (width, columns) in new[] { (5, "Id, Created, Title, Price, Qty"), (20, "*") })
		{
			var big = Measure("sync", stats, () => timed.Command($"select {columns} from [{table}]").Map<SynRow>().ToList().Count, s.Rows, s.Warmup, s.Ops);
			var one = Measure("sync", stats, () => timed.Command($"select top (1) {columns} from [{table}]").Map<SynRow>().ToList().Count, 1, s.Warmup, s.Ops);
			double perRowMedianNs = (big.TotalMedianUs - one.TotalMedianUs) / (s.Rows - 1) * 1000d;
			double perRowMeanNs = (big.TotalMeanUs - one.TotalMeanUs) / (s.Rows - 1) * 1000d;
			Console.WriteLine($"per-row, width {width,2}: total {s.Rows} rows median {Ms(big.TotalMedianUs)} ms mean {Ms(big.TotalMeanUs)} ms; 1 row median {Ms(one.TotalMedianUs)} ms mean {Ms(one.TotalMeanUs)} ms; PER ROW {perRowMedianNs.ToString("F1", CultureInfo.InvariantCulture)} ns (from medians), {perRowMeanNs.ToString("F1", CultureInfo.InvariantCulture)} ns (from means)");
		}

		var ks = new List<double>();
		var perOperation = new List<double>();
		for (int extra = 0; extra <= 4; extra++)
		{
			string sql = $"select top (1) Id from [{table}]" + string.Concat(Enumerable.Repeat($"; select Id from [{table}] where 1 = 0", extra));
			var times = new NextResultTimes();
			var perCall = new List<double>();
			var totals = new List<double>();
			for (int i = 0; i < s.Warmup + s.Ops; i++)
			{
				times.Calls = 0;
				times.Ticks = 0;
				long t0 = Stopwatch.GetTimestamp();
				timed.Command(sql).ExecuteReader(reader =>
				{
					reader.Read();
					while (true)
					{
						long n0 = Stopwatch.GetTimestamp();
						bool more = reader.NextResult();
						times.Ticks += Stopwatch.GetTimestamp() - n0;
						times.Calls++;
						if (!more) break;
					}

					return 0;
				}, CommandBehavior.SequentialAccess);
				long t1 = Stopwatch.GetTimestamp();
				if (i < s.Warmup) continue;
				perCall.Add(ToUs(times.Ticks) / times.Calls);
				totals.Add(ToUs(t1 - t0));
			}

			double perCallMedian = Median([.. perCall]), totalMedian = Median([.. totals]);
			ks.Add(extra);
			perOperation.Add(totalMedian);
			Console.WriteLine($"per-NextResult, {extra} extra empty result set(s): {times.Calls} NextResult calls per operation, time inside one call median {perCallMedian.ToString("F2", CultureInfo.InvariantCulture)} us; operation total median {totalMedian.ToString("F1", CultureInfo.InvariantCulture)} us");
		}

		double meanK = ks.Average(), meanT = perOperation.Average();
		double slope = ks.Zip(perOperation, (k, t) => (k - meanK) * (t - meanT)).Sum() / ks.Sum(k => (k - meanK) * (k - meanK));
		Console.WriteLine($"per-NextResult from the slope of operation total against the number of extra result sets (least squares over k = 0..4): {slope.ToString("F2", CultureInfo.InvariantCulture)} us per extra result set");
	}

	// ---- SQLite ----------------------------------------------------------------
	static int RunSqlite(In6Settings s)
	{
		string folder = s.Scratch!;
		string file = Path.Combine(folder, "in6-" + Guid.NewGuid().ToString("N")[..12] + ".db");
		try
		{
			Directory.CreateDirectory(folder);
			string connectionString = new SqliteConnectionStringBuilder { DataSource = file, Pooling = false, Mode = SqliteOpenMode.ReadWriteCreate }.ToString();
			using var connection = new SqliteConnection(connectionString);
			connection.Open();
			Console.WriteLine($"SQLite: Microsoft.Data.Sqlite file database ({connection.ServerVersion}), {s.Rows} rows (file deleted at the end)");
			Exec(connection, """
				create table t20 (Id integer not null, Created text not null, Title text not null, Price text not null, Qty integer null, IsActive integer not null,
					"Key" text not null, Score real not null, Note text null, Blob blob not null, Big integer not null, Body text not null, Amount text null,
					Stamp text null, Tag text not null, Ratio real not null, Flag integer null, Link text null, "Count" integer not null, Summary text null)
				""");
			var table = SyntheticTable.Create(20, s.Rows);
			using (var transaction = connection.BeginTransaction())
			{
				using var insert = connection.CreateCommand();
				insert.Transaction = transaction;
				insert.CommandText = "insert into t20 values (" + string.Join(",", Enumerable.Range(0, 20).Select(static c => "$p" + c)) + ")";
				var parameters = new SqliteParameter[20];
				for (int c = 0; c < 20; c++) insert.Parameters.Add(parameters[c] = new SqliteParameter("$p" + c, null));
				for (int i = 0; i < s.Rows; i++)
				{
					for (int c = 0; c < 20; c++) parameters[c].Value = CellValue(table, c, i);
					insert.ExecuteNonQuery();
				}

				transaction.Commit();
			}

			Console.WriteLine("-- SQLite file, three paths, pressure off and on --");
			RunCell<In6LiteRow>(s, "Sqlite", "L1", connection, "select Id, Created, Title, Price, Qty from t20", s.Rows, s.Ops, [0, s.K]);
			RunCell<In6LiteRow>(s, "Sqlite", "L2", connection, "select * from t20", s.Rows, s.Ops, [0, s.K]);
			Console.WriteLine("SQLite: ReadAsync is counted by the wrappers: an 'incomplete' count above 0 would mean it can complete asynchronously; 0 means every ReadAsync completed before returning.");
			return 0;
		}
		catch (Exception ex)
		{
			Console.WriteLine($"SQLITE PART FAILED: {ex.GetType().Name}: {Scrub(ex.Message)}");
			return 1;
		}
		finally
		{
			SqliteConnection.ClearAllPools();
			foreach (string path in new[] { file, file + "-journal", file + "-wal", file + "-shm" })
			{
				try { File.Delete(path); }
				catch (IOException ex) { Console.WriteLine($"could not delete {Path.GetFileName(path)}: {Scrub(ex.Message)}"); }
			}

			Console.WriteLine("SQLite: file deleted: " + !File.Exists(file));
		}
	}

	// ---- --selftest-in6: the engine and the wrappers, against the synthetic provider (no database) ----------------------------------------------------------------
	public static int SelfTest()
	{
		int failures = 0;
		void Check(bool ok, string what)
		{
			if (ok) return;
			failures++;
			Console.WriteLine("FAIL " + what);
		}

		const int Rows = 1000, Buffer = 100, RefillUs = 100, Warmup = 1, Ops = 5;
		int boundaries = (Rows - 1) / Buffer;
		var recorder = new SyntheticRecorder();
		var table = SyntheticTable.Create(5, Rows);
		var inner = new SyntheticConnection(table, new SyntheticOptions { BufferRows = Buffer, RefillCostUs = RefillUs, Recorder = recorder });
		var stats = new In6Stats();
		var timed = new TimedConnection(inner, stats);
		Console.WriteLine($"synthetic provider behind the wrappers: {Rows} rows, width 5, a refill every {Buffer} rows for {RefillUs} us ({boundaries} boundaries); {Warmup} warm-up + {Ops} measured operations per path");
		var results = RunPaths<SynRow>(timed, stats, SyntheticVariants.Sql, Rows, Warmup, Ops);
		Check(results.Count == 3, $"{results.Count} paths measured, expected 3");
		foreach (var r in results)
		{
			Console.WriteLine("  " + Line("synthetic", "-", "off", r));
			Check(r.OneReaderPerOperation, $"{r.Path}: one closed reader per operation");
			Check(r.OpenMedianUs >= 0.9 * boundaries * RefillUs, $"{r.Path}: reader-open median {r.OpenMedianUs:F0} us, expected at least {0.9 * boundaries * RefillUs:F0} (the refills)");
			Check(r.TotalMeanUs >= r.OpenMedianUs * 0.5, $"{r.Path}: total mean {r.TotalMeanUs:F0} us is not near the reader-open time {r.OpenMedianUs:F0} us");
			if (r.Path == "EnableAsyncRead")
			{
				Check(r.ReadAsyncCalls == Rows + 1, $"{r.Path}: {r.ReadAsyncCalls} ReadAsync calls per operation, expected {Rows + 1}");
				Check(r.ReadAsyncIncomplete == boundaries, $"{r.Path}: {r.ReadAsyncIncomplete} incomplete ReadAsync per operation, expected {boundaries}");
				Check(r.ResumedOnOtherThread >= 1, $"{r.Path}: no continuation was seen resuming on another thread");
			}
			else
			{
				Check(r.ReadAsyncCalls == 0 && r.ReadAsyncIncomplete == 0, $"{r.Path}: {r.ReadAsyncCalls} ReadAsync calls, expected 0");
			}
		}

		foreach (var r in results)
		{
			if (r.Path == "EnableAsyncRead")
			{
				double expectedWait = boundaries * (double)RefillUs;
				Check(r.WaitSumMedianUs >= 0.9 * expectedWait && r.WaitSumMedianUs <= (1.1 * expectedWait) + (boundaries * 300), $"{r.Path}: wait time sum {r.WaitSumMedianUs:F0} us per operation, expected about {expectedWait:F0} (boundaries x refill; tolerance -10% to +10% and 300 us per boundary)");
				Check(r.WaitMaxUs >= 0.9 * RefillUs && r.WaitMaxUs <= 5000, $"{r.Path}: longest single wait {r.WaitMaxUs:F0} us, expected from {0.9 * RefillUs:F0} us up to a few ms");
				var spots = r.Positions;
				Check(spots is { Total: > 0 } && spots.Total == boundaries * Ops && spots.Distinct == boundaries && spots.Operations == Ops, $"{r.Path}: {spots?.Total} incomplete reads recorded over {spots?.Operations} operations, expected {boundaries * Ops} over {Ops}");
				bool rowsOk = spots is not null;
				for (int k = 0; rowsOk && k < boundaries; k++) rowsOk = spots!.IndexAt(k) == (k + 1) * Buffer && spots.CountAt(k) == Ops;
				Check(rowsOk, $"{r.Path}: recorded row indices are not 100, 200, ..., 900 each seen {Ops} times: {spots?.Format()}");
				bool shape = spots is not null && spots.Bucket(0) == 0;
				for (int k = 1; shape && k < 10; k++) shape = spots!.Bucket(k) == Ops;
				Check(shape, $"{r.Path}: histogram shape, expected bucket 0 empty and buckets 1 to 9 each {Ops} (one wait per operation at rows 100 to 900 of 1000): {spots?.Format()}");
				Console.WriteLine($"  wait rows, {r.Path}: {spots?.Format()} (expected: 100(x{Ops}), 200(x{Ops}), ..., 900(x{Ops}) and a histogram of 0 followed by nine times {Ops})");
				Console.WriteLine($"  wait time in ReadAsync, {r.Path}: sum {r.WaitSumMedianUs:F0} us (expected about {expectedWait:F0}), max single wait {r.WaitMaxUs:F0} us (refill {RefillUs} us)");
			}
			else
			{
				Check(r.WaitSumMedianUs == 0 && r.WaitMaxUs == 0 && r.ReadAsyncCalls == 0, $"{r.Path}: sync path has no ReadAsync waits to time (sum {r.WaitSumMedianUs:F0}, calls {r.ReadAsyncCalls})");
			}
		}

		// Completed reads (no refill) record no wait at all.
		var plain = new TimedConnection(new SyntheticConnection(SyntheticTable.Create(5, Rows), new SyntheticOptions()), stats);
		var plainResults = RunPaths<SynRow>(plain, stats, SyntheticVariants.Sql, Rows, 1, 3);
		var plainAsync = plainResults.First(static x => x.Path == "EnableAsyncRead");
		Check(plainAsync.ReadAsyncCalls == Rows + 1 && plainAsync.ReadAsyncIncomplete == 0 && plainAsync.WaitSumMedianUs == 0 && plainAsync.WaitMaxUs == 0 && plainAsync.Positions is { Total: 0, Distinct: 0 },
			$"no refill: {plainAsync.ReadAsyncCalls} ReadAsync calls, {plainAsync.ReadAsyncIncomplete} incomplete, wait sum {plainAsync.WaitSumMedianUs:F0} us, max {plainAsync.WaitMaxUs:F0} us; expected {Rows + 1}, 0, 0, 0");
		Console.WriteLine("  " + Line("synthetic", "-", "no refill", plainAsync));

		// The recorder inside the synthetic reader and the wrappers around it time the same readers: the wrapper's window contains the recorder's.
		long readers = recorder.Readers;
		Check(readers == 3L * (Warmup + Ops), $"the synthetic recorder saw {readers} readers, expected {3 * (Warmup + Ops)}");
		Check(recorder.RefillsBlocked == 2L * (Warmup + Ops) * boundaries && recorder.RefillsAsync == 1L * (Warmup + Ops) * boundaries,
			$"the recorder counted {recorder.RefillsBlocked} blocked and {recorder.RefillsAsync} async refills, expected {2 * (Warmup + Ops) * boundaries} and {(Warmup + Ops) * boundaries}");
		Console.WriteLine($"  recorder (inside the provider) over all {readers} readers: median reader-open {recorder.Summary().MedianOpenUs:F0} us; the wrapper medians above include the command and the close");

		// The rows come out the same through the wrappers.
		var rows = timed.Command(SyntheticVariants.Sql).Map<SynRow>().ToList();
		bool equal = rows.Count == Rows;
		for (int i = 0; equal && i < rows.Count; i++) equal = SynRow.Diff(SynRow.FromTable(table, i), rows[i]).Count == 0;
		Check(equal, "the rows read through the wrappers equal the table");

		Console.WriteLine(failures == 0 ? "SELFTEST IN6 PASS" : $"SELFTEST IN6 FAIL ({failures} failed)");
		return failures == 0 ? 0 : 1;
	}
}
