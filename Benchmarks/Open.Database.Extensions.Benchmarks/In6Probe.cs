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
	double OtherAsyncCalls, double OtherAsyncIncomplete, bool OneReaderPerOperation);

sealed class In6Settings
{
	public const string DefaultSqlServer = @"Server=(localdb)\MSSQLLocalDB;Database=tempdb;Integrated Security=true;Encrypt=False";
	public static readonly string[] Parts = ["sqlserver", "sqlite", "calibration"];
	public int Rows = 10_000, Warmup = 5, Ops = 20;
	public string Pressure = "auto";
	public string SqlServer = DefaultSqlServer;
	public string? Scratch;
	public bool DryRun, Help;
	public HashSet<string> Only = new(Parts, StringComparer.OrdinalIgnoreCase);
	public int K => ThreadPoolPressure.Parse(Pressure);
}

public static class In6Probe
{
	const string Usage = """
		--in6: sync Read against ReadAsync on real providers (SqlClient on LocalDB, Microsoft.Data.Sqlite on a file). Not a BenchmarkDotNet run.

		  --in6 --help                 this text
		  --in6 --dry-run              print the scenarios, the number of operations and the settings; opens no connection, creates nothing
		  --in6 --scratch <folder>     run everything; the SQLite file is created in <folder> (never on drive C:) and deleted afterwards
		  --only sqlserver,sqlite,calibration   run only these parts (default: all three; sqlite needs --scratch)
		  --pressure auto|N            K busy thread-pool items for the "pressure on" cells (default auto = processor count x 2)
		  --rows 10000 --warmup 5 --ops 20      rows per scenario, warm-up and measured operations per path
		  --connection "<string>"      the SQL Server connection string (default: LocalDB, tempdb, integrated security, Encrypt=False)

		Test data: SQL Server tables with a unique name in tempdb, dropped afterwards; the SQLite file under --scratch, deleted afterwards.
		""";

	public static int Run(string[] args)
	{
		In6Settings s;
		try
		{
			s = Parse(args);
		}
		catch (Exception ex) when (ex is ArgumentException or FormatException)
		{
			Console.WriteLine("--in6: " + ex.Message);
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

		if (s.Only.Contains("sqlite") && s.Scratch is null)
		{
			Console.WriteLine("--in6: the SQLite part needs --scratch <folder> (a folder not on drive C:). Pass it, or --only sqlserver,calibration.");
			return 2;
		}

		Console.WriteLine("runtime: " + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription + ", processors " + Environment.ProcessorCount + ", pressure K = " + s.K);
		int failures = 0;
		if (s.Only.Contains("sqlserver") || s.Only.Contains("calibration")) failures += RunSqlServer(s);
		if (s.Only.Contains("sqlite")) failures += RunSqlite(s);
		Console.WriteLine(failures == 0 ? "IN6 DONE" : $"IN6 DONE WITH {failures} PART(S) SKIPPED OR FAILED (see the lines above)");
		return failures == 0 ? 0 : 1;
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

		if (s.Rows < 2 || s.Warmup < 0 || s.Ops < 1) throw new ArgumentException("--rows needs at least 2, --warmup at least 0, --ops at least 1");
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
		Console.WriteLine($"settings: rows {s.Rows}, warm-up {s.Warmup} + measured {s.Ops} operations per path, pressure K = {k} (processors {Environment.ProcessorCount}), parts: {string.Join(",", s.Only.Order())}");
		Console.WriteLine("read paths: sync (sync execute + Read) | async+Read (async execute + Read) | EnableAsyncRead (async execute + ReadAsync); all through Map<T>().ToList[Async]()");
		Console.WriteLine($"SQL Server connection: {s.SqlServer}");
		Console.WriteLine($"SQLite file: {(s.Scratch is null ? "(--scratch not given)" : Path.Combine(s.Scratch, "in6-<guid>.db"))}");
		int cells = 0;
		if (s.Only.Contains("sqlserver"))
		{
			Console.WriteLine("scenarios, SQL Server (SqlClient, tables in tempdb under a unique name, dropped afterwards):");
			foreach (var (id, text) in SqlServerScenarios(s.Rows)) Console.WriteLine($"  {id}: {text}");
			cells += 3 * 2;
		}

		if (s.Only.Contains("sqlite"))
		{
			Console.WriteLine("scenarios, SQLite (Microsoft.Data.Sqlite, a file database, deleted afterwards):");
			foreach (var (id, text) in SqliteScenarios(s.Rows)) Console.WriteLine($"  {id}: {text}");
			cells += 2 * 2;
		}

		Console.WriteLine($"each scenario runs with pressure off and on (K = {k}); a cell is one scenario x one pressure setting x 3 paths x {perPath} operations");
		Console.WriteLine($"cells: {cells}, operations: {cells * 3 * perPath}");
		if (s.Only.Contains("calibration"))
		{
			Console.WriteLine("calibration (SQL Server, sync Read, no pressure): per-row time = (time of the full result - time of 1 row) / (rows - 1), for width 5 and 20; per-NextResult time for k = 0..4 extra empty result sets");
			Console.WriteLine($"  operations: {2 * 2 * perPath} for the per-row part, {5 * perPath} for the NextResult part");
		}

		Console.WriteLine("output per path: reader-open median/min/max (ms), total mean (ms), allocated (KB, calling thread and whole process), ReadAsync calls, incomplete ReadAsync, continuations resumed on another thread");
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
		double allocThread = 0, allocProcess = 0, readAsync = 0, incomplete = 0, resumed = 0, otherAsync = 0, otherIncomplete = 0;
		bool oneReader = true;
		for (int i = 0; i < warmup + ops; i++)
		{
			stats.Reset();
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
			readAsync / ops, incomplete / ops, resumed / ops, otherAsync / ops, otherIncomplete / ops, oneReader);
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
			+ (r.OtherAsyncCalls > 0 ? $" | other async {N(r.OtherAsyncCalls)}/{N(r.OtherAsyncIncomplete)} incomplete" : "")
			+ (r.OneReaderPerOperation ? "" : " | WARNING: not one closed reader per operation");

	static void RunCell<T>(In6Settings s, string provider, string scenario, DbConnection connection, string sql, int expectedRows)
		where T : class
	{
		var stats = new In6Stats();
		var timed = new TimedConnection(connection, stats);
		foreach (int k in new[] { 0, s.K })
		{
			string label = k == 0 ? "off" : "on K=" + k;
			try
			{
				if (k > 0) ThreadPoolPressure.Start(k);
				foreach (var r in RunPaths<T>(timed, stats, sql, expectedRows, s.Warmup, s.Ops, (name, ex) => Console.WriteLine($"{provider,-9} {scenario,-3} {label,-9} {name,-16} FAILED: {ex.GetType().Name}: {ex.Message}")))
					Console.WriteLine(Line(provider, scenario, label, r));
			}
			catch (Exception ex)
			{
				Console.WriteLine($"{provider,-9} {scenario,-3} {label,-9} FAILED: {ex.GetType().Name}: {ex.Message}");
			}
			finally
			{
				ThreadPoolPressure.Stop();
			}
		}
	}

	// ---- test data ----------------------------------------------------------------
	static object CellValue(SyntheticTable table, int column, int row)
		=> table.Nulls[column] is { } nulls && nulls[row] ? DBNull.Value : table.Columns[column].GetValue(row)!;

	static DataTable ToDataTable(SyntheticTable table)
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

	static void Exec(DbConnection connection, string sql)
	{
		using var command = connection.CreateCommand();
		command.CommandText = sql;
		command.CommandTimeout = 300;
		command.ExecuteNonQuery();
	}

	// ---- SQL Server ----------------------------------------------------------------
	static int RunSqlServer(In6Settings s)
	{
		string suffix = Guid.NewGuid().ToString("N")[..12];
		string t20 = "ode_in6_w20_" + suffix, tBig = "ode_in6_big_" + suffix;
		SqlConnection connection;
		var created = new List<string>();
		try
		{
			connection = new SqlConnection(s.SqlServer);
			connection.Open();
		}
		catch (Exception ex)
		{
			Console.WriteLine($"SQL SERVER SKIPPED: could not open the connection: {ex.GetType().Name}: {ex.Message}");
			return 1;
		}

		using (connection)
		{
			try
			{
				Console.WriteLine($"SQL Server: {connection.DataSource}, database {connection.Database}, server version {connection.ServerVersion}; tables {t20} and {tBig} (dropped at the end)");
				Exec(connection, $"""
					create table [{t20}] (Id int not null primary key, Created datetime2 not null, Title nvarchar(50) not null, Price decimal(18,2) not null,
						Qty int null, IsActive bit not null, [Key] uniqueidentifier not null, Score float not null, Note nvarchar(50) null,
						Blob varbinary(16) not null, Big bigint not null, Body nvarchar(200) not null, Amount decimal(18,3) null, Stamp datetime2 null,
						Tag nvarchar(20) not null, Ratio float not null, Flag bit null, Link uniqueidentifier null, [Count] bigint not null, Summary nvarchar(200) null)
					""");
				created.Add(t20);
				using (var bulk = new SqlBulkCopy(connection) { DestinationTableName = $"[{t20}]", BulkCopyTimeout = 300, BatchSize = 5000 })
					bulk.WriteToServer(ToDataTable(SyntheticTable.Create(20, s.Rows)));

				if (s.Only.Contains("sqlserver"))
				{
					Exec(connection, $"create table [{tBig}] (Id int not null primary key, Body nvarchar(max) not null)");
					created.Add(tBig);
					Exec(connection, $"with n as (select top ({s.Rows}) row_number() over (order by (select null)) as i from sys.all_objects a cross join sys.all_objects b) insert into [{tBig}] (Id, Body) select i, replicate(cast(N'x' as nvarchar(max)), 4000) from n");
					Console.WriteLine($"data ready: {s.Rows} rows in each table; {ScalarLong(connection, $"select sum(datalength(Body)) from [{tBig}]") / s.Rows} bytes of Body per row in {tBig}");
					Console.WriteLine("-- SQL Server, three paths, pressure off and on --");
					RunCell<SynRow>(s, "SqlServer", "S1", connection, $"select Id, Created, Title, Price, Qty from [{t20}]", s.Rows);
					RunCell<SynRow>(s, "SqlServer", "S2", connection, $"select * from [{t20}]", s.Rows);
					RunCell<In6BigRow>(s, "SqlServer", "S3", connection, $"select Id, Body from [{tBig}]", s.Rows);
				}

				if (s.Only.Contains("calibration")) Calibrate(s, connection, t20);
				return 0;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"SQL SERVER PART FAILED: {ex.GetType().Name}: {ex.Message}");
				return 1;
			}
			finally
			{
				foreach (string table in created)
				{
					try { Exec(connection, $"drop table if exists [{table}]"); }
					catch (Exception ex) { Console.WriteLine($"could not drop {table}: {ex.Message}"); }
				}

				Console.WriteLine("SQL Server: tables dropped: " + string.Join(", ", created));
			}
		}
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
			RunCell<In6LiteRow>(s, "Sqlite", "L1", connection, "select Id, Created, Title, Price, Qty from t20", s.Rows);
			RunCell<In6LiteRow>(s, "Sqlite", "L2", connection, "select * from t20", s.Rows);
			Console.WriteLine("SQLite: ReadAsync is counted by the wrappers: an 'incomplete' count above 0 would mean it can complete asynchronously; 0 means every ReadAsync completed before returning.");
			return 0;
		}
		catch (Exception ex)
		{
			Console.WriteLine($"SQLITE PART FAILED: {ex.GetType().Name}: {ex.Message}");
			return 1;
		}
		finally
		{
			SqliteConnection.ClearAllPools();
			foreach (string path in new[] { file, file + "-journal", file + "-wal", file + "-shm" })
			{
				try { File.Delete(path); }
				catch (IOException ex) { Console.WriteLine($"could not delete {Path.GetFileName(path)}: {ex.Message}"); }
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
