using System.Data;
using System.Data.Common;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Benchmarks;

// BS-RP: the real-provider stage. The same variants as the synthetic suite (the ones that exist per provider), over SQL Server LocalDB and a SQLite
// FILE database, so the synthetic ranking can be checked against a real wire and real drivers (BS4). Sync only: on both providers ReadAsync is
// effectively synchronous (IN6), so this stage ranks the mapping paths and does not judge sync against async.
//
// Test data: tables of the synthetic catalog (the first Width columns, Rows rows, the SAME generator as the synthetic provider), created in GlobalSetup
// and dropped in GlobalCleanup. LocalDB: a dedicated database ode_rp_<guid> whose files live in the scratch folder (never tempdb, never the default
// location, never drive C:). If CREATE DATABASE fails the case stops with a clear message; there is no fallback. SQLite: a file under the
// scratch folder (unique name), deleted afterwards.
//
// Settings (environment variable or command-line option; the run scripts pass them):
//   ODX_RP_SCRATCH      --scratch <folder>        the folder for the LocalDB database files (and for the SQLite file when no other folder is given)
//   ODX_RP_SQLITE_DIR   --sqlite-dir <folder>     the folder for the SQLite file (default: the scratch folder)
//   ODX_RP_SQLSERVER    --connection "<string>"   the SQL Server connection (default: LocalDB, integrated security, Encrypt=False); the database is created through master
//   ODX_RP_PROVIDERS    --providers SqlServer,Sqlite   which providers (default: both)
//   ODX_RP_WIDTHS       --rp-widths 5,20          (default 5,20)
//   ODX_RP_ROWS         --rp-rows 1,100,10000     (default 1,100,10000)

/// <summary>Which parameter values a real-provider run uses, and the folders and connection it needs.</summary>
public static class RealProviderParams
{
	public const string SqlServer = "SqlServer", Sqlite = "Sqlite";
	public const string ProvidersVariable = "ODX_RP_PROVIDERS", WidthsVariable = "ODX_RP_WIDTHS", RowsVariable = "ODX_RP_ROWS",
		ScratchVariable = "ODX_RP_SCRATCH", SqliteDirVariable = "ODX_RP_SQLITE_DIR", SqlServerVariable = "ODX_RP_SQLSERVER";

	public const string DefaultSqlServer = @"Server=(localdb)\MSSQLLocalDB;Database=master;Integrated Security=true;Encrypt=False";

	public static readonly int[] DefaultWidths = [5, 20], DefaultRows = [1, 100, 10_000];

	public static IEnumerable<string> Providers => ParseProviders(Environment.GetEnvironmentVariable(ProvidersVariable));
	public static IEnumerable<int> Widths => SyntheticParams.Parse(Environment.GetEnvironmentVariable(WidthsVariable), DefaultWidths);
	public static IEnumerable<int> Rows => SyntheticParams.Parse(Environment.GetEnvironmentVariable(RowsVariable), DefaultRows);

	public static string[] ParseProviders(string? list)
	{
		if (string.IsNullOrWhiteSpace(list)) return [SqlServer, Sqlite];
		return
		[
			.. list.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(static p => p.ToLowerInvariant() switch
			{
				"sqlserver" or "localdb" => SqlServer,
				"sqlite" => Sqlite,
				_ => throw new ArgumentException("Unknown provider '" + p + "' (SqlServer or Sqlite).", nameof(list)),
			}),
		];
	}

	/// <summary>Maps a command-line option to its variable; false when the option is not one of ours.</summary>
	public static bool TryGetVariable(string option, out string variable)
	{
		variable = option switch
		{
			"--scratch" => ScratchVariable,
			"--sqlite-dir" => SqliteDirVariable,
			"--connection" => SqlServerVariable,
			"--providers" => ProvidersVariable,
			"--rp-widths" => WidthsVariable,
			"--rp-rows" => RowsVariable,
			_ => "",
		};
		return variable.Length > 0;
	}

	public static string? Get(string variable) => Environment.GetEnvironmentVariable(variable) is { Length: > 0 } v ? v : null;

	/// <summary>Puts the real-provider options found in the arguments into their variables (for the check modes, which run before the generic option handling).</summary>
	public static void ApplyOptions(string[] args)
	{
		for (int i = 0; i + 1 < args.Length; i++)
		{
			if (TryGetVariable(args[i], out string variable)) Environment.SetEnvironmentVariable(variable, args[++i]);
		}
	}
}

/// <summary>A provider or its database cannot be used (LocalDB absent, CREATE DATABASE refused, no scratch folder). The message says what to do; nothing falls back.</summary>
public sealed class RealProviderUnavailableException(string message) : Exception(message);

/// <summary>One case's data: the tables, the open real connection, the timing wrapper over it, and the recorder that receives the reader-open times.</summary>
public sealed class RealProviderCase
{
	// The catalog's 20 columns as each provider stores them (BS-RP.md has the table). The first Width of them make a table.
	sealed class Column(string name, string sqlServer, string sqlite)
	{
		public string Name { get; } = name;
		public string SqlServer { get; } = sqlServer;
		public string Sqlite { get; } = sqlite;
	}

	static readonly Column[] Columns =
	[
		new("[Id]", "int not null primary key", "integer not null primary key"),
		new("[Created]", "datetime2 not null", "text not null"),
		new("[Title]", "nvarchar(50) not null", "text not null"),
		new("[Price]", "decimal(18,2) not null", "text not null"),
		new("[Qty]", "int null", "integer null"),
		new("[IsActive]", "bit not null", "integer not null"),
		new("[Key]", "uniqueidentifier not null", "text not null"),
		new("[Score]", "float not null", "real not null"),
		new("[Note]", "nvarchar(50) null", "text null"),
		new("[Blob]", "varbinary(16) not null", "blob not null"),
		new("[Big]", "bigint not null", "integer not null"),
		new("[Body]", "nvarchar(200) not null", "text not null"),
		new("[Amount]", "decimal(18,3) null", "text null"),
		new("[Stamp]", "datetime2 null", "text null"),
		new("[Tag]", "nvarchar(20) not null", "text not null"),
		new("[Ratio]", "float not null", "real not null"),
		new("[Flag]", "bit null", "integer null"),
		new("[Link]", "uniqueidentifier null", "text null"),
		new("[Count]", "bigint not null", "integer not null"),
		new("[Summary]", "nvarchar(200) null", "text null"),
	];

	// Small files: 16 MB of data and 8 MB of log are enough for 10,000 rows of width 20 (a few MB); each grows by the same amount if it must.
	const string DataSize = "size = 16MB, filegrowth = 16MB", LogSize = "size = 8MB, filegrowth = 8MB";

	string? _database, _dataFile, _logFile, _sqliteFile, _masterConnection;
	bool _cleaned;

	RealProviderCase(string provider, int width, int rows, SyntheticTable table, DbConnection real, In6Stats stats)
	{
		Provider = provider;
		Width = width;
		Rows = rows;
		Table = table;
		Real = real;
		Stats = stats;
		Recorder = new SyntheticRecorder();
		stats.Recorder = Recorder;
		Timed = new TimedConnection(real, stats);
	}

	public string Provider { get; }
	public int Width { get; }
	public int Rows { get; }

	/// <summary>The seed data: the oracle every variant is compared with.</summary>
	public SyntheticTable Table { get; }

	/// <summary>The provider's own connection (SqlConnection or SqliteConnection), open for the life of the case.</summary>
	public DbConnection Real { get; }

	/// <summary>The same connection behind the timing wrapper: what the library variants and Dapper get.</summary>
	public TimedConnection Timed { get; }

	public In6Stats Stats { get; }

	public SyntheticRecorder Recorder { get; }

	public string Describe => Provider == RealProviderParams.SqlServer ? $"SQL Server database {_database} (data file {_dataFile})" : $"SQLite file {_sqliteFile}";

	public static RealProviderCase Create(string provider, int width, int rows)
		=> provider == RealProviderParams.SqlServer ? CreateSqlServer(width, rows) : provider == RealProviderParams.Sqlite ? CreateSqlite(width, rows)
			: throw new ArgumentException("Unknown provider " + provider, nameof(provider));

	static string Ddl(int width, Func<Column, string> type)
		=> "create table [" + RealProviderVariants.Table + "] (" + string.Join(", ", Columns.Take(width).Select(c => c.Name + " " + type(c))) + ")";

	static string FullPath(string folder) => Path.GetFullPath(folder);

	// ---- SQL Server ----------------------------------------------------------------
	static string ConnectionString(string baseConnection, string database)
		=> new SqlConnectionStringBuilder(baseConnection) { InitialCatalog = database, Pooling = false }.ConnectionString;

	static RealProviderCase CreateSqlServer(int width, int rows)
	{
		string baseConnection = RealProviderParams.Get(RealProviderParams.SqlServerVariable) ?? RealProviderParams.DefaultSqlServer;
		if (RealProviderParams.Get(RealProviderParams.ScratchVariable) is not { } scratch)
			throw new RealProviderUnavailableException("STOP: no scratch folder for the SQL Server database files. Pass --scratch <folder> or set ODX_RP_SCRATCH (a folder not on drive C:). There is no fallback to tempdb or to the default location.");
		string folder = FullPath(scratch);
		string name = "ode_rp_" + Guid.NewGuid().ToString("N")[..12];
		string dataFile = Path.Combine(folder, name + ".mdf"), logFile = Path.Combine(folder, name + "_log.ldf");
		string master = ConnectionString(baseConnection, "master");
		try
		{
			using var probe = new SqlConnection(master);
			probe.Open();
			Directory.CreateDirectory(folder);
			In6Probe.Exec(probe, $"create database [{name}] on (name = N'{name}', filename = N'{In6Probe.Quote(dataFile)}', {DataSize}) log on (name = N'{name}_log', filename = N'{In6Probe.Quote(logFile)}', {LogSize})");
		}
		catch (Exception ex)
		{
			DropDatabase(master, name, dataFile, logFile, out _);
			throw new RealProviderUnavailableException($"STOP: SQL SERVER (LocalDB) is not usable: could not open the connection or CREATE DATABASE [{name}] with its files in the scratch folder: {ex.GetType().Name}: {ex.Message} There is no fallback to tempdb or to the default location.");
		}

		SqlConnection? real = null;
		try
		{
			var table = SyntheticTable.Create(width, rows);
			real = new SqlConnection(ConnectionString(baseConnection, name));
			real.Open();
			In6Probe.Exec(real, Ddl(width, static c => c.SqlServer));
			using (var bulk = new SqlBulkCopy(real) { DestinationTableName = "[" + RealProviderVariants.Table + "]", BulkCopyTimeout = 300, BatchSize = 5000 })
				bulk.WriteToServer(In6Probe.ToDataTable(table));
			return new RealProviderCase(RealProviderParams.SqlServer, width, rows, table, real, new In6Stats())
			{
				_database = name,
				_dataFile = dataFile,
				_logFile = logFile,
				_masterConnection = master,
			};
		}
		catch
		{
			real?.Dispose();
			SqlConnection.ClearAllPools();
			DropDatabase(master, name, dataFile, logFile, out _);
			throw;
		}
	}

	// Drops the database (single user first so an open connection cannot block it) and reports whether both files are gone. Never throws.
	static bool DropDatabase(string master, string name, string dataFile, string logFile, out string problem)
	{
		problem = "";
		try
		{
			using var connection = new SqlConnection(master);
			connection.Open();
			In6Probe.Exec(connection, $"if db_id(N'{name}') is not null alter database [{name}] set single_user with rollback immediate");
			In6Probe.Exec(connection, $"if db_id(N'{name}') is not null drop database [{name}]");
		}
		catch (Exception ex)
		{
			problem = $"could not drop database {name}: {ex.GetType().Name}: {ex.Message}";
		}

		if (File.Exists(dataFile) || File.Exists(logFile)) problem += (problem.Length > 0 ? "; " : "") + $"database files still exist (data {File.Exists(dataFile)}, log {File.Exists(logFile)})";
		return problem.Length == 0;
	}

	// ---- SQLite ----------------------------------------------------------------
	static RealProviderCase CreateSqlite(int width, int rows)
	{
		string scratch = RealProviderParams.Get(RealProviderParams.SqliteDirVariable) ?? RealProviderParams.Get(RealProviderParams.ScratchVariable)
			?? throw new RealProviderUnavailableException("STOP: no folder for the SQLite file. Pass --sqlite-dir <folder> (or --scratch <folder>) or set ODX_RP_SQLITE_DIR / ODX_RP_SCRATCH (a folder not on drive C:).");
		string folder = FullPath(scratch);
		Directory.CreateDirectory(folder);
		string file = Path.Combine(folder, "rp-" + Guid.NewGuid().ToString("N")[..12] + ".db");
		SqliteConnection? real = null;
		try
		{
			var table = SyntheticTable.Create(width, rows);
			real = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = file, Pooling = false, Mode = SqliteOpenMode.ReadWriteCreate }.ToString());
			real.Open();
			In6Probe.Exec(real, Ddl(width, static c => c.Sqlite));
			using (var transaction = real.BeginTransaction())
			{
				using var insert = real.CreateCommand();
				insert.Transaction = transaction;
				insert.CommandText = "insert into [" + RealProviderVariants.Table + "] values (" + string.Join(",", Enumerable.Range(0, width).Select(static c => "$p" + c)) + ")";
				var parameters = new SqliteParameter[width];
				for (int c = 0; c < width; c++) insert.Parameters.Add(parameters[c] = new SqliteParameter("$p" + c, null));
				for (int i = 0; i < rows; i++)
				{
					for (int c = 0; c < width; c++) parameters[c].Value = In6Probe.CellValue(table, c, i);
					insert.ExecuteNonQuery();
				}

				transaction.Commit();
			}

			return new RealProviderCase(RealProviderParams.Sqlite, width, rows, table, real, new In6Stats()) { _sqliteFile = file };
		}
		catch
		{
			real?.Dispose();
			SqliteConnection.ClearAllPools();
			DeleteSqliteFiles(file);
			throw;
		}
	}

	static void DeleteSqliteFiles(string file)
	{
		foreach (string path in new[] { file, file + "-journal", file + "-wal", file + "-shm" })
		{
			try { File.Delete(path); }
			catch (IOException) { }
			catch (UnauthorizedAccessException) { }
		}
	}

	/// <summary>
	/// Closes the connection and removes what this case created: LocalDB: SINGLE_USER WITH ROLLBACK IMMEDIATE, DROP DATABASE, then both files must be gone;
	/// SQLite: the file. Throws (loudly) when something is left behind. Safe to call twice.
	/// </summary>
	public void Cleanup()
	{
		if (_cleaned) return;
		_cleaned = true;
		Real.Dispose();
		if (_sqliteFile is { } file)
		{
			SqliteConnection.ClearAllPools();
			DeleteSqliteFiles(file);
			if (File.Exists(file)) throw new IOException("The SQLite file was not deleted: " + file);
			return;
		}

		SqlConnection.ClearAllPools();
		if (!DropDatabase(_masterConnection!, _database!, _dataFile!, _logFile!, out string problem))
			throw new IOException("LocalDB cleanup failed: " + problem);
	}

	/// <summary>True when the case's files are gone (the SQLite file, or both LocalDB files).</summary>
	public bool FilesGone => _sqliteFile is { } f ? !File.Exists(f) : !File.Exists(_dataFile!) && !File.Exists(_logFile!);
}

/// <summary>The ReaderOpen column only: there is no Drain column here (the wrapper does not time NextResult).</summary>
public sealed class RealProviderConfig : ManualConfig
{
	public RealProviderConfig()
	{
		Add(DefaultConfig.Instance);
		AddColumn(new OpenTimeColumn("ReaderOpen", "Reader-open: from ExecuteReader until the reader is closed (the timing wrapper's clock, or V9c's own stopwatch on the real reader), summed over the readers of one operation. MEAN per operation over the iterations BenchmarkDotNet's Mean is made of", static s => s.OpenUs));
	}
}

/// <summary>
/// V1, V2, V3, V7 and V9c over SQL Server LocalDB and a SQLite file, sync. Run it in process (<c>--inprocess</c>) to get the ReaderOpen column.
/// Parameters: Provider, Width (5, 20), Rows (1, 100, 10000).
/// </summary>
[MemoryDiagnoser]
[Config(typeof(RealProviderConfig))]
public class RealProviderBenchmarks
{
	[ParamsSource(nameof(ProviderValues), Priority = 0)]
	public string Provider { get; set; } = "";

	[ParamsSource(nameof(WidthValues), Priority = 1)]
	public int Width { get; set; }

	[ParamsSource(nameof(RowValues), Priority = 2)]
	public int Rows { get; set; }

	public static IEnumerable<string> ProviderValues => RealProviderParams.Providers;
	public static IEnumerable<int> WidthValues => RealProviderParams.Widths;
	public static IEnumerable<int> RowValues => RealProviderParams.Rows;

	// The case of this run; --verify-rp drives the benchmark methods through Setup and reads its recorder.
	internal RealProviderCase Case { get; private set; } = null!;

	Func<System.Data.IDataRecord, SynRow> _hand = null!;
	Func<List<SynRow>> _v9c = null!;
	Func<DbDataReader, SynRow> _v9cWrapped = null!;
	string _label = "";

	[GlobalSetup]
	public void Setup()
	{
		Case = RealProviderCase.Create(Provider, Width, Rows);
		RealProviderVariants.PrepareDapper(Provider);
		_hand = SyntheticVariants.Hand(Width);
		_v9cWrapped = Hands.For(Width).CDb;
		var recorder = Case.Recorder;
		if (Case.Real is SqlConnection sql)
		{
			var read = RealProviderVariants.SqlReader(Width);
			_v9c = () => RealProviderVariants.V9cSql(sql, recorder, read);
		}
		else
		{
			var lite = (SqliteConnection)Case.Real;
			var read = RealProviderVariants.LiteReader(Width);
			_v9c = () => RealProviderVariants.V9cLite(lite, recorder, read);
		}

		SyntheticIterationListener.Attach(recorder);
	}

	[GlobalCleanup]
	public void Cleanup()
	{
		if (Case is null) return;
		if (_label.Length > 0)
			SyntheticBenchmarks.FileResults(SyntheticBenchmarks.Key(_label, [(nameof(Provider), Provider), (nameof(Width), Width), (nameof(Rows), Rows)]), Case.Recorder);
		Case.Cleanup();
		Console.WriteLine($"// REALPROVIDER {Provider} width={Width} rows={Rows}: test data removed, files gone: {Case.FilesGone}");
	}

	[Benchmark(Baseline = true)]
	public List<SynRow> V1_Sync() { _label = nameof(V1_Sync); return RealProviderVariants.V1(Case.Timed); }

	[Benchmark]
	public List<SynRow> V2_Sync() { _label = nameof(V2_Sync); return RealProviderVariants.V2(Case.Timed, _hand); }

	[Benchmark]
	public List<SynRow> V9c_Sync() { _label = nameof(V9c_Sync); return _v9c(); }

	[Benchmark]
	public List<SynItem> V3_Sync() { _label = nameof(V3_Sync); return RealProviderVariants.V3(Case.Timed); }

	[Benchmark]
	public List<SynRow> V7_Sync() { _label = nameof(V7_Sync); return RealProviderVariants.V7(Case.Timed); }

	// V_best: the slot for the best BS-C candidate. EMPTY until the planning session names it (nothing is wired, nothing is guessed).

	// The control for the timing wrapper (not in the run matrix): V9c's loop with DbDataReader getters through the wrapper. Its gap to V9c_Sync is the
	// forwarding (and the sealed-type devirtualization); its gap to V1_Sync and V2_Sync is what the mapping path costs on equal footing.
	[Benchmark]
	public List<SynRow> V9c_Wrapped_Sync() { _label = nameof(V9c_Wrapped_Sync); return RealProviderVariants.V9cWrapped(Case.Timed, _v9cWrapped); }
}
