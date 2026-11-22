using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Benchmarks;

// S4: the command line of the large-value probe (the cell runner and the self-test are in S4Cells.cs).
//   --s4 <scenario>:<path>   run exactly one cell in this process and print one S4RESULT line and one S4VERIFY line
//   --s4-setup               create the database ode_s4_<id> and the four tables, server-side (REPLICATE); prints "S4DB name=<id>"
//   --s4-teardown            drop the database named by --s4-db
//   --s4-rtt                 50 warm-up and 200 timed SELECT 1 round trips
//   --selftest-s4            no server: the cell runner, the result line format and the checksum on SQLite
//   server options (as --in6 with --sql-tcp): --sql-tcp <host,port> --sql-password-file <path> [--affinity <hex>] [--priority belownormal]
//   --s4-db <name>           the database (for --s4 and --s4-teardown)
//   --sql-localdb <instance> use integrated security against a LocalDB instance instead of --sql-tcp/--sql-password-file (smoke tests)
//   --s4-command-timeout <s> --s4 only: the command timeout in seconds of every cell path (default 30, the provider's default; printed as cmd_timeout=)
//   --s4-sqlclient-switches new|default   --s4 only: `new` sets Switch.Microsoft.Data.SqlClient.UseCompatibilityAsyncBehaviour and ...UseCompatibilityProcessSni to false before any SqlClient use (default: unset);
//                            every process prints one S4SWITCHES line with the state read back, and S4RESULT ends with switches=new|default
//   --s4-scale <divisor>     setup only: divide the value sizes (for example 1024) so a smoke test runs in seconds; the same expressions as the full size
//   --s4-data-dir <folder>   setup only: a folder INSIDE the server's file system for the data and log files (default: the server's own default location)
public static partial class S4Probe
{
	public static bool Handles(string[] args) => args.Any(static a => a.StartsWith("--s4", StringComparison.Ordinal) || a == "--selftest-s4");

	// Everything that must never be printed: the password and every connection string built from it.
	static readonly List<string> Secrets = [];

	static string Scrub(string text)
	{
		foreach (string secret in Secrets)
		{
			if (secret.Length > 0) text = text.Replace(secret, "***", StringComparison.Ordinal);
		}

		return PasswordPair().Replace(text, "$1=***");
	}

	[System.Text.RegularExpressions.GeneratedRegex(@"(?i)\b(password|pwd)\s*=\s*[^;]*")]
	private static partial System.Text.RegularExpressions.Regex PasswordPair();

	sealed class Options
	{
		public string? Cell, Db, DataDir, SqlTcp, PasswordFile, LocalDb, Affinity, Priority;
		public int Scale = 1;
		public ushort CommandTimeout = 30;
		public string? SqlClientSwitches;
		public bool Setup, Teardown, Rtt, SelfTest, CommandTimeoutSet;
	}

	public static int Run(string[] args)
	{
		try
		{
			// First, before any SqlClient type is touched: SqlClient reads its AppContext switches once, on first use.
			int at = Array.IndexOf(args, "--s4-sqlclient-switches");
			S4Switches.Apply(at < 0 ? "default" : at + 1 < args.Length ? args[at + 1] : throw new ArgumentException("--s4-sqlclient-switches needs a value"));
			Console.WriteLine(S4Switches.Line());
			return RunCore(Parse(args));

		}
		catch (Exception ex)
		{
			Console.WriteLine($"S4 ERROR: {ex.GetType().Name}: {Scrub(ex.Message)}");
			return 1;
		}
	}

	static Options Parse(string[] args)
	{
		var o = new Options();
		for (int i = 0; i < args.Length; i++)
		{
			string Value() => ++i < args.Length ? args[i] : throw new ArgumentException(args[i - 1] + " needs a value");
			switch (args[i])
			{
				case "--s4": o.Cell = Value(); break;
				case "--s4-setup": o.Setup = true; break;
				case "--s4-teardown": o.Teardown = true; break;
				case "--s4-rtt": o.Rtt = true; break;
				case "--selftest-s4": o.SelfTest = true; break;
				case "--s4-db": o.Db = Value(); break;
				case "--s4-data-dir": o.DataDir = Value(); break;
				case "--s4-command-timeout": o.CommandTimeout = ushort.TryParse(Value(), NumberStyles.None, CultureInfo.InvariantCulture, out ushort seconds) ? seconds : throw new ArgumentException("--s4-command-timeout takes a number of seconds (0 to 65535; 0 is no limit)"); o.CommandTimeoutSet = true; break;
				case "--s4-sqlclient-switches": o.SqlClientSwitches = Value(); break;
				case "--s4-scale": o.Scale = int.TryParse(Value(), NumberStyles.None, CultureInfo.InvariantCulture, out int scale) && scale >= 1 ? scale : throw new ArgumentException("--s4-scale takes an integer divisor of at least 1"); break;
				case "--sql-localdb": o.LocalDb = Value(); break;
				case "--sql-tcp": o.SqlTcp = Value(); break;
				case "--sql-password-file": o.PasswordFile = Value(); break;
				case "--affinity": o.Affinity = Value(); break;
				case "--priority": o.Priority = Value(); break;
				default: throw new ArgumentException("unknown option " + args[i]);
			}
		}

		return o;
	}

	static int RunCore(Options o)
	{
		if (o.SelfTest) return S4Cells.SelfTest();
		int modes = (o.Cell is null ? 0 : 1) + (o.Setup ? 1 : 0) + (o.Teardown ? 1 : 0) + (o.Rtt ? 1 : 0);
		if (modes != 1) throw new ArgumentException("exactly one of --s4 <scenario>:<path>, --s4-setup, --s4-teardown, --s4-rtt, --selftest-s4 is needed");
		if (o.LocalDb is null && (o.SqlTcp is null || o.PasswordFile is null)) throw new ArgumentException("--sql-tcp <host,port> and --sql-password-file <path> are needed (or --sql-localdb <instance>)");
		if (o.SqlClientSwitches is not null && o.Cell is null) throw new ArgumentException("--s4-sqlclient-switches is only used with --s4");
		if (o.CommandTimeoutSet && o.Cell is null) throw new ArgumentException("--s4-command-timeout is only used with --s4");
		if (o.Scale != 1 && !o.Setup) throw new ArgumentException("--s4-scale is only used with --s4-setup");
		if (o.DataDir is not null && !o.Setup) throw new ArgumentException("--s4-data-dir is only used with --s4-setup");
		if ((o.Cell is not null || o.Teardown) && o.Db is null) throw new ArgumentException("--s4-db <name> is needed");
		if (o.Setup && o.LocalDb is not null && o.DataDir is null) throw new ArgumentException("--s4-data-dir <folder> is needed with --sql-localdb: LocalDB would otherwise put the database files in its default location");
		ApplyProcessSettings(o);
		if (o.Cell is not null) return RunCell(o);
		if (o.Setup) return Setup(o);
		if (o.Teardown) return Teardown(o);
		return Rtt(o);
	}

	static void ApplyProcessSettings(Options o)
	{
		if (o.Priority is null && o.Affinity is null) return;
		var process = Process.GetCurrentProcess();
		if (o.Priority is not null)
		{
			if (!Enum.TryParse(o.Priority, ignoreCase: true, out ProcessPriorityClass priority) || priority is ProcessPriorityClass.High or ProcessPriorityClass.RealTime || !Enum.IsDefined(priority))
				throw new ArgumentException("--priority takes belownormal, normal, abovenormal or idle");
			process.PriorityClass = priority;
		}

		if (o.Affinity is not null)
		{
			string hex = o.Affinity.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? o.Affinity[2..] : o.Affinity;
			if (!long.TryParse(hex, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out long mask) || mask <= 0) throw new ArgumentException("--affinity takes a non-zero hexadecimal mask, for example 0xF00000");
			if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux()) SetAffinity(process, mask);
			else throw new PlatformNotSupportedException("--affinity needs Windows or Linux");
			ThreadPool.GetMinThreads(out _, out int io);
			ThreadPool.SetMinThreads(System.Numerics.BitOperations.PopCount((ulong)mask), io);
		}

		long affinity = OperatingSystem.IsWindows() || OperatingSystem.IsLinux() ? process.ProcessorAffinity : 0;
		Console.WriteLine($"process: priority {process.PriorityClass}, affinity 0x{affinity:X}");
	}

	[System.Runtime.Versioning.SupportedOSPlatform("windows")]
	[System.Runtime.Versioning.SupportedOSPlatform("linux")]
	static void SetAffinity(Process process, long mask) => process.ProcessorAffinity = new IntPtr(mask);

	static string Connection(Options o, string database)
	{
		if (o.LocalDb is not null)
		{
			string local = new SqlConnectionStringBuilder { DataSource = o.LocalDb, IntegratedSecurity = true, TrustServerCertificate = true, InitialCatalog = database, Pooling = false }.ConnectionString;
			Secrets.Add(local);
			return local;
		}

		string password = File.ReadAllText(o.PasswordFile!).Trim();
		if (password.Length == 0) throw new InvalidOperationException("the password file is empty");
		Secrets.Add(password);
		string[] parts = o.SqlTcp!.Split(',', StringSplitOptions.TrimEntries);
		if (parts.Length != 2 || parts[0].Length == 0 || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int port) || port is < 1 or > 65535)
			throw new ArgumentException("--sql-tcp takes <host,port>, for example 127.0.0.1,14333");
		string text = new SqlConnectionStringBuilder { DataSource = "tcp:" + parts[0] + "," + parts[1], UserID = "sa", Password = password, TrustServerCertificate = true, InitialCatalog = database, Pooling = false }.ConnectionString;
		Secrets.Add(text);
		return text;
	}

	static SqlConnection Open(Options o, string database)
	{
		var connection = new SqlConnection(Connection(o, database));
		connection.Open();
		return connection;
	}

	static void Exec(DbConnection connection, string sql)
	{
		using var command = connection.CreateCommand();
		command.CommandText = sql;
		command.CommandTimeout = 600;
		command.ExecuteNonQuery();
	}

	static long ScalarLong(DbConnection connection, string sql)
	{
		using var command = connection.CreateCommand();
		command.CommandText = sql;
		command.CommandTimeout = 600;
		return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
	}

	static int RunCell(Options o)
	{
		var (scenario, path) = S4Cells.ParseCell(o.Cell!);
		try
		{
			using var connection = Open(o, o.Db!);
			var result = S4Cells.RunAsync(connection, scenario, path, scenario.WarmupSqlServer, o.CommandTimeout).GetAwaiter().GetResult();
			Console.WriteLine(result.Line(scenario.Id, path));
			Console.WriteLine(result.VerifyLine(scenario.Id, path));
			return 0;
		}
		catch (Exception ex)
		{
			Console.WriteLine($"S4FAIL scenario={scenario.Id} path={path} error={ex.GetType().Name}: {Scrub(ex.Message).Replace('\n', ' ').Replace('\r', ' ')}");
			return 1;
		}
	}

	const string Pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

	static int Repeats(int full, int scale) => Math.Max(1, full / scale);

	static int Setup(Options o)
	{
		string name = "ode_s4_" + Guid.NewGuid().ToString("N")[..12];
		using var master = Open(o, "master");
		try
		{
			if (o.DataDir is null)
			{
				Exec(master, $"create database [{name}]");
			}
			else
			{
				string dir = o.DataDir.TrimEnd('/', '\\').Replace("'", "''", StringComparison.Ordinal);
				char separator = dir.Contains('\\', StringComparison.Ordinal) ? '\\' : '/';
				string data = o.Scale > 1 ? "64MB" : "640MB", log = o.Scale > 1 ? "32MB" : "256MB";
				Exec(master, $"create database [{name}] on (name = N'{name}', filename = N'{dir}{separator}{name}.mdf', size = {data}, filegrowth = 32MB) log on (name = N'{name}_log', filename = N'{dir}{separator}{name}_log.ldf', size = {log}, filegrowth = 32MB)");
			}

			Console.WriteLine($"S4DB name={name}");
			Exec(master, $"alter database [{name}] set recovery simple");
			if (o.DataDir is null)
			{
				Exec(master, $"alter database [{name}] modify file (name = N'{name}', size = 640MB)");
				Exec(master, $"alter database [{name}] modify file (name = N'{name}_log', size = 256MB)");
			}

			master.ChangeDatabase(name);
			string rows100 = "(select top (100) cast(row_number() over (order by (select null)) as int) as n from sys.all_objects) t";
			// REPLICATE of binary data returns varchar, not varbinary: the 1 KB random block goes through varchar(max) (binary to character without a style is a raw
			// byte copy) and the result is converted back with CONVERT. REPLICATE of a (max) value yields a (max) value, so there is no 8000-byte truncation;
			// datalength is verified after the inserts.
			int v1 = Repeats(102400, o.Scale), v2 = Repeats(1024, o.Scale), n1 = Repeats(781250, o.Scale), n2 = Repeats(4096, o.Scale);
			Exec(master, "create table [V1] (Id int not null primary key, Body varbinary(max) not null)");
			Exec(master, $"insert [V1] (Id, Body) select 1, convert(varbinary(max), replicate(convert(varchar(max), crypt_gen_random(1024)), {v1}))");
			Exec(master, "create table [V2] (Id int not null primary key, Body varbinary(max) not null)");
			Exec(master, $"insert [V2] (Id, Body) select n, convert(varbinary(max), replicate(convert(varchar(max), crypt_gen_random(1024)), {v2})) from {rows100}");
			Exec(master, "create table [N1] (Id int not null primary key, Body nvarchar(max) not null)");
			Exec(master, $"insert [N1] (Id, Body) select 1, replicate(convert(nvarchar(max), N'{Pattern}'), {n1})");
			Exec(master, "create table [N2] (Id int not null primary key, Body nvarchar(max) not null)");
			Exec(master, $"insert [N2] (Id, Body) select n, stuff(replicate(convert(nvarchar(max), N'{Pattern}'), {n2}), 1, 3, right(N'000' + convert(nvarchar(3), n), 3)) from {rows100}");
			var expectedLength = new Dictionary<string, long> { ["V1"] = v1 * 1024L, ["V2"] = v2 * 1024L, ["N1"] = n1 * 64L * 2, ["N2"] = n2 * 64L * 2 };
			master.ChangeDatabase("master");
			int bad = 0;
			foreach (var scenario in S4Scenario.All)
			{
				master.ChangeDatabase(name);
				long rows = ScalarLong(master, $"select count(*) from [{scenario.Id}]");
				long bytes = ScalarLong(master, $"select sum(cast(datalength(Body) as bigint)) from [{scenario.Id}]");
				long expectedBytes = scenario.Rows * expectedLength[scenario.Id];
				bool ok = rows == scenario.Rows && bytes == expectedBytes;
				if (!ok) bad++;
				Console.WriteLine($"S4SETUP table={scenario.Id} rows={rows} bytes={bytes} expected_rows={scenario.Rows} expected_bytes={expectedBytes} {(ok ? "ok" : "MISMATCH")}");
			}

			master.ChangeDatabase("master");
			return bad == 0 ? 0 : 1;
		}
		catch (Exception ex)
		{
			Console.WriteLine($"S4 SETUP FAILED: {ex.GetType().Name}: {Scrub(ex.Message)}");
			try { DropDatabase(master, name); }
			catch (Exception cleanup) { Console.WriteLine($"cleanup after the failed setup also failed: {Scrub(cleanup.Message)}"); }

			return 1;
		}
	}

	static long DropDatabase(SqlConnection master, string name)
	{
		SqlConnection.ClearAllPools();
		master.ChangeDatabase("master");
		long databaseId = ScalarLong(master, $"select isnull(db_id(N'{name}'), 0)");
		if (databaseId == 0) return 0;
		Exec(master, $"alter database [{name}] set single_user with rollback immediate");
		Exec(master, $"drop database [{name}]");
		return ScalarLong(master, $"select count(*) from sys.master_files where database_id = {databaseId}");
	}

	static int Teardown(Options o)
	{
		using var master = Open(o, "master");
		long files = DropDatabase(master, o.Db!);
		Console.WriteLine($"S4TEARDOWN db={o.Db} files_after={files}");
		return files == 0 ? 0 : 1;
	}

	static int Rtt(Options o)
	{
		const int Warm = 50, Timed = 200;
		using var connection = Open(o, "master");
		using var command = connection.CreateCommand();
		command.CommandText = "select 1";
		for (int i = 0; i < Warm; i++) command.ExecuteScalar();
		var times = new double[Timed];
		for (int i = 0; i < Timed; i++)
		{
			long t0 = Stopwatch.GetTimestamp();
			command.ExecuteScalar();
			times[i] = (Stopwatch.GetTimestamp() - t0) * 1000d / Stopwatch.Frequency;
		}

		Array.Sort(times);
		Console.WriteLine($"rtt (SELECT 1 round trip, ms): median {((times[Timed / 2 - 1] + times[Timed / 2]) / 2).ToString("F3", CultureInfo.InvariantCulture)} min {times[0].ToString("F3", CultureInfo.InvariantCulture)} max {times[^1].ToString("F3", CultureInfo.InvariantCulture)} (n={Timed})");
		return 0;
	}
}
