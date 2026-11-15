using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Benchmarks;

// S4: large values (100 MB and 1 MB, binary and text) read WHOLE into memory, by eight paths, one (scenario, path) cell per process.
//   R1  raw ADO.NET, CommandBehavior.Default,          sync Read, sync getters
//   R2  raw ADO.NET, CommandBehavior.Default,          await ReadAsync, sync getters
//   R3  raw ADO.NET, CommandBehavior.SequentialAccess, sync Read, sync getters
//   R4  raw ADO.NET, CommandBehavior.SequentialAccess, await ReadAsync, sync getters
//   R5  raw ADO.NET, CommandBehavior.SequentialAccess, await ReadAsync, await GetFieldValueAsync<T> for the large column (Id: the sync getter GetInt32)
//   O1  ODX EnableAsyncRead().Map(selector).ToListAsync()   (Default + ReadAsync inside ODX)
//   O2  ODX EnableAsyncRead().Map<T>().ToListAsync()        (SequentialAccess + ReadAsync inside ODX)
//   O3  ODX Map<T>().ToList()                               (SequentialAccess + sync Read)
// The raw paths use the provider's own connection and command with no wrapper types; the ODX paths run behind the IN6 timing wrapper (wrapper=yes).

/// <summary>A row with an Id and a binary large value.</summary>
public sealed class S4BinRow
{
	public int Id { get; set; }
	public byte[]? Body { get; set; }
}

/// <summary>A row with an Id and a text large value.</summary>
public sealed class S4StrRow
{
	public int Id { get; set; }
	public string? Body { get; set; }
}

/// <summary>One scenario: a table named like the scenario, rows of Id and Body; ValueSize is in bytes (binary) or characters (text).</summary>
public sealed record S4Scenario(string Id, bool Binary, int Rows, int ValueSize)
{
	public static readonly S4Scenario[] All =
	[
		new("V1", true, 1, 100 * 1_048_576),
		new("V2", true, 100, 1_048_576),
		new("N1", false, 1, 50_000_000),
		new("N2", false, 100, 262_144),
	];

	/// <summary>The same four shapes scaled down to KB, for the self-test.</summary>
	public static readonly S4Scenario[] Small =
	[
		new("V1", true, 1, 100 * 1024),
		new("V2", true, 100, 1024),
		new("N1", false, 1, 50_000),
		new("N2", false, 100, 256),
	];

	public static S4Scenario Find(string id)
		=> All.FirstOrDefault(x => x.Id.Equals(id, StringComparison.OrdinalIgnoreCase)) ?? throw new ArgumentException($"unknown scenario '{id}' (V1, V2, N1, N2)");

	public string Sql => $"select Id, Body from [{Id}] order by Id";

	/// <summary>One 1 KB row made by the server itself, read by the same path before the measured read (JIT, first-use costs).</summary>
	public string WarmupSqlServer => Binary
		? "select 1 as Id, convert(varbinary(max), replicate(convert(varchar(max), 0x41), 1024)) as Body"
		: "select 1 as Id, replicate(cast(N'a' as nvarchar(max)), 512) as Body";

	public string WarmupSqlite => Binary
		? "select 1 as Id, zeroblob(1024) as Body"
		: "select 1 as Id, replace(hex(zeroblob(256)), '0', 'a') as Body";
}

/// <summary>What was read: rows, total value bytes (text counts 2 bytes per char) and a checksum over the Id and the first and last 64 bytes of every value.</summary>
public readonly record struct S4Verify(int Rows, long Bytes, ulong Check)
{
	public string Format() => $"rows={Rows} bytes={Bytes} first_last_checksum={Check:x16}";
}

/// <summary>The counters one cell collects; the raw paths fill them inline, the ODX paths copy them from the wrapper.</summary>
public sealed class S4Counters
{
	public long OpenTicks, AsyncReadTicks, AsyncGetterTicks;
	public int AsyncReads, AsyncGetters;
	public bool Wrapper;
	/// <summary>The command timeout in seconds applied to every command of the cell (not reset between the warm-up and the measured read).</summary>
	public ushort CommandTimeout = 30;

	public void Reset()
	{
		OpenTicks = AsyncReadTicks = AsyncGetterTicks = 0;
		AsyncReads = AsyncGetters = 0;
		Wrapper = false;
	}
}

/// <summary>The result of one cell.</summary>
public sealed class S4Result(S4Counters counters, long totalTicks, long allocBytes, long peakWorkingSet, S4Verify verify)
{
	public S4Counters Counters => counters;
	public S4Verify Verify => verify;
	public long AllocBytes => allocBytes;
	public long PeakWorkingSet => peakWorkingSet;

	static string Ms(long ticks) => (ticks * 1000d / Stopwatch.Frequency).ToString("F3", CultureInfo.InvariantCulture);

	public string Line(string scenario, string path)
		=> $"S4RESULT scenario={scenario} path={path} open_ms={Ms(counters.OpenTicks)} total_ms={Ms(totalTicks)} alloc_bytes={allocBytes} peak_ws_bytes={peakWorkingSet}"
		+ $" async_reads={counters.AsyncReads} async_wait_ms={Ms(counters.AsyncReadTicks)} async_getters={counters.AsyncGetters} async_getter_wait_ms={Ms(counters.AsyncGetterTicks)}"
		+ $" rows={verify.Rows} bytes={verify.Bytes} wrapper={(counters.Wrapper ? "yes" : "no")} cmd_timeout={counters.CommandTimeout} switches={S4Switches.Mode}";

	public string VerifyLine(string scenario, string path) => $"S4VERIFY scenario={scenario} path={path} {verify.Format()}";
}

/// <summary>How to build, read, measure and check one row type.</summary>
abstract class S4Kind<TRow, TBody> where TRow : class, new()
{
	public abstract Func<IDataRecord, TRow> Selector { get; }
	public abstract void Set(TRow row, int id, TBody body);
	public abstract int IdOf(TRow row);
	public abstract TBody? BodyOf(TRow row);
	/// <summary>The sync getter for the large column.</summary>
	public abstract TBody ReadBody(DbDataReader reader);
	public abstract long Length(TBody body);
	public abstract ulong Mix(ulong hash, TBody body);
	/// <summary>The deterministic value of a row of the self-test data (salt changes the value).</summary>
	public abstract TBody Make(int row, int size, int salt);

	internal static ulong Fnv(ulong hash, ReadOnlySpan<byte> data)
	{
		foreach (byte b in data)
		{
			hash ^= b;
			hash *= 1_099_511_628_211UL;
		}

		return hash;
	}

	public S4Verify Summarize(List<TRow> rows)
	{
		ulong hash = 14_695_981_039_346_656_037UL;
		long bytes = 0;
		Span<byte> id = stackalloc byte[4];
		foreach (var row in rows)
		{
			System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(id, IdOf(row));
			hash = Fnv(hash, id);
			var body = BodyOf(row);
			if (body is null) continue;
			bytes += Length(body);
			hash = Mix(hash, body);
		}

		return new S4Verify(rows.Count, bytes, hash);
	}
}

sealed class S4BinKind : S4Kind<S4BinRow, byte[]>
{
	public static readonly S4BinKind Instance = new();
	static readonly Func<IDataRecord, S4BinRow> Select = static r => new S4BinRow { Id = r.GetInt32(0), Body = (byte[])r.GetValue(1) };
	public override Func<IDataRecord, S4BinRow> Selector => Select;
	public override void Set(S4BinRow row, int id, byte[] body) { row.Id = id; row.Body = body; }
	public override int IdOf(S4BinRow row) => row.Id;
	public override byte[]? BodyOf(S4BinRow row) => row.Body;
	public override byte[] ReadBody(DbDataReader reader) => reader.GetFieldValue<byte[]>(1);
	public override long Length(byte[] body) => body.Length;

	public override ulong Mix(ulong hash, byte[] body)
	{
		ReadOnlySpan<byte> all = body;
		int n = Math.Min(64, all.Length);
		hash = Fnv(hash, all[..n]);
		return Fnv(hash, all[^n..]);
	}

	public override byte[] Make(int row, int size, int salt)
	{
		var bytes = new byte[size];
		for (int j = 0; j < bytes.Length; j++) bytes[j] = unchecked((byte)(j * 31 + row * 7 + salt * 13 + (j >> 8)));
		return bytes;
	}
}

sealed class S4StrKind : S4Kind<S4StrRow, string>
{
	public static readonly S4StrKind Instance = new();
	static readonly Func<IDataRecord, S4StrRow> Select = static r => new S4StrRow { Id = r.GetInt32(0), Body = r.GetString(1) };
	public override Func<IDataRecord, S4StrRow> Selector => Select;
	public override void Set(S4StrRow row, int id, string body) { row.Id = id; row.Body = body; }
	public override int IdOf(S4StrRow row) => row.Id;
	public override string? BodyOf(S4StrRow row) => row.Body;
	public override string ReadBody(DbDataReader reader) => reader.GetString(1);
	public override long Length(string body) => body.Length * 2L;

	public override ulong Mix(ulong hash, string body)
	{
		ReadOnlySpan<char> all = body;
		int n = Math.Min(32, all.Length);
		hash = Fnv(hash, MemoryMarshal.AsBytes(all[..n]));
		return Fnv(hash, MemoryMarshal.AsBytes(all[^n..]));
	}

	public override string Make(int row, int size, int salt)
		=> string.Create(size, row + salt, static (span, seed) =>
		{
			for (int j = 0; j < span.Length; j++) span[j] = (char)('A' + (j * 7 + seed) % 26);
		});
}

/// <summary>The cell runner: one (scenario, path) in this process.</summary>
public static partial class S4Cells
{
	public static readonly string[] Paths = ["R1", "R2", "R3", "R4", "R5", "O1", "O2", "O3"];

	public static (S4Scenario Scenario, string Path) ParseCell(string text)
	{
		int colon = text.IndexOf(':', StringComparison.Ordinal);
		if (colon <= 0 || colon == text.Length - 1) throw new ArgumentException("--s4 takes <scenario>:<path>, for example V1:R2");
		var scenario = S4Scenario.Find(text[..colon]);
		string path = text[(colon + 1)..].ToUpperInvariant();
		if (!Paths.Contains(path)) throw new ArgumentException($"unknown path '{path}' ({string.Join(", ", Paths)})");
		return (scenario, path);
	}

	/// <summary>Warm-up read of a tiny row on the same path, then the measured read of the scenario's table.</summary>
	public static Task<S4Result> RunAsync(DbConnection connection, S4Scenario scenario, string path, string warmupSql, ushort commandTimeout = 30)
		=> scenario.Binary
			? RunKindAsync(S4BinKind.Instance, connection, scenario, path, warmupSql, commandTimeout)
			: RunKindAsync(S4StrKind.Instance, connection, scenario, path, warmupSql, commandTimeout);

	static async Task<S4Result> RunKindAsync<TRow, TBody>(S4Kind<TRow, TBody> kind, DbConnection connection, S4Scenario scenario, string path, string warmupSql, ushort commandTimeout)
		where TRow : class, new()
	{
		var stats = new In6Stats();
		var timed = new TimedConnection(connection, stats);
		var counters = new S4Counters { CommandTimeout = commandTimeout };
		await Execute(kind, path, connection, timed, stats, warmupSql, counters).ConfigureAwait(false);
		stats.Reset();
		counters.Reset();
		GC.Collect();
		long alloc0 = GC.GetTotalAllocatedBytes(true);
		long start = Stopwatch.GetTimestamp();
		var rows = await Execute(kind, path, connection, timed, stats, scenario.Sql, counters).ConfigureAwait(false);
		long total = Stopwatch.GetTimestamp() - start;
		long alloc = GC.GetTotalAllocatedBytes(true) - alloc0;
		var verify = kind.Summarize(rows);
		long peak = Process.GetCurrentProcess().PeakWorkingSet64;
		return new S4Result(counters, total, alloc, peak, verify);
	}

	static Task<List<TRow>> Execute<TRow, TBody>(S4Kind<TRow, TBody> kind, string path, DbConnection connection, TimedConnection timed, In6Stats stats, string sql, S4Counters counters)
		where TRow : class, new()
		=> path switch
		{
			"R1" => Raw(kind, connection, sql, CommandBehavior.Default, asyncRead: false, asyncGetter: false, counters),
			"R2" => Raw(kind, connection, sql, CommandBehavior.Default, asyncRead: true, asyncGetter: false, counters),
			"R3" => Raw(kind, connection, sql, CommandBehavior.SequentialAccess, asyncRead: false, asyncGetter: false, counters),
			"R4" => Raw(kind, connection, sql, CommandBehavior.SequentialAccess, asyncRead: true, asyncGetter: false, counters),
			"R5" => Raw(kind, connection, sql, CommandBehavior.SequentialAccess, asyncRead: true, asyncGetter: true, counters),
			"O1" => OdxSelector(kind, timed, stats, sql, counters),
			"O2" => OdxMapAsync<TRow>(timed, stats, sql, counters),
			"O3" => OdxMapSync<TRow>(timed, stats, sql, counters),
			_ => throw new ArgumentException("unknown path " + path),
		};

	static async Task<List<TRow>> Raw<TRow, TBody>(S4Kind<TRow, TBody> kind, DbConnection connection, string sql, CommandBehavior behavior, bool asyncRead, bool asyncGetter, S4Counters counters)
		where TRow : class, new()
	{
		using var command = connection.CreateCommand();
		command.CommandText = sql;
		command.CommandTimeout = counters.CommandTimeout;
		long openStart = Stopwatch.GetTimestamp();
		var reader = asyncRead
			? await command.ExecuteReaderAsync(behavior).ConfigureAwait(false)
			: command.ExecuteReader(behavior);
		var list = new List<TRow>();
		try
		{
			while (true)
			{
				bool more;
				if (asyncRead)
				{
					long start = Stopwatch.GetTimestamp();
					var task = reader.ReadAsync();
					bool incomplete = !task.IsCompleted;
					more = await task.ConfigureAwait(false);
					if (incomplete)
					{
						counters.AsyncReads++;
						counters.AsyncReadTicks += Stopwatch.GetTimestamp() - start;
					}
				}
				else
				{
					more = reader.Read();
				}

				if (!more) break;

				// Ordinal order: Id (always the sync getter), then the large column.
				int id = reader.GetInt32(0);
				TBody body;
				if (asyncGetter)
				{
					long start = Stopwatch.GetTimestamp();
					var task = reader.GetFieldValueAsync<TBody>(1);
					bool incomplete = !task.IsCompleted;
					body = await task.ConfigureAwait(false);
					if (incomplete)
					{
						counters.AsyncGetters++;
						counters.AsyncGetterTicks += Stopwatch.GetTimestamp() - start;
					}
				}
				else
				{
					body = kind.ReadBody(reader);
				}

				var row = new TRow();
				kind.Set(row, id, body);
				list.Add(row);
			}
		}
		finally
		{
			if (asyncRead) await reader.DisposeAsync().ConfigureAwait(false);
			else reader.Dispose();
		}

		counters.OpenTicks = Stopwatch.GetTimestamp() - openStart;
		return list;
	}

	static void CopyStats(In6Stats stats, S4Counters counters)
	{
		counters.Wrapper = true;
		counters.OpenTicks = stats.OpenTicks;
		counters.AsyncReads = stats.ReadAsyncIncomplete;
		counters.AsyncReadTicks = stats.WaitTicks;
		counters.AsyncGetters = stats.GetterAsyncIncomplete;
		counters.AsyncGetterTicks = stats.GetterWaitTicks;
	}

	static async Task<List<TRow>> OdxSelector<TRow, TBody>(S4Kind<TRow, TBody> kind, TimedConnection timed, In6Stats stats, string sql, S4Counters counters)
		where TRow : class, new()
	{
		var list = await timed.Command(sql).SetTimeout(counters.CommandTimeout).AsyncReads().Map(kind.Selector).ToListAsync().ConfigureAwait(false);
		CopyStats(stats, counters);
		return list;
	}

	static async Task<List<TRow>> OdxMapAsync<TRow>(TimedConnection timed, In6Stats stats, string sql, S4Counters counters)
		where TRow : class, new()
	{
		var list = await timed.Command(sql).SetTimeout(counters.CommandTimeout).AsyncReads().Map<TRow>().ToListAsync().ConfigureAwait(false);
		CopyStats(stats, counters);
		return list;
	}

	static Task<List<TRow>> OdxMapSync<TRow>(TimedConnection timed, In6Stats stats, string sql, S4Counters counters)
		where TRow : class, new()
	{
		var list = timed.Command(sql).SetTimeout(counters.CommandTimeout).Map<TRow>().ToList();
		CopyStats(stats, counters);
		return Task.FromResult(list);
	}

	// ---- the self-test: no server; SQLite in memory with the four shapes scaled down to KB -----------------------------------------------------

	[GeneratedRegex(@"^S4RESULT scenario=(V1|V2|N1|N2) path=(R[1-5]|O[1-3]) open_ms=\d+\.\d{3} total_ms=\d+\.\d{3} alloc_bytes=\d+ peak_ws_bytes=\d+ async_reads=\d+ async_wait_ms=\d+\.\d{3} async_getters=\d+ async_getter_wait_ms=\d+\.\d{3} rows=\d+ bytes=\d+ wrapper=(yes|no) cmd_timeout=[0-9]+ switches=(new|default)$", RegexOptions.CultureInvariant)]
	private static partial Regex ResultFormat();

	[GeneratedRegex(@"^S4VERIFY scenario=(V1|V2|N1|N2) path=(R[1-5]|O[1-3]) rows=\d+ bytes=\d+ first_last_checksum=[0-9a-f]{16}$", RegexOptions.CultureInvariant)]
	private static partial Regex VerifyFormat();

	static S4Verify Seed<TRow, TBody>(S4Kind<TRow, TBody> kind, SqliteConnection connection, S4Scenario scenario, int salt)
		where TRow : class, new()
	{
		var rows = new List<TRow>();
		for (int i = 1; i <= scenario.Rows; i++)
		{
			var row = new TRow();
			kind.Set(row, i, kind.Make(i, scenario.ValueSize, salt));
			rows.Add(row);
		}

		if (salt == 0)
		{
			using (var create = connection.CreateCommand())
			{
				create.CommandText = $"create table [{scenario.Id}] (Id integer primary key, Body {(scenario.Binary ? "blob" : "text")} not null)";
				create.ExecuteNonQuery();
			}

			using var transaction = connection.BeginTransaction();
			foreach (var row in rows)
			{
				using var insert = connection.CreateCommand();
				insert.Transaction = transaction;
				insert.CommandText = $"insert into [{scenario.Id}] (Id, Body) values ($id, $body)";
				insert.Parameters.AddWithValue("$id", kind.IdOf(row));
				insert.Parameters.AddWithValue("$body", kind.BodyOf(row)!);
				insert.ExecuteNonQuery();
			}

			transaction.Commit();
		}

		return kind.Summarize(rows);
	}

	public static int SelfTest()
	{
		int failures = 0;
		void Check(bool ok, string what)
		{
			if (ok) return;
			failures++;
			Console.WriteLine("FAIL " + what);
		}

		Check(ParseCell("v2:r5") is { Scenario.Id: "V2", Path: "R5" }, "ParseCell accepts v2:r5");
		foreach (string bad in new[] { "V1", "V1:", ":R1", "V9:R1", "V1:R9" })
		{
			bool threw = false;
			try { ParseCell(bad); }
			catch (ArgumentException) { threw = true; }
			Check(threw, $"ParseCell rejects '{bad}'");
		}

		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		foreach (var scenario in S4Scenario.Small)
		{
			var expected = scenario.Binary ? Seed(S4BinKind.Instance, connection, scenario, 0) : Seed(S4StrKind.Instance, connection, scenario, 0);
			var different = scenario.Binary ? Seed(S4BinKind.Instance, connection, scenario, 1) : Seed(S4StrKind.Instance, connection, scenario, 1);
			long perValue = scenario.Binary ? scenario.ValueSize : scenario.ValueSize * 2L;
			Check(expected.Rows == scenario.Rows && expected.Bytes == scenario.Rows * perValue, $"{scenario.Id}: expected rows/bytes {expected.Rows}/{expected.Bytes}");
			Check(expected.Check != different.Check, $"{scenario.Id}: the checksum tells different data apart");
			Console.WriteLine($"{scenario.Id}: expected {expected.Format()}");
			foreach (string path in Paths)
			{
				S4Result result;
				try
				{
					result = RunAsync(connection, scenario, path, scenario.WarmupSqlite).GetAwaiter().GetResult();
				}
				catch (Exception ex)
				{
					Check(false, $"{scenario.Id}:{path} threw {ex.GetType().Name}: {ex.Message}");
					continue;
				}

				string line = result.Line(scenario.Id, path), verifyLine = result.VerifyLine(scenario.Id, path);
				Check(ResultFormat().IsMatch(line), $"{scenario.Id}:{path} result line format: {line}");
				Check(VerifyFormat().IsMatch(verifyLine), $"{scenario.Id}:{path} verify line format: {verifyLine}");
				Check(result.Verify == expected, $"{scenario.Id}:{path} read {result.Verify.Format()}, expected {expected.Format()}");
				Check(result.Counters.Wrapper == path.StartsWith('O'), $"{scenario.Id}:{path} wrapper flag {result.Counters.Wrapper}");
				Check(result.AllocBytes >= expected.Bytes, $"{scenario.Id}:{path} allocated {result.AllocBytes} bytes, at least {expected.Bytes} expected");
				Check(result.PeakWorkingSet > 0, $"{scenario.Id}:{path} peak working set {result.PeakWorkingSet}");
				Check(path == "R5" || result.Counters.AsyncGetters == 0, $"{scenario.Id}:{path} counted async getters {result.Counters.AsyncGetters}");
				Check(path is "R2" or "R4" or "R5" or "O1" or "O2" || result.Counters.AsyncReads == 0, $"{scenario.Id}:{path} counted async reads {result.Counters.AsyncReads} on a sync path");
				Console.WriteLine("  " + line);
			}
		}

		// The command timeout reaches the command of every path (warm-up and measured read) and is printed in the result line.
		var spy = new S4SpyConnection(connection);
		foreach (string path in Paths)
		{
			spy.Commands.Clear();
			var result = RunAsync(spy, S4Scenario.Small[1], path, S4Scenario.Small[1].WarmupSqlite, 45).GetAwaiter().GetResult();
			Check(spy.Commands.Count == 2 && spy.Commands.All(static c => c.CommandTimeout == 45), $"{path}: {spy.Commands.Count} commands, timeouts {string.Join(",", spy.Commands.Select(static c => c.CommandTimeout))}, expected 2 x 45");
			Check(result.Line("V2", path).Contains(" cmd_timeout=45 ", StringComparison.Ordinal), $"{path}: result line carries cmd_timeout=45");
		}

		var defaults = RunAsync(spy, S4Scenario.Small[1], "O1", S4Scenario.Small[1].WarmupSqlite).GetAwaiter().GetResult();
		Check(defaults.Line("V2", "O1").Contains(" cmd_timeout=30 ", StringComparison.Ordinal), "the default command timeout is 30");

		// The SqlClient switches helper: `default` leaves both unset (nothing in this process has set them), `new` sets both to false and reads both back.
		// Last in the test: an AppContext switch cannot be unset again.
		S4Switches.Apply("default");
		Check(S4Switches.Line() == "S4SWITCHES UseCompatibilityAsyncBehaviour=unset UseCompatibilityProcessSni=unset", "default leaves both switches unset: " + S4Switches.Line());
		Check(S4Switches.Mode == "default", "mode default");
		S4Switches.Apply("new");
		Check(S4Switches.Line() == "S4SWITCHES UseCompatibilityAsyncBehaviour=false UseCompatibilityProcessSni=false", "new sets both switches to false: " + S4Switches.Line());
		Check(S4Switches.Mode == "new", "mode new");
		Check(RunAsync(connection, S4Scenario.Small[0], "R2", S4Scenario.Small[0].WarmupSqlite).GetAwaiter().GetResult().Line("V1", "R2").EndsWith(" switches=new", StringComparison.Ordinal), "the result line carries switches=new");
		bool rejected = false;
		try { S4Switches.Apply("old"); }
		catch (ArgumentException) { rejected = true; }
		Check(rejected, "an unknown switches mode is rejected");

		Console.WriteLine(failures == 0 ? "S4 self-test: OK (8 paths x 4 scenarios agree on rows, bytes and checksum; line formats match)" : $"S4 self-test: {failures} FAILED");
		return failures == 0 ? 0 : 1;
	}
}

/// <summary>A pass-through connection that remembers the commands it created (the self-test reads their CommandTimeout).</summary>
sealed class S4SpyConnection(DbConnection inner) : DbConnection
{
	public List<DbCommand> Commands { get; } = [];

	[System.Diagnostics.CodeAnalysis.AllowNull]
	public override string ConnectionString { get => inner.ConnectionString; set => inner.ConnectionString = value; }
	public override string Database => inner.Database;
	public override string DataSource => inner.DataSource;
	public override string ServerVersion => inner.ServerVersion;
	public override ConnectionState State => inner.State;
	public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
	public override void Close() => inner.Close();
	public override void Open() => inner.Open();
	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

	protected override DbCommand CreateDbCommand()
	{
		var command = inner.CreateCommand();
		Commands.Add(command);
		return command;
	}

	// The test owns the inner connection.
	protected override void Dispose(bool disposing) { }
}

/// <summary>
/// SqlClient's opt-in AppContext switches (Microsoft.Data.SqlClient 7.1.1 has both): `new` sets UseCompatibilityAsyncBehaviour and UseCompatibilityProcessSni to false.
/// SqlClient reads them once on first use, so Apply must run before any SqlClient type is touched.
/// </summary>
public static class S4Switches
{
	const string AsyncBehaviour = "Switch.Microsoft.Data.SqlClient.UseCompatibilityAsyncBehaviour", ProcessSni = "Switch.Microsoft.Data.SqlClient.UseCompatibilityProcessSni";

	/// <summary>The mode last applied: "default" (nothing set) or "new".</summary>
	public static string Mode { get; private set; } = "default";

	public static void Apply(string mode)
	{
		switch (mode)
		{
			case "default":
				break;
			case "new":
				AppContext.SetSwitch(AsyncBehaviour, false);
				AppContext.SetSwitch(ProcessSni, false);
				break;
			default:
				throw new ArgumentException("--s4-sqlclient-switches takes new or default");
		}

		Mode = mode;
	}

	static string Read(string name) => AppContext.TryGetSwitch(name, out bool value) ? (value ? "true" : "false") : "unset";

	/// <summary>The real state of both switches, read back with AppContext.TryGetSwitch.</summary>
	public static string Line() => $"S4SWITCHES UseCompatibilityAsyncBehaviour={Read(AsyncBehaviour)} UseCompatibilityProcessSni={Read(ProcessSni)}";
}
