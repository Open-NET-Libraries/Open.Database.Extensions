using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Globalization;
using System.Runtime;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Reports;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

// The three checks of the synthetic provider, run from the command line. None of them is a benchmark.
//   --verify                 equal final objects from every variant, the provider's switches, every read path, a timing printout
//   --selftest-open-time     a delay inside an open reader grows reader-open time; the same delay after the close does not
//                            and the ReaderOpen/Drain summary covers the WorkloadActual iterations only (BS-RO)
//   --selftest-pace          consumer pace grows reader-open for the inline variants (V3, V3s, V4a, V4b, V6c, Dapper twins) and not for V5 and V6d
//   --selftest-alloc         the typed-getter loop allocates 0 B per row
public static partial class SyntheticChecks
{
	static readonly int[] VerifyWidths = [1, 3, 5, 10, 20];
	static readonly int[] VerifyRows = [1, 5, 100];

	sealed class Report
	{
		public int Checks, Failures;

		public void Check(bool ok, string what)
		{
			Checks++;
			if (ok) return;
			Failures++;
			Console.WriteLine("FAIL " + what);
		}

		public void Fail(string what) => Check(false, what);
	}

	static T Wait<T>(ValueTask<T> task) => task.AsTask().GetAwaiter().GetResult();

	// ---- --verify ----------------------------------------------------------------
	public static int Verify()
	{
		var report = new Report();
		Console.WriteLine("== equal final objects: every variant against V1 (and V1 against the table); V3-V7 against the domain oracle ==");
		VerifyReaderIsSynthetic(report);
		VerifyF0ColumnNames(report);
		VerifyUnsealedReader(report);
		foreach (int width in VerifyWidths)
		{
			foreach (int rows in VerifyRows) VerifyVariants(report, width, rows);
		}

		Console.WriteLine("== V10: the resolver follows RowMap's column match ==");
		VerifyV10Names(report);

		Console.WriteLine("== scalar variants V8 and V7 scalars: each against the table value, its own reference ==");
		VerifyScalars(report);

		Console.WriteLine("== the benchmark methods through the benchmark class's own Setup ==");
		VerifyBenchmarkPath(report);

		Console.WriteLine("== BS-C: object sizes, V9a buffers, the DateTimeOffset scenario, the one-off overloads ==");
		VerifyObjectSizes(report);
		VerifyBufferBytes(report);
		VerifyShapeScenario(report);
		VerifyOverloads(report);
		VerifyShapeBenchmarkPath(report);

		Console.WriteLine("== provider switches ==");
		VerifySwitches(report);

		Console.WriteLine("== read paths ==");
		VerifyReadPaths(report);

		Console.WriteLine("== indicative timings (one process, 50 runs each, NOT a benchmark) ==");
		PrintTimings();

		Console.WriteLine(report.Failures == 0
			? $"VERIFY PASS ({report.Checks} checks)"
			: $"VERIFY FAIL ({report.Failures} of {report.Checks} checks failed)");
		return report.Failures == 0 ? 0 : 1;
	}

	static void VerifyVariants(Report report, int width, int rows)
	{
		var table = SyntheticTable.Create(width, rows);
		var conn = new SyntheticConnection(table, new SyntheticOptions());
		var hand = SyntheticVariants.Hand(width);
		string where = $"width={width} rows={rows}";

		var v1 = SyntheticVariants.V1(conn);
		var expected = new List<SynRow>(rows);
		for (int i = 0; i < rows; i++) expected.Add(SynRow.FromTable(table, i));
		Compare(report, "V1 sync vs table", where, expected, v1);

		KeyValuePair<string, Func<List<SynRow>>>[] variants =
		[
			new("V0b sync", () => SyntheticVariants.V0b(conn)),
			new("V0b async", () => Wait(SyntheticVariants.V0bAsync(conn))),
			new("V1 async", () => Wait(SyntheticVariants.V1Async(conn))),
			new("V1 async ReadAsync", () => Wait(SyntheticVariants.V1AsyncRead(conn))),
			new("V2 sync", () => SyntheticVariants.V2(conn, hand)),
			new("V2 async", () => Wait(SyntheticVariants.V2Async(conn, hand))),
			new("V7a Dapper.AOT V1 sync", () => SyntheticDapperAot.V1(conn)),
		];
		foreach (var variant in variants)
			Compare(report, variant.Key, where, v1, variant.Value());

		// V7a (Dapper.AOT) against V2's rows too: the same typed row from a hand-written read.
		Compare(report, "V7a Dapper.AOT V1 sync vs V2", where, SyntheticVariants.V2(conn, hand), SyntheticDapperAot.V1(conn));

		// V10 (generator-shaped; GeneratorShaped.cs): inside ODX's loop (V10), the raw loop on DbDataReader (V10r) and on the sealed reader (V10c), each against V2's rows (and V1's, the table's).
		VerifyV10(report, conn, table, width, rows, v1, hand, where);

		// Gen and GenD (the prototype source generator; SyntheticGenerated.cs) against V2's rows.
		VerifyGenerated(report, conn, width, SyntheticVariants.V2(conn, hand), where);

		// V0 produces object[] rows: its final objects are compared as GetValues returns them.
		foreach (var kv in new KeyValuePair<string, List<object[]>>[] { new("V0 sync", SyntheticVariants.V0(conn)), new("V0 async", Wait(SyntheticVariants.V0Async(conn))) })
		{
			report.Check(kv.Value.Count == v1.Count, $"{kv.Key} {where}: {kv.Value.Count} rows, V1 has {v1.Count}");
			for (int i = 0; i < Math.Min(v1.Count, kv.Value.Count); i++)
			{
				var want = v1[i].Boxed(width);
				var got = kv.Value[i];
				bool same = got.Length == want.Length;
				for (int c = 0; same && c < want.Length; c++) same = SynRow.Same(want[c], got[c]);
				if (!same)
				{
					report.Fail($"{kv.Key} {where} row {i}: object[] differs from V1's row ([{string.Join(", ", want)}] vs [{string.Join(", ", got)}])");
					break;
				}
			}
		}

		VerifyVariantsB(report, conn, table, width, rows, v1, where);
		VerifyVariantsC(report, conn, table, width, rows, v1, where);

		Console.WriteLine($"  ok  {where}: V0, V0b, V1, V2, V3-V7 (sync and async) equal ({rows} rows)");
	}

	// V10, V10r, V10c: the same rows as V2 (and V1 and the table) at every width and row count; the ordinals are resolved once per query (a counter on the resolver, which
	// runs once per query and not per row); the ordinals of the catalog table ascend (SequentialAccess needs it).
	static void VerifyV10(Report report, SyntheticConnection conn, SyntheticTable table, int width, int rows, List<SynRow> v1, Func<System.Data.IDataRecord, SynRow> hand, string where)
	{
		var v2 = SyntheticVariants.V2(conn, hand);
		var fromTable = new List<SynRow>(rows);
		for (int i = 0; i < rows; i++) fromTable.Add(SynRow.FromTable(table, i));

		KeyValuePair<string, Func<List<SynRow>>>[] variants =
		[
			new("V10 (inside ODX's loop)", () => GeneratorShaped.V10(conn, GeneratorShaped.Sql)),
			new("V10r (raw loop, DbDataReader)", () => GeneratorShaped.V10r(conn, GeneratorShaped.Sql)),
			new("V10c (raw loop, SyntheticReader)", () => GeneratorShaped.V10c(conn, GeneratorShaped.Sql)),
			new("V10i (V10, row method AggressiveInlining)", () => GeneratorShaped.V10i(conn, GeneratorShaped.Sql)),
			new("V10L (whole loop generated)", () => GeneratorShaped.V10L(conn, GeneratorShaped.Sql)),
			new("V10ri (raw loop, AggressiveInlining row method)", () => GeneratorShaped.V10ri(conn, GeneratorShaped.Sql)),
			new("V10rn (raw loop, NoInlining row method)", () => GeneratorShaped.V10rn(conn, GeneratorShaped.Sql)),
			new("V10k (required members, NoInlining row method)", () => GeneratorShaped.V10k(conn, GeneratorShaped.Sql, width)),
			new("V10kri (required members, AggressiveInlining row method)", () => GeneratorShaped.V10kri(conn, GeneratorShaped.Sql, width)),
			new("V10kL (required members, whole loop generated)", () => GeneratorShaped.V10kL(conn, GeneratorShaped.Sql, width)),
		];
		foreach (var variant in variants)
		{
			long before = SynRowGenerated.Resolves;
			var result = variant.Value();
			Compare(report, variant.Key + " vs V2", where, v2, result);
			Compare(report, variant.Key + " vs V1", where, v1, result);
			Compare(report, variant.Key + " vs table", where, fromTable, result);
			report.Check(SynRowGenerated.Resolves - before == (rows == 0 ? 0 : 1), $"{variant.Key} {where}: the ordinals were resolved {SynRowGenerated.Resolves - before} times for one query, expected once per result set");
		}

		using var probe = new SyntheticReaderProbe(conn);
		report.Check(SynRowGenerated.Resolve(probe.Reader).Ascend(), $"V10 {where}: the resolved ordinals do not ascend");
	}

	sealed class SyntheticReaderProbe : IDisposable
	{
		readonly System.Data.Common.DbCommand _command;

		public SyntheticReaderProbe(SyntheticConnection conn)
		{
			_command = conn.CreateCommand();
			_command.CommandText = SyntheticVariants.Sql;
			Reader = _command.ExecuteReader();
		}

		public System.Data.Common.DbDataReader Reader { get; }

		public void Dispose()
		{
			Reader.Dispose();
			_command.Dispose();
		}
	}

	// V10's resolver follows RowMap: columns named in another case, or with underscores, feed the same properties as Map<T>() gives (V1 is the oracle), in tiers.
	static void VerifyV10Names(Report report)
	{
		var plain = SyntheticTable.Create(20, 50);
		string[] names = [.. plain.Names];
		names[1] = names[1].ToUpperInvariant();        // Created -> CREATED   (tier 2)
		names[2] = names[2].ToLowerInvariant();        // Title   -> title     (tier 2)
		names[5] = "is_active";                        // IsActive            (tier 3)
		names[12] = "AMOUNT_";                         // Amount              (tier 3)
		names[19] = "s_u_m_m_a_r_y";                   // Summary             (tier 3)
		var renamed = new SyntheticTable { Width = plain.Width, Rows = plain.Rows, Names = names, Types = plain.Types, Columns = plain.Columns, Nulls = plain.Nulls };
		var conn = new SyntheticConnection(renamed, new SyntheticOptions());
		var v1 = SyntheticVariants.V1(conn);
		var oracle = new List<SynRow>(renamed.Rows);
		for (int i = 0; i < renamed.Rows; i++) oracle.Add(SynRow.FromTable(renamed, i));
		Compare(report, "V1 on renamed columns vs table", "renamed", oracle, v1);
		Compare(report, "V10 on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10(conn, GeneratorShaped.Sql));
		Compare(report, "V10r on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10r(conn, GeneratorShaped.Sql));
		Compare(report, "V10c on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10c(conn, GeneratorShaped.Sql));
		Compare(report, "V10i on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10i(conn, GeneratorShaped.Sql));
		Compare(report, "V10L on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10L(conn, GeneratorShaped.Sql));
		Compare(report, "V10ri on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10ri(conn, GeneratorShaped.Sql));
		Compare(report, "V10rn on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10rn(conn, GeneratorShaped.Sql));
		Compare(report, "V10k on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10k(conn, GeneratorShaped.Sql, 20));
		Compare(report, "V10kri on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10kri(conn, GeneratorShaped.Sql, 20));
		Compare(report, "V10kL on renamed columns vs V1", "renamed", v1, GeneratorShaped.V10kL(conn, GeneratorShaped.Sql, 20));
		// Required members: a column missing from the result set throws at resolve time (width 20 against a width 5 table: IsActive, the first member with no column).
		var narrow = new SyntheticConnection(SyntheticTable.Create(5, 3), new SyntheticOptions());
		foreach (var form in new KeyValuePair<string, Func<List<SynRow>>>[] { new("V10k", () => GeneratorShaped.V10k(narrow, GeneratorShaped.Sql, 20)), new("V10kri", () => GeneratorShaped.V10kri(narrow, GeneratorShaped.Sql, 20)), new("V10kL", () => GeneratorShaped.V10kL(narrow, GeneratorShaped.Sql, 20)) })
		{
			string thrown = "(nothing thrown)";
			try { _ = form.Value(); }
			catch (Exception ex) { thrown = string.Join(" <- ", WalkChain(ex).Select(static e => e.GetType().Name + ": " + e.Message)); }
			report.Check(thrown.Contains("IsActive", StringComparison.Ordinal) && thrown.Contains("InvalidOperationException", StringComparison.Ordinal), $"{form.Key} with a missing required column: expected an InvalidOperationException naming IsActive, got {thrown}");
		}

		Console.WriteLine("  ok  V10 forms (V10, V10r, V10c, V10i, V10ri, V10rn, V10L, V10k, V10kri, V10kL) on columns named in another case and with underscores (tiers 2 and 3 of RowMap's match) equal V1, and a missing required column throws at resolve time");
	}

	// The benchmark methods themselves, through the benchmark class's own Setup (so a field Setup forgets to assign, or a variant wired to the wrong helper,
	// fails here and not in a run): every [Benchmark] method is invoked by reflection, its result compared with the oracle, and the number of NextResult
	// calls it made is checked (3 extra result sets: every variant, hand-written, library and Dapper, makes the same 4 calls, so a NextResult cost is
	// charged to all alike; the scalar variants read a one-set table and are not part of this).
	static void VerifyBenchmarkPath(Report report)
	{
		int invoked = 0;
		foreach (int width in VerifyWidths)
		{
			foreach (int rows in VerifyRows)
			{
				var table = SyntheticTable.Create(width, rows);
				var v1 = SyntheticVariants.V1(new SyntheticConnection(table, new SyntheticOptions()));
				var plain = new List<SynItem>(rows);
				var massaged = new List<SynItem>(rows);
				for (int i = 0; i < rows; i++)
				{
					plain.Add(SynItem.From(SynRow.FromTable(table, i)));
					massaged.Add(SynItem.Massage(SynItem.From(SynRow.FromTable(table, i)), width));
				}

				var bench = new SyntheticBenchmarks { Width = width, Rows = rows, RowCost = 0, ExtraSets = 3, NextResultCost = 0, Pace = 0 };
				bench.Setup();
				foreach (var method in typeof(SyntheticBenchmarks).GetMethods().Where(static m => m.GetCustomAttributes(typeof(BenchmarkDotNet.Attributes.BenchmarkAttribute), false).Length > 0))
				{
					// The control and the decomposition of Gen read the 20 catalog columns at fixed ordinals: width 20 only.
					if (width != 20 && WidthTwentyOnly(method.Name)) continue;

					bench.Connection.Options.Recorder.Reset();
					object? result;
					try { result = Await(method.Invoke(bench, null)); }
					catch (Exception ex)
					{
						report.Fail($"benchmark {method.Name} width={width} rows={rows} threw {(ex.InnerException ?? ex).GetType().Name}: {(ex.InnerException ?? ex).Message}");
						continue;
					}

					invoked++;
					string where = $"width={width} rows={rows}";
					string name = "benchmark " + method.Name;
					switch (result)
					{
						case List<SynRow> typed:
							Compare(report, name, where, v1, typed);
							break;
						case List<SynItem> domain:
							CompareItems(report, name, where, method.Name.Contains("V6", StringComparison.Ordinal) || method.Name is "V9a_On_Sync" or "V9b_Sync" or "V9d_M_Sync" ? massaged : plain, domain);
							break;
						case List<object[]> boxed:
							report.Check(boxed.Count == rows, $"{name} {where}: {boxed.Count} rows, expected {rows}");
							break;
						case System.Collections.IList generated when method.Name.StartsWith("Gen", StringComparison.Ordinal) || method.Name == "Ctl_Sync":
							CompareGenerated(report, name, where, v1, generated);
							break;
						case System.Collections.IList shaped:
							CompareItems(report, name, where, massaged, Items(shaped));
							break;
						default:
							report.Fail($"{name} {where}: unexpected result {result?.GetType().Name ?? "null"}");
							break;
					}

					var rec = bench.Connection.Options.Recorder;
					report.Check(rec.NextResultCalls == 4, $"{name} {where}: {rec.NextResultCalls} NextResult calls, expected 4 (every variant makes the same drain)");
				}

				bench.Cleanup();
			}
		}

		var scalars = new SyntheticScalarBenchmarks();
		scalars.Setup();
		int expected = ((int[])SyntheticTable.Create(1, 1).Columns[0])[0];
		foreach (var method in typeof(SyntheticScalarBenchmarks).GetMethods().Where(static m => m.GetCustomAttributes(typeof(BenchmarkDotNet.Attributes.BenchmarkAttribute), false).Length > 0))
		{
			invoked++;
			var result = Await(method.Invoke(scalars, null));
			report.Check(result is int value && value == expected, $"benchmark {method.Name} (scalar, own reference): got {result}, expected {expected}");
		}

		scalars.Cleanup();
		Console.WriteLine($"  ok  {invoked} benchmark-method invocations through Setup (output equal to the oracle; NextResult drain: 4 calls for every variant)");
	}

	static object? Await(object? result)
	{
		if (result is null) return null;
		var type = result.GetType();
		if (result is Task task)
		{
			task.GetAwaiter().GetResult();
			return type.GetProperty("Result")?.GetValue(result);
		}

		if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTask<>))
			return Await(type.GetMethod("AsTask")!.Invoke(result, null));
		return result;
	}

	// V3 to V6 produce SynItem: compared with the oracle built from the table (SynItem.From of the table's row; V6 also massaged).
	// Dapper's rows (SynRow) compare with V1's, its items with the V3/V6 oracles. The pace changes timing only, so a pace of 25 is run too.
	static void VerifyVariantsB(Report report, SyntheticConnection conn, SyntheticTable table, int width, int rows, List<SynRow> v1, string where)
	{
		var plain = new List<SynItem>(rows);
		var massaged = new List<SynItem>(rows);
		for (int i = 0; i < rows; i++)
		{
			plain.Add(SynItem.From(SynRow.FromTable(table, i)));
			massaged.Add(SynItem.Massage(SynItem.From(SynRow.FromTable(table, i)), width));
		}

		var handM = SyntheticVariantsB.HandMassaged(width);
		KeyValuePair<string, Func<List<SynItem>>>[] items =
		[
			new("V3 sync", () => SyntheticVariantsB.V3(conn, 0)),
			new("V3 sync pace=25", () => SyntheticVariantsB.V3(conn, 25)),
			new("V3 async", () => Wait(SyntheticVariantsB.V3Async(conn, 0))),
			new("V3s async", () => Wait(SyntheticVariantsB.V3s(conn, 0))),
			new("V3s async pace=25", () => Wait(SyntheticVariantsB.V3s(conn, 25))),
			new("V4a sync", () => SyntheticVariantsB.V4a(conn, width, 0)),
			new("V4a sync pace=25", () => SyntheticVariantsB.V4a(conn, width, 25)),
			new("V4a async", () => Wait(SyntheticVariantsB.V4aAsync(conn, width, 0))),
			new("V4b sync", () => SyntheticVariantsB.V4b(conn, width, 0)),
			new("V4b sync pace=25", () => SyntheticVariantsB.V4b(conn, width, 25)),
			new("V4b async", () => Wait(SyntheticVariantsB.V4bAsync(conn, width, 0))),
			new("V5 sync", () => SyntheticVariantsB.V5(conn)),
			new("V5 async", () => Wait(SyntheticVariantsB.V5Async(conn))),
			new("V7 Dapper V3 sync (unbuffered)", () => SyntheticDapper.V3(conn, 0)),
			new("V7 Dapper V3s async (unbuffered)", () => SyntheticDapper.V3s(conn, 0).GetAwaiter().GetResult()),
			new("V7 Dapper V5 sync", () => SyntheticDapper.V5(conn)),
			new("V7 Dapper V5 async", () => SyntheticDapper.V5Async(conn).GetAwaiter().GetResult()),
		];
		foreach (var variant in items)
			CompareItems(report, variant.Key, where, plain, variant.Value());

		KeyValuePair<string, Func<List<SynItem>>>[] massagedItems =
		[
			new("V6a sync", () => SyntheticVariantsB.V6a(conn, handM)),
			new("V6a async", () => Wait(SyntheticVariantsB.V6aAsync(conn, handM))),
			new("V6c sync", () => SyntheticVariantsB.V6c(conn, width, 0)),
			new("V6c sync pace=25", () => SyntheticVariantsB.V6c(conn, width, 25)),
			new("V6c async", () => Wait(SyntheticVariantsB.V6cAsync(conn, width, 0))),
			new("V6d sync", () => SyntheticVariantsB.V6d(conn, width)),
			new("V6d async", () => Wait(SyntheticVariantsB.V6dAsync(conn, width))),
			new("V7 Dapper V6c sync", () => SyntheticDapper.V6c(conn, width, 0)),
			new("V7 Dapper V6d sync", () => SyntheticDapper.V6d(conn, width)),
			new("V7 Dapper V6d async", () => SyntheticDapper.V6dAsync(conn, width).GetAwaiter().GetResult()),
		];
		foreach (var variant in massagedItems)
			CompareItems(report, variant.Key, where, massaged, variant.Value());

		// Dapper's typed rows against the library's V1 rows.
		Compare(report, "V7 Dapper V1 sync vs V1", where, v1, SyntheticDapper.V1(conn));
		Compare(report, "V7 Dapper V1 async vs V1", where, v1, SyntheticDapper.V1Async(conn).GetAwaiter().GetResult());

		// The massage must change something at every width (R1 at least, on rows 10, 20, ...), or the V6 check is empty.
		if (rows >= 100) report.Check(massaged.Zip(plain).Any(static p => SynItem.Diff(p.First, p.Second).Count > 0), $"the V6 massage changed no row {where}");
	}

	static void CompareItems(Report report, string name, string where, List<SynItem> expected, List<SynItem> actual)
	{
		if (actual.Count != expected.Count)
		{
			report.Fail($"{name} {where}: {actual.Count} rows, expected {expected.Count}");
			return;
		}

		for (int i = 0; i < expected.Count; i++)
		{
			var diff = SynItem.Diff(expected[i], actual[i]);
			if (diff.Count == 0) continue;
			report.Fail($"{name} {where} row {i} differs: {string.Join("; ", diff)}");
			return;
		}

		report.Check(true, name);
	}

	// The scalar variants have no row objects: each is compared with the value the table holds (Id of row 0), its own reference.
	// Dapper's scalar variants are compared the same way (they do not compare with the library's objects).
	static void VerifyScalars(Report report)
	{
		var table = SyntheticTable.Create(1, 1);
		var conn = new SyntheticConnection(table, new SyntheticOptions());
		int want = ((int[])table.Columns[0])[0];
		KeyValuePair<string, Func<int>>[] scalars =
		[
			new("V8a ExecuteScalar<int>", () => SyntheticVariantsB.V8a(conn)),
			new("V8a async", () => Wait(SyntheticVariantsB.V8aAsync(conn))),
			new("V8b object path", () => SyntheticVariantsB.V8b(conn)),
			new("V8b async", () => Wait(SyntheticVariantsB.V8bAsync(conn))),
			new("V8c Map<int>().First()", () => SyntheticVariantsB.V8c(conn)),
			new("V8c async", () => Wait(SyntheticVariantsB.V8cAsync(conn))),
			new("V7 Dapper ExecuteScalar<int>", () => SyntheticDapper.V8a(conn)),
			new("V7 Dapper ExecuteScalar<int> async", () => SyntheticDapper.V8aAsync(conn).GetAwaiter().GetResult()),
			new("V7 Dapper QueryFirst<int>", () => SyntheticDapper.V8b(conn)),
			new("V7 Dapper QueryFirst<int> async", () => SyntheticDapper.V8bAsync(conn).GetAwaiter().GetResult()),
		];
		foreach (var s in scalars)
		{
			int got = s.Value();
			report.Check(got == want, $"{s.Key} (scalar, own reference = the table's Id of row 0): got {got}, expected {want}");
		}

		Console.WriteLine($"  ok  {scalars.Length} scalar variants return the table value {want} (each against its own reference, not against the row variants)");
	}

	static void Compare(Report report, string name, string where, List<SynRow> expected, List<SynRow> actual)
	{
		if (actual.Count != expected.Count)
		{
			report.Fail($"{name} {where}: {actual.Count} rows, expected {expected.Count}");
			return;
		}

		for (int i = 0; i < expected.Count; i++)
		{
			var diff = SynRow.Diff(expected[i], actual[i]);
			if (diff.Count == 0) continue;
			report.Fail($"{name} {where} row {i} differs: {string.Join("; ", diff)}");
			return; // The first differing row is enough.
		}

		report.Check(true, name);
	}

	// ---- the switches ----------------------------------------------------------------
	sealed record DrainResult(int Moves, SyntheticRecorder Recorder, double DrainUs);

	static void ReadAllRows(DbDataReader reader)
	{
		while (reader.Read()) { }
	}

	static DbDataReader Open(SyntheticConnection conn, CommandBehavior behavior = CommandBehavior.Default)
		=> conn.CreateCommand().ExecuteReader(behavior);

	static void VerifySwitches(Report report)
	{
		const int Rows = 200, Heavy = 20_000;

		// Per-row cost: off pays nothing and records nothing; on pays once per row, and costs real time.
		double Median(SyntheticOptions options)
		{
			var times = new double[5];
			for (int t = 0; t < times.Length; t++)
			{
				var conn = new SyntheticConnection(SyntheticTable.Create(3, Rows), options);
				long start = Stopwatch.GetTimestamp();
				using (var r = Open(conn)) ReadAllRows(r);
				times[t] = Stopwatch.GetTimestamp() - start;
			}

			Array.Sort(times);
			return times[2] * 1_000_000d / Stopwatch.Frequency;
		}

		var offRec = new SyntheticRecorder();
		var offOptions = new SyntheticOptions { Recorder = offRec };
		double off = Median(offOptions);
		report.Check(offRec.RowCostCharges == 0, $"row cost off: {offRec.RowCostCharges} rows paid it, expected 0");
		report.Check(offRec.Rows == 5 * Rows, $"row cost off: {offRec.Rows} rows read, expected {5 * Rows}");
		var onRec = new SyntheticRecorder();
		double on = Median(new SyntheticOptions { RowCostIterations = Heavy, Recorder = onRec });
		report.Check(onRec.RowCostCharges == 5 * Rows, $"row cost on: {onRec.RowCostCharges} rows paid it, expected {5 * Rows}");
		report.Check(on > off * 3, $"row cost on is not clearly slower than off ({on:F0}us against {off:F0}us for {Rows} rows)");
		Console.WriteLine($"  row cost: off {off:F0}us, on ({Heavy} iterations per row) {on:F0}us for {Rows} rows; charges off={offRec.RowCostCharges} on={onRec.RowCostCharges}");

		// NextResult: k extra empty sets are served, the last call returns false, the cost is paid per call and only when on.
		DrainResult Drain(int extra, int cost, CommandBehavior behavior)
		{
			var rec = new SyntheticRecorder();
			var conn = new SyntheticConnection(SyntheticTable.Create(3, 10), new SyntheticOptions { ExtraResultSets = extra, NextResultCostIterations = cost, Recorder = rec });
			int moves = 0;
			using (var r = Open(conn, behavior))
			{
				ReadAllRows(r);
				while (r.NextResult())
				{
					moves++;
					report.Check(!r.Read(), "an extra result set returned a row");
				}
			}

			return new DrainResult(moves, rec, rec.Summary().MedianDrainUs);
		}

		var none = Drain(0, 0, CommandBehavior.Default);
		report.Check(none.Moves == 0 && none.Recorder.NextResultCalls == 1, $"k=0: moved {none.Moves} times in {none.Recorder.NextResultCalls} call(s), expected 0 in 1");
		var cheap = Drain(3, 0, CommandBehavior.Default);
		report.Check(cheap.Moves == 3 && cheap.Recorder.NextResultCalls == 4 && cheap.Recorder.ExtraSetsServed == 3, $"k=3: moved {cheap.Moves} times in {cheap.Recorder.NextResultCalls} call(s), expected 3 in 4");
		report.Check(cheap.Recorder.NextResultCostCharges == 0, $"next-result cost off: {cheap.Recorder.NextResultCostCharges} calls paid it, expected 0");
		var costly = Drain(3, Heavy, CommandBehavior.Default);
		report.Check(costly.Recorder.NextResultCostCharges == 4, $"next-result cost on: {costly.Recorder.NextResultCostCharges} calls paid it, expected 4");
		report.Check(costly.DrainUs > cheap.DrainUs * 3, $"next-result cost on is not clearly slower in the drain ({costly.DrainUs:F1}us against {cheap.DrainUs:F1}us)");
		var single = Drain(3, Heavy, CommandBehavior.SingleResult);
		report.Check(single.Moves == 0 && single.Recorder.ExtraSetsServed == 0, $"SingleResult with k=3: moved {single.Moves} times, expected 0");
		Console.WriteLine($"  next-result: k=0 {none.Moves} moves; k=3 off {cheap.Moves} moves, drain {cheap.DrainUs:F1}us; k=3 on ({Heavy} per call) {costly.Moves} moves, drain {costly.DrainUs:F1}us; SingleResult {single.Moves} moves");

		// Through the library: a mapped read drains by default, and .SingleResult() does not. The recorder shows it.
		var libRec = new SyntheticRecorder();
		var libConn = new SyntheticConnection(SyntheticTable.Create(3, 10), new SyntheticOptions { ExtraResultSets = 3, NextResultCostIterations = Heavy, Recorder = libRec });
		SyntheticVariants.V1(libConn);
		report.Check(libRec.NextResultCalls == 4 && libRec.NextResultCostCharges == 4, $"Map<T>().ToList(), k=3: {libRec.NextResultCalls} NextResult calls, expected 4");
		var drainUs = libRec.Summary();
		report.Check(drainUs.MedianDrainUs > 0 && drainUs.MedianOpenUs >= drainUs.MedianDrainUs, $"drain is not inside reader-open time ({drainUs})");
		libRec.Reset();
		libConn.Command(SyntheticVariants.Sql).SingleResult().Map<SynRow>().ToList();
		report.Check(libRec.NextResultCalls == 0, $"SingleResult().Map<T>().ToList(), k=3: {libRec.NextResultCalls} NextResult calls, expected 0");
		Console.WriteLine($"  library drain: Map<T>().ToList() makes {4} NextResult calls (k=3), .SingleResult() makes {libRec.NextResultCalls}; {drainUs}");
	}

	// ---- every read path ----------------------------------------------------------------
	static void VerifyReadPaths(Report report)
	{
		var table = SyntheticTable.Create(20, 40);
		var conn = new SyntheticConnection(table, new SyntheticOptions { ExtraResultSets = 2 });

		// Default access: the typed getter, GetValue, GetValues, GetFieldValue<T> and GetFieldValueAsync<T> agree, and agree with the table.
		using (var r = Open(conn))
		{
			var all = new object[20];
			int row = 0;
			while (r.ReadAsync().GetAwaiter().GetResult())
			{
				r.GetValues(all);
				for (int c = 0; c < 20; c++)
				{
					object? expected = ExpectedValue(table, row, c);
					bool isNull = expected is null;
					report.Check(r.IsDBNull(c) == isNull && r.IsDBNullAsync(c).GetAwaiter().GetResult() == isNull, $"IsDBNull/IsDBNullAsync, row {row} column {c}");
					bool nullable = table.IsNullable(c);
					report.Check(SynRow.Same(expected, Normalize(all[c])), $"GetValues, row {row} column {c}");
					report.Check(SynRow.Same(expected, Normalize(r.GetValue(c))), $"GetValue, row {row} column {c}");
					if (!isNull) report.Check(SynRow.Same(expected, Typed(r, c)), $"typed getter, row {row} column {c}");
					report.Check(SynRow.Same(expected, FieldValue(r, c, nullable, async: false)), $"GetFieldValue<T>, row {row} column {c}");
					report.Check(SynRow.Same(expected, FieldValue(r, c, nullable, async: true)), $"GetFieldValueAsync<T>, row {row} column {c}");
				}

				row++;
			}

			report.Check(row == 40, $"ReadAsync returned {row} rows, expected 40");
			int sets = 0;
			while (r.NextResultAsync().GetAwaiter().GetResult()) sets++;
			report.Check(sets == 2, $"NextResultAsync moved {sets} times, expected 2");
		}

		// Sequential access: in order works; going back throws, as a real provider does; the default does not.
		using (var r = Open(conn, CommandBehavior.SequentialAccess))
		{
			r.Read();
			_ = r.GetInt32(0);
			_ = r.GetString(2);
			_ = r.GetString(2);
			bool threw = false;
			try { _ = r.GetInt32(0); }
			catch (InvalidOperationException) { threw = true; }
			report.Check(threw, "SequentialAccess: reading a column after a later one did not throw");
			r.Read();
			_ = r.GetInt32(0); // A new row starts again at column 0.
		}

		using (var r = Open(conn))
		{
			r.Read();
			_ = r.GetString(2);
			_ = r.GetInt32(0); // Default access has no order.
		}

		// GetBytes: the length, then the content in two parts.
		using (var r = Open(conn))
		{
			r.Read();
			long length = r.GetBytes(9, 0, null, 0, 0);
			var buffer = new byte[length];
			long n = r.GetBytes(9, 0, buffer, 0, 8) + r.GetBytes(9, 8, buffer, 8, 100);
			report.Check(length == 16 && n == 16 && buffer.AsSpan().SequenceEqual(((byte[][])table.Columns[9])[0]), "GetBytes did not return the stored bytes");
		}

		Console.WriteLine("  ok  typed getters, GetValue, GetValues, GetFieldValue<T>, GetFieldValueAsync<T>, IsDBNull(Async), ReadAsync, NextResultAsync, GetBytes, SequentialAccess order");
	}

	// The value the table holds: null for NULL. Compared against what each path returns once DBNull is read as null.
	static object? ExpectedValue(SyntheticTable t, int row, int c)
		=> t.Nulls[c] is { } n && n[row] ? null : t.Columns[c].GetValue(row);

	static object? Normalize(object? o) => o is DBNull ? null : o;

	static object? Typed(DbDataReader r, int c)
	{
		var type = r.GetFieldType(c);
		if (type == typeof(int)) return r.GetInt32(c);
		if (type == typeof(DateTime)) return r.GetDateTime(c);
		if (type == typeof(string)) return r.GetString(c);
		if (type == typeof(decimal)) return r.GetDecimal(c);
		if (type == typeof(bool)) return r.GetBoolean(c);
		if (type == typeof(Guid)) return r.GetGuid(c);
		if (type == typeof(long)) return r.GetInt64(c);
		if (type == typeof(double)) return r.GetDouble(c);
		if (type == typeof(byte[]))
		{
			var buffer = new byte[r.GetBytes(c, 0, null, 0, 0)];
			r.GetBytes(c, 0, buffer, 0, buffer.Length);
			return buffer;
		}

		throw new NotSupportedException(type.Name);
	}

	static object? FV<T>(DbDataReader r, int c, bool async)
		=> async ? r.GetFieldValueAsync<T>(c).GetAwaiter().GetResult() : r.GetFieldValue<T>(c);

	static object? FieldValue(DbDataReader r, int c, bool nullable, bool async)
	{
		var type = r.GetFieldType(c);
		if (type == typeof(int)) return nullable ? FV<int?>(r, c, async) : FV<int>(r, c, async);
		if (type == typeof(DateTime)) return nullable ? FV<DateTime?>(r, c, async) : FV<DateTime>(r, c, async);
		if (type == typeof(string)) return FV<string>(r, c, async);
		if (type == typeof(decimal)) return nullable ? FV<decimal?>(r, c, async) : FV<decimal>(r, c, async);
		if (type == typeof(bool)) return nullable ? FV<bool?>(r, c, async) : FV<bool>(r, c, async);
		if (type == typeof(Guid)) return nullable ? FV<Guid?>(r, c, async) : FV<Guid>(r, c, async);
		if (type == typeof(long)) return nullable ? FV<long?>(r, c, async) : FV<long>(r, c, async);
		if (type == typeof(double)) return nullable ? FV<double?>(r, c, async) : FV<double>(r, c, async);
		if (type == typeof(byte[])) return FV<byte[]>(r, c, async);
		throw new NotSupportedException(type.Name);
	}

	// ---- the printout: reader-open, drain and total, one process, indicative only ----------------------------------------------------------------
	static void PrintTimings()
	{
		Console.WriteLine("  (reader-open includes the drain; total is a stopwatch around the whole variant call (mean of 50 runs; the others are medians); every variant makes the same drain (--verify checks the call counts); widths 5 and 20, 100 rows, 3 extra result sets at 2000 iterations each, no row cost)");
		foreach (int width in new[] { 5, 20 })
		{
			var table = SyntheticTable.Create(width, 100);
			var hand = SyntheticVariants.Hand(width);
			KeyValuePair<string, Func<SyntheticConnection, object>>[] variants =
			[
				new("V0  ", static c => SyntheticVariants.V0(c)),
				new("V0b ", static c => SyntheticVariants.V0b(c)),
				new("V1  ", static c => SyntheticVariants.V1(c)),
				new("V2  ", c => SyntheticVariants.V2(c, hand)),
			];
			foreach (var variant in variants)
			{
				var rec = new SyntheticRecorder();
				var conn = new SyntheticConnection(table, new SyntheticOptions { ExtraResultSets = 3, NextResultCostIterations = 2000, Recorder = rec });
				for (int i = 0; i < 20; i++) variant.Value(conn); // warm up
				rec.Reset();
				long start = Stopwatch.GetTimestamp();
				for (int i = 0; i < 50; i++) variant.Value(conn);
				double totalUs = (Stopwatch.GetTimestamp() - start) * 1_000_000d / Stopwatch.Frequency / 50;
				var s = rec.Summary();
				Console.WriteLine($"  width {width,2} {variant.Key}: reader-open {s.MedianOpenUs,8:F1}us  drain {s.MedianDrainUs,6:F1}us  total {totalUs,8:F1}us");
			}
		}
	}

	// ---- --selftest-open-time ----------------------------------------------------------------
	public static int SelfTestOpenTime()
	{
		const int Rows = 100, Reps = 9;
		const double DelayUs = 100;
		const double expectedAdd = Rows * DelayUs;
		var table = SyntheticTable.Create(3, Rows);
		var report = new Report();

		double Run(Action<SyntheticConnection> work)
		{
			var rec = new SyntheticRecorder();
			var conn = new SyntheticConnection(table, new SyntheticOptions { Recorder = rec });
			work(conn); // warm up, then discard
			rec.Reset();
			for (int i = 0; i < Reps; i++) work(conn);
			return rec.Summary().MedianOpenUs;
		}

		// Raw ADO.NET against the provider.
		double rawBase = Run(static c => { using var r = Open(c); ReadAllRows(r); });
		double rawInside = Run(static c => { using var r = Open(c); while (r.Read()) Spin.Microseconds(DelayUs); });
		double rawAfter = Run(static c =>
		{
			using (var r = Open(c)) ReadAllRows(r);
			for (int i = 0; i < Rows; i++) Spin.Microseconds(DelayUs);
		});

		// Through the library: Stream holds the reader open while the consumer works; ToList() closes it first.
		double libBase = Run(static c => SyntheticVariants.V1(c));
		double libInside = Run(static c =>
		{
			IAsyncEnumerator<SynRow> rows = c.Command(SyntheticVariants.Sql).Map<SynRow>().Stream(Timeout.InfiniteTimeSpan).GetAsyncEnumerator();
			try
			{
				while (rows.MoveNextAsync().AsTask().GetAwaiter().GetResult()) Spin.Microseconds(DelayUs);
			}
			finally
			{
				rows.DisposeAsync().AsTask().GetAwaiter().GetResult();
			}
		});
		double libAfter = Run(static c =>
		{
			foreach (var _ in SyntheticVariants.V1(c)) Spin.Microseconds(DelayUs);
		});

		Console.WriteLine($"{Rows} rows, delay {DelayUs}us per row = {expectedAdd:F0}us expected when the delay is inside; median reader-open over {Reps} runs, microseconds:");
		Console.WriteLine($"  raw ADO.NET   no delay {rawBase,9:F1}   delay inside {rawInside,9:F1}   delay after close {rawAfter,9:F1}");
		Console.WriteLine($"  Map<T> via ODE no delay {libBase,8:F1}   delay inside {libInside,9:F1}   delay after close {libAfter,9:F1}");

		void Judge(string name, double baseline, double inside, double after)
		{
			double grew = inside - baseline, moved = after - baseline;
			Console.WriteLine($"  {name}: inside grew by {grew:F0}us ({grew / expectedAdd:P0} of expected), after the close changed by {moved:F0}us ({moved / expectedAdd:P0})");
			report.Check(grew is >= 0.8 * expectedAdd and <= 1.5 * expectedAdd + 1000, $"{name}: a delay inside the open reader grew reader-open time by {grew:F0}us, expected about {expectedAdd:F0}us");
			report.Check(Math.Abs(moved) <= 0.1 * expectedAdd, $"{name}: a delay after the close changed reader-open time by {moved:F0}us, expected about 0");
		}

		Judge("raw", rawBase, rawInside, rawAfter);
		Judge("library", libBase, libInside, libAfter);
		CheckV9aWindow(report, DelayUs, Run, Judge);

		// The drain is reported on its own: NextResult cost shows in the drain and inside reader-open time, and nowhere else.
		var rec = new SyntheticRecorder();
		var conn = new SyntheticConnection(table, new SyntheticOptions { ExtraResultSets = 3, NextResultCostIterations = 20_000, Recorder = rec });
		for (int i = 0; i < 3; i++) SyntheticVariants.V1(conn);
		rec.Reset();
		for (int i = 0; i < Reps; i++) SyntheticVariants.V1(conn);
		var s = rec.Summary();
		Console.WriteLine($"  drain: k=3 extra sets, 20000 iterations per NextResult call, via Map<T>().ToList(): {s}");
		report.Check(s.MedianDrainUs > 0 && s.MedianOpenUs >= s.MedianDrainUs, "the drain is not reported inside reader-open time");
		CheckMeasuredIterations(report, table);

		Console.WriteLine(report.Failures == 0 ? $"SELFTEST OPEN TIME PASS ({report.Checks} checks)" : $"SELFTEST OPEN TIME FAIL ({report.Failures} of {report.Checks})");
		return report.Failures == 0 ? 0 : 1;
	}

	// ---- the ReaderOpen/Drain summary covers the WorkloadActual iterations only (BS-RO) ----------------------------------------------------------------
	// No benchmark runs here. The listener is fed with BenchmarkDotNet's real engine events, written through reflection (the engine writes them itself
	// in a run), and BenchmarkDotNet's measurements are built by hand with the shape of the stage-1a log (12 iterations, the last an outlier).
	static void CheckMeasuredIterations(Report report, SyntheticTable table)
	{
		var sourceType = typeof(EngineEventSource);
		object log = sourceType.GetField("Log", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;
		var start = sourceType.GetMethod("IterationStart", BindingFlags.NonPublic | BindingFlags.Instance)!;
		var stop = sourceType.GetMethod("IterationStop", BindingFlags.NonPublic | BindingFlags.Instance)!;

		var rec = new SyntheticRecorder();
		var conn = new SyntheticConnection(table, new SyntheticOptions { Recorder = rec });
		SyntheticIterationListener.Attach(rec);

		void Iteration(IterationMode mode, IterationStage stage, long operations, int readers, double holdUs)
		{
			start.Invoke(log, [mode, stage, operations]);
			for (int i = 0; i < readers; i++)
			{
				using var r = Open(conn);
				ReadAllRows(r);
				Spin.Microseconds(holdUs);
			}

			stop.Invoke(log, [mode, stage, operations]);
		}

		// Every stage BenchmarkDotNet has, with a long hold (5 ms) in all but the WorkloadActual iterations; a reader outside any iteration too.
		const double Hold = 500;
		Iteration(IterationMode.Workload, IterationStage.Jitting, 1, 1, 5000);
		Iteration(IterationMode.Workload, IterationStage.Pilot, 4, 4, 5000);
		Iteration(IterationMode.Overhead, IterationStage.Actual, 4, 0, 0);
		Iteration(IterationMode.Workload, IterationStage.Warmup, 4, 4, 5000);
		for (int i = 0; i < 3; i++) Iteration(IterationMode.Workload, IterationStage.Actual, 4, 4, Hold);
		using (var r = Open(conn)) { ReadAllRows(r); Spin.Microseconds(5000); } // the extra GC pass after the last iteration, or a stray reader
		var seen = rec.MeasuredIterations;
		report.Check(seen.Length == 3 && seen.All(static i => i.Operations == 4), $"the listener recorded {seen.Length} iterations ({string.Join(",", seen.Select(static i => i.Operations))}), expected 3 of 4 operations (WorkloadActual only)");
		var all = MeasuredOpenTime.MeanOfAll(seen);
		report.Check(all.OpenUs >= Hold && all.OpenUs <= Hold + 300 && all.DrainUs == 0, $"reader-open per operation was {all.OpenUs:F1}us, expected {Hold}us plus the read of {table.Rows} rows; drain {all.DrainUs:F1}us, expected 0 (warm-up, pilot and stray readers of 5000us must not count)");
		Console.WriteLine($"  iterations: 3 WorkloadActual of 4 operations recorded out of 7 iterations and a stray reader; reader-open per operation {all.OpenUs:F1}us (hold {Hold}us per reader)");

		// What the recorder sums in an iteration is at most what the iteration took (ReaderOpen can never exceed Mean), and most of it for this loop.
		rec.Reset();
		long t0 = Stopwatch.GetTimestamp();
		start.Invoke(log, [IterationMode.Workload, IterationStage.Actual, 300L]);
		for (int i = 0; i < 300; i++) SyntheticVariants.V1(conn);
		stop.Invoke(log, [IterationMode.Workload, IterationStage.Actual, 300L]);
		double wallUs = (Stopwatch.GetTimestamp() - t0) * 1_000_000d / Stopwatch.Frequency / 300;
		double openUs = MeasuredOpenTime.MeanOfAll(rec.MeasuredIterations).OpenUs;
		Console.WriteLine($"  ReaderOpen {openUs:F2}us per operation against {wallUs:F2}us for the whole operation (V1, {table.Rows} rows)");
		report.Check(openUs <= wallUs * 1.05 && openUs >= wallUs * 0.5, $"reader-open per operation {openUs:F2}us against {wallUs:F2}us for the operation: it must be at most 105% and, for this loop, over 50%");
		SyntheticIterationListener.Detach(rec);

		// Matching to BenchmarkDotNet's report: 12 actual iterations, the 6th and the last removed as outliers, every result is its actual less the same overhead.
		double[] perOpUs = [47.0367, 51.4732, 49.1089, 47.7901, 47.1352, 52.0846, 50.0557, 47.1315, 47.8332, 47.2171, 47.4400, 61.8285];
		const long Operations = 16384;
		const double OverheadNs = 34200;
		var actual = new List<Measurement>();
		var recorded = new List<MeasuredIteration>();
		for (int i = 0; i < perOpUs.Length; i++)
		{
			actual.Add(new Measurement(1, IterationMode.Workload, IterationStage.Actual, i + 1, Operations, perOpUs[i] * 1000 * Operations));
			recorded.Add(new MeasuredIteration(Operations, (long)(perOpUs[i] * 0.95 * Operations * Stopwatch.Frequency / 1_000_000d), 0));
		}

		var removed = new HashSet<int> { 5, 11 };
		var results = new List<Measurement>();
		foreach (int i in Enumerable.Range(0, perOpUs.Length).Where(i => !removed.Contains(i)))
			results.Add(new Measurement(1, IterationMode.Workload, IterationStage.Result, results.Count + 1, Operations, (perOpUs[i] * 1000 * Operations) - OverheadNs));

		double expected = 0.95 * Enumerable.Range(0, perOpUs.Length).Where(i => !removed.Contains(i)).Average(i => perOpUs[i]);
		double everything = 0.95 * perOpUs.Average();
		bool ok = MeasuredOpenTime.TrySummarize(recorded, actual, results, out var summary, out string reason);
		report.Check(ok && summary.Iterations == 10 && Math.Abs(summary.OpenUs - expected) < 0.01, $"matching the 10 results to the 12 iterations: ok={ok} ({reason}), mean {summary.OpenUs:F3}us over {summary.Iterations}, expected {expected:F3}us");
		report.Check(Math.Abs(everything - expected) > 0.5, "the test data do not tell the removed outliers apart (the all-iterations mean equals the survivors' mean)");
		Console.WriteLine($"  matching: 12 iterations, 2 removed (6th and 12th), survivors' mean {summary.OpenUs:F3}us (expected {expected:F3}us), all 12 would give {everything:F3}us");

		report.Check(!MeasuredOpenTime.TrySummarize(recorded.Take(11).ToList(), actual, results, out _, out _), "a recording of 11 iterations was accepted for 12 reported");
		var wrongOps = recorded.ToList();
		wrongOps[3] = wrongOps[3] with { Operations = 8192 };
		report.Check(!MeasuredOpenTime.TrySummarize(wrongOps, actual, results, out _, out _), "an iteration with other operations was accepted");
		var shifted = results.ToList();
		shifted[4] = new Measurement(1, IterationMode.Workload, IterationStage.Result, 5, Operations, shifted[4].Nanoseconds - (3 * Operations));
		report.Check(!MeasuredOpenTime.TrySummarize(recorded, actual, shifted, out _, out _), "results that differ from their iterations by different amounts were accepted");
		var every = actual.Select(static a => new Measurement(1, IterationMode.Workload, IterationStage.Result, 1, a.Operations, a.Nanoseconds - OverheadNs)).ToList();
		report.Check(MeasuredOpenTime.TrySummarize(recorded, actual, every, out var none, out _) && none.Iterations == 12, "no outlier removed: all 12 iterations were not used");
	}

	// ---- --selftest-pace ----------------------------------------------------------------
	// Consumer pace is a busy-wait per item, paid by the variants that run caller code while the reader is open (V3, V4a, V4b, V6c, V3s and the
	// Dapper unbuffered twins). Reader-open must grow with it for those, and must not for V5 and V6d (their work is after the close; the pace is ignored).
	// A control shows the same cost paid by the consumer after the close: total time grows, reader-open does not.
	public static int SelfTestPace()
	{
		const int Rows = 100, Reps = 9, Width = 3;
		var table = SyntheticTable.Create(Width, Rows);
		var hand = SyntheticVariants.Hand(Width);
		var handM = SyntheticVariantsB.HandMassaged(Width);
		var report = new Report();

		// Calibrate: how long one Spin iteration takes here, so the pace is about 50 us per item (informational; the expectation is measured from it).
		Spin.Iterations(200_000);
		long t0 = Stopwatch.GetTimestamp();
		for (int i = 0; i < 20; i++) Spin.Iterations(100_000);
		double nsPerIteration = (Stopwatch.GetTimestamp() - t0) * 1_000_000_000d / Stopwatch.Frequency / (20d * 100_000);
		int pace = Math.Max(1, (int)(50_000 / nsPerIteration));
		Console.WriteLine($"{Rows} rows, width {Width}; one Spin iteration is about {nsPerIteration:F1} ns here, so pace = {pace} iterations per item (about 50 us), {Rows * 50} us expected per reader when the pace is paid while the reader is open; median over {Reps} runs, microseconds");

		(double Open, double Total) Run(Func<SyntheticConnection, object?> work)
		{
			var rec = new SyntheticRecorder();
			var conn = new SyntheticConnection(table, new SyntheticOptions { Recorder = rec });
			work(conn);
			rec.Reset();
			long start = Stopwatch.GetTimestamp();
			for (int i = 0; i < Reps; i++) work(conn);
			double total = (Stopwatch.GetTimestamp() - start) * 1_000_000d / Stopwatch.Frequency / Reps;
			return (rec.Summary().MedianOpenUs, total);
		}

		double expected = Rows * pace * nsPerIteration / 1000d;

		void Judge(string name, Func<int, Func<SyntheticConnection, object?>> make, bool paysWhileOpen)
		{
			var off = Run(make(0));
			var on = Run(make(pace));
			double grew = on.Open - off.Open;
			Console.WriteLine($"  {name,-28} reader-open {off.Open,9:F1} -> {on.Open,9:F1} ({(grew >= 0 ? "+" : "")}{grew:F0}us)   total {off.Total,9:F1} -> {on.Total,9:F1}   {(paysWhileOpen ? "inline: must grow" : "after close: must not grow")}");
			if (paysWhileOpen)
				report.Check(grew >= 0.8 * expected && grew <= 1.5 * expected + 1000, $"{name}: reader-open grew by {grew:F0}us with the pace, expected about {expected:F0}us");
			else
				report.Check(Math.Abs(grew) <= 0.1 * expected, $"{name}: reader-open changed by {grew:F0}us with the pace, expected about 0");
		}

		Judge("V3 sync", p => c => SyntheticVariantsB.V3(c, p), true);
		Judge("V3 async", p => c => Wait(SyntheticVariantsB.V3Async(c, p)), true);
		Judge("V3s async (streaming)", p => c => Wait(SyntheticVariantsB.V3s(c, p)), true);
		Judge("V4a sync", p => c => SyntheticVariantsB.V4a(c, Width, p), true);
		Judge("V4b sync", p => c => SyntheticVariantsB.V4b(c, Width, p), true);
		Judge("V6c sync", p => c => SyntheticVariantsB.V6c(c, Width, p), true);
		Judge("V7 Dapper V3 (unbuffered)", p => c => SyntheticDapper.V3(c, p), true);
		Judge("V7 Dapper V6c (unbuffered)", p => c => SyntheticDapper.V6c(c, Width, p), true);
		// V5 and V6d take no pace: the work is after the close. The harness hands them none, so the pair below is the same call twice (a stability check).
		Judge("V5 sync (no pace taken)", static _ => c => SyntheticVariantsB.V5(c), false);
		Judge("V6d sync (no pace taken)", static _ => c => SyntheticVariantsB.V6d(c, Width), false);
		// Controls: the consumer pays the pace itself AFTER the close, once per item. Total grows, reader-open does not.
		Judge("V5 + pace after close", p => c =>
		{
			var items = SyntheticVariantsB.V5(c);
			if (p > 0) foreach (var _ in items) Spin.Iterations(p);
			return items;
		}, false);
		Judge("V6d + pace after close", p => c =>
		{
			var items = SyntheticVariantsB.V6d(c, Width);
			if (p > 0) foreach (var _ in items) Spin.Iterations(p);
			return items;
		}, false);

		_ = hand;
		_ = handM;
		Console.WriteLine(report.Failures == 0 ? $"SELFTEST PACE PASS ({report.Checks} checks)" : $"SELFTEST PACE FAIL ({report.Failures} of {report.Checks})");
		return report.Failures == 0 ? 0 : 1;
	}

	// ---- --selftest-alloc ----------------------------------------------------------------
	const int AllocRows = 10_000, AllocWindows = 100;

	// Tolerance: 0.001 B per row, which is 10 B per million rows. The expected figure is exactly 0.
	const double AllocToleranceBytesPerRow = 0.001;
	[System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0052:Remove unread private members", Justification = "Write-only on purpose: accumulating each result keeps the JIT from discarding the measured work.")]
	static long _sink;

	public static int SelfTestAlloc()
	{
		var table = SyntheticTable.Create(20, AllocRows);
		var conn = new SyntheticConnection(table, new SyntheticOptions());
		var kinds = new int[20];
		var nullable = new bool[20];
		for (int c = 0; c < 20; c++)
		{
			var t = table.Types[c];
			kinds[c] = t == typeof(int) ? 0 : t == typeof(DateTime) ? 1 : t == typeof(string) ? 2 : t == typeof(decimal) ? 3 : t == typeof(bool) ? 4
				: t == typeof(Guid) ? 5 : t == typeof(long) ? 6 : t == typeof(double) ? 7 : 8;
			nullable[c] = table.IsNullable(c);
		}

		var buffer = new byte[16];
		var values = new object[20];
		var report = new Report();
		Console.WriteLine($"width 20, {AllocRows} rows per window, {AllocWindows} windows per attempt; tolerance {AllocToleranceBytesPerRow} B per row for the gated rows; SequentialAccess, in column order");

		AllocPass(report, conn, "typed getters + Read()", gate: true, r => TypedPass(r, kinds, nullable, buffer, async: false));
		AllocPass(report, conn, "typed getters + ReadAsync()", gate: true, r => TypedPass(r, kinds, nullable, buffer, async: true));
		AllocPass(report, conn, "GetFieldValue<T> (typed, nullable forms too)", gate: true, r => FieldValuePass(r, kinds, nullable));
			AllocPass(report, conn, "V10 generated-shape loop (ordinals once, typed getters, SynRow built per row; the row object itself is subtracted)", gate: true, r => V10Pass(r, SynRowGenerated.Read), floor: SynRowBytes());
			AllocPass(report, conn, "V10L whole generated loop (ordinals once, mapping inline in the loop; the row object itself is subtracted, the List growth is not in the loop of the window)", gate: false, r => V10LPass(r), floor: SynRowBytes());
			AllocPass(report, conn, "V10rn generated-shape loop, NoInlining row method (the row object itself is subtracted)", gate: true, r => V10Pass(r, SynRowGenerated.ReadNoInline), floor: SynRowBytes());
			AllocPass(report, conn, "V10ri generated-shape loop, AggressiveInlining row method (the row object itself is subtracted)", gate: true, r => V10Pass(r, SynRowGenerated.ReadInline), floor: SynRowBytes());
		AllocPass(report, conn, "GetValue (boxes value types, inherent)", gate: false, r => GetValuePass(r));
		AllocPass(report, conn, "GetValues into a reused object[20] (boxes, inherent)", gate: false, r => GetValuesPass(r, values));
		AllocPass(report, conn, "GetFieldValueAsync<int>(0) (one Task, inherent)", gate: false, r => AsyncPass(r));

		ShapeAllocInfo(report);

		Console.WriteLine(report.Failures == 0 ? $"SELFTEST ALLOC PASS ({report.Checks} checks)" : $"SELFTEST ALLOC FAIL ({report.Failures} of {report.Checks})");
		return report.Failures == 0 ? 0 : 1;
	}

	// Warm-up makes the tier-up happen first (many calls over more than a second, with pauses so the background tier-1 compile and its 100 ms call-counting delay both run).
	// Then each attempt is measured with JitInfo.GetCompiledMethodCount around it: an attempt in which the JIT compiled anything is thrown away and repeated,
	// so the figure comes from a window in which no method changed tier.
	static void AllocPass(Report report, SyntheticConnection conn, string name, bool gate, Func<DbDataReader, long> body, double floor = 0)
	{
		var warm = Stopwatch.StartNew();
		int passes = 0;
		while (warm.ElapsedMilliseconds < 1200 || passes < 60)
		{
			using var r = Open(conn, CommandBehavior.SequentialAccess);
			_sink += body(r);
			if (++passes % 15 == 0) Thread.Sleep(150);
		}

		for (int attempt = 1; attempt <= 8; attempt++)
		{
			long allocated = 0, jitBefore = JitInfo.GetCompiledMethodCount(currentThread: false);
			for (int w = 0; w < AllocWindows; w++)
			{
				using var r = Open(conn, CommandBehavior.SequentialAccess);
				long before = GC.GetAllocatedBytesForCurrentThread();
				_sink += body(r);
				allocated += GC.GetAllocatedBytesForCurrentThread() - before;
			}

			long jit = JitInfo.GetCompiledMethodCount(currentThread: false) - jitBefore;
			if (jit != 0)
			{
				Thread.Sleep(300); // The JIT was still moving methods up a tier; let it finish and measure again.
				continue;
			}

			double perRow = allocated / (double)((long)AllocRows * AllocWindows) - floor;
			bool ok = !gate || perRow <= AllocToleranceBytesPerRow;
			Console.WriteLine($"  {(gate ? (ok ? "PASS" : "FAIL") : "info"),-4} {name}: {perRow:F3} B per row ({allocated} B over {(long)AllocRows * AllocWindows} rows), no JIT activity in the window (attempt {attempt})");
			if (gate) report.Check(ok, $"{name}: {perRow:F3} B per row, tolerance {AllocToleranceBytesPerRow}");
			return;
		}

		report.Fail($"{name}: the JIT was still compiling in every window (8 attempts); no valid measurement");
	}

	// V10: the generated-shape loop (ordinals once per result set, typed getters, a SynRow built per row). Its floor is the SynRow object itself: the bytes one new SynRow() allocates.
	delegate SynRow RowMethod(DbDataReader r, in SynRowOrdinals o);

	static long V10Pass(DbDataReader r, RowMethod read)
	{
		long sum = 0;
		var ordinals = SynRowGenerated.Resolve(r);
		while (r.Read()) sum += read(r, in ordinals).Id;
		return sum;
	}

	static long V10LPass(DbDataReader r)
	{
		var list = SynRowGenerated.ReadAll(r);
		return list.Count;
	}

	static double SynRowBytes()
	{
		const int n = 100_000;
		var keep = new SynRow[n];
		for (int i = 0; i < n; i++) keep[i] = new SynRow(); // warm up, so nothing is compiled inside the window
		long before = GC.GetAllocatedBytesForCurrentThread();
		for (int i = 0; i < n; i++) keep[i] = new SynRow();
		double bytes = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)n;
		_sink += keep[n - 1].Id;
		return bytes;
	}

	static long TypedPass(DbDataReader r, int[] kinds, bool[] nullable, byte[] buffer, bool async)
	{
		long sum = 0;
		while (async ? r.ReadAsync().Result : r.Read())
		{
			for (int c = 0; c < kinds.Length; c++)
			{
				if (nullable[c] && r.IsDBNull(c)) continue;
				switch (kinds[c])
				{
					case 0: sum += r.GetInt32(c); break;
					case 1: sum += r.GetDateTime(c).Ticks; break;
					case 2: sum += r.GetString(c).Length; break;
					case 3: sum += (long)r.GetDecimal(c); break;
					case 4: if (r.GetBoolean(c)) sum++; break;
					case 5: sum += r.GetGuid(c).GetHashCode(); break;
					case 6: sum += r.GetInt64(c); break;
					case 7: sum += (long)r.GetDouble(c); break;
					default: sum += r.GetBytes(c, 0, buffer, 0, buffer.Length); break;
				}
			}
		}

		return sum;
	}

	static long FieldValuePass(DbDataReader r, int[] kinds, bool[] nullable)
	{
		long sum = 0;
		while (r.Read())
		{
			for (int c = 0; c < kinds.Length; c++)
			{
				bool n = nullable[c];
				switch (kinds[c])
				{
					case 0: sum += n ? r.GetFieldValue<int?>(c) ?? 0 : r.GetFieldValue<int>(c); break;
					case 1: sum += n ? r.GetFieldValue<DateTime?>(c)?.Ticks ?? 0 : r.GetFieldValue<DateTime>(c).Ticks; break;
					case 2: sum += r.GetFieldValue<string>(c)?.Length ?? 0; break;
					case 3: sum += (long)(n ? r.GetFieldValue<decimal?>(c) ?? 0 : r.GetFieldValue<decimal>(c)); break;
					case 4: if (n ? r.GetFieldValue<bool?>(c) == true : r.GetFieldValue<bool>(c)) sum++; break;
					case 5: sum += (n ? r.GetFieldValue<Guid?>(c) ?? default : r.GetFieldValue<Guid>(c)).GetHashCode(); break;
					case 6: sum += n ? r.GetFieldValue<long?>(c) ?? 0 : r.GetFieldValue<long>(c); break;
					case 7: sum += (long)(n ? r.GetFieldValue<double?>(c) ?? 0 : r.GetFieldValue<double>(c)); break;
					default: sum += r.GetFieldValue<byte[]>(c).Length; break;
				}
			}
		}

		return sum;
	}

	static long GetValuePass(DbDataReader r)
	{
		long sum = 0;
		while (r.Read())
		{
			for (int c = 0; c < r.FieldCount; c++)
			{
				if (r.GetValue(c) is DBNull) sum++;
			}
		}

		return sum;
	}

	static long GetValuesPass(DbDataReader r, object[] values)
	{
		long sum = 0;
		while (r.Read()) sum += r.GetValues(values);
		return sum;
	}

	static long AsyncPass(DbDataReader r)
	{
		long sum = 0;
		while (r.Read()) sum += r.GetFieldValueAsync<int>(0).Result;
		return sum;
	}
}
