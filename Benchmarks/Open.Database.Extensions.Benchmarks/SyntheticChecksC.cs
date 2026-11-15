using System.Collections;
using System.Data;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Benchmarks;

// The checks of BS-C (the DS1a candidate variants, the DateTimeOffset scenario, the one-off overload class). None of them is a benchmark.
public static partial class SyntheticChecks
{
	// The delegate calls V6b-E makes per row at each catalog width: one per computed member (R1; R1, R2; R1 to R3; R1 to R4; R1 to R4).
	static int DelegateCallsPerRow(int width) => width switch { 1 => 1, 3 => 2, 5 => 3, _ => 4 };

	// Hand-computed, from the catalog: bytes of one row slot of every V9a buffer at width 20 (element sizes 4 8 8 16 4 1 16 8 8 8 8 8 16 8 8 8 1 16 8 8 = 170,
	// plus one bitmap byte for each of the five nullable value columns Qty, Amount, Stamp, Flag, Link) = 175. ArrayPool rents 16 slots at first and doubles.
	const long BufferBytesPerSlotAtWidth20 = 175;

	static long PowerOfTwoSlots(int rows)
	{
		long slots = 16;
		while (slots < rows) slots *= 2;
		return slots;
	}

	static List<SynItem> Items(IEnumerable shaped)
	{
		var list = new List<SynItem>();
		foreach (ShapedBase s in shaped) list.Add(s.ToItem());
		return list;
	}

	// The variants of BS-C against V1, the table and the massaged oracle, at one width and row count.
	static void VerifyVariantsC(Report report, SyntheticConnection conn, SyntheticTable table, int width, int rows, List<SynRow> v1, string where)
	{
		var hands = Hands.For(width);
		var fHands = FHands.For(width);
		var connU = new UnsealedSyntheticConnection(table, conn.Options);
		var fromTable = new List<SynRow>(rows);
		var massaged = new List<SynItem>(rows);
		for (int i = 0; i < rows; i++)
		{
			fromTable.Add(SynRow.FromTable(table, i));
			massaged.Add(SynItem.Massage(SynItem.From(SynRow.FromTable(table, i)), width));
		}

		PoolTally.Reset();
		KeyValuePair<string, Func<List<SynRow>>>[] typed =
		[
			new("V1-fresh", () => SyntheticVariantsC.V1Fresh(conn)),
			new("V9c-Db", () => SyntheticVariantsC.V9cDb(conn, hands)),
			new("V9c-Syn", () => SyntheticVariantsC.V9cSyn(conn, hands)),
			new("F0", () => SyntheticVariantsF.F0(conn, fHands)),
			new("F1", () => SyntheticVariantsF.F1(conn, fHands)),
			new("F2", () => SyntheticVariantsF.F2(conn, fHands)),
			new("F3", () => SyntheticVariantsF.F3(conn, fHands)),
			new("F3r", () => SyntheticVariantsF.F3r(conn, fHands)),
			new("F1f", () => SyntheticVariantsF.F1f(conn, fHands)),
			new("F2u", () => SyntheticVariantsF.F2u(connU, fHands)),
			new("F4", () => SyntheticVariantsF.F4(conn, fHands)),
			new("V9d", () => SyntheticVariantsC.V9d(conn, hands)),
			new("V9e", () => SyntheticVariantsC.V9e(conn, hands)),
			new("V9a off", () => SyntheticVariantsC.V9aOff(conn, hands)),
		];
		foreach (var variant in typed)
		{
			var result = variant.Value();
			Compare(report, variant.Key + " vs V1", where, v1, result);
			Compare(report, variant.Key + " vs table", where, fromTable, result);
		}

		KeyValuePair<string, Func<List<SynItem>>>[] items =
		[
			new("V6b-F", () => SyntheticVariantsC.V6bF(conn, hands)),
			new("V6b-E", () => SyntheticVariantsC.V6bE(conn, hands)),
			new("V6b-K", () => Items(SyntheticVariantsC.K(width)(conn))),
			new("V9a on", () => SyntheticVariantsC.V9aOn(conn, hands)),
			new("V9b", () => SyntheticVariantsC.V9b(conn, width)),
			new("V9d-M", () => SyntheticVariantsC.V9dM(conn, hands)),
		];
		foreach (var variant in items)
			CompareItems(report, variant.Key, where, massaged, variant.Value());

		// V6b-E: the delegate calls per row (counted by the counting twin; the measured V6b-E has no counter) and the same output.
		ECountingDelegates.Calls = 0;
		var counted = HandCounted(conn, hands);
		CompareItems(report, "V6b-E (counting delegates)", where, massaged, counted);
		long expectedCalls = (long)rows * DelegateCallsPerRow(width);
		report.Check(ECountingDelegates.Calls == expectedCalls, $"V6b-E {where}: {ECountingDelegates.Calls} delegate calls, expected {expectedCalls} ({DelegateCallsPerRow(width)} per row)");

		// V9a: every rented array came back (rents equal returns, growth included).
		report.Check(PoolTally.Rented == PoolTally.Returned && PoolTally.Rented > 0, $"V9a {where}: ArrayPool rented {PoolTally.Rented} arrays, returned {PoolTally.Returned}");

		Console.WriteLine($"  ok  {where}: V9c-Db, V9c-Syn, F0, F1, F2, F2u, F3, F3r, F1f, F4, V9d, V9e, V9a-off equal V1 and the table; V6b-F, V6b-E ({DelegateCallsPerRow(width)} delegate calls per row), V6b-K, V9a-on, V9b, V9d-M equal the massaged oracle; pool rented={PoolTally.Rented} returned={PoolTally.Returned}");
	}

	static List<SynItem> HandCounted(SyntheticConnection conn, Hands hands)
		=> SyntheticVariantsC.HandList(conn, hands.ECounting);

	// F2u: the copy of the reader class is not sealed (read from the type itself), is what the ExecuteReader callback of the copied provider path hands out, and reads the same data.
	static void VerifyUnsealedReader(Report report)
	{
		report.Check(!typeof(UnsealedSyntheticReader).IsSealed, "UnsealedSyntheticReader is sealed");
		report.Check(typeof(SyntheticReader).IsSealed, "SyntheticReader is not sealed");
		var conn = new UnsealedSyntheticConnection(SyntheticTable.Create(1, 1), new SyntheticOptions());
		bool own = conn.Command(SyntheticVariants.Sql).ExecuteReader(static reader => reader.GetType() == typeof(UnsealedSyntheticReader), CommandBehavior.SequentialAccess);
		report.Check(own, "the reader handed to ExecuteReader on the unsealed connection is not the UnsealedSyntheticReader itself");
		Console.WriteLine($"  ok  UnsealedSyntheticReader.IsSealed={typeof(UnsealedSyntheticReader).IsSealed}, SyntheticReader.IsSealed={typeof(SyntheticReader).IsSealed}; the unsealed connection hands out the UnsealedSyntheticReader itself");

		// F3r / F3: the emitted method lives in a non-collectible / a collectible assembly (read from the delegate's method).
		var fHands = FHands.For(1);
		bool f3Collectible = fHands.F3.Method.DeclaringType!.Assembly.IsCollectible;
		bool f3rCollectible = fHands.F3r.Method.DeclaringType!.Assembly.IsCollectible;
		report.Check(f3Collectible, "F3's assembly is not collectible");
		report.Check(!f3rCollectible, "F3r's assembly is collectible");
		Console.WriteLine($"  ok  F3 assembly collectible={f3Collectible}, F3r assembly collectible={f3rCollectible}; F1f is {fHands.F1f.Method.GetType().Name} over {fHands.F1f.Method.DeclaringType?.Name ?? "(no declaring type)"}, F1 is {fHands.F1.Method.GetType().Name}");
	}

	// F0, planted bad cell: one cell fails (GetInt32 on a string column; the provider throws InvalidCastException) and the exception F0 throws must name that column.
	static void VerifyF0ColumnNames(Report report)
	{
		foreach (var (width, ordinal) in new[] { KeyValuePair.Create(3, 2), KeyValuePair.Create(5, 2), KeyValuePair.Create(10, 2), KeyValuePair.Create(20, 11), KeyValuePair.Create(20, 14) })
		{
			var table = SyntheticTable.Create(width, 3);
			var conn = new SyntheticConnection(table, new SyntheticOptions());
			var planted = FHands.PlantedBadCell(width, ordinal);
			ColumnReadException? found = null;
			string chain = "(nothing thrown)";
			try
			{
				_ = SyntheticVariants.V2(conn, planted);
			}
			catch (Exception ex)
			{
				chain = string.Join(" <- ", WalkChain(ex).Select(static e => e.GetType().Name));
				found = WalkChain(ex).OfType<ColumnReadException>().FirstOrDefault();
			}

			string expected = table.Names[ordinal];
			bool ok = found is not null && found.Column == expected && found.Ordinal == ordinal && found.InnerException is InvalidCastException;
			report.Check(ok, $"F0 planted bad cell at width {width}, ordinal {ordinal}: expected a ColumnReadException for column {expected} over an InvalidCastException, got {chain}: {found?.Message}");
			Console.WriteLine($"  ok  F0 planted bad cell, width {width}, ordinal {ordinal}: {chain}; message: {found?.Message}");
		}
	}

	static IEnumerable<Exception> WalkChain(Exception? ex)
	{
		for (; ex is not null; ex = ex.InnerException) yield return ex;
	}

	// Run once for the whole verify: the reader the callback gets is the provider's own (the hand loop casts it; V9c-Syn relies on it).
	static void VerifyReaderIsSynthetic(Report report)
	{
		var conn = new SyntheticConnection(SyntheticTable.Create(1, 1), new SyntheticOptions());
		bool own = conn.Command(SyntheticVariants.Sql).ExecuteReader(static reader => reader is SyntheticReader, CommandBehavior.SequentialAccess);
		report.Check(own, "the reader handed to ExecuteReader is not the SyntheticReader itself");
		Console.WriteLine("  ok  the reader handed to the ExecuteReader callback is the SyntheticReader itself");
	}

	// E4 (V6b-K): ShapedN carries all 20 members, so its object is the size of SynItem; and what the constructor path does with a get-only property
	// that shares its name with a constructor parameter (one slot, filled through the constructor): the first V6b-K comparison above shows the values arrive.
	static void VerifyObjectSizes(Report report)
	{
		static long Size(Type type)
		{
			_ = RuntimeHelpers.GetUninitializedObject(type);
			long before = GC.GetAllocatedBytesForCurrentThread();
			object o = RuntimeHelpers.GetUninitializedObject(type);
			long size = GC.GetAllocatedBytesForCurrentThread() - before;
			GC.KeepAlive(o);
			return size;
		}

		long item = Size(typeof(SynItem));
		string line = $"SynItem {item} B";
		foreach (var type in new[] { typeof(Shaped1), typeof(Shaped3), typeof(Shaped5), typeof(Shaped10), typeof(Shaped20) })
		{
			long size = Size(type);
			line += $", {type.Name} {size} B";
			report.Check(size == item, $"{type.Name} is {size} B, SynItem is {item} B (the V6b-K object must be the size of the domain object)");
		}

		Console.WriteLine("  ok  object sizes (GC.GetAllocatedBytesForCurrentThread around one allocation): " + line);
	}

	// V9a BufferBytes at width 20: the sum of the rented lengths x element sizes plus the bitmaps, against the hand-computed figure.
	static void VerifyBufferBytes(Report report)
	{
		foreach (int rows in new[] { 1, 100, 1000 })
		{
			var conn = new SyntheticConnection(SyntheticTable.Create(20, rows), new SyntheticOptions());
			var hands = Hands.For(20);
			PoolTally.Reset();
			_ = SyntheticVariantsC.V9aOn(conn, hands);
			long on = SyntheticVariantsC.LastBufferBytes;
			_ = SyntheticVariantsC.V9aOff(conn, hands);
			long off = SyntheticVariantsC.LastBufferBytes;
			long expected = PowerOfTwoSlots(rows) * BufferBytesPerSlotAtWidth20;
			report.Check(on == expected && off == expected, $"V9a BufferBytes at width 20, rows {rows}: on {on}, off {off}, hand-computed {expected}");
			report.Check(PoolTally.Rented == PoolTally.Returned, $"V9a pool balance at width 20, rows {rows}: rented {PoolTally.Rented}, returned {PoolTally.Returned}");
			Console.WriteLine($"  ok  V9a BufferBytes width 20 rows {rows}: {on} B (= {PowerOfTwoSlots(rows)} slots x {BufferBytesPerSlotAtWidth20} B); rented {PoolTally.Rented} returned {PoolTally.Returned} arrays");
		}
	}

	// ---- the DateTimeOffset scenario ----------------------------------------------------------------
	static void CompareDto(Report report, string name, string where, SyntheticTable table, List<DtoRow> actual)
	{
		if (actual.Count != table.Rows)
		{
			report.Fail($"{name} {where}: {actual.Count} rows, expected {table.Rows}");
			return;
		}

		for (int i = 0; i < actual.Count; i++)
		{
			var want = SyntheticVariantsC.DtoFromTable(table, i);
			var got = actual[i];
			bool same = got.Id == want.Id && got.Title == want.Title && got.When.UtcTicks == want.When.UtcTicks && got.When.Offset == want.When.Offset;
			if (same) continue;
			report.Fail($"{name} {where} row {i} differs: expected {want.Id}/{want.Title}/{want.When:O}, actual {got.Id}/{got.Title}/{got.When:O} (UtcTicks and Offset are both compared)");
			return;
		}

		report.Check(true, name);
	}

	static void CompareDt(Report report, string name, string where, SyntheticTable table, List<DtRow> actual)
	{
		if (actual.Count != table.Rows)
		{
			report.Fail($"{name} {where}: {actual.Count} rows, expected {table.Rows}");
			return;
		}

		for (int i = 0; i < actual.Count; i++)
		{
			var want = SyntheticVariantsC.DtFromTable(table, i);
			var got = actual[i];
			if (got.Id == want.Id && got.Title == want.Title && got.When == want.When && got.When.Kind == want.When.Kind) continue;
			report.Fail($"{name} {where} row {i} differs: expected {want.Id}/{want.Title}/{want.When:O}, actual {got.Id}/{got.Title}/{got.When:O}");
			return;
		}

		report.Check(true, name);
	}

	static void VerifyShapeScenario(Report report)
	{
		foreach (int rows in new[] { 1, 100 })
		{
			string where = $"rows={rows}";
			var dtoTable = SyntheticTable.Create(SyntheticCatalog.DateTimeOffsetColumns, rows);
			var dtTable = SyntheticTable.Create(SyntheticCatalog.DateTimeColumns, rows);
			var dto = new SyntheticConnection(dtoTable, new SyntheticOptions());
			var dt = new SyntheticConnection(dtTable, new SyntheticOptions());
			var offsetOk = ((DateTimeOffset[])dtoTable.Columns[2]).All(static w => w.Offset == TimeSpan.FromHours(2));
			report.Check(offsetOk, $"the scenario's When column does not carry the fixed +02:00 offset {where}");
			CompareDto(report, "Dto V1", where, dtoTable, SyntheticVariantsC.DtoV1(dto));
			CompareDto(report, "Dto hand boxed", where, dtoTable, SyntheticVariantsC.DtoHandBoxed(dto));
			CompareDto(report, "Dto hand typed (GetFieldValue<DateTimeOffset>)", where, dtoTable, SyntheticVariantsC.DtoHandTyped(dto));
			CompareDt(report, "Dt V1 (control)", where, dtTable, SyntheticVariantsC.DtV1(dt));
			Console.WriteLine($"  ok  DateTimeOffset scenario {where}: Dto V1, hand boxed, hand typed equal the table (UtcTicks and Offset), Dt V1 equals its table");
		}
	}

	// ---- item 11: the one-off overloads ----------------------------------------------------------------
	static void VerifyOverloads(Report report)
	{
		const int Width = OneOffOverloadBenchmarks.Width, Rows = OneOffOverloadBenchmarks.Rows;
		var table = SyntheticTable.Create(Width, Rows);
		var conn = new SyntheticConnection(table, new SyntheticOptions());
		var expected = new List<SynRow> { SynRow.FromTable(table, 0) };
		string where = $"width={Width} rows={Rows}";
		Compare(report, "one-off params overload, List", where, expected, SyntheticVariantsC.OneOffList(conn));
		Compare(report, "declared map, List", where, expected, SyntheticVariantsC.DeclaredList(conn));
		Compare(report, "one-off params overload, First", where, expected, [SyntheticVariantsC.OneOffFirst(conn)]);
		Compare(report, "declared map, First", where, expected, [SyntheticVariantsC.DeclaredFirst(conn)]);
		Compare(report, "Map<T>() (V1) at the same shape", where, expected, SyntheticVariants.V1(conn));
		Console.WriteLine($"  ok  one-off overload class {where}: OneOff and Declared, List and First, equal the table row and V1");
	}

	// The benchmark methods of the two new classes through their own Setup (as BenchmarkDotNet runs them), compared with the same oracles.
	static void VerifyShapeBenchmarkPath(Report report)
	{
		int invoked = 0;
		foreach (int rows in new[] { 1, 100 })
		{
			var bench = new SyntheticShapeBenchmarks { Rows = rows };
			bench.Setup();
			var dtoTable = SyntheticTable.Create(SyntheticCatalog.DateTimeOffsetColumns, rows);
			var dtTable = SyntheticTable.Create(SyntheticCatalog.DateTimeColumns, rows);
			foreach (var method in typeof(SyntheticShapeBenchmarks).GetMethods().Where(static m => m.GetCustomAttributes(typeof(BenchmarkDotNet.Attributes.BenchmarkAttribute), false).Length > 0))
			{
				invoked++;
				string name = "benchmark " + method.Name;
				switch (method.Invoke(bench, null))
				{
					case List<DtoRow> dto: CompareDto(report, name, $"rows={rows}", dtoTable, dto); break;
					case List<DtRow> dt: CompareDt(report, name, $"rows={rows}", dtTable, dt); break;
					default: report.Fail(name + ": unexpected result"); break;
				}
			}

			bench.Cleanup();
		}

		var overloads = new OneOffOverloadBenchmarks();
		overloads.Setup();
		var table = SyntheticTable.Create(OneOffOverloadBenchmarks.Width, OneOffOverloadBenchmarks.Rows);
		var expected = new List<SynRow> { SynRow.FromTable(table, 0) };
		foreach (var method in typeof(OneOffOverloadBenchmarks).GetMethods().Where(static m => m.GetCustomAttributes(typeof(BenchmarkDotNet.Attributes.BenchmarkAttribute), false).Length > 0))
		{
			invoked++;
			string name = "benchmark " + method.Name;
			switch (method.Invoke(overloads, null))
			{
				case List<SynRow> list: Compare(report, name, "1 row", expected, list); break;
				case SynRow row: Compare(report, name, "1 row", expected, [row]); break;
				default: report.Fail(name + ": unexpected result"); break;
			}
		}

		overloads.Cleanup();
		Console.WriteLine($"  ok  {invoked} invocations of the DateTimeOffset and one-off overload benchmark methods through their own Setup");
	}

	// ---- --selftest-open-time: V9a keeps construction and caller code out of the open window ----------------------------------------------------------------
	// The BS2 shape. A per-row delay in the build step (after the close) must not grow reader-open; the same delay inside the capture (the control: the
	// callback is still running) must. Both go through the real V9aOn, with one delegate of the hand set wrapped.
	static void CheckV9aWindow(Report report, double delayUs, Func<Action<SyntheticConnection>, double> run, Action<string, double, double, double> judge)
	{
		var plain = Hands.For(3);
		double baseline = run(c => SyntheticVariantsC.V9aOn(c, plain));

		var slowBuild = Hands.For(3);
		var realBuild = slowBuild.BuildOn;
		slowBuild.BuildOn = cap =>
		{
			var list = realBuild(cap);
			for (int i = 0; i < list.Count; i++) Spin.Microseconds(delayUs);
			return list;
		};
		double after = run(c => SyntheticVariantsC.V9aOn(c, slowBuild));

		var slowCapture = Hands.For(3);
		var realCapture = slowCapture.Capture;
		slowCapture.Capture = r =>
		{
			var cap = realCapture(r);
			for (int i = 0; i < cap.Rows; i++) Spin.Microseconds(delayUs);
			return cap;
		};
		double inside = run(c => SyntheticVariantsC.V9aOn(c, slowCapture));
		judge("V9a (delay per row in Build = after the close; in Capture = inside)", baseline, inside, after);
		report.Check(true, "V9a window check ran");
	}

	// ---- --selftest-alloc: the DateTimeOffset scenario, bytes per row (information, plus one comparison) ----------------------------------------------------------------
	static void ShapeAllocInfo(Report report)
	{
		const int Rows = 1000;
		var dto = new SyntheticConnection(SyntheticTable.Create(SyntheticCatalog.DateTimeOffsetColumns, Rows), new SyntheticOptions());
		var dt = new SyntheticConnection(SyntheticTable.Create(SyntheticCatalog.DateTimeColumns, Rows), new SyntheticOptions());

		static long Measure(Action work)
		{
			for (int i = 0; i < 30; i++) work();
			long best = long.MaxValue;
			for (int i = 0; i < 20; i++)
			{
				long before = GC.GetAllocatedBytesForCurrentThread();
				work();
				best = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - before);
			}

			return best;
		}

		long dtoV1 = Measure(() => _sink += SyntheticVariantsC.DtoV1(dto).Count);
		long dtV1 = Measure(() => _sink += SyntheticVariantsC.DtV1(dt).Count);
		long boxed = Measure(() => _sink += SyntheticVariantsC.DtoHandBoxed(dto).Count);
		long typed = Measure(() => _sink += SyntheticVariantsC.DtoHandTyped(dto).Count);
		Console.WriteLine($"  info DateTimeOffset scenario, bytes per row over {Rows} rows (the whole call, list included): Dto_V1 {dtoV1 / (double)Rows:F1}, Dt_V1 {dtV1 / (double)Rows:F1}, Dto_HandBoxed {boxed / (double)Rows:F1}, Dto_HandTyped {typed / (double)Rows:F1}");
		report.Check(dtoV1 > dtV1, $"Dto_V1 allocated {dtoV1} B, Dt_V1 {dtV1} B: the DateTimeOffset path must allocate more (the box)");
	}
}
