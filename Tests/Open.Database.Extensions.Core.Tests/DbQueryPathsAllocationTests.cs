using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The bytes of every way of adding parameters to a definition, before and after Using where both orders exist, and the cost a
// provider adds to a name that is spelled without its prefix. Release only: the byte counts are those of an optimized build.
// What the chain builds is counted exactly, on strings (a string is not boxed, so what is counted is the call itself); what the
// provider adds is counted on SQLite, as the difference between spellings and the growth from five parameters to ten.
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class DbQueryPathsAllocationTests
{
	const string Value = "value";

	readonly record struct S1(string P0);

	readonly record struct S5(string P0, string P1, string P2, string P3, string P4);

	sealed class Existing1
	{
		public string P0 { get; set; } = Value;
	}

	sealed class Existing5
	{
		public string P0 { get; set; } = Value;

		public string P1 { get; set; } = Value;

		public string P2 { get; set; } = Value;

		public string P3 { get; set; } = Value;

		public string P4 { get; set; } = Value;
	}

#if !DEBUG
	static readonly DbQuery Q1 = DbQuery.Text("select @p0");

	static readonly DbQuery Q5 = DbQuery.Text("select @p0, @p1, @p2, @p3, @p4");

	static readonly MappedDbQuery<long> M1 = Q1.Map<long>();

	static readonly MappedDbQuery<long> M5 = Q5.Map<long>();

	static readonly MappedDbQuery<S1, long> T1 = Q1.Params<S1>().Map<long>();

	static readonly MappedDbQuery<S5, long> T5 = Q5.Params<S5>().Map<long>();

	[SuppressMessage("CodeQuality", "IDE0052:Remove unread private members", Justification = "Write-only on purpose: the count keeps the measured call from being optimized away.")]
	static int _sunk;

	// Takes a value without boxing it (assigning a struct to an object would box it and be counted).
	[System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
	static void Sink<TValue>(TValue value)
	{
		if (value is null) _sunk++;
	}

	// Bytes per call: after a warm-up, the smallest of five rounds of 2,000 calls.
	// The pause before each round lets the JIT finish replacing its first, unoptimized code in the background,
	// so the smallest round measures the optimized code an application runs, whatever the runner's timing.
	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 300; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 5; round++)
		{
			Thread.Sleep(200);
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 2000; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 2000);
		}

		return best;
	}

	// A count that differs by less than one object (8 B) is the same count: something a runner allocates once in a while
	// (a provider cache, a resize) spreads across the calls of a round as a few bytes, differently on each path.
	static void Near(long expected, long actual)
		=> Assert.InRange(actual, expected - 7, expected + 7);

	static void Same(long expected, params long[] actual)
	{
		foreach (long bytes in actual) Near(expected, bytes);
	}

	static void Report(string line)
	{
		if (Environment.GetEnvironmentVariable("ODE_ALLOC_LOG") is { Length: > 0 } path)
			File.AppendAllText(path, line + Environment.NewLine);
	}
#endif

	// What the chain builds, per path (what each costs on top of the provider's own parameters):
	//  - a written name, with or without its prefix, or a derived name: one array, 24 B and 16 B per value, whatever the order;
	//  - the members of an object: one holder (a header and the object, or the struct itself), and the object when it is built for the call;
	//  - the typed values: the same holder as an untyped struct; the definition adds nothing.
	[Fact]
	public static void EachWayOfAddingParameters_CostsExactlyWhatItHolds()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using var c = new SqliteConnection("Data Source=:memory:");
		string p0 = Value, p1 = Value, p2 = Value, p3 = Value, p4 = Value;
		var existing1 = new Existing1();
		var existing5 = new Existing5();

		// A written name.
		long explicit1 = BytesPerCall(() => Sink(M1.Using(c).AddParam("p0", p0)));
		long explicit5 = BytesPerCall(() => Sink(M5.Using(c).AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4)));
		long explicitBefore1 = BytesPerCall(() => Sink(M1.AddParam("p0", p0)));
		long explicitBefore5 = BytesPerCall(() => Sink(M5.AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4)));

		// A written name with its prefix.
		long at1 = BytesPerCall(() => Sink(M1.Using(c).AddParam("@p0", p0)));
		long at5 = BytesPerCall(() => Sink(M5.Using(c).AddParam("@p0", p0).AddParam("@p1", p1).AddParam("@p2", p2).AddParam("@p3", p3).AddParam("@p4", p4)));
		long atBefore5 = BytesPerCall(() => Sink(M5.AddParam("@p0", p0).AddParam("@p1", p1).AddParam("@p2", p2).AddParam("@p3", p3).AddParam("@p4", p4)));

		// A derived name.
		long derived1 = BytesPerCall(() => Sink(M1.Using(c).AddParam(p0)));
		long derived5 = BytesPerCall(() => Sink(M5.Using(c).AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3).AddParam(p4)));
		long derivedBefore5 = BytesPerCall(() => Sink(M5.AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3).AddParam(p4)));

		// The members of an anonymous object, built for the call.
		long anon1 = BytesPerCall(() => Sink(M1.Using(c).AddParams(new { p0 })));
		long anon5 = BytesPerCall(() => Sink(M5.Using(c).AddParams(new { p0, p1, p2, p3, p4 })));
		long anonBefore5 = BytesPerCall(() => Sink(M5.AddParams(new { p0, p1, p2, p3, p4 })));

		// The members of an object that already exists.
		long existingAfter1 = BytesPerCall(() => Sink(M1.Using(c).AddParams(existing1)));
		long existingAfter5 = BytesPerCall(() => Sink(M5.Using(c).AddParams(existing5)));
		long existingBefore5 = BytesPerCall(() => Sink(M5.AddParams(existing5)));

		// The members of a struct, built for the call, on a definition that does not declare it.
		long struct1 = BytesPerCall(() => Sink(M1.Using(c).AddParams(new S1(p0))));
		long struct5 = BytesPerCall(() => Sink(M5.Using(c).AddParams(new S5(p0, p1, p2, p3, p4))));
		long structBefore5 = BytesPerCall(() => Sink(M5.AddParams(new S5(p0, p1, p2, p3, p4))));

		// The same struct, declared by the definition. Held before Using the values are the struct itself, so
		// nothing is allocated until Using gives the source and the holder is made once.
		long typedHeld5 = BytesPerCall(() => Sink(T5.AddParams(new(p0, p1, p2, p3, p4))));
		long typed1 = BytesPerCall(() => Sink(T1.Using(c).AddParams(new(p0))));
		long typed5 = BytesPerCall(() => Sink(T5.Using(c).AddParams(new(p0, p1, p2, p3, p4))));
		long typedBefore5 = BytesPerCall(() => Sink(T5.AddParams(new(p0, p1, p2, p3, p4)).Using(c)));

		Report($"paths, 1 / 5 strings: written {explicit1} / {explicit5} (before Using {explicitBefore1} / {explicitBefore5}), with prefix {at1} / {at5} (before {atBefore5}), "
			+ $"derived {derived1} / {derived5} (before {derivedBefore5}), anonymous {anon1} / {anon5} (before {anonBefore5}), "
			+ $"existing object {existingAfter1} / {existingAfter5} (before {existingBefore5}), struct {struct1} / {struct5} (before {structBefore5}), "
			+ $"typed {typed1} / {typed5} (before {typedBefore5})");

		// One array: 24 B and 16 B per value, the same for every spelling and for both orders.
		Same(40, explicit1, explicitBefore1, at1, derived1);
		Same(104, explicit5, explicitBefore5, at5, atBefore5, derived5, derivedBefore5);

		// A holder of 24 B (a header and the reference to an object that already exists), whatever the object holds.
		Same(24, existingAfter1, existingAfter5, existingBefore5);

		// The anonymous object built for the call: a header and a reference a member, on top of its holder.
		Same(24 + 24, anon1);
		Same(24 + 56, anon5, anonBefore5);

		// A holder of 16 B and the struct itself, 8 B a member; the typed definition adds nothing to the untyped one.
		Same(24, struct1, typed1);
		Same(56, struct5, structBefore5, typed5, typedBefore5);
		Same(0, typedHeld5);
#endif
	}

	// The unmapped form, DbQuery.Using(c).AddParam(...).Map<T>(), is the same text with the same cost: the call maps without being
	// boxed, so what it costs is what the map-first form costs.
	[Fact]
	public static void TheUnmappedForm_CostsWhatTheMapFirstFormCosts()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using var c = new SqliteConnection("Data Source=:memory:");
		string p0 = Value, p1 = Value, p2 = Value, p3 = Value, p4 = Value;
		DbQuery<S5> typed5 = Q5.Params<S5>();

		long using5 = BytesPerCall(() => Sink(Q5.Using(c).Map<long>()));
		long one = BytesPerCall(() => Sink(Q1.Using(c).AddParam("p0", p0).Map<long>()));
		long five = BytesPerCall(() => Sink(Q5.Using(c).AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4).Map<long>()));
		long derived = BytesPerCall(() => Sink(Q5.Using(c).AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3).AddParam(p4).Map<long>()));
		long selector = BytesPerCall(() => Sink(Q5.Using(c).AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4).Map(static r => r.GetInt64(0))));
		long rows = BytesPerCall(() => Sink(Q5.Using(c).AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4).AsObjectArrays()));
		long typed = BytesPerCall(() => Sink(typed5.Using(c).AddParams(new(p0, p1, p2, p3, p4)).Map<long>()));

		Report($"unmapped: Using {using5}, 1 / 5 values {one} / {five}, derived {derived}, selector {selector}, rows {rows}, typed {typed}");

		Same(0, using5);
		Same(40, one);
		Same(104, five, derived, rows);

		// A selector adds the object that holds it (24 B).
		Same(104 + 24, selector);
		Same(56, typed);
#endif
	}

	// A name spelled without its prefix costs the provider an allocation for every parameter when it is sent as written (SQLite builds
	// the prefixed names to look for). The definition has scanned the text, so a written name is sent as the text declares it and
	// both spellings cost the same.
	// The floor is the same call written by hand with the prefixed names; what a call adds to it is the array that holds the values
	// (16 B a value), so the growth from five parameters to ten is 80 B more than the floor's.
	[Fact]
	public static void AWrittenName_CostsTheProviderNothingMore_WithOrWithoutItsPrefix()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using var c = new SqliteConnection("Data Source=:memory:");
		c.Open();
		// Locals, not constants: the values stand for values known only at run time.
		int p0 = 1, p1 = 2, p2 = 3, p3 = 4, p4 = 5, p5 = 6, p6 = 7, p7 = 8, p8 = 9, p9 = 10;
		var q5 = DbQuery.Text("select @p0 + @p1 + @p2 + @p3 + @p4");
		var q10 = DbQuery.Text("select @p0 + @p1 + @p2 + @p3 + @p4 + @p5 + @p6 + @p7 + @p8 + @p9");
		MappedDbQuery<long> m5 = q5.Map<long>();
		MappedDbQuery<long> m10 = q10.Map<long>();

		long raw5 = BytesPerCall(() =>
		{
			using var cmd = c.CreateCommand();
			cmd.CommandText = "select @p0 + @p1 + @p2 + @p3 + @p4";
			cmd.Parameters.Add(new SqliteParameter("@p0", p0));
			cmd.Parameters.Add(new SqliteParameter("@p1", p1));
			cmd.Parameters.Add(new SqliteParameter("@p2", p2));
			cmd.Parameters.Add(new SqliteParameter("@p3", p3));
			cmd.Parameters.Add(new SqliteParameter("@p4", p4));
			_ = cmd.ExecuteScalar();
		});
		long raw10 = BytesPerCall(() =>
		{
			using var cmd = c.CreateCommand();
			cmd.CommandText = "select @p0 + @p1 + @p2 + @p3 + @p4 + @p5 + @p6 + @p7 + @p8 + @p9";
			cmd.Parameters.Add(new SqliteParameter("@p0", p0));
			cmd.Parameters.Add(new SqliteParameter("@p1", p1));
			cmd.Parameters.Add(new SqliteParameter("@p2", p2));
			cmd.Parameters.Add(new SqliteParameter("@p3", p3));
			cmd.Parameters.Add(new SqliteParameter("@p4", p4));
			cmd.Parameters.Add(new SqliteParameter("@p5", p5));
			cmd.Parameters.Add(new SqliteParameter("@p6", p6));
			cmd.Parameters.Add(new SqliteParameter("@p7", p7));
			cmd.Parameters.Add(new SqliteParameter("@p8", p8));
			cmd.Parameters.Add(new SqliteParameter("@p9", p9));
			_ = cmd.ExecuteScalar();
		});

		long bare5 = BytesPerCall(() => _ = m5.Using(c).AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4).FirstOrDefault());
		long at5 = BytesPerCall(() => _ = m5.Using(c).AddParam("@p0", p0).AddParam("@p1", p1).AddParam("@p2", p2).AddParam("@p3", p3).AddParam("@p4", p4).FirstOrDefault());
		long derived5 = BytesPerCall(() => _ = m5.Using(c).AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3).AddParam(p4).FirstOrDefault());
		long before5 = BytesPerCall(() => _ = m5.AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4).Using(c).FirstOrDefault());
		long bare10 = BytesPerCall(() => _ = m10.Using(c).AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4).AddParam("p5", p5).AddParam("p6", p6).AddParam("p7", p7).AddParam("p8", p8).AddParam("p9", p9).FirstOrDefault());
		long at10 = BytesPerCall(() => _ = m10.Using(c).AddParam("@p0", p0).AddParam("@p1", p1).AddParam("@p2", p2).AddParam("@p3", p3).AddParam("@p4", p4).AddParam("@p5", p5).AddParam("@p6", p6).AddParam("@p7", p7).AddParam("@p8", p8).AddParam("@p9", p9).FirstOrDefault());
		long derived10 = BytesPerCall(() => _ = m10.Using(c).AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3).AddParam(p4).AddParam(p5).AddParam(p6).AddParam(p7).AddParam(p8).AddParam(p9).FirstOrDefault());

		Report($"names on SQLite: floor {raw5} / {raw10}; written {bare5} / {bare10}, with prefix {at5} / {at10}, derived {derived5} / {derived10}, before Using {before5}");

		// The spelling makes no difference, nor does the order.
		Near(at5, bare5);
		Near(at5, derived5);
		Near(at5, before5);
		Near(at10, bare10);
		Near(at10, derived10);

		// Over the floor a call adds 16 B for each value it holds, and nothing the provider does for a name.
		Near(raw10 - raw5 + 16 * 5, bare10 - bare5);
#endif
	}
}
