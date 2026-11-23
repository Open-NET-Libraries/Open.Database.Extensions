using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The bytes of every way of adding parameters to a command built with Command(...), and the cost a provider adds to a name that
// is spelled without its prefix. Release only: the byte counts are those of an optimized build.
// What the chain adds is counted as the difference from the same command with no parameters, on strings (a string is not boxed,
// so what is counted is the call itself); what the provider adds is counted on SQLite, as the difference between spellings.
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class ClassicParamPathsAllocationTests
{
	// An interned literal: the text is scanned once, so the list is sized from it and a written name is resolved against it.
	const string Text5 = "select @p0, @p1, @p2, @p3, @p4";

#if !DEBUG
	const string Value = "value";

	const string Text1 = "select @p0";

	// The bytes the chain adds, measured in Release.
	// What the chain adds for one parameter and for five, the same for every way of adding them (the list and its array).
	const long One = 104, Five = 296;

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

	static int _sunk;

	[System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
	static void Sink<TValue>(TValue value)
	{
		if (value is null) _sunk++;
	}

	// Bytes per call: after a warm-up, the smallest of five rounds of 2,000 calls.
	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 300; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 5; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 2000; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 2000);
		}

		return best;
	}

	// A count that differs by less than one object (8 B) is the same count.
	static void Near(long expected, long actual)
		=> Assert.InRange(actual, expected - 7, expected + 7);

	static void Report(string line)
	{
		if (Environment.GetEnvironmentVariable("ODE_ALLOC_LOG") is { Length: > 0 } path)
			File.AppendAllText(path, line + Environment.NewLine);
	}
#endif

	[Fact]
	public static void EachWayOfAddingParameters_CostsWhatItHolds()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using var c = new SqliteConnection("Data Source=:memory:");
		const string p0 = Value, p1 = Value, p2 = Value, p3 = Value, p4 = Value;
		var s1 = new S1(Value);
		var s5 = new S5(Value, Value, Value, Value, Value);
		var e1 = new Existing1();
		var e5 = new Existing5();

		long none1 = BytesPerCall(() => Sink(c.Command(Text1)));
		long none5 = BytesPerCall(() => Sink(c.Command(Text5)));

		long bare1 = BytesPerCall(() => Sink(c.Command(Text1).AddParam("p0", Value))) - none1;
		long bare5 = BytesPerCall(() => Sink(c.Command(Text5).AddParam("p0", Value).AddParam("p1", Value).AddParam("p2", Value).AddParam("p3", Value).AddParam("p4", Value))) - none5;
		long at1 = BytesPerCall(() => Sink(c.Command(Text1).AddParam("@p0", Value))) - none1;
		long at5 = BytesPerCall(() => Sink(c.Command(Text5).AddParam("@p0", Value).AddParam("@p1", Value).AddParam("@p2", Value).AddParam("@p3", Value).AddParam("@p4", Value))) - none5;
		long derived1 = BytesPerCall(() => Sink(c.Command(Text1).AddParam(p0))) - none1;
		long derived5 = BytesPerCall(() => Sink(c.Command(Text5).AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3).AddParam(p4))) - none5;
		long existing1 = BytesPerCall(() => Sink(c.Command(Text1).AddParams(e1))) - none1;
		long existing5 = BytesPerCall(() => Sink(c.Command(Text5).AddParams(e5))) - none5;
		long struct1 = BytesPerCall(() => Sink(c.Command(Text1).AddParams(s1))) - none1;
		long struct5 = BytesPerCall(() => Sink(c.Command(Text5).AddParams(s5))) - none5;

		Report($"classic chain: none {none1} / {none5}; written {bare1} / {bare5}; with prefix {at1} / {at5}; derived {derived1} / {derived5}; existing object {existing1} / {existing5}; struct {struct1} / {struct5}");

		Near(bare1, at1);
		Near(bare5, at5);

		foreach (long bytes in new[] { bare1, at1, derived1, existing1, struct1 }) Near(One, bytes);
		foreach (long bytes in new[] { bare5, at5, derived5, existing5, struct5 }) Near(Five, bytes);
#endif
	}

	// A name spelled without its prefix costs the provider an allocation for every parameter when it is sent as written (SQLite builds
	// the prefixed names to look for). The text declares its names, so a written name is sent as the text spells it and both
	// spellings cost the same, as the derived name does.
	[Fact]
	public static void AWrittenName_CostsTheProviderNothingMore_WithOrWithoutItsPrefix()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using var c = new SqliteConnection("Data Source=:memory:");
		c.Open();
		const int p0 = 1, p1 = 2, p2 = 3, p3 = 4, p4 = 5;

		long written = BytesPerCall(() => _ = c.Command("select @p0 + @p1 + @p2 + @p3 + @p4").AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2).AddParam("p3", p3).AddParam("p4", p4).ExecuteScalar());
		long prefixed = BytesPerCall(() => _ = c.Command("select @p0 + @p1 + @p2 + @p3 + @p4").AddParam("@p0", p0).AddParam("@p1", p1).AddParam("@p2", p2).AddParam("@p3", p3).AddParam("@p4", p4).ExecuteScalar());
		long derived = BytesPerCall(() => _ = c.Command("select @p0 + @p1 + @p2 + @p3 + @p4").AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3).AddParam(p4).ExecuteScalar());

		Report($"classic names on SQLite: written {written}, with prefix {prefixed}, derived {derived}");

		Near(prefixed, written);
		Near(prefixed, derived);
#endif
	}

	// What a run sends: a name written without its prefix goes under the text's own spelling; Params keeps what was added.
	[Fact]
	public static void AWrittenName_IsSentUnderTheTextsOwnSpelling_AndParamsKeepsWhatWasAdded()
	{
		using var c = new SqliteConnection("Data Source=:memory:");
		c.Open();

		ExpressiveDbCommand command = c.Command("select @Id, @name").AddParam("id", 1).AddParam("NAME", "x");

		string[] sent = command.Execute(static cmd => cmd.Parameters.Cast<SqliteParameter>().Select(static p => p.ParameterName).ToArray());

		Assert.Equal(["@Id", "@name"], sent);
		Assert.Equal(["id", "NAME"], command.Params.Select(static p => p.Name));
	}

	[Fact]
	public static void AWrittenName_TheTextDoesNotDeclare_IsSentAsWritten()
	{
		using var c = new SqliteConnection("Data Source=:memory:");
		c.Open();

		ExpressiveDbCommand command = c.Command("select @a").AddParam("a", 1).AddParam("other", 2);

		string[] sent = command.Execute(static cmd => cmd.Parameters.Cast<SqliteParameter>().Select(static p => p.ParameterName).ToArray());

		Assert.Equal(["@a", "other"], sent);
	}

	[Fact]
	public static void AnOutputParameter_IsSentUnderTheNameItWasAddedWith()
	{
		// SQLite takes input parameters only; the tracking provider takes any direction.
		var tally = new DbTally();
		TrackedConnection c = tally.NewOpenConnection();

		ExpressiveDbCommand command = c.Command("select @Id, @Name")
			.AddParam("id", null, ParameterDirection.Output)
			.AddParam("name", "x");

		string[] sent = command.Execute(static cmd => cmd.Parameters.Cast<System.Data.Common.DbParameter>().Select(static p => p.ParameterName).ToArray());

		Assert.Equal(["id", "@Name"], sent);
	}

	[Fact]
	public static void AnOutputParameter_NextToADerivedOne_IsSentUnderTheNameItWasAddedWith()
	{
		var tally = new DbTally();
		TrackedConnection c = tally.NewOpenConnection();
		const int id = 1;

		ExpressiveDbCommand command = c.Command("select @Id, @Name")
			.AddParam(id)
			.AddParam("name", null, ParameterDirection.Output);

		string[] sent = command.Execute(static cmd => cmd.Parameters.Cast<System.Data.Common.DbParameter>().Select(static p => p.ParameterName).ToArray());

		Assert.Equal(["@Id", "name"], sent);
	}

	[Theory]
	[InlineData(ParameterDirection.Input, ParameterDirection.Output)]
	[InlineData(ParameterDirection.Output, ParameterDirection.Input)]
	public static void TwoSpellingsOfOneName_OfAnyDirection_AreSentAsWritten(ParameterDirection first, ParameterDirection second)
	{
		var tally = new DbTally();
		TrackedConnection c = tally.NewOpenConnection();

		ExpressiveDbCommand command = c.Command("select @Id")
			.AddParam("id", null, first)
			.AddParam("@Id", null, second);

		string[] sent = command.Execute(static cmd => cmd.Parameters.Cast<System.Data.Common.DbParameter>().Select(static p => p.ParameterName).ToArray());

		Assert.Equal(["id", "@Id"], sent);
	}

	[Fact]
	public static void TwoWrittenSpellingsOfOneName_AreSentAsWritten()
	{
		using var c = new SqliteConnection("Data Source=:memory:");
		c.Open();

		ExpressiveDbCommand command = c.Command("select @Id").AddParam("id", 1).AddParam("@Id", 2);

		string[] sent = command.Execute(static cmd => cmd.Parameters.Cast<SqliteParameter>().Select(static p => p.ParameterName).ToArray());

		Assert.Equal(["id", "@Id"], sent);
	}

	[Fact]
	public static void AWrittenName_NextToADerivedOne_IsSentUnderTheTextsOwnSpelling()
	{
		using var c = new SqliteConnection("Data Source=:memory:");
		c.Open();
		const int id = 1;

		ExpressiveDbCommand command = c.Command("select @Id, @name").AddParam(id).AddParam("NAME", "x");

		string[] sent = command.Execute(static cmd => cmd.Parameters.Cast<SqliteParameter>().Select(static p => p.ParameterName).ToArray());

		Assert.Equal(["@Id", "@name"], sent);
	}

	[Fact]
	public static void TheListIsSizedFromTheScannedNames()
	{
		using var c = new SqliteConnection("Data Source=:memory:");

		// Five names: the list holds five, not the 4 then 8 of the default growth.
		ExpressiveDbCommand five = c.Command(Text5).AddParam("p0", 0);
		Assert.Equal(5, five.Params.Capacity);

		// A text with no names, and a procedure, keep the default.
		ExpressiveDbCommand none = c.Command("select 1").AddParam("x", 0);
		Assert.Equal(4, none.Params.Capacity);
	}
}
