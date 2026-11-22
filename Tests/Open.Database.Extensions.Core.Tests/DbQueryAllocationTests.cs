using System.Data.Common;
using Open.Database.Extensions.Benchmarks;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The measurement runs each call to completion on this thread (the byte counter is per thread), so it blocks on purpose.
#pragma warning disable xUnit1031

// What a call of the definition-first query allocates, per call, on the
// synthetic provider (one row of five columns; the per-thread byte counter is
// exact, so only a stray JIT or GC adds noise) against hand-written ADO.NET on
// the same provider, and against the command builder.
// The synthetic provider is the benchmark project's, linked in, so these
// bytes are the ones the design numbers were taken on.
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class DbQueryAllocationTests
{
	sealed class Row
	{
		public int Id { get; set; }

		public DateTime Created { get; set; }

		public string? Title { get; set; }

		public decimal Price { get; set; }

		public int? Qty { get; set; }
	}

	readonly record struct Args1(int P0);

	readonly record struct Args2(int P0, long P1);

	readonly record struct Args4(int P0, long P1, string P2, int P3);

	const int VA = 42;
	const int VD = 7;
	const long VB = 1234567890123L;
	const string VC = "abc";

	// Every arm declares exactly the parameters it binds: @p0 .. @p{k-1}.
	static string SqlFor(int k)
	{
		string sql = "select Id, Created, Title, Price, Qty from t where @p0 > 0";
		for (int i = 1; i < k; i++) sql += " and @p" + i + " > 0";
		return sql;
	}

	static readonly DbQuery Q1 = DbQuery.Text(SqlFor(1));
	static readonly DbQuery Q2 = DbQuery.Text(SqlFor(2));
	static readonly DbQuery Q4 = DbQuery.Text(SqlFor(4));

	static readonly DbQuery<Args1> T1 = Q1.Params<Args1>();
	static readonly DbQuery<Args2> T2 = Q2.Params<Args2>();
	static readonly DbQuery<Args4> T4 = Q4.Params<Args4>();

	static ValueTask<List<Row>> Typed(DbConnection c, int k)
		=> k switch
		{
			1 => T1.Using(c).AddParams(new(VA)).Map<Row>().ToListAsync(),
			2 => T2.Using(c).AddParams(new(VA, VB)).Map<Row>().ToListAsync(),
			_ => T4.Using(c).AddParams(new(VA, VB, VC, VD)).Map<Row>().ToListAsync(),
		};

	// The names come from the variables.
	static ValueTask<List<Row>> AdHoc(DbConnection c, int k)
	{
		int p0 = VA;
		long p1 = VB;
		string p2 = VC;
		int p3 = VD;
		if (k == 1)
			return Q1.Using(c).AddParam(p0).Map<Row>().ToListAsync();
		if (k == 2)
			return Q2.Using(c).AddParam(p0).AddParam(p1).Map<Row>().ToListAsync();
		return Q4.Using(c).AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3)
			.Map<Row>().ToListAsync();
	}

	static ValueTask<List<Row>> Builder(DbConnection c, int k)
		=> k switch
		{
			1 => c.Command(SqlFor(1)).AddParam("p0", VA).Map<Row>().ToListAsync(),
			2 => c.Command(SqlFor(2)).AddParam("p0", VA).AddParam("p1", VB)
				.Map<Row>().ToListAsync(),
			_ => c.Command(SqlFor(4)).AddParam("p0", VA).AddParam("p1", VB)
				.AddParam("p2", VC).AddParam("p3", VD).Map<Row>().ToListAsync(),
		};

	static async ValueTask<List<Row>> HandWritten(DbConnection connection, int k)
	{
		using DbCommand command = connection.CreateCommand();
		command.CommandText = SqlFor(k);
		command.CommandType = CommandType.Text;
		Add(command, "p0", DbType.Int32, VA);
		if (k >= 2) Add(command, "p1", DbType.Int64, VB);
		if (k >= 4)
		{
			Add(command, "p2", DbType.String, VC);
			Add(command, "p3", DbType.Int32, VD);
		}

		DbDataReader reader = await command
			.ExecuteReaderAsync(CommandBehavior.SequentialAccess)
			.ConfigureAwait(false);
		try
		{
			List<Row> list = [];
			while (reader.Read())
			{
				list.Add(new Row
				{
					Id = reader.GetInt32(0),
					Created = reader.GetDateTime(1),
					Title = reader.GetString(2),
					Price = reader.GetDecimal(3),
					Qty = reader.IsDBNull(4) ? null : reader.GetInt32(4),
				});
			}

			while (reader.NextResult()) { }
			return list;
		}
		finally
		{
			await reader.DisposeAsync().ConfigureAwait(false);
		}
	}

	static void Add(DbCommand command, string name, DbType type, object value)
	{
		DbParameter parameter = command.CreateParameter();
		parameter.ParameterName = name;
		parameter.DbType = type;
		parameter.Value = value;
		command.Parameters.Add(parameter);
	}

	// ---- measuring ----

	// A count that differs by less than one object (8 B) is the same count: something a runner allocates once in a while
	// (a provider cache, a resize) spreads across the calls of a round as a few bytes, differently on each path.
	static void Near(long expected, long actual)
		=> Assert.InRange(actual, expected - 7, expected + 7);

	// Bytes per call: after a warm-up, the smallest of five rounds of 2,000 calls.
	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 300; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 5; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 2000; i++) call();
			best = Math.Min(
				best,
				(GC.GetAllocatedBytesForCurrentThread() - before) / 2000);
		}

		return best;
	}

	static long Bytes(Func<ValueTask<List<Row>>> call)
		=> BytesPerCall(() => call().AsTask().GetAwaiter().GetResult());

	static SyntheticConnection NewConnection(int rows = 1)
		=> new(
			SyntheticTable.Create(5, rows),
			new SyntheticOptions());

	// The numbers are carried by the failure message; when ODE_ALLOC_LOG names a
	// file they are also appended there, so a passing run can be read too.
	static void Report(string line)
	{
		if (Environment.GetEnvironmentVariable("ODE_ALLOC_LOG") is { Length: > 0 } path)
			File.AppendAllText(path, line + Environment.NewLine);
	}

	public static TheoryData<int> Ks => [1, 2, 4];

	[Theory]
	[MemberData(nameof(Ks))]
	public static void TheTwoForms_ReadTheSameRowsAsHandWrittenAdoNet(int k)
	{
		using DbConnection c = NewConnection(3);

		List<Row> expected = HandWritten(c, k).AsTask().GetAwaiter().GetResult();
		List<Row> typed = Typed(c, k).AsTask().GetAwaiter().GetResult();
		List<Row> adHoc = AdHoc(c, k).AsTask().GetAwaiter().GetResult();

		Assert.Equal(3, expected.Count);
		Assert.Equal(expected.Select(static r => r.Id), typed.Select(static r => r.Id));
		Assert.Equal(expected.Select(static r => r.Id), adHoc.Select(static r => r.Id));
		Assert.Equal(expected.Select(static r => r.Title), typed.Select(static r => r.Title));
	}

	// The bar: a call costs no more than writing it by hand.
	// At K=1 the array the call keeps its value in is not recovered by
	// the rest of the path (see TheFewestValues).
	[Theory]
	[InlineData(2)]
	[InlineData(4)]
	public static void ACall_CostsNoMoreThanHandWrittenAdoNet(int k)
	{
#if DEBUG
		// The byte budgets are those of an optimized build: a Debug build
		// allocates a class per async state machine and keeps locals alive,
		// so what each form costs there is not what a caller gets.
		Assert.Skip($"Release only (K={k}).");
#else
		using DbConnection c = NewConnection();
		long hand = Bytes(() => HandWritten(c, k));
		long typed = Bytes(() => Typed(c, k));
		long adHoc = Bytes(() => AdHoc(c, k));
		long builder = Bytes(() => Builder(c, k));

		Report($"K={k}: typed {typed} B, ad hoc {adHoc} B, hand-written {hand} B, today's builder (explicit names) {builder} B");
		Assert.True(typed <= hand + 7, $"K={k}: typed {typed} B, hand-written {hand} B.");
		Assert.True(adHoc <= hand + 7, $"K={k}: ad hoc {adHoc} B, hand-written {hand} B.");
		Assert.True(typed < builder, $"K={k}: typed {typed} B, builder {builder} B.");
		Assert.True(adHoc < builder, $"K={k}: ad hoc {adHoc} B, builder {builder} B.");
#endif
	}

	static readonly DbQuery Q5 = DbQuery.Text(SqlFor(5));
	static readonly DbQuery Q10 = DbQuery.Text(SqlFor(10));

	static readonly MappedDbQuery<Row> M1 = Q1.Map<Row>();
	static readonly MappedDbQuery<Row> M2 = Q2.Map<Row>();
	static readonly MappedDbQuery<Row> M5 = Q5.Map<Row>();
	static readonly MappedDbQuery<Row> M10 = Q10.Map<Row>();

	// Map-first: a mapped definition reads the same call at no more than the chain that maps after the source,
	// and the values cost the same array whether they come before the source or after it.
	[Fact]
	public static void AMappedDefinition_CostsNoMoreThanTheChainThatMapsAfterTheSource()
	{
#if DEBUG
		// Release only, as ACall_CostsNoMoreThanHandWrittenAdoNet.
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		int p0 = VA;
		long p1 = VB;
		long classic = Bytes(() => Q2.Using(c).AddParam(p0).AddParam(p1).Map<Row>().ToListAsync());
		long after = Bytes(() => M2.Using(c).AddParam(p0).AddParam(p1).ToListAsync());
		long before = Bytes(() => M2.AddParam(p0).AddParam(p1).Using(c).ToListAsync());

		Report($"mapped definition: classic {classic} B, params after Using {after} B, params before Using {before} B");

		// Mapping the call of an unmapped definition costs no more than a definition that maps first.
		Assert.True(after <= classic + 7, $"params after Using {after} B, classic chain {classic} B.");

		// Giving the source after the values copies nothing.
		Near(after, before);
#endif
	}

	// Takes a value without boxing it (assigning a struct to an object would box it and be counted).
	static int _sunk;

	[System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
	static void Sink<TValue>(TValue value)
	{
		if (value is null) _sunk++;
	}

	// Where each byte of the map-first path goes. A call keeps its values in one array sized from the definition:
	// 24 B of array header and 16 B per value (a name and a value), allocated by the first value added and written
	// in place by the next ones. A value type is boxed once, where it is added, because the provider takes an object;
	// strings are not boxed, so a chain of strings costs exactly its array. The same holds when the values are added
	// before the source is known, and the source then adds nothing.
	[Fact]
	public static void TheMapFirstPath_AllocatesOneArray_WhateverTheOrder()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		string s = VC;

		long using0 = BytesPerCall(() => Sink(M2.Using(c)));
		long after1 = BytesPerCall(() => Sink(M1.Using(c).AddParam("p0", s)));
		long after5 = BytesPerCall(() => Sink(M5.Using(c).AddParam("p0", s).AddParam("p1", s).AddParam("p2", s).AddParam("p3", s).AddParam("p4", s)));
		long after10 = BytesPerCall(() => Sink(M10.Using(c).AddParam("p0", s).AddParam("p1", s).AddParam("p2", s).AddParam("p3", s).AddParam("p4", s).AddParam("p5", s).AddParam("p6", s).AddParam("p7", s).AddParam("p8", s).AddParam("p9", s)));
		long before1 = BytesPerCall(() => Sink(M1.AddParam("p0", s)));
		long before5 = BytesPerCall(() => Sink(M5.AddParam("p0", s).AddParam("p1", s).AddParam("p2", s).AddParam("p3", s).AddParam("p4", s)));
		long before10 = BytesPerCall(() => Sink(M10.AddParam("p0", s).AddParam("p1", s).AddParam("p2", s).AddParam("p3", s).AddParam("p4", s).AddParam("p5", s).AddParam("p6", s).AddParam("p7", s).AddParam("p8", s).AddParam("p9", s)));
		UnboundCall<Row> held5 = M5.AddParam("p0", s).AddParam("p1", s).AddParam("p2", s).AddParam("p3", s).AddParam("p4", s);
		long bind5 = BytesPerCall(() => Sink(held5.Using(c)));

		Report($"hops: Using {using0} B; after Using {after1} / {after5} / {after10} B for 1 / 5 / 10 strings; "
			+ $"before Using {before1} / {before5} / {before10} B; Using on five held values {bind5} B");

		// Binding the source allocates nothing: the view holds the call as it is.
		Near(0, using0);
		Near(24 + (16 * 1), after1);
		Near(24 + (16 * 5), after5);
		Near(24 + (16 * 10), after10);

		// The same array whichever order the values and the source are given in, and giving the source copies nothing.
		Near(after1, before1);
		Near(after5, before5);
		Near(after10, before10);
		Near(0, bind5);
#endif
	}

	// Boxing a value type is the provider's cost, paid once where the value is added: an int is 24 B on top of the array.
	[Fact]
	public static void AValueType_IsBoxedOnce_WhereItIsAdded()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		int i = VA;
		long l = VB;

		long oneInt = BytesPerCall(() => Sink(M1.Using(c).AddParam("p0", i)));
		long intAndLong = BytesPerCall(() => Sink(M2.Using(c).AddParam("p0", i).AddParam("p1", l)));

		Report($"boxes: one int {oneInt} B, an int and a long {intAndLong} B");
		Near(40 + 24, oneInt);
		Near(56 + 24 + 24, intAndLong);
#endif
	}

	// One value: the typed form costs no more than hand-written code, and the ad hoc form (the array that holds the value)
	// stays within 16 B of it, since hand-written code boxes its value as well.
	[Fact]
	public static void TheFewestValues_StayWithinASmallMarginOfHandWrittenAdoNet()
	{
#if DEBUG
		// Release only, as ACall_CostsNoMoreThanHandWrittenAdoNet.
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		long hand = Bytes(() => HandWritten(c, 1));
		long typed = Bytes(() => Typed(c, 1));
		long adHoc = Bytes(() => AdHoc(c, 1));
		long builder = Bytes(() => Builder(c, 1));

		Report($"K=1: typed {typed} B, ad hoc {adHoc} B, hand-written {hand} B, today's builder (explicit names) {builder} B");
		Assert.True(typed <= hand + 7, $"K=1: typed {typed} B, hand-written {hand} B.");
		Assert.True(adHoc <= hand + 23, $"K=1: ad hoc {adHoc} B, hand-written {hand} B.");
		Assert.True(typed < builder, $"K=1: typed {typed} B, builder {builder} B.");
#endif
	}

	// Derived names cost nothing over named ones: the facts of an expression
	// are worked out once per expression text.
	[Theory]
	[MemberData(nameof(Ks))]
	public static void DerivedNames_CostNoMoreThanNamedOnes(int k)
	{
		using DbConnection c = NewConnection();
		int p0 = VA;
		long p1 = VB;
		string p2 = VC;
		int p3 = VD;
		DbQuery q = k switch { 1 => Q1, 2 => Q2, _ => Q4 };

		long derived = Bytes(() => k switch
		{
			1 => q.Using(c).AddParam(p0).Map<Row>().ToListAsync(),
			2 => q.Using(c).AddParam(p0).AddParam(p1).Map<Row>().ToListAsync(),
			_ => q.Using(c).AddParam(p0).AddParam(p1).AddParam(p2).AddParam(p3)
				.Map<Row>().ToListAsync(),
		});
		long named = Bytes(() => k switch
		{
			1 => q.Using(c).AddParam("p0", p0).Map<Row>().ToListAsync(),
			2 => q.Using(c).AddParam("p0", p0).AddParam("p1", p1).Map<Row>().ToListAsync(),
			_ => q.Using(c).AddParam("p0", p0).AddParam("p1", p1).AddParam("p2", p2)
				.AddParam("p3", p3).Map<Row>().ToListAsync(),
		});

		Report($"K={k}: derived {derived} B, named {named} B");
		Assert.True(derived <= named + 7, $"K={k}: derived {derived} B, named {named} B.");
	}

	readonly record struct OtherArgs2(int P0, long P1);

	// Two values types on one definition, in turn: the members of each are matched
	// once, so alternating costs nothing more than either alone.
	[Fact]
	public static void TwoValuesTypesOnOneDefinition_AlternateWithoutAllocatingMore()
	{
		using DbConnection c = NewConnection();
		DbQuery<Args2> a = Q2.Params<Args2>();
		DbQuery<OtherArgs2> b = Q2.Params<OtherArgs2>();

		long aloneA = BytesPerCall(() => a.Using(c).AddParams(new(VA, VB)).Map<Row>().ToListAsync().AsTask().GetAwaiter().GetResult());
		long aloneB = BytesPerCall(() => b.Using(c).AddParams(new(VA, VB)).Map<Row>().ToListAsync().AsTask().GetAwaiter().GetResult());
		long alternating = BytesPerCall(() =>
		{
			a.Using(c).AddParams(new(VA, VB)).Map<Row>().ToListAsync().AsTask().GetAwaiter().GetResult();
			b.Using(c).AddParams(new(VA, VB)).Map<Row>().ToListAsync().AsTask().GetAwaiter().GetResult();
		});

		Report($"alternating two values types: {alternating} B per pair, alone {aloneA} B and {aloneB} B");
		Assert.True(alternating <= aloneA + aloneB + 7, $"alternating {alternating} B per pair, alone {aloneA} + {aloneB} B.");
	}

	// ---- the pieces, and the seam ----

	// What a call is made of: the array that holds the values (24 B and 16 B per value, here four, as the text declares),
	// the box of each value type, and nothing else; binding the source and the typed values cost what they hold.
	// Adding to a kept call a second time copies the values it sees into an array of its own, so the first call is never changed.
	[Fact]
	public static void ThePieces_AreExactlyTheirFields_SoTheSeamAddsNothing()
	{
#if DEBUG
		// Release only, as the other byte counts: a Debug build boxes a struct that is passed to a generic method.
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		// A struct is taken without boxing it, so what is counted is what the call allocates.
		long binding = BytesPerCall(() => Sink(Q4.Using(c)));
		long first = BytesPerCall(() => Sink(Q4.Using(c).AddParam("p0", 42)));
		long both = BytesPerCall(() => Sink(Q4.Using(c).AddParam("p0", 42).AddParam("p1", 7L)));
		DbQueryCall held = Q4.Using(c).AddParam("p0", 42);
		long branched = BytesPerCall(() => Sink(held.AddParam("p1", 7L)));
		long typed = BytesPerCall(() => Sink(T4.Using(c).AddParams(new(VA, VB, VC, VD))));

		Report($"binding {binding} B, first value {first} B, a second in the chain {both - first} B, a second on a kept call {branched} B, typed {typed} B");
		Near(0, binding);
		Near(88 + 24, first);
		Near(24, both - first);
		Near(88 + 24, branched);

		// The holder of the typed values: a header and the struct (an int, a long, a string and an int: 24 B).
		Near(40, typed);
#endif
	}

	[Fact]
	public static void TheSeamMembers_AllocateNothing_AndNeverRunOnTheNormalPath()
	{
		using DbConnection c = NewConnection();
		DbQueryCall a = Q2.Using(c).AddParam("p0", 1).AddParam("p1", 2L);
		DbQueryCall b = Q2.Using(c).AddParam("p0", 1).AddParam("p1", 2L);
		bool equal = false;
		int hash = 0;

		long bytes = BytesPerCall(() =>
		{
			equal = a.Equals(b);
			hash = a.GetHashCode();
			_ = a.Source;
		});

		Assert.True(equal);
		Assert.NotEqual(0, hash);
		Near(0, bytes);
	}
}
