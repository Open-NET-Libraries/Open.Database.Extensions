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
	// At K=1 the one node the call is built from (48 B) is not recovered by
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
		Assert.True(typed <= hand, $"K={k}: typed {typed} B, hand-written {hand} B.");
		Assert.True(adHoc <= hand, $"K={k}: ad hoc {adHoc} B, hand-written {hand} B.");
		Assert.True(typed < builder, $"K={k}: typed {typed} B, builder {builder} B.");
		Assert.True(adHoc < builder, $"K={k}: ad hoc {adHoc} B, builder {builder} B.");
#endif
	}

	// One value: the single node (48 B for the first, the one a bound query
	// returns) is the whole difference from hand-written code.
	[Fact]
	public static void TheFewestValues_CostOneNodeOverHandWrittenAdoNet()
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
		Assert.True(typed <= hand + 48, $"K=1: typed {typed} B, hand-written {hand} B.");
		Assert.True(adHoc <= hand + 48, $"K=1: ad hoc {adHoc} B, hand-written {hand} B.");
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
		Assert.True(derived <= named, $"K={k}: derived {derived} B, named {named} B.");
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
		Assert.True(alternating <= aloneA + aloneB, $"alternating {alternating} B per pair, alone {aloneA} + {aloneB} B.");
	}

	// ---- the pieces, and the seam ----

	// The memoize seam adds no state to a node: the first node holds the
	// definition, the source, a name and the value (48 B), every later node its
	// parent, a name and the value (40 B), and binding allocates nothing.
	[Fact]
	public static void ThePieces_AreExactlyTheirFields_SoTheSeamAddsNothing()
	{
		using DbConnection c = NewConnection();
		object? sink = null;

		long binding = BytesPerCall(() => _ = Q4.Using(c));
		long first = BytesPerCall(() => sink = Q4.Using(c).AddParam("p0", 42));
		DbQueryCall held = Q4.Using(c).AddParam("p0", 42);
		long second = BytesPerCall(() => sink = held.AddParam("p1", 7L));
		long typed = BytesPerCall(() => sink = T4.Using(c).AddParams(new(VA, VB, VC, VD)));

		// What a caller that keys on the call does is a separate, opt-in cost.
		GC.KeepAlive(sink);
		Report($"binding {binding} B, first node {first} B, later node {second} B, typed root {typed} B");
		Assert.Equal(0, binding);
		Assert.Equal(48, first);
		Assert.Equal(40, second);
		Assert.Equal(64, typed);
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
		Assert.Equal(0, bytes);
	}
}
