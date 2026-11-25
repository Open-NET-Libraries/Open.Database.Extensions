using Open.Database.Extensions.Benchmarks;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The measurement runs each call to completion on this thread (the byte counter is per thread), so it blocks on purpose.
#pragma warning disable xUnit1031

// What DbQuery.Scalar<T>() and DbNonQuery allocate per call, on the synthetic
// provider, against hand-written ADO.NET and against the command builder; as
// DbQueryAllocationTests does for the rows. Release only: a Debug build
// allocates a class per async state machine.
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class ScalarAndNonQueryAllocationTests
{
#if !DEBUG
	const string Sql = "select Id from t where @p0 > 0 and @p1 > 0";
	const int VA = 42;
	const long VB = 1234567890123L;

	static readonly DbQuery Query = DbQuery.Text(Sql);
	static readonly ScalarDbQuery<int> Scalar = Query.Scalar<int>();
	static readonly DbNonQuery Write = DbNonQuery.Text(Sql);

	static SyntheticConnection NewConnection()
		=> new(SyntheticTable.Create(5, 1), new SyntheticOptions());

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

	static long Bytes<T>(Func<ValueTask<T>> call)
		=> BytesPerCall(() => call().AsTask().GetAwaiter().GetResult());

	static void Report(string line)
	{
		if (Environment.GetEnvironmentVariable("ODE_ALLOC_LOG") is { Length: > 0 } path)
			File.AppendAllText(path, line + Environment.NewLine);
	}

	// The array a call keeps two values in.
	const int TwoValues = 24 + 16 * 2;

	static DbCommand HandCommand(DbConnection connection, bool values)
	{
		DbCommand command = connection.CreateCommand();
		command.CommandText = Sql;
		command.CommandType = CommandType.Text;
		if (values)
		{
			Add(command, "p0", DbType.Int32, VA);
			Add(command, "p1", DbType.Int64, VB);
		}

		return command;
	}

	static void Add(DbCommand command, string name, DbType type, object value)
	{
		DbParameter parameter = command.CreateParameter();
		parameter.ParameterName = name;
		parameter.DbType = type;
		parameter.Value = value;
		command.Parameters.Add(parameter);
	}

	static async ValueTask<int> HandScalar(DbConnection connection, bool values)
	{
		await using DbCommand command = HandCommand(connection, values);
		return (int)(await command.ExecuteScalarAsync().ConfigureAwait(false))!;
	}

	static async ValueTask<int> HandWrite(DbConnection connection, bool values)
	{
		await using DbCommand command = HandCommand(connection, values);
		return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
	}

	[System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
	static void Sink<TValue>(TValue value)
	{
		if (value is null) Report("null");
	}
#endif

	// With no values the run allocates what the hand-written call does; values
	// add the one array that holds them (24 B and 16 B per value), as on a rows
	// chain, and nothing else.
	[Fact]
	public static void AScalarCall_CostsHandWrittenAdoNet_PlusItsValuesArray()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		long bare = Bytes(() => HandScalar(c, values: false));
		long none = Bytes(() => Scalar.Using(c).ExecuteScalarAsync());
		long hand = Bytes(() => HandScalar(c, values: true));
		long after = Bytes(() => Scalar.Using(c).AddParam("p0", VA).AddParam("p1", VB).ExecuteScalarAsync());
		long before = Bytes(() => Scalar.AddParam("p0", VA).AddParam("p1", VB).Using(c).ExecuteScalarAsync());
		long builder = Bytes(() => c.Command(Sql).AddParam("p0", VA).AddParam("p1", VB).ExecuteScalarAsync<int>());

		Report($"scalar: no values {none} B (hand-written {bare} B); two values {after} B, values first {before} B (hand-written {hand} B, command builder {builder} B)");
		Assert.True(none <= bare + 7, $"no values: {none} B, hand-written {bare} B.");
		Assert.True(after <= hand + TwoValues + 7, $"two values: {after} B, hand-written {hand} B.");
		Assert.True(before <= hand + TwoValues + 7, $"values first: {before} B, hand-written {hand} B.");
		Assert.True(after < builder, $"two values: {after} B, command builder {builder} B.");
#endif
	}

	// With no values the run allocates what the hand-written call does; values
	// add the one array that holds them (24 B and 16 B per value), as on a rows
	// chain, and nothing else.
	[Fact]
	public static void AWriteCall_CostsHandWrittenAdoNet_PlusItsValuesArray()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		long bare = Bytes(() => HandWrite(c, values: false));
		long none = Bytes(() => Write.Using(c).ExecuteNonQueryAsync());
		long hand = Bytes(() => HandWrite(c, values: true));
		long after = Bytes(() => Write.Using(c).AddParam("p0", VA).AddParam("p1", VB).ExecuteNonQueryAsync());
		long before = Bytes(() => Write.AddParam("p0", VA).AddParam("p1", VB).Using(c).ExecuteNonQueryAsync());
		long builder = Bytes(() => c.Command(Sql).AddParam("p0", VA).AddParam("p1", VB).ExecuteNonQueryAsync());

		Report($"write: no values {none} B (hand-written {bare} B); two values {after} B, values first {before} B (hand-written {hand} B, command builder {builder} B)");
		Assert.True(none <= bare + 7, $"no values: {none} B, hand-written {bare} B.");
		Assert.True(after <= hand + TwoValues + 7, $"two values: {after} B, hand-written {hand} B.");
		Assert.True(before <= hand + TwoValues + 7, $"values first: {before} B, hand-written {hand} B.");
		Assert.True(after < builder, $"two values: {after} B, command builder {builder} B.");
#endif
	}

	// The hops cost what the rows chain's do: nothing for the source, and one
	// array (24 B and 16 B per value) in either order.
	[Fact]
	public static void TheHops_AllocateWhatTheRowsChainDoes()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		using DbConnection c = NewConnection();
		const string s = "abc";

		long rows = BytesPerCall(() => Sink(Query.Using(c).AddParam("p0", s).AddParam("p1", s)));
		long using0 = BytesPerCall(() => Sink(Scalar.Using(c)));
		long after = BytesPerCall(() => Sink(Scalar.Using(c).AddParam("p0", s).AddParam("p1", s)));
		long before = BytesPerCall(() => Sink(Scalar.AddParam("p0", s).AddParam("p1", s)));
		PendingCall<NonQueryKind> held = Write.AddParam("p0", s).AddParam("p1", s);
		long bind = BytesPerCall(() => Sink(held.Using(c)));

		Report($"hops: rows chain {rows} B, Using {using0} B, after Using {after} B, before Using {before} B, Using on held values {bind} B");
		Assert.InRange(using0, 0, 7);
		Assert.InRange(bind, 0, 7);
		Assert.InRange(after, rows - 7, rows + 7);
		Assert.InRange(before, rows - 7, rows + 7);
#endif
	}
}
