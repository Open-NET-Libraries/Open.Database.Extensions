using Dapper;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// V7: Dapper 2.1.89 over the same synthetic connection, for the shapes Dapper has an equivalent of. The final objects are the same classes
/// (<see cref="SynRow"/>, <see cref="SynItem"/>), so --verify compares them with the library's variants.
/// <para>
/// Dapper CANNOT do (and nothing is faked): V2 and V6a (a hand-written selector over the IDataRecord; Dapper maps by reflection-emitted
/// code and has no per-row record selector on Query, so a hand-written read through a reader is raw ADO.NET, which is V0/V2 territory),
/// V4a and V4b (Dapper always creates a new instance per row; it cannot fill a held or pooled one), V0/V0b (not a Dapper concern).
/// Dapper also never drains later result sets (its buffered and unbuffered reads use CommandBehavior.SingleResult), so the drain column
/// is 0 for it and an ExtraSets case does not compare with the library.
/// Mapping of shapes: V7_V1 = V1/V5 without the second object; V7_V3 = V3 (unbuffered Query, the reader stays open while the Select runs);
/// V7_V3s = V3s (QueryUnbufferedAsync); V7_V5 = V5; V7_V6c = V6c; V7_V6d = V6d; V7_V8a = V8a/V8c shape (ExecuteScalar&lt;int&gt;), V7_V8b = QueryFirst&lt;int&gt;.
/// </para>
/// </summary>
public static class SyntheticDapper
{
	const string Sql = SyntheticVariants.Sql;

	static readonly Func<SynRow, SynItem> ToItem = static row => SynItem.From(row);

	/// <summary>V7_V1: buffered Query of the typed row (the V1 shape).</summary>
	public static List<SynRow> V1(SyntheticConnection connection) => connection.Query<SynRow>(Sql).AsList();

	public static async Task<List<SynRow>> V1Async(SyntheticConnection connection)
		=> (await connection.QueryAsync<SynRow>(Sql).ConfigureAwait(false)).AsList();

	/// <summary>V7_V3: unbuffered Query (the reader stays open) with the Select inline: two objects per row.</summary>
	public static List<SynItem> V3(SyntheticConnection connection, int pace)
	{
		var list = new List<SynItem>();
		foreach (var row in connection.Query<SynRow>(Sql, buffered: false))
		{
			if (pace > 0) Spin.Iterations(pace);
			list.Add(SynItem.From(row));
		}

		return list;
	}

	/// <summary>V7_V3s: QueryUnbufferedAsync streamed; the pace is paid in the consumer's loop.</summary>
	public static async Task<List<SynItem>> V3s(SyntheticConnection connection, int pace)
	{
		var list = new List<SynItem>();
		await foreach (var row in connection.QueryUnbufferedAsync<SynRow>(Sql).ConfigureAwait(false))
		{
			if (pace > 0) Spin.Iterations(pace);
			list.Add(SynItem.From(row));
		}

		return list;
	}

	/// <summary>V7_V5: buffered Query (the reader closes), then the Select after the close.</summary>
	public static List<SynItem> V5(SyntheticConnection connection)
		=> connection.Query<SynRow>(Sql).AsList().Select(ToItem).ToList();

	public static async Task<List<SynItem>> V5Async(SyntheticConnection connection)
		=> (await connection.QueryAsync<SynRow>(Sql).ConfigureAwait(false)).AsList().Select(ToItem).ToList();

	/// <summary>V7_V6c: massage inline while the unbuffered reader is open.</summary>
	public static List<SynItem> V6c(SyntheticConnection connection, int width, int pace)
	{
		var list = new List<SynItem>();
		foreach (var row in connection.Query<SynRow>(Sql, buffered: false))
		{
			if (pace > 0) Spin.Iterations(pace);
			list.Add(SynItem.Massage(SynItem.From(row), width));
		}

		return list;
	}

	/// <summary>V7_V6d: massage after the close.</summary>
	public static List<SynItem> V6d(SyntheticConnection connection, int width)
		=> connection.Query<SynRow>(Sql).AsList().Select(row => SynItem.Massage(SynItem.From(row), width)).ToList();

	public static async Task<List<SynItem>> V6dAsync(SyntheticConnection connection, int width)
		=> (await connection.QueryAsync<SynRow>(Sql).ConfigureAwait(false)).AsList().Select(row => SynItem.Massage(SynItem.From(row), width)).ToList();

	/// <summary>V7_V8a: Dapper's ExecuteScalar&lt;int&gt;.</summary>
	public static int V8a(SyntheticConnection connection) => connection.ExecuteScalar<int>(Sql);

	public static Task<int> V8aAsync(SyntheticConnection connection) => connection.ExecuteScalarAsync<int>(Sql)!;

	/// <summary>V7_V8b: Dapper's QueryFirst&lt;int&gt; (the reader path).</summary>
	public static int V8b(SyntheticConnection connection) => connection.QueryFirst<int>(Sql);

	public static Task<int> V8bAsync(SyntheticConnection connection) => connection.QueryFirstAsync<int>(Sql);
}
