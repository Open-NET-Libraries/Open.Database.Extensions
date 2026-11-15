using Dapper;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// V7a: the V7 call (<see cref="SyntheticDapper.V1"/>: buffered <c>Query&lt;SynRow&gt;(sql).AsList()</c>) through Dapper.AOT's intercepted path.
/// Dapper.AOT is off unless asked for: only this class carries <c>[DapperAot]</c>, so only the call sites in this file are intercepted and V7 (SyntheticDapper.cs) stays on plain Dapper.
/// </summary>
[DapperAot]
public static class SyntheticDapperAot
{
	const string Sql = SyntheticVariants.Sql;

	/// <summary>V7a_V1: buffered Query of the typed row, the same call as V7_V1.</summary>
	public static List<SynRow> V1(SyntheticConnection connection) => connection.Query<SynRow>(Sql).AsList();
}
