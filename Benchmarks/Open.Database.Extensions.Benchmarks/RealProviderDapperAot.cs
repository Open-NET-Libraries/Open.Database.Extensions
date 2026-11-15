using System.Data.Common;
using Dapper;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// V7a over the real providers: the V7 call (<see cref="RealProviderVariants.V7"/>: buffered <c>Query&lt;SynRow&gt;(sql).AsList()</c>) through Dapper.AOT's intercepted path.
/// Only this class carries <c>[DapperAot]</c>, so only this call site is intercepted and V7 stays on plain Dapper. One call site serves both providers: the interceptor takes the
/// <see cref="System.Data.IDbConnection"/> it is given, and its generated row factory reads by column name and type at run time.
/// </summary>
[DapperAot]
public static class RealProviderDapperAot
{
	public static List<SynRow> V7a(DbConnection connection) => connection.Query<SynRow>(RealProviderVariants.Sql).AsList();
}
