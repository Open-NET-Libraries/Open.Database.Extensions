# Coming from Dapper

Open.Database.Extensions is a set of extensions on ADO.NET, not a Dapper clone, and plenty of code uses both on the same connection. This page is for readers who know Dapper's API and want the equivalent here. For speed and memory, see [Performance](performance.md).

## Side by side

Both snippets run against the same data in the build, and the results are checked to be equal.

**A list**

[!code-csharp[](../samples/DapperComparison.cs#dapper_list)]

[!code-csharp[](../samples/DapperComparison.cs#ode_list)]

**The first row**

[!code-csharp[](../samples/DapperComparison.cs#dapper_first)]

[!code-csharp[](../samples/DapperComparison.cs#ode_first)]

**A scalar**

[!code-csharp[](../samples/DapperComparison.cs#dapper_scalar)]

[!code-csharp[](../samples/DapperComparison.cs#ode_scalar)]

**A command with no result set**

[!code-csharp[](../samples/DapperComparison.cs#dapper_execute)]

[!code-csharp[](../samples/DapperComparison.cs#ode_execute)]

## Differences to know about

| | Dapper | Open.Database.Extensions |
|---|---|---|
| Parameters | An anonymous object or `DynamicParameters` | `.AddParam(name, value)` calls on the command |
| Starting point | Extension methods on `IDbConnection` | `connection.Command(sql)` or `connectionFactory.Command(sql)` |
| Connection handling | Opens a closed connection for the call and closes it again | Same rule for a connection. From a factory, it creates and disposes the connection |
| Mapping to a class | Constructor binding, settable properties and fields, configurable name matching | Public settable properties only; see [the rules](mapping.md#the-rules) |
| Positional records | Supported | Not supported: `Map<T>()` needs `new T()` |
| Other mapping features | Multi-mapping, `QueryMultiple`, dynamic results, custom type handlers | Not provided; use `Map(selector)` or the reader helpers |

Dapper's features are described in Dapper's own documentation. The record example below runs in the build against Dapper 2.1.89:

[!code-csharp[](../samples/DapperComparison.cs#dapper_record)]

## Migrating a call site

1. Replace the anonymous parameter object with `.AddParam` calls.
2. Replace `Query<T>(sql, ...)` with `Command(sql)...Map<T>().ToList()` (or `ToArray()`).
3. If a model uses constructor binding, give it a parameterless constructor and settable properties, or write a `Map(selector)`.
4. If column names differ from property names, declare a `RowMap<T>` (see [Columns named differently](mapping.md#columns-named-differently)): Dapper's global name matching has no equivalent here.
