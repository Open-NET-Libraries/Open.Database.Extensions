# Expressive commands

An expressive command is a small fluent object: pick the SQL (or stored procedure), add parameters, then call a terminal that runs it.

## Two starting points

The factory form (`connectionFactory.Command(sql)` or `connectionFactory.StoredProcedure(name)`) and the connection form (`connection.Command(sql)` or `connection.StoredProcedure(name)`) look the same after the first call. Side by side, row against row:

<div class="side-by-side">
<div>

**Factory form**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_list_call)]

</div>
<div>

**Connection form**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_list_own)]

</div>
</div>

In the factory form the command creates, opens and disposes a connection for every terminal. In the connection form the connection is yours: the command opens it only when it is closed, closes only what it opened, and never disposes it. See [who owns the connection](foundation.md#who-owns-the-connection).

A command is not safe to share across threads: one built from a single connection throws `InvalidOperationException` if two terminals run at once.

## Which command type you get

| You call it on | You get | Reader type |
|---|---|---|
| `IDbConnection`, `IDbTransaction`, `IDbConnectionFactory` | `ExpressiveCommand` | `IDataReader` |
| `DbConnection`, `DbTransaction`, `IDbConnectionFactory<TConnection>` | `ExpressiveDbCommand` | `DbDataReader` (async reads) |
| `SqlConnection` or a `SqlConnectionFactory` (MSSqlClient package) | `ExpressiveSqlCommand` | `SqlDataReader` |

Pass a `CommandType` as the second argument of `Command` if you need `TableDirect`.

## Parameters

`AddParam(name, value)` adds an input parameter. Overloads accept a database type and a `ParameterDirection`. `AddParamIf(condition, name, value)` adds the parameter only when the condition is true, which keeps the SQL text constant:

[!code-csharp[](../samples/ExpressiveCommands.cs#param_if)]

`AddReturnParam` registers a return value; call `ExecuteReturn<T>()` to read it.

## Terminals

| Terminal | Returns |
|---|---|
| `ExecuteNonQuery()` | Rows affected |
| `ExecuteScalar()`, `ExecuteScalar<T>()` | The first column of the first row |
| `Execute(command => ...)` | Whatever your function returns, given the raw `IDbCommand` |
| `ExecuteReader(reader => ...)` | Whatever your function returns, given the live reader |
| `Map<T>()` then a terminal | Mapped objects: see [Mapping rows to objects](mapping.md) |
| `ToChannel<T>()`, `ToTargetBlock<T>()` | See [Channels and Dataflow](channels-and-dataflow.md) |

`ExpressiveDbCommand` and `ExpressiveSqlCommand` add `Async` twins (for example `ExecuteNonQueryAsync()`) that honor the command's cancellation token (`UseCancellationToken(token)`). `SetTimeout(seconds)` sets the command timeout.

`ExecuteScalar<T>()` casts the value the provider returns, so `T` must be that exact type. SQLite returns a `long` for `count(*)` and SQL Server returns an `int`. To convert, pass a function instead:

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_and_nonquery)]

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_convert)]
