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

`AddParam(name, value)` adds an input parameter. Overloads accept a database type and a `ParameterDirection`; a parameter with no value (an output or return-value parameter) needs its direction, `AddParam(name, direction)`, and a type and size for an output parameter on SQL Server. Output parameters can be sent, but their values cannot yet be read back after the call. `AddParamIf(condition, name, value)` adds the parameter only when the condition is true, which keeps the SQL text constant:

[!code-csharp[](../samples/ExpressiveCommands.cs#param_if)]

### Name a parameter from its argument

`AddParam(value)` takes the name from the argument itself, so you do not spell it twice. The argument must be a variable or a chain of members (properties and fields, members of members): `search.Author`, `order.Customer.Id`.

[!code-csharp[](../samples/ExpressiveCommands.cs#param_from_argument)]

For a stored procedure the name is settled when the command runs. The provider reports the procedure's real parameter names (SQL Server does) and each argument is matched to one of them: exactly, else ignoring case, else ignoring case and underscores, so `TitleContains` finds `@title_contains`. A chain tries its longer endings first (`order.Customer.Id` tries `OrderCustomerId`, `CustomerId`, then `Id`), so `@customer_id` is chosen over a bare `@id`. The names are read once and remembered for five minutes on the connection factory, so register the factory once; a procedure that changes can show its old names for up to five minutes, and when a remembered name no longer fits, the names are read again once before anything fails. When the real names are not available, as with a text command or another provider, the last member is used as written.

A NULL argument is sent with the database type of the argument (`int?` as `Int32`, `byte[]?` as `Binary`), which SQL Server needs to accept it for a typed parameter such as `varbinary`; a NULL passed by name has no type to use.

[!code-csharp[](../samples/SqlServer.cs#sql_server_derived_names)]

`AddParams(source)` sends the public properties and fields of an object (those of its static type, so pass a variable of the type you mean, not an `object`). When the procedure's names are known, only the members it declares are sent; a parameter added by name always wins over a member. Without the procedure's names (a text command, another provider) every member is sent, named as written. A procedure parameter that no member fills is left alone: it takes its default, or you add it with `AddParam`; a misspelled member is simply not sent. An anonymous object works: `AddParams(new { search.Author, TitleContains = "ode" })`.

An argument that is not a plain member chain (a call, an index, an operator, a literal) throws `ArgumentException` where you wrote it: name the parameter with `AddParam("name", value)`. A name that matches no parameter, or more than one, throws `InvalidOperationException` that lists what the procedure declares.

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

`ExpressiveDbCommand` and `ExpressiveSqlCommand` add `Async` twins (for example `ExecuteNonQueryAsync()`) that honor the command's cancellation token (`UseCancellationToken(token)`). `SetTimeout(seconds)` sets the command timeout. The default is 60 seconds.

`ExecuteScalar<T>()` casts the value the provider returns, so `T` must be that exact type (any other type throws `InvalidCastException`); a `NULL` or no rows returns `default(T)`, so use `long?` when you need to tell that from 0. SQLite returns a `long` for `count(*)` and SQL Server returns an `int`. To convert, pass a function instead:

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_and_nonquery)]

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_convert)]
