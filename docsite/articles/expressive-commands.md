# Expressive commands

An expressive command is a small object you build with chained calls: pick the SQL (or stored procedure), add parameters, then call a terminal that runs it.

## Two starting points

The factory form (`connectionFactory.Command(sql)` or `connectionFactory.StoredProcedure(name)`) and the connection form (`connection.Command(sql)` or `connection.StoredProcedure(name)`) look the same after the first call. In the samples `author` is a variable (`int author = 42;`). Side by side, row against row:

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

Choose by what you need: only the last two rows have `Async` terminals, so a service that awaits starts from a `DbConnection` or an `IDbConnectionFactory<TConnection>` (for example `IDbConnectionFactory<DbConnection>`). Pass a `CommandType` as the second argument of `Command` if you need `TableDirect`.

## Parameters

Name a parameter in the way that fits what you have:

- **A variable or a member holds the value:** `AddParam(author)` or `AddParam(search.Author)`. The name comes from the argument. This is the simplest form.
- **A literal, or any other expression, or a compiler older than C# 10:** name it yourself, `AddParam("author", 42)`.
- **An object holds several values:** `AddParams(search)`.

A name you write is sent exactly as written: the library never adds or removes an `@`. Write it without one (`"author"` for `@author`), as the samples do; a derived name never has one either. SQLite accepts a name with or without the `@`, as the samples check.

### Name a parameter from its argument

`AddParam(value)` takes the name from the argument, so you do not spell it twice. The argument must be a variable or a chain of members (properties and fields, members of members): `author`, `search.Author`, `order.Customer.Id`. Deriving needs C# 10 or newer; an older compiler gets an `ArgumentException` and must pass the name.

[!code-csharp[](../samples/ExpressiveCommands.cs#param_from_argument)]

For a stored procedure the name is settled when the command runs. The provider reports the procedure's real parameter names (SQL Server does) and each argument is matched to one of them: exactly, else ignoring case, else ignoring case and underscores, so `TitleContains` finds `@title_contains`. When none of those match, the same argument is tried against the real name with one common parameter prefix removed (`p_`, `in_`, `i_`, `prm_`, `par_`, `v_`, or a lone `p` before a capital letter), so `Author` also finds `@p_author` and `@pAuthor`; a field prefix on the argument (`m_author`, `s_author`, `_author`) is also tried without it. A chain tries its longer endings first, each one through all of those steps before the next (`order.Customer.Id` tries `OrderCustomerId`, `CustomerId`, then `Id`), so `@customer_id` and `@p_customer_id` are chosen over a bare `@id`. The names are read once and remembered for five minutes on the connection factory (or on the connection itself when you start from a connection), so register the factory once; a procedure that changes can show its old names for up to five minutes, and when a remembered name no longer fits, the names are read again once before anything fails. When the real names are not available, as with a text command or another provider, the last member is used as written, without an `@`.

A NULL argument is sent with the database type of the argument (`int?` as `Int32`, `byte[]?` as `Binary`), which SQL Server needs to accept it for a typed parameter such as `varbinary`; a NULL passed by name has no type to use.

[!code-csharp[](../samples/SqlServer.cs#sql_server_derived_names)]

`AddParams(source)` sends the public properties and fields of an object (those of its static type, so pass a variable of the type you mean, not an `object`). When the procedure's names are known, only the members it declares are sent; a parameter added by name always wins over a member. Without the procedure's names (a text command, another provider) every member is sent, named as written. A procedure parameter that no member fills is left alone: it takes its default, or you add it with `AddParam`; a misspelled member is simply not sent. An anonymous object works: `AddParams(new { search.Author, TitleContains = "ode" })`.

An argument that is not a plain member chain (a call, an index, an operator, a literal) throws `ArgumentException` where you wrote it: name the parameter with `AddParam("name", value)`. A name that matches no parameter, or more than one, throws `InvalidOperationException` that lists what the procedure declares.

### Name a parameter yourself

`AddParam(name, value)` adds an input parameter under the name you give. Overloads accept a database type and a `ParameterDirection`. `AddParamIf(condition, value)` (or `AddParamIf(condition, name, value)`) adds the parameter only when the condition is true, which keeps the SQL text constant:

[!code-csharp[](../samples/ExpressiveCommands.cs#param_if)]

### Return values and output parameters

`ExecuteReturn<T>()` runs the command and returns the value of its return-value parameter, which it adds itself (you do not call `AddReturnParam` for it). As with `ExecuteScalar<T>()`, `T` must be the exact type the provider returns, and a NULL gives `default(T)`. A SQL Server stored procedure returns an `int`, its `RETURN` code:

[!code-csharp[](../samples/SqlServer.cs#sql_server_return)]

Output values cannot be read back yet. You can send an output parameter (`AddParam(name, ParameterDirection.Output)`, with a type and size on SQL Server) but not read what the procedure put in it. A procedure that hands its results back through output parameters needs plain ADO.NET for now; a result set and the return value are readable.

## Terminals

| Terminal | Returns |
|---|---|
| `ExecuteNonQuery()` | Rows affected |
| `ExecuteScalar<T>()`, `ExecuteScalar(value => ...)`, `ExecuteScalar()` | The first column of the first row: [which form](#which-scalar-form) |
| `ExecuteReturn<T>()` | The value of the return-value parameter: [see above](#return-values-and-output-parameters) |
| `Execute(command => ...)` | Whatever your function returns, given the raw `IDbCommand` |
| `ExecuteReader(reader => ...)` | Whatever your function returns, given the live reader |
| `Map<T>()` then a terminal | Mapped objects: see [Mapping rows to objects](mapping.md) |
| `ToChannel<T>()`, `ToTargetBlock<T>()` | See [Channels and Dataflow](channels-and-dataflow.md) |

`ExpressiveDbCommand` and `ExpressiveSqlCommand` add `Async` twins (for example `ExecuteNonQueryAsync()`) that honor the command's cancellation token (`UseCancellationToken(token)`). `SetTimeout(seconds)` sets the command timeout. The default is 60 seconds.

`UseCancellationToken(token)` sets a token for the command. A token passed to an async terminal such as `ToListAsync(token)` is used together with it, and either one cancels the call. The call then stops with `OperationCanceledException`, except that a provider may report a cancellation that arrives while the command is executing as its own exception (SQL Server's `SqlException`, "Operation cancelled by user"); the library does not translate it. The token covers opening the connection, looking up stored-procedure parameter names, executing the command and reading the rows.

### Which scalar form

- **`ExecuteScalar<T>()`** when `T` is the type the provider returns. A `NULL` or no rows gives `default(T)`.
- **`ExecuteScalar(value => ...)`** to convert. The function takes the provider's value as an `object?` (`Func<object?, T>`): `null` for a `NULL` or no rows, never `DBNull`.
- **`ExecuteScalar()`** for the raw value: `DBNull.Value` for a `NULL`, `null` for no rows.

`ExecuteScalar<T>()` casts the value the provider returns, so `T` must be that exact type, and any other type throws `InvalidCastException`. To tell a `NULL` from 0, use the nullable form of the provider's type: `long?` for `count(*)` on SQLite, `int?` on SQL Server.

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_and_nonquery)]

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_convert)]
