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

A name you write is sent exactly as written: the library never adds or removes an `@`. Write it without one (`"author"` for `@author`), as the samples do. A name derived from an argument has none either; for a stored procedure or a text command it is replaced by the real name, which has one (`@title_contains`). SQLite accepts a name with or without the `@`, as the samples check.

### Name a parameter from its argument

`AddParam(value)` takes the name from the argument, so you do not spell it twice. The argument must be a variable or a chain of members (properties and fields, members of members): `author`, `search.Author`, `order.Customer.Id`. A type-qualified static or constant is a chain too: `DateTime.UtcNow` derives the name `UtcNow` and `string.Empty` derives `Empty`, so name such a value yourself unless that is the name you want. Deriving needs C# 10 or newer; an older compiler gets an `ArgumentException` and must pass the name.

[!code-csharp[](../samples/ExpressiveCommands.cs#param_from_argument)]

For a stored procedure the name is settled when the command runs. The provider reports the procedure's real parameter names (SQL Server does, through any `DbConnection`, so a command made from an `IDbConnectionFactory<DbConnection>` finds them too), and each argument is matched to one of them:

1. Exactly, else ignoring case, else ignoring case and underscores: `TitleContains` finds `@title_contains`.
2. Else with one common parameter prefix removed from the real name (`p_`, `in_`, `i_`, `prm_`, `par_`, `v_`, or a lone `p` before a capital letter): `Author` finds `@p_author` and `@pAuthor`. A field prefix on the argument (`m_author`, `s_author`, `_author`) is also tried without it.
3. A chain tries its longer endings first, each through all of those steps before the next: `order.Customer.Id` tries `orderCustomerId`, `CustomerId`, then `Id`, so `@customer_id` and `@p_customer_id` are chosen over a bare `@id`.

For example `AddParam(search.Author)` sends `@p_author` when the procedure declares that, `@Author` or `@author` when it declares one of those, and throws when it declares none of them. The prefix step (rule 2) can bind a parameter whose name only starts like a prefix: `Stock` finds `@in_stock`, `Value` finds `@par_value` and `Progress` finds `@in_progress`. When that is not the parameter you mean, name it yourself: `AddParam("stock", stock)`.

For a text command the real names are the `@name` markers found in its SQL text, matched in the same way: `AddParam(regionId)` on `... WHERE region_id = @region_id` sends `@region_id`. A marker inside a string, a quoted or `[bracketed]` identifier or a comment does not count, and neither does a `@@system` variable. Only `@` is recognised; a text that uses `:name` or `?` has no markers. The markers of a text are found once and remembered.

When the real names are not available, as with a text command that has no `@name` marker or a provider that does not implement the `ProcedureParameters` schema collection (the collection that ADO.NET's `DbConnection.GetSchema` answers), the last member is used as written, without an `@`: `Author`. The names are read once and then remembered: see [Remembered names](#remembered-names).

A `null` value is sent as a database NULL. `AddParam(note)` also sends the database type of the argument (`string?` as `String`, `int?` as `Int32`, `byte[]?` as `Binary`), which SQL Server needs to accept a NULL for a typed parameter such as `varbinary`. A `null` given with a name, `AddParam("note", null)`, has no type to use, so pass one:

[!code-csharp[](../samples/SqlServer.cs#sql_server_null_by_name)]

[!code-csharp[](../samples/SqlServer.cs#sql_server_derived_names)]

`AddParams(source)` sends the public properties and fields of an object. When the procedure's names are known, only the members it declares are sent, under the procedure's real names; a parameter you add with `AddParam`, by name or from an argument, wins over a member. Without the procedure's names (a text command with no `@name` marker, a provider that does not implement the `ProcedureParameters` schema collection) every member is sent, named as written. A procedure parameter that no member fills is left alone: it takes its default, or you add it with `AddParam`; a misspelled member is simply not sent. Pass only an object you trust: every declared member is sent, so an object bound from a request can fill a parameter you meant to decide on the server (a tenant, a deleted-rows flag). Add such a value with `AddParam("name", value)`, which wins.

The members are those of the static type of the argument, not of the object's runtime type: a variable typed `object` has no members and sends nothing, an interface sends only its own members, and a `Dictionary<,>` sends its own properties (`Count`, `Keys`) rather than its entries. Pass a variable of the type you mean. Every public readable property is read when you call `AddParams`, even one the procedure does not declare, so a getter that throws or loads lazily runs then. An anonymous object works: `AddParams(new { search.Author, TitleContains = "ode" })`.

An argument that is not a plain member chain (a call, an index, an operator, a literal) throws `ArgumentException` where you wrote it: name the parameter with `AddParam("name", value)`. A name that matches no parameter, or more than one, or two arguments that resolve to the same parameter, throws `InvalidOperationException` that says what you wrote and, when the names are known, what the procedure declares.

#### Remembered names

The names are read once and remembered for five minutes, per server, database and procedure name, on the connection factory (or on the connection itself when you start from a connection). Register the factory once: a connection or a factory you create for each call starts with nothing remembered, so its names are read for every command. Names read inside a transaction are remembered too, even if the transaction rolls back.

A procedure that changes can show its old names for up to five minutes. Only one case reads again before it fails: an argument from `AddParam(value)` that matches none of the remembered names makes the library read the names once more. Everything else uses the remembered names as they are until they expire:

- a member of an `AddParams` object that the remembered names do not declare is skipped, so a new optional parameter is not sent;
- a less specific match is kept while a more specific new parameter exists (`@id` for `order.Customer.Id` although `@customer_id` now exists);
- an ambiguity, or two arguments that resolve to the same parameter, is reported from the remembered names;
- when the second read finds nothing (the procedure is no longer visible to the login), the argument is sent as written and the old names stay until they expire.

#### What was sent?

Before a terminal runs, `command.Params` lists what you added: a name you wrote as written, an argument-derived name as its last member (nothing has been matched yet), and the members of an `AddParams` object under their member names. After a terminal has run, `command.Params` holds the names that were sent. To see what reached the provider, give `Execute(command => ...)` a function that reads the provider's command: its `CommandText`, `CommandType` and `Parameters` (name and value of each) already have the real names. The library keeps no log of the statement and has no other hook.

A derived name stays as written when no real names were read: a text command with no `@name` marker, a provider that does not implement the `ProcedureParameters` schema collection, a procedure name with a database part (`otherdb.dbo.proc`), a name without a schema that exists in more than one schema (write `dbo.proc`), a procedure the login cannot see or that does not exist, or one that declares no parameters. Name the parameters yourself when you call a procedure in another database.

### Name a parameter yourself

`AddParam(name, value)` adds an input parameter under the name you give. Overloads accept a database type and a `ParameterDirection`. `AddParamIf(condition, value)` (or `AddParamIf(condition, name, value)`) adds the parameter only when the condition is true, which keeps the SQL text constant:

[!code-csharp[](../samples/ExpressiveCommands.cs#param_if)]

### A generated id

To get the id an insert generated, select it in the same command and read it with `ExecuteScalar<T>()` (the statement that returns the id depends on your database; SQLite is shown). `ExecuteReturn<T>()` is only for a procedure's `RETURN` code.

[!code-csharp[](../samples/ExpressiveCommands.cs#generated_id)]

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
| `Execute(command => ...)` | Whatever your function returns, given the raw command with its text, parameters and timeout applied: use it for a call the other terminals do not wrap |
| `ExecuteReader(reader => ...)` | Whatever your function returns, given the live reader |
| `Map<T>()` then a terminal | Mapped objects: see [Mapping rows to objects](mapping.md) |
| `ToChannel<T>()`, `ToTargetBlock<T>()` | See [Channels and Dataflow](channels-and-dataflow.md) |

`ExpressiveDbCommand` and `ExpressiveSqlCommand` add `Async` twins (for example `ExecuteNonQueryAsync()`) that honor the command's cancellation token (`UseCancellationToken(token)`). `SetTimeout(seconds)` sets the command timeout. The default is 60 seconds. On SQL Server the lookup of a stored procedure's parameter names uses the same timeout. A failed lookup throws the provider's own exception (a `SqlException` stays a `SqlException`, so a retry policy still sees it), with the procedure's name in `Exception.Data["Procedure"]`.

`UseCancellationToken(token)` sets a token for the command. A token passed to an async terminal such as `ToListAsync(token)` is used together with it, and either one cancels the call. The call then stops with `OperationCanceledException`, except that a provider may report a cancellation that arrives while the command is executing as its own exception (SQL Server's `SqlException`, "Operation cancelled by user"); the library does not translate it. The token covers opening the connection, looking up stored-procedure parameter names, executing the command and reading the rows.

### Which scalar form

- **`ExecuteScalar<T>()`** when `T` is the type the provider returns. A `NULL` or no rows gives `default(T)`.
- **`ExecuteScalar(value => ...)`** to convert. The function takes the provider's value as an `object?` (`Func<object?, T>`): `null` for a `NULL` or no rows, never `DBNull`.
- **`ExecuteScalar()`** for the raw value: `DBNull.Value` for a `NULL`, `null` for no rows.

`ExecuteScalar<T>()` casts the value the provider returns, so `T` must be that exact type, and any other type throws `InvalidCastException`. To tell a `NULL` from 0, use the nullable form of the provider's type: `long?` for `count(*)` on SQLite, `int?` on SQL Server.

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_and_nonquery)]

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_convert)]
