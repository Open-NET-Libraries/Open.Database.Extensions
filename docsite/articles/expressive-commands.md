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

A connection pool (`IDbConnectionPool`) is also a valid source, but it is there for a pooling scheme of your own: a pool a particular database needs, or one kept in memory and reused locally. Providers already pool their connections (Microsoft SQL Server included), so with those a factory is the right source and a pool of your own adds nothing.

A command is not safe to share across threads: one built from a single connection throws `InvalidOperationException` if two terminals run at once.

## Which command type you get

| You call it on | You get | Reader type |
|---|---|---|
| `IDbConnection`, `IDbTransaction`, `IDbConnectionFactory` | `ExpressiveCommand` | `IDataReader` |
| `DbConnection`, `DbTransaction`, `IDbConnectionFactory<TConnection>` | `ExpressiveDbCommand` | `DbDataReader` (async reads) |
| `SqlConnection` or a `SqlConnectionFactory` (MSSqlClient package) | `ExpressiveSqlCommand` | `SqlDataReader` |

Choose by what you need: only the last two rows have `Async` terminals, so a service that awaits starts from a `DbConnection` or an `IDbConnectionFactory<TConnection>` (for example `IDbConnectionFactory<DbConnection>`). Pass a `CommandType` as the second argument of `Command` if you need `TableDirect`.

## Defining a query once

When the same query runs from several places, define it once, with no connection, and bind it where it runs. A `DbQuery` is immutable, so it is safe in a `static readonly` field and shared by every caller. `Using(source)` binds it to a connection, a transaction, a pool or a factory; each `AddParam` returns a new immutable call and leaves the one before it as it was, so a held call can branch.

```csharp
static readonly DbQuery PostsByAuthor
    = DbQuery.Procedure("dbo.PostsByAuthor");

// The names come from the variables, as AddParam(author) does on a command.
List<Post> posts = await PostsByAuthor.Using(db)
    .AddParam(authorId)
    .AddParam(day)
    .Map<Post>()
    .ToListAsync(cancellationToken);
```

To have the values checked at compile time, declare their type once. It is a `struct` that implements `IEquatable<TParams>` (a `readonly record struct` is the intended shape, and does) whose public properties and fields are the parameters, given in one call that the compiler checks:

```csharp
readonly record struct AuthorDayParams(int AuthorId, DateTime Day);

static readonly DbQuery<AuthorDayParams> PostsByAuthorTyped
    = DbQuery.Procedure("dbo.PostsByAuthor").Params<AuthorDayParams>();

List<Post> posts = await PostsByAuthorTyped.Using(db)
    .AddParams(new(AuthorId: authorId, Day: day))
    .Map<Post>()
    .ToListAsync(cancellationToken);
```

### Map the definition too

`Map<T>()` also works on the definition, before any connection exists. The result, a `MappedDbQuery<T>`, is immutable like the `DbQuery` under it, so it too lives in a `static readonly` field, and the map, with the reader compiled from it, lives with it: there is no separate `RowMap<T>` field to declare. (`DbQuery<TParams>` has the same `Map`, giving a `MappedDbQuery<TParams, T>`.) A class that holds its queries as definitions, next to the same class building each command on every call:

[!code-csharp[](../samples/MapFirst.cs#mapfirst_before)]

[!code-csharp[](../samples/MapFirst.cs#mapfirst_after)]

`Using(source)` takes the same four sources and returns the ordinary `MappedQuery<T>` view, with every terminal, `Skip` and `Take`. The parameters can come after `Using`, as above, or before it, and the command is the same either way. The values before the source are held without a connection, so the held call has no terminal: it cannot be read until `Using` gives it a source.

[!code-csharp[](../samples/MapFirst.cs#mapfirst_values_first)]

What the definition fixes is what never changes from one call to the next: the command text, the map, and `SingleResult()`, `AsyncReads()` and `UsePool(pool, clearOnReturn)`, each of which returns a new definition. What each call chooses is the values and, on the view, `Skip` and `Take`:

[!code-csharp[](../samples/MapFirst.cs#mapfirst_paged)]

A parameter added after `Using` is added to a query that `Using` made from a definition. A query built the other way (`connection.Command(sql).Map<T>()`) takes its parameters on the command, before `Map`, and `AddParam` on it throws `InvalidOperationException`. A `default` value of one of these types throws the same exception, as the other structs do.

Everything after `Using` is what a command does: `Map<T>()` is the same extension, every terminal and `SingleResult()` apply, the connection is opened late and closed again only if the call opened it, a factory's connection is disposed and a pool's is given back. `SetTimeout(seconds)` on a definition returns a new definition. A `DbQuery` takes the same sources as `Using` does for a command, but only the typed ones: a connection (`DbConnection`), a transaction, an `IDbConnectionPool<TConnection>` or an `IDbConnectionFactory<TConnection>`; a bare `Func<TConnection>` is not one (wrap it in `new DbConnectionFactory<TConnection>(func)`).

A call is cheaper than the command builder and no dearer than ADO.NET written by hand, apart from one small node (at most 48 bytes) at a single value: it is a few small immutable objects, and the typed form is one. Names are resolved as a command resolves them (see [Name a parameter from its argument](#name-a-parameter-from-its-argument)): `AddParam(authorId)` finds `@author_id`, and the typed form matches each member of the values type the same way and, when the names are known, does not send a member the query does not declare. A text definition finds its `@name` markers once, when it is made; a procedure's names are remembered for five minutes per connection source (the factory, the pool or the connection, also for a transaction) and per server and database, and read again only when they do not fit.

## Parameters

Name a parameter in the way that fits what you have:

- **A variable or a member holds the value:** `AddParam(author)` or `AddParam(search.Author)`. The name comes from the argument. This is the simplest form.
- **A literal, or any other expression, or a compiler older than C# 10:** name it yourself, `AddParam("author", 42)`.
- **An object holds several values:** `AddParams(search)`.

A name you write is sent exactly as written: the library never adds or removes an `@`. Write it without one (`"author"` for `@author`), as the samples do. A name derived from an argument has none either; for a stored procedure it is replaced by the provider's real name, which has one (`@title_contains`). SQLite accepts a name with or without the `@`, as the samples check.

### Name a parameter from its argument

`AddParam(value)` takes the name from the argument, so you do not spell it twice. The argument must be a variable or a chain of members (properties and fields, members of members): `author`, `search.Author`, `order.Customer.Id`. A type-qualified static or constant is a chain too: `DateTime.UtcNow` derives the name `UtcNow` and `string.Empty` derives `Empty`, so name such a value yourself unless that is the name you want. Deriving needs C# 10 or newer; an older compiler gets an `ArgumentException` and must pass the name.

[!code-csharp[](../samples/ExpressiveCommands.cs#param_from_argument)]

For a stored procedure the name is settled when the command runs. The provider reports the procedure's real parameter names (SQL Server does), and each argument is matched to one of them:

1. Exactly, else ignoring case, else ignoring case and underscores: `TitleContains` finds `@title_contains`.
2. Else with one common parameter prefix dropped from the real name (`p_`, `in_`, `i_`, `prm_`, `par_`, `v_`, or a lone `p` before a capital letter): `Author` finds `@p_author` and `@pAuthor`. A field prefix on the argument (`m_author`, `s_author`, `_author`) is also tried without it.
3. A chain tries its longer endings first, each through all of those steps before the next: `order.Customer.Id` tries `orderCustomerId`, `CustomerId`, then `Id`, so `@customer_id` and `@p_customer_id` are chosen over a bare `@id`.

For example `AddParam(search.Author)` sends `@p_author` when the procedure declares that, `@Author` or `@author` when it declares one of those, and throws when it declares none of them. The prefix step (rule 2) can bind a parameter whose name only starts like a prefix: `Stock` finds `@in_stock`, `Value` finds `@par_value` and `Progress` finds `@in_progress`. When that is not the parameter you mean, name it yourself: `AddParam("stock", stock)`.

When the real names are not available, as with a text command or another provider, the last member is used as written, without an `@`: `Author`. The names are read once and then remembered: see [Remembered names](#remembered-names).

A `null` value is sent as a database NULL. A NULL also carries the database type of its argument (`string?` as `String`, `int?` as `Int32`, `byte[]?` as `Binary`), which SQL Server needs to accept a NULL for a typed parameter such as `varbinary`: `AddParam(note)` takes it from the argument, and `AddParam("note", (int?)null)` from the overload. A value of type `object`, or a call that gives the direction by position, has no type to use, so pass one:

[!code-csharp[](../samples/SqlServer.cs#sql_server_null_by_name)]

[!code-csharp[](../samples/SqlServer.cs#sql_server_derived_names)]

`AddParams(source)` sends the public properties and fields of an object. When the procedure's names are known, only the members it declares are sent, under the procedure's real names; a parameter you add with `AddParam`, by name or from an argument, wins over a member. A text command that has `@name` markers is matched against them in the same way, so a member with no marker is not sent. When the names are not known, every member is sent, named as written: a stored procedure on a provider that cannot report its parameter names, or a text command with no `@name` marker. A procedure parameter that no member fills is left alone: it takes its default, or you add it with `AddParam`; a misspelled member is simply not sent. Pass only an object you trust: every declared member is sent, so an object bound from a request can fill a parameter you meant to decide on the server (a tenant, a deleted-rows flag). Add such a value with `AddParam("name", value)`, which wins.

The members are those of the static type of the argument, not of the object's runtime type: a variable typed `object` has no members and sends nothing, an interface sends only its own members, and a `Dictionary<,>` sends its own properties (`Count`, `Keys`) rather than its entries. Pass a variable of the type you mean. Every public readable property is read when you call `AddParams`, even one the procedure does not declare, so a getter that throws or loads lazily runs then. An anonymous object works: `AddParams(new { search.Author, TitleContains = "ode" })`.

An argument that is not a plain member chain (a call, an index, an operator, a literal) throws `ArgumentException` where you wrote it: name the parameter with `AddParam("name", value)`. A name that matches no parameter, or more than one, or two arguments that resolve to the same parameter, throws `InvalidOperationException` that says what you wrote and, when the names are known, what the procedure declares.

#### Remembered names

Known names are read once and remembered for five minutes (an empty answer for 15 seconds, see below), per server, database and procedure name, on the connection factory (or on the connection itself when you start from a connection). Register the factory once: a connection or a factory you create for each call starts with nothing remembered, so its names are read for every command. Names read inside a transaction are remembered too, even if the transaction rolls back.

A procedure that changes can show its old names for up to five minutes. Two cases read again before they fail or drop anything: an argument from `AddParam(value)` that matches none of the remembered names, and a member of an `AddParams` object that the remembered names do not declare. Either makes the library read the names once more. A read that follows a miss is itself remembered together with the fact that it was a second look, so a member the procedure never declares costs one extra read per five minutes, not one per run. Everything else uses the remembered names as they are until they expire:

- a member of an `AddParams` object that the second read still does not find is skipped, so a parameter added to the procedure after that read is not sent until the names expire;
- a less specific match is kept while a more specific new parameter exists (`@id` for `order.Customer.Id` although `@customer_id` now exists);
- an ambiguity, or two arguments that resolve to the same parameter, is reported from the remembered names;
- when the second read finds nothing (the procedure is not visible to the login, or it now declares no parameters), the argument is sent as written, and the old names are dropped.

A read that finds nothing (an unreadable procedure, one that does not exist, or one that declares no parameters) is remembered too, but for 15 seconds only, so a procedure that cannot be read costs one lookup in 15 seconds, not one in every run, and a procedure that appears or changes is picked up soon. Until then its derived names are sent as written.

#### What was sent?

`command.Params` always lists what you added: a name you wrote as written, an argument-derived name as its last member, and the members of an `AddParams` object under their member names. A run never changes it. The real names are worked out in every run, for the connection that run uses, so one command object can run against two databases that name the parameters of a procedure differently. To see what the run sent, give `Execute(command => ...)` a function that reads the provider's command: its `CommandText`, `CommandType` and `Parameters` (name and value of each) already have the real names. The library keeps no log of the statement and has no other hook.

A derived name stays as written when no real names were read: a text command, another provider, a procedure name with a database part (`otherdb.dbo.proc`), a procedure the login cannot see or that does not exist, or one that declares no parameters. Name the parameters yourself when you call a procedure in another database.

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
| `ToListAsync<T>()`, `ToArrayAsync<T>()`, `ToImmutableArrayAsync<T>()`, `FirstAsync<T>()`, `FirstOrDefaultAsync<T>()`, `SingleAsync<T>()`, `SingleOrDefaultAsync<T>()`, `ToHashSetAsync<T>()` | `Map<T>()` then that async terminal (for a synchronous read write `Map<T>().ToList()`); a single value such as `int` reads the first column: [One value per row](mapping.md#one-value-per-row) |
| `Map<T>()` then `ToChannelReader()`, or `ToTargetBlock<T>()` | See [Channels and Dataflow](channels-and-dataflow.md) |

The blocking terminals (`ExecuteNonQuery()`, `ExecuteScalar`, `ExecuteReturn`, `Execute`, `ExecuteReader` and the `Map<T>()` terminals such as `ToList()`) are in the `Open.Database.Extensions.Synchronous` namespace: add `using Open.Database.Extensions.Synchronous;` to call them. The `Async` terminals need no extra `using`.

The terminals that take the type read one column as easily as a model; the token goes last:

[!code-csharp[](../samples/ExpressiveCommands.cs#single_value_async)]

`ExpressiveDbCommand` and `ExpressiveSqlCommand` add `Async` twins (for example `ExecuteNonQueryAsync()`) that honor the command's cancellation token (`UseCancellationToken(token)`). `SetTimeout(seconds)` sets the command timeout. The default is 60 seconds. On SQL Server the lookup of a stored procedure's parameter names uses the same timeout. A failed lookup throws the provider's own exception (a `SqlException` stays a `SqlException`, so a retry policy still sees it), with the procedure's name in `Exception.Data["Procedure"]`.

`UseCancellationToken(token)` sets a token for the command. A token passed to an async terminal such as `ToListAsync(token)` is combined with it, and either one cancels the call. The `Async` terminals of the command itself (`ExecuteNonQueryAsync`, `ExecuteScalarAsync`, `ExecuteReaderAsync`, `ExecuteReturnAsync`) take a `CancellationToken` too, and it is combined with this one. The call then stops with `OperationCanceledException`, except that a provider may report a cancellation that arrives while the command is executing as its own exception (SQL Server's `SqlException`, "Operation cancelled by user"); the library does not translate it. The token covers opening the connection, looking up stored-procedure parameter names, executing the command and reading the rows.

### Which scalar form

- **`ExecuteScalar<T>()`** for the value as `T`; it converts the way `Map<T>()` does (a `bigint` read as `int`). A `NULL` or no rows gives `default(T)`.
- **`ExecuteScalar(value => ...)`** to convert in your own way. The function takes the provider's value as an `object?` (`Func<object?, T>`): `null` for a `NULL` or no rows, never `DBNull`.
- **`ExecuteScalar()`** for the raw value: `DBNull.Value` for a `NULL`, `null` for no rows.
- **`ToListAsync<T>()`** (or `Map<T>().ToList()`, `ToArrayAsync<T>()`, ...) for many rows of one column: it converts like a property, as `ExecuteScalar<T>()` does. See [One value per row](mapping.md#one-value-per-row).

`ExecuteScalar<T>()` converts the value the provider returns the way `Map<T>()` converts a column. A value that is already a `T` is returned as it is. An integer converts to another integer type with a range check (a `bigint` read as `int`, an `int` read as `long`), and any other value is converted by the same code `Map<T>()` uses (a `numeric`, which is a `decimal`, read as `long`). A value that cannot be converted, or does not fit, throws `ValueMappingException` with the conversion's own error inside. To tell a `NULL` from 0, ask for the nullable type: `int?` or `long?`.

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_read_as_int)]

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_and_nonquery)]

[!code-csharp[](../samples/ExpressiveCommands.cs#scalar_convert)]
