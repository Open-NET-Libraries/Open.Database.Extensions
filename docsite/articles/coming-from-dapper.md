# Coming from Dapper

Open.Database.Extensions is a set of extensions on ADO.NET, not a Dapper clone, and plenty of code uses both. This page is for readers who know Dapper. The biggest difference is not the query syntax: it is who handles the connection. For speed and memory, see [Performance](performance.md).

Column names that differ from your properties by more than case and underscores need a `RowMap<T>` declared once: see [Renamed columns](#renamed-columns). Names that differ only by case or underscores need nothing.

## The steps, side by side

Each comparison lists the same four steps in the same order, row against row: (1) get the connection, (2) the command or query, (3) the parameters, (4) the terminal and the mapping. A row that one side does not need says "nothing". The SQL is predefined once, so the lines stay short:

[!code-csharp[](../samples/ConnectionPattern.cs#shared_sql)]

**Dapper needs no setup.** It needs the connection string and the provider type in scope at every call site. It opens a closed connection for the call and closes it again afterwards, so `connection.Open()` is not required; add it only if you want to control when the connection opens.

**Open.Database.Extensions needs a connection factory,** created or registered once per application (see [Registering the factory once](#registering-the-factory-once)):

[!code-csharp[](../samples/ConnectionPattern.cs#ode_setup_once)]

After that, no call site writes any connection code. The counts below are the lines of the samples shown with them, without this one line of one-time setup and without the `// nothing` comments.

### A list of rows

<div class="side-by-side">
<div>

**Dapper**

[!code-csharp[](../samples/ConnectionPattern.cs#dapper_list_call)]

</div>
<div>

**Open.Database.Extensions (factory)**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_list_call)]

</div>
</div>

Lines you write per call in the two samples above: Dapper 4, Open.Database.Extensions 3 (plus the one-time setup).

What is different:

- **The connection.** With Dapper you create it and dispose it, and Dapper opens it if it is closed and closes it when the call finishes. With a factory, the factory creates it for each execution, the command opens it only if it is closed and disposes it when the terminal returns, so it is open only for the duration of the call.
- **Rows.** Dapper's `Query<T>` buffers by default (`buffered = true`) and returns an `IEnumerable<T>`, turned into a list in the sample by Dapper's `AsList()`. `ToList()` reads every row into a list during the call. `Read(handler)` offers a lazy sequence that is only valid inside the handler.
- **Mapping.** Both match columns to members by name, ignoring case. Dapper can also bind constructor parameters and fields and, if you turn on `MatchNamesWithUnderscores`, match `first_name` to `FirstName`. The extensions set public settable properties (`init` and `required` ones too) and bind the one public constructor of a record or class (see [Positional records](#positional-records)), but never set fields. They always ignore underscores when no exact or ignore-case match exists (`first_name` feeds `FirstName`; there is nothing to turn on). A column that differs by more than case and underscores needs a `RowMap<T>` (see [Renamed columns](#renamed-columns)).
- **NULL.** A `NULL` in a nullable property (`int?`, `string?`), or in a reference-type property in code with nullable annotations off, gives `null`. In a property the type declares non-nullable (`int`, `bool`, `DateTime`, an enum, or a `string` with nullable annotations on) it leaves the initializer, else the default, and nothing throws: declare the property `int?` when you need to tell NULL from 0. See [Types and NULL](mapping.md#types-and-null).
- **Null parameters.** Both convert a null parameter value to `DBNull`.
- **Mapping errors.** Dapper wraps them in a `DataException` ("Error parsing column ..."). The extensions throw a `ValueMappingException` (also a `DataException`) that names the property and the column.

### One row

<div class="side-by-side">
<div>

**Dapper**

[!code-csharp[](../samples/ConnectionPattern.cs#dapper_first_call)]

</div>
<div>

**Open.Database.Extensions (factory)**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_first_call)]

</div>
</div>

Lines you write per call: Dapper 3, Open.Database.Extensions 3 (plus the one-time setup).

What is different:

- **No rows, or several.** Dapper's `QueryFirstOrDefault` returns `default` for no rows and the first row for several. `QuerySingleOrDefault` returns `default` for none and throws for more than one. `QueryFirst` and `QuerySingle` throw when there is no row. The extensions' `FirstOrDefault()` returns `null` for no rows and the first row for several; `SingleOrDefault()` returns `null` for none and throws `InvalidOperationException` for more than one. There is no throwing `First` or `Single`.

### A scalar

<div class="side-by-side">
<div>

**Dapper**

[!code-csharp[](../samples/ConnectionPattern.cs#dapper_scalar_call)]

</div>
<div>

**Open.Database.Extensions (factory)**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_scalar_call)]

</div>
</div>

Lines you write per call: Dapper 3, Open.Database.Extensions 3 (plus the one-time setup).

What is different:

- **The value's type.** Dapper converts the provider's value to `T` where it can (it uses `Convert.ChangeType`) and returns `default` for `NULL`. The extensions' `ExecuteScalar<T>()` also returns `default` for `NULL` or no rows, and otherwise casts the value exactly as the provider returns it, so `T` must be that type: SQLite returns `count(*)` as `long` and SQL Server as `int`, and any other `T` throws `InvalidCastException`. To convert, pass a function: `ExecuteScalar(value => ...)`.

### A command with no result set

<div class="side-by-side">
<div>

**Dapper**

[!code-csharp[](../samples/ConnectionPattern.cs#dapper_nonquery_call)]

</div>
<div>

**Open.Database.Extensions (factory)**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_nonquery_call)]

</div>
</div>

Lines you write per call: Dapper 3, Open.Database.Extensions 3 (plus the one-time setup).

What is different:

- **The result.** Both return the number of rows affected, as an `int`.

### A transaction

Dapper opens a closed connection for plain queries and closes it again afterwards. ADO.NET requires an open connection to begin a transaction, so any code that begins one opens the connection first. Dapper does not begin, commit or roll back transactions: its methods take an `IDbTransaction` that you created from an open connection.

<div class="side-by-side">
<div>

**Dapper**

[!code-csharp[](../samples/ConnectionPattern.cs#dapper_transaction_call)]

</div>
<div>

**Open.Database.Extensions (factory)**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_transaction_call)]

</div>
</div>

Lines you write per call: Dapper 14, Open.Database.Extensions 6 (plus the one-time setup).

What is different:

- **Who opens the connection.** Dapper: you, with `Open()`, because `BeginTransaction()` needs it. Microsoft's `SqlConnection.BeginTransaction` examples open the connection first, and Microsoft.Data.Sqlite reports that the call "can only be called when the connection is open". The extensions' `ExecuteTransaction` opens the connection if it is closed.
- **Begin, commit, roll back.** Dapper: you write all three, usually with a `try`/`catch`, and pass the transaction to every call. The extensions begin the transaction, run your action, commit when it returns without throwing, and roll back (and rethrow) when it throws. `ExecuteTransactionConditional` commits only when your action returns `true`; otherwise it rolls back.
- **A connection that was already open.** The extensions close only a connection they opened; an open connection stays open.
- **Where the helpers live.** They are extension methods on `DbConnection`, with async forms (`ExecuteTransactionAsync`, `ExecuteTransactionConditionalAsync`), an optional isolation level and a cancellation token. With a factory, call them inside `factory.Using(connection => ...)` (see [foundation](foundation.md)).

### A connection you own

If you would rather own the connection, the extensions work on a connection the way Dapper does:

<div class="side-by-side">
<div>

**Dapper**

[!code-csharp[](../samples/ConnectionPattern.cs#dapper_list_call)]

</div>
<div>

**Open.Database.Extensions (your connection)**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_list_own)]

</div>
</div>

Lines you write per call: Dapper 4, Open.Database.Extensions 4.

What is different:

- **Nothing about the connection.** You create it and dispose it in both. Each opens a closed connection for the call and closes only what it opened; neither needs `Open()`. The extensions never dispose a connection you gave them.
- **Concurrency.** A command built from one connection throws `InvalidOperationException` if two terminals run at once.

### The total

Across the list, one row, scalar and no-result-set calls above, Dapper takes 13 lines and the extensions 12, plus one line of one-time setup. The counts are close: the difference is what the lines are. None of the extensions' per-call lines creates, owns or disposes a connection, so there is nothing to forget, to leak or to keep open too long, and connection handling lives in one place instead of at every call site.

## Who owns what

| | Dapper | Extensions with a factory | Extensions with your connection |
|---|---|---|---|
| Creates the connection | You, at each call site | The factory, for each execution | You |
| Needs the connection string and provider type in scope | At each call site | Once, where the factory is created | You, where you create it |
| Opens it | Dapper, if it is closed | The command, if it is closed | The command, only if it is closed |
| Closes it | Dapper, if it opened it | The command disposes it | The command, only if it opened it |
| Disposes it | You | The command | You. Never the command |

## Registering the factory once

`IDbConnectionFactory<TConnection>` is the abstraction, `DbConnectionFactory<TConnection>` wraps any `Func<TConnection>`, and `SqlConnectionFactory` (in the MSSqlClient package) takes a connection string. The library has no dependency-injection extension of its own: register the factory with whatever container you use. With Microsoft.Extensions.DependencyInjection:

[!code-csharp[](../samples/ConnectionPattern.cs#di_registration)]

Consumers take the factory as a constructor parameter and never see a connection string:

[!code-csharp[](../samples/ConnectionPattern.cs#di_consumer)]

For SQL Server, register `SqlConnectionFactory` as `IDbConnectionFactory<SqlConnection>`:

[!code-csharp[](../samples/ConnectionPattern.cs#sql_server_registration)]

A singleton is fine because the factory holds no connection: `Create()` returns a new connection object each time.

## Renamed columns

Dapper can match underscored names (`MatchNamesWithUnderscores`) or you can alias the columns in SQL. The extensions ignore underscores when matching, so `first_name` already feeds `FirstName` with no map. For a column that differs by more than case and underscores, declare a `RowMap<T>` once (see [Columns named differently](mapping.md#columns-named-differently)):

[!code-csharp[](../samples/Mapping.cs#map_declared)]

and pass it to `Map<T>`:

[!code-csharp[](../samples/ConnectionPattern.cs#ode_renamed_columns)]

## Positional records

A positional record works with both: Dapper binds its constructor, and so does `Map<T>()`. Both read this record:

[!code-csharp[](../samples/Models.cs#record_post_model)]

<div class="side-by-side">
<div>

**Dapper**

[!code-csharp[](../samples/DapperComparison.cs#dapper_record)]

</div>
<div>

**Open.Database.Extensions**

[!code-csharp[](../samples/DapperComparison.cs#ode_record)]

</div>
</div>

What is different:

- **Choosing the constructor.** Dapper picks a constructor by the columns of the query. The extensions use the one public constructor, or the public parameterless one when the type has it, whatever the columns. A type with two or more public constructors and none without parameters throws `InvalidOperationException` at the call: make all but one non-public, or write a `Map(selector)`.
- **A parameter with no column, and NULL.** A parameter no column feeds takes its declared default value, else `default`, and a `NULL` into a non-nullable parameter takes it too (see [A missing column and a NULL](mapping.md#a-missing-column-and-a-null)).
- **`required` and `init`.** The extensions set `init` properties after the constructor and enforce `required` ones: a result with no column for a `required` property throws `ValueMappingException` at its first row (see [`required` and `init` members](mapping.md#required-and-init-members)).

## Async and cancellation

Dapper's `QueryAsync<T>` returns a `Task<IEnumerable<T>>`, and a cancellation token travels in a `CommandDefinition`. The extensions' terminals have `Async` twins that take a `CancellationToken` and return a `ValueTask`, for example `ToArrayAsync(cancellationToken)`; a token set on the command with `UseCancellationToken(token)` is used together with it, and either one cancels the call.

## Where Dapper does more

Dapper has features this library does not offer:

- **Choosing a constructor by the columns.** Dapper picks the constructor that fits the columns of the query. The extensions use the one public constructor, whatever the columns (see [Positional records](#positional-records)).
- **Multi-mapping and `QueryMultiple`.** Dapper maps several types from one row and reads several result sets from one call.
- **Type handlers and custom type maps.** `SqlMapper.AddTypeHandler` and `SqlMapper.SetTypeMap`.
- **Dynamic results.** `Query(sql)` without a type returns dynamic rows.
- **Parameter bags and output parameters.** An anonymous object, `new { id = 1 }`, becomes `AddParams(new { id = 1 })` (see [Name a parameter from its argument](expressive-commands.md#name-a-parameter-from-its-argument)). `DynamicParameters` has no equivalent: add each value with `AddParam`. Output parameters can be sent, but their values cannot yet be read back, so a procedure that returns values through them stays on Dapper for now.

With the extensions, use `Map(selector)` or `ExecuteReader(reader => ...)` for those jobs. For dynamic rows, the reader's `AsEnumerable()` (see [Reader and command helpers](foundation.md#reader-and-command-helpers)) gives each row as an `object[]`.

## Migrating a call site

1. Create the connection factory once and register or inject it.
2. Replace `using var connection = ...` and `connection.Query<T>(sql, new { ... })` with `connectionFactory.Command(sql).AddParam(...).Map<T>().ToList()` (or `ToArray()`); an anonymous parameter object becomes `AddParams(new { ... })`.
3. A positional record or a class with one public constructor needs nothing. If a model has two or more public constructors and none without parameters, make all but one non-public, or write a `Map(selector)`.
4. If a column name differs from its property name by more than case and underscores, declare a `RowMap<T>` (see [Columns named differently](mapping.md#columns-named-differently)). Names that differ only by case or underscores need nothing.
