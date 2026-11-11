# What's new in 11

> [!NOTE]
> This is `11.0.0-rc.3`, a release candidate. The API is settled. Please try it and report what you find.

Version 11 is a performance release and a tidy-up release. Result mapping is rewritten around a cached, typed row reader, column mapping is declared once, NULL and error handling follow simple rules, and stored-procedure parameters can be named from the arguments you already have. See [Performance](performance.md) for what the speed-up does and does not buy you. Coming from 10.x? Read "Upgrading from 10.x" below first.

## Breaking in rc.4

Rebuild consumers of rc.3. Both changes are source-compatible for every call except the ones noted.

- `ExpressiveDbCommand.RetrieveAsync` takes a trailing `CancellationToken cancellationToken = default` on the three overloads that can: `RetrieveAsync(CancellationToken)`, `RetrieveAsync(IEnumerable<int>, CancellationToken)` and `RetrieveAsync(IEnumerable<string>, bool, CancellationToken)`. The token is used together with the command's own token. A `params` list must come last, so the two `params` overloads (`RetrieveAsync(int, params IEnumerable<int>)` and `RetrieveAsync(string, params IEnumerable<string>)`) are unchanged and take no token: pass the ordinals or names as a collection to the `IEnumerable` overload to cancel. A call that passed no token binds to the same overload as before, but the signatures are new, so rebuild.
- Three parameters are renamed to say what they hold. This breaks only a caller that passes the argument by name: `CreateTextCommand(SqlTransaction, ...)` in `SqlClient` and `MSSqlClient` takes `commandText` (it was `procedureName`), `CreateStoredProcedureCommand(IDbConnection, ...)` in Core takes `procedureName` (it was `commandText`), and the type parameter `TConn` of the `Using` and `UsingAsync` connection helpers is now `TConnection`. The type-parameter rename breaks no caller.

## Changes since rc.3

- `reader.Map<T>()` and `command.Map<T>()` map a plain `IDataReader` or `IDbCommand` with the same terminals as an expressive command's `Map<T>()`. The reader is never opened, closed, disposed or advanced to the next result set; a command opens and closes only the connection it opened. See [Mapping a reader or a command you already have](mapping.md#mapping-a-reader-or-a-command-you-already-have).
- A null `RowMap<T>` passed to `Map<T>(map)` no longer throws `ArgumentNullException`: it maps exactly as `Map<T>()` does (a single value reads the first column).
- A type of your own that implements both `IExecuteReader` and `IDbCommand` (or `IDataReader`) now gets an ambiguous `Map` call (CS0121); cast it to the interface you mean.
- A `default` `MappedQuery<T>` (or `MappedQueryWithSkip<T>`, `MappedQueryWithTake<T>`) now throws `InvalidOperationException` when used, where it threw `NullReferenceException`.
- The `Map<T>` documentation now says how columns match by default: exactly, else ignoring case, else ignoring case and underscores (`author_id` feeds `AuthorId`).

## Changes since rc.2

- A `Map` terminal now reads through the later result sets after the first, so a failure in a later statement is thrown, where rc.2 returned the rows and lost it. This is a behavior change from rc.2. `Map` still returns only the first set's rows. `.SingleResult()` after `Map` stops after the first set and does not see a later failure. A later failure can follow a write that already committed, so a caller that retries can repeat it. The older command methods, Channel and Dataflow do not read through yet.
- `ToDataTable` now reads one result set and leaves the reader on it, so `NextResult` works after it. A second `ToDataTable` call in a row returns no rows, because the reader is still on that set and its rows are already read. A loop on `while (!reader.IsClosed)` around `ToDataTable` no longer ends: use `NextResult`. `ToDataTables` returns one table per result set.
- A connection the library opened is closed when the provider fails to start a reader, in Core and in the two SQL Server packages.
- `EnsureOpenAsync` and `OpenAsync` always use `ConfigureAwait(false)`. The overloads with a `bool configureAwait` are obsolete and ignore it.
- A null collection is treated as empty, and a null reader or selector is checked once.

## Changes since rc.1

- Generic commands (`ExpressiveCommand`, `ExpressiveDbCommand`) send each parameter's direction, so output parameters work.
- `AddParam(name, DbType, direction)` works on the SQL Server commands.
- `AddParams(x)` with `x` typed `object` sends the members of its real type.
- `AddParam(value)` finds a stored procedure's parameter names through the `ProcedureParameters` schema on any `DbConnection`.
- On a text command, `AddParam(value)` and `AddParams(object)` use the `@name` markers in the SQL.
- `Map<T>()` on a dictionary or `ExpandoObject` throws, where it used to return empty rows.

## Highlights

- **Faster, allocation-light mapping.** The mapping for a result-set shape is compiled once and cached, and values are read with typed getters, so nothing is boxed on the common path. Before, every `Map<T>()` compiled a setter per property (about 0.94 ms per query in the library's benchmark). In that benchmark (in-memory provider, 12-column rows, Dapper 2.1.89), reading 100 or more rows into a list or array takes 0.36 to 0.42 times Dapper's time (2.4 to 2.8 times faster) and 0.33 to 0.37 times its allocations (`Benchmarks/RESULTS.md`, re-measured on the release candidate; a positional record costs the same). Against SQL Server LocalDB the database dominates: times are 0.92 to 1.04 times Dapper's in that benchmark (0.89 to 1.08 in the re-run), within the noise, and allocations 0.55 to 0.6 times on multi-row results.
- **`RowMap<T>`: declare a column mapping once.** `public static RowMap<Post> PostMap => field ??= [new(nameof(Post.Id), "post_id"), ...];`, passed as the `map` argument of every method that maps rows to a model. It takes one pair, several or none, and a read through it allocates what `Map<T>()` allocates. With no map, a column matches a property exactly, else ignoring case, else ignoring case and underscores (`first_name` fills `FirstName`). Only public instance properties are mapped. See [Columns named differently](mapping.md#columns-named-differently) and [snake_case columns map to PascalCase properties](mapping.md#snake_case-columns-map-to-pascalcase-properties).
- **`MappedQuery<T>`: direct read loops, and one token for the whole call.** `Map<T>()` returns a deferred query. `ToList`, `ToArray`, `ToImmutableArray`, `ToHashSet`, `ToDictionary`, `FirstOrDefault`, `SingleOrDefault`, `Read` and their async twins read straight from the reader; `Skip` advances without mapping and `Take` maps no more rows once it is met. They are instance methods, so the query struct is not boxed; the `IMappedQuery<T>` extensions remain for generic code. The token passed to an async terminal is used together with the command's `UseCancellationToken` token, and either one cancels the whole call: opening the connection, looking up parameter names, executing and reading. The command's own `Async` terminals and the Channel and Dataflow `IExecuteReader` overloads take the token the same way. Readers open with `SequentialAccess` (`Map(selector)` skips it, because a selector may read columns in any order); `.SingleResult()` adds `SingleResult`. See [Terminals](expressive-commands.md#terminals).
- **Commands take the type, asynchronously.** `ToListAsync<T>()`, `ToArrayAsync<T>()`, `ToImmutableArrayAsync<T>()`, `FirstOrDefaultAsync<T>()`, `SingleOrDefaultAsync<T>()` and `ToHashSetAsync<T>()` on a command are `Map<T>()` then the terminal (there is no synchronous shortcut: write `Map<T>().ToList()`): `await db.Command(sql).ToArrayAsync<int>(ct)`. A single value such as `int` reads the first column, converted as a property of that type would be; `Map<int>()` no longer throws. See [One value per row](mapping.md#one-value-per-row).
- **Less overhead around the query.** `IExecuteReader` has state-passing `ExecuteReader<TState, TResult>` overloads, so a `static` lambda allocates no closure. A command created from a connection allocates no pool wrapper, `Params` is created on first use, and async execution skips awaiting when the connection is already open.
- **Constructor binding, `required` and `init`.** Positional records, record structs and classes with one public constructor are built through it, and the `init` and settable properties it does not cover are set; a parameter is fed like a property. A `required` property is enforced: when no column feeds it, the first row of that shape throws `ValueMappingException`. `where T : new()` is gone from every method that maps rows, so a type that cannot be mapped (an interface, an abstract class, a delegate, a class with no public constructor, two or more public constructors and none without parameters (use `Map(selector)`), or a `required` public field) throws `InvalidOperationException` at the call. A single value such as `int` is not a row model but is read from the first column by `Map<int>()` and `ToListAsync<int>()`. See [Records and constructors](mapping.md#records-and-constructors).
- **Clear NULL rules.** A nullable member (`int?`, `string?`), a reference type in code with nullable annotations off, and a member declared as a type parameter get `null`. A non-nullable value type, or a non-nullable reference with nullable annotations on, keeps its initializer, else its default; a constructor parameter gets its declared default (`int views = 3` gives 3), else `default`, also when no column feeds it. Nothing throws. `ExecuteScalar<T>()`, `ExecuteReturn<T>()` and `FirstOrdinalResults<T>` give `default(T)` for NULL. A single value read by `Map<T>()` or `ToListAsync<T>()` follows the same rule: `default(T)` for a non-nullable value type, `null` for `int?`, `string` and `byte[]`. See [NULL in a non-nullable column](mapping.md#null-in-a-non-nullable-column).
- **`ValueMappingException`.** A value that cannot be mapped (an unconvertible value, an overflow) throws it: a `DataException` that names the property, the column and the target type. Provider exceptions (`DbException`) are not wrapped.
- **Stored procedures without spelling every name twice.** `AddParam(author)` and `AddParamIf(condition, author)` take the parameter name from the argument (a variable or a chain such as `search.Author`) and match it to the procedure's real parameter name: exactly, ignoring case, ignoring case and underscores (`TitleContains` finds `@title_contains`), then without a common prefix (`p_`, `in_`, `i_`, `prm_`, `par_`, `v_`); `m_author`, `s_author` and `_author` also try `author`. `AddParams(source)` sends the public properties and fields of `source` that the procedure declares. A name you pass yourself, `AddParam("name", value)`, always wins. A NULL is sent with the type of its argument. SQL Server is supported in both SQL Server packages. See [Parameters](expressive-commands.md#name-a-parameter-from-its-argument).
- **Five packages.** `Open.Database.Extensions.Core` (any ADO.NET provider), `MSSqlClient` (SQL Server on `Microsoft.Data.SqlClient`), `Channel`, `Dataflow`, and `SqlClient` (the older `System.Data.SqlClient`, **deprecated in favor of MSSqlClient**). Each README opens with its banner and has the same table of the five.

## Upgrading from 10.x

**Rebuild every consumer: 11.0 is not binary compatible with 10.x.** Against 10.1.0, the Core public API has 46 signatures removed or changed and 175 added. Every removal is one of two things:

- A mapping overload that took key/value pairs or `(string Field, string? Column)` tuples (44 signatures). Each method now takes one `RowMap<T>? map` in the same position, optional (`null` reads by column name) except on the mapped `ToChannel`, `ToChannelAsync`, `ToTargetBlock`, `ToTargetBlockAsync`, `AsSourceBlock` and `AsSourceBlockAsync`, where you pass a map or `null`. A caller that passed it by name (`fieldMappingOverrides:`) must rename the argument `map`.
- The `ParameterDirection direction = Input` default of `AddParam(string, ParameterDirection)` and `AddParamIf(bool, string, ParameterDirection)` (2 signatures).

The common case, and its fix: declare the map once and pass it.

```cs
// 10.x
var people = cmd.Results<Person>(new("FirstName", "first_name"), new("LastName", "last_name"));

// 11
public static RowMap<Person> PersonMap => field ??= [new(nameof(Person.FirstName), "first_name"), new(nameof(Person.LastName), "last_name")];
var people = cmd.Map<Person>(PersonMap).ToList();
```

To keep a dictionary, build the map from it: `new RowMap<T>(dictionary)`. Only `Map<T>` also takes the pairs themselves, but that builds a new map on every call (about 0.43 ms and 29.5 KB for 12 properties): declare a map for anything that runs more than occasionally.

These can change behavior without a compile error, most likely first:

1. **`AddParam(parameterName)` with a string variable.** In 10.x it added a parameter with that name and no value. In 11 it compiles and adds a parameter named `parameterName` whose value is the string. A string literal, `AddParam("name")`, throws `ArgumentException` at run time, as does a compiler older than C# 10. `AddParamIf` is the same. For a parameter with no value pass its direction, `AddParam("name", ParameterDirection.Output)`; to name a parameter yourself, `AddParam("name", value)`.
2. **`ValueMappingException` replaces `InvalidOperationException`** for a value that cannot be mapped. It carries `PropertyName`, `ColumnName` and `TargetType`, with the conversion or provider error inside. Catch it or `DataException`.
3. **NULL rules.** In 10.x a NULL into a non-nullable value-type property threw, and a reference-type property always got `null`. Now a NULL never overwrites a member the C# type declares non-nullable: `string Name { get; set; } = "";` keeps `""`. Declare `string?` to get `null`.
4. **Columns match properties ignoring underscores.** `q_type` now feeds `QType`, so a column that was ignored can fill a property. To opt a property out, give it a `null` column in a declared `RowMap<T>`.
5. **A `RowMap<T>` is checked when it is created.** These throw `ArgumentException`: a property listed twice (the last used to win), an empty column name, two properties on one column, and a property that does not exist or is static (it was `KeyNotFoundException`, or ignored with a `null` column).
6. **The scalar transform overloads receive `null`, not `DBNull`, for a NULL:** `ExecuteScalar<T>(Func<object?, T>)` and its two async forms. A function that tested `o is DBNull` must test `o is null`. The raw `ExecuteScalar()` still returns `DBNull.Value`.
7. **`IExecuteReader` has three new members:** two state-passing overloads and an `ExecuteReaderAsync` that takes the call's token. A custom implementation must add them; the `ExpressiveCommand*` classes already do.
8. **`RowMap<T>.Default` is a property,** not a field; reading it throws `InvalidOperationException` for a type that cannot be mapped.
9. **`Map<T>()` of a single value such as `int` reads the first column** (10.x returned `default(T)` for every row). Other types that cannot be mapped (an interface, an abstract class, ...) throw at the call, even for an empty result.
10. **A struct with one declared public constructor is built through it.** A `record struct` gets equal results; a struct whose constructor does more than assign now runs it. A constructor parameter no column feeds gets its declared default, replacing the plain default of before.
11. **Channels and Dataflow.** On the mapped overloads a mapping failure faults the target when `complete` is `true`; the target is never completed or faulted before the reader is released; `ArgumentNullException.ParamName` for a null target is `"target"`; a cancelled mapped async channel call succeeds only if the reader had already reported the end of the results, else it throws `OperationCanceledException`.
12. **Async calls pass the command's `CancellationToken` to the connection open.**
13. **`EnsureOpenAsync` and `OpenAsync` have an obsolete `bool configureAwait` overload.** The library always awaits with `ConfigureAwait(false)`; returning to your context is your call. The `bool` overloads still compile with a warning and ignore the argument.
14. **Smaller items.** A static property is no longer mapped. `ResultsBuffered*` and `FirstOrdinalResults<T>` still buffer into a queue and return a dequeuing enumerable.

## Deprecated in 11.0

These are obsolete in 11.0 (compile warning CS0618) and will be removed in 12. Each warning names the call to use instead. The old call still works until then.

| Old call | New call | Behaviour difference |
|---|---|---|
| `Results<T>(IExecuteReader)` | `.Map<T>().ToList()`; with a map `.Map(map).ToList()` | Later result sets are read, so a later failure throws (`.SingleResult()` keeps the old behaviour). Returns a `List<T>`. |
| `ExpressiveDbCommand.ResultsAsync<T>` | `.Map<T>().ToListAsync()` (token optional: `.ToListAsync(cancellationToken)`); with a map `.Map(map).ToListAsync()` | The same as above. |
| `ResultsFromDataTable<T>` (reader, command) | `using var table = command.ToDataTable(); using var tableReader = table.CreateDataReader(); tableReader.Map(map).ToList()` | The same rows. The old call cleared the table as rows were read; clear or dispose it yourself. A type that cannot be mapped is reported after the data is loaded, not before the command runs: read `RowMap<T>.Default` first to fail early. |
| `FirstOrDefault(selector)`, `SingleOrDefault(selector)` on `IExecuteReader` | `.Map(selector).FirstOrDefault()`, `.Map(selector).SingleOrDefault()` | Later result sets are read (`.SingleResult()` keeps the old behaviour). |
| `Take(n, selector)`, `Skip(n, selector)`, `SkipThenTake(s, t, selector)` on `IExecuteReader` | `.Map(selector).Take(n).ToList()`, `.Map(selector).Skip(n).ToList()`, `.Map(selector).Skip(s).Take(t).ToList()` | The selector is not run on skipped rows, so a selector that throws on one no longer fails the call. `Take(0)` now runs the command. Later result sets are read. |
| `ExpressiveDbCommand.TakeAsync(selector, n)`, `ExpressiveDbCommand.ToListAsync(selector)` | `.Map(selector).Take(n).ToListAsync(cancellationToken)`, `.Map(selector).ToListAsync(cancellationToken)` | Later result sets are read. Returns a `List<T>`. `Take(0)` now runs the command. `Map` has no `CommandBehavior` argument: if you passed one, call `IterateReaderAsync(handler, behavior)` or `IterateReaderWhileAsync(handler, behavior)` yourself. |
| `QueryResultCollection<T>`, `QueryResultCollection<T, TResult>` | None | Nothing in the library returns it. Use the `List<T>` from `Map<T>().ToList()`, or your own class. |
| `cmd.Retrieve().DequeueAs<T>()` (`QueryResultExtensions.DequeueAs<T>`, 5 overloads) | `cmd.Map<T>().ToList()`; with a map `cmd.Map(map).ToList()`. Async: `(await cmd.RetrieveAsync()).DequeueAs<T>()` becomes `await cmd.Map<T>().ToListAsync()`. On an `IDbCommand`: `command.Results<T>()`. On an `IDataReader`: `reader.ResultsBuffered<T>()`. On a `DbCommand`, async: `await command.ResultsAsync<T>()`. | `Map` reads in one pass instead of copying raw rows first. A missing `required` column is reported at the first row, so not for an empty result. Later result sets are read (`.SingleResult()` keeps the first only, as `Retrieve` did). `DequeueAs<T>` returned a one-pass enumerable; `ToList()` returns a `List<T>`. `Results<T>(IDbCommand)` takes no `CommandBehavior`, where `Retrieve(IDbCommand, behavior)` did. |
| `QueryResultExtensions.AsMappedDictionaries` | `cmd.Retrieve().DequeueAsMappedDictionaries()`; async `(await cmd.RetrieveAsync()).DequeueAsMappedDictionaries()` | Nothing in the library produces the input it takes. `DequeueAsMappedDictionaries` drains the queue as it is enumerated, so it can be read once, where `AsMappedDictionaries` could be read again. |
| `ToList(transform)`, `ToArray(transform)`, `ToImmutableArray(transform)` on an `IDataReader`; `ToListAsync(transform)` on a `DbDataReader` | `reader.Map(transform).ToList()`, `.ToArray()`, `.ToImmutableArray()`; `await reader.Map(transform).ToListAsync(cancellationToken)` | The same rows, from the current result set; the asynchronous form reads them with `Read()` where `ToListAsync` used `ReadAsync`. The synchronous `ToList` took a `CancellationToken` that `Map` does not: use `reader.Select(transform, cancellationToken).ToList()`. `Select` and `SelectAsync` are not deprecated. |
| `ToList(transform, behavior)`, `ToArray(transform, behavior)`, `ToImmutableArray(transform, behavior)` on an `IExecuteReader` | `.Map(transform).ToList()`, `.Map(transform).ToArray()`, `.Map(transform).ToImmutableArray()` | Later result sets are read (`.SingleResult()` keeps the old behaviour). `Map` has no `CommandBehavior` argument: if you passed one, call `cmd.ExecuteReader(r => r.Map(transform).ToList(), behavior \| CommandBehavior.SingleResult)`. |
| `IterateReader<TEntity, TResult>(transform, selector, behavior)` on an `IExecuteReader` | `.Map(transform).Read(selector)` | Later result sets are read (`.SingleResult()` keeps the old behaviour). If you passed a `CommandBehavior`, call `cmd.ExecuteReader(r => selector(r.Select(transform)), behavior \| CommandBehavior.SingleResult)`. |
| `ToListAsync(transform)` on a `DbCommand` (three overloads, with a synchronous transform) | `await command.Map(transform).ToListAsync(cancellationToken)` | Later result sets are read (`.SingleResult()` keeps the old behaviour). `Map` executes asynchronously and then reads the rows with `Read()`, as `useReadAsync: false` did. It has no `CommandBehavior` argument: if you passed one, call `command.ExecuteReaderAsync(r => r.Map(transform).ToList(), behavior \| CommandBehavior.SingleResult, cancellationToken)`. The `IDbCommand` and `DbCommand` selector methods that are not listed here are not deprecated. |
| `To<T>(DataTable, map, clearSourceTable)` and `Transformer<T>.Results(DataTable, bool)` | `table.CreateDataReader().Map(map).ToList()` | Returns a `List<T>`, not a lazy enumerable. There is no `clearSourceTable`: clear the table yourself after reading it. |
| `MappedQueryExtensions.ReadAsync` with the `CancellationToken` first (four overloads) | `query.ReadAsync(handler, cancellationToken)` | The same call with the token last. |
| Every public type of the `Open.Database.Extensions.SqlClient` package | The same type in `Open.Database.Extensions.MSSqlClient` | The same API on `Microsoft.Data.SqlClient`. |
| `AddParameterType(this IDbCommand, string, SqlDbType)` (both SQL packages) | Call `AddParameterType` on the `SqlCommand` | The overload casts any `IDbCommand` to `SqlCommand`, so it throws `InvalidCastException` for any other command. |
| `ExpressiveCommandBase.Concat<T>` (protected) | `remaining is null ? [first] : remaining.Prepend(first)` | `Prepend` alone throws on null. Only a subclass that calls it is affected. |

### Before and after

Each pair below is compiled and run by the documentation build, and a check asserts that the two give the same result.

`Results<T>` on a command:

[!code-csharp[](../samples/Deprecated.cs#deprecated_results_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_results_after)]

`ResultsAsync<T>` on an expressive command:

[!code-csharp[](../samples/Deprecated.cs#deprecated_resultsasync_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_resultsasync_after)]

`ResultsFromDataTable<T>`:

[!code-csharp[](../samples/Deprecated.cs#deprecated_resultsfromdatatable_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_resultsfromdatatable_after)]

The selector forms on an `IExecuteReader`:

[!code-csharp[](../samples/Deprecated.cs#deprecated_selector_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_selector_after)]

`TakeAsync` and `ToListAsync` with a selector:

[!code-csharp[](../samples/Deprecated.cs#deprecated_takeasync_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_takeasync_after)]

`QueryResultCollection<T>` has no replacement; a list holds the same rows:

[!code-csharp[](../samples/Deprecated.cs#deprecated_collection_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_collection_after)]

`AsMappedDictionaries`:

[!code-csharp[](../samples/Deprecated.cs#deprecated_mappeddictionaries_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_mappeddictionaries_after)]

`Retrieve().DequeueAs<T>()`:

[!code-csharp[](../samples/Deprecated.cs#deprecated_dequeueas_before)]
[!code-csharp[](../samples/Deprecated.cs#deprecated_dequeueas_after)]

### Why `Map` reads later result sets

The old calls read only the first result set. `Map` reads through the later ones, so a failure in a later result set is thrown, not left unread. See [Results, and a failure after the first one](mapping.md#results-and-a-failure-after-the-first-one). Add `.SingleResult()` to read only the first, as the old calls did.

## Fixes

- **`ExecuteReturn<T>()` and `ExecuteReturnAsync<T>()`** return `default(T)` for a NULL return value instead of throwing `NullReferenceException` for a value type.
- **Channels and Dataflow.** With `complete: false`, the command no longer runs twice. The mapped `ToChannel<T>`, `ToChannelAsync<T>` and `ToTargetBlock<T>` overloads complete your target only after every row has been delivered and the reader released (before, up to about 1,024 buffered rows could be lost with a bounded target). They no longer lose the last row when the reader fails mid-stream, and they map correctly when the reader has extra unmapped columns.
- **Connections and commands.** `OpenAsync(IDbConnection)` no longer opens a `DbConnection` twice. `EnsureOpenAsync` closes a connection that finished opening just as cancellation arrived. `FirstOrdinalResults<T>(IDbCommand, behavior)` honors `behavior`.
- **The synchronization context.** The async transaction helpers and the `ExecuteReaderAsync` overloads of the two SQL Server packages no longer resume on the caller's synchronization context: they await with `ConfigureAwait(false)`, and so does the connection open they trigger. A caller that blocked on one from a thread with a synchronization context no longer waits on that context inside the library.
- **A parameter with no value is now sent by both SQL Server packages.** It was never added to the command, so an output parameter (`AddParam(name, direction)`) never reached SQL Server. SQL Server needs a type and size for an output parameter and refuses the command without them.
- **`AddParam("name", someString)` swapped the name and the value on a compiler older than C# 13** (the .NET 8 SDK). It was found in `11.0.0-preview05` and fixed in `11.0.0-preview06`: `AddParam(string, string?)` and `AddParamIf(bool, string, string?)` use the name you write on every compiler.
- **`AddParam(name, value, dbType)` compiles again.** In the 11.0 previews a call that gave a value and a database type, such as `AddParam("@id", id, SqlDbType.Int)`, failed with an ambiguous-call error (CS0121) for an `int`, `long`, `short`, `byte` or `string` value (the `int` forms compiled in 10.1.0). New overloads with a required database type, on `AddParam` and `AddParamIf`, make every such call bind to one overload; no existing member changed.
- **A `char` is sent as the character again.** In the 11.0 previews, `AddParam("x", 'c')` bound to the `int` overload and sent the number 99; 10.1.0 sent the character. It does again.

## Known limitations

- `Retrieve()` followed by `DequeueAs<T>()` is deprecated in 11.0 (a two-pass route, slower than `Map<T>()`): use `Map<T>().ToList()`. See [Deprecated in 11.0](#deprecated-in-110).
- The unmapped `ToChannel` overloads can still lose the row that was in flight when a read fails while the target is full. The cause is in the underlying channel library.
- Output parameters can be sent, but their values cannot be read back after the call. Providers other than SQL Server supply no real parameter names, so `AddParam(search.Author)` uses the last member as written there.
- `MappedQuery<T>` has `FirstOrDefault` and `SingleOrDefault` but no throwing `First` or `Single`: check the result for `null`.
- A missing column for a `required` property is reported by `Map` at the first row, so an empty result does not report it. `DataTable.To<T>()` (deprecated), `ToChannel` and the Dataflow methods know the columns before any row and report it even when no row follows. (`DequeueAs<T>()` did too; it is deprecated.)
- The synchronous `ToTargetBlock` overloads post each row without waiting: when the target is complete, or bounded and full, or when the internal 1,024-row buffer of the mapped overloads is full, reading stops without an error, and only the number the call returns (the rows read and handed over) shows the shortfall. Use the `Async` overload to feed a bounded target.
- `AddParams(source)` sends every public member of `source` that the procedure declares, so pass an object you trust, and add a value that only the server may decide (a tenant, a deleted-rows flag) with `AddParam("name", value)`: a name you pass yourself always wins.
- The unit tests run on in-memory providers. The documentation samples run on SQLite, and the benchmarks on the in-memory provider, SQLite and SQL Server LocalDB. Feedback from other providers and workloads is welcome.
- The tests of the mapper, the pipelines and the commands run on net10.0. The netstandard2.0, netstandard2.1 and net472 builds share the code through `#if` switches; a small test project runs the `AddParam` overloads on net472, net8.0 and net10.0 under older language versions.

Earlier versions are in [Earlier release notes](release-notes.md).
