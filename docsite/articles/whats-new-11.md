# What's new in 11

> [!NOTE]
> This is `11.0.0-rc.1`, a release candidate. The API is settled. Please try it and report what you find.

Version 11 is a performance release and a tidy-up release. Result mapping is rewritten around a cached, typed row reader, column mapping is declared once, NULL and error handling follow simple rules, and stored-procedure parameters can be named from the arguments you already have. See [Performance](performance.md) for what the speed-up does and does not buy you. Coming from 10.x? Read "Upgrading from 10.x" below first.

## Highlights

- **Faster, allocation-light mapping.** The mapping for a result-set shape is compiled once and cached, and values are read with typed getters, so nothing is boxed. Before, every `Map<T>()` compiled a setter per property (about 0.94 ms per query in the library's benchmark). In that benchmark (in-memory provider, 12-column rows, Dapper 2.1.89), reading 100 or more rows into a list, array or stream takes 0.29 to 0.32 times Dapper's time and 0.33 to 0.37 times its allocations. Against SQL Server LocalDB the database dominates: times are within about 5% of Dapper's, allocations 0.55 to 0.6 times on multi-row results.
- **`RowMap<T>`: declare a column mapping once.** `public static RowMap<Post> PostMap => field ??= [new(nameof(Post.Id), "post_id"), ...];`, passed as the `map` argument of every method that maps rows to a model. It takes one pair, several or none, and a read through it allocates what `Map<T>()` allocates. With no map, a column matches a property exactly, else ignoring case, else ignoring case and underscores (`first_name` fills `FirstName`). Only public instance properties are mapped. See [Columns named differently](mapping.md#columns-named-differently) and [snake_case columns map to PascalCase properties](mapping.md#snake_case-columns-map-to-pascalcase-properties).
- **`MappedQuery<T>`: direct read loops, and one token for the whole call.** `Map<T>()` returns a deferred query. `ToList`, `ToArray`, `ToImmutableArray`, `ToHashSet`, `ToDictionary`, `FirstOrDefault`, `SingleOrDefault`, `Read` and their async twins read straight from the reader; `Skip` advances without mapping and `Take` stops reading once it is met. They are instance methods, so the query struct is not boxed; the `IMappedQuery<T>` extensions remain for generic code. The token passed to an async terminal is used together with the command's `UseCancellationToken` token, and either one cancels the whole call: opening the connection, looking up parameter names, executing and reading. Readers open with `SequentialAccess | SingleResult` (`Map(selector)` skips `SequentialAccess`, because a selector may read columns in any order). See [Terminals](expressive-commands.md#terminals).
- **Less overhead around the query.** `IExecuteReader` has state-passing `ExecuteReader<TState, TResult>` overloads, so a `static` lambda allocates no closure. A command created from a connection allocates no pool wrapper, `Params` is created on first use, and async execution skips awaiting when the connection is already open.
- **Constructor binding, `required` and `init`.** Positional records, record structs and classes with one public constructor are built through it, and the `init` and settable properties it does not cover are set; a parameter is fed like a property. A `required` property is enforced: when no column feeds it, the first row of that shape throws `ValueMappingException`. `where T : new()` is gone from every method that maps rows, so a type that cannot be mapped (an interface, an abstract class, a delegate, a single value such as `int`, a class with no public constructor, two or more public constructors and none without parameters (use `Map(selector)`), or a `required` public field) throws `InvalidOperationException` at the call. See [Records and constructors](mapping.md#records-and-constructors).
- **Clear NULL rules.** A nullable member (`int?`, `string?`), a reference type in code with nullable annotations off, and a member declared as a type parameter get `null`. A non-nullable value type, or a non-nullable reference with nullable annotations on, keeps its initializer, else its default; a constructor parameter gets its declared default (`int views = 3` gives 3), else `default`, also when no column feeds it. Nothing throws. `ExecuteScalar<T>()`, `ExecuteReturn<T>()` and `FirstOrdinalResults<T>` give `default(T)` for NULL. See [NULL in a non-nullable column](mapping.md#null-in-a-non-nullable-column).
- **`ValueMappingException`.** A value that cannot be mapped (an unconvertible value, an overflow) throws it: a `DataException` that names the property, the column and the target type. Provider exceptions (`DbException`) are not wrapped.
- **Stored procedures without spelling every name twice.** `AddParam(author)` and `AddParamIf(condition, author)` take the parameter name from the argument (a variable or a chain such as `search.Author`) and match it to the procedure's real parameter name: exactly, ignoring case, ignoring case and underscores (`TitleContains` finds `@title_contains`), then without a common prefix (`p_`, `in_`, `i_`, `prm_`, `par_`, `v_`); `m_author`, `s_author` and `_author` also try `author`. `AddParams(source)` sends the public properties and fields of `source` that the procedure declares. A name you pass yourself, `AddParam("name", value)`, always wins. A NULL is sent with the type of its argument. SQL Server is supported in both SQL Server packages. See [Parameters](expressive-commands.md#name-a-parameter-from-its-argument).
- **Five packages.** `Open.Database.Extensions.Core` (any ADO.NET provider), `MSSqlClient` (SQL Server on `Microsoft.Data.SqlClient`), `Channel`, `Dataflow`, and `SqlClient` (the older `System.Data.SqlClient`, **deprecated in favor of MSSqlClient**). Each README opens with its banner and has the same table of the five.

## Upgrading from 10.x

**Rebuild every consumer: 11.0 is not binary compatible with 10.x.** Against 10.1.0, the Core public API has 46 signatures removed or changed and 163 added. Every removal is one of two things:

- A mapping overload that took key/value pairs or `(string Field, string? Column)` tuples (44 signatures). Each method now takes one `RowMap<T>? map` in the same position, optional (`null` reads by column name) except on the mapped `ToChannel`, `ToChannelAsync`, `ToTargetBlock`, `ToTargetBlockAsync`, `AsSourceBlock` and `AsSourceBlockAsync`, where you pass a map or `null`. A caller that passed it by name (`fieldMappingOverrides:`) must rename the argument `map`.
- The `ParameterDirection direction = Input` default of `AddParam(string, ParameterDirection)` and `AddParamIf(bool, string, ParameterDirection)` (2 signatures).

The common case, and its fix: declare the map once and pass it.

```cs
// 10.x
var people = cmd.Results<Person>(new("FirstName", "first_name"), new("LastName", "last_name"));

// 11
public static RowMap<Person> PersonMap => field ??= [new(nameof(Person.FirstName), "first_name"), new(nameof(Person.LastName), "last_name")];
var people = cmd.Results<Person>(PersonMap);
```

To keep a dictionary, build the map from it: `new RowMap<T>(dictionary)`. Only `Map<T>` also takes the pairs themselves, but that builds a new map on every call (about 0.43 ms and 29.5 KB for 12 properties): declare a map for anything that runs more than occasionally.

These can change behavior without a compile error, most likely first:

1. **`AddParam(parameterName)` with a string variable.** In 10.x it added a parameter with that name and no value. In 11 it compiles and adds a parameter named `parameterName` whose value is the string. A string literal, `AddParam("name")`, throws `ArgumentException` at run time, as does a compiler older than C# 10. `AddParamIf` is the same. For a parameter with no value pass its direction, `AddParam("name", ParameterDirection.Output)`; to name a parameter yourself, `AddParam("name", value)`.
2. **`ValueMappingException` replaces `InvalidOperationException`** for a value that cannot be mapped. It carries `PropertyName`, `ColumnName` and `TargetType`, with the conversion or provider error inside. Catch it or `DataException`.
3. **NULL rules.** In 10.x a NULL into a non-nullable value-type property threw `InvalidOperationException`, and a reference-type property always got `null`. Now a NULL never overwrites a member the C# type declares non-nullable: `string Name { get; set; } = "";` keeps `""`. Declare `string?` to get `null`.
4. **Columns match properties ignoring underscores.** `q_type` now feeds `QType`, so a column that was ignored can fill a property. To opt a property out, give it a `null` column in a declared `RowMap<T>`.
5. **A `RowMap<T>` is checked when it is created.** These throw `ArgumentException`: a property listed twice (the last used to win), an empty column name, two properties on one column, and a property that does not exist or is static (it was `KeyNotFoundException`, or ignored with a `null` column).
6. **The scalar transform overloads receive `null`, not `DBNull`, for a NULL:** `ExecuteScalar<T>(Func<object?, T>)` and its two async forms. A function that tested `o is DBNull` must test `o is null`. The raw `ExecuteScalar()` still returns `DBNull.Value`.
7. **`IExecuteReader` has three new members:** two state-passing overloads and an `ExecuteReaderAsync` that takes the call's token. A custom implementation must add them; the `ExpressiveCommand*` classes already do.
8. **`RowMap<T>.Default` is a property,** not a field; reading it throws `InvalidOperationException` for a type that cannot be mapped.
9. **`Map<int>()` and other unmappable types throw at the call,** even for an empty result. They used to return default objects. For one value use `ExecuteScalar<T>()` or `FirstOrdinalResults<T>()`.
10. **A struct with one declared public constructor is built through it.** A `record struct` gets equal results; a struct whose constructor does more than assign now runs it. A constructor parameter no column feeds gets its declared default, replacing the plain default of before.
11. **Channels and Dataflow.** On the mapped overloads a mapping failure faults the target when `complete` is `true`; the target is never completed or faulted before the reader is released; `ArgumentNullException.ParamName` for a null target is `"target"`; a cancelled mapped async channel call succeeds only if the reader had already reported the end of the results, else it throws `OperationCanceledException`.
12. **Async calls pass the command's `CancellationToken` to the connection open.**
13. **Smaller items.** A static property is no longer mapped. `ResultsBuffered*` and `FirstOrdinalResults<T>` still buffer into a queue and return a dequeuing enumerable.

## Fixes

- **`ExecuteReturn<T>()` and `ExecuteReturnAsync<T>()`** return `default(T)` for a NULL return value instead of throwing `NullReferenceException` for a value type.
- **Channels and Dataflow.** With `complete: false`, the command no longer runs twice. The mapped `ToChannel<T>`, `ToChannelAsync<T>` and `ToTargetBlock<T>` overloads complete your target only after every row has been delivered and the reader released (before, up to about 1,024 buffered rows could be lost with a bounded target). They no longer lose the last row when the reader fails mid-stream, and they map correctly when the reader has extra unmapped columns.
- **Connections and commands.** `OpenAsync(IDbConnection)` no longer opens a `DbConnection` twice. `EnsureOpenAsync` closes a connection that finished opening just as cancellation arrived. `FirstOrdinalResults<T>(IDbCommand, behavior)` honors `behavior`.
- **The synchronization context.** The async transaction helpers and the `ExecuteReaderAsync` overloads of the two SQL Server packages no longer resume on the caller's synchronization context: they await with `ConfigureAwait(false)`, and so does the connection open they trigger. A caller that blocked on one from a thread with a synchronization context no longer waits on that context inside the library.
- **A parameter with no value is now sent by both SQL Server packages.** It was never added to the command, so an output parameter (`AddParam(name, direction)`) never reached SQL Server. SQL Server needs a type and size for an output parameter and refuses the command without them.
- **`AddParam("name", someString)` swapped the name and the value on a compiler older than C# 13** (the .NET 8 SDK). It was found in `11.0.0-preview05` and fixed in `11.0.0-preview06`: `AddParam(string, string?)` and `AddParamIf(bool, string, string?)` use the name you write on every compiler.

## Known limitations

- `Retrieve()` followed by `DequeueAs<T>()` is a two-pass route: rows are copied into `object[]` and mapped as they are drained. It is slower and allocates more than a direct `Map<T>()` at larger sizes. For typed results prefer `Map<T>()` or `ResultsBuffered<T>()`.
- The unmapped `ToChannel` overloads can still lose the row that was in flight when a read fails while the target is full. The cause is in the underlying channel library.
- Output parameters can be sent, but their values cannot be read back after the call. Providers other than SQL Server supply no real parameter names, so `AddParam(search.Author)` uses the last member as written there.
- `MappedQuery<T>` has `FirstOrDefault` and `SingleOrDefault` but no throwing `First` or `Single`: check the result for `null`.
- A missing column for a `required` property is reported at the first row, so an empty result does not report it. `DequeueAs<T>()`, `DataTable.To<T>()`, `ToChannel` and the Dataflow methods know the columns before any row and report it even when no row follows.
- Benchmarks and tests cover the in-memory provider, SQLite and SQL Server LocalDB. Feedback from other providers and workloads is welcome.

Earlier versions are in [Earlier release notes](release-notes.md).
