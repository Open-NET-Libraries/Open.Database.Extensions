# What's new in 11

> [!NOTE]
> This is `11.0.0-rc.10`, a release candidate. The API is settled, apart from the removal listed first below under "Changes since rc.7". Please try it and report what you find.

Version 11 is a performance release and a tidy-up release. Result mapping is rewritten around a cached, typed row reader, column mapping is declared once, NULL and error handling follow simple rules, and stored-procedure parameters can be named from the arguments you already have. See [Performance](performance.md) for what the speed-up does and does not buy you. Coming from 10.x? Read "Upgrading from 10.x" below first.

## Start here

This is for anyone moving to 11, from 10.x or from an earlier 11 release candidate, whether a person or a coding agent. Read "Start here", then "Upgrading from 10.x", then "Deprecated in 11.0"; the "Changes since" sections are history.

### The new way, in one example

A command, a parameter named from its argument, a typed map, and an async terminal with a token:

```cs
const int author = 42;

List<Post> posts = await connectionFactory
	.Command("select * from posts where author = @author")
	.AddParam(author) // the name "author" comes from the variable
	.Map<Post>()
	.ToListAsync(cancellationToken);
```

When a column is named differently, declare the map once and pass it to every query:

```cs
public static RowMap<Post> PostMap => field ??= [
	new(nameof(Post.Id), "post_id"),
	new(nameof(Post.Author), "author_id"),
];

List<Post> posts = await connectionFactory
	.Command("select * from posts where author_id = @author")
	.AddParam(author)
	.Map(PostMap)
	.ToListAsync(cancellationToken);
```

### What is new, and how to use it

- **`Map<T>()` and its terminals.** `command.Map<Post>().ToListAsync(cancellationToken)`; `ToArrayAsync`, `ToImmutableArrayAsync`, `ToHashSetAsync`, `ToDictionaryAsync`, `FirstOrDefaultAsync` and `SingleOrDefaultAsync` work the same way, and `Skip` and `Take` come before the terminal. It is better because the terminals read straight from the reader through a mapping that is compiled once and cached, with no boxing on the common path. In the library's in-memory benchmark, 100 or more rows into a list take 0.36 to 0.42 times Dapper's time; against SQL Server the database dominates and the times are within the noise. See [Performance](performance.md).
- **`RowMap<T>`, declared once.** `.Map(PostMap)` (shown above); `RowMap<T>.From(static (int id, string title) => new PostSummary(id, title))` builds one from a delegate, and `map.Convert<long, string>(nameof(Order.OrderNum), static n => n.ToString("D6"))` converts a column on its way to a member. It is better because a map built at the call site costs about 0.43 ms and 29.5 KB for 12 properties each time, and a declared one is built once. With no map, a column matches a property exactly, else ignoring case, else ignoring case and underscores (`first_name` fills `FirstName`). See [Mapping rows to objects](mapping.md).
- **Parameter names from the arguments.** `.AddParam(author)`, `.AddParamIf(condition, author)` and `.AddParams(search)` take the name from the variable or member chain and match it to the stored procedure's real name: exactly, ignoring case, ignoring case and underscores, then without a prefix such as `p_` or `in_`. `AddParams` sends only the members the procedure or the `@name` markers declare. It is better because you do not spell a name twice, and a name you pass yourself, `AddParam("name", value)`, always wins. See [Parameters](expressive-commands.md#name-a-parameter-from-its-argument).
- **Later result sets are read; `.SingleResult()` opts out.** `command.SingleResult().Map<Post>().ToListAsync(cancellationToken)` reads only the first set. It is better because a failure in a later statement is thrown, not lost. See [Results, and a failure after the first one](mapping.md#results-and-a-failure-after-the-first-one).
- **Row reads are synchronous by default; `AsyncReads()` opts in.** `command.AsyncReads().Map<Post>()` reads rows with `ReadAsync()`. The command still executes, and the connection still opens, asynchronously. It is better because a `Read()` costs less CPU, and about 40% less memory on the `Map(Func)` path, for the same wall time over a real network link. See [AsyncReads()](async-reads.md).
- **`First` and `Single` throw on no rows.** `await command.Map<long>().SingleAsync(cancellationToken)` (or `command.SingleAsync<long>(cancellationToken)`) throws `InvalidOperationException` for no row, and `Single` also for more than one; `FirstOrDefaultAsync` and `SingleOrDefaultAsync` return `default` instead. It is better because an insert that returns its new id needs no handler to check the row count. See [Terminals](mapping.md#terminals).
- **`ExecuteScalar<T>()` converts.** `await command.ExecuteScalarAsync<int>(cancellationToken)` converts a `bigint` to `int` with a range check, the way `Map<T>()` does, where 10.x threw `InvalidCastException`. NULL and no row give `default(T)`.
- **A typed NULL parameter.** `AddParam("@id", (long?)null)` is sent with the database type of its overload, so SQL Server sees the type you meant. It is better because a NULL with no type could change the parameter type and the query plan.
- **Buffers that release the connection early.** `Map<Post>().ToAsyncBuffer(clearOnDispose, cancellationToken)` returns a `PooledBuffer<Post>`; `await buffer.Items` gives a read-only `PooledArray<Post>`; dispose the buffer to return the arrays to the pool. `ToAsyncBufferStream` (chunks) and `ToBufferedAsyncStream` (elements) are lazy streams with the same overloads. It is better because a background producer reads the query and the connection is released as soon as the last row is read, however slowly the consumer works. See [Async buffers and streams](async-buffers.md).
- **NULL rules.** A nullable member gets `null`; a non-nullable value type keeps its initializer, else its default; a constructor parameter gets its declared default. Nothing throws, and a NULL never overwrites a member that the C# type declares non-nullable. See [NULL in a non-nullable column](mapping.md#null-in-a-non-nullable-column).
- **`ValueMappingException`.** A value that cannot be mapped throws it, a `DataException` that names the property, the column and the target type; it replaces `InvalidOperationException` for that case. Provider exceptions are not wrapped.
- **Constructor binding, `required` and `init`.** Positional records and classes with one public constructor are built through it, and a `required` property that no column feeds throws on the first row. See [Records and constructors](mapping.md#records-and-constructors).
- **Packages.** Use `Open.Database.Extensions.MSSqlClient` (on `Microsoft.Data.SqlClient`) for SQL Server. The `SqlClient` package, on the older `System.Data.SqlClient`, is deprecated, and every type in it has its twin in `MSSqlClient`.

### Instead of ... use ...

The old calls still work in 11 with a compile warning (CS0618) and are removed in 12. Each warning names the new call.

| Instead of | Use |
|---|---|
| `Results<T>(IExecuteReader)` | `.Map<T>().ToListAsync(cancellationToken)` |
| `ExpressiveDbCommand.ResultsAsync<T>` | `.Map<T>().ToListAsync(cancellationToken)` |
| `FirstOrDefault(selector)` on an `IExecuteReader` | `.Map(selector).FirstOrDefaultAsync(cancellationToken)` |
| `cmd.Retrieve().DequeueAs<T>()` | `cmd.Map<T>().ToListAsync(cancellationToken)` |
| `ToListAsync(selector)` on a command | `.Map(selector).ToListAsync(cancellationToken)` |
| Types of `Open.Database.Extensions.SqlClient` | The same types in `Open.Database.Extensions.MSSqlClient` |

The rest, with what changes in each, is in [Deprecated in 11.0](#deprecated-in-110).

> [!WARNING]
> Streaming members (`Read`, `ReadAsync`, `ForEach` and the like) hold the connection while your code runs for each row, so they are not the recommended way to read rows. Buffer with `ToListAsync`, or use the buffer and stream terminals above. See [Streaming holds the connection](mapping.md#streaming-holds-the-connection).

## Changes since rc.9

- **`ToListAsync`, `ToArrayAsync`, `ToImmutableArrayAsync`, `ToHashSetAsync` and `ToDictionaryAsync` build an exactly sized result, once.** A list or array used to grow while the reader was open (doubling, so a 10,000-row list kept room for 16,384). The rows now go into a chain of pooled chunks on the calling flow, with no background producer, and the result is built right after the last row: one block copy for a list, an array or an immutable array, and a hash set or dictionary created at the size of the result, so it never regrows. `ToImmutableArrayAsync` wraps the array without a copy on .NET 8 and later. The synchronous `ToHashSet` and `ToDictionary` work the same way. In the library's synthetic benchmark, 10,000 rows into `ToListAsync` take 206 µs, against 522 µs in rc.9; with a 50 µs network link every route is within 3%, so expect less time on the CPU and less garbage, not a faster query. Bytes per call against SQL Server LocalDB are at most 16 B above rc.9 for 1 and 100 rows, and 1,320 B below it for a 100-row list. One change you can see: a duplicate or null key in `ToDictionary` and `ToDictionaryAsync` throws the same exception as before, but only after every row has been read, and the key and value selectors run after the read. New: `PooledArray<T>.ToArray()` and `ToList()` copy a buffer's rows into an exactly sized array or list.

- **Derived parameter names cost no more than explicit ones when the SQL text is built at run time.** The `@name` markers of a text command were remembered per string object, so a text assembled on every call was read again on every call: 680 B more per call for one parameter, 1,360 B for four. Texts of up to 4,096 characters are now also remembered by their content (up to 1,024 of them), and a rebuilt text costs within 16 B of naming the parameters yourself. A literal or kept text was never affected; longer texts behave as before. No API changes.

- **An unreadable procedure is looked up once in 15 seconds, not once per run, and a stale list no longer outlives an empty answer.** When the provider reported no parameter names for a stored procedure (the login cannot see it, it does not exist, or it declares no parameters), nothing was remembered, so every run read the names again: one extra schema query per execute while the connection was open. And when such an answer came after an older list, the older list stayed for the full five minutes, so a procedure that had lost its parameters was still sent the old ones. An empty answer now replaces the older list and is remembered for 15 seconds (a fraction of the five minutes for known names, so a procedure that is created or changed shows up soon). Derived names are sent as written until then. No API changes.

- **A parameter added to a stored procedure is no longer silently left out of an `AddParams` call.** `AddParams(new { Id, Title })` against a procedure that declared only `@Id` remembered those names, and when the procedure later gained `@Title` (inside the five minutes the names are remembered), the run dropped `Title` with no error and the procedure used its default. A member that the remembered names do not declare now makes the library read the names once more before it drops the member, as an unmatched `AddParam(value)` argument already did. If the second read still lacks the member it is dropped as before, and that read is remembered, so a member the procedure never takes costs one extra read per five minutes, not one per run (a procedure that declares no parameters: every 15 seconds, see above). No signatures change. See [Remembered names](expressive-commands.md#remembered-names).

- **A mapped read no longer boxes a date, a decimal or a bool that the provider hands over as another type.** SQLite stores a date and a decimal as text and a bool as an integer, so the column type differs from the member type, and each such cell cost a box on top of the value: 24 bytes for a `DateTime`, 32 for a `decimal`, 48 for a `bool`, per row (the nullable members too). The map now reads them through a typed conversion with the same culture, the same failures and the same NULL rules as before, and allocates what the hand-written read does. No signatures change.

- **Docs.** "Start here" at the top of this page: what is new, how to use it, and what to use instead. New articles: [Shaping rows](shaping-rows.md) and [How little code](how-little-code.md) (the same four tasks with ODX, Dapper, EF Core and ADO.NET). The streaming members are documented as not the recommended way to read rows, because they hold the connection for every row. [Async buffers and streams](async-buffers.md) covers who owns a `PooledBuffer` and for how long, including returning one from a controller. [Mapping](mapping.md) shows how to get an immutable or frozen result from the terminals.

## Changes since rc.8

- **A NULL passed to a typed `AddParam` overload is now sent with that type.** `AddParam("@id", (long?)null)`, `AddParam("@x", default(int?))` and `AddParam("@s", (string?)null)` (and the same forms of `AddParamIf`) used to send the NULL with no database type, so the provider chose its own default. A consumer reported that as a different parameter type, an implicit conversion and a different query plan on SQL Server. The parameter now takes the database type of the overload's static type, the same mapping `AddParams` already used for a NULL member. A database type you pass always wins, and a value is left to the provider as before. Two things to know: a bare `AddParam("@a", null)` literal binds the `string?` overload, so it is now sent as a `String` NULL; and a value of type `object`, or a call that gives the direction by position (`AddParam(name, value, ParameterDirection.Input)`), has no type to use, so pass a database type or write `direction: ...`. No signatures change.

## Changes since rc.7

- **New: `ToAsyncBuffer`, `ToAsyncBufferStream` and `ToBufferedAsyncStream` on the typed views** (what `Map<T>()` returns, with `Skip` and `Take`). Each reads the query on its own background producer into pooled chunks, so the connection is released as soon as the last row is read, however slowly the consumer works. `ToAsyncBuffer` starts the read at the call and returns a `PooledBuffer<T>` that you can hand to another layer (a controller, say) and read later: `await buffer.Items` gives a `PooledArray<T>`, a read-only list with an O(1) indexer that LINQ copies quickly. Disposing the buffer returns the arrays to the pool, and cancels the query if it is still running. `ToAsyncBufferStream` is a lazy, single-use `IAsyncEnumerable<ReadOnlyMemory<T>>` of chunks, and `ToBufferedAsyncStream` is the same stream element by element. Each comes in three overloads: with a `clearOnDispose` or `clearOnReturn` flag (the shared pool; the choice is yours), with your own `MemoryPool<T>`, or both. Nothing is cleared unless you ask. See [Async buffers and streams](async-buffers.md).
- **Clearing is done off the read path, and bounded.** When you ask for cleared buffers, the clear runs on one background loop. If that loop ever has 10,240 buffers waiting, something is wrong: further buffers are not cleared and not returned to the pool, they are left to the garbage collector. A custom pool must tolerate an owner that is never disposed (one holding native memory or a fixed number of slots would lose it for good), and its `Dispose` runs on that one loop. Nothing reports a discard.
- **Removed: `ToChannelReader` and `ToChannel` on the typed views** (what `Map<T>()` returns, with `Skip` and `Take`): 48 signatures in all. A call to one no longer compiles. They are planned to come back in the Channel library. To read a query while a consumer works, use `ToBufferedAsyncStream(clearOnReturn)` (the elements) or `ToAsyncBufferStream(clearOnReturn)` (the chunks): the query is read on a background producer and the connection is released as soon as the last row is read. For a projection after the map, use `System.Linq.Async` over the stream. The row count that `ToChannel(out Task<long> readCount, ...)` returned has no replacement: count the rows as you read them, or read `Items.Count` of a buffer. See [Channels and Dataflow](channels-and-dataflow.md). The `ToChannel` overloads of the Channel library, on a command or a reader, are not affected.

## Changes since rc.6

- **A cancel that cuts a reader-level read short now throws `OperationCanceledException`.** `ForEach`, `IterateWhile`, `Select` and `SelectAsync` on a reader, their async forms, and the reader-level `ToChannel` overloads that take a transform used to end quietly by default and return a partial result that looked complete. They now check the token at the top of the loop, before each read, and the async forms hand it to `ReadAsync` and `NextResultAsync`, so the provider cancels the read itself. A read that already returned its last row is complete: a cancel after that is not an error. **Removed:** the `throwOnCancellation` parameter on `ForEach`, `IterateWhile`, `Select` and `SelectAsync`, six signatures in all. A call that passed it no longer compiles (CS1503 or CS1501): delete the argument, as in `Select(reader, transform, cancellationToken)`. To stop early without an exception, return `false` from the predicate or leave the loop. A mapped query over a plain `IDataReader` now forwards the token, so a cancel between rows throws.
- **`AddParam` and `AddParamIf` take an `object?` value** in the `(name, value, type, direction)` forms, so a typed NULL such as `AddParam(name, (byte[]?)null, SqlDbType.VarBinary)` compiles without a nullability warning and sends `DBNull`. No existing call changes.
- **A stored procedure's parameter names are resolved on every run.** A command that gets its names from the database (`AddParam(value)` with no name) used to resolve them once and write them into `Params`, so a second run of the same command object, after a change of connection, database or procedure, could send the first run's names. The names are now resolved on every run into a list of their own, and `Params` keeps what you added. No API changes. One visible change: after a run, `Params` shows what you added, not the resolved names. To see what was sent, read the provider command's `Parameters` in `Execute(command => ...)`.
- **Documentation:** a new [AsyncReads()](async-reads.md) page says why rows are read with `Read()` by default, what the opt-in costs and when to use it. The [Performance](performance.md) page now leads with ranked charts, with their data tables and CSVs in [Performance data](performance-data.md), and the same charts are on a [benchmark charts](../benchmarks/ranking.html) page. The Core README has a section on trimming and native AOT, with a recipe for a trimmed app (see "Known limitations"). The examples in the README, the mapping article, the samples and the XML documentation use async terminals with a real `CancellationToken`, `static` lambdas and one pair per line.

## Changes since rc.5

- **Rows are read with `Read()` by default, on async terminals too.** `ToListAsync`, `FirstOrDefaultAsync` and the other async terminals still execute the command and open the connection asynchronously, but they read the rows with `Read()`. On the measured runs a synchronous row read costs less CPU, and about 40% less memory on the `Map(Func)` path, for the same wall time over a real network link. To read the rows with `ReadAsync()`, for a caller whose thread must not block while rows arrive, call `AsyncReads()`: `cmd.AsyncReads().Map<T>()`, or `dbCommand.Map<T>().AsyncReads()`. **Removed, with no bridge:** `EnableAsyncRead(bool)` and the `UseAsyncRead` property. Replace `EnableAsyncRead()` with `AsyncReads()`. A plain `IDataReader` read by an async terminal now checks the cancellation token on every row.
- **`RowMap<T>.From(factory, pairs)` builds a map from a delegate**, such as a lambda, a method group or a constructor wrapper. Its parameters bind to columns by the rules a constructor already uses: a pair renames a parameter to a column, then columns match by name, ignoring case. A missing or `NULL` value in a non-nullable parameter gets the parameter's declared default, else `default`. Declare the map once:

  ```cs
  static readonly RowMap<PostSummary> Summary = RowMap<PostSummary>.From(
  	static (int id, string title) => new PostSummary(id, title));

  List<PostSummary> posts = await cmd
  	.Map(Summary)
  	.ToListAsync(cancellationToken);
  ```

  On C# 12 the pairs argument must be written out. A single target-typed `new(...)` as the only pair does not compile: write the element type.
- **The `Map<T>(pair...)` overloads are removed.** There is one way to pass pairs: a `RowMap<T>`, declared once and passed to `Map`. A one-off is `Map(new RowMap<T>(pair, pair))`. `Map<T>([...])`, with a collection expression, still compiles: it converts to a `RowMap<T>`. One behavior change comes with it, with no compile error: `Map<int>([])` now throws (item 15 of "Upgrading from 10.x").
- **`Select(selector)` on an `IMappedQuery<T>`, added in rc.5, is removed:** it built a second object for every row while the reader was open. Shape each row into the result type with `Map(RowMap<TResult>.From(factory))` instead. A data class can return that mapped query and let each caller choose the terminal. The reader-level `Select` is unchanged.
- **Fix:** the async terminals on an `IMappedQuery<T>` that this library did not build (your own implementation) no longer resume on the caller's synchronization context.
- **Fix:** an `AddParams` member that the procedure, or the command text, does not declare is no longer sent under a stale name left in a shared pool.
- A package built in a Release build no longer carries the build machine's folder names: the SDK's `DeterministicSourcePaths` maps the repository folder to a neutral prefix in the dll, the debug symbols and the Source Link mapping.

## Changes since rc.4

- Version 11 targets .NET 11: every package adds `net11.0` next to `net10.0` (LTS), `netstandard2.0` and `netstandard2.1` (`MSSqlClient`: `net472`, `net10.0` and `net11.0`). Each major version of the library targets the matching .NET. Building the repository needs the .NET 11 SDK (`global.json`).
- `.SingleResult()` can be written before `Map`, so a query reads as behavior first, then shape: `command.SingleResult().Map<Post>().ToListAsync(cancellationToken)`. It works on an expressive command (`IExecuteReader`) and on a raw `IDbCommand` or `DbCommand`, with the same `Map` overloads as before, and it is the same as `command.Map<Post>().SingleResult()`, which still works. There is none on `IDataReader` or `DataTable`: their views never read through, so there is nothing to opt out of. Nothing changes by default: without it a `Map` terminal still reads through the later result sets.
- **`ExecuteScalar<T>()` and `ExecuteScalarAsync<T>()` convert the way `Map<T>()` does.** They cast the provider's value exactly, so a `bigint` read as `int`, or a `numeric` read as `long`, threw `InvalidCastException`, which a port from Dapper met only on a real database. Now a value that is already a `T` is returned as it is, an integer converts to another integer type with a range check, and any other value is converted by the same code `Map<T>()` uses. `NULL` and no row still give `default(T)`. A value that cannot be converted, or does not fit, throws `ValueMappingException` with the conversion's error inside. **Behavior change:** a call that threw `InvalidCastException` now converts, or throws that exception. `ExecuteReturn<T>()` and `ExecuteReturnAsync<T>()` convert the same way. The overloads that take a `Func<object?, T>` are unchanged.
- A `RowMap<T>` takes a converter for a column that needs a trim, a parse or a format on its way to a member: `map.Convert<long, string>(nameof(Order.OrderNum), n => n.ToString("D6"))`, or `Convert<TColumn, TMember>(member, column, convert)` to name the column as well. It returns a new map, and the delegate runs inside the compiled row reader, so a result maps straight into the type you want, with no row type to copy from. A converter is not called for a NULL cell (the member follows the NULL rules); a `Nullable<>` `TColumn` is called with null. See [Converting a column](mapping.md#converting-a-column).
- `First()` and `Single()` (and `FirstAsync(cancellationToken)`, `SingleAsync(cancellationToken)`) on `Map<T>()` throw `InvalidOperationException` when the query returns no row, and `Single` also when it returns more than one. An insert that returns its new id is one `await command.Map<long>().SingleAsync(cancellationToken)`, with no handler to check the row count. The commands have the shortcuts too: `command.SingleAsync<long>(cancellationToken)`. A `NULL` value is still a row. The error for no rows is thrown after the connection is released and the later result sets are read, so a failure in one of them comes first. See [Terminals](mapping.md#terminals).
- `ToDictionary` and `ToDictionaryAsync` on `Map<T>()` take a value selector: `ToDictionaryAsync(post => post.Id, post => post.Body ?? "")` builds the lookup in one pass, without keeping the rows. A duplicate key throws, as it does without the value selector.

## Breaking in rc.4

Rebuild consumers of rc.3. Both changes are source-compatible for every call except the ones noted.

- `ExpressiveDbCommand.RetrieveAsync` takes a trailing `CancellationToken cancellationToken = default` on the three overloads that can: `RetrieveAsync(CancellationToken)`, `RetrieveAsync(IEnumerable<int>, CancellationToken)` and `RetrieveAsync(IEnumerable<string>, bool, CancellationToken)`. The token is used together with the command's own token. A `params` list must come last, so the two `params` overloads (`RetrieveAsync(int, params IEnumerable<int>)` and `RetrieveAsync(string, params IEnumerable<string>)`) are unchanged and take no token: pass the ordinals or names as a collection to the `IEnumerable` overload to cancel. A call that passed no token binds to the same overload as before, but the signatures are new, so rebuild.
- Three parameters are renamed to say what they hold. This breaks only a caller that passes the argument by name: `CreateTextCommand(SqlTransaction, ...)` in `SqlClient` and `MSSqlClient` takes `commandText` (it was `procedureName`), `CreateStoredProcedureCommand(IDbConnection, ...)` in Core takes `procedureName` (it was `commandText`), and the type parameter `TConn` of the `Using` and `UsingAsync` connection helpers is now `TConnection`. The type-parameter rename breaks no caller.

## Changes since rc.3

- The asynchronous terminals on a command take the element type: `await command.ToArrayAsync<int>(cancellationToken)` is `Map<int>()` then the terminal. `Map<T>()` of a single value such as `int` reads the first column.
- `reader.Map<T>()` and `command.Map<T>()` map a plain `IDataReader` or `IDbCommand` with the same terminals as an expressive command's `Map<T>()`. The reader is never opened, closed, disposed or advanced to the next result set; a command opens and closes only the connection it opened. See [Mapping a reader or a command you already have](mapping.md#mapping-a-reader-or-a-command-you-already-have).
- `table.Map<T>()` maps a DataTable you already have (a `MappedTable<T>`; each terminal reads through its own DataTableReader).
- A null `RowMap<T>` passed to `Map<T>(map)` no longer throws `ArgumentNullException`: it maps exactly as `Map<T>()` does (a single value reads the first column).
- A type of your own that implements both `IExecuteReader` and `IDbCommand` (or `IDataReader`) now gets an ambiguous `Map` call (CS0121); cast it to the interface you mean.
- A `default` `MappedQuery<T>` (or `MappedQueryWithSkip<T>`, `MappedQueryWithTake<T>`) now throws `InvalidOperationException` when used, where it threw `NullReferenceException`.
- Members with a better route are marked `[Obsolete]`. Each message names the replacement, and they are removed in 12. They are warnings, never errors, but a build that treats warnings as errors stops on them until you move to the replacement or suppress `CS0618`. See [Deprecated in 11.0](#deprecated-in-110).
- The asynchronous command paths dispose their reader with `ConfigureAwait(false)` on `netstandard2.1`, `net10.0` and `net11.0`.
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
var people = await cmd.ResultsAsync<Person>(
	cancellationToken,
	("FirstName", "first_name"),
	("LastName", "last_name"));

// 11
public static RowMap<Person> PersonMap => field ??= [
	new(nameof(Person.FirstName), "first_name"),
	new(nameof(Person.LastName), "last_name"),
];
var people = await cmd.Map<Person>(PersonMap).ToListAsync(cancellationToken);
```

To keep a dictionary, build the map from it: `new RowMap<T>(dictionary)`. A map built at the call site builds a new map on every call (about 0.43 ms and 29.5 KB for 12 properties): declare a map for anything that runs more than occasionally.

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
15. **`Map<int>([])` throws.** An empty collection expression is a `RowMap<int>` now (the removed `Map<T>(params pairs)` read it as no pairs, so it read the first column), and an `int` is not a row model: it fails with `InvalidOperationException` at run time, not at compile time. Use `Map<int>()`.
16. **Async terminals read the rows with `Read()`.** The command still executes, and the connection still opens, asynchronously. Call `AsyncReads()` before or after `Map` to read the rows with `ReadAsync()`: see "Changes since rc.5" above.
17. **A cancelled reader-level read throws.** `ForEach`, `IterateWhile`, `Select`, `SelectAsync` and their async forms used to stop quietly and return the rows read so far; they now throw `OperationCanceledException`. The `throwOnCancellation` parameter is removed: delete the argument. See "Changes since rc.6" above.
18. **After a run, `Params` shows what you added.** A command that names its stored-procedure parameters from the database no longer rewrites `Params` to the resolved names; read `Execute(command => ...)` to see what was sent. See "Changes since rc.6" above.

## Deprecated in 11.0

These are obsolete in 11.0 (compile warning CS0618) and will be removed in 12. Each warning names the call to use instead. The old call still works until then.

| Old call | New call | Behaviour difference |
|---|---|---|
| `Results<T>(IExecuteReader)` | `.Map<T>().ToList()`; with a map `.Map(map).ToList()` | Later result sets are read, so a later failure throws (`.SingleResult()` keeps the old behaviour). Returns a `List<T>`. |
| `ExpressiveDbCommand.ResultsAsync<T>` | `.Map<T>().ToListAsync()` (token optional: `.ToListAsync(cancellationToken)`); with a map `.Map(map).ToListAsync()` | The same as above. |
| `ResultsFromDataTable<T>` (reader, command) | `using var table = command.ToDataTable(); table.Map(map).ToList()` | The same rows. The old call cleared the table as rows were read; clear or dispose it yourself. A type that cannot be mapped is reported after the data is loaded, not before the command runs: read `RowMap<T>.Default` first to fail early. |
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
| `To<T>(DataTable, map, clearSourceTable)` and `Transformer<T>.Results(DataTable, bool)` | `table.Map(map).ToList()` | Returns a `List<T>`, not a lazy enumerable. There is no `clearSourceTable`: clear the table yourself after reading it. |
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

The old calls read only the first result set. `Map` reads through the later ones, so a failure in a later result set is thrown, not left unread. See [Results, and a failure after the first one](mapping.md#results-and-a-failure-after-the-first-one). Call `.SingleResult()` before `Map` to read only the first, as the old calls did: `command.SingleResult().Map<Post>().ToListAsync(cancellationToken)`. `command.Map<Post>().SingleResult()` still works.

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

- The library is not marked trimmable and does not support native AOT in 11.0. A trimmed app must keep its model types and the provider's reader type: the recipe is in the [Core README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Source/Core/README.md#trimming-and-native-aot).
- `Retrieve()` followed by `DequeueAs<T>()` is deprecated in 11.0 (a two-pass route, slower than `Map<T>()`): use `Map<T>().ToList()`. See [Deprecated in 11.0](#deprecated-in-110).
- The unmapped `ToChannel` overloads can still lose the row that was in flight when a read fails while the target is full. The cause is in the underlying channel library.
- Output parameters can be sent, but their values cannot be read back after the call. Providers other than SQL Server supply no real parameter names, so `AddParam(search.Author)` uses the last member as written there.
- A missing column for a `required` property is reported by `Map` at the first row, so an empty result does not report it. `DataTable.To<T>()` (deprecated), `ToChannel` and the Dataflow methods know the columns before any row and report it even when no row follows. (`DequeueAs<T>()` did too; it is deprecated.)
- The synchronous `ToTargetBlock` overloads post each row without waiting: when the target is complete, or bounded and full, or when the internal 1,024-row buffer of the mapped overloads is full, reading stops without an error, and only the number the call returns (the rows read and handed over) shows the shortfall. Use the `Async` overload to feed a bounded target.
- `AddParams(source)` sends every public member of `source` that the procedure declares, so pass an object you trust, and add a value that only the server may decide (a tenant, a deleted-rows flag) with `AddParam("name", value)`: a name you pass yourself always wins.
- The unit tests run on in-memory providers. The documentation samples run on SQLite, and the benchmarks on the in-memory provider, SQLite and SQL Server LocalDB. Feedback from other providers and workloads is welcome.
- The tests of the mapper, the pipelines and the commands run on net10.0 and net11.0. The netstandard2.0, netstandard2.1 and net472 builds share the code through `#if` switches; a small test project runs the `AddParam` overloads on net472, net8.0 and net10.0 under older language versions.

Earlier versions are in [Earlier release notes](release-notes.md).
