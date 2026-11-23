# Coming from earlier versions

Version 11 is a significant break from 10.x: rebuild every consumer, because 11 is not binary compatible with 10.x. The ADO.NET extensions themselves changed little. How typed results are mapped and returned changed a lot. This page is for a person or a coding agent moving a 10.x codebase to 11, grouped by theme. The rest of the documentation describes version 11 only.

Each member that is gone has a replacement that is shorter, and the old way is not kept beside it. The compiler names the member that is missing; find it here by its theme. For the release candidates one by one, see the [release notes](release-notes.md).

Packages: use `Open.Database.Extensions.MSSqlClient` (on `Microsoft.Data.SqlClient`) for SQL Server. The `Open.Database.Extensions.SqlClient` package (on `System.Data.SqlClient`) is not published for 11; every type in it has its twin in `MSSqlClient`. Version 11 targets `net11.0` next to `net10.0`, `netstandard2.0` and `netstandard2.1` (`MSSqlClient`: `net472`, `net10.0` and `net11.0`).

## Mapping: `Map<T>()`

Every way of turning rows into objects is `Map<T>()` followed by a terminal. The terminals read straight from the reader through a mapping compiled once and cached, so nothing is boxed on the common path.

[!code-csharp[](../samples/Migration.cs#migration_results_after)]

| In 10.x | In 11 |
|---|---|
| `Results<T>()`, `ResultsAsync<T>()`, `ResultsBuffered<T>()` | `.Map<T>().ToListAsync(cancellationToken)` |
| `command.ToListAsync<T>(cancellationToken)` and the other command shortcut terminals (`ToArrayAsync<T>`, `ToImmutableArrayAsync<T>`, `ToHashSetAsync<T>`, `FirstAsync<T>`, `FirstOrDefaultAsync<T>`, `SingleAsync<T>`, `SingleOrDefaultAsync<T>`) | `command.Map<T>().ToListAsync(cancellationToken)` and the sibling terminals |
| `FirstOrDefault(selector)`, `Take(n, selector)`, `Skip`, `SkipThenTake`, `ToList(selector)`, `IterateReader(...)` | `.Map(selector).Skip(s).Take(t).ToListAsync(cancellationToken)` |
| `ResultsFromDataTable<T>()`, `DataTable.To<T>()` | `command.ToDataTable()`, then `table.Map(map).ToList()` |
| `Map<T>(("Name", "column"), ...)` and the other key/value and tuple overloads | a `RowMap<T>` declared once and passed to `Map` |
| `Select` on a mapped query | `Map(RowMap<TResult>.From(factory))`, or LINQ over the list |

A column map is declared once and passed to every query:

```cs
public static RowMap<Person> PersonMap => field ??= [
	new(nameof(Person.FirstName), "first_name"),
	new(nameof(Person.LastName), "last_name"),
];

List<Person> people = await command
	.Map(PersonMap)
	.ToListAsync(cancellationToken);
```

What behaves differently without a compile error, most likely first:

- **Later result sets are read.** A failure in a later statement is thrown, where 10.x returned the rows and lost it. `.SingleResult()` before `Map` reads only the first set.
- **NULL.** A NULL never overwrites a member the C# type declares non-nullable: `string Name { get; set; } = "";` keeps `""`. Declare `string?` to get `null`.
- **`ValueMappingException`** (a `DataException` that names the property, the column and the target type) replaces `InvalidOperationException` for a value that cannot be mapped.
- **Columns match properties ignoring underscores:** `q_type` feeds `QType`. A property is opted out with a `null` column in a declared `RowMap<T>`.
- **A `RowMap<T>` is checked when it is created:** a property listed twice, an empty column name, two properties on one column, or a property that does not exist or is static throws `ArgumentException`.
- **`Map<T>()` of a single value such as `int` reads the first column.** `Map<int>([])` throws: an empty collection expression is a `RowMap<int>`.
- **`ExecuteScalar<T>()` converts** the way `Map<T>()` does (a `bigint` to `int` with a range check) instead of throwing `InvalidCastException`. The `Func<object?, T>` overloads receive `null`, not `DBNull`, for a NULL.
- **`First` and `Single` throw `InvalidOperationException` on no row;** `FirstOrDefault` and `SingleOrDefault` return `default`.
- **A struct with one declared public constructor is built through it,** and a `required` property that no column feeds throws on the first row.
- **A model decorated with `[Column]` or `[NotMapped]`** is read as declared: a declared column that is missing throws `ValueMappingException`.
- **`RowMap<T>.Default` is a property,** and it throws `InvalidOperationException` for a type that cannot be mapped.

See [Mapping rows to objects](mapping.md).

## Rows without a model

`AsObjectArrays()` and `AsDictionaryRows()` are mapped views with the same terminals, `Skip` and `Take`, `SingleResult()`, `AsyncReads()`, `UsePool` and pooling as `Map<T>()`.

[!code-csharp[](../samples/Migration.cs#migration_objectarrays_after)]

[!code-csharp[](../samples/Migration.cs#migration_mappeddictionaries_after)]

| In 10.x | In 11 |
|---|---|
| `Retrieve()`, `RetrieveAsync()` | `AsObjectArrays()`, or `AsDictionaryRows()` when names are wanted |
| `QueryResult<T>`, `DequeueAs<T>()`, `DequeueAsMappedDictionaries()`, `AsMappedDictionaries()` | `AsDictionaryRows()` (`row.ToDictionary()` for a `Dictionary` you own), or `Map<T>()` for typed rows |
| `FirstOrdinalResults<T>()` | `Map<T>()` (`Map<int?>()` tells NULL from 0) |
| `AsEnumerable(reader)`, `GetValues(record, ...)`, `Transformer<T>` | `reader.Map<T>()` or `reader.AsObjectArrays()` |

A database NULL is `null` in a row (it was `DBNull`). A column subset is chosen in SQL.

## Buffered terminals, and `Stream(stallTimeout)`

A buffered terminal (`ToListAsync`, `ToArrayAsync`, `ToImmutableArrayAsync`, `ToHashSetAsync`, `ToDictionaryAsync`, `ToAsyncBuffer`) reads every row, releases the connection, then hands the result over. It is the way to read rows.

`Stream(stallTimeout, cancellationToken)` is for stopping early: it hands rows to your loop while the reader is open, and a consumer that stalls longer than the timeout is stopped.

```cs
// 10.x: handler-based row reads
await command.Map<Post>().ReadAsync(async rows => { /* ... */ });

// 11
await foreach (Post post in command
	.Map<Post>()
	.Stream(TimeSpan.FromSeconds(30), cancellationToken))
{
	/* ... */
}
```

| In 10.x | In 11 |
|---|---|
| `Read`, `ReadAsync`, `ForEachAsync`, `IterateReader*` handlers on a command or mapped query | `Stream(stallTimeout, ct)`, or a buffered terminal |
| `ToAsyncBuffer(pool, clearOnReturn, ct)` and the flag and pool overloads of the buffer streams | `.UsePool(pool, clearOnReturn).ToAsyncBuffer(ct)` |
| `EnableAsyncRead(bool)`, `UseAsyncRead`, the `useReadAsync` argument | `AsyncReads()` before or after `Map` |
| a reader helper's `throwOnCancellation` argument | delete it: a cancel that cuts the read short throws `OperationCanceledException` |

Rows are read with `Read()` by default, on async terminals too; `AsyncReads()` opts in to `ReadAsync()`. The token can be passed positionally after the other optional arguments of every terminal, so `reader.ForEachAsync(handler, default)` binds the token and reads with `ReadAsync`. See [Async buffers and streams](async-buffers.md) and [AsyncReads()](async-reads.md).

## The Synchronous namespace

The blocking calls (`Execute`, `ExecuteNonQuery`, `ExecuteScalar`, `ExecuteReader`, `ToList`, `ToArray`, `First`, `Single`, `LoadTable` and the like on a command, a query definition or a mapped query) are extension methods in `Open.Database.Extensions.Synchronous`. Add `using Open.Database.Extensions.Synchronous;` where the compiler says a blocking call is missing (CS1061, CS1929). The async calls, the builders (`Command`, `StoredProcedure`, `Map`, `Using`, `AsPool`, `Open`) and the reader-level helpers stay where they are.

`Execute<T>(f)` and `ExecuteReader<T>(f)` infer `T`: a call that named `T` alone needs both type arguments or none. A command type of your own that implements `IExecuteReader` or `IExecuteCommand` throws `NotSupportedException` when a blocking call reaches it: use the async call.

## `DbQuery`

A query is defined once, mapped, with no connection; the source and the values are given where it runs. There is nothing to migrate: it is new in 11.

```cs
static readonly MappedDbQuery<Post> ByAuthor = DbQuery
	.Text("select * from posts where author = @author")
	.Map<Post>();

public ValueTask<List<Post>> ByAuthorAsync(int author, CancellationToken cancellationToken)
	=> ByAuthor.Using(connectionFactory).AddParam(author).ToListAsync(cancellationToken);
```

See [Expressive commands](expressive-commands.md).

## Parameters

- **`AddParam(parameterName)` with a string variable** added a parameter with that name and no value in 10.x. In 11 it adds a parameter named `parameterName` whose value is the string. A literal, `AddParam("name")`, throws `ArgumentException`. For a parameter with no value pass its direction, `AddParam("name", ParameterDirection.Output)`.
- **Names from arguments.** `.AddParam(author)` takes the name from the variable and matches it to the stored procedure's real name. `AddParam("name", value)` always wins.
- **A typed NULL** such as `AddParam("@id", (long?)null)` is sent with the database type of its overload.
- **After a run, `Params` holds what you added,** under the names as added. Read the provider command's `Parameters` in `Execute(command => ...)` to see what was sent.
- **The `direction = Input` default** of `AddParam(string, ParameterDirection)` and `AddParamIf` is gone: pass the direction.

See [Parameters](expressive-commands.md#name-a-parameter-from-its-argument).

## Pooling and clearing

Rows that hold references are always cleared before their array goes back to the pool. Rows without references are cleared only when asked: per query with `.UsePool(pool, clearOnReturn)` after `Map`, or for the whole app with the switch `Open.Database.Extensions.ClearPooledArrays`. A chunk of 128 KB or more of rows (rows times row size, value rows included) is cleared off the read path. A read that finds no rows completes `Items` with a shared empty view, which keeps working after the buffer is disposed. See [Async buffers and streams](async-buffers.md#pooled-memory).

## Transactions and connections


`new ...ConnectionFactoryToPoolAdapter(factory)` is not public any more: `factory.AsPool()` returns an `IDbConnectionPool`.
The `ExecuteTransaction*` overloads that took the token right after the connection are gone: pass the token after the callback, `ExecuteTransaction(connection, action, cancellationToken)`. `EnsureOpenAsync` and `OpenAsync` have no `bool configureAwait` overload: the library always awaits with `ConfigureAwait(false)`, and returning to your context is your call. `EnsureOpenScopeAsync` on a connection, a factory or a pool opens the connection if it is needed and lets go of it the way its source calls for. A null argument to a connection extension throws `ArgumentNullException` at the call.

## Channel and Dataflow

Every mapped overload takes one `RowMap<T>? map` where 10.x took key/value pairs or tuples; a caller that passed the argument by name renames it `map`. On the mapped overloads a mapping failure faults the target when `complete` is `true`, and the target is never completed or faulted before the reader is released. The typed `ToChannelReader()` and `ToChannel()` are not on the typed views of Core: use `ToBufferedAsyncStream` or `ToAsyncBufferStream`. See [Channels and Dataflow](channels-and-dataflow.md).

## Removed members

- `Concat<T>` (protected) on `ExpressiveCommandBase`: `remaining is null ? [first] : remaining.Prepend(first)`.
- `AddParameterType(IDbCommand, string, SqlDbType)`: call it on the `SqlCommand`.
- `QueryResultCollection<T>`: a list holds the same rows.
- `reader.AsAsyncEnumerable(n, others...)`: `reader.AsAsyncEnumerable([n, ..others])`.
- `record.GetMatchingOrdinals(names, sort)`: `record.MatchingOrdinals(names, sort).ToArray()`.
- A custom `IExecuteReader` must implement the state-passing `ExecuteReader` overloads and an `ExecuteReaderAsync` that takes the call's token; a custom `IMappedQuery<T>` implements `ToAsyncBufferStream` and `Stream`.
- A static property is not mapped.

## Known limitations

- The library is not marked trimmable and does not support native AOT. A trimmed app keeps its model types and the provider's reader type: the recipe is in the [Core README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Source/Core/README.md#trimming-and-native-aot).
- Output parameters can be sent, but their values cannot be read back after the call. On providers other than SQL Server, which report no procedure parameter names, `AddParam(search.Author)` sends the last member of the expression, `Author`, as the name.
- A missing column for a `required` property is reported by `Map` at the first row, so an empty result does not report it.
- The unmapped `ToChannel` overloads can lose the row that was in flight when a read fails while the target is full. The cause is in the underlying channel library.
- The synchronous `ToTargetBlock` overloads post each row without waiting: when the target is complete, or bounded and full, reading stops without an error, and only the number the call returns (the rows read and handed over) shows the shortfall. Use the `Async` overload to feed a bounded target.
- `AddParams(source)` sends every public member of `source` that the procedure declares, so pass an object you trust, and add a value that only the server may decide with `AddParam("name", value)`.
- The unit tests run on in-memory providers, the documentation samples on SQLite, and the benchmarks on the in-memory provider, SQLite and SQL Server LocalDB. Feedback from other providers and workloads is welcome.
- The tests of the mapper, the pipelines and the commands run on net10.0 and net11.0. The netstandard2.0, netstandard2.1 and net472 builds share the code through `#if` switches; a small test project runs the `AddParam` overloads on net472, net8.0, net10.0 and net11.0 under older language versions.
