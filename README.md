# Open.Database.Extensions

[![NuGet](https://img.shields.io/nuget/v/Open.Database.Extensions.Core.svg?style=flat)](https://www.nuget.org/packages/Open.Database.Extensions.Core/)

Useful set of utilities and abstractions for simplifying modern database operations and ensuring dependency injection compatibility.

## Principles

- Minimize connection open time.
- Deferred/lazy transformation.
- Optimize for specific use cases.
- Minimize boilerplate code.

## Features
- Provides a fluent interface for database operations.
- Supports dependency injection for connection factories.
- Support both synchronous and asynchronous operations.
- Provides expressive commands for executing SQL queries and stored procedures.

### Connection Factories

Connection factories facilitate creation and disposal of connections without the concern of a connection reference or need for awareness of a connection string.
A `SqlConnectionFactory` is provided and can be overridden to provide more specific dependency injection configurations.

### Expressive Commands

The provided expressive command classes allow for an expressive means to append parameters and execute the results without lengthy complicated setup.

Extensions are provided to create commands from connection factories.

### Example

```cs
var result = connectionFactory
   .StoredProcedure("[procedure name]")
   .AddParam("a",1)
   .AddParam("b",true)
   .AddParam("c","hello")
   .ExecuteScalar();
```

## Extensions

Instead of writing this:

```cs
var myResult = new List<T>();
using(var reader = await mySqlCommand.ExecuteReaderAsync(CommandBehavior.CloseConnection))
{
    while(await reader.ReadAsync())
        myResult.Add(transform(reader));
}
```

Is now simplified to this:

```cs
var myResult = await mySqlCommand.ToListAsync(transform);
```

## Deferred Transformation

In order to keep connection open time to a minimum, some methods cache data before closing the connection and then subsequently applying the transformations as needed.

### `Results<T>()` and `ResultsAsync<T>()`

Queues all the data.  Then using the provided type `T` entity, the data is coerced by which properties intersect with the ones available to the `IDataReader`.

Optionally a field to column override map can be passed as a parameter.  If a column is set as `null` then that field is ignored (not applied to the model).

### Examples

If all the columns in the database map exactly to a field: (A column that has no associated field/property is ignored.)

```cs
var people = cmd.Results<Person>();
```

If the database fields don't map exactly:

```cs
var people = cmd.Results<Person>(
 (Field:"FirstName", Column:"first_name"),
 (Field:"LastName", Column:"last_name")));
```

or

```cs
var people = cmd.Results<Person>(
 ("FirstName", "first_name"),
 ("LastName", "last_name"));
```

or

```cs
var people = cmd.Results<Person>(new Dictionary<string,string>{
 {"FirstName", "first_name"},
 {"LastName", "last_name"});
```

### `Retrieve()` and `RetrieveAsync()`

Queues all the data.  Returns a `QueryResult<Queue<object[]>>` containing the requested data and column information.  The `.DequeueAsMappedDictionaries()` extension will iteratively convert the results to dictionaries for ease of access.

### `ResultsAsync<T>`

`ResultsAsync<T>()` is fully asynchronous from end-to-end but returns an `IEnumerable<T>` that although has fully buffered the all the data into memory, has deferred the transformation until enumerated.  This way, the asynchronous data pipeline is fully complete before synchronously transforming the data.

## Mapped queries: `Map<T>()`

`Map<T>()` is the fast, expressive way to read rows into a model. It is deferred: nothing runs until a terminal is called, and rows are mapped as the reader streams.

```cs
var posts = await connection
    .Command("select * from posts where author = @author")
    .AddParam("author", 42)
    .Map<Post>()
    .ToArrayAsync();

var page = connection.Command("select * from posts order by id")
    .Map<Post>()
    .Skip(20).Take(10)     // skipped rows are advanced, never mapped; reading stops once the take is met
    .ToArray();

var one = connection.Command("select * from posts where id = @id").AddParam("id", 7).Map<Post>().SingleOrDefault();
```

Prefer `ToArray` for results you only read: it allocates one exactly-sized array (no spare capacity, no collection object) and an empty result is the shared static empty array. Use `ToList` when you need a mutable collection.

Terminals: `ToArray`, `ToList`, `ToImmutableArray`, `ToDictionary`, `ToHashSet`, `FirstOrDefault`, `SingleOrDefault` (each with an `…Async` twin) and `Read(...)`/`ReadAsync(...)` for streaming.

How it stays fast:

- The mapping from a result set to `T` is compiled **once per result-set shape** (column names + column types) and cached, so a repeat query does no reflection and no compilation.
- Values are read with the provider's typed getters (`GetInt32`, `GetString`, …), so nothing is boxed. A column whose type differs from the property converts numerically (`long` → `int`, `int` → enum, …) or through a general conversion (string → enum/`Guid`).
- `NULL` is honored for reference types and `Nullable<T>` properties. A `NULL` in a column mapped to a non-nullable value-type property is the provider's error to raise, and the exception names the property and column.
- Columns are matched to properties case-insensitively; unmatched columns are ignored, and unmatched properties keep their defaults. Field/column overrides are supported: `.Map<Post>(("Id", "PostId"), ("Body", null))` (a `null` column ignores the field). To reuse them, define a `RowMap<T>` once (below).
- Readers are opened with `SequentialAccess | SingleResult` (`SingleRow` for `FirstOrDefault`), like Dapper.
- The execution path allocates no closures: state-passing `ExecuteReader`/`ExecuteReaderAsync` overloads on `IExecuteReader` take a `static` lambda plus the state it needs.

### Column names that differ from the properties: `RowMap<T>`

Per-call overrides repeat the mapping at every call site. Define a `RowMap<T>` once instead and pass it wherever overrides are accepted (`Map`, `Results`, `ToChannel`, …). It is checked where you create it (a property that does not exist or cannot be set, a property named twice, or two properties on one column throws `ArgumentException` right there, not on the first query), it is immutable, and it holds its own compiled reader, so passing it costs no allocation, key or lookup. Properties it does not mention still map by name, and a `null` column ignores the property.

Create each mapping on first use with the C# 14 `field` keyword. A large app (hundreds of models) then builds only the mappings it actually uses, instead of all of them at startup:

```cs
static class Maps
{
    public static RowMap<Post> Post => field ??= new(("Id", "post_id"), ("Title", "post_title"), ("Body", null));
    public static RowMap<Customer> Customer => field ??= new(("Id", "customer_id"), ("Name", "full_name"));
}

var posts = connection.Command("select * from posts").Map<Post>(Maps.Post).ToList();
```

- **Concurrency.** A reference write is atomic and publishing an object reference is a release in the .NET memory model ([`Memory-model.md`](https://github.com/dotnet/runtime/blob/main/docs/design/specs/Memory-model.md): *Atomic memory accesses*, *Object assignment*), so a thread that sees the mapping sees it fully built. The one effect of two threads racing on first use is that two equivalent, immutable mappings may be built; either works.
- **`static readonly`** also works: `static readonly RowMap<Post> PostMap = new(...);`. It is built when its containing type's static initializer first runs, together with every other static field of that type, so keep such mappings in their own small static class. It skips the null check, but in our micro-benchmark it read no faster than `field ??=` (about 1.2 ns either way; see `Benchmarks/RESULTS.md`).
- **Exactly one instance:** `public static RowMap<Post> Post => LazyInitializer.EnsureInitialized(ref field, static () => new(("Id", "post_id")));` publishes with `Interlocked.CompareExchange` (the factory may still run more than once) and adds a method call per read (about 2.8 ns instead of 1.2 ns in our micro-benchmark). `Lazy<T>` works too, at the price of an extra object (72 B) per mapping.
- Compilers older than C# 14 need an explicit backing field: `static RowMap<Post>? _post; public static RowMap<Post> Post => _post ??= new(("Id", "post_id"));`.

## Transactions

Example:

```cs
// Returns true if the transaction is successful.
public static bool TryTransaction()
=> ConnectionFactory.Using(connection =>
    // Open a connection and start a transaction.
    connection.ExecuteTransactionConditional(transaction => {

        // First procedure does some updates.
        var count = transaction
            .StoredProcedure("[Updated Procedure]")
            .ExecuteNonQuery();

        // Second procedure validates the results.
        // If it returns true, then the transaction is committed.
        // If it returns false, then the transaction is rolled back.
        return transaction
            .StoredProcedure("[Validation Procedure]")
            .AddParam("@ExpectedCount", count)
            .ExecuteScalar<bool>();
    }));
```

## 8.0 Release Notes

- All `.ConfigureAwait(true)` are now `.ConfigureAwait(false)` as they should be.  The caller will need to `.ConfigureAwait(true)` if they need to resume on the calling context.
- Added `Open.Database.Extensions.MSSqlClient` for `Microsoft.Data.SqlClient` support.
- .NET 8.0 added to targets to ensure potential compilation and performance improvements are available.
- Improved nullable integrity.

## 9.0 Release Notes

- Open.Database.Extensions meta-package is discontinued.
- .NET 9.0 added to targets to ensure potential compilation and performance improvements are available.
- Implemented some .NET 8 and 9 specific features.
- Significant cleanup and simplification where possible.

## 10.1.0 Release Notes

A modernization and allocation-reduction pass. **Source-compatible with 10.0** — existing code recompiles cleanly.

- **Target frameworks:** `net10.0` is now the primary/modern target. `netstandard2.0` and `netstandard2.1` are retained as shimmed legacy paths (net8/net9 consumers use the `netstandard2.1` build); `Open.Database.Extensions.MSSqlClient` also targets `net472`. The explicit `net8.0`/`net9.0` targets were dropped.
- **Fewer allocations on the hot paths:** case-insensitive column-to-property matching now uses `StringComparer.OrdinalIgnoreCase` (and `FrozenDictionary` on `net10.0`) instead of allocating an upper-cased string per column; assorted per-query LINQ and dead code removed.
- **Ergonomic field-mapping overrides:** `Results<T>`, `ResultsAsync<T>`, `To<T>`, etc. now accept target-typed `new(...)` and collection expressions:

  ```cs
  cmd.Results<Person>(new("FirstName", "first_name"), new("LastName", "last_name"));
  cmd.Results<Person>([new("FirstName", "first_name"), new("LastName", "last_name")]);
  ```

- **Modernized `params`:** ordinal helpers (`Retrieve`, `AsEnumerable`, …) accept `params IEnumerable<int>`/`params IEnumerable<string>` — any enumerable, not just arrays.
- **Read-only inputs:** several read-only `IList<T>` parameters were widened to `IReadOnlyList<T>` (`GetValuesFromOrdinals`, `EnumerateValuesFromOrdinals`, `ToDictionary`). Arrays, `List<T>`, `ImmutableArray<T>`, etc. all still bind. *(A custom type implementing only `IList<T>` — and not also `IReadOnlyList<T>` — would need to recompile; no BCL collection is affected.)*
- **Lower per-query overhead:** `Transformer<T>` caches its per-type reflection once instead of rebuilding it on every query; buffered/data-table paths pre-size collections and avoid redundant `Select` iterators.
- Fixed a wasted-allocation / incorrect-return bug in `CopyToDBNullAsNull`.
- Public API surface is now tracked with `Microsoft.CodeAnalysis.PublicApiAnalyzers`, and unit-test coverage was substantially expanded.

## 11.0.0 Release Notes

> **11.0.0-preview01.** This is a preview: please try it and tell us what you find. See [Known limitations](#known-limitations-in-this-preview) below.

A performance release: **result mapping is rewritten and now beats Dapper 2.1.89** in the included benchmarks (`Benchmarks/`, see `Benchmarks/RESULTS.md`), with a fraction of the allocations on multi-row results.

- **Cached, typed row mapping.** The mapping for a result-set shape is compiled once and reused. Previously every query compiled a setter per property (~1 ms fixed cost per `Map<T>()`). Values are read with typed getters, so nothing is boxed.
- **`RowMap<T>`: declare a column mapping once.** When a query's columns are named differently from the model's properties, define a validated, immutable `RowMap<T>` once (for example `public static RowMap<Post> Post => field ??= new(("Id", "post_id"), ...)`) and pass it to `Map<T>`, `Results<T>`, `ToChannel` and every other override parameter. Reads through it cost what `Map<T>()` costs on matching names. See [Mapped queries](#mapped-queries-mapt).
- **Direct read loops.** `ToList`, `ToArray`, `FirstOrDefault`, `SingleOrDefault`, `ToDictionary`, `ToHashSet`, `ToImmutableArray` (and async twins) read straight from the reader. `Skip` advances without mapping, `Take` stops reading when met. They are now instance methods on `MappedQuery<T>` (no boxing of the query struct); the `IMappedQuery<T>` extensions remain for generic code.
- **No per-call closures.** `IExecuteReader` gained state-passing `ExecuteReader<TState, TResult>` / `ExecuteReaderAsync<TState, TResult>`; pass a `static` lambda and the state it needs.
- **Leaner commands.** A command created from a connection no longer allocates a pool wrapper, `Params` is created on first use, and async execution skips awaiting when the connection is already open.
- **Readers use `SequentialAccess | SingleResult`** for `Map<T>()` (and `SingleRow` for `FirstOrDefault`), like Dapper. `Map(selector)` keeps the default access mode because a selector may read columns in any order.
- **Channel and Dataflow fixes.** `complete: false` no longer runs the command twice. The mapped `ToChannel<T>`/`ToChannelAsync<T>` and `ToTargetBlock<T>` overloads complete your target only after every row has been delivered and the database reader has been released (before, up to about 1,000 buffered rows could be lost with a bounded target), no longer lose the last row when the reader fails mid-stream, and map correctly when the reader has extra unmapped columns.
- **Connection and command fixes.** `OpenAsync(IDbConnection)` no longer opens a `DbConnection` twice, `EnsureOpenAsync` closes a connection that finished opening just as cancellation arrived, and `FirstOrdinalResults<T>(IDbCommand, behavior)` honors `behavior`.

### Breaking changes

- `IExecuteReader` has two new members (above). Custom implementations must add them; `ExpressiveCommand*` already do.
- A `NULL` in a column mapped to a **non-nullable value-type** property is the provider's error to raise (it is wrapped in an `InvalidOperationException` naming the property and column). Map nullable columns to `Nullable<T>`/reference properties.
- Field-mapping overrides naming a property that does not exist throw `ArgumentException` (was `KeyNotFoundException`). Only properties with a public setter are mapped.
- `FirstOrdinalResults<T>` reads `NULL` as `default`. (`ResultsBuffered*` and `FirstOrdinalResults<T>` still buffer into a queue and return a dequeuing enumerable, so items are released to the GC as they are drained.)
- `ExpressiveCommandBase.Params` is created lazily.
- Async command execution passes the command's `CancellationToken` to the connection open.
- Mapped Channel and Dataflow overloads: a mapping failure now faults the target when `complete` is `true`; the target is never completed or faulted before the reader is released; `ArgumentNullException.ParamName` for a null target is `"target"`; and a mapped async channel call that is cancelled succeeds only if the reader had already reported the end of the results, otherwise it throws `OperationCanceledException`.

### Known limitations in this preview

- `Retrieve()` followed by `DequeueAs<T>()` is a two-pass route (rows are copied into `object[]` and mapped as they are drained); it is slower and allocates more than a direct `Map<T>()` at larger sizes. For typed results prefer `Map<T>()` or `ResultsBuffered<T>()`. See `Benchmarks/RESULTS.md`.
- The unmapped `ToChannel` overloads can still lose the row that was in flight when a read fails while the target is full; the cause is in the underlying channel library and a fix is planned.
- Benchmarks and tests cover the in-memory provider, SQLite and SQL Server LocalDB. Real-world feedback on other providers and workloads is welcome.
