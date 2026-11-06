# What's new in 11

> [!NOTE]
> Version 11.0.0 is currently a preview (`11.0.0-preview02`). Please try it and report what you find.

Version 11 is a performance release. Result mapping is rewritten around a cached, typed row plan. See [Performance](performance.md) for what that does and does not buy you.

## Preview 02

New since `11.0.0-preview01`:

- **`RowMap<T>`: declare a column mapping once.** When a query's columns are named differently from the model's properties, declare a validated, immutable `RowMap<T>` once (`public static RowMap<Post> PostMap => field ??= [new(nameof(Post.Id), "post_id"), ...];`) and pass it to `Map<T>`, `Results<T>`, `ToChannel`, `ToTargetBlock`, `AsSourceBlock` and every other override parameter. A map is checked where it is declared, and a `null` column means "do not map this property". A read through a map allocates what `Map<T>()` allocates on matching names. See [Reusing a mapping](mapping.md#reusing-a-mapping).
- **`Map<T>` takes the pairs inline.** `Map<Post>(new(nameof(Post.Id), "post_id"), new(nameof(Post.Body), null))` is for one-offs: it builds a new map and compiles the row reader on every call and caches nothing.
- **Key/value pair overloads for `ToChannel`, `ToChannelAsync`, `ToTargetBlock`, `ToTargetBlockAsync`, `AsSourceBlock` and `AsSourceBlockAsync`.** They accept pairs, and so a `RowMap<T>`. Their tuple overloads remain as a convenience that converts the tuples to pairs.
- **Only public instance properties are mapped.** A static property is no longer treated as a mappable property: name matching ignores it, and a `RowMap<T>` that names one throws where it is declared.

## New and improved

- **Cached, typed row mapping.** The mapping for a result-set shape is compiled once and reused. Before, every query compiled a setter per property, which was a fixed cost of about a millisecond per `Map<T>()` in the library's own benchmark. Values are read with typed getters, so nothing is boxed.
- **Direct read loops.** `ToList`, `ToArray`, `FirstOrDefault`, `SingleOrDefault`, `ToDictionary`, `ToHashSet`, `ToImmutableArray` and their async twins read straight from the reader. `Skip` advances without mapping and `Take` stops reading once it is met. They are instance methods on `MappedQuery<T>`, so the query struct is not boxed; the `IMappedQuery<T>` extensions remain for generic code.
- **No per-call closures.** `IExecuteReader` gained state-passing `ExecuteReader<TState, TResult>` and `ExecuteReaderAsync<TState, TResult>`. Pass a `static` lambda and the state it needs.
- **Leaner commands.** A command created from a connection no longer allocates a pool wrapper, `Params` is created on first use, and async execution skips awaiting when the connection is already open.
- **Reader behavior.** `Map<T>()` opens readers with `SequentialAccess | SingleResult`, and `FirstOrDefault` adds `SingleRow` when nothing is skipped. `Map(selector)` does not use `SequentialAccess`, because a selector may read columns in any order.

## Fixes

- **Channels and Dataflow.** With `complete: false`, the command no longer runs twice. The mapped `ToChannel<T>`, `ToChannelAsync<T>` and `ToTargetBlock<T>` overloads complete your target only after every row has been delivered and the reader has been released (before, up to about 1,000 buffered rows could be lost with a bounded target). They no longer lose the last row when the reader fails mid-stream, and they map correctly when the reader has extra unmapped columns.
- **Connections and commands.** `OpenAsync(IDbConnection)` no longer opens a `DbConnection` twice. `EnsureOpenAsync` closes a connection that finished opening just as cancellation arrived. `FirstOrdinalResults<T>(IDbCommand, behavior)` honors `behavior`.

## Breaking changes

- `IExecuteReader` has two new members (above). Custom implementations must add them; the `ExpressiveCommand*` classes already do.
- A `NULL` in a column mapped to a non-nullable value-type property is the provider's error to raise, wrapped in an `InvalidOperationException` that names the property and the column. Map nullable columns to `Nullable<T>` or reference properties.
- A field-mapping override that names a property that does not exist throws `ArgumentException` (it was `KeyNotFoundException`). Only properties with a public setter are mapped.
- `FirstOrdinalResults<T>` reads `NULL` as `default`. `ResultsBuffered*` and `FirstOrdinalResults<T>` still buffer into a queue and return a dequeuing enumerable, so items are released to the garbage collector as they are drained.
- `ExpressiveCommandBase.Params` is created lazily.
- Async command execution passes the command's `CancellationToken` to the connection open.
- Mapped Channel and Dataflow overloads: a mapping failure now faults the target when `complete` is `true`; the target is never completed or faulted before the reader is released; `ArgumentNullException.ParamName` for a null target is `"target"`; and a cancelled mapped async channel call succeeds only if the reader had already reported the end of the results, otherwise it throws `OperationCanceledException`.

## Known limitations in this preview

- `Retrieve()` followed by `DequeueAs<T>()` is a two-pass route: rows are copied into `object[]` and mapped as they are drained. It is slower and allocates more than a direct `Map<T>()` at larger sizes. For typed results prefer `Map<T>()` or `ResultsBuffered<T>()`.
- The unmapped `ToChannel` overloads can still lose the row that was in flight when a read fails while the target is full. The cause is in the underlying channel library.
- Benchmarks and tests cover the in-memory provider, SQLite and SQL Server LocalDB. Feedback from other providers and workloads is welcome.

Earlier versions are in [Earlier release notes](release-notes.md).
