# Earlier release notes

Version 11 is covered in [What's new in 11](whats-new-11.md).

## 10.1.0

A modernization and allocation-reduction pass. Source-compatible with 10.0: existing code recompiles cleanly.

- **Target frameworks.** `net10.0` is the primary, modern target. `netstandard2.0` and `netstandard2.1` are retained as shimmed legacy paths (.NET 8 and .NET 9 consumers use the `netstandard2.1` build). `Open.Database.Extensions.MSSqlClient` also targets `net472`. The explicit `net8.0` and `net9.0` targets were dropped.
- **Fewer allocations on the hot paths.** Case-insensitive column-to-property matching uses `StringComparer.OrdinalIgnoreCase` (and `FrozenDictionary` on `net10.0`) instead of allocating an upper-cased string per column. Assorted per-query LINQ and dead code were removed.
- **Ergonomic field-mapping overrides.** `Results<T>`, `ResultsAsync<T>`, `To<T>` and similar methods accept target-typed `new(...)` and collection expressions:

  ```cs
  cmd.Results<Person>(new("FirstName", "first_name"), new("LastName", "last_name"));
  cmd.Results<Person>([new("FirstName", "first_name"), new("LastName", "last_name")]);
  ```

- **Modernized `params`.** Ordinal helpers (`Retrieve`, `AsEnumerable` and others) accept `params IEnumerable<int>` and `params IEnumerable<string>`: any enumerable, not just arrays.
- **Read-only inputs.** Several read-only `IList<T>` parameters were widened to `IReadOnlyList<T>` (`GetValuesFromOrdinals`, `EnumerateValuesFromOrdinals`, `ToDictionary`). Arrays, `List<T>`, `ImmutableArray<T>` and similar types still bind. A custom type that implements only `IList<T>`, and not also `IReadOnlyList<T>`, would need to recompile; no BCL collection is affected.
- **Lower per-query overhead.** `Transformer<T>` caches its per-type reflection once instead of rebuilding it on every query. Buffered and data-table paths pre-size collections and avoid redundant `Select` iterators.
- Fixed a wasted-allocation and incorrect-return bug in `CopyToDBNullAsNull`.
- The public API surface is tracked with `Microsoft.CodeAnalysis.PublicApiAnalyzers`, and unit-test coverage was substantially expanded.

## 9.0

- The `Open.Database.Extensions` meta-package is discontinued.
- .NET 9.0 was added to the targets so that compilation and performance improvements are available.
- Some .NET 8 and .NET 9 specific features were implemented.
- Significant cleanup and simplification where possible.

## 8.0

- All `.ConfigureAwait(true)` became `.ConfigureAwait(false)`, as they should be. A caller that needs to resume on the calling context must `.ConfigureAwait(true)` itself.
- Added `Open.Database.Extensions.MSSqlClient` for `Microsoft.Data.SqlClient` support.
- .NET 8.0 was added to the targets so that compilation and performance improvements are available.
- Improved nullable integrity.
