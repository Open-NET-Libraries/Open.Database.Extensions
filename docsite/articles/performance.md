# Performance

The library rests on one idea: keep a connection open for as short a time as possible and allocate as little as possible while it is. This page says what is measured, what the numbers mean, and where the library is not faster.

> [!NOTE]
> The figures below describe the 11.0.0-preview01 build. A re-baseline is pending, and this page will be updated with it.

## What is measured

The benchmarks live in [`Benchmarks/`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/Benchmarks) and compare this library with Dapper 2.1.89 on the same job. They use BenchmarkDotNet 0.15.8 on .NET 10, against three providers:

| Provider | What it tells you |
|---|---|
| An in-memory ADO.NET provider with 12-column rows | The library's own overhead, with no I/O |
| SQL Server LocalDB (`Microsoft.Data.SqlClient` 7.1.1) | A real provider, where the database dominates the time |
| SQLite | A second real provider |

The published run is in [`Benchmarks/RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md). To reproduce it:

```sh
dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*"
```

## Results for `Map<T>()`

An independent re-run of preview01 checked the published claims. Timings came from a noisy machine, so read the ratios as indicative; allocation counts are deterministic.

| Provider | Time compared with Dapper | Allocations compared with Dapper |
|---|---|---|
| In-memory, 100 or more rows | Roughly 2 to 3 times faster | About 0.33 to 0.38 of Dapper's on multi-row results |
| SQL Server LocalDB | On par to a few percent faster (ratios 0.89 to 1.08, mostly ties) | About 0.55 to 0.6 times on multi-row results |
| SQLite | Published as faster, not independently re-run | Published as lower, not independently re-run |

With a real database the server and the network dominate the time, so the speed difference shrinks to a tie or a few percent. The allocation difference remains, which matters for garbage-collection pressure on busy services.

## Where it is not faster

- `Retrieve().DequeueAs<T>()` copies every row to `object[]` first and maps later. It is not faster than a plain loop, and for typed results `Map<T>()` is the better route.
- `ToChannel` and the Dataflow methods are not faster than reading rows in a plain loop. They exist to feed pipelines: see [Channels and Dataflow](channels-and-dataflow.md).
- Selector-based mapping (`Map(selector)`) and `ScalarAsync` are ties with Dapper.
- A few fixed-cost scenarios, such as a scalar query, allocate slightly more than Dapper because of the command object.

## Why `Map<T>()` is cheaper

The changes behind the numbers are listed in [What's new in 11](whats-new-11.md): the mapping is compiled once per result-set shape and cached, values are read with typed getters (no boxing), terminals read straight from the reader, execution allocates no closures, and a command built from a connection allocates no pool wrapper.

## Limits of these numbers

- The in-memory provider has no network or server time, so do not read its ratios as end-to-end gains.
- The LocalDB run is a short job where ties within a few percent are noise.
- Results vary by machine, runtime and data shape. Run the benchmarks on yours.
- The per-scenario tables in `RESULTS.md` predate the latest changes; use them as indicative until the re-baseline.
