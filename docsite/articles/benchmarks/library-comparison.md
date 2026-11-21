# ODX against Dapper and hand-written code

**The question:** for the same query and the same rows, how does `Map<T>()` compare with Dapper and with ADO.NET written by hand?

## In short

- **With no database in the way**, `Map<T>()` holds the connection open **2.48 times shorter** than Dapper's `Query<T>` (81.6 against 202.4 µs) and allocates **2.41 times less memory** (227.4 against 548.1 KB). 20 columns, 1,000 rows, synthetic reader.
- It allocates exactly what a hand-written selector allocates (227.4 KB) and takes **1.74 times as long** (81.6 against 46.8 µs). A hand-written selector is the floor.
- **On a real database the gap closes.** On SQL Server LocalDB the three are within the noise of one run (25.2 to 27.6 ms for 10,000 rows). What you can count on there is memory: Dapper allocates 29.4% more.
- **Not faster:** on a SQLite file, 10,000 rows of 20 columns, `Map<T>()` takes 31.3 ms. That is 5.1% more than Dapper (29.8 ms) and 20.9% more than a hand-written selector (25.9 ms). These runs do not show why.
- **EF Core** is not measured yet. No number for it appears on this page.

## With no database: a synthetic reader

<div class="perf-chart"><img src="../../images/perf/perf-summary.svg" alt="Two panels of four bars each for 20 columns and 1,000 rows on a synthetic reader. Connection held open: hand-written selector 46.8 microseconds, Map of T 81.6, object arrays 178.4, Dapper Query of T 202.4. Memory allocated: Map of T and the hand-written selector 227.4 kilobytes, object arrays 516.6, Dapper 548.1."></div>

Each panel shows one thing about the same job, reading 1,000 rows of 20 columns into objects. The top panel is how long the connection is held open; the bottom panel is how much memory the call allocates. Shorter and smaller are better.

[!include[](../../includes/perf/summary.md)]

Two of the four are for context. **Hand-written selector** is `Map(selector)` with a function you write yourself: it is the fastest way to use ODX, and what `Map<T>()` is compared against. **`object[]` rows** copy every row into an array of boxed values and map later: it is the usual shortcut, and the slowest way to hold a connection.

## On real databases

<div class="perf-chart"><img src="../../images/perf/perf-real-time.svg" alt="Two panels of ranked bars on one scale, time per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand-written selector 25.2 milliseconds, hand loop 25.6, Map of T 25.9, row class with Select 27.2, Dapper 27.6. SQLite file: hand-written selector 25.9, hand loop 25.9, Dapper 29.8, row class with Select 30.9, Map of T 31.3."></div>

The same job on a real provider, with 10,000 rows of 20 columns. A real provider spends its own time on every row, so the large gaps of the synthetic reader shrink to a few percent. The *hand loop* is the ADO.NET you would write by hand, with typed getters straight on the provider's reader. It is the reference the others are measured against.

<div class="perf-chart"><img src="../../images/perf/perf-real-memory.svg" alt="Two panels of ranked bars on one scale, memory per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand loop, Map of T and hand-written selector 9.48 megabytes, row class with Select 11.54, Dapper 12.27. SQLite file: hand loop and hand-written selector 11.96, Map of T 14.11, Dapper 14.79, row class with Select 16.17."></div>

The second chart is memory. It does not shrink on a real provider, because the extra allocations are the library's, not the database's.

The tables for both providers, and the same runs at 5 columns, are on [On real databases](real-databases.md). The row class with `Select` is the usual way to get a second object per row: read a row class, then call `Select`. It costs more memory than the other approaches on every provider.

## A wider set of everyday calls (older run)

The synthetic runs above read lists. This table is from an older run of the same project that covers more calls: a 12-column row, an in-memory provider, ODX 11 against Dapper 2.1.89, 1,000 rows per call, .NET 10.0.14, measured 2026-11-08. It is kept in [`RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md), which also has the 1-row and 100-row cases and the SQL Server and SQLite rows. It was not repeated on .NET 11 and has no data file.

| Call | Dapper | ODX | ODX time against Dapper | Dapper memory | ODX memory |
|---|---:|---:|---:|---:|---:|
| `List` | 100,275 ns | 38,959 ns | 0.39 | 355,048 B | 128,768 B |
| `Array` | 101,615 ns | 39,642 ns | 0.39 | 363,073 B | 120,192 B |
| `ListAsync` | 99,021 ns | 41,350 ns | 0.42 | 355,016 B | 128,944 B |
| `First` | 876 ns | 107 ns | 0.12 | 576 B | 280 B |
| Selector (a hand-written function) | 4,389 ns | 3,745 ns | 0.85 | 33,096 B | 33,176 B |
| Untyped rows | 108,936 ns | 98,712 ns | 0.91 | 411,688 B | 363,184 B |
| `Scalar` (one value) | 61 ns | 40 ns | 0.66 | 136 B | 168 B |

ODX is at or below Dapper's time in all 54 cases of that run, and a tie in a few (`ScalarAsync` 0.98). It allocates more than Dapper in a few small fixed-cost cases: `Scalar` and `Execute` by 32 bytes, because of the command object the fluent API returns.

## EF Core

EF Core is not in these benchmarks yet, so this page gives no number for it.

## Why `Map<T>()` is faster here

The library is built on one idea: hold the connection open as briefly as possible, and allocate as little as possible while it is open. Four things in the changes of [What's new in 11](../whats-new-11.md) account for the numbers above:

- The mapping from columns to your type is compiled once for each result-set shape and cached. A call after the first does not look at the type again.
- Values are read with typed getters (`GetInt32`, `GetString`), so nothing is boxed.
- Terminals read straight from the reader, with no copy of every row first.
- Running a command allocates no closures, and a command built from a connection allocates no pool wrapper.

The same list explains why the memory column matches a hand-written selector: there is nothing left to allocate but the list and one object per row.

**Where the gain does not survive.** Once a real provider does the work, most of the time is the provider's. SQL Server LocalDB reads 10,000 rows in about 25 ms whichever library maps them, so a 2.5-times lead on a synthetic reader becomes a tie. If your queries are slow because of the database, no mapping library will change that. The thing ODX still changes there is memory.

## How this was measured

- **Question:** the same job through four libraries or hand-written code: read rows into objects.
- **Setup:** 20 typed columns, 1,000 rows on the synthetic reader; 20 columns and 10,000 rows on SQL Server LocalDB (`Microsoft.Data.SqlClient` 7.1.1) and a SQLite file (`Microsoft.Data.Sqlite`). Synchronous terminals. On SQLite, Dapper maps text to `Guid` with a type handler.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. See [How we measure](how-we-measure.md).
- **Build, date, machine:** synthetic runs: build `b27a02f`, 2026-11-14; real-provider runs: build `fbb13d7`, 2026-11-14; AMD Ryzen 9 7900, .NET 11.0.0, Windows 10. The older table: .NET 10.0.14, 2026-11-08, master `41af17c`.
- **Data:** [synthetic-overhead.csv](../../perf-data/synthetic-overhead.csv) ([source](../../perf-data/synthetic-overhead.source.txt)), [real-providers.csv](../../perf-data/real-providers.csv) ([source](../../perf-data/real-providers.source.txt)).
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks*"`, and `"*RealProviderBenchmarks*"` for the two real databases.
