# On real databases

**The question:** the synthetic reader has no database behind it. How much of ODX's lead survives when a real provider does the work?

## In short

- **On SQL Server LocalDB, `Map<T>()` ties with hand-written code and is ahead of Dapper.** For 10,000 rows of 20 columns it took 25.6 ms, a hand-written selector 25.5 ms and Dapper 29.4 ms, which is 14.9% longer than `Map<T>()` (the error margins do not overlap). BenchmarkDotNet's own error margin on the 20-column SQL Server runs is 2.7 to 4.9% of the mean (the tables show each one), so the gap to hand-written code is not a result.
- **`Map<T>()` allocates less than Dapper on both databases.** On SQL Server `Map<T>()` allocates 9.48 MB, the same as hand-written code; Dapper allocates 12.27 MB, 29.4% more. On SQLite `Map<T>()` allocates 12.51 MB: 15.4% less than Dapper (14.79) and 4.6% more than hand-written code (11.96).
- **On SQLite `Map<T>()` is behind hand-written code and ahead of Dapper.** `Map<T>()` is 4.8% slower than a hand-written selector at 20 columns (27.9 against 26.6 ms, within the error margins) and 15.1% slower at 5 columns. It is 23.2% faster than Dapper at 20 columns and 26.4% faster at 5. Dapper's 20-column SQLite figure here (36.3 ms) is higher than in the two runs on [ODX against Dapper and hand-written code](library-comparison.md#on-real-databases), where it is 29.8 ms against 27.7 ms for `Map<T>()`.

## Time per call

<div class="perf-chart"><img src="../../images/perf/perf-real-time.svg" alt="Two panels of ranked bars on one scale, time per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand loop 25.0 milliseconds, hand-written selector 25.5, Map of T 25.6, factory map to the domain object 25.8, Dapper 29.4. SQLite file: hand loop 25.8, hand-written selector 26.6, Map of T 27.9, factory map to the domain object 28.1, Dapper 36.3."></div>

Time to read 10,000 rows of 20 columns, on two providers drawn on one scale. Shorter is better. The bars inside each panel are ranked, best first. On SQL Server four bars are close and Dapper is clearly behind; on SQLite two are ahead, two follow close behind, and Dapper is far behind.

## Memory per call

<div class="perf-chart"><img src="../../images/perf/perf-real-memory.svg" alt="Two panels of ranked bars on one scale, memory per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand loop, Map of T, factory map to the domain object and hand-written selector 9.48 megabytes, Dapper 12.27. SQLite file: hand loop and hand-written selector 11.96, Map of T and factory map to the domain object 12.51, Dapper 14.79."></div>

The same cases, measuring the memory each call allocates. They do not depend on how busy the machine was.

## The numbers

Each table lists time and memory for the same five ways of reading the rows. *Hand loop* is a loop written directly on the provider's reader, with typed getters. *Factory map, domain object* is a factory map straight to the domain object: one object per row, built while the reader is open.

### 20 columns, 10,000 rows

**SQL Server LocalDB**

[!include[](../../includes/perf/real-sqlserver.md)]

**SQLite file**

[!include[](../../includes/perf/real-sqlite.md)]

### 5 columns, 10,000 rows

**SQL Server LocalDB**

[!include[](../../includes/perf/real-sqlserver-w5.md)]

**SQLite file**

[!include[](../../includes/perf/real-sqlite-w5.md)]

## Why, and what to take from it

**The database sets the pace.** The synthetic reader serves rows for free, so the library's own work is the whole bar. Here the provider reads, parses and converts every row first, and that is most of the time. A lead of 2.6 times over Dapper on a synthetic reader becomes 14.9% on SQL Server at 20 columns, and 1.6% (within the noise) at 5 columns.

**What you can count on is memory, against Dapper.** Allocations are the library's, so they show through any provider. They are also what the garbage collector later pays for.

**SQLite is where `Map<T>()` is furthest behind.** The hand loop and the hand-written selector are the two leaders on SQLite at both widths. At 20 columns `Map<T>()` is 8.2% behind the hand loop and 4.8% behind the selector; at 5 columns, 15.6% and 15.1%.

**How long the connection is held open is not on these charts.** The real-provider runs measure the whole call, not how long the connection is held. The synthetic pages show the connection time, which is the number a busy service cares about.

## How this was measured

- **Question:** how do the mapping approaches compare when a real provider is behind them?
- **Setup:** a table of 10,000 rows, 20 typed columns (and a second run at 5 columns). SQL Server LocalDB through `Microsoft.Data.SqlClient` 7.1.1; a SQLite file through `Microsoft.Data.Sqlite`. The database and the file are created for the run and deleted after it. Synchronous terminals. Every approach returns the same objects. On SQLite, Dapper maps text to `Guid` with the usual `SqlMapper.TypeHandler` recipe, because it does not convert it on its own.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `6a770e8`, 2026-11-23; AMD Ryzen 9 7900 (12 cores), .NET 11.0.0, Windows 10.
- **Data:** [real-providers.csv](../../perf-data/real-providers.csv) ([source and conditions](../../perf-data/real-providers.source.txt)).
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*RealProviderBenchmarks*"`. Set `ODX_RP_PROVIDERS`, `ODX_RP_WIDTHS` and `ODX_RP_ROWS` to choose the case; the [generator README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf) lists the exact settings of the published runs.
