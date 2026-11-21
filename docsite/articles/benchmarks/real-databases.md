# On real databases

**The question:** the synthetic reader has no database behind it. How much of ODX's lead survives when a real provider does the work?

## In short

- **On SQL Server LocalDB, `Map<T>()` ties with hand-written code and with Dapper.** For 10,000 rows of 20 columns it took 25.9 ms, a hand-written selector 25.2 ms and Dapper 27.6 ms. BenchmarkDotNet's own error margin on these runs is 2 to 9% of the mean, so those gaps are not a result.
- **Memory is where ODX still wins.** `Map<T>()` allocates 9.48 MB, the same as hand-written code. Dapper allocates 12.27 MB, 29.4% more.
- **Not faster on SQLite.** `Map<T>()` is 20.9% slower than a hand-written selector at 20 columns (31.3 against 25.9 ms) and 24.5% slower at 5 columns. Against Dapper it is 5.1% slower at 20 columns and 2.0% faster at 5. These runs do not show why.

## Time per call

<div class="perf-chart"><img src="../../images/perf/perf-real-time.svg" alt="Two panels of ranked bars on one scale, time per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand-written selector 25.2 milliseconds, hand loop 25.6, Map of T 25.9, row class with Select 27.2, Dapper 27.6. SQLite file: hand-written selector 25.9, hand loop 25.9, Dapper 29.8, row class with Select 30.9, Map of T 31.3."></div>

Time to read 10,000 rows of 20 columns, on two providers drawn on one scale. Shorter is better. The bars inside each panel are ranked, best first. On SQL Server the five bars are close; on SQLite two are clearly ahead of the other three.

## Memory per call

<div class="perf-chart"><img src="../../images/perf/perf-real-memory.svg" alt="Two panels of ranked bars on one scale, memory per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand loop, Map of T and hand-written selector 9.48 megabytes, row class with Select 11.54, Dapper 12.27. SQLite file: hand loop and hand-written selector 11.96, Map of T 14.11, Dapper 14.79, row class with Select 16.17."></div>

The same cases, measuring the memory each call allocates. The differences are larger than they are for time, and they do not depend on how busy the machine was.

## The numbers

Each table lists time and memory for the same five ways of reading the rows. *Hand loop* is a loop written directly on the provider's reader, with typed getters. *Row class, inline Select* reads a row class and calls `Select` while the reader is open, which is the usual way to get a second object per row.

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

## What to take from it

**The database sets the pace.** The synthetic reader serves rows for free, so the library's own work is the whole bar. Here the provider reads, parses and converts every row first, and that is most of the time. A lead of 2.5 times over Dapper on a synthetic reader becomes a few percent on SQL Server.

**What you can count on is memory.** Allocations are the library's, so they show through any provider. They are also what the garbage collector later pays for.

**SQLite is the one place `Map<T>()` is behind.** The hand loop and the hand-written selector are the two leaders on SQLite at both widths, and `Map<T>()` is about a fifth behind at 20 columns. These runs do not show why, so we do not claim a cause.

**Reader-open time is not on these charts.** The real-provider runs measure the whole call. The synthetic pages show how long the connection is held, which is the number a busy service cares about.

## How this was measured

- **Question:** how do the mapping approaches compare when a real provider is behind them?
- **Setup:** a table of 10,000 rows, 20 typed columns (and a second run at 5 columns). SQL Server LocalDB through `Microsoft.Data.SqlClient` 7.1.1; a SQLite file through `Microsoft.Data.Sqlite`. The database and the file are created for the run and removed after it. Synchronous terminals. Every approach returns the same objects. On SQLite, Dapper maps text to `Guid` with the usual `SqlMapper.TypeHandler` recipe, because it does not convert it on its own.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `fbb13d7`, 2026-11-14; AMD Ryzen 9 7900 (12 cores), .NET 11.0.0, Windows 10.
- **Data:** [real-providers.csv](../../perf-data/real-providers.csv) ([source and conditions](../../perf-data/real-providers.source.txt)).
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*RealProviderBenchmarks*"`. Set `ODX_RP_PROVIDERS`, `ODX_RP_WIDTHS` and `ODX_RP_ROWS` to choose the case; the [generator README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf) lists the exact settings of the published runs.
