# ODX against Dapper and hand-written code

**The question:** for the same query and the same rows, how does `Map<T>()` compare with Dapper, with Dapper.AOT and with ADO.NET written by hand?

ODX is not an ORM, so EF Core is not on these charts.

## In short

- **With no database in the way**, `Map<T>()` holds the connection open for **2.9 times less time** than Dapper's `Query<T>` (84.9 against 246.0 µs) and allocates **2.4 times less memory** (227.4 against 548.1 KB). 20 columns, 1,000 rows, synthetic reader. Dapper's number is noisy in this run (an error margin of ±12.4%); at the low end of it the lead is still 2.5 times.
- It allocates exactly what a hand-written selector and Dapper.AOT allocate (227.4 KB).
- **On a real database the gap is small, and it is in ODX's favor.** For 10,000 rows of 20 columns, Dapper took 5% and 13% longer than `Map<T>()` on SQL Server LocalDB in two runs, and 14% and 8% longer on a SQLite file. In three of those four runs the error margins of the two overlap, so read it as "probably a little faster", not as proven. What does not move between runs is memory: Dapper allocates 29.4% more on SQL Server and 18.2% more on SQLite.
- **Not faster than:** Dapper.AOT and a hand-written selector. On the synthetic reader Dapper.AOT holds the connection 25% less time than `Map<T>()` (63.4 against 84.9 µs) and a hand-written selector 47% less (44.8 µs). On a real database neither is measurably different from `Map<T>()`.

## With no database: a synthetic reader

<div class="perf-chart"><img src="../../images/perf/perf-summary.svg" alt="Two panels of four bars each for 20 columns and 1,000 rows on a synthetic reader. Connection held open: hand-written selector 44.8 microseconds, Dapper.AOT 63.4, Map of T 84.9, Dapper Query of T 246.0. Memory allocated: hand-written selector, Dapper.AOT and Map of T 227.4 kilobytes, Dapper 548.1."></div>

Each panel shows one thing about the same job, reading 1,000 rows of 20 columns into objects. The top panel is how long the connection is held open; the bottom panel is how much memory the call allocates. Shorter and smaller are better.

[!include[](../../includes/perf/summary.md)]

Two of the four are for context. **Hand-written selector** is `Map(selector)` with a function you write yourself: the fastest of the four here, and what `Map<T>()` is compared against. **Dapper.AOT** is Dapper's ahead-of-time variant: the same `Query<T>` call, with the code that reads the rows generated when you build instead of when the program runs. It is faster than `Map<T>()` on this reader, and it allocates the same.

The error margin in the table is BenchmarkDotNet's own, the half-width of a 99.9% confidence interval, as a share of the mean. It is largest for Dapper (±12.4%) and smallest for `Map<T>()` (±3.6%).

## On real databases

<div class="perf-chart"><img src="../../images/perf/perf-lib-real-time.svg" alt="Two panels of ranked bars on one scale, the mean of two runs of time per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand-written selector 25.8 milliseconds, Dapper.AOT 26.7, Map of T 27.8, Dapper 30.1. SQLite file: hand-written selector 29.3, Dapper.AOT 29.5, Map of T 30.8, Dapper 34.3."></div>

The same job on a real provider, with 10,000 rows of 20 columns. A real provider spends its own time on every row, so the large gaps of the synthetic reader shrink to a few percent. Each bar is the mean of two runs; the table below has each run on its own, because the machine's speed drifted between them (SQL Server LocalDB took 30.1 ms for `Map<T>()` in the first run and 25.6 in the second).

[!include[](../../includes/perf/lib-real-time.md)]

Compare the two numbers of one run, not the numbers of different runs. The "against `Map<T>()`" columns do that: 1.05× means 5% longer than `Map<T>()` in the same run. The figure in brackets is BenchmarkDotNet's error margin in milliseconds. Where two bars' margins overlap, the runs do not tell them apart.

- **SQL Server LocalDB.** Dapper took 1.05 and 1.13 times as long as `Map<T>()`. In the second run the margins do not overlap (25.55 ± 1.15 against 28.81 ± 1.24 ms); in the first they do. Hand-written code and Dapper.AOT are within the noise of `Map<T>()` in both runs.
- **SQLite file.** Dapper took 1.14 and 1.08 times as long, with overlapping margins in both runs. Dapper.AOT was 1.06 times as long in the first run and 0.86 in the second, which is the noise of these runs.

<div class="perf-chart"><img src="../../images/perf/perf-lib-real-memory.svg" alt="Two panels of ranked bars on one scale, memory per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand-written selector, Dapper.AOT and Map of T 9.48 megabytes, Dapper 12.27. SQLite file: hand-written selector 11.96, Map of T 12.51, Dapper.AOT 12.72, Dapper 14.79."></div>

Memory is the same in both runs, because it is the library's allocation and not a timing.

[!include[](../../includes/perf/lib-real-memory.md)]

On SQL Server `Map<T>()` allocates exactly what hand-written code allocates, and Dapper 29.4% more. On SQLite `Map<T>()` allocates 4.6% more than a hand-written selector (12.51 against 11.96 MB), and Dapper 18.2% more than `Map<T>()`. The older run of this page, on an earlier build, showed `Map<T>()` slower than Dapper on SQLite. That no longer holds; see [Why SQLite looks different from the older run](#why-sqlite-looks-different-from-the-older-run).

The tables for the older run, which also had a hand loop on the provider's reader and a row class followed by `Select`, and a second run at 5 columns, are on [On real databases](real-databases.md).

## A wider set of everyday calls (older run)

The runs above read lists. This table is from an older run of the same project that covers more calls: a 12-column row, an in-memory provider, ODX 11 against Dapper 2.1.89, 1,000 rows per call, .NET 10.0.14, measured 2026-11-08. It is kept because the newer run covers only the list call, and not `First`, a scalar or a hand-written selector against Dapper. It is in [`RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md), which also has the 1-row and 100-row cases and the SQL Server and SQLite rows. It was not repeated on .NET 11 and has no data file. Read it as a picture from an earlier build.

| Call | Dapper | ODX | ODX time against Dapper | Dapper memory | ODX memory |
|---|---:|---:|---:|---:|---:|
| `List` | 100,275 ns | 38,959 ns | 0.39 | 355,048 B | 128,768 B |
| `Array` | 101,615 ns | 39,642 ns | 0.39 | 363,073 B | 120,192 B |
| `ListAsync` | 99,021 ns | 41,350 ns | 0.42 | 355,016 B | 128,944 B |
| `First` | 876 ns | 107 ns | 0.12 | 576 B | 280 B |
| Selector (a hand-written function) | 4,389 ns | 3,745 ns | 0.85 | 33,096 B | 33,176 B |
| Untyped rows | 108,936 ns | 98,712 ns | 0.91 | 411,688 B | 363,184 B |
| `Scalar` (one value) | 61 ns | 40 ns | 0.66 | 136 B | 168 B |

ODX is at or below Dapper's time in all 54 cases of that run, and a tie in a few (`ScalarAsync` 0.98). It allocates more than Dapper in a few small cases: `Scalar` and `Execute` by 32 bytes (the command object the fluent API returns), the hand-written selector by 80 bytes (33,176 against 33,096 B in the table), and at one row async scalar and untyped rows by 16 bytes each.

## Why `Map<T>()` is faster here

The library is built on one idea: hold the connection open as briefly as possible, and allocate as little as possible while it is open. Four design choices from [What's new in 11](../whats-new-11.md) are the likely reasons for the numbers above. These runs did not isolate each one:

- The mapping from columns to your type is compiled once for each result-set shape and cached. A call after the first does not look at the type again.
- Values are read with typed getters (`GetInt32`, `GetString`), so nothing is boxed.
- Terminals read straight from the reader, with no copy of every row first.
- Running a command allocates no closures, and a command built from a connection allocates no pool wrapper.

That also fits the memory column matching a hand-written selector on SQL Server and on the synthetic reader: nothing is left to allocate but the list and one object per row.

**Where the gain does not survive.** Once a real provider does the work, most of the time is the provider's. SQL Server LocalDB reads 10,000 rows in 24 to 31 ms whichever library maps them, so a 2.9-times lead on a synthetic reader becomes a lead of 5 to 13%. If your queries are slow because of the database, no mapping library will change that. The thing ODX still changes there is memory: on SQL Server 22.7% less than Dapper (Dapper allocates 29.4% more), on SQLite 15.4% less (Dapper allocates 18.2% more).

### Why SQLite looks different from the older run

The earlier version of this page was measured on builds from 2026-11-14 and showed `Map<T>()` 5.1% slower than Dapper on SQLite, and allocating 14.11 MB where a hand-written selector allocated 11.96 MB. The run on this page is from a later build (2026-11-21), and `Map<T>()` allocates 12.51 MB there, which is 1.6 MB less on the same rows. In between, a change removed a box per cell where SQLite hands over a different type than the member has: a `bool` stored as an integer, and a `DateTime` or a `decimal` stored as text. It is the likely reason for the difference; these runs did not isolate it. Hand-written code did not change (11.96 MB in both runs), so the gap to it shrank from 2.15 to 0.55 MB.

## Limits

- One machine, one run per case, except the real databases, where each provider was run twice. The error margins are shown; several are as large as the differences.
- Synthetic: 20 columns, 1,000 rows. Real databases: 20 columns, 10,000 rows. Other widths and row counts were measured on an earlier build and are on the other Benchmarks pages.
- Synchronous terminals only. Async is on [Sync reads against async reads](sync-vs-async.md).
- Dapper.AOT 1.1.0 was run through its intercepted path, as it is meant to be used. We did not look at how much set-up a program needs to use it.

## How this was measured

- **Question:** the same job through four libraries or hand-written code: read rows into objects.
- **Setup:** 20 typed columns, 1,000 rows on the synthetic reader; 20 columns and 10,000 rows on SQL Server LocalDB (`Microsoft.Data.SqlClient` 7.1.1) and a SQLite file (`Microsoft.Data.Sqlite` 10.0.14), each run twice, in the order SQLite, SQL Server, SQLite, SQL Server. Four methods: `Map<T>()`, `Map(selector)` with a hand-written selector, Dapper 2.1.89 `Query<T>`, and Dapper.AOT 1.1.0 `Query<T>`. Synchronous terminals. On SQLite, Dapper maps text to `Guid` with a type handler.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch. The runs began once nothing else was using the machine. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `b5b0014`, 2026-11-21, AMD Ryzen 9 7900, .NET 11.0.0, Windows 10. The older table: .NET 10.0.14, 2026-11-08, master `41af17c`. The charts of [On real databases](real-databases.md) and the other Benchmarks pages that this run did not repeat are from builds `b27a02f` and `fbb13d7`, 2026-11-14, and are marked as such there.
- **Data:** [library-synthetic.csv](../../perf-data/library-synthetic.csv) ([source](../../perf-data/library-synthetic.source.txt)), [library-real.csv](../../perf-data/library-real.csv) ([source](../../perf-data/library-real.source.txt)). The numbers were read from the summary table that BenchmarkDotNet printed at the end of each run.
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks.V1_Sync" "*SyntheticBenchmarks.V2_Sync" "*SyntheticBenchmarks.V7_V1_Sync" "*SyntheticBenchmarks.V7a_V1_Sync"`, with `ODX_SYN_WIDTHS=20` and `ODX_SYN_ROWS=1000`; and the methods `V1_Sync`, `V2_Sync`, `V7_Sync` and `V7a_Sync` of `RealProviderBenchmarks` for each real provider. See the [tools page](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/tools/perf/README.md) for the settings.
