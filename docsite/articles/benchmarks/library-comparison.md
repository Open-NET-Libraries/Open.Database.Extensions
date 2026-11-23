# ODX against Dapper and hand-written code

**The question:** for the same query and the same rows, how does `Map<T>()` compare with Dapper, with Dapper.AOT and with ADO.NET written by hand?

ODX is not an ORM, so EF Core is not on these charts.

## In short

- **With no database in the way**, `Map<T>()` holds the connection open for **2.6 times less time** than Dapper's `Query<T>` (76.1 against 199.9 µs) and allocates **2.4 times less memory** (227.4 against 548.1 KB). 20 columns, 1,000 rows, synthetic reader. Dapper's error margin here is ±0.6%.
- It allocates exactly what a hand-written selector and Dapper.AOT allocate (227.4 KB).
- **On a real database the gap is small, and it is in ODX's favor.** For 10,000 rows of 20 columns, Dapper took 11% and 11% longer than `Map<T>()` on SQL Server LocalDB in two runs, and 8% and 8% longer on a SQLite file. In none of those four runs do the error margins of the two overlap. What does not move between runs is memory: Dapper allocates 29.4% more on SQL Server and 18.2% more on SQLite.
- **Not faster than:** a hand-written selector, and on the synthetic reader Dapper.AOT. There Dapper.AOT holds the connection 29% less time than `Map<T>()` (54.4 against 76.1 µs) and a hand-written selector 47% less (40.7 µs). On a real database a hand-written selector takes 0.94 of the time of `Map<T>()` in all four runs; Dapper.AOT takes 0.99 and 1.00 of it on SQL Server and 1.03 on SQLite.

## With no database: a synthetic reader

<div class="perf-chart"><img src="../../images/perf/perf-summary.svg" alt="Two panels of four bars each for 20 columns and 1,000 rows on a synthetic reader. Connection held open: hand-written selector 40.7 microseconds, Dapper.AOT 54.4, Map of T 76.1, Dapper Query of T 199.9. Memory allocated: hand-written selector, Dapper.AOT and Map of T 227.4 kilobytes, Dapper 548.1."></div>

Each panel shows one thing about the same job, reading 1,000 rows of 20 columns into objects. The top panel is how long the connection is held open; the bottom panel is how much memory the call allocates. Shorter and smaller are better.

[!include[](../../includes/perf/summary.md)]

Two of the four are for context. **Hand-written selector** is `Map(selector)` with a function you write yourself: the fastest of the four here, and what `Map<T>()` is compared against. **Dapper.AOT** is Dapper's ahead-of-time variant: the same `Query<T>` call, with the code that reads the rows generated when you build instead of when the program runs. It is faster than `Map<T>()` on this reader, and it allocates the same.

The error margin in the table is BenchmarkDotNet's own, the half-width of a 99.9% confidence interval, as a share of the mean. It is largest for `Map<T>()` (±0.7%) and smallest for Dapper.AOT (±0.2%).

## On real databases

<div class="perf-chart"><img src="../../images/perf/perf-lib-real-time.svg" alt="Two panels of ranked bars on one scale, the mean of two runs of time per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand-written selector 24.6 milliseconds, Dapper.AOT 26.0, Map of T 26.1, Dapper 28.9. SQLite file: hand-written selector 26.0, Map of T 27.7, Dapper.AOT 28.5, Dapper 29.8."></div>

The same job on a real provider, with 10,000 rows of 20 columns. A real provider spends its own time on every row, so the large gaps of the synthetic reader shrink to 8 to 11% for Dapper. Each bar is the mean of two runs; the table below has each run on its own.

[!include[](../../includes/perf/lib-real-time.md)]

Compare the two numbers of one run, not the numbers of different runs. The "against `Map<T>()`" columns do that: 1.11× means 11% longer than `Map<T>()` in the same run. The figure in brackets is BenchmarkDotNet's error margin in milliseconds. Where two bars' margins overlap, the runs do not tell them apart.

- **SQL Server LocalDB.** Dapper took 1.11 times as long as `Map<T>()` in both runs, and the margins do not overlap in either (26.13 ± 0.65 against 29.01 ± 0.58 ms; 26.07 ± 1.02 against 28.84 ± 0.49 ms). Dapper.AOT is within the noise of `Map<T>()` in both runs. The hand-written selector took 0.94 times as long in both; its margins clear those of `Map<T>()` in the first run (24.58 ± 0.70 ms) and overlap them in the second.
- **SQLite file.** Dapper took 1.08 times as long in both runs, and the margins do not overlap in either. Dapper.AOT took 1.03 times as long in both, with margins that just touch those of `Map<T>()`. The hand-written selector took 0.94 times as long in both.

<div class="perf-chart"><img src="../../images/perf/perf-lib-real-memory.svg" alt="Two panels of ranked bars on one scale, memory per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand-written selector, Dapper.AOT and Map of T 9.48 megabytes, Dapper 12.27. SQLite file: hand-written selector 11.96, Map of T 12.51, Dapper.AOT 12.72, Dapper 14.79."></div>

Memory is the same in both runs, because it is the library's allocation and not a timing.

[!include[](../../includes/perf/lib-real-memory.md)]

On SQL Server `Map<T>()` allocates exactly what hand-written code allocates, and Dapper 29.4% more. On SQLite `Map<T>()` allocates 4.6% more than a hand-written selector (12.51 against 11.96 MB), and Dapper 18.2% more than `Map<T>()`.

The tables with a hand loop on the provider's reader, a factory map to the domain object and a run at 5 columns are on [On real databases](real-databases.md).

## A wider set of everyday calls (older run)

The runs above read lists. This table is from an older run of the same project that covers more calls: a 12-column row, an in-memory provider, ODX 11 against Dapper 2.1.89, 1,000 rows per call, .NET 10.0.14, measured 2026-11-08. It covers calls the newer run does not measure. It is in [`RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md), which also has the 1-row and 100-row cases and the SQL Server and SQLite rows. It was not repeated on .NET 11 and has no data file.

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

## Where the gain does not survive

Once a real provider does the work, most of the time is the provider's. SQL Server LocalDB reads 10,000 rows in 25 to 29 ms whichever library maps them, so a 2.6-times lead on a synthetic reader becomes a lead of 11%. If your queries are slow because of the database, no mapping library will change that. The thing ODX still changes there is memory: on SQL Server 22.7% less than Dapper (Dapper allocates 29.4% more), on SQLite 15.4% less (Dapper allocates 18.2% more).

## Limits

- One machine, one run per case, except the real databases, where each provider was run twice. The error margins are shown; several are as large as the differences.
- Synthetic: 20 columns, 1,000 rows. Real databases: 20 columns, 10,000 rows. Other widths and row counts are on the other Benchmarks pages.
- Synchronous terminals only. Async is on [Sync reads against async reads](sync-vs-async.md).
- Dapper.AOT 1.1.0 was run through its intercepted path, as it is meant to be used. We did not look at how much set-up a program needs to use it.

## How this was measured

- **Question:** the same job through four libraries or hand-written code: read rows into objects.
- **Setup:** 20 typed columns, 1,000 rows on the synthetic reader; 20 columns and 10,000 rows on SQL Server LocalDB (`Microsoft.Data.SqlClient` 7.1.1) and a SQLite file (`Microsoft.Data.Sqlite` 10.0.14), each run twice, in the order SQLite, SQL Server, SQLite, SQL Server. Four methods: `Map<T>()`, `Map(selector)` with a hand-written selector, Dapper 2.1.89 `Query<T>`, and Dapper.AOT 1.1.0 `Query<T>`. Synchronous terminals. On SQLite, Dapper maps text to `Guid` with a type handler.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch. The runs began once nothing else was using the machine. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `6a770e8`, 2026-11-23, AMD Ryzen 9 7900, .NET 11.0.0, Windows 10. The older table: .NET 10.0.14, 2026-11-08, master `41af17c`.
- **Data:** [library-synthetic.csv](../../perf-data/library-synthetic.csv) ([source](../../perf-data/library-synthetic.source.txt)), [library-real.csv](../../perf-data/library-real.csv) ([source](../../perf-data/library-real.source.txt)). The numbers were read from the summary table that BenchmarkDotNet printed at the end of each run.
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks.V1_Sync" "*SyntheticBenchmarks.V2_Sync" "*SyntheticBenchmarks.V7_V1_Sync" "*SyntheticBenchmarks.V7a_V1_Sync"`, with `ODX_SYN_WIDTHS=20` and `ODX_SYN_ROWS=1000`; and the methods `V1_Sync`, `V2_Sync`, `V7_Sync` and `V7a_Sync` of `RealProviderBenchmarks` for each real provider. See the [tools page](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/tools/perf/README.md) for the settings.
