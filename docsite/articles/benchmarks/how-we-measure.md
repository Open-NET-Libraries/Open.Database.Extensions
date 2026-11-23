# How we measure

Every page in [Benchmarks](../performance.md) is measured the same way. This page says how, so the other pages can stay short.

## In short

- Almost every number comes from [BenchmarkDotNet](https://benchmarkdotnet.org/) 0.15.8, run in Release mode on one desktop PC, with every case run once. The exceptions are [Slow links and large values](slow-links-and-large-values.md), which uses the project's own probe programs against SQL Server in a container, and the bin tests on [Pooling and clearing](pooling-and-clearing.md); those pages say so.
- All the rows of a result come back as the same objects from every library. A check in the benchmark project (`--verify`) fails if two variants return different data.
- Most runs use a synthetic reader, so they show the library's own cost with no database in the way. Two runs use a real database (SQL Server LocalDB and a SQLite file), and they show how much of that cost survives.
- Gaps of a few percent between two bars are noise. We say so on each page where it applies.

## What the three numbers mean

| Number | What it is | Where it comes from |
|---|---|---|
| **Connection held open** | How long the reader is open: from the moment `ExecuteReader` is entered until the reader is closed. The connection can serve nobody else in that time, so this is the cost a busy service pays. | A stopwatch inside the benchmark that wraps the reader (the `reader_open_us` column of the data files). |
| **Whole call** | How long the call takes from start to finish, including work that happens after the reader has closed. | BenchmarkDotNet's `Mean` column. |
| **Memory allocated** | The managed memory the call allocates, in KB (1 KB is 1,024 bytes; 1 MB is 1,024 KB; B is a byte). | BenchmarkDotNet's `MemoryDiagnoser`, the `Allocated` column: bytes per call. |

Allocation is deterministic, so memory does not depend on how busy the machine was. Time does.

## What the benchmarks read from

| Provider | What it is | What it tells you |
|---|---|---|
| **Synthetic reader** | An in-memory ADO.NET reader that serves up to 20 typed columns (`int`, `long`, `decimal`, `double`, `bool`, `DateTime`, `Guid`, short and long strings, `byte[]`, some of them with `NULL`s), generated from the row number so every run gets the same data. No network, no disk, no server. | The library's own overhead. |
| **SQL Server LocalDB** | A database created for the run, read through `Microsoft.Data.SqlClient` 7.1.1, and deleted after it. | A real provider where the database does most of the work. |
| **SQLite file** | A file created for the run, read through `Microsoft.Data.Sqlite`, and deleted after it. | A second real provider. |

The synthetic reader can also add a wait every N rows (a buffer refill) and keep the thread pool busy, so a benchmark can model a slow link. That is how [Sync reads against async reads](sync-vs-async.md) is built.

## Fairness rules

1. **Same job.** Every approach on a page runs the same query and returns the same rows as the same kind of objects. `--verify` compares them.
2. **Warm.** Each case runs 5 warm-up iterations, then 12 measured ones, in one launch. The compiled mapping is built before the first measured call, except in the one benchmark that measures building it ([What building a map costs](map-cost.md)).
3. **One at a time.** Each benchmark runs alone, on a machine with nothing else running.
4. **Dapper is written the way its documentation shows.** `Query<T>` for a list, and the usual type handler recipe where it needs one (on SQLite, text to `Guid`). Dapper.AOT is run through its intercepted path.
5. **Reference points are labelled.** A *hand loop* is a loop written directly on the provider's reader with typed getters. It is the reference point, not a limit: on SQL Server at 20 columns a hand-written selector came out 1.9% slower than it, which is within the noise.

## What we do not measure

- **Network and server time** in the synthetic runs. They have none, so do not read their ratios as end-to-end gains. The real-database pages show how the ratios shrink when a real provider does the work.
- **Cold start.** The runs are warmed. The one exception is the cost of building a map, which has its own page.
- **Other machines.** All the data was run on one PC. Run the benchmarks on yours.

## How sure are the numbers?

Each case is one BenchmarkDotNet run. On the real providers BenchmarkDotNet reported error margins (the half-width of its 99.9% confidence interval) of 0.3 to 6.3% of the mean, and the tables show each one, so a gap of a few percent between two bars is not a result. Allocation does not have this problem: it is the same every time.

## How fresh is the data?

Each data set has a CSV file in the repository and a source file beside it that names the benchmark, the build, the date, the machine and the result file of every number. The build is the commit of the benchmark project that ran; the date is when the run finished.

[!include[](../../includes/perf/freshness.md)]

Data files and sources: [library-synthetic](../../perf-data/library-synthetic.csv) ([source](../../perf-data/library-synthetic.source.txt)), [library-real](../../perf-data/library-real.csv) ([source](../../perf-data/library-real.source.txt)), [synthetic-overhead](../../perf-data/synthetic-overhead.csv) ([source](../../perf-data/synthetic-overhead.source.txt)), [width-sweep](../../perf-data/width-sweep.csv) ([source](../../perf-data/width-sweep.source.txt)), [map-cost](../../perf-data/map-cost.csv) ([source](../../perf-data/map-cost.source.txt)), [real-providers](../../perf-data/real-providers.csv) ([source](../../perf-data/real-providers.source.txt)), [sync-async](../../perf-data/sync-async.csv) ([source](../../perf-data/sync-async.source.txt)).

Each page gives the build and date of its runs on its *Build, date, machine* line, and says so at the top when it rests on older or hand-recorded numbers.

The [interactive ranking](../../benchmarks/ranking.html) shows the 16-variant synthetic run as one page with hover details. It is a snapshot from 2026-11-23.

## Reproduce

```sh
dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*"
```

To rebuild the charts and the tables on these pages from new CSV files, see the [README next to the generator](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf).
