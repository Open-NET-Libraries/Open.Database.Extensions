# Performance

<a id="headline"></a>
![Four headline ratios for the RowMap plan, Map of T, on a synthetic reader with 20 columns and 1,000 rows: 2.48 times less reader-open time than Dapper, 2.41 times fewer bytes allocated than Dapper, 2.19 times faster than dumping rows into object arrays, and 1.74 times slower than a hand-written selector.](../images/perf/perf-summary.svg)

The RowMap plan (`Map<T>()`) holds the connection open 2.48 times less than Dapper does and allocates 2.41 times fewer bytes, on a synthetic reader with no I/O. It is 1.74 times slower than a hand-written selector. The hand-written selector, the `object[]` rows and Dapper are on these charts for context: they show how far the plan is from the floor and from the common alternatives, and the sections below show each comparison in full.

Every number on this page comes from a BenchmarkDotNet result file, one run per case, so small gaps are noise. The build, the date and the machine of each data set are listed under [How fresh the data is](performance-data.md#how-fresh-the-data-is). [The numbers behind the headline](performance-data.md#headline) | [CSV](../perf-data/synthetic-overhead.csv)

<a id="open-time"></a>
## Reader-open time

![Ranked bars of reader-open time per call, shortest first, 16 ways of reading 20 columns and 1,000 rows. The hand-written selector is fastest at 46.8 microseconds, the RowMap plan is 81.6, the row class with an inline Select is 106.4, object arrays are 178.4 and Dapper is 202.4 to 257.6.](../images/perf/perf-synthetic-open.svg)

Reader-open time is how long the connection is held, from `ExecuteReader` until the reader closes, and it is the number that matters most for a busy service. The plan takes 81.6 µs: 2.48 times less than Dapper's plain `Query<T>` (202.4 µs) and 2.19 times less than dumping every row into `object[]` (178.4 µs). The hand-written selector is the floor at 46.8 µs, and the plan is 1.74 times that.

The Select and shaping variants are here because the usual way to get a second object per row is a row class followed by `Select`. With the reader open that costs 106.4 µs, 30% more than the plan. Every Dapper variant takes between 197.5 and 257.6 µs, so even Dapper's best is 2.4 times the plan. The two variants that run `Select` after the reader closes (82.1 and 82.3 µs) look as fast as the plan on this chart only because their remaining work happens after the close; the next chart shows it. [Data table](performance-data.md#open-time) | [CSV](../perf-data/synthetic-overhead.csv)

<a id="total-time"></a>
## Total time per call

![Ranked bars of total time per call, shortest first, the same 16 variants. The hand-written selector is 47.0 microseconds, the RowMap plan 81.7, the row class then Select after the close 97.8, object arrays 178.5 and Dapper 202.7 to 257.9.](../images/perf/perf-synthetic-total.svg)

Total time includes the work done after the reader closes. Where it is far above reader-open time, the work moved out of the connection's lifetime but not out of the call: a row class followed by `Select` after the close takes 97.8 µs in total, 20% more than the plan, and shaping after the close takes 134.5 µs. The plan's total of 81.7 µs is 2.48 times less than Dapper's 202.7 µs.

The page shows both because they answer different questions: reader-open time is the cost to the database and the connection pool, total time is the cost to the caller. [Data table](performance-data.md#total-time) | [CSV](../perf-data/synthetic-overhead.csv)

<a id="memory"></a>
## Memory allocated per call

![Ranked bars of memory allocated per call, smallest first, the same 16 variants. The RowMap plan and the hand-written selector allocate 227.4 kilobytes; a row class with Select 438.7; object arrays 516.6; Dapper 548.1 to 837.4.](../images/perf/perf-synthetic-memory.svg)

The RowMap plan and the hand-written selector allocate the same 227.4 KB: the list and one object per row. A row class followed by `Select` allocates about twice that (438.7 KB), because every row exists twice, and Dapper's plain `Query<T>` allocates 548.1 KB, 2.41 times the plan. Allocation is deterministic, so unlike the timings it does not depend on how busy the machine was. [Data table](performance-data.md#memory) | [CSV](../perf-data/synthetic-overhead.csv)

<a id="width"></a>
## Cost grows with the number of columns

![A line chart of total time per call against 1 to 20 columns at 1,000 rows, five approaches. Every line is close to straight. At 20 columns the order from fastest is the hand-written selector at 47.0 microseconds, the RowMap plan 81.7, a row class with Select 106.7, object arrays 178.5 and Dapper 202.7.](../images/perf/perf-width.svg)

Every approach grows close to a straight line, so the slope is its cost per cell: 1.91 ns for the hand-written selector, 3.65 ns for the plan, 4.12 ns for a row class with `Select`, 8.90 ns for `object[]` rows and 9.70 ns for Dapper. The `object[]` rows are the fastest approach at one column (9.5 µs, against 12.4 µs for the plan) because they do the least fixed work, and the second slowest at twenty, because every cell is boxed. The chart is here to show that the plan's lead over `object[]` rows and Dapper grows with the width of the result. [Data table](performance-data.md#width) | [CSV](../perf-data/width-sweep.csv)

<a id="map-cost"></a>
## What building a map costs

![Bars of the one-time cost of building and compiling a map: 0.19 milliseconds for 1 column, 0.33 for 5 columns and 0.87 for 20 columns.](../images/perf/perf-map-cost.svg)

Every number above uses a map that ODX builds once per result shape and caches. Building and compiling a map costs 0.19 ms at one column, 0.33 ms at five and 0.87 ms at twenty, and allocates 15 to 52 KB. A one-row call that reuses the cached map takes 0.17 to 0.27 µs, against 192 to 875 µs for a call that builds a new map every time: a map is built once, and this is why ODX does not build one per call. [Data table](performance-data.md#map-cost) | [CSV](../perf-data/map-cost.csv)

<a id="real-time"></a>
## On real databases: time per call

![Two panels of ranked bars on one scale, time per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand-written selector 25.2 milliseconds, hand loop 25.6, RowMap plan 25.9, row class with Select 27.2, Dapper 27.6. SQLite file: hand-written selector 25.9, hand loop 25.9, Dapper 29.8, row class with Select 30.9, RowMap plan 31.3.](../images/perf/perf-real-time.svg)

The same mapping paths on SQL Server LocalDB (through `Microsoft.Data.SqlClient`) and on a SQLite file, 20 columns and 10,000 rows. A real provider spends its own time on every row, so the large gaps of the synthetic reader shrink. On SQL Server the plan (25.9 ms) is 2.9% behind the hand-written selector (25.2 ms) and 6.4% ahead of Dapper (27.6 ms); BenchmarkDotNet's own error margin on these runs is 2 to 9% of the mean, so these are ties. The hand loop is the leanest reader loop possible, written directly on the provider's reader: it is the reference the others are measured against.

SQLite is the one place on this page where the plan is behind. It takes 31.3 ms, 20.9% more than the hand-written selector (25.9 ms) and 5.1% more than Dapper (29.8 ms). These runs do not show why. [Data table](performance-data.md#real-time) | [CSV](../perf-data/real-providers.csv)

<a id="real-memory"></a>
## On real databases: memory per call

![Two panels of ranked bars on one scale, memory per call at 20 columns and 10,000 rows. SQL Server LocalDB: hand loop, RowMap plan and hand-written selector 9.48 megabytes, row class with Select 11.54, Dapper 12.27. SQLite file: hand loop and hand-written selector 11.96, RowMap plan 14.11, Dapper 14.79, row class with Select 16.17.](../images/perf/perf-real-memory.svg)

Memory differences do not shrink on a real provider. On SQL Server the plan, the hand-written selector and the hand loop all allocate 9.48 MB; a row class with `Select` allocates 21.7% more and Dapper 29.4% more. On SQLite the plan allocates 14.11 MB, which is 17.9% more than the hand-written selector (11.96 MB) and 4.6% less than Dapper (14.79 MB). [Data table](performance-data.md#real-memory) | [CSV](../perf-data/real-providers.csv)

<a id="sync-async"></a>
## Sync reads against async reads

![Four panels of three bars, each panel on its own scale, reader-open time of the RowMap plan at 20 columns and 1,000 rows. With neither a refill nor a busy pool: sync 77.3 microseconds, async execute 80.9, async read 82.3. With a busy pool: 94.4, 97.4, 102.4. With buffer refills: 257.9, 261.2, 314.3. With refills and a busy pool: 279.3, 279.0 and 11.2 milliseconds.](../images/perf/perf-sync-async.svg)

The same plan, read three ways: sync (`ExecuteReader`, then `Read()`), async execute (`ExecuteReaderAsync`, then `Read()`, which is what the async terminals do by default) and async read (`ReadAsync()` for every row, the opt-in now called `AsyncReads()`). Each panel is one condition on its own scale, so compare the bars inside a panel, not across panels. With neither a refill nor a busy pool, async read costs 6.5% more than sync: that is the async state machine. With a refill every 100 rows and a busy thread pool, each refill makes `ReadAsync()` wait for a free thread while the reader stays open: 11.2 ms against 0.28 ms for sync, 40 times longer.

This is a model: a synthetic reader with an artificial refill shows the mechanism, not what a given server will do. The real-provider measurements, and the reasons the default is sync, are in [Row reads: sync by default, AsyncReads() to opt in](https://open-net-libraries.github.io/Open.Database.Extensions/articles/async-reads.html). Allocations are within 4 KB of each other in every case. [Data table](performance-data.md#sync-async) | [CSV](../perf-data/sync-async.csv)

## Where it is not faster

- On SQLite, at 10,000 rows of 20 columns, the plan is slower than Dapper and than a hand-written selector.
- On SQL Server the plan, a hand-written selector and Dapper are within the noise of one run; the difference you can count on there is allocation.
- `Retrieve().DequeueAs<T>()` is deprecated: it copies every row to `object[]` first and maps later, so it is not faster than a plain loop. Use `Map<T>()`.
- `ToChannel` and the Dataflow methods are not faster than reading rows in a plain loop. They exist to feed pipelines: see [Channels and Dataflow](channels-and-dataflow.md).

## Why `Map<T>()` is cheaper

The library rests on one idea: keep a connection open for as short a time as possible and allocate as little as possible while it is. The changes behind the numbers are listed in [What's new in 11](whats-new-11.md): the mapping is compiled once per result-set shape and cached, values are read with typed getters (no boxing), terminals read straight from the reader, execution allocates no closures, and a command built from a connection allocates no pool wrapper.

## What is measured

The benchmarks live in [`Benchmarks/`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/Benchmarks) and compare this library with Dapper 2.1.89 on the same job, with BenchmarkDotNet 0.15.8 in process on .NET 11.0, against three providers:

| Provider | What it tells you |
|---|---|
| A synthetic in-memory ADO.NET reader, 20 typed columns, 1,000 rows | The library's own overhead, with no I/O |
| SQL Server LocalDB (`Microsoft.Data.SqlClient`) | A real provider, where the database dominates the time |
| A SQLite file (`Microsoft.Data.Sqlite`) | A second real provider |

On SQLite, Dapper maps text to `Guid` through the usual `SqlMapper.TypeHandler` recipe; it does not convert it on its own.

## How fresh the data is

The charts and the tables are generated from CSV files. Each CSV has a source file beside it that names the benchmark, the build, the date, the machine and the result file of every number. The build and the date of each data set are listed on the [data page](performance-data.md#how-fresh-the-data-is). To refresh the charts after a new run, see the [README next to the generator](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf).

The same ranking as one interactive page, with hover details, is kept as a snapshot: [Interactive ranking (snapshot)](../benchmarks/ranking.html), measured 2026-11-14, before 11.0.0-rc.6. It is replaced when a fresh run replaces the data on this page.

## Limits of these numbers

- The synthetic reader has no network or server time, so do not read its ratios as end-to-end gains.
- Each case is one BenchmarkDotNet run; on the real providers its reported error margin is 2 to 9% of the mean, so gaps of a few percent are noise.
- Reader-open time comes from a timer inside the benchmark, around the life of the reader; total time is the BenchmarkDotNet mean of the whole call.
- Results vary by machine, runtime and data shape. Run the benchmarks on yours:

```sh
dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*"
```
