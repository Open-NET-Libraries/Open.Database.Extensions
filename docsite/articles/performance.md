# Benchmarks

ODX is built on one idea: hold the connection open for as short a time as possible, and allocate as little as possible while it is open. These pages show what that buys, where it buys nothing, and how every number was made.

## In short

- **With no database in the way**, `Map<T>()` holds the connection open for **2.5 times less time** than Dapper (81.6 against 202.4 µs) and allocates **2.4 times less memory** (227.4 against 548.1 KB). 20 columns, 1,000 rows, synthetic reader.
- **Against hand-written code** it takes 1.74 times as long as a hand-written selector (the function you pass to `Map(selector)` that reads one row into your type, 46.8 µs) and allocates the same memory.
- **On a real database the gap closes.** On SQL Server LocalDB, `Map<T>()`, hand-written code and Dapper are within the noise of one run for 10,000 rows; what you can count on is memory, where Dapper allocates 29.4% more than `Map<T>()`.
- **Where ODX is not faster:** on a SQLite file `Map<T>()` is 20.9% behind a hand-written selector and 5.1% behind Dapper; the opt-in async read costs more than the default; channels and Dataflow are slower than a plain loop. [The full list is below.](#where-it-is-not-faster)
- **Reading rows synchronously is the default on purpose.** Over a real link it took the same time as `ReadAsync()`, and on `Map(Func)` it allocated 42% less memory.

<div class="perf-chart"><img src="../images/perf/perf-summary.svg" alt="Two panels of four bars each for 20 columns and 1,000 rows on a synthetic reader. Connection held open: hand-written selector 46.8 microseconds, Map of T 81.6, object arrays 178.4, Dapper Query of T 202.4. Memory allocated: Map of T and the hand-written selector 227.4 kilobytes, object arrays 516.6, Dapper 548.1."></div>

How long the connection is held open (top) and how much memory is allocated (bottom) for the same job, four ways. Shorter and smaller are better. These are the best case, with no database: the [real databases](benchmarks/real-databases.md) page shows how much survives a real provider.

## What was asked, and the answer

A *terminal* is the call that ends a query and gives you the rows: `ToListAsync()`, `ToArrayAsync()`, `First()` and the like. Each row is one page. Every page has the same order: the answer first, then the chart and the numbers, then why, then how it was measured.

| What we asked | The answer in a few words | Page |
|---|---|---|
| How does `Map<T>()` compare with Dapper and hand-written code? | 2.5 times less connection time and 2.4 times less memory than Dapper, with no database; behind a hand-written selector | [ODX against Dapper and hand-written code](benchmarks/library-comparison.md) |
| Which of 16 ways of mapping is best? | A hand-written selector holds the connection least; three selector recipes you write yourself come next; `Map<T>()` is the fastest that needs no code | [Every way of mapping, ranked](benchmarks/mapping-variants.md) |
| Does the lead survive a real database? | A tie on SQL Server within noise; less memory; behind on SQLite | [On real databases](benchmarks/real-databases.md) |
| What does a wider result cost? | `Map<T>()` costs 3.65 ns per cell (one value in one row); Dapper 9.70 ns | [Cost grows with the number of columns](benchmarks/cost-per-column.md) |
| What does it cost to build a mapping? | 0.19 to 0.87 ms, once; never per call | [What building a map costs](benchmarks/map-cost.md) |
| Sync reads or async reads? | Sync is cheapest or tied in every case measured; async can wait on a busy thread pool | [Sync reads against async reads](benchmarks/sync-vs-async.md) |
| What happens on a slow link or with a 100 MB value? | The link sets the time; the sync read allocates less; one async path took 5.4 times as long | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| Are channels and Dataflow faster than a loop? | No: 1.26 and 2.1 times a plain loop at 1,000 rows (older run) | [Channels and Dataflow against a plain loop](benchmarks/pipelines.md) |
| Which pool, and how are rows cleared? | Plain `ArrayPool` arrays; 2 to 5% faster end to end; always clearing reference rows costs 3 to 4 µs on a one-row call and nothing you can see from 100 rows | [Pooling and clearing](benchmarks/pooling-and-clearing.md) |

Also: [How we measure](benchmarks/how-we-measure.md) (the method, the settings, how fresh the data is), the [data files](performance-data.md), and the [interactive ranking](../benchmarks/ranking.html) of the 16 synthetic variants, a snapshot of 2026-11-14.

## Where it is not faster

- On SQLite, at 10,000 rows of 20 columns, `Map<T>()` is slower than Dapper and than a hand-written selector. [On real databases](benchmarks/real-databases.md).
- On SQL Server, `Map<T>()`, a hand-written selector and Dapper are within the noise of one run. The difference you can count on there is allocation.
- A hand-written selector is faster than `Map<T>()` at every width, and `object[]` rows are faster at one column. [Cost per column](benchmarks/cost-per-column.md).
- `AsyncReads()` costs more than the default. With a 20 µs wait every 100 rows and a busy thread pool it took 40 times as long (a synthetic model); with only a busy pool, no refill, it took 8% more than a fully sync read (102.4 against 94.4 µs). [Sync reads against async reads](benchmarks/sync-vs-async.md).
- `ToChannel` and the Dataflow methods are not faster than reading rows in a plain loop. They exist to feed pipelines: see [Channels and Dataflow](channels-and-dataflow.md).
- `Retrieve().DequeueAs<T>()` is deprecated: it copies every row to `object[]` first and maps later. In an older run it took 1.29 and 1.33 times as long as a plain loop at 100 and 1,000 rows (6% less at one row; [`RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md)). Use `Map<T>()`.

## Limits of these numbers

- The synthetic reader has no network or server time, so do not read its ratios as end-to-end gains.
- Each case is one run. On the real providers BenchmarkDotNet reported error margins of 0.4 to 8.8% of the mean (the tables show each one), so gaps of a few percent are noise.
- Results vary by machine, runtime and data shape, and all of this was run on one PC. [Run the benchmarks on yours.](benchmarks/how-we-measure.md#reproduce)
