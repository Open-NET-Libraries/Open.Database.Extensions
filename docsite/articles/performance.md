# Benchmarks

ODX is built on one idea: hold the connection open for as short a time as possible, and allocate as little as possible while it is open. These pages show what that buys, where it buys nothing, and how every number was made.

## In short

- **With no database in the way**, `Map<T>()` holds the connection open for **2.9 times less time** than Dapper (84.9 against 246.0 µs) and allocates **2.4 times less memory** (227.4 against 548.1 KB). 20 columns, 1,000 rows, synthetic reader.
- **Against hand-written code** it takes 1.9 times as long as a hand-written selector (the function you pass to `Map(selector)` that reads one row into your type, 44.8 µs) and allocates the same memory.
- **On a real database the gap is small, and still in ODX's favor.** For 10,000 rows of 20 columns Dapper took 5 to 13% longer than `Map<T>()` on SQL Server LocalDB and 8 to 14% longer on a SQLite file (two runs each, with overlapping error margins in three of the four). What you can count on is memory: Dapper allocates 29.4% more on SQL Server and 18.2% more on SQLite.
- **Where ODX is not faster:** Dapper.AOT and a hand-written selector hold the connection 25% and 47% less time than `Map<T>()` on the synthetic reader (and are not measurably different on a real database); the opt-in async read costs more than the default; channels and Dataflow are slower than a plain loop. [The full list is below.](#where-it-is-not-faster)
- **Reading rows synchronously is the default on purpose.** Over a real link it took the same time as `ReadAsync()`, and on `Map(Func)` it allocated 42% less memory.

<div class="perf-chart"><img src="../images/perf/perf-summary.svg" alt="Two panels of four bars each for 20 columns and 1,000 rows on a synthetic reader. Connection held open: hand-written selector 44.8 microseconds, Dapper.AOT 63.4, Map of T 84.9, Dapper Query of T 246.0. Memory allocated: hand-written selector, Dapper.AOT and Map of T 227.4 kilobytes, Dapper 548.1."></div>

How long the connection is held open (top) and how much memory is allocated (bottom) for the same job, four ways. Shorter and smaller are better. These are the best case, with no database: the [real databases](benchmarks/real-databases.md) page shows how much survives a real provider.

## What was asked, and the answer

A *terminal* is the call that ends a query and gives you the rows: `ToListAsync()`, `ToArrayAsync()`, `First()` and the like. Each row is one page. Every page has the same order: the answer first, then the chart and the numbers, then why, then how it was measured.

| What we asked | The answer in a few words | Page |
|---|---|---|
| How does `Map<T>()` compare with Dapper and hand-written code? | 2.9 times less connection time and 2.4 times less memory than Dapper, with no database; a little ahead of Dapper on real databases; behind Dapper.AOT and a hand-written selector | [ODX against Dapper and hand-written code](benchmarks/library-comparison.md) |
| Which of 16 ways of mapping is best? | A hand-written selector holds the connection least; three selector recipes you write yourself come next; `Map<T>()` is the fastest that needs no code | [Every way of mapping, ranked](benchmarks/mapping-variants.md) |
| Does the lead survive a real database? | Partly: Dapper takes 5 to 14% longer on SQL Server and SQLite, with error margins that overlap in most runs, and allocates 18 to 29% more | [ODX against Dapper](benchmarks/library-comparison.md#on-real-databases), and the older run on [On real databases](benchmarks/real-databases.md) |
| What does a wider result cost? | `Map<T>()` costs 3.65 ns per cell (one value in one row); Dapper 9.70 ns | [Cost grows with the number of columns](benchmarks/cost-per-column.md) |
| What does it cost to build a mapping? | 0.19 to 0.87 ms, once; never per call | [What building a map costs](benchmarks/map-cost.md) |
| Sync reads or async reads? | Sync is cheapest or tied in every case measured; async can wait on a busy thread pool | [Sync reads against async reads](benchmarks/sync-vs-async.md) |
| What happens on a slow link or with a 100 MB value? | The link sets the time; the sync read allocates less; one async path took 5.4 times as long | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| Are channels and Dataflow faster than a loop? | No: 1.26 and 2.1 times a plain loop at 1,000 rows (older run) | [Channels and Dataflow against a plain loop](benchmarks/pipelines.md) |
| Which pool, and how are rows cleared? | Plain `ArrayPool` arrays; 2 to 5% faster end to end; always clearing reference rows costs 3 to 4 µs on a one-row call and nothing you can see from 100 rows | [Pooling and clearing](benchmarks/pooling-and-clearing.md) |

Also: [How we measure](benchmarks/how-we-measure.md) (the method, the settings, how fresh the data is), the [data files](performance-data.md), and the [interactive ranking](../benchmarks/ranking.html) of the 16 synthetic variants, a snapshot of 2026-11-14.

## Where it is not faster

- Dapper.AOT and a hand-written selector are faster than `Map<T>()` on the synthetic reader (63.4 and 44.8 against 84.9 µs of connection time). On SQL Server and SQLite, at 10,000 rows of 20 columns, they are within the noise of one run. [ODX against Dapper and hand-written code](benchmarks/library-comparison.md).
- On a SQLite file an earlier build of `Map<T>()` was behind Dapper; the current build is not. [On real databases](benchmarks/real-databases.md) keeps that older run and says what changed.
- A hand-written selector is faster than `Map<T>()` at every width, and `object[]` rows are faster at one column. [Cost per column](benchmarks/cost-per-column.md).
- `AsyncReads()` costs more than the default. With a 20 µs wait every 100 rows and a busy thread pool it took 40 times as long (a synthetic model); with only a busy pool, no refill, it took 8% more than a fully sync read (102.4 against 94.4 µs). [Sync reads against async reads](benchmarks/sync-vs-async.md).
- `ToChannel` and the Dataflow methods are not faster than reading rows in a plain loop. They exist to feed pipelines: see [Channels and Dataflow](channels-and-dataflow.md).
- `Map<T>()` reads each row once, straight into the model. A route that copies every row to `object[]` first and maps later took 1.29 and 1.33 times as long as a plain loop at 100 and 1,000 rows in an older run (6% less at one row; [`RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md)).

## Limits of these numbers

- The synthetic reader has no network or server time, so do not read its ratios as end-to-end gains.
- Each case is one run. On the real providers BenchmarkDotNet reported error margins of 0.4 to 21% of the mean (the tables show each one), so gaps of a few percent are noise.
- Results vary by machine, runtime and data shape, and all of this was run on one PC. [Run the benchmarks on yours.](benchmarks/how-we-measure.md#reproduce)
