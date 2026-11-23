# Benchmarks

ODX is built on one idea: hold the connection open for as short a time as possible, and allocate as little as possible while it is open.
## In short

- **With no database in the way**, `Map<T>()` holds the connection open for **2.6 times less time** than Dapper (76.1 against 199.9 µs) and allocates **2.4 times less memory** (227.4 against 548.1 KB). 20 columns, 1,000 rows, synthetic reader.
- **Against hand-written code** it takes 1.9 times as long as a hand-written selector (the function you pass to `Map(selector)` that reads one row into your type, 40.7 µs) and allocates the same memory.
- **On a real database the gap is small, and still in ODX's favor.** For 10,000 rows of 20 columns Dapper took 11% longer than `Map<T>()` on SQL Server LocalDB and 8% longer on a SQLite file (two runs each, with no overlap between the error margins in any of the four). What you can count on is memory: Dapper allocates 29.4% more on SQL Server and 18.2% more on SQLite.
- **Where ODX is not faster:** Dapper.AOT and a hand-written selector hold the connection 29% and 47% less time than `Map<T>()` on the synthetic reader (on a real database the hand-written selector takes about 6% less, and Dapper.AOT is level on SQL Server and 3% slower on SQLite); the opt-in async read costs more than the default; channels and Dataflow are slower than a plain loop for one row. [The full list is below.](#where-it-is-not-faster)
- **Reading rows synchronously is the default on purpose.** Over a real link it took the same time as `ReadAsync()`, and on `Map(Func)` it allocated 42% less memory.

<div class="perf-chart"><img src="../images/perf/perf-summary.svg" alt="Two panels of four bars each for 20 columns and 1,000 rows on a synthetic reader. Connection held open: hand-written selector 40.7 microseconds, Dapper.AOT 54.4, Map of T 76.1, Dapper Query of T 199.9. Memory allocated: hand-written selector, Dapper.AOT and Map of T 227.4 kilobytes, Dapper 548.1."></div>

How long the connection is held open (top) and how much memory is allocated (bottom) for the same job, four ways. Shorter and smaller are better. These are the best case, with no database: the [real databases](benchmarks/real-databases.md) page shows how much survives a real provider.

## What was asked, and the answer

A *terminal* is the call that ends a query and gives you the rows: `ToListAsync()`, `ToArrayAsync()`, `First()` and the like. Each row is one page.

| What we asked | The answer in a few words | Page |
|---|---|---|
| How does `Map<T>()` compare with Dapper and hand-written code? | 2.6 times less connection time and 2.4 times less memory than Dapper, with no database; a little ahead of Dapper on real databases; behind Dapper.AOT and a hand-written selector | [ODX against Dapper and hand-written code](benchmarks/library-comparison.md) |
| Which of 16 ways of mapping is best? | A hand-written selector holds the connection least; three selector recipes you write yourself come next; `Map<T>()` is the fastest that needs no code | [Every way of mapping, ranked](benchmarks/mapping-variants.md) |
| Does the lead survive a real database? | Partly: Dapper takes 8 to 11% longer on SQL Server and SQLite, with error margins that do not overlap in any run, and allocates 18 to 29% more | [ODX against Dapper](benchmarks/library-comparison.md#on-real-databases), and [On real databases](benchmarks/real-databases.md) |
| What does a wider result cost? | `Map<T>()` costs 3.69 ns per cell (one value in one row); Dapper 9.50 ns | [Cost grows with the number of columns](benchmarks/cost-per-column.md) |
| What does it cost to build a mapping? | 0.19 to 0.87 ms, once; never per call | [What building a map costs](benchmarks/map-cost.md) |
| Sync reads or async reads? | Sync is cheapest or tied in every case measured; async can wait on a busy thread pool | [Sync reads against async reads](benchmarks/sync-vs-async.md) |
| What happens on a slow link or with a 100 MB value? | The link sets the time; the sync read allocates less; one async path took 5.4 times as long | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| Are channels and Dataflow faster than a loop? | At 1,000 rows, yes: 0.32 and 0.72 of a plain loop's time; at one row, no: 1.6 and 2.6 times | [Channels and Dataflow against a plain loop](benchmarks/pipelines.md) |
| Which pool, and how are rows cleared? | Plain `ArrayPool` arrays; end to end, 0 to 2.5% faster for the collecting terminals and 2 to 23% for the buffer terminals than the build of 2026-11-20 (a different build on a different day); always clearing reference rows costs nothing you can measure: at 1, 100 and 10,000 rows the current build is as fast or faster than the 2026-11-20 build that cleared nothing | [Pooling and clearing](benchmarks/pooling-and-clearing.md) |

Also: [How we measure](benchmarks/how-we-measure.md) (the method, the settings, how fresh the data is), the [data files](performance-data.md), and the [interactive ranking](../benchmarks/ranking.html) of the 16 synthetic variants, a snapshot of 2026-11-23.

## Where it is not faster

- Dapper.AOT and a hand-written selector are faster than `Map<T>()` on the synthetic reader (54.4 and 40.7 against 76.1 µs of connection time). On SQL Server and SQLite, at 10,000 rows of 20 columns, the hand-written selector takes about 6% less than `Map<T>()` in all four runs (the error margins are apart in three), and Dapper.AOT is level on SQL Server and 3% slower on SQLite. [ODX against Dapper and hand-written code](benchmarks/library-comparison.md).
- A hand-written selector is faster than `Map<T>()` at every width, and `object[]` rows are faster at one column. [Cost per column](benchmarks/cost-per-column.md).
- `AsyncReads()` costs more than the default. With a 20 µs wait every 100 rows and a busy thread pool it took 8 times as long (2,227 against 280 µs; a synthetic model); with only a busy pool, no refill, it took 3.8% more than a fully sync read (99.3 against 95.6 µs). [Sync reads against async reads](benchmarks/sync-vs-async.md).
- `ToChannel` and the Dataflow methods are slower than reading rows in a plain loop for one row (1.6 and 2.6 times), and Dataflow is 1.10 times a plain loop at 100 rows; at 1,000 rows they are faster (0.32 and 0.72). They exist to feed pipelines: see [Channels and Dataflow](channels-and-dataflow.md).

## Limits of these numbers

- The synthetic reader has no network or server time, so do not read its ratios as end-to-end gains.
- Each case is one run. On the real providers BenchmarkDotNet reported error margins of 0.3 to 6.3% of the mean (the tables show each one), so gaps of a few percent are noise.
- Results vary by machine, runtime and data shape, and all of this was run on one PC. [Run the benchmarks on yours.](benchmarks/how-we-measure.md#reproduce)
