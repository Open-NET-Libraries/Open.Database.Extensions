# Cost grows with the number of columns

**The question:** if a query returns more columns, how much longer does each approach take?

> **Older build.** These numbers are from build `b27a02f` (2026-11-14), the only run that measured widths other than 20. The newer run on the [library comparison](library-comparison.md) (build `b5b0014`, 2026-11-21) measured 20 columns only: `Map<T>()` 85.1 µs and Dapper 246.4 µs for 1,000 rows (81.7 and 202.7 here).

## In short

- Every approach grows in a nearly straight line, so each has a **cost per cell**. `Map<T>()` costs **3.65 ns** per cell. Dapper costs 9.70 ns, and `object[]` rows 8.90 ns.
- That makes `Map<T>()`'s lead **grow with the width of the result**: at 20 columns it is 2.5 times faster than Dapper (81.7 against 202.7 µs for 1,000 rows); at 1 column it is 1.5 times faster (12.4 against 18.5 µs).
- **Not faster:** at one column the plain `object[]` rows are the fastest of the five (9.5 µs, against 12.4 µs for `Map<T>()`). A hand-written selector is faster than `Map<T>()` at every width (47.0 against 81.7 µs at 20 columns).

## The chart

<div class="perf-chart"><img src="../../images/perf/perf-width.svg" alt="A line chart of total time per call against 1 to 20 columns at 1,000 rows, five approaches. Every line is close to straight. At 20 columns the order from fastest is the hand-written selector at 47.0 microseconds, Map of T 81.7, a row class with Select 106.7, object arrays 178.5 and Dapper 202.7."></div>

Time for one call that reads 1,000 rows, as the result gets wider. Each line is one approach, and lower is better. The steeper a line is, the more each extra column costs. The lines for Dapper and `object[]` rows climb the fastest; the line for `Map<T>()` is the second flattest, rising at about twice the selector's cost per cell (3.65 against 1.91 ns).

[!include[](../../includes/perf/width.md)]

The last column is the cost of one more cell: the time at 20 columns minus the time at 1 column, divided by the cells added (19 columns of 1,000 rows each). Times in the other columns are microseconds for the whole call.

The practical reading: the more columns a query returns, the more it matters how the cells are read. For a narrow result the choice hardly shows; for a wide one it is the main cost.

## How this was measured

- **Question:** how does the time of one call change with the number of columns?
- **Setup:** the synthetic reader, 1,000 rows, 1, 3, 5, 10 and 20 typed columns (the first columns of a fixed 20-column catalog). Five approaches: a hand-written selector, `Map<T>()`, a row class followed by `Select`, `object[]` rows, and Dapper's `Query<T>`. Synchronous terminals. Every approach returns the same objects.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. The time is the whole call. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `b27a02f`, 2026-11-14; AMD Ryzen 9 7900, .NET 11.0.0, Windows 10.
- **Data:** [width-sweep.csv](../../perf-data/width-sweep.csv) ([source and conditions](../../perf-data/width-sweep.source.txt)). Each point comes from exactly one run.
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks.V?_Sync"` with `ODX_SYN_WIDTHS=1,5,20` and `ODX_SYN_ROWS=1000`, then a second run with `ODX_SYN_WIDTHS=3,10`; the [generator README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf) has the full table of filters and settings.
