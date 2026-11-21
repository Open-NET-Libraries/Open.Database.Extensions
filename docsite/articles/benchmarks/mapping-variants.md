# Every way of mapping, ranked

**The question:** there are many ways to turn rows into objects, with ODX and with Dapper. Which ones hold the connection least, finish soonest and allocate least?

This is the long version of the [library comparison](library-comparison.md): all 16 ways, three measures, one synthetic reader.

## In short

- **Connection held open.** A hand-written selector is fastest (46.8 µs). `Map<T>()` is next among the simple ones (81.6 µs). Dapper's variants take 197.5 to 257.6 µs, 2.4 to 3.2 times `Map<T>()`.
- **Memory.** `Map<T>()` and a hand-written selector allocate 227.4 KB. A row class followed by `Select` allocates about twice that, because every row exists twice. Dapper's variants allocate 548.1 to 837.4 KB.
- **Not the fastest:** four ODX variants beat `Map<T>()` on time: the hand-written selector (46.8 µs), and three selector variants (58.8, 62.8 and 67.6 µs). Each asks you to write the selector or the reuse yourself. `Map<T>()` is the one that needs none.
- **Two variants look as fast as `Map<T>()` on the first chart only because their remaining work happens after the reader closes.** The second chart shows it.

20 columns, 1,000 rows, synthetic reader. Every number is in the table at the end of the charts.

## Connection held open

<div class="perf-chart"><img src="../../images/perf/perf-synthetic-open.svg" alt="Ranked bars of connection-held-open time per call, shortest first, 16 ways of reading 20 columns and 1,000 rows. The hand-written selector is fastest at 46.8 microseconds, Map of T is 81.6, the row class with an inline Select is 106.4, object arrays are 178.4 and Dapper's variants are 197.5 to 257.6."></div>

The time from `ExecuteReader` until the reader closes. It is the number that matters most for a busy service, because the connection can serve nobody else until then. Shorter is better.

`Map<T>()` takes 81.6 µs: 2.5 times less time than Dapper's plain `Query<T>` (202.4 µs) and 2.2 times less than copying every row into `object[]` (178.4 µs). The usual way to get a second object per row is a row class followed by `Select`. With the reader open that costs 106.4 µs, 30% more than `Map<T>()`. Every Dapper variant takes between 197.5 and 257.6 µs, so even Dapper's best is 2.4 times `Map<T>()`.

## Total time per call

<div class="perf-chart"><img src="../../images/perf/perf-synthetic-total.svg" alt="Ranked bars of total time per call, shortest first, the same 16 variants. The hand-written selector is 47.0 microseconds, Map of T 81.7, the row class then Select after the close 97.8, object arrays 178.5 and Dapper 202.7 to 257.9."></div>

The whole call, including work done after the reader closes. Where a bar here is much longer than in the first chart, the work was moved out of the connection's lifetime but not out of the call: a row class followed by `Select` after the close takes 97.8 µs in total, 20% more than `Map<T>()`, and shaping after the close takes 134.5 µs.

The two charts answer different questions. Connection time is the cost to the database and the connection pool. Total time is the cost to the caller.

## Memory allocated

<div class="perf-chart"><img src="../../images/perf/perf-synthetic-memory.svg" alt="Ranked bars of memory allocated per call, smallest first, the same 16 variants. Map of T and the hand-written selector allocate 227.4 kilobytes; a row class with Select 438.7; object arrays 516.6; Dapper 548.1 to 837.4."></div>

Memory allocated per call. `Map<T>()` and the hand-written selector allocate the same 227.4 KB: the list and one object per row. Nothing else is left to allocate. Allocation is deterministic, so unlike the timings it does not depend on how busy the machine was.

## All the numbers

Every variant, ranked by how long it holds the connection. Times are in microseconds; memory is in KB.

[!include[](../../includes/perf/variants.md)]

You can also explore the same data in the [interactive ranking](../../benchmarks/ranking.html) (a snapshot of the run of 2026-11-14).

## Why the variants differ

- **Hand-written selector, and the selector variants.** You write the function that reads each row, so there is nothing for the library to work out at run time. The two variants that fill one held row, or rent one and return it, avoid allocating a second object per row.
- **`Map<T>()`.** The library builds the same kind of function for you, once, from your type, and caches it. That is why its memory equals the hand-written selector's.
- **Row class, then `Select`.** Every row exists twice: once as the row class, once as the shaped object. That is the doubled memory. Doing the `Select` after the reader closes moves the second step out of the connection's lifetime and back into the caller's.
- **`object[]` rows.** Each row is copied into an array of boxed values, and objects are built from it afterwards.
- **Dapper.** The numbers say how much each variant takes; they do not say why. That is not something these runs isolated.

## How this was measured

- **Question:** for one job, how do 16 ways of mapping compare on connection time, total time and memory?
- **Setup:** the synthetic reader with 20 typed columns and 1,000 rows. Synchronous terminals. The 16 variants are 11 ODX ones (a selector, `Map<T>()`, a row class with `Select`, `object[]` rows and their shaping forms) and 5 Dapper ones. All return the same final objects (`--verify` checks it).
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. Connection time comes from a timer around the life of the reader; total time and memory are BenchmarkDotNet's `Mean` and `Allocated`. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `b27a02f`, 2026-11-14; AMD Ryzen 9 7900, .NET 11.0.0, Windows 10.
- **Data:** [synthetic-overhead.csv](../../perf-data/synthetic-overhead.csv) ([source and conditions](../../perf-data/synthetic-overhead.source.txt)).
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks.V?_Sync"` (and `V??_Sync`, `V7_*_Sync`; `?` stands for one character) with `ODX_SYN_WIDTHS=20` and `ODX_SYN_ROWS=1000`; the [generator README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf) has the exact filters.
