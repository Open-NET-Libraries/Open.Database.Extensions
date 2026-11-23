# Every way of mapping, ranked

**The question:** there are many ways to turn rows into objects, with ODX and with Dapper. Which ones hold the connection least, finish soonest and allocate least?

This is the long version of the [library comparison](library-comparison.md): all 16 ways, three measures, one synthetic reader.

These figures come from their own run (`synthetic-overhead.csv`); the library comparison uses a separate run of the same day, so the same call reads 76.1 µs there against 83.1 µs here.

## In short

- **Connection held open.** A hand-written selector is fastest (49.2 µs). `Map<T>()` is next among the simple ones (83.1 µs). Dapper's variants take 199.4 to 256.6 µs, 2.4 to 3.1 times `Map<T>()`.
- **Memory.** `Map<T>()`, a hand-written selector and a factory map straight to the domain object allocate 227.4 KB. A row class followed by `Select` after the reader closes allocates about twice that (446.3 KB), because every row exists twice. Dapper's variants allocate 548.1 to 837.4 KB.
- **Not the fastest:** four ODX variants beat `Map<T>()` on time by more than 10%: the hand-written selector (49.2 µs) and three selector variants (59.2, 62.2 and 74.0 µs). Each asks you to write the selector or the reuse yourself. `Map<T>()` is the one that needs none.
- **Two variants, a row class followed by `Select` after the reader closes and a row class with shaping after the close, hold the connection about as long as `Map<T>()` on the first chart (82.4 and 86.5 µs, against 83.1 µs) only because their remaining work happens after the reader closes.** The second chart shows it.

20 columns, 1,000 rows, synthetic reader. Every number is in the table at the end of the charts.

## Connection held open

<div class="perf-chart"><img src="../../images/perf/perf-synthetic-open.svg" alt="Ranked bars of connection-held-open time per call, shortest first, 16 ways of reading 20 columns and 1,000 rows. The hand-written selector is fastest at 49.2 microseconds, Map of T is 83.1, the factory map to the domain object is 101.7, object arrays are 177.7 and Dapper's variants are 199.4 to 256.6."></div>

The time from `ExecuteReader` until the reader closes. It is the number that matters most for a busy service, because the connection can serve nobody else until then. Shorter is better.

`Map<T>()` takes 83.1 µs: 2.4 times less time than Dapper's plain `Query<T>` (199.6 µs) and 2.1 times less than copying every row into `object[]` (177.7 µs). A factory map straight to the domain object builds one object per row while the reader is open. It takes 101.7 µs, 22% more than `Map<T>()`. Every Dapper variant takes between 199.4 and 256.6 µs, so even Dapper's best is 2.4 times `Map<T>()`.

## Total time per call

<div class="perf-chart"><img src="../../images/perf/perf-synthetic-total.svg" alt="Ranked bars of total time per call, shortest first, the same 16 variants. The hand-written selector is 49.4 microseconds, Map of T 83.2, the row class then Select after the close 97.1, object arrays 177.9 and Dapper 199.9 to 256.9."></div>

The whole call, including work done after the reader closes. Where a bar here is much longer than in the first chart, the work was moved out of the connection's lifetime but not out of the call: a row class followed by `Select` after the close takes 97.1 µs in total, 17% more than `Map<T>()`, and shaping after the close takes 137.2 µs.

The two charts answer different questions. Connection time is the cost to the database and the connection pool. Total time is the cost to the caller.

## Memory allocated

<div class="perf-chart"><img src="../../images/perf/perf-synthetic-memory.svg" alt="Ranked bars of memory allocated per call, smallest first, the same 16 variants. Map of T, the factory map to the domain object and the hand-written selector allocate 227.4 kilobytes; the row class then Select after the close 446.3; object arrays 516.6; Dapper 548.1 to 837.4."></div>

Memory allocated per call. `Map<T>()`, the hand-written selector and the factory map to the domain object allocate the same 227.4 KB: the list and one object per row. Nothing else is left to allocate. Allocation is deterministic, so unlike the timings it does not depend on how busy the machine was.

## All the numbers

Every variant, ranked by how long it holds the connection. Times are in microseconds; memory is in KB.

[!include[](../../includes/perf/variants.md)]

You can also explore the same data in the [interactive ranking](../../benchmarks/ranking.html) (a snapshot of the run of 2026-11-23).

## Why the variants differ

- **Hand-written selector, and the selector variants.** You write the function that reads each row, so there is nothing for the library to work out at run time. The two variants that fill one held row, or rent one and return it, avoid allocating a second object per row.
- **`Map<T>()`.** The library builds the same kind of function for you, once, from your type, and caches it. That is why its memory equals the hand-written selector's.
- **Factory map, domain object.** One object per row is built while the reader is open, so memory equals `Map<T>()`'s (227.4 KB), and with shaping inside the map it equals shaping inside the selector (297.7 and 297.8 KB). The cost is time: 101.7 µs against 83.1 µs for `Map<T>()`, and 133.1 µs with the shaping.
- **Row class, then `Select` after the close.** Every row exists twice: once as the row class, once as the shaped object. That is the doubled memory (446.3 KB; with shaping 516.7 KB, against 297.7 KB for shaping in the factory map). Doing the `Select` after the reader closes moves the second step out of the connection's lifetime and back into the caller's.
- **`object[]` rows.** Each row is copied into an array of boxed values, and objects are built from it afterwards.
- **Dapper.** The numbers say how much each variant takes; they do not say why. That is not something these runs isolated.

## How this was measured

- **Question:** for one job, how do 16 ways of mapping compare on connection time, total time and memory?
- **Setup:** the synthetic reader with 20 typed columns and 1,000 rows. Synchronous terminals. The 16 variants are 11 ODX ones (a selector, `Map<T>()`, a factory map, a row class followed by `Select` after the close, `object[]` rows and their shaping forms) and 5 Dapper ones. All return the same final objects (`--verify` checks it).
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. Connection time comes from a timer around the life of the reader; total time and memory are BenchmarkDotNet's `Mean` and `Allocated`. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `6a770e8`, 2026-11-23; AMD Ryzen 9 7900, .NET 11.0.0, Windows 10.
- **Data:** [synthetic-overhead.csv](../../perf-data/synthetic-overhead.csv) ([source and conditions](../../perf-data/synthetic-overhead.source.txt)).
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks.V?_Sync"` (and `V??_Sync`, `V7_*_Sync`; `?` stands for one character) with `ODX_SYN_WIDTHS=20` and `ODX_SYN_ROWS=1000`; the [generator README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf) has the exact filters.
