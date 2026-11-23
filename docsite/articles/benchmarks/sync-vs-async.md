# Sync reads against async reads

**The question:** an async terminal such as `ToListAsync()` reads rows with the synchronous `Read()` by default. `AsyncReads()` switches it to `ReadAsync()` for every row. What does each cost, and when does it matter?

The numbers on this page come from a model: a synthetic reader that pretends to need a refill every so often. They show the mechanism, not what your server will do. The same question on a real SQL Server link is on [Slow links and large values](slow-links-and-large-values.md).

In this page, "the default" is what an async terminal does: `ExecuteReaderAsync`, then `Read()` for every row. "Fully sync" is `ExecuteReader` then `Read()`.

## In short

- **The fully sync read is the cheapest, or level with the default, in all four conditions.** With no refill and no busy thread pool, the default (`ExecuteReaderAsync`, then `Read()`) costs **4.7% more** than the fully sync read (80.9 against 77.3 µs for 1,000 rows of 20 columns). `ReadAsync()` for every row costs **6.5% more** (82.3 µs), which is 1.8% more than the default. That extra time is the cost of the async calls.
- **A busy thread pool plus a wait for data is the hazard.** With a 20 µs wait every 100 rows (a buffer refill) and a busy pool, a read took **11.2 ms** with `AsyncReads()` and **0.28 ms** with the default, 40 times as long. Each refill makes `ReadAsync()` wait for a free thread while the reader stays open.
- **Not faster:** in no condition measured is the async read faster, and the benefit of releasing a thread while a row is on the wire was not measured here. The default is within 4.7% of the fully sync read in all four conditions (4.7%, 3.1% and 1.3% more, and 0.1% less in the last).
- Allocations are within 4 KB of each other in every case.

## The chart

<div class="perf-chart"><img src="../../images/perf/perf-sync-async.svg" alt="Four panels of three bars, each panel on its own scale, connection held open by Map of T at 20 columns and 1,000 rows. With neither a refill nor a busy pool: sync 77.3 microseconds, async execute 80.9, async read 82.3. With a busy pool: 94.4, 97.4, 102.4. With buffer refills: 257.9, 261.2, 314.3. With refills and a busy pool: 279.3 microseconds, 279.0 microseconds and 11.2 milliseconds."></div>

Each panel is one condition, with its own scale, so compare the bars inside a panel and not across panels. In each panel the three bars are:

- **Sync:** `ExecuteReader`, then `Read()` for every row.
- **Async execute:** `ExecuteReaderAsync`, then `Read()`. This is what the async terminals do by default.
- **Async read:** `ExecuteReaderAsync`, then `ReadAsync()` for every row. This is the opt-in, `AsyncReads()`.

[!include[](../../includes/perf/sync-async.md)]

The four conditions are: *neither* (the reader always has the next row ready), *busy thread pool* (8 work items kept queued on the pool), *buffer refills* (every 100 rows the reader waits 20 µs), and *refills and busy pool* (both).

## Why

A synchronous `Read()` blocks the calling thread until the row is there. A `ReadAsync()` that cannot complete at once returns an unfinished task, and the rest of the work runs on a thread-pool thread when the data arrives. If the pool is busy, that continuation waits in a queue, and the reader, and the connection, stay open the whole time.

So the opt-in trades a blocked thread for a possible wait on the pool. It costs a little more when nothing waits, and a great deal more when something waits and the pool has no thread to spare. The default does the opposite: it blocks a thread and never queues, and in these runs it was within 4.7% of the fully sync read in every condition. For the full argument, read [Row reads: sync by default, AsyncReads() to opt in](../async-reads.md).

## How this was measured

- **Question:** what does the read path cost, with and without a buffer refill and a busy thread pool?
- **Setup:** the synthetic reader, 20 typed columns, 1,000 rows per call, `Map<T>()`. A refill is a wait every 100 rows (20 µs, a busy wait) and, for the async read, an incomplete task completed from another thread. A busy pool keeps 8 work items queued on the thread pool. Four runs, one per condition.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. The time is the connection held open. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `ea5f096`, 2026-11-14; AMD Ryzen 9 7900, .NET 11.0.0, Windows 10.
- **Data:** [sync-async.csv](../../perf-data/sync-async.csv) ([source and conditions](../../perf-data/sync-async.source.txt)).
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks.V1_Sync" "*SyntheticBenchmarks.V1_Async" "*SyntheticBenchmarks.V1_AsyncRead"`, with `ODX_SYN_WIDTHS=20`, `ODX_SYN_ROWS=1000` and `ODX_SYN_BUFFER_ROWS`, `ODX_SYN_REFILL_US`, `ODX_SYN_PRESSURE` set per condition (the [generator README](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf) lists them).
