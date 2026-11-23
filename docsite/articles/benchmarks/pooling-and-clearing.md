# Pooling and clearing

**The question:** ODX keeps rows in arrays it rents from a pool and gives back when the call is done. Which kind of pool is fastest, what does it cost to clear the rows before an array goes back, and where should the clearing happen?

A *pool* is a shop for arrays: you rent one, use it, and return it, so the memory is reused and not allocated again. A *chunk* is one such array in a result that grows (16 rows, then 32, then 64 and so on).

## In short

- **Arrays from `ArrayPool<T>` beat `MemoryPool<T>` in every store test, and allocate less.** To collect 10,000 `int` rows the two array designs took 9.1 and 9.8 µs (12.9 µs with an owner object per chunk). `MemoryPool<T>` took 16.3 µs with one span per chunk and 30.3 µs row by row. Reading a buffered result through the indexer took 22.3 µs with the design ODX uses (19.7 µs with the fastest array design) and 48.4 µs with `Memory<T>` chunks.
- **The chunk store ODX uses is 2.1 to 5.2% faster end to end** than a `Memory<T>` chunk store, with memory within 1 KB (10,000 rows: `ToListAsync` 5.2% less time, `ToAsyncBufferStream` 2.1% less).
- **Handing a chunk to the recycle bin costs the releasing thread less than clearing it inline.** For class rows it costs 95 ns at 4,096 rows, 75 ns at 16,384 and 144 ns at 65,536. Clearing inline costs 550, 1,786 and 6,738 ns: 6 to 47 times as much.
- **Not faster:** the bin does not save time or CPU. End to end, time with the bin was 0.95 to 1.02 times the time with inline clearing, and CPU 0.89 to 1.06 times, over three runs. It moves the clearing to another thread; it does not remove it. Always clearing reference rows also costs 2.6 to 4.3 µs (8 to 18%) on a one-row call, and nothing measurable from 100 rows.
- **`MemoryPool<T>` was slower even in its best case.** With one span per chunk it took 1.2 to 2.8 times as long as arrays (collect 16.3 against 9.1 µs, buffer 54.2 against 19.7 µs, stream 23.5 against 19.5 µs); row by row, 2.7 to 5.9 times. Only on the pool alone, renting one array and filling it, did it tie or win: 18 to 21% less time than `ArrayPool<T>` at 1,024 and 8,192 `int` slots when one span covers the array, and slower at 16 slots.

## The rules

These are what ODX does with the arrays it rents. They are written in full on [Pooled memory](../async-buffers.md#pooled-memory).

1. **Rows that hold references are always cleared.** A class, a string, a struct that contains either: the rows that were written are cleared before the array goes back, so a pooled array never keeps a row alive. Nothing turns this off.
2. **Rows with no references are cleared when asked.** An `int` or a plain struct holds nothing to keep alive, so the default is not to clear them. Ask for it for one query with `.UsePool(pool, clearOnReturn: true)`, or for the whole application with the `Open.Database.Extensions.ClearPooledArrays` switch. An explicit `clearOnReturn: false` wins over the switch.
3. **Only the part that was used is cleared.** An array can be longer than the rows written into it. ODX clears the written rows and not the rest. When every slot was written, it asks the pool to clear the array.
4. **Where the clear happens depends on the size.** A chunk that is cleared at all is cleared by the code that releases it, unless its rows add up to 128 KB or more, of any kind of row. Then it is handed to one background loop (the *recycle bin*), which clears it and returns the array to the pool, so neither the read nor the consumer waits for it. If the bin is full, the clear runs where the chunk is released.

The same query with the shared pool and clearing on:

[!code-csharp[](../../samples/AsyncBuffers.cs#buffers_clearing_shared)]

## Arrays or Memory

<div class="perf-chart"><img src="../../images/perf/perf-pool-store.svg" alt="Three panels of six bars, each panel on its own scale, the time to collect, buffer or stream 10,000 int rows with six designs of the chunk store. Collect: arrays in one store 9.1 microseconds, arrays with a lease per chunk 9.8, Memory chunks 11.7, arrays with an owner object 12.9, MemoryPool with a span per chunk 16.3, MemoryPool with a span per row 30.3. Buffer: one store 19.7, lease per chunk 22.3, owner object 24.1, Memory chunks 48.4, MemoryPool per row 53.7, MemoryPool per chunk 54.2. Stream: one store 19.5, lease per chunk 21.7, MemoryPool per chunk 23.5, owner object 24.4, Memory chunks 43.1, MemoryPool per row 115.9."></div>

The same job with six designs for where the rows live. *One store*: one object owns all the arrays of a call. *Lease per chunk*: each array travels with a small struct that knows how to give it back (this is what ODX uses: a buffer's chunks are released one at a time by their consumer, so one store cannot own them all). *Owner object*: the same, with a class per chunk. *`Memory<T>` chunks*: a store of `Memory<T>` chunks, for comparison. *`MemoryPool<T>`*: rents `IMemoryOwner<T>` objects; *span per row* writes each row through `Memory<T>.Span`, *span per chunk* writes a whole chunk through one span. The job: add 10,000 `int` rows, then collect them into one array (top), read each one through an indexer (middle), or read them chunk by chunk (bottom). Shorter is better, and each panel has its own scale; the "indexer" is the call that reads row number i of a buffer. The blue bars are the array designs without an owner object; the grey bars are the others. The two fastest bars in every panel are the array designs.

The table has every cell of the store tests: `int` rows and class rows (an `int` and a string), at 1, 100 and 10,000 rows.

[!include[](../../includes/perf/pool-store.md)]

Two pieces of the answer are in the pool itself. This is the time to rent an array, write every slot and return it:

[!include[](../../includes/perf/pool-rent.md)]

And this is what each way of holding a chunk allocates:

[!include[](../../includes/perf/pool-bytes.md)]

A struct lease costs nothing to hold. An owner object costs 32 bytes for every chunk, and the `IMemoryOwner<T>` that `MemoryPool<T>` hands out 24 bytes.

## End to end

<div class="perf-chart"><img src="../../images/perf/perf-pool-e2e.svg" alt="Eight bars, one per terminal and row count: how much less time the final arrays design takes than the Memory chunks build it replaced. At 10,000 rows ToListAsync 5.2 percent, ToArrayAsync 4.8, ToAsyncBuffer Items 2.9, ToAsyncBufferStream 2.1. At 100 rows 3.1, 3.4, 3.2 and 3.3 percent."></div>

The same benchmark, compiled against the build before the change and the build after it, on SQLite in memory with class rows. Each bar is how much less time the call took. Longer is better. The collectors (`ToListAsync`, `ToArrayAsync`) and the buffer terminals each came out 2 to 5% faster, and allocated within 1 KB of the same.

[!include[](../../includes/perf/pool-e2e.md)]

## Where the clear runs: inline, or on the bin

The rule the bin has to meet is simple: **handing a used array to the bin must cost the releasing thread less than clearing it inline.** If it does not, the bin has no reason to exist. Here is the cost of the hand-off against the cost of the clear.

<div class="perf-chart"><img src="../../images/perf/perf-pool-bin.svg" alt="Three panels of three bars each, each panel on its own scale, the time on the releasing thread to release one chunk of class rows. 4,096 rows: no clear 36 nanoseconds, inline clear 550, hand to the bin 95. 16,384 rows: 27, 1,786 and 75. 65,536 rows: 44, 6,738 and 144."></div>

One chunk of class rows (an `int` and a string) released three ways: with no clear, with a clear on the releasing thread (inline), and by handing it to the recycle bin. The numbers are the time on the releasing thread, the median of three runs, with a pace that keeps the bin from filling. Shorter is better, and each panel has its own scale. Handing a chunk to the bin costs 95 ns at 4,096 rows, 75 ns at 16,384 and 144 ns at 65,536. Clearing it inline costs 550, 1,786 and 6,738 ns. The hand-off hardly grows with the chunk; the inline clear grows with it, at about 0.1 ns for every row.

[!include[](../../includes/perf/pool-release-path.md)]

The figures in brackets are the lowest and the highest of the three runs. With a clear asked, an `int` chunk costs about a third of what a class chunk costs to clear inline (181 ns at 4,096 rows, 2,075 ns at 65,536). The probe's bin arm for `int` rows ran the same inline path (so the table leaves it out): the bin's cost for `int` rows is not measured here.

## Total time and CPU do not change

The bin moves the clearing off the thread that releases the chunk. It does not remove it: the same rows are cleared, on another thread. So time and CPU, taken over a whole call, should not change, and they did not.

End to end, on SQLite in memory with class rows, the shipped bin against clearing inline always (a test-only setting). Three runs, the two arms interleaved:

[!include[](../../includes/perf/pool-bin-proof-e2e.md)]

Time with the bin was between 0.95 and 1.02 times the time with inline clearing in every cell of every run, and CPU between 0.89 and 1.06 times, which is the noise of one run. A second measurement counted the CPU of all threads for one chunk, in two runs. Renting and filling the chunk are in every number and are most of it, so read it only as "the bin adds no CPU you can see":

[!include[](../../includes/perf/pool-bin-cpu.md)]

## How fast is the clear itself

The clear that ODX runs on the used part of an array is `AsSpan(0, used).Clear()`. The pool has its own: `Return(array, clearArray: true)`. This is the time to clear and return one array each way, with a warm pool, in nanoseconds, the median of three runs:

[!include[](../../includes/perf/pool-clear-speed.md)]

When the whole array was used, the span clear and the pool's own clear cost the same, for `int` rows and for class rows (558 against 566 ns for 16,384 `int` slots; 6,725 against 6,788 ns for 65,536 class rows). When only half was used, the span clear costs half as much, because the pool's flag clears the whole array whatever was written (323 against 555 ns). So the clear is as fast per byte whichever way it is called. ODX asks the pool to clear when every slot was written, and clears the used part itself otherwise.

## What always clearing reference rows costs end to end

The recycle bin and the always-clear rule for reference rows together, against a build that cleared nothing. Class rows, SQLite in memory:

[!include[](../../includes/perf/pool-bin-cost.md)]

At one row the rule costs 2.6 to 4.3 µs, which is 8 to 18%. At 100 rows it is a tie. At 10,000 rows the calls took 2.3 and 8.6% *less* time. We do not know why they were faster, and an earlier build of the change was used for this pair, so read it as "the clearing does not cost time you can see on a large result", not as a speed-up.

## Limits

The store tests ran on .NET 10 only; the end-to-end pairs ran on .NET 11; the bin tests ran on .NET 10, on build 11.0.0-rc.11, which handed only chunks of reference rows of 4,096 rows or more to the bin (the threshold is 128 KB of rows of any kind). Every case is one run, except the bin tests, which were run three times (the CPU count twice). The stores were timed with clearing on. The bin tests keep the bin from filling; a bin that is full clears where the chunk is released, and that case was not measured here.

## How this was measured

- **Question:** which pool type, which clearing rule and which release path are fastest, and what do they cost end to end?
- **Setup, the store tests:** a self-contained prototype with six designs of the chunk store, each returning the same rows, checked before any number was taken. `int` rows and class rows (an `int` and a string), 1, 100 and 10,000 rows, the same chunk schedule (16 rows, doubling) in every design, the used part cleared and then returned. Bytes are counted exactly (`GC.GetAllocatedBytesForCurrentThread`).
- **Setup, the bin tests:** a probe program run three times against the library after the change: the release path of one chunk with no clear, an inline clear and a hand-off to the bin, paced so the bin never fills; the speed of four ways to clear and return an array; and the shipped bin against clearing inline always, end to end.
- **Setup, end to end:** `PoolEndToEndBenchmarks` and `PoolBinCostBenchmarks`, the collecting and the buffer terminals on SQLite in memory with class rows, the same file compiled against two builds.
- **Settings:** BenchmarkDotNet 0.15.8, in process, 5 warm-up and 12 measured iterations, one launch, 4 cores at below-normal priority, one run at a time, for the store tests and the end-to-end pairs; the bin tests use their own probe program (median of the arrays of a run, 8 logical cores). See [How we measure](how-we-measure.md).
- **Build, date, machine:** store tests: build `3c05c47` plus the prototype, .NET 10.0.15, 2026-11-20. End to end: before `3c05c47` and after `d4b47b8`; for the always-clear cost before `f6942a7` and after `b86608b`; .NET 11.0.0, 2026-11-20. Bin tests: master `6a88b60` (11.0.0-rc.11), .NET 10.0.15, 2026-11-21. AMD Ryzen 9 7900, Windows 10.
- **Data:** [pool-store.csv](../../perf-data/pool-store.csv), [pool-rent.csv](../../perf-data/pool-rent.csv), [pool-bytes.csv](../../perf-data/pool-bytes.csv), [pool-e2e.csv](../../perf-data/pool-e2e.csv), [pool-bin-cost.csv](../../perf-data/pool-bin-cost.csv), [pool-release-path.csv](../../perf-data/pool-release-path.csv), [pool-clear-speed.csv](../../perf-data/pool-clear-speed.csv), [pool-bin-proof-e2e.csv](../../perf-data/pool-bin-proof-e2e.csv), [pool-bin-cpu.csv](../../perf-data/pool-bin-cpu.csv), each with a `.source.txt` beside it. The store numbers were copied from the pool spike report and spot-checked against the BenchmarkDotNet output; the bin numbers were copied from the console output of the probe; the end-to-end numbers come from the BenchmarkDotNet reports.
- **Reproduce:** the end-to-end pairs: `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*PoolEndToEndBenchmarks*"` (and `"*PoolBinCostBenchmarks*"`), once on each build to compare. The store prototype and the bin probe are not published.
