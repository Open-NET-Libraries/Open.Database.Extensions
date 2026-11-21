# Pooling and clearing

**The question:** to read rows quickly, ODX keeps them in arrays it rents from a pool and gives back when the call is done. Which kind of pool is fastest, what does it cost to clear the rows before an array goes back, and where should the clearing happen?

A *pool* is a shop for arrays: you rent one, use it, and return it, so the memory is reused and not allocated again. A *chunk* is one such array in a result that grows (16 rows, then 32, then 64 and so on).

## In short

- **Plain arrays from `ArrayPool<T>` are the fastest and the lightest.** To collect 10,000 `int` rows the array designs took 9.1 to 9.8 µs. `MemoryPool<T>` took 16.3 µs when it can use one span per chunk and 30.3 µs when it reads row by row. Reading a buffered result through the indexer took 19.7 µs with arrays and 48.4 µs with `Memory<T>` chunks.
- **The design that was built is 2.1 to 5.2% faster end to end** than the `Memory<T>` chunks build it replaced, with the same memory (10,000 rows: `ToListAsync` 5.2% less time, `ToAsyncBufferStream` 2.1% less).
- **Clearing is cheap where it happens inline.** Clearing the rows of a chunk of up to 256 rows adds 3 to 5 ns. For a chunk of 4,096 class rows it adds about 425 ns, on a chunk that takes 9.2 µs to write and release.
- **Not faster:** handing the clear to the background (the recycle bin) cost *more* time on the releasing thread than clearing inline in the spike, for rows that hold references, at every size measured (12.5 µs against 9.6 µs at 4,096 rows). What the bin buys, that the read and the consumer do not wait for the clear, was not measured in isolation. End to end, always clearing reference rows cost 4.3 µs (18%) at one row and nothing measurable at 100 rows.
- **`MemoryPool<T>` is only level with `ArrayPool<T>` when one span covers a whole chunk.** Written row by row through `Memory<T>.Span` it is 2.8 times slower.

## The rules

These are what ODX does with the arrays it rents. They are written in full on [Pooled memory](../async-buffers.md#pooled-memory).

1. **Rows that hold references are always cleared.** A class, a string, a struct that contains either: the rows that were written are cleared before the array goes back, so a pooled array never keeps a row alive. Nothing turns this off.
2. **Rows with no references are cleared when asked.** An `int` or a plain struct holds nothing to keep alive, so the default is not to clear them. Ask for it for one query with `.UsePool(pool, clearOnReturn: true)`, or for the whole application with the `Open.Database.Extensions.ClearPooledArrays` switch. An explicit `clearOnReturn: false` wins over the switch.
3. **Only the part that was used is cleared.** An array can be longer than the rows written into it. ODX clears the written rows and not the rest. When every slot was written, it asks the pool to clear the array.
4. **Where the clear happens depends on the size.** A chunk under 4,096 rows, or of rows with no references, is cleared by the code that releases it. A chunk of 4,096 rows or more that holds references is cleared by one background loop (the *recycle bin*), which also returns the array to the pool.

The same query with the shared pool, clearing on, and the rows read asynchronously:

[!code-csharp[](../../samples/AsyncBuffers.cs#buffers_clearing_shared)]

## Arrays or Memory

<div class="perf-chart"><img src="../../images/perf/perf-pool-store.svg" alt="Three panels of six bars, each panel on its own scale, the time to collect, buffer or stream 10,000 int rows with six designs of the chunk store. Collect: arrays in one store 9.1 microseconds, arrays with a lease per chunk 9.8, Memory chunks 11.7, arrays with an owner object 12.9, MemoryPool with a span per chunk 16.3, MemoryPool with a span per row 30.3. Buffer: 19.7, 22.3, 24.1, 48.4, 53.7 and 54.2. Stream: 19.5, 21.7, 23.5, 24.4, 43.1 and 115.9."></div>

The same job with six designs for where the rows live: add 10,000 `int` rows, then collect them into one array (top), read each one through an indexer (middle), or read them chunk by chunk (bottom). Shorter is better, and each panel has its own scale. The two blue bars are the array designs, and ODX uses the lease design. The grey bars are the alternatives: `Memory<T>` chunks, an owner object for every chunk, and `MemoryPool<T>`. Arrays are fastest or level in every panel.

The table has every cell of the spike: `int` rows and class rows (an `int` and a string), at 1, 100 and 10,000 rows.

[!include[](../../includes/perf/pool-store.md)]

Two pieces of the answer are in the pool itself. This is the time to rent an array, write every slot and return it:

[!include[](../../includes/perf/pool-rent.md)]

And this is what each way of holding a chunk allocates:

[!include[](../../includes/perf/pool-bytes.md)]

A struct lease costs nothing to hold. An owner object costs 32 bytes for every chunk, and the `IMemoryOwner<T>` that `MemoryPool<T>` hands out 24 bytes.

## End to end

<div class="perf-chart"><img src="../../images/perf/perf-pool-e2e.svg" alt="Eight bars, one per terminal and row count: how much less time the final arrays design takes than the Memory chunks build it replaced. At 10,000 rows ToListAsync 5.2 percent, ToArrayAsync 4.8, ToAsyncBuffer Items 2.9, ToAsyncBufferStream 2.1. At 100 rows 3.1, 3.4, 3.2 and 3.3 percent."></div>

The same benchmark, compiled against the build before the change and the build after it, on SQLite in memory with class rows. Each bar is how much less time the call took. Longer is better. The collectors (`ToListAsync`, `ToArrayAsync`) and the buffer terminals each came out 2 to 5% faster, and allocated the same.

[!include[](../../includes/perf/pool-e2e.md)]

## What clearing costs

<div class="perf-chart"><img src="../../images/perf/perf-pool-release.svg" alt="Four panels of three bars each, each panel on its own scale, the time on the releasing thread to release one chunk of int rows. 16 rows: no clear 11.5 nanoseconds, inline clear 14.1, post to the recycle bin 154. 256 rows: 110, 115 and 205. 4,096 rows: 1,691, 1,910 and 1,629. 8,192 rows: 3,377, 3,560 and 2,809."></div>

One chunk released three ways: with no clear, with a clear on the releasing thread (inline), and by posting it to the recycle bin. The time includes writing the rows. Shorter is better. For small chunks the inline clear is almost free, and the post costs roughly 120 to 200 ns for chunks up to 256 rows. For `int` rows the bin starts to win from 4,096 rows. For rows that hold references, the table shows it did not win in this test at any size:

[!include[](../../includes/perf/pool-release.md)]

The bin rows are the noisiest in that run: the 4,096-row class row has an error margin of ±14%. The test is also a tight loop: it releases a chunk and rents again at once, and an array posted to the bin is not back in the pool until the bin's loop reaches it, so the next rent misses the pool. A real read does not behave that way, which is why the number to trust for the bin is the end-to-end one below, and why it is not proof that the bin is the better choice.

**The pool's own clear.** `ArrayPool<T>.Return(array, clearArray: true)` clears the *whole* array, and its time grows with the array's length. Clearing only the rows that were written costs the same whatever the length:

[!include[](../../includes/perf/pool-return-clear.md)]

So ODX uses the pool's flag when every slot of the array was written (it was faster than clearing by hand when the whole array is used: 2,675 against 4,567 ns for 10,000 `int` rows), and clears the written part itself otherwise.

**End to end.** The recycle bin and the always-clear rule for reference rows together, against the code before the change, which cleared nothing. Class rows, SQLite in memory:

[!include[](../../includes/perf/pool-bin-cost.md)]

At one row the new rule costs 2.6 to 4.3 µs, which is 8 to 18%. At 100 rows it is a tie. At 10,000 rows the calls took 2.3 and 8.6% *less* time. We do not know why they were faster, and an earlier build of the change was used for this pair, so read it as "the clearing does not cost time you can see on a large result", not as a speed-up.

## Why

**Why arrays win.** Each row of a buffer is read through an indexer. With arrays the indexer is one array access. With `Memory<T>` every read goes through `Memory<T>.Span`, which is more work for every row; that is where the 2.5 times on the buffer read comes from. `MemoryPool<T>` adds a rent that allocates 24 bytes and takes about 6 ns more, and the same per-row cost. It only keeps up when the whole chunk is written through one span, and a per-row read cannot do that.

**Why only the used part.** The pool's clear flag works on the whole array, so it is the right tool only when the whole array was used. For a long array with few rows written, clearing those rows is cheaper.

**Why a recycle bin at all.** Clearing a big chunk takes time, and the code that releases it is often the consumer of a stream or the end of a call. The bin moves the clear and the return to the pool to one background loop. That is a design choice for large chunks of reference rows. The spike shows what it costs on the releasing thread; it does not show the benefit.

**Limits.** The spike ran on .NET 10 only; the end-to-end pairs ran on .NET 11. The spike's code is a prototype that is not in the repository; the end-to-end benchmarks (`PoolEndToEndBenchmarks`, `PoolBinCostBenchmarks`) are. Every case is one run. The stores were timed with clearing on.

## How this was measured

- **Question:** which pool type, which clearing rule and which release path are fastest, and what do they cost end to end?
- **Setup, the spike:** a self-contained prototype with six designs of the chunk store, each returning the same rows, checked before any number was taken. `int` rows and class rows (an `int` and a string), 1, 100 and 10,000 rows, the same chunk schedule (16 rows, doubling) in every design, the used part cleared and then returned. Bytes are counted exactly (`GC.GetAllocatedBytesForCurrentThread`).
- **Setup, end to end:** `PoolEndToEndBenchmarks` and `PoolBinCostBenchmarks`, the collecting and the buffer terminals on SQLite in memory with class rows, the same file compiled against two builds.
- **Settings:** BenchmarkDotNet 0.15.8, in process, 5 warm-up and 12 measured iterations, one launch, 4 cores at below-normal priority, one run at a time. See [How we measure](how-we-measure.md).
- **Build, date, machine:** spike: build `3c05c47` plus the prototype, .NET 10.0.15, 2026-11-20. End to end: before `3c05c47` and after `d4b47b8`; for the bin cost before `f6942a7` and after `b86608b`; .NET 11.0.0, 2026-11-20. AMD Ryzen 9 7900, Windows 10.
- **Data:** [pool-store.csv](../../perf-data/pool-store.csv), [pool-rent.csv](../../perf-data/pool-rent.csv), [pool-bytes.csv](../../perf-data/pool-bytes.csv), [pool-release.csv](../../perf-data/pool-release.csv), [pool-return-clear.csv](../../perf-data/pool-return-clear.csv), [pool-e2e.csv](../../perf-data/pool-e2e.csv), [pool-bin-cost.csv](../../perf-data/pool-bin-cost.csv), each with a `.source.txt` beside it. The spike numbers were copied from the pool spike report and spot-checked against the BenchmarkDotNet output; the end-to-end numbers come from the BenchmarkDotNet reports.
- **Reproduce:** the end-to-end pairs: `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*PoolEndToEndBenchmarks*"` (and `"*PoolBinCostBenchmarks*"`), once on each build to compare. The spike prototype is not published.
