# Row reads: sync by default, AsyncReads() to opt in

Every command-level terminal and mapped view reads its rows with `Read()`, and that includes the async terminals. `ToListAsync`, `FirstOrDefaultAsync` and the rest still execute the command, and open the connection, asynchronously; only the row reads are synchronous. `AsyncReads()` opts one call in to reading the rows with `ReadAsync(token)`.

This page says what each choice does, what it costs, and why the default is the one it is. Every number has a benchmark page, linked in [Sources](#sources), and the figure has a text description under it.

## The default, stated plainly

This covers every terminal on a command (`cmd.Map<T>().ToListAsync(token)`, `FirstOrDefaultAsync`, `ToArrayAsync` and the others), and every mapped view: `Map<T>()` and `Map(Func)`, on an expressive command and on a plain `IDbCommand`. The command still executes, and the connection still opens, asynchronously. Only the rows are read with `Read()`.

`AsyncReads()` opts one call in to `ReadAsync(token)`, with synchronous getters, and to `NextResultAsync(token)` for the later result sets.

The raw reader extensions stay raw. The async members you call on a reader you opened (`AsAsyncEnumerable`, `SelectAsync`, `ForEachAsync` and `IterateWhileAsync`) read with `ReadAsync` by default: there you opened the reader and asked to iterate it asynchronously.

The same query, both ways:

<div class="side-by-side">
<div>

**Default**

[!code-csharp[](../samples/AsyncReads.cs#asyncreads_default)]

</div>
<div>

**Opt in**

[!code-csharp[](../samples/AsyncReads.cs#asyncreads_optin)]

</div>
</div>

Both return the same rows. The compiled sample cannot show which read was used: SQLite's `ReadAsync` completes synchronously, and its provider cannot count calls, so the sample shows the calls and that the rows match, and it measures nothing. The test that pins the behavior is `AsyncReadsTests` in the Core tests: it counts the calls on a reader and asserts that an async terminal calls `Read` and never `ReadAsync` by default, and calls `ReadAsync` and never `Read` after `AsyncReads()`.

## What an async terminal does, and where a thread waits

An async terminal has three stages: it executes the command, it opens the connection, and it reads the rows. The first two are awaited in both lanes of the figure, so the thread is free while they run. The lanes differ at the third. While a row is still on the network, `Read()` holds the thread until the row arrives. `ReadAsync` returns an incomplete task, the thread goes back to the pool, and a continuation resumes when the row arrives.

<div style="overflow-x: auto; margin: 1rem 0;"><img src="../images/asyncreads-flow.svg" alt="Two lanes. Default: the thread reads with Read and is blocked while a row is on the wire. Opt in: ReadAsync releases the thread while the row is on the wire." style="display: block; width: 100%; min-width: 600px; height: auto;"></div>

The figure has no numbers. It shows two lanes that read left to right with the same stages in the same places. In both, the command executes and the connection opens asynchronously, so the thread is released while each awaits. In the default lane the thread then reads two rows with `Read()`, is blocked while the next row is on the wire, and reads the row after that. In the opt-in lane `ReadAsync` is called for two rows, returns an incomplete task for the row on the wire, which releases the thread, and the continuation resumes when the row arrives. The sync default holds the thread while a row is on the wire; the opt-in releases it, and pays for the state machine.

## Why the default is sync: the measured reasons

Three things were measured: CPU, memory, and wall time over a real link. Each has a benchmark page with the chart, the numbers and how they were made.

- **CPU.** The synchronous read costs less. With nothing waiting, `ReadAsync()` for every row cost 6.5% more than the fully synchronous read (82.3 against 77.3 µs for 1,000 rows of 20 columns). [Sync reads against async reads](benchmarks/sync-vs-async.md).
- **Memory.** On `Map(Func)` over SQL Server, the default allocated about 42% less (9,713 against 16,812 KB). `Map<T>()` was equal. [Slow links and large values](benchmarks/slow-links-and-large-values.md#memory).
- **Wall time on a real link.** The same: over a 5 ms, 100 Mbit link the link sets the time, and every read path took 537 to 541 ms for 10,000 rows. [Slow links and large values](benchmarks/slow-links-and-large-values.md#time-over-the-link).

So on the measured runs the synchronous read costs less CPU, about 42% less memory on `Map(Func)`, and the same time over a real link. The maintainer's design reason is a separate matter. It is a reason, not a measurement:

> "we don't want to allow anyone in front of us while waiting"

## AsyncReads(): why it exists

The runs on this page were taken under the name `EnableAsyncRead(true)`, the same read as `AsyncReads()`.

The default is the right read for most calls. A few callers cannot spend a thread on waiting for the network, and `AsyncReads()` is for them. It is per call: nothing else in the application changes.

### What it does

Rows are read with `ReadAsync(token)` and synchronous getters. Later result sets are read through with `NextResultAsync(token)`. The executing and the opening were already asynchronous. It changes nothing on a synchronous terminal.

It is available in these places:

- on the expressive command, before `Map`: `cmd.AsyncReads().Map<T>()`, as in the side-by-side above;
- on every mapped view, after `Map`, in any order with `Skip`, `Take` and `SingleResult`: `cmd.Map<T>().AsyncReads()`;
- on the mapped view of a plain `IDbCommand`: `dbCommand.Map<T>().AsyncReads()`.

[!code-csharp[](../samples/AsyncReads.cs#asyncreads_view)]

[!code-csharp[](../samples/AsyncReads.cs#asyncreads_dbcommand)]

[!code-csharp[](../samples/AsyncReads.cs#asyncreads_order)]

A plain `IDataReader` (one that is not a `DbDataReader`) that an async terminal reads checks the cancellation token on every row; `AsyncReadsTests` pins that.

### When to use it

Use it when the calling thread must not block on network waits for rows: for example, a slow link with large result sets, where blocked threads are the scarce resource.

That benefit, a thread released while rows are on the wire, is UNMEASURED. No run measured a gain from unblocking the thread, and the sync default was chosen with this benefit unmeasured. This page does not claim it as a number.

### What it costs

On `Map(Func)`, +73% memory at equal time: 9,713 KB against 16,812 KB, and 538.3 against 538.2 ms.

[!code-csharp[](../samples/AsyncReads.cs#asyncreads_func)]

On `Map<T>()`, equal memory (+0.35%). CPU, with nothing waiting, is +1.8% against the default at width 20 (82.3 against 80.9 µs per 1,000 rows), on a synthetic provider where every `ReadAsync` completes at once, so that figure is the async state machine alone. See [Slow links and large values](benchmarks/slow-links-and-large-values.md) and [Sync reads against async reads](benchmarks/sync-vs-async.md).

### The busy-pool hazard

Measured, synthetic, `Map<T>()` shape: with a read wait every 100 rows (20 µs) and 8 busy thread-pool items, 1,000 rows at width 20 took 11.2 ms with the opt-in against 0.28 ms with the default (40 times as long). This is a synthetic provider, one run, one width; do not generalize past that setup. See [Sync reads against async reads](benchmarks/sync-vs-async.md).

The hazard points the opposite way from "use it on a starved pool". On a real link it is UNMEASURED.

### The large-value warning for Map(Func)

One value of 100 MB (one row, SQL Server, over a simulated slow link) took 8.86 s with a raw sync `Read` and 48.0 s with raw `Default` and `ReadAsync`: 5.4 times as long. `Map<T>()` with the opt-in took 8.86 s, and it opens its reader with `SequentialAccess`, as the raw 8.86 s cell does (we did not isolate that as the reason). `Map(Func)` opens its reader with `Default`, so the 48.0 s cell is the one that applies to it; ODX `Map(Func)` with the opt-in on a 100 MB value was NOT measured, and its cost is inferred from the raw cell. Turning off both SqlClient compatibility switches brought the raw cell back to 8.87 s, at about 130 MB more peak memory. The chart and the table are on [Slow links and large values](benchmarks/slow-links-and-large-values.md#one-100-mb-value).

The default, a synchronous `Read`, is the safe choice for large values.

### The side-by-side sample

The [side-by-side sample](#the-default-stated-plainly) at the top of this page is the same query read both ways. The other forms above are compiled in the samples project too, and each returns the same rows.

## Sources

Every number on this page is on a benchmark page, with its chart, its table, and how it was made: [Sync reads against async reads](benchmarks/sync-vs-async.md) (the model) and [Slow links and large values](benchmarks/slow-links-and-large-values.md) (SQL Server over a simulated link).

Two claims here are not measurements. Which reader behavior each map uses (`Map<T>()`: `SequentialAccess`; `Map(Func)`: `Default`, and each adds `SingleResult` after `.SingleResult()`) comes from the library source. That an async terminal calls `Read` and not `ReadAsync` by default, and the reverse after `AsyncReads()`, is pinned by `AsyncReadsTests` in the Core tests.
