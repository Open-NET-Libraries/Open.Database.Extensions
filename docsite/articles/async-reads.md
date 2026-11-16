# Row reads: sync by default, AsyncReads() to opt in

Every command-level terminal and mapped view reads its rows with `Read()`, and that includes the async terminals. `ToListAsync`, `FirstOrDefaultAsync` and the rest still execute the command, and open the connection, asynchronously; only the row reads are synchronous. `AsyncReads()` opts one call in to reading the rows with `ReadAsync(token)`.

This page says what each choice does, what it costs, and why the default is the one it is. Every number has its run in [Sources](#sources), and every figure has a text description under it.

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

Three things were measured: CPU, memory, and wall time over a real link. Each is shown with its source in the sentence and in the figure.

> [!NOTE]
> Every number on this page is from one run. The CPU cells are four runs of different cases, each case run once. They show the size of the effect on that setup and are not a promise about yours. The runs used the opt-in as it was named in rc.5; the section on why it exists says how.

**CPU.** On a synthetic provider where every `ReadAsync` completes at once, with 1,000 buffered rows per `Map<T>()` call, no refill and no thread-pool pressure, the default took 28.92 us at width 5 and the opt-in 32.30 us (+11.7%); at width 20 the default took 80.92 us and the opt-in 82.34 us (+1.8%). Because every `ReadAsync` completes at once, that difference is the async state machine alone. Source: the SA run, synthetic provider, one run per case.

**Memory.** `Map(Func)` over SQL Server on a link with 5 ms of latency and 100 Mbit of bandwidth, 10,000 rows of width 20, allocated 9,713 KB per operation by default and 16,812 KB with the opt-in: 7,099 KB more, +73.1%. Put the other way, the default used about 42% less. `Map<T>()` was equal: 9,709 KB against 9,743 KB (+0.35%). Source: the M-ALLOC run 2, whole-process KB per operation, one run.

On `Map(Func)`, 788.7 of 10,001 `ReadAsync` calls were incomplete (about 7.9%); on `Map<T>()`, 4 of 10,001. The likely reason is an inference from the provider's documentation, not something these runs isolated: in `Default` mode `ReadAsync` reads the whole row before it returns. A second, differently built comparison points the same way: ODX `Map<T>().ToList()` allocated 9,706 KB, and raw SqlClient with `Default`, `ReadAsync` and synchronous getters allocated 16,636 KB (S2, one run). That pair is an ODX read against a raw read, not two ODX reads.

<div style="overflow-x: auto; margin: 1rem 0;"><img src="../images/asyncreads-cost.svg" alt="Four panels of paired bars. CPU, Map of T: 28.92 against 32.30 us at width 5 and 80.92 against 82.34 us at width 20. Memory over SQL Server: Map of Func 9,713 against 16,812 KB; Map of T 9,709 against 9,743 KB." style="display: block; width: 100%; min-width: 600px; height: auto;"></div>

Four panels, each a pair of bars that start at zero: the default on the left, the opt-in on the right. (a) CPU of `Map<T>()` on the synthetic provider at width 5: 28.92 against 32.30 us per 1,000 rows, +11.7%. (b) The same at width 20: 80.92 against 82.34 us, +1.8%; in (a) and (b) every `ReadAsync` completes at once, so the figure is the async state machine alone. (c) Memory of `Map(Func)` over SQL Server, 10,000 rows, width 20: 9,713 against 16,812 KB, +73.1%, at equal time (538.3 against 538.2 ms). (d) Memory of `Map<T>()` in the same run: 9,709 against 9,743 KB, +0.35%, equal. Panels (c) and (d) share one scale. One run for every number; the runs used the opt-in's rc.5 name.

**Wall time on a real link.** The 5 ms link is the bottleneck, and the choice of read does not change the time. In the raw-provider comparison run, one operation over 10,000 rows took 537.5 ms for ODX `Map<T>().ToList()` and 540.7 ms for raw SqlClient with `Default`, `ReadAsync` and synchronous getters (S2, 0.6% apart); on S3, the larger scenario in the same run, 6,759.7 ms against 6,757.4 ms. In the memory run, every cell took between 536.9 and 538.9 ms: 536.9 ms for the sync read and 538.2 ms for the opt-in on both `Map<T>()` and `Map(Func)`. Source: the P4-P6 run and the M-ALLOC run 2, SQL Server, one run each.

<div style="overflow-x: auto; margin: 1rem 0;"><img src="../images/asyncreads-link-time.svg" alt="Two panels of paired bars in milliseconds. S2: 537.5 against 540.7. S3, the larger scenario: 6,759.7 against 6,757.4." style="display: block; width: 100%; min-width: 600px; height: auto;"></div>

Two panels, each a pair of bars that start at zero, in milliseconds per operation over the 5 ms and 100 Mbit link, 10,000 rows. The left bar is the synchronous read, ODX `Map<T>().ToList()`; the right bar is raw SqlClient with `Default`, `ReadAsync` and synchronous getters, no ODX. S2: 537.5 against 540.7 ms. S3: 6,759.7 against 6,757.4 ms. The bars of each pair are the same height to within the line width, which is the point: over this link the link, not the read, sets the time. The right bar is not an ODX opt-in.

So on the measured runs the synchronous read costs less CPU, about 42% less memory on `Map(Func)`, and the same time over a real link. The maintainer's design reason is a separate matter. It is a reason, not a measurement:

> "we don't want to allow anyone in front of us while waiting"

## AsyncReads(): why it exists

The runs on this page were taken with the rc.5 name `EnableAsyncRead(true)`; it is the same read as `AsyncReads()`.

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

On `Map<T>()`, equal memory (+0.35%). CPU is +11.7% at width 5 and +1.8% at width 20, on a synthetic provider where every `ReadAsync` completes at once, so that figure is the async state machine alone.

### The busy-pool hazard

Measured, synthetic, `Map<T>()` shape: with a read wait every 100 rows (20 us) and 8 busy thread-pool items, 1,000 rows at width 5 took 10,873.63 us with the opt-in against 218.83 us with the default (49.7 times as long). The synchronous read, for reference, took 214.71 us. This is a synthetic provider, one run, one width; do not generalize past that setup.

The hazard points the opposite way from "use it on a starved pool". On a real link it is UNMEASURED.

### The large-value warning for Map(Func)

One value of 100 MB (one row, SQL Server, the same link) was read in 8.86 s by raw SqlClient with `Default` and a synchronous `Read`, and in 48.0 s by raw SqlClient with `Default` and `ReadAsync`: 5.4 times as long, with a peak working set of 369.5 MB against 254.1 MB. Raw SqlClient with `SequentialAccess` and `ReadAsync` took 8.86 s, and so did ODX `Map<T>()` with the opt-in (the opt-in's rc.5 name was used). Each is one run.

`Map(Func)` opens its reader with `Default`, so the 48.0 s cell is the one that applies to it. ODX `Map(Func)` with the opt-in on a 100 MB value was NOT measured; its cost here is inferred from the raw cell. `Map<T>()` opens its reader with `SequentialAccess`.

One mitigation was measured, once, on raw SqlClient: with both SqlClient switches off, `Switch.Microsoft.Data.SqlClient.UseCompatibilityAsyncBehaviour` and `Switch.Microsoft.Data.SqlClient.UseCompatibilityProcessSni` set to false, the same raw `Default` and `ReadAsync` read took 8.87 s, at a peak working set 129.6 MB above the sync read's (383.7 MB against 254.1 MB; about +130 MB). Per the SqlClient documentation, the switches are available from SqlClient 7.0 and later.

<div style="overflow-x: auto; margin: 1rem 0;"><img src="../images/asyncreads-large-value.svg" alt="Horizontal bars in seconds for one 100 MB value. Raw sync Read 8.86; raw Default with ReadAsync 48.0; raw SequentialAccess with ReadAsync 8.86; ODX Map of T with AsyncReads 8.86; raw Default with ReadAsync and both switches off 8.87; ODX Map of Func with AsyncReads not measured." style="display: block; width: 100%; min-width: 600px; height: auto;"></div>

Horizontal bars that start at zero, in seconds, for one row holding one 100 MB value over the 5 ms and 100 Mbit link, SQL Server, one run each. Raw SqlClient sync `Read` with `Default`: 8.86 s. Raw `Default` with `ReadAsync`: 48.0 s, 5.4 times the sync read. Raw `SequentialAccess` with `ReadAsync`: 8.86 s. ODX `Map<T>()` with the opt-in, which uses `SequentialAccess`: 8.86 s. Raw `Default` with `ReadAsync` and both SqlClient switches off: 8.87 s. ODX `Map(Func)` with the opt-in, which uses `Default`, was not measured; it is inferred from the raw 48.0 s bar and is not drawn.

The default, a synchronous `Read`, is the safe choice for large values.

### The side-by-side sample

The [side-by-side sample](#the-default-stated-plainly) at the top of this page is the same query read both ways. The other forms above are compiled in the samples project too, and each returns the same rows.

## Sources

Each row is one number or a group of numbers. Every run is one run; the SA run is four runs of different cases, each case once. The synthetic provider is in-process and has no network.

| Figure or claim | Value | Run (label, provider, link, scenario) | Runs |
|---|---|---|---|
| CPU, `Map<T>()`, width 5 | 28.92 us default, 32.30 us opt-in per 1,000 buffered rows (+11.7%) | SA run, synthetic provider, no refill, no pressure | one run per case |
| CPU, `Map<T>()`, width 20 | 80.92 us default, 82.34 us opt-in (+1.8%) | SA run, same | one run per case |
| Busy pool, `Map<T>()`, width 5, 1,000 rows | 218.83 us default, 10,873.63 us opt-in (49.7 times); sync 214.71 us. A read wait every 100 rows, 8 busy thread-pool items | SA run, synthetic provider | one run per case |
| Memory, `Map(Func)` | 9,713 KB default, 16,812 KB opt-in (+7,099 KB, +73.1%); under 8 busy pool items 9,768 and 16,875 KB (+72.8%). 788.7 of 10,001 `ReadAsync` calls incomplete | M-ALLOC run 2, SQL Server, 5 ms + 100 Mbit link, width 20 (S2), 10,000 rows, mean of 3 operations after 1 warm-up; whole-process KB | one run |
| Memory, `Map<T>()` | 9,709 KB default, 9,743 KB opt-in (+0.35%); under pressure 9,768 and 9,816 KB (+0.49%); sync anchor `Map<T>().ToList()` 9,708 KB. 4 of 10,001 `ReadAsync` calls incomplete | M-ALLOC run 2, same | one run |
| Time in the memory run | 536.9 ms sync; 538.3 and 538.2 ms for `Map<T>()` default and opt-in; 538.3 and 538.2 ms for `Map(Func)`; every cell 536.9 to 538.9 ms | M-ALLOC run 2, same | one run |
| Memory, ODX against raw | ODX `Map<T>().ToList()` 9,706 KB; raw SqlClient `Default` + `ReadAsync` + synchronous getters 16,636 KB. Not an ODX opt-in figure | P4-P6 run, SQL Server, same link, S2, 10,000 rows, mean of 3 operations after 1 warm-up | one run |
| Wall time, S2 | 537.5 ms ODX `Map<T>().ToList()`, 540.7 ms raw SqlClient `Default` + `ReadAsync` + synchronous getters (0.6% apart) | P4-P6 run, SQL Server, 5 ms + 100 Mbit link (round trip median 5.67 ms), S2, 10,000 rows | one run |
| Wall time, S3 (the larger scenario in the same run) | 6,759.7 ms and 6,757.4 ms (0.03% apart); thread-pool pressure on or off changed a cell by at most 3 ms | P4-P6 run, same | one run |
| Large value, 100 MB | raw sync `Read` 8,863 ms; raw `Default` + `ReadAsync` 48,015 ms (peak 369.5 MB against 254.1 MB); raw `SequentialAccess` + `ReadAsync` 8,861 ms; ODX `Map<T>()` + opt-in 8,862 ms; ODX `Map<T>().ToList()` 8,862 ms | S4 run, SQL Server, 5 ms + 100 Mbit link, `varbinary(max)`, 1 row, one measured operation after a warm-up | one run per cell |
| Large value, ODX `Map(Func)` + opt-in | NOT MEASURED; inferred from the raw 48,015 ms cell | S4 run | none |
| SqlClient switches off | raw `Default` + `ReadAsync` 8,873 ms, peak 383.7 MB (+129.6 MB against the sync read's 254.1 MB) | S4 run, switches-off run, SQL Server, same link | one run |
| Which reader behavior each map uses | `Map<T>()`: `SequentialAccess`; `Map(Func)`: `Default` (each adds `SingleResult` after `.SingleResult()`) | the library source | not a measurement |
| The pinning test | an async terminal calls `Read` and not `ReadAsync` by default, and the reverse after `AsyncReads()` | `AsyncReadsTests`, Core tests | a test, not a run |

The runs used the opt-in under its rc.5 name, as explained above. For the summary of what changed in rc.6, see [What's new in 11](whats-new-11.md).
