# Slow links and large values

**The question:** the [sync against async](sync-vs-async.md) page uses a model. What happens on a real SQL Server over a slow link, and with one very large value?

All the numbers here are from a hand-run probe on SQL Server in a container, behind a simulated link (5 ms delay, 100 Mbit). Each case is one launch: one warm-up, then the mean of 3 measured operations (one for the large value).

## In short

- **The link sets the time, not the read.** Over the simulated link, 10,000 rows of 20 columns took 537.5 ms with ODX's sync read and 540.7 ms with a raw `ReadAsync()`, and 538.2 to 538.3 ms in the memory run with and without `AsyncReads()`. With an 8,000-byte column they took about 6.76 s either way. The read path did not change the wall time.
- **The sync read allocates less, sometimes much less.** With `Map(Func)` (`Map` with a selector function you write, the hand-written selector of the other pages), `AsyncReads()` allocated **16,812 KB** against **9,713 KB** (+73.1%) for the same rows in the same time. With `Map<T>()` the two were equal (9,743 against 9,709 KB, +0.35%).
- **One 100 MB value is the dangerous case.** Raw SqlClient with `Default` (the normal mode, which reads a whole row into memory first) and `ReadAsync` took **48.0 s**, against **8.86 s** with a sync `Read()` (5.4 times as long). `Map<T>()` with `AsyncReads()` took 8.86 s too. It opens its reader with `SequentialAccess` (which streams a value), as the raw 8.86 s cell does; we did not isolate that as the reason.
- **Not measured:** `Map(Func)` with `AsyncReads()` on a 100 MB value. It opens its reader with `Default`, so the 48.0 s cell is the one that would apply. That is an inference from the raw cell, not a measurement.
- **Not faster:** `AsyncReads()` took the same wall time as the sync read (538.2 against 538.3 ms), and gained nothing in the runs.

## Time over the link

<div class="perf-chart"><img src="../../images/perf/perf-link-time.svg" alt="Two panels of paired bars in milliseconds. S2, 10,000 rows of 20 columns: ODX with a sync read 537.5, raw SqlClient with ReadAsync 540.7. S3, 10,000 rows with an 8,000-byte column: 6,759.7 against 6,757.4."></div>

Two scenarios, each a pair of bars that start at zero, in milliseconds per operation: S2 reads 10,000 rows of 20 columns; S3 reads 10,000 rows that each have an 8,000-byte column. The first bar of each pair is ODX with the default sync read; the second is raw SqlClient with `Default`, `ReadAsync` and synchronous getters, with no ODX in it. The bars of a pair are the same height to within the line width. That is the point: over this link, the link sets the time. The raw bar is not an ODX opt-in.

[!include[](../../includes/perf/link-time.md)]

In the memory run, every cell took 538.2 to 538.3 ms.

## Memory

<div class="perf-chart"><img src="../../images/perf/perf-link-memory.svg" alt="Two panels on one scale, memory allocated per operation over the slow link. Map of Func: sync read 9,713 kilobytes, AsyncReads 16,812. Map of T: sync read 9,709, AsyncReads 9,743."></div>

Memory per operation for the same 10,000 rows of 20 columns, for the sync read and for `AsyncReads()`. Smaller is better. The two panels share one scale. The time is equal in all four cases.

[!include[](../../includes/perf/link-memory.md)]

On `Map(Func)` with `AsyncReads()`, about 7.9% of the `ReadAsync` calls (on average 789 of 10,001 per operation) did not complete at once; on `Map<T>()`, 4 of 10,001.

## One 100 MB value

<div class="perf-chart"><img src="../../images/perf/perf-large-value.svg" alt="Horizontal bars in seconds for one 100 MB value. Raw sync Read 8.86; raw Default with ReadAsync 48.0; raw SequentialAccess with ReadAsync 8.86; ODX Map of T with AsyncReads 8.86; raw Default with ReadAsync and both SqlClient switches off 8.87; ODX Map of Func with AsyncReads not measured."></div>

How long one row holding one 100 MB value takes. One bar is far longer than the rest: the raw `Default` read with `ReadAsync`. The others, including ODX `Map<T>()` with `AsyncReads()`, take about the same time as a sync read.

[!include[](../../includes/perf/large-value.md)]

**A mitigation, measured once, on raw SqlClient.** With both SqlClient switches off, `Switch.Microsoft.Data.SqlClient.UseCompatibilityAsyncBehaviour` and `Switch.Microsoft.Data.SqlClient.UseCompatibilityProcessSni` set to false, the same raw `Default` and `ReadAsync` read took 8.87 s, at a peak working set 129.6 MB above the sync read's (383.7 against 254.1 MB). The switches exist from SqlClient 7.0.

## Why, and what to take from it

The default, a synchronous `Read()`, allocates about 42% less on `Map(Func)` and takes the same time over a real link; in the synthetic runs it also took less time with nothing waiting (see [Sync reads against async reads](sync-vs-async.md)). For large values it is the safe choice. See [Row reads: sync by default, AsyncReads() to opt in](../async-reads.md).

## How this was measured

- **Question:** does the read path change time, memory or behavior on a real SQL Server over a slow link, and with a very large value?
- **Setup:** SQL Server 2022 in a Docker container (4 CPUs) on the same PC, reached over TCP, behind a simulated link with a 5 ms delay and a 100 Mbit rate (round trip median about 5.6 ms). The time and memory runs read 10,000 rows (S2: 20 typed columns; S3: an 8,000-byte `Body` column per row). The large-value run reads one `varbinary(max)` value of 100 MB. The probes also checked that every path reads the same values on the first 5 rows.
- **Settings:** not BenchmarkDotNet. These are the benchmark project's own probe programs: one warm-up operation, then 3 measured operations per cell (one for the large value), reported as the mean. The probe process ran at below-normal priority on 4 processors. The memory is the whole-process allocation per operation.
- **Build, date, machine:** build not recorded; 2026-11-14 (time) and 2026-11-15 (memory and large value); AMD Ryzen 9 7900, .NET 11.0.0, Windows 10.
- **Data:** [link-time.csv](../../perf-data/link-time.csv) ([source](../../perf-data/link-time.source.txt)), [link-memory.csv](../../perf-data/link-memory.csv) ([source](../../perf-data/link-memory.source.txt)), [large-value.csv](../../perf-data/large-value.csv) ([source](../../perf-data/large-value.source.txt)). The numbers were copied from the console output of the runs, which is not in the repository.
- **Reproduce:** the probes are the `--in6` (`In6Probe.cs`) and `--s4` (`S4Probe.cs`) modes of the benchmark project. They need a SQL Server and a way to add latency, so they are not a one-command run.
