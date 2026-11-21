# Channels and Dataflow against a plain loop

**The question:** `ToChannel` and the Dataflow methods feed a pipeline from a query. Are they faster than reading the rows in a plain loop?

> [!NOTE]
> This page rests on an **older run**, recorded in [`RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md) on .NET 10, before the channel terminals moved out of the typed views of Core in 11.0.0-rc.8. It was not repeated on .NET 11. Read the direction of the numbers, not the exact figures.

## In short

- **No, they are not faster.** Draining a channel took 1.26 times as long as a plain loop over 1,000 rows (359 against 284 µs). Feeding a `BufferBlock` (a TPL Dataflow queue) took 2.1 times as long (325 against 156 µs).
- **For one row they cost most**: 2.4 times for a channel and 9.6 times for a `BufferBlock`, probably because handing a row to another thread dominates a tiny read.
- **Memory is close for larger results** (1.01 to 1.15 times the loop at 100 and 1,000 rows) and higher for one row (2.2 and 2.8 times).
- **They exist to feed pipelines**, not to be quick. Use them when something downstream needs a channel or a block. For a plain list, use a terminal such as `ToListAsync()`. See [Channels and Dataflow](../channels-and-dataflow.md).

## The chart

<div class="perf-chart"><img src="../../images/perf/perf-channels.svg" alt="Two panels of two bars each, each on its own scale, the time to drain 1,000 rows. Channel: plain loop 284 microseconds, ToChannel 359. BufferBlock: plain loop 156 microseconds, Dataflow 325."></div>

The time to move 1,000 rows into a channel (top panel) and into a `BufferBlock` (bottom panel), with ODX and with a hand-written loop that does the same job in one stage. Shorter is better, and each panel has its own scale.

[!include[](../../includes/perf/channels.md)]

## Why

ODX's mapped overloads read raw rows into a bounded buffer of about 1,024 rows and map them in a second stage. The hand-written loop reads and writes in one step. The second stage is a second hand-off between threads, and that costs time: little for a thousand rows, a lot for one.

A separate study in the same file compared three designs for the mapped overloads (one stage, two stages, and a spill design) with an unbounded target channel. With an unbounded target the second stage bought nothing and cost about three times as much on the producer side (for 10,000 rows: 5.09 ms against 1.61 ms for one stage, 3.2 times). The full study, and what it said about a bounded target, is in `RESULTS.md`. It predates later changes to the channel code, so we do not repeat its other findings here.

## How this was measured

- **Question:** how much do `ToChannel` and a `BufferBlock` cost against a hand-written loop over the same rows?
- **Setup:** an in-memory provider; 1, 100 and 1,000 rows. The loop is a hand-written single-stage loop. Both sides produce the same rows.
- **Settings:** BenchmarkDotNet 0.15.8, `--inprocess`, 5 warm-up and 12 measured iterations. The Dataflow rows were re-run as scoped jobs on 4 cores at below-normal priority under a watchdog. The Dataflow one-row time depends on thread scheduling and moves a lot between runs; read its 100- and 1,000-row ratios.
- **Build, date, machine:** build not recorded; 2026-11-07 for the Dataflow rows, earlier for the channel rows; AMD Ryzen 9 7900, .NET 10, Windows 10.
- **Data:** [channels.csv](../../perf-data/channels.csv) ([source](../../perf-data/channels.source.txt)), copied from the "Other features" table of `RESULTS.md`.
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*ChannelBenchmarks*"` and `"*DataflowBenchmarks*"`, and `-- --channel-study` for the design study.
