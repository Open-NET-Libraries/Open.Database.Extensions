# Channels and Dataflow against a plain loop

**The question:** `ToChannelReader()` and `ToTargetBlock()` feed a pipeline from a query. Are they faster than a plain loop that reads the rows and feeds the same pipeline?

## In short

- **At 1,000 rows they are faster.** Draining a channel took 0.32 times as long as a plain loop (88 against 276 µs). Feeding a `BufferBlock` (a TPL Dataflow queue) took 0.72 times as long (95 against 132 µs).
- **At 100 rows the channel is faster and the `BufferBlock` is not**: 0.45 times for the channel (18.7 against 41.2 µs), 1.10 times for the `BufferBlock` (22.8 against 20.7 µs).
- **For one row they cost more than the loop**: 1.61 times for a channel (2.4 against 1.5 µs) and 2.56 times for a `BufferBlock` (4.8 against 1.9 µs).
- **Memory follows**: 0.35 to 0.47 times the loop at 100 and 1,000 rows, and 1.82 and 1.84 times for one row.
- **Use them when something downstream needs a channel or a block.** For a plain list, use a terminal such as `ToListAsync()`. See [Channels and Dataflow](../channels-and-dataflow.md).

## The chart

<div class="perf-chart"><img src="../../images/perf/perf-channels.svg" alt="Two panels of two bars each, each on its own scale, the time to drain 1,000 rows. Channel: plain loop 275.6 microseconds, ToChannel 87.8. BufferBlock: plain loop 132.0 microseconds, Dataflow 95.4."></div>

The time to move 1,000 rows into a channel (top panel) and into a `BufferBlock` (bottom panel), with ODX (`ToChannelReader(true)` and `ToTargetBlock`, the bars labelled ToChannel and Dataflow) and with a hand-written loop that does the same job. Shorter is better, and each panel has its own scale.

[!include[](../../includes/perf/channels.md)]

## How this was measured

- **Question:** how much do `ToChannel` and a `BufferBlock` cost against a hand-written loop over the same rows?
- **Setup:** an in-memory provider; 1, 100 and 1,000 rows. Channel: the loop is a producer task that writes every row into an unbounded channel, and the reader drains it; ODX is `Map<T>().ToChannelReader(true)` drained with `ReadAllAsync`. Dataflow: the loop reads the rows, then sends each into a `BufferBlock`, which is drained; ODX is `Map<T>().ToTargetBlock(block, complete: true)`, drained. Both sides produce the same rows.
- **Settings:** BenchmarkDotNet 0.15.8, `--inprocess`, 5 warm-up and 12 measured iterations, one launch, at below-normal priority under a watchdog, one run at a time. Each time is one run; the CSV has no error column.
- **Build, date, machine:** build `6a770e8`, 2026-11-23; AMD Ryzen 9 7900, .NET 11.0.0, Windows 10.
- **Data:** [channels.csv](../../perf-data/channels.csv) ([source](../../perf-data/channels.source.txt)), from the BenchmarkDotNet reports.
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*ChannelBenchmarks*"` and `"*DataflowBenchmarks*"`.
