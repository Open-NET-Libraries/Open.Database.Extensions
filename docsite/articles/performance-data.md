# Benchmark data files

Every chart and table on the [benchmark pages](performance.md) is generated from a CSV file in the repository. Each CSV has a source file beside it that names the benchmark, the build, the date, the machine and the result file of every number. Download them, check them, or draw your own charts.

| Data file | Source and conditions | Used on |
|---|---|---|
| [synthetic-overhead.csv](../perf-data/synthetic-overhead.csv) | [source](../perf-data/synthetic-overhead.source.txt) | [Library comparison](benchmarks/library-comparison.md), [Every way of mapping](benchmarks/mapping-variants.md) |
| [width-sweep.csv](../perf-data/width-sweep.csv) | [source](../perf-data/width-sweep.source.txt) | [Cost per column](benchmarks/cost-per-column.md) |
| [map-cost.csv](../perf-data/map-cost.csv) | [source](../perf-data/map-cost.source.txt) | [What building a map costs](benchmarks/map-cost.md) |
| [real-providers.csv](../perf-data/real-providers.csv) | [source](../perf-data/real-providers.source.txt) | [On real databases](benchmarks/real-databases.md) |
| [sync-async.csv](../perf-data/sync-async.csv) | [source](../perf-data/sync-async.source.txt) | [Sync reads against async reads](benchmarks/sync-vs-async.md) |
| [link-time.csv](../perf-data/link-time.csv) | [source](../perf-data/link-time.source.txt) | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| [link-memory.csv](../perf-data/link-memory.csv) | [source](../perf-data/link-memory.source.txt) | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| [large-value.csv](../perf-data/large-value.csv) | [source](../perf-data/large-value.source.txt) | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| [channels.csv](../perf-data/channels.csv) | [source](../perf-data/channels.source.txt) | [Channels and Dataflow](benchmarks/pipelines.md) |

## How fresh the data is

The build is the commit of the benchmark project that ran; the date is when the run finished. Where a build is "not recorded", the numbers were copied from the console output of an older run, and the page says so.

[!include[](../includes/perf/freshness.md)]

For how the numbers were measured, see [How we measure](benchmarks/how-we-measure.md). To rebuild the charts and tables from new CSV files, see the [README next to the generator](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf).
