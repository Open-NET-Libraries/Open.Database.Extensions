# Benchmark data files

Every chart, and every table except the two marked "older run" (the everyday calls in the library comparison, and the call-site map in What building a map costs), is generated from a CSV file in the repository. Each CSV has a source file beside it that names the benchmark, the build, the date, the machine and the result file of every number. Download them, check them, or draw your own charts.

| Data file | Source and conditions | Used on |
|---|---|---|
| [library-synthetic.csv](../perf-data/library-synthetic.csv) | [source](../perf-data/library-synthetic.source.txt) | [Library comparison](benchmarks/library-comparison.md) |
| [library-real.csv](../perf-data/library-real.csv) | [source](../perf-data/library-real.source.txt) | [Library comparison](benchmarks/library-comparison.md) |
| [synthetic-overhead.csv](../perf-data/synthetic-overhead.csv) | [source](../perf-data/synthetic-overhead.source.txt) | [Every way of mapping](benchmarks/mapping-variants.md) (an older build) |
| [width-sweep.csv](../perf-data/width-sweep.csv) | [source](../perf-data/width-sweep.source.txt) | [Cost per column](benchmarks/cost-per-column.md) |
| [map-cost.csv](../perf-data/map-cost.csv) | [source](../perf-data/map-cost.source.txt) | [What building a map costs](benchmarks/map-cost.md) |
| [real-providers.csv](../perf-data/real-providers.csv) | [source](../perf-data/real-providers.source.txt) | [On real databases](benchmarks/real-databases.md) (an older build) |
| [sync-async.csv](../perf-data/sync-async.csv) | [source](../perf-data/sync-async.source.txt) | [Sync reads against async reads](benchmarks/sync-vs-async.md) |
| [link-time.csv](../perf-data/link-time.csv) | [source](../perf-data/link-time.source.txt) | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| [link-memory.csv](../perf-data/link-memory.csv) | [source](../perf-data/link-memory.source.txt) | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| [large-value.csv](../perf-data/large-value.csv) | [source](../perf-data/large-value.source.txt) | [Slow links and large values](benchmarks/slow-links-and-large-values.md) |
| [channels.csv](../perf-data/channels.csv) | [source](../perf-data/channels.source.txt) | [Channels and Dataflow](benchmarks/pipelines.md) |
| [pool-store.csv](../perf-data/pool-store.csv), [pool-rent.csv](../perf-data/pool-rent.csv), [pool-bytes.csv](../perf-data/pool-bytes.csv) | [store](../perf-data/pool-store.source.txt), [rent](../perf-data/pool-rent.source.txt), [bytes](../perf-data/pool-bytes.source.txt) | [Pooling and clearing](benchmarks/pooling-and-clearing.md) |
| [pool-e2e.csv](../perf-data/pool-e2e.csv), [pool-bin-cost.csv](../perf-data/pool-bin-cost.csv) | [e2e](../perf-data/pool-e2e.source.txt), [bin cost](../perf-data/pool-bin-cost.source.txt) | [Pooling and clearing](benchmarks/pooling-and-clearing.md) |
| [pool-release-path.csv](../perf-data/pool-release-path.csv), [pool-clear-speed.csv](../perf-data/pool-clear-speed.csv), [pool-bin-proof-e2e.csv](../perf-data/pool-bin-proof-e2e.csv), [pool-bin-cpu.csv](../perf-data/pool-bin-cpu.csv) | [release path](../perf-data/pool-release-path.source.txt), [clear speed](../perf-data/pool-clear-speed.source.txt), [bin end to end](../perf-data/pool-bin-proof-e2e.source.txt), [bin CPU](../perf-data/pool-bin-cpu.source.txt) | [Pooling and clearing](benchmarks/pooling-and-clearing.md) |

## How fresh the data is

The build is the commit of the benchmark project that ran; the date is when the run finished. Where a build is "not recorded", the numbers were copied from the console output of an older run, and the page says so.

[!include[](../includes/perf/freshness.md)]

For how the numbers were measured, see [How we measure](benchmarks/how-we-measure.md). To rebuild the charts and tables from new CSV files, see the [README next to the generator](https://github.com/Open-NET-Libraries/Open.Database.Extensions/tree/master/docsite/tools/perf).
