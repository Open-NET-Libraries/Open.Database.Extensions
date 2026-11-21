# What building a map costs

**The question:** `Map<T>()` is fast because it builds a mapping once and reuses it. How much does that one build cost, and what happens if you build one on every call?

## In short

- **Building and compiling a mapping costs 0.19 to 0.87 ms** (1 to 20 columns) and allocates 15 to 52 KB. ODX does it **once per result-set shape** and caches it.
- **After that a one-row call takes 0.17 to 0.27 µs.** A call that builds a new mapping every time takes 192 to 875 µs, 1,100 to 3,300 times longer. That is why ODX never builds one per call.
- **Not faster, if you hold it wrong:** a `RowMap<T>` written at the call site (`new RowMap<T>([...])`) is built again on every call and pays the full cost each time. Declare it once, in a field. Measured on an older run: 0.43 ms and about 29.5 KB per call, against 0.1 µs through a declared map.

## The chart

<div class="perf-chart"><img src="../../images/perf/perf-map-cost.svg" alt="Bars of the one-time cost of building and compiling a map: 0.19 milliseconds for 1 column, 0.33 for 5 columns and 0.87 for 20 columns."></div>

How long it takes to build and compile one mapping, for a result of 1, 5 and 20 columns. It is the extra time of a call that builds a new mapping, compared with a call that reuses the cached one. Shorter is better, and it is paid once.

[!include[](../../includes/perf/map-cost.md)]

The first column of numbers is the cost of a call that reuses the mapping: that is what every call after the first pays.

## Why it is done once

A mapping is the plan that says which column goes into which property, and the compiled code that reads a row that way. Building it means reading the type, matching names and compiling. The cost grows with the number of columns. A busy service reads the same shape of result thousands of times, so paying it once and reusing the result is the whole idea of `Map<T>()`.

You get this by default: a plain `Map<T>()` finds its cached mapping by the result-set shape. Only a `RowMap<T>` you write yourself needs you to hold on to it. See [Mapping](../mapping.md#columns-named-differently).

## When the mapping is built on every call (older run)

The call-site forms (a collection expression or `new RowMap<T>([...])` written inline) build a new mapping on every call. This older run measured it on a 12-property model, a one-row read, an in-memory provider, at commit `9840925`, .NET 10 and BenchmarkDotNet 0.15.8, before the final API of 11.0.0-preview02. It was not repeated on the final code and has no data file. The source is [`RESULTS.md`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md#rowmapt-a-prebuilt-column-mapping-when-every-column-is-named-differently).

| One-row `List` read | Time | Memory |
|---|---:|---:|
| `Map<T>()` on matching names (the control) | 114.5 ns | 368 B |
| A declared `RowMap<T>` | 113.3 ns | 368 B |
| A new mapping on every call | 444,904 ns | 29,557 B |
| Dapper with `SetTypeMap` | 338.8 ns | 856 B |

The cost of building does not shrink with larger results: 451 µs and 42,754 B at 100 rows, 473 µs and 157,964 B at 1,000. On SQL Server LocalDB the declared map and plain `Map<T>()` tie within noise.

The same file measured ways to hold a declared map: a `static readonly` field, `field ??=`, `Lazy<T>` and `LazyInitializer` all read in about 1 to 2 ns and allocate nothing. Pick whichever reads best; the choice is not measurable next to a query.

## How this was measured

- **Question:** what does one build of a mapping cost, against reusing a cached one?
- **Setup:** the synthetic reader, one row per call, 1, 5 and 20 typed columns. One case reuses a cached mapping; the other builds and compiles a new one on every call. Synchronous terminals.
- **Settings:** BenchmarkDotNet 0.15.8, in-process, 5 warm-up and 12 measured iterations, one launch, one run per case. The one-time cost on the chart is the time (and memory) of the building case minus the reusing case. See [How we measure](how-we-measure.md).
- **Build, date, machine:** build `544cc01`, 2026-11-14; AMD Ryzen 9 7900, .NET 11.0.0, Windows 10.
- **Data:** [map-cost.csv](../../perf-data/map-cost.csv) ([source and conditions](../../perf-data/map-cost.source.txt)).
- **Reproduce:** `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*SyntheticBenchmarks.V1_*Sync"` with `ODX_SYN_WIDTHS=1,5,20` and `ODX_SYN_ROWS=1,1000`.
