# Benchmark results: Open.Database.Extensions 11.0.0 vs Dapper 2.1.89

Both libraries run the same job against the same in-memory ADO.NET provider (`BenchProvider.cs`), so each
difference is the library's own overhead. Rows have 12 columns (int, long, short, bool, double, decimal,
DateTime, Guid, strings, three of them nullable).

- BenchmarkDotNet 0.15.8, .NET 10.0.14, X64 RyuJIT, AMD Ryzen 9 7900, 5 warmup + 12 measured iterations, 1 launch, Server GC off.
- **Ratio** is ODE time / Dapper time (below 1 is faster). **Alloc** is ODE bytes / Dapper bytes (below 1 is less memory).
- Reproduce: `dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*"`
- Not measured here: real network/database time (the provider is in-memory). Cross-check against a real provider before quoting these as end-to-end gains.

| Scenario | Rows | Dapper | ODE | Ratio | Dapper alloc | ODE alloc | Alloc |
|---|---:|---:|---:|---:|---:|---:|---:|
| Array | 1 | 329 ns | 122 ns | 0.37 | 888 B | 344 B | 0.39 |
| Array | 100 | 11594 ns | 3368 ns | 0.29 | 37264 B | 12224 B | 0.33 |
| Array | 1000 | 106683 ns | 33787 ns | 0.32 | 363072 B | 120224 B | 0.33 |
| Column | 1 | 209 ns | 74 ns | 0.35 | 536 B | 336 B | 0.63 |
| Column | 100 | 1250 ns | 358 ns | 0.29 | 4024 B | 1840 B | 0.46 |
| Column | 1000 | 9915 ns | 2528 ns | 0.25 | 32864 B | 12680 B | 0.39 |
| Dictionary | 1 | 333 ns | 131 ns | 0.39 | 1072 B | 528 B | 0.49 |
| Dictionary | 100 | 11366 ns | 4180 ns | 0.37 | 39568 B | 21592 B | 0.55 |
| Dictionary | 1000 | 112348 ns | 38255 ns | 0.34 | 386064 B | 214416 B | 0.56 |
| Execute | 1 | 66 ns | 37 ns | 0.56 | 136 B | 168 B | 1.24 |
| Execute | 100 | 70 ns | 38 ns | 0.55 | 136 B | 168 B | 1.24 |
| Execute | 1000 | 67 ns | 38 ns | 0.56 | 136 B | 168 B | 1.24 |
| First | 1 | 281 ns | 105 ns | 0.37 | 576 B | 312 B | 0.54 |
| First | 100 | 352 ns | 103 ns | 0.29 | 576 B | 312 B | 0.54 |
| First | 1000 | 866 ns | 102 ns | 0.12 | 576 B | 312 B | 0.54 |
| List | 1 | 314 ns | 117 ns | 0.37 | 856 B | 400 B | 0.47 |
| List | 100 | 10571 ns | 3248 ns | 0.31 | 36440 B | 13592 B | 0.37 |
| List | 1000 | 106826 ns | 31179 ns | 0.29 | 355048 B | 128800 B | 0.36 |
| Params | 1 | 334 ns | 153 ns | 0.46 | 1016 B | 752 B | 0.74 |
| Params | 100 | 10410 ns | 3731 ns | 0.36 | 36600 B | 13944 B | 0.38 |
| Params | 1000 | 108841 ns | 31949 ns | 0.29 | 355208 B | 129152 B | 0.36 |
| Scalar | 1 | 63 ns | 38 ns | 0.61 | 136 B | 168 B | 1.24 |
| Scalar | 100 | 64 ns | 40 ns | 0.62 | 136 B | 168 B | 1.24 |
| Scalar | 1000 | 61 ns | 40 ns | 0.66 | 136 B | 168 B | 1.24 |
| Selector | 1 | 83 ns | 73 ns | 0.88 | 264 B | 344 B | 1.30 |
| Selector | 100 | 565 ns | 511 ns | 0.90 | 4352 B | 4432 B | 1.02 |
| Selector | 1000 | 4389 ns | 3745 ns | 0.85 | 33096 B | 33176 B | 1.00 |
| Single | 1 | 270 ns | 105 ns | 0.39 | 576 B | 312 B | 0.54 |
| Single | 100 | 269 ns | 95 ns | 0.35 | 576 B | 312 B | 0.54 |
| Single | 1000 | 278 ns | 95 ns | 0.34 | 576 B | 312 B | 0.54 |
| Stream | 1 | 287 ns | 116 ns | 0.40 | 768 B | 408 B | 0.53 |
| Stream | 100 | 10576 ns | 3372 ns | 0.32 | 34248 B | 11496 B | 0.34 |
| Stream | 1000 | 106004 ns | 33338 ns | 0.31 | 338448 B | 112296 B | 0.33 |
| Untyped | 1 | 308 ns | 214 ns | 0.70 | 984 B | 1000 B | 1.02 |
| Untyped | 100 | 11665 ns | 11214 ns | 0.96 | 42672 B | 37376 B | 0.88 |
| Untyped | 1000 | 108936 ns | 98712 ns | 0.91 | 411688 B | 363184 B | 0.88 |
| ArrayAsync | 1 | 322 ns | 220 ns | 0.68 | 856 B | 488 B | 0.57 |
| ArrayAsync | 100 | 10497 ns | 3749 ns | 0.36 | 37232 B | 12368 B | 0.33 |
| ArrayAsync | 1000 | 98123 ns | 34937 ns | 0.36 | 363040 B | 120368 B | 0.33 |
| ExecuteAsync | 1 | 96 ns | 60 ns | 0.63 | 168 B | 168 B | 1.00 |
| ExecuteAsync | 100 | 96 ns | 62 ns | 0.64 | 168 B | 168 B | 1.00 |
| ExecuteAsync | 1000 | 97 ns | 62 ns | 0.64 | 168 B | 168 B | 1.00 |
| FirstAsync | 1 | 295 ns | 178 ns | 0.60 | 736 B | 456 B | 0.62 |
| FirstAsync | 100 | 470 ns | 179 ns | 0.38 | 736 B | 456 B | 0.62 |
| FirstAsync | 1000 | 1951 ns | 190 ns | 0.10 | 736 B | 456 B | 0.62 |
| ListAsync | 1 | 305 ns | 196 ns | 0.64 | 824 B | 544 B | 0.66 |
| ListAsync | 100 | 10630 ns | 3525 ns | 0.33 | 36408 B | 13736 B | 0.38 |
| ListAsync | 1000 | 99021 ns | 41350 ns | 0.42 | 355016 B | 128944 B | 0.36 |
| ParamsAsync | 1 | 350 ns | 242 ns | 0.69 | 1040 B | 896 B | 0.86 |
| ParamsAsync | 100 | 10237 ns | 3670 ns | 0.36 | 36624 B | 14088 B | 0.38 |
| ParamsAsync | 1000 | 101862 ns | 34206 ns | 0.34 | 355232 B | 129296 B | 0.36 |
| ScalarAsync | 1 | 81 ns | 74 ns | 0.91 | 296 B | 312 B | 1.05 |
| ScalarAsync | 100 | 80 ns | 78 ns | 0.98 | 296 B | 312 B | 1.05 |
| ScalarAsync | 1000 | 79 ns | 78 ns | 0.98 | 296 B | 312 B | 1.05 |

## Reading the table

- **Speed:** ODE is at or below Dapper's time in all 54 rows (worst: async scalar at 0.98, i.e. a tie). Mapped results are 2-3x faster at 100+ rows and 2-9x faster for `First`/`Single`.
- **Memory:** multi-row results use about a third of Dapper's allocations (typed getters, so no boxing). A few tiny fixed-cost rows allocate slightly more than Dapper: `Scalar`/`Execute` by 32 B (the fluent command object), async scalar by 16 B, `Selector` at 1 row by 80 B, `Untyped` at 1 row by 16 B.
- **Before this work** (same machine, same provider): every `Map<T>` query paid a ~0.94 ms fixed cost (1 row: 957,000 ns vs Dapper's 313 ns) and ~210 ns/row afterwards vs Dapper's ~115 ns.
- **ZLinq** (`LinqBenchmarks`): ties a plain loop and beats `System.Linq`, but never beats the loops this library uses, has no async support, and adds package dependencies on `netstandard2.0`, so it is not a dependency.

## Real providers and the other features (short job, `--inprocess`; the provider dominates the time, so ties within a few percent are noise)

Both libraries return identical objects (checked in setup).

**SQL Server (LocalDB, Microsoft.Data.SqlClient 7.1.1), ODE vs Dapper**

| Scenario | 1 row | 100 rows | 1000 rows | alloc |
|---|---:|---:|---:|---:|
| List | 68.5 vs 72.0 µs | 128 vs 136 µs | 797 vs 801 µs | 0.91 / 0.58 / 0.55 |
| ListAsync | 93.2 vs 93.7 µs | 153 vs 162 µs | 815 vs 868 µs | 0.97 / 0.60 / 0.55 |
| Params | 78.3 vs 77.8 µs | 139 vs 148 µs | 785 vs 852 µs | 0.96 / 0.60 / 0.55 |
| First | 70.1 vs 70.2 µs | 117 vs 120 µs | 542 vs 551 µs | 0.94 |
| Scalar | 61.5 vs 60.0 µs | 72.3 vs 73.0 µs | 149 vs 149 µs | 1.02 |

**SQLite** (see the PR comment for the table): List/ListAsync/First/Params 0.82-0.97x time, 0.73-0.93x allocations; Scalar a tie at 1.04x allocations.

**Other features (in-memory or SQLite provider; Dapper side is the idiomatic hand-written equivalent)**

| Feature | 1 row | 100 rows | 1000 rows | alloc |
|---|---:|---:|---:|---:|
| ToChannel (drain a channel) | 3.1 vs 1.3 µs (2.4x slower) | 45 vs 36 µs (1.24x slower) | 359 vs 284 µs (1.26x slower) | 2.20 / 1.09 / 1.01 |
| Dataflow (`BufferBlock`) | 9.6 vs 2.2 µs (4.4x slower) | 42.7 vs 15.0 µs (2.8x slower) | 322 vs 130 µs (2.5x slower) | 2.85 / 1.12 / 1.01 |
| DataTable | 84.8 vs 86.2 µs | 244 vs 247 µs | 2,019 vs 2,007 µs | 1.00 (a tie) |
| Transaction | 4.05 vs 4.12 µs | 5.56 vs 5.79 µs | 20.3 vs 20.3 µs | 1.08 |
| `Retrieve().DequeueAs<T>()` | **0.94x** (290 vs 308 ns) | **1.29x** slower | **1.33x** slower | 1.48 / 1.39 / 1.39 |

**ToChannel and Dataflow:** these keep their original two-stage design (raw rows into a bounded 1024-row buffer, a second stage maps them). The "Dapper" rows above are a hand-written single-stage loop. See the channel study below before reading anything into the gap.

`Retrieve().DequeueAs<T>()` is a deliberate two-pass route (copy every row into boxed `object[]`, map later), so it cannot match a single-pass typed read at larger sizes. It improved from 3.3x slower at 1 row. For typed results prefer `Map<T>()` or `ResultsBuffered<T>()` (which buffers mapped rows in a draining queue).

## Channel study: does the second stage earn its place?

`dotnet Open.Database.Extensions.Benchmarks.dll --channel-study` (in-memory provider, so reads are instant and "reader-held" is the time the reader/connection is busy). Three designs, with a fast and slow consumer and an unbounded or bounded (100) target channel:

- **one-stage**: map on the reading thread, write straight to the target.
- **two-stage**: ODE's `ToChannel` as it exists on `master`: raw rows into a bounded 1024-row buffer, a second task maps them into the target.
- **spill**: write straight to the target while it accepts; only when it is full, spill into a bounded 1024-row buffer drained by a pump task. The target is completed after everything is delivered.

Selected rows (ms, median of 9; the full table is the program's output):

| rows | target | consumer | one-stage held / total | two-stage held / total | spill held / total |
|---:|---|---|---:|---:|---:|
| 1000 | unbounded | none | 0.38 / 0.41 | **1.23 / 1.23** | 0.39 / 0.39 |
| 10000 | unbounded | none | 1.61 / 1.61 | **5.09 / 5.09** | 1.50 / 1.50 |
| 1000 | bounded 100 | 2 µs/item | 2.04 / 2.25 | 0.26 / 0.47 **LOST 807 rows** | 0.10 / 2.24 |
| 1000 | bounded 100 | 20 µs/item | 18.8 / 20.8 | 0.24 / 2.26 **LOST 890 rows** | 0.11 / 20.8 |
| 10000 | bounded 100 | 20 µs/item | 206 / 208 | 184 / 186 **LOST 1025 rows** | 185 / 208 |

What it shows:
1. **With an unbounded target the second stage buys nothing** and costs 2-3x on the producer side (and the same total time when the consumer is the bottleneck).
2. **The second stage's one real effect is freeing the reader earlier when the target is bounded and the consumer is slow**, but as implemented it **drops rows**: the target is completed when stage one finishes reading, while stage two still holds up to ~1024 rows. The study measured the same loss on untouched `master` (a separate project against a clean `origin/master` checkout), so this is pre-existing, not caused by this PR.
3. **Spill keeps the benefit without the loss**: the reader is freed as soon as the 1024-row buffer absorbs the backlog (0.1 ms vs 18.8 ms for 1000 rows), no rows are lost, and it runs at one-stage speed when the target keeps up.

## `RowMap<T>`: a prebuilt column mapping when every column is named differently

A 12-property model read from a result set whose every column is named differently (`post_id`, `post_title`, …). Four ways to read it:
`Map<T>()` on a model whose property names are the column names (the control: same result set, same column-name lengths, so the provider does the same work), a declared `RowMap<T>`, the pairs overload (`Map<T>(RenamedColumns.Pairs.AsSpan())`, which builds a new map and compiles the row reader on every call; the timings below were taken with the value-tuple overload that preceded it, removed in 11.0, which did the same work), and Dapper with `SqlMapper.SetTypeMap` set once.

**What was measured when.** The timings below were taken at commit 9840925, before the final API of 11.0.0-preview02. The compiled reader and the per-layout cache of a declared map were not changed afterwards (a query now also reads `map ?? RowMap<T>.Default` once), but the tables were not re-timed on the final code. The scenarios that existed then and no longer exist (a cached, lenient per-call overload and an inline-pairs overload) are not reported as current results; the one-off column below is the inline-pairs overload, which built a new map and compiled the row reader on every call, the same work that a collection expression written at the call site and the tuple overloads do now (`List_TupleOverload` and `First_TupleOverload` in the benchmark classes were added after that run and have not been timed). The allocation figures for the final code come from `--map-memory`, below.

Machine and protocol for the timings: AMD Ryzen 9 7900 (24 logical cores), Windows 10 22H2, .NET SDK 10.0.403, BenchmarkDotNet 0.15.8, Release, in-process, `--warmupCount 5 --iterationCount 12 --launchCount 1`, normal priority, one benchmark process at a time with the build servers shut down and nothing else running.
Total CPU was sampled for 15 s (1 s samples) before and after each class: before 6.9% (in-memory), 9.8% (LocalDB), 7.3% (lazy); after 5.3%, 9.1%, 5.7%. All are under the 12% threshold set for calling a class noisy, so none is labelled noisy; single-second peaks reached 15.9%. The samples are of the machine around each class, not during it.
Reproduce: `dotnet <benchmarks dll> --filter "*.RenamedColumnBenchmarks.*" --warmupCount 5 --iterationCount 12 --launchCount 1 --inprocess` (in-memory), `"*.SqlServerRenamedColumnBenchmarks.*"` (LocalDB), `"*.LazyMapBenchmarks.*"`, and `--map-memory`.

**In-memory provider: time per call (ns)**

| Scenario | Rows | `Map<T>()` same names | `RowMap<T>` | One-off: a new map every call (measured on the removed inline-pairs overload) | Dapper `SetTypeMap` | Map ÷ control | Map ÷ Dapper |
|---|---:|---:|---:|---:|---:|---:|---:|
| List | 1 | 114.5 | 113.3 | 444,904 | 338.8 | 0.99 | 0.33 |
| List | 100 | 3,127.6 | 3,214.2 | 451,093 | 11,678.3 | 1.03 | 0.28 |
| List | 1000 | 31,857.5 | 30,856.0 | 472,645 | 112,212.3 | 0.97 | 0.27 |
| First | 1 | 101.5 | 99.5 | 428,867 | 312.7 | 0.98 | 0.32 |
| First | 100 | 100.6 | 100.7 | 438,554 | 376.6 | 1.00 | 0.27 |
| First | 1000 | 103.9 | 99.6 | 433,027 | 910.6 | 0.96 | 0.11 |

**In-memory provider: allocated bytes per call (same run)**

| Scenario | Rows | `Map<T>()` same names | `RowMap<T>` | One-off: a new map every call (inline-pairs overload) | Dapper `SetTypeMap` |
|---|---:|---:|---:|---:|---:|
| List | 1 | 368 | 368 | 29,557 | 856 |
| List | 100 | 13,560 | 13,560 | 42,754 | 36,440 |
| List | 1000 | 128,768 | 128,768 | 157,964 | 355,048 |
| First | 1, 100, 1000 | 280 | 280 | 29,462-29,470 | 576 |

**The one-off path** builds a new map and compiles the row reader on every call and caches nothing. On the in-memory provider with the 12-property model a one-row read took about 0.43-0.45 ms (`First` 428.9 µs, `List` 444.9 µs) and allocated about 29.5 KB, against about 0.1 µs and 280-368 B through a declared map. The cost does not shrink with the result size: 451 µs and 42.8 KB at 100 rows, 473 µs and 158 KB at 1000.

**What each way of passing a map allocates on the final code** (`--map-memory`: a one-row `List` read of the 12-property model on the in-memory provider, average of 200 reads after 50 warm-up reads; allocation only, no timing)

| Way of passing the mapping | Allocated per call | Over a declared map |
|---|---:|---:|
| Declared `RowMap<T>` | 464 B | - |
| Tuple overload (removed in 11.0; a prebuilt array of 12 tuples) | 29,568 B | +29,104 B |
| `ImmutableArray` of pairs passed to the constructor at the call site | 29,288 B | +28,824 B |
| `new RowMap<T>([...])` of 12 pairs written at the call site | 29,507 B | +29,043 B |

The 464 B of the declared map includes the command and the list of this harness; the in-memory benchmark above allocates 368 B for the same read.

**SQL Server LocalDB (Microsoft.Data.SqlClient 7.1.1): time per call (µs)**

| Scenario | Rows | `Map<T>()` same names | `RowMap<T>` | Dapper `SetTypeMap` | Map ÷ control | Map ÷ Dapper |
|---|---:|---:|---:|---:|---:|---:|
| List | 1 | 70.4 | 69.2 | 72.3 | 0.98 | 0.96 |
| List | 100 | 132.4 | 133.6 | 142.5 | 1.01 | 0.94 |
| List | 1000 | 797.1 | 807.8 | 807.0 | 1.01 | 1.00 |
| First | 1 | 69.6 | 68.9 | 71.3 | 0.99 | 0.97 |
| First | 100 | 122.9 | 127.3 | 122.3 | 1.04 | 1.04 |
| First | 1000 | 564.7 | 578.0 | 558.0 | 1.02 | 1.04 |

The 95% confidence half-widths on LocalDB run from 0.3 µs at one row to 8.4 µs at 100 rows and 34 µs at 1000 rows (for example `First` of 100 rows: 127.3 ± 8.4 µs), so every LocalDB time ratio above is a tie within noise; the provider dominates the time.

**SQL Server LocalDB: allocated memory per call**

| Scenario | Rows | `Map<T>()` same names | `RowMap<T>` | Dapper `SetTypeMap` |
|---|---:|---:|---:|---:|
| List | 1 | 4.62 KB | 4.62 KB | 5.10 KB |
| List | 100 | 27.95 KB | 27.95 KB | 47.96 KB |
| List | 1000 | 236.77 KB | 236.78 KB | 434.32 KB |
| First | 1, 100, 1000 | 4.52 KB (4.54 at 1000) | 4.52 KB (4.53-4.54 at 100, 1000) | 4.82 KB (4.83 at 100, 1000) |

**Reading the numbers (same-run ratios above).**
- *Allocation:* a `RowMap<T>` read allocates exactly what `Map<T>()` allocates on matching names (0 B difference in memory, at most 0.01 KB on LocalDB), at 1, 100 and 1000 rows, and less than Dapper with a type map in every scenario (in memory 0.43x at one row and 0.36-0.37x at 100 and 1000; on LocalDB 0.91x, 0.58x and 0.55x, and 0.94x for `First`).
- *Time against `Map<T>()` on matching names:* in memory the ratios are 0.96-1.03; the 1.03 at 100 rows is matched by the short-named `Map<Post>()` at 1.04 in the same run, so it follows the model class, not the map. On LocalDB 0.98-1.04 are ties within the intervals.
- *Time against Dapper with `SetTypeMap`:* in memory a map read takes 0.11-0.33x Dapper's time. On LocalDB it is 0.94-1.04x, a tie within noise; the two `First` rows at 100 and 1000 rows are 1.04x (+5 µs and +20 µs, inside the ±8 µs and ±25 µs intervals): not below Dapper, not distinguishable from it.
- *What declaring a map saves:* a one-row in-memory read through a map takes about 0.1 µs; the one-off path took about 0.43 ms and allocated about 29.5 KB, and the cost does not shrink with larger results (above). The pairs overload of `Map<T>` and a `new RowMap<T>([...])` written at the call site do that same work on every call.
- *The interface lookup of the row reader* (`RowSource<T>` became an internal interface so the map can be the public type) cost nothing measurable. The control `Map<T>()` took the same change, and against the earlier run on the first design it moved from 119.9 to 114.5 ns (List, 1 row), 3,434.6 to 3,127.6 ns (100 rows), 31,675.5 to 31,857.5 ns (1000 rows), and 104.5 to 101.5, 102.0 to 100.6, 103.1 to 103.9 ns for `First`: between -9% and +0.8%, while the Dapper column drifted by up to +8% (10,778 to 11,678 ns at 100 rows) between the two runs, so run-to-run noise is of that size. The map itself moved from 121.0 to 113.3 ns, 3,471.7 to 3,214.2 ns and 32,358.4 to 30,856.0 ns (List) and 101.8 to 99.5, 102.0 to 100.7, 102.8 to 99.6 ns (First): never slower.
- Against `Map<T>()` on the *short-named* table the renamed result set allocates 0.13 KB more on LocalDB (4.49 vs 4.62 KB for `List` of one row), consistent with SqlClient allocating each column name on every query (62 more characters across the 12 columns is 124 B); the control uses the same names for that reason.

**What a map costs to build and retains** (`--map-memory`, managed heap only, 12 fields, averaged over 1,000 maps; the machine code the JIT emits for a compiled reader is not counted)

| | First design: `RowMap<T>` built from tuples, wrapping a separate map object | `RowMap<T>` built from an `ImmutableArray` of pairs | One column table instead of three structures (9840925) | Final: the pairs are no longer retained |
|---|---:|---:|---:|---:|
| Allocated to build one from a prepared array | 3,392 B | 1,088 B | 224 B | 216 B |
| Allocated to build one from a literal written at the call site | 3,632 B | 1,304 B | 440 B | 432 B |
| Retained while unused | 1,536 B | 936 B | 192 B | 184 B |
| Added by its first read (one result-set layout, typed reader compiled) | 6,369 B | 6,369 B | 6,369 B | 6,369 B |
| Total retained after the first read | 7,905 B | 7,305 B | 6,561 B | 6,553 B |
| Objects allocated to build one (counted from the code, not instrumented) | about 19 | 8 | 3 | 3 |

Eliminated along the way: the second object (the wrapper around the map), the builder and its growing arrays that re-materialised the pairs, the `HashSet` of named properties and the transient property-to-column dictionary (now a `string?[]` and a `bool[]`), the retained property-to-column dictionary, the two name arrays, the tuple enumeration and, in the final column, the retained copy of the caller's pairs (a map enumerates the mapping from its column table). The first read adds the same 6,369 B in every design. Five hundred models cost about 0.1 MB if every map were built up front and about 3.3 MB if every one had been read, which is why the documentation builds each map on first use.

**Holding a map** (`LazyMapBenchmarks`; warm reads, 1,000 per invocation, each through a non-inlined call so the JIT cannot fold the read away, so each time includes that call; per-operation figures; two quiet runs shown: the earlier one on the first design, the later one at 9840925)

| Variant | Time per read (earlier, later) | Allocated per read |
|---|---:|---:|
| `static readonly` field | 0.988 ns, 0.996 ns | 0 B |
| `field ??= [...]` | 1.194 ns, 1.856 ns | 0 B |
| `Lazy<T>.Value` | 1.186 ns, 1.180 ns | 0 B |
| `LazyInitializer.EnsureInitialized(ref field, static () => ...)` | 1.989 ns, 2.009 ns | 0 B |
| Build one 12-field map (the first-call cost, before compiling the reader) | 996 ns / 3,360 B (first design), 200.4 ns / 224 B (at 9840925; 216 B on the final code) | |
| Create the `Lazy<T>` (the `Lazy<T>` and its internal helper object; the factory delegate is a cached static lambda and is not counted) | 5.6 ns, 5.7 ns | 72 B |

Every variant reads in about 1-2 ns and allocates nothing. The differences between variants (up to about 1 ns) are small, and the `field ??=` figure moved by 0.7 ns between the two runs with nothing changed in that code, so do not read an ordering into them; all of them are negligible next to a query that costs at least 70 µs. A first call costs about 0.2 µs to build the map, plus compiling the reader for the result-set layout (the same as for `Map<T>()`).
