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
| ToChannel (drain a channel) | 2.05 vs 1.53 µs (tie; includes the thread hand-off) | 22.0 vs 42.5 µs (0.52x) | 133 vs 328 µs (0.41x) | 1.07 / 0.39 / 0.34 |
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
