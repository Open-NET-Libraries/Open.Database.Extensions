# The performance charts

The charts and tables on the Performance page are generated. Nothing on that page is drawn or typed by hand,
except the sentences that quote a number (see step 4 below).

| Path | What it is |
|---|---|
| `docsite/perf-data/*.csv` | The numbers behind every chart, one CSV per data set (units in the column names). |
| `docsite/perf-data/*.source.txt` | Beside each CSV: the benchmark, the build, the run date, the machine, the conditions and the BenchmarkDotNet result file of every number. |
| `docsite/tools/perf/Extract-PerfData.ps1` | Reads BenchmarkDotNet's own `*-report.csv` and `*-report-github.md` files of a run and writes the CSVs and the source files. |
| `docsite/tools/perf/New-PerfCharts.ps1` | Reads the CSVs and writes the SVG charts (`docsite/images/perf/`) and the data page (`docsite/articles/performance-data.md`). |

Regenerate every chart and the data page from the committed CSVs, with one command:

```sh
pwsh -NoProfile -File docsite/tools/perf/New-PerfCharts.ps1
```

The output is deterministic: a second run over the same CSVs gives byte-identical files.
Each SVG follows the operating system's light or dark setting and draws its own background.

## Updating the performance charts

Do this after a release candidate, or when a change moves performance. The numbers published on the site are
meant to come from the current `master`: a pull request that changes the charts is reviewed on whatever
results exist, and it merges only with data from a fresh run on current `master`. The run date, the build
(the commit of the benchmark project that ran) and the machine are shown on the data page, so a reader can
see how fresh the data is.

1. **Run the benchmarks** on a quiet machine: nothing else running, one benchmark at a time. Use a build of
   current `master`, in Release, in process, one launch (these are the settings of the published runs):

   ```sh
   dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- \
     --filter "<filter>" --warmupCount 5 --iterationCount 12 --launchCount 1 --inprocess
   ```

   Run each row of the table below in its own working folder, so each run's `BenchmarkDotNet.Artifacts` is
   kept apart. Set the environment variables first. The times are those of the published runs, on a 12-core
   desktop; the total is about 59 minutes of measured runs.

   | Data set | Filter (class and methods) | Environment | Time |
   |---|---|---|---|
   | `synthetic-overhead`, `width-sweep` (widths 1, 5, 20) | three runs: `*SyntheticBenchmarks.V?_Sync`, `*SyntheticBenchmarks.V??_Sync`, `*SyntheticBenchmarks.V7_*_Sync` | `ODX_SYN_WIDTHS=1,5,20`, `ODX_SYN_ROWS=1,1000` | 7.7 + 8.5 + 7.1 min |
   | `width-sweep` (widths 3, 10) | the same three filters | `ODX_SYN_WIDTHS=3,10`, `ODX_SYN_ROWS=1000` | 2.2 + 3.2 + 2.7 min |
   | `map-cost` | `*SyntheticBenchmarks.V1_*Sync` (also matches `V1_Async`: harmless) | `ODX_SYN_WIDTHS=1,5,20`, `ODX_SYN_ROWS=1,1000` | 4.7 min |
   | `real-providers` | five methods of `RealProviderBenchmarks`: `V1_Sync`, `V2_Sync`, `V3_Sync`, `V7_Sync`, `V9c_Sync`, one run per provider and width: SqlServer and Sqlite, widths 5 and 20 | `ODX_RP_PROVIDERS`, `ODX_RP_WIDTHS`, `ODX_RP_ROWS=1,10000`, `ODX_RP_SCRATCH` and `ODX_RP_SQLITE_DIR` (folders for the LocalDB files and the SQLite file) | 2.3 + 2.4 + 2.3 + 2.7 min |
   | `sync-async` | `*SyntheticBenchmarks.V1_Sync`, `*SyntheticBenchmarks.V1_Async`, `*SyntheticBenchmarks.V1_AsyncRead` (one `--filter` with the three globs), four runs named `SA1` to `SA4` | `ODX_SYN_WIDTHS=5,20`, `ODX_SYN_ROWS=1,1000`, then `ODX_SYN_BUFFER_ROWS` / `ODX_SYN_REFILL_US` / `ODX_SYN_PRESSURE` = SA1: 0 / 0 / 0; SA2: 0 / 0 / 8; SA3: 100 / 20 / 0; SA4: 100 / 20 / 8 | 3 min each, 12 min |

   The width sweep and the synthetic overhead chart use the same filters; the second row of the table fills
   the 3-column and 10-column points. Each (variant, width) must come from exactly one run.

2. **Build the CSVs** from the run folders (one command; it reads only BenchmarkDotNet's own exported files,
   and stops if a case appears in two runs or a file is missing):

   ```powershell
   pwsh -NoProfile -File docsite/tools/perf/Extract-PerfData.ps1 `
     -SyntheticRuns <folders of the three width 1, 5, 20 runs> `
     -SweepRuns <the same three, then the three width 3, 10 runs> `
     -MapCostRuns <the map-cost run folder> `
     -RealRuns <the four real-provider run folders> `
     -SyncAsyncRuns <the four SA1..SA4 run folders> `
     -SyntheticBuild <commit> -MapCostBuild <commit> -RealBuild <commit> -SyncAsyncBuild <commit> `
     -OutDir docsite/perf-data
   ```

   The builds are the commit of the benchmark project that ran (`git rev-parse --short HEAD`). The run date,
   the machine and the runtime come from the files themselves.

3. **Build the charts and the data page**: the one command at the top of this file.

4. **Re-read the sentences that quote a number.** The explanation under each chart in
   `docsite/articles/performance.md` names the winner and the margin in words, and the SVG alt text lists
   the ranking; both are typed, not generated. Compare each against the new data page and fix what moved.
   The data page and the SVGs need no editing.

5. **Check and commit**: build the docs (`dotnet docfx docsite/docfx.json`), look at the charts in light and
   dark, and commit the CSVs, the source files, the SVGs and the pages together. In the pull request, state
   the run date, the commit and the machine of the new data.

Not in the repository: the BenchmarkDotNet result folders themselves. The `*.source.txt` file names every
result file by run folder, so the numbers can be traced to them on the machine that ran the benchmarks.
