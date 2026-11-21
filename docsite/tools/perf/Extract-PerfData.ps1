<#
.SYNOPSIS
  Builds the CSV files behind the performance charts from BenchmarkDotNet result folders.

.DESCRIPTION
  Each run folder is one BenchmarkDotNet run: it holds BenchmarkDotNet.Artifacts/results/*-report.csv
  (and the matching *-report-github.md, whose header names the host). Nothing is read from anywhere
  else, so a re-run is a data swap: point the parameters at the new run folders, run this script,
  then run New-PerfCharts.ps1.

  Writes to OutDir: synthetic-overhead.csv, width-sweep.csv, map-cost.csv, real-providers.csv and
  sync-async.csv, and one <name>.source.txt per CSV (the benchmark, build, date, machine,
  conditions, source files).

  Variant ids (V0, V1, ...) are the names the benchmark classes use; the table below gives each its
  plain name, its family (which fixes the chart color) and a one-line description.

  The four sync-versus-async run folders must end in SA1, SA2, SA3 and SA4 (the benchmark's own
  condition names; see $conditions below).

.EXAMPLE
  ./Extract-PerfData.ps1 -SyntheticRuns a,b,c -SweepRuns a,b,c,d,e,f -MapCostRuns g -RealRuns h,i,j,k `
    -SyncAsyncRuns l1,l2,l3,l4 -SyntheticBuild b27a02f -MapCostBuild 544cc01 -RealBuild fbb13d7 `
    -SyncAsyncBuild ea5f096 -OutDir ../../perf-data
#>
param(
  [Parameter(Mandatory)][string[]]$SyntheticRuns,
  [Parameter(Mandatory)][string[]]$SweepRuns,
  [Parameter(Mandatory)][string[]]$MapCostRuns,
  [Parameter(Mandatory)][string[]]$RealRuns,
  [Parameter(Mandatory)][string[]]$SyncAsyncRuns,
  [Parameter(Mandatory)][string]$SyntheticBuild,
  [Parameter(Mandatory)][string]$MapCostBuild,
  [Parameter(Mandatory)][string]$RealBuild,
  [Parameter(Mandatory)][string]$SyncAsyncBuild,
  [Parameter(Mandatory)][string]$OutDir
)
$ErrorActionPreference = 'Stop'
$inv = [Globalization.CultureInfo]::InvariantCulture

# id -> family key, plain name, description. Families: odx-one, odx-select, odx-raw, dapper, reference.
$variants = [ordered]@{
  'V0'     = @('odx-raw',    'object[] rows (baseline)',       'Every cell boxed into an object[] with GetValues; the baseline')
  'V0b'    = @('odx-raw',    'object[] rows, objects later',   'GetValues into object[], typed objects built afterwards')
  'V1'     = @('odx-one',    'Map<T>(), the RowMap plan',      'The cached, compiled typed plan')
  'V2'     = @('odx-one',    'Map(selector), hand-written',    'A static Func<IDataRecord,T> with typed getters')
  'V3'     = @('odx-select', 'Row class, inline Select',       'Two objects per row: a row class, then Select while the reader is open')
  'V4a'    = @('odx-one',    'Selector, reused row, Select',   'Caller recipe: fill one held row, project it')
  'V4b'    = @('odx-one',    'Selector, pooled row, Select',   'Caller recipe: rent a row, project it, return it')
  'V5'     = @('odx-select', 'Row class, close, then Select',  'Map<Row>().ToList(), then Select after the reader closes')
  'V6a'    = @('odx-one',    'Shaping inside the selector',    'Computed members built in the selector')
  'V6c'    = @('odx-select', 'Row class, shaping in Select',   'Computed members built in an inline Select, reader open')
  'V6d'    = @('odx-select', 'Row class, shaping after close', 'Computed members built after the reader closes')
  'V7'     = @('dapper',     'Dapper Query<T>',                'Dapper 2.1.89, buffered Query<Row> into a list')
  'V7_V1'  = @('dapper',     'Dapper Query<T>',                'Dapper 2.1.89, buffered Query<T>, the plain map')
  'V7_V3'  = @('dapper',     'Dapper, inline Select',          'Query<Row>(buffered: false) with Select')
  'V7_V5'  = @('dapper',     'Dapper, then Select after',      'Query<Row>, then Select after')
  'V7_V6c' = @('dapper',     'Dapper, shaping in Select',      'Unbuffered Query<Row>, computed members in Select')
  'V7_V6d' = @('dapper',     'Dapper, shaping after',          'Query<Row>, computed members afterwards')
  'V9c'    = @('reference',  'Hand loop (reference)',          'Typed getters written directly on the provider reader')
}

# The sync-versus-async benchmark's four conditions, by run name (the environment the run was started with).
$conditions = [ordered]@{
  'SA1' = @('Neither',               0,   0,  0)
  'SA2' = @('Busy thread pool',      0,   0,  8)
  'SA3' = @('Buffer refills',        100, 20, 0)
  'SA4' = @('Refills and busy pool', 100, 20, 8)
}
$paths = @{ 'V1_Sync' = 'Sync'; 'V1_Async' = 'Async execute'; 'V1_AsyncRead' = 'Async read' }

function ConvertTo-Microseconds([string]$text) {
  $m = [regex]::Match($text.Trim(), '^([\d,\.]+)\s*(\S+)$')
  if (-not $m.Success) { throw "cannot read time '$text'" }
  $v = [double]::Parse(($m.Groups[1].Value -replace ',', ''), $inv)
  switch ($m.Groups[2].Value) {
    'ns' { return $v / 1e3 }
    'ms' { return $v * 1e3 }
    's'  { return $v * 1e6 }
    default { return $v }   # the micro sign spelling (U+03BC or U+00B5) or 'us'
  }
}
function ConvertTo-Kilobytes([string]$text) {
  $m = [regex]::Match($text.Trim(), '^([\d,\.]+)\s*(B|KB|MB|GB)$')
  if (-not $m.Success) { throw "cannot read size '$text'" }
  $v = [double]::Parse(($m.Groups[1].Value -replace ',', ''), $inv)
  switch ($m.Groups[2].Value) {
    'B'  { return $v / 1024 }
    'KB' { return $v }
    'MB' { return $v * 1024 }
    'GB' { return $v * 1048576 }
  }
}
function Format-Num([double]$v, [int]$decimals) { [math]::Round($v, $decimals).ToString('0.' + ('#' * $decimals), $inv) }
function Quote([string]$s) { '"' + $s.Replace('"', '""') + '"' }

function Read-Run([string]$run) {
  $results = Join-Path $run 'BenchmarkDotNet.Artifacts/results'
  $csv = @(Get-ChildItem -LiteralPath $results -Filter '*-report.csv')
  if ($csv.Count -ne 1) { throw "run folder '$run': expected one *-report.csv, found $($csv.Count)" }
  $md = @(Get-ChildItem -LiteralPath $results -Filter '*-report-github.md')
  $rows = @(Import-Csv -LiteralPath $csv[0].FullName -Encoding UTF8)
  [pscustomobject]@{
    Name = Split-Path -Leaf $run; File = $csv[0].Name; Date = $csv[0].LastWriteTime.ToString('yyyy-MM-dd')
    Rows = $rows; Header = if ($md.Count -eq 1) { @(Get-Content -LiteralPath $md[0].FullName -TotalCount 9 | Where-Object { $_.Trim() -ne '' -and $_ -ne '```' }) } else { @() }
  }
}
# 'V7_V1_Sync' -> 'V7_V1'; any other method name -> $null
function Get-Variant([string]$method) {
  $m = [regex]::Match($method, '^(V[0-9a-z]+(?:_V[0-9a-z]+)?)_Sync$')
  if (-not $m.Success) { return $null }
  return $m.Groups[1].Value
}
function Write-Lf([string]$path, [string[]]$lines) {
  [IO.File]::WriteAllText($path, (($lines -join "`n") + "`n"), (New-Object Text.UTF8Encoding($false)))
}
function Write-Source([string]$name, $runs, [string]$build, [string[]]$conditionLines) {
  $dates = @($runs | ForEach-Object { $_.Date } | Sort-Object -Unique)
  $lines = @("Benchmark build: $build", "Run date: $($dates -join ', ')")
  $lines += @($runs[0].Header | Select-Object -First 4)
  $lines += $conditionLines
  $lines += @($runs[0].Header | Where-Object { $_ -match '^Toolchain=|^WarmupCount=' } | ForEach-Object { $_.Trim() })
  $lines += 'Source files (BenchmarkDotNet report CSVs, one per run; the run folders are not in the repository):'
  $lines += @($runs | ForEach-Object { "  $($_.Name)/BenchmarkDotNet.Artifacts/results/$($_.File)" })
  Write-Lf (Join-Path $OutDir "$name.source.txt") $lines
}
function Assert-Unique($items, [string]$what) {
  if (@($items | Group-Object Key | Where-Object Count -gt 1).Count -gt 0) { throw "$what appears in two runs" }
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$syn = @($SyntheticRuns | ForEach-Object { Read-Run $_ })
$swp = @($SweepRuns | ForEach-Object { Read-Run $_ })
$fresh = @($MapCostRuns | ForEach-Object { Read-Run $_ })
$real = @($RealRuns | ForEach-Object { Read-Run $_ })
$sa = @($SyncAsyncRuns | ForEach-Object { Read-Run $_ })

# ---- synthetic-overhead.csv: width 20, 1,000 rows, every variant ----
$items = @()
foreach ($run in $syn) {
  foreach ($r in $run.Rows | Where-Object { $_.Width -eq '20' -and $_.Rows -eq '1000' }) {
    $id = Get-Variant $r.Method
    if (-not $id) { continue }
    if (-not $variants.Contains($id)) { throw "unknown variant $id" }
    $v = $variants[$id]
    $items += [pscustomobject]@{ Key = $id; Line = ('{0},{1},{2},{3},20,1000,{4},{5},{6},{7}' -f $id, (Quote $v[1]), $v[0], (Quote $v[2]),
      (Format-Num ([double]::Parse($r.'ReaderOpen (us)', $inv)) 3), (Format-Num (ConvertTo-Microseconds $r.Mean) 3), (Format-Num (ConvertTo-Kilobytes $r.Allocated) 3), (Format-Num (ConvertTo-Microseconds $r.Error) 3), $run.Name) }
  }
}
Assert-Unique $items 'a synthetic variant'
Write-Lf (Join-Path $OutDir 'synthetic-overhead.csv') (@('variant_id,variant,family,description,columns,rows,reader_open_us,total_us,allocated_kb,source_run') + @($items | ForEach-Object Line))
Write-Source 'synthetic-overhead' $syn $SyntheticBuild @(
  'Benchmark: SyntheticBenchmarks (the library over an in-memory ADO.NET reader, no I/O, no per-row cost)',
  'Conditions: 20 typed columns, 1,000 rows per call, synchronous terminals, one run per case',
  'Columns: reader_open_us = mean time from ExecuteReader until the reader closes; total_us = mean time of the whole call (BenchmarkDotNet Mean); allocated_kb = bytes allocated per call / 1024')

# ---- width-sweep.csv: five variants, 1,000 rows, widths 1 3 5 10 20 ----
$sweepIds = 'V2', 'V1', 'V3', 'V0', 'V7_V1'
$items = @()
foreach ($run in $swp) {
  foreach ($r in $run.Rows | Where-Object { $_.Rows -eq '1000' }) {
    $id = Get-Variant $r.Method
    if (-not $id -or $sweepIds -notcontains $id) { continue }
    $v = $variants[$id]
    $items += [pscustomobject]@{ Id = $id; Width = [int]$r.Width; Key = "$id/$($r.Width)"; Line = ('{0},{1},{2},{3},1000,{4},{5}' -f $id, (Quote $v[1]), $v[0], $r.Width, (Format-Num (ConvertTo-Microseconds $r.Mean) 3), $run.Name) }
  }
}
Assert-Unique $items 'a variant and width'
$out = @('variant_id,variant,family,columns,rows,total_us,source_run')
foreach ($id in $sweepIds) { $out += @($items | Where-Object Id -eq $id | Sort-Object Width | ForEach-Object Line) }
Write-Lf (Join-Path $OutDir 'width-sweep.csv') $out
Write-Source 'width-sweep' $swp $SyntheticBuild @(
  'Benchmark: SyntheticBenchmarks (the library over an in-memory ADO.NET reader, no I/O, no per-row cost)',
  'Conditions: 1,000 rows per call, widths 1, 3, 5, 10 and 20 typed columns, synchronous terminals, one run per case',
  'Columns: total_us = mean time of the whole call (BenchmarkDotNet Mean)')

# ---- map-cost.csv: one row per call, a reused map against a map built on every call ----
$items = @()
foreach ($run in $fresh) {
  foreach ($w in '1', '5', '20') {
    $reused = @($run.Rows | Where-Object { $_.Method -eq 'V1_Sync' -and $_.Width -eq $w -and $_.Rows -eq '1' })
    $built = @($run.Rows | Where-Object { $_.Method -eq 'V1_Fresh_Sync' -and $_.Width -eq $w -and $_.Rows -eq '1' })
    if ($reused.Count -ne 1 -or $built.Count -ne 1) { throw "map cost: width $w is not exactly once in $($run.Name)" }
    $items += [pscustomobject]@{ Key = $w; Line = ('{0},1,{1},{2},{3},{4},{5}' -f $w, (Format-Num (ConvertTo-Microseconds $reused[0].Mean) 3), (Format-Num (ConvertTo-Kilobytes $reused[0].Allocated) 3),
      (Format-Num (ConvertTo-Microseconds $built[0].Mean) 3), (Format-Num (ConvertTo-Kilobytes $built[0].Allocated) 3), $run.Name) }
  }
}
Assert-Unique $items 'a map-cost width'
Write-Lf (Join-Path $OutDir 'map-cost.csv') (@('columns,rows,reused_total_us,reused_allocated_kb,built_every_call_total_us,built_every_call_allocated_kb,source_run') + @($items | ForEach-Object Line))
Write-Source 'map-cost' $fresh $MapCostBuild @(
  'Benchmark: SyntheticBenchmarks, V1_Sync (a map ODX builds once and caches) and V1_Fresh_Sync (a new map built and compiled on every call)',
  'Conditions: one row per call, widths 1, 5 and 20 typed columns, synchronous terminals, one run per case',
  'Columns: *_total_us = mean time of the whole call (BenchmarkDotNet Mean); *_allocated_kb = bytes allocated per call / 1024')

# ---- real-providers.csv: SQL Server LocalDB and SQLite file, 10,000 rows, widths 5 and 20 ----
$providers = @{ 'SqlServer' = 'SQL Server LocalDB'; 'Sqlite' = 'SQLite file' }
$items = @()
foreach ($run in $real) {
  foreach ($r in $run.Rows | Where-Object { $_.Rows -eq '10000' }) {
    $id = Get-Variant $r.Method
    if (-not $id) { continue }
    $v = $variants[$id]
    $items += [pscustomobject]@{ P = $r.Provider; W = [int]$r.Width; Id = $id; Key = "$($r.Provider)/$($r.Width)/$id"
      Line = ('{0},{1},{2},{3},{4},{5},10000,{6},{7},{8},{9}' -f (Quote $providers[$r.Provider]), $id, (Quote $v[1]), $v[0], (Quote $v[2]), $r.Width,
        (Format-Num (ConvertTo-Microseconds $r.Mean) 3), (Format-Num (ConvertTo-Kilobytes $r.Allocated) 3), (Format-Num (ConvertTo-Microseconds $r.Error) 3), $run.Name) }
  }
}
Assert-Unique $items 'a provider, width and variant'
$order = @($variants.Keys)
$sorted = $items | Sort-Object P, W, @{ Expression = { [array]::IndexOf($order, $_.Id) } }
Write-Lf (Join-Path $OutDir 'real-providers.csv') (@('provider,variant_id,variant,family,description,columns,rows,total_us,allocated_kb,error_us,source_run') + @($sorted | ForEach-Object Line))
Write-Source 'real-providers' $real $RealBuild @(
  'Benchmark: RealProviderBenchmarks (the library against SQL Server LocalDB through Microsoft.Data.SqlClient, and against a SQLite file through Microsoft.Data.Sqlite)',
  'Conditions: 10,000 rows per call, widths 5 and 20 typed columns, synchronous terminals, one run per case; the LocalDB database and the SQLite file are created for the run and removed after it',
  'SQLite: Dapper maps text to Guid through the usual SqlMapper.TypeHandler recipe; it does not convert it on its own',
  'Columns: total_us = mean time of the whole call (BenchmarkDotNet Mean); allocated_kb = allocated per call (BenchmarkDotNet, KB = 1024 bytes); error_us = BenchmarkDotNet Error, the half-width of the 99.9% confidence interval of total_us')

# ---- sync-async.csv: the RowMap plan read three ways under four conditions ----
$items = @()
foreach ($run in $sa) {
  $key = [regex]::Match($run.Name, '(SA[1-4])$').Groups[1].Value
  if (-not $conditions.Contains($key)) { throw "run folder '$($run.Name)' must end in SA1, SA2, SA3 or SA4" }
  $c = $conditions[$key]
  foreach ($r in $run.Rows | Where-Object { $_.Width -eq '20' -and $_.Rows -eq '1000' }) {
    if (-not $paths.ContainsKey($r.Method)) { continue }
    $items += [pscustomobject]@{ Key = "$key/$($r.Method)"
      Cond = $key; Path = $paths[$r.Method]
      Line = ('{0},{1},{2},{3},{4},20,1000,{5},{6},{7}' -f (Quote $c[0]), $c[1], $c[2], $c[3], (Quote $paths[$r.Method]),
        (Format-Num ([double]::Parse($r.'ReaderOpen (us)', $inv)) 3), (Format-Num (ConvertTo-Kilobytes $r.Allocated) 3), $run.Name) }
  }
}
Assert-Unique $items 'a condition and path'
$pathOrder = @('Sync', 'Async execute', 'Async read')
$condOrder = @($conditions.Keys)
$sorted = $items | Sort-Object @{ Expression = { $condOrder.IndexOf($_.Cond) } }, @{ Expression = { $pathOrder.IndexOf($_.Path) } }
Write-Lf (Join-Path $OutDir 'sync-async.csv') (@('condition,buffer_refill_rows,refill_us,busy_pool_work_items,path,columns,rows,reader_open_us,allocated_kb,source_run') + @($sorted | ForEach-Object Line))
Write-Source 'sync-async' $sa $SyncAsyncBuild @(
  'Benchmark: SyntheticBenchmarks V1_Sync, V1_Async, V1_AsyncRead (the RowMap plan read three ways; the in-memory reader models a buffer refill and a busy thread pool)',
  'Conditions: 20 typed columns, 1,000 rows per call, one run per case; buffer_refill_rows = rows between refills (0 = none), refill_us = the wait of each refill, busy_pool_work_items = thread-pool work items kept queued (0 = none)',
  'Paths: Sync = ExecuteReader then Read(); Async execute = ExecuteReaderAsync then Read() (what the async terminals do by default); Async read = ReadAsync() for every row (the opt-in now named AsyncReads(); these runs used its rc.5 name EnableAsyncRead(true))',
  'Columns: reader_open_us = mean time from ExecuteReader until the reader closes; allocated_kb = bytes allocated per call / 1024')
"wrote 5 CSV files and 5 source files to $OutDir"
