<#
.SYNOPSIS
  Builds library-synthetic.csv and library-real.csv (and a .source.txt beside each) from the console logs of
  the library comparison run: the synthetic reader and the two real providers, ODX against Dapper,
  Dapper.AOT and a hand-written selector.

.DESCRIPTION
  Reads only the summary table that BenchmarkDotNet prints at the end of each log (the same table as its
  *-report-github.md). Nothing is read from anywhere else, so a re-run is a data swap.

  Each log is one BenchmarkDotNet run. The two real providers were run twice, "a" and "b", and both runs are kept.

  ./Extract-LibraryComparison.ps1 -SyntheticLog bench4d-syn.log `
    -RealLogs bench4d-sqlite-a.log,bench4d-sqlite-b.log,bench4d-sqlserver-a.log,bench4d-sqlserver-b.log `
    -Build b5b0014 -RunDate 2026-11-21 -OutDir docsite/perf-data
#>
param(
  [Parameter(Mandatory)][string]$SyntheticLog,
  [Parameter(Mandatory)][string[]]$RealLogs,
  [Parameter(Mandatory)][string]$Build,
  [Parameter(Mandatory)][string]$RunDate,
  [Parameter(Mandatory)][string]$OutDir
)
$ErrorActionPreference = 'Stop'
$inv = [Globalization.CultureInfo]::InvariantCulture
$OutDir = [IO.Path]::GetFullPath($OutDir)

function Quote([string]$s) { '"' + $s.Replace('"', '""') + '"' }
function Num([double]$v, [int]$d) { [math]::Round($v, $d).ToString('0.' + ('#' * $d), $inv) }
function To-Us([string]$text) {
  $m = [regex]::Match($text.Trim(), '^([\d.,]+)\s*(ns|us|ms|s)$')
  if (-not $m.Success) { throw "unreadable time: '$text'" }
  $v = [double]::Parse($m.Groups[1].Value.Replace(',', ''), $inv)
  switch ($m.Groups[2].Value) { 'ns' { $v / 1000 } 'us' { $v } 'ms' { $v * 1000 } 's' { $v * 1e6 } }
}
function To-Kb([string]$text) {
  $m = [regex]::Match($text.Trim(), '^([\d.,]+)\s*(B|KB|MB|GB)$')
  if (-not $m.Success) { throw "unreadable size: '$text'" }
  $v = [double]::Parse($m.Groups[1].Value.Replace(',', ''), $inv)
  switch ($m.Groups[2].Value) { 'B' { $v / 1024 } 'KB' { $v } 'MB' { $v * 1024 } 'GB' { $v * 1048576 } }
}

# The summary table of one log, as a list of objects keyed by column name.
function Read-Summary([string]$log) {
  $lines = [IO.File]::ReadAllLines($log)
  $s = [array]::IndexOf($lines, '// * Summary *')
  if ($s -lt 0) { throw "no summary in $log" }
  $table = @()
  for ($i = $s; $i -lt $lines.Count; $i++) {
    if ($lines[$i].StartsWith('|')) { $table += $lines[$i] } elseif ($table.Count) { break }
  }
  if ($table.Count -lt 3) { throw "no table in $log" }
  $cols = @($table[0].Trim('|').Split('|') | ForEach-Object { $_.Trim() })
  foreach ($row in $table[2..($table.Count - 1)]) {
    $cells = @($row.Trim('|').Split('|') | ForEach-Object { $_.Trim() })
    $o = [ordered]@{}
    for ($k = 0; $k -lt $cols.Count; $k++) { $o[$cols[$k]] = $cells[$k] }
    [pscustomobject]$o
  }
}

# One plain name and family for every benchmark method of the run.
$names = @{
  'V1'   = @('Map<T>()', 'odx-one', 'The cached, compiled typed plan')
  'V2'   = @('Hand-written selector', 'odx-one', 'A static Func<IDataRecord,T> with typed getters, passed to Map')
  'V7'   = @('Dapper Query<T>', 'dapper', 'Dapper 2.1.89, buffered Query<Row> into a list')
  'V7a'  = @('Dapper.AOT', 'dapper-aot', 'Dapper.AOT 1.1.0, the same Query<Row> through its intercepted path')
}
function Variant-Of([string]$method) {
  if ($method -match '^V7_V1_Sync$') { return 'V7' }
  if ($method -match '^V7a_V1_Sync$') { return 'V7a' }
  if ($method -match '^(V1|V2|V7|V7a)_Sync$') { return $Matches[1] }
  throw "unknown method $method"
}

$synLog = Split-Path -Leaf $SyntheticLog
$out = @('variant_id,variant,family,description,columns,rows,reader_open_us,total_us,error_us,allocated_kb,source_run')
foreach ($r in (Read-Summary $SyntheticLog)) {
  $id = Variant-Of $r.Method
  $n = $names[$id]
  $out += (@($id, (Quote $n[0]), $n[1], (Quote $n[2]), $r.Width, $r.Rows,
      (Num ([double]::Parse($r.'ReaderOpen (us)', $inv)) 3), (Num (To-Us $r.Mean) 3), (Num (To-Us $r.Error) 3),
      (Num (To-Kb $r.Allocated) 3), $synLog) -join ',')
}
[IO.File]::WriteAllText((Join-Path $OutDir 'library-synthetic.csv'), (($out -join "`n") + "`n"), (New-Object Text.UTF8Encoding($false)))

$provName = @{ 'Sqlite' = 'SQLite file'; 'SqlServer' = 'SQL Server LocalDB' }
$out = @('provider,run,variant_id,variant,family,description,columns,rows,total_us,error_us,allocated_kb,source_run')
foreach ($log in $RealLogs) {
  $leaf = Split-Path -Leaf $log
  $run = [regex]::Match($leaf, '-([ab])\.log$').Groups[1].Value
  if (-not $run) { throw "cannot tell the run letter from $leaf" }
  foreach ($r in (Read-Summary $log)) {
    $id = Variant-Of $r.Method
    $n = $names[$id]
    $out += (@((Quote $provName[$r.Provider]), $run, $id, (Quote $n[0]), $n[1], (Quote $n[2]), $r.Width, $r.Rows,
        (Num (To-Us $r.Mean) 3), (Num (To-Us $r.Error) 3), (Num (To-Kb $r.Allocated) 3), $leaf) -join ',')
  }
}
[IO.File]::WriteAllText((Join-Path $OutDir 'library-real.csv'), (($out -join "`n") + "`n"), (New-Object Text.UTF8Encoding($false)))

function Write-Source([string]$name, [string[]]$conditions, [string[]]$files) {
  $lines = @("Benchmark build: $Build", "Run date: $RunDate",
    'Machine: AMD Ryzen 9 7900 3.70GHz, 1 CPU, 24 logical and 12 physical cores, Windows 10 (10.0.19045.6466/22H2/2022Update)',
    'Runtime: .NET 11.0.0 (11.0.0, 11.0.26.53018), X64 RyuJIT x86-64-v4; .NET SDK 11.0.100; BenchmarkDotNet v0.15.8',
    'Toolchain=InProcessEmitToolchain  IterationCount=12  LaunchCount=1  WarmupCount=5') + $conditions +
  @('Source files (the BenchmarkDotNet console log of each run, its Summary table; the logs are not in the repository):') + @($files | ForEach-Object { "  $_" })
  [IO.File]::WriteAllText((Join-Path $OutDir "$name.source.txt"), (($lines -join "`n") + "`n"), (New-Object Text.UTF8Encoding($false)))
}
Write-Source 'library-synthetic' @(
  'Benchmark: SyntheticBenchmarks (the library over an in-memory ADO.NET reader, no I/O, no per-row cost): V1_Sync, V2_Sync, V7_V1_Sync (Dapper), V7a_V1_Sync (Dapper.AOT)',
  'Conditions: 20 typed columns, 1,000 rows per call, synchronous terminals, one run per case',
  'Columns: reader_open_us = mean time from ExecuteReader until the reader closes; total_us = mean time of the whole call (BenchmarkDotNet Mean); error_us = BenchmarkDotNet Error, the half-width of the 99.9% confidence interval of total_us; allocated_kb = bytes allocated per call / 1024') @($synLog)
Write-Source 'library-real' @(
  'Benchmark: RealProviderBenchmarks (the library against SQL Server LocalDB through Microsoft.Data.SqlClient, and against a SQLite file through Microsoft.Data.Sqlite): V1_Sync, V2_Sync, V7_Sync (Dapper), V7a_Sync (Dapper.AOT)',
  'Conditions: 10,000 rows per call, 20 typed columns, synchronous terminals; each provider was run twice, runs a and b, one after the other; the LocalDB database and the SQLite file are created for the run and removed after it',
  'SQLite: Dapper maps text to Guid through the usual SqlMapper.TypeHandler recipe; it does not convert it on its own',
  'Columns: total_us = mean time of the whole call (BenchmarkDotNet Mean); error_us = BenchmarkDotNet Error, the half-width of the 99.9% confidence interval of total_us; allocated_kb = allocated per call (BenchmarkDotNet, KB = 1024 bytes)') @($RealLogs | ForEach-Object { Split-Path -Leaf $_ })
"wrote library-synthetic.csv and library-real.csv to $OutDir"
