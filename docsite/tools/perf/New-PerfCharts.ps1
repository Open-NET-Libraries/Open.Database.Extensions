<#
.SYNOPSIS
  Generates the benchmark charts (SVG) and the data tables (markdown includes) from the CSV files in docsite/perf-data.

.DESCRIPTION
  One command regenerates everything:  pwsh -NoProfile -File docsite/tools/perf/New-PerfCharts.ps1

  Reads  docsite/perf-data/*.csv and *.source.txt
  Writes docsite/images/perf/*.svg and docsite/includes/perf/*.md (one table per section of a benchmark
  page; the pages pull them in with [!include])

  The output is deterministic (invariant culture, LF line endings, UTF-8 without a BOM), so a second run
  over the same CSV files gives byte-identical files. Each SVG follows the operating system's light or
  dark setting and draws its own background, so it stays readable when the site's theme switch and the
  operating system disagree.
#>
param(
  [string]$DataDir = (Join-Path $PSScriptRoot '../../perf-data'),
  [string]$ImageDir = (Join-Path $PSScriptRoot '../../images/perf'),
  [string]$IncludeDir = (Join-Path $PSScriptRoot '../../includes/perf')
)
$ErrorActionPreference = 'Stop'
$inv = [Globalization.CultureInfo]::InvariantCulture
$DataDir = [IO.Path]::GetFullPath($DataDir)
$ImageDir = [IO.Path]::GetFullPath($ImageDir)
$IncludeDir = [IO.Path]::GetFullPath($IncludeDir)
New-Item -ItemType Directory -Force -Path $ImageDir | Out-Null
New-Item -ItemType Directory -Force -Path $IncludeDir | Out-Null

$mu = [string][char]0xB5          # micro sign, for markdown
$times = [string][char]0xD7       # multiplication sign
$muX = $mu                   # micro sign, for SVG text (the file is UTF-8)
$timesX = $times

# ---------- helpers ----------
function N([double]$v) { [math]::Round($v, 2).ToString('0.##', $inv) }           # attribute numbers
function F([double]$v, [int]$d) { $v.ToString("N$d", $inv) }                      # display numbers, with separators
function X([string]$s) { [Security.SecurityElement]::Escape($s) }                  # XML escape
function M([string]$s) { $s.Replace('|', '\|').Replace('<', '&lt;').Replace('>', '&gt;') }  # markdown table cell
function Write-Lf([string]$path, [string]$text) {
  [IO.File]::WriteAllText($path, ($text.TrimEnd("`n") + "`n"), (New-Object Text.UTF8Encoding($false)))
}
function Read-Data([string]$name) { @(Import-Csv -LiteralPath (Join-Path $DataDir "$name.csv") -Encoding UTF8) }
function Read-Source([string]$name) {
  $lines = Get-Content -LiteralPath (Join-Path $DataDir "$name.source.txt")
  [pscustomobject]@{
    Build = ($lines | Where-Object { $_ -like 'Benchmark build:*' } | Select-Object -First 1).Substring(16).Trim()
    Date = ($lines | Where-Object { $_ -like 'Run date:*' } | Select-Object -First 1).Substring(9).Trim()
    Machine = $(if ($lines | Where-Object { $_ -like 'Machine:*' }) { (($lines | Where-Object { $_ -like 'Machine:*' } | Select-Object -First 1).Substring(8)).Trim() } else { $lines[[array]::IndexOf(@($lines | ForEach-Object { $_ -like 'BenchmarkDotNet v*' }), $true) + 1].Trim() })
    Runtime = $(if ($lines | Where-Object { $_ -like 'Runtime:*' }) { (($lines | Where-Object { $_ -like 'Runtime:*' } | Select-Object -First 1).Substring(8)).Trim() } else { ($lines | Where-Object { $_ -match '\[Host\]' } | Select-Object -First 1).Trim() })
  }
}
function Get-NiceStep([double]$max, [int]$parts) {
  $raw = $max / $parts
  $mag = [math]::Pow(10, [math]::Floor([math]::Log10($raw)))
  foreach ($m in 1, 2, 2.5, 5, 10) { if ($raw -le $m * $mag + 1e-12) { return $m * $mag } }
  return 10 * $mag
}
function Format-Micro([double]$us) { if ($us -ge 1000) { (F ($us / 1000) 1) + ' ms' } else { (F $us 1) + " $mu" + 's' } }
function Format-MicroX([double]$us) { if ($us -ge 1000) { (F ($us / 1000) 1) + ' ms' } else { (F $us 1) + " ${muX}s" } }
function Format-Pct([double]$v, [double]$best) { if ($v -eq $best) { 'best' } else { '+' + (F (($v / $best - 1) * 100) 1) + '%' } }
function Get-Words([string]$text, [int]$max) {
  $lines = @(); $cur = ''
  foreach ($w in $text.Split(' ')) {
    if ($cur -and ($cur.Length + 1 + $w.Length) -gt $max) { $lines += $cur; $cur = $w } else { $cur = if ($cur) { "$cur $w" } else { $w } }
  }
  if ($cur) { $lines += $cur }
  return $lines
}

# Family key -> CSS class, legend text, legend order.
$famClass = @{ 'arr' = 'one'; 'alt' = 'grey'; 'p-none' = 'grey'; 'p-inline' = 'one'; 'p-bin' = 'select'; 'loop' = 'grey'; 'odx' = 'one'; 'rd-sync' = 'one'; 'rd-async' = 'select'; 'raw' = 'grey'; 'odx-one' = 'one'; 'odx-select' = 'select'; 'odx-raw' = 'grey'; 'reference' = 'grey'; 'dapper' = 'dapper'; 'dapper-aot' = 'select' }
$famName = @{ 'arr' = 'Arrays from ArrayPool (the lease per chunk is what ODX uses)'; 'alt' = 'The alternatives measured'; 'p-none' = 'No clear'; 'p-inline' = 'Inline clear'; 'p-bin' = 'Post to the recycle bin'; 'loop' = 'Plain loop (hand-written)'; 'odx' = 'ODX'; 'rd-sync' = 'ODX, sync read (the default)'; 'rd-async' = 'ODX with AsyncReads() (opt-in)'; 'raw' = 'Raw SqlClient, no ODX'; 'odx-one' = 'ODX: one object per row'; 'odx-select' = 'ODX: shaped to a domain object'; 'odx-raw' = 'ODX: raw object[] rows'; 'reference' = 'Hand loop on the provider reader (reference)'; 'dapper' = 'Dapper 2.1.89'; 'dapper-aot' = 'Dapper.AOT 1.1.0' }
$famOrder = 'arr', 'alt', 'p-none', 'p-inline', 'p-bin', 'loop', 'odx', 'rd-sync', 'rd-async', 'raw', 'odx-one', 'odx-select', 'odx-raw', 'reference', 'dapper', 'dapper-aot'

$css = @'
    :root {
      --ink: #1f2328;
      --muted: #4b5563;
      --bg: #ffffff;
      --frame: #d0d7de;
      --grid: #e6eaef;
      --axis: #8c959f;
      --one-line: #2a78d6;
      --select-line: #1baf7a;
      --dapper-line: #eb6834;
      --grey-line: #8b929c;
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --ink: #e6edf3;
        --muted: #b1bac4;
        --bg: #0d1117;
        --frame: #30363d;
        --grid: #21262d;
        --axis: #8b949e;
        --one-line: #3987e5;
        --select-line: #199e70;
        --dapper-line: #d95926;
        --grey-line: #7a828c;
      }
    }
    text { font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; fill: var(--ink); font-size: 13px; }
    .title { font-size: 16px; font-weight: 600; }
    .head { font-weight: 600; }
    .muted { fill: var(--muted); }
    .small { font-size: 11.5px; }
    .val { font-size: 12.5px; }
    .big { font-size: 30px; font-weight: 600; }
    .name { text-anchor: end; }
    .mid { text-anchor: middle; }
    .frame { fill: var(--bg); stroke: var(--frame); stroke-width: 1; }
    .grid { stroke: var(--grid); stroke-width: 1; }
    .axis { stroke: var(--axis); stroke-width: 1.5; }
    .hit { fill: transparent; }
    .tile { fill: none; stroke: var(--frame); stroke-width: 1; }
    .one { fill: var(--one-line); }
    .select { fill: var(--select-line); }
    .dapper { fill: var(--dapper-line); }
    .grey { fill: var(--grey-line); }
    .l-one { fill: none; stroke: var(--one-line); stroke-width: 2.5; stroke-linejoin: round; }
    .l-select { fill: none; stroke: var(--select-line); stroke-width: 2.5; stroke-linejoin: round; }
    .l-dapper { fill: none; stroke: var(--dapper-line); stroke-width: 2.5; stroke-linejoin: round; }
    .l-grey { fill: none; stroke: var(--grey-line); stroke-width: 2.5; stroke-linejoin: round; }
    .dash { stroke-dasharray: 7 4; }
    .m-one { fill: var(--one-line); stroke: var(--bg); stroke-width: 2; }
    .m-select { fill: var(--select-line); stroke: var(--bg); stroke-width: 2; }
    .m-dapper { fill: var(--dapper-line); stroke: var(--bg); stroke-width: 2; }
    .m-grey { fill: var(--grey-line); stroke: var(--bg); stroke-width: 2; }
'@

function New-Svg([string]$id, [string]$title, [string]$desc, [int]$w, [int]$h, [string]$body) {
  @"
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 $w $h" role="img" aria-labelledby="$id-title $id-desc">
  <title id="$id-title">$(X $title)</title>
  <desc id="$id-desc">$(X $desc)</desc>
  <style>
$css
  </style>
  <rect class="frame" x="0.5" y="0.5" width="$($w - 1)" height="$($h - 1)" rx="8"/>
$body</svg>
"@
}

# A legend: swatches and names, wrapped to the width; returns the SVG and the number of rows.
function New-Legend([string[]]$fams, [int]$y, [int]$w) {
  $sb = New-Object Text.StringBuilder
  $x = 16; $row = 0
  foreach ($f in $famOrder | Where-Object { $fams -contains $_ }) {
    $label = $famName[$f]
    $itemW = 18 + [math]::Ceiling($label.Length * 6.6) + 14
    if ($x + $itemW -gt $w - 12 -and $x -gt 16) { $x = 16; $row++ }
    $yy = $y + $row * 20
    [void]$sb.AppendLine("  <rect class=""$($famClass[$f])"" x=""$x"" y=""$($yy - 10)"" width=""12"" height=""12"" rx=""3""/>")
    [void]$sb.AppendLine("  <text class=""small"" x=""$($x + 18)"" y=""$yy"">$(X $label)</text>")
    $x += $itemW
  }
  [pscustomobject]@{ Svg = $sb.ToString(); Rows = $row + 1 }
}

# Ranked horizontal bars. Panels: @{ Title; Rows = @(@{ Name; Fam; Value; Detail }) }, rows already in display order.
# Scale: 'shared' (one axis for all panels), 'panel' (each panel its own, no ticks).
function New-BarSvg([string]$id, [string]$title, [string]$subtitle, $panels, [scriptblock]$format, [string]$axisCaption,
  [string]$footer, [string]$scale, [int]$left, [string]$descIntro, [int]$W = 560) {
  $rowH = 26; $plotW = $W - $left - 72
  $allMax = ($panels | ForEach-Object { $_.Rows } | ForEach-Object { $_.Value } | Measure-Object -Maximum).Maximum
  $fams = @($panels | ForEach-Object { $_.Rows } | ForEach-Object { $_.Fam } | Where-Object { $_ } | Select-Object -Unique)
  $legend = if ($fams.Count -gt 1 -or $fams -contains 'reference') { New-Legend $fams 72 $W } else { $null }
  $y = 66; if ($legend) { $y = 72 + ($legend.Rows - 1) * 20 + 22 }
  $sb = New-Object Text.StringBuilder
  [void]$sb.AppendLine("  <text class=""title"" x=""16"" y=""28"">$(X $title)</text>")
  [void]$sb.AppendLine("  <text class=""muted"" x=""16"" y=""48"">$(X $subtitle)</text>")
  if ($legend) { [void]$sb.Append($legend.Svg) }
  $descParts = @()
  foreach ($p in $panels) {
    $fmtP = if ($p.Format) { $p.Format } else { $format }
    $max = if ($scale -eq 'shared') { $allMax } else { ($p.Rows | ForEach-Object { $_.Value } | Measure-Object -Maximum).Maximum }
    $ticks = $scale -eq 'shared'
    $step = Get-NiceStep $max 5
    $axisMax = if ($ticks) { [math]::Ceiling($max / $step - 1e-9) * $step } else { $max * 1.0 }
    if ($p.Title) { [void]$sb.AppendLine("  <text class=""head"" x=""16"" y=""$($y + 14)"">$(X $p.Title)</text>"); $y += 24 }
    $top = $y; $n = @($p.Rows).Count; $bottom = $top + $n * $rowH
    if ($ticks) {
      for ($t = 0.0; $t -le $axisMax + 1e-9; $t += $step) {
        $gx = $left + $t / $axisMax * $plotW
        [void]$sb.AppendLine("  <line class=""grid"" x1=""$(N $gx)"" y1=""$top"" x2=""$(N $gx)"" y2=""$bottom""/>")
        $dec = if ([math]::Abs($step - [math]::Round($step)) -gt 1e-9) { 1 } else { 0 }
        [void]$sb.AppendLine("  <text class=""muted small mid"" x=""$(N $gx)"" y=""$($bottom + 16)"">$(F $t $dec)</text>")
      }
    }
    $i = 0
    foreach ($r in $p.Rows) {
      $ry = $top + $i * $rowH
      $bw = [math]::Max(3.0, $r.Value / $axisMax * $plotW)
      [void]$sb.AppendLine("  <g>")
      [void]$sb.AppendLine("    <title>$(X $r.Detail)</title>")
      [void]$sb.AppendLine("    <rect class=""hit"" x=""0"" y=""$ry"" width=""$W"" height=""$rowH""/>")
      [void]$sb.AppendLine("    <text class=""name"" x=""$($left - 10)"" y=""$($ry + 17)"">$(X $r.Name)</text>")
      $rr = [math]::Min(4.0, $bw / 2); $y0 = $ry + 5
      [void]$sb.AppendLine("    <path class=""$($famClass[$r.Fam])"" d=""M$(N $left),$y0 h$(N ($bw - $rr)) a$(N $rr),$(N $rr) 0 0 1 $(N $rr),$(N $rr) v$(N (16 - 2 * $rr)) a$(N $rr),$(N $rr) 0 0 1 -$(N $rr),$(N $rr) h-$(N ($bw - $rr)) z""/>")
      [void]$sb.AppendLine("    <text class=""val"" x=""$(N ($left + $bw + 6))"" y=""$($ry + 17)"">$(X (& $fmtP $r.Value))</text>")
      [void]$sb.AppendLine("  </g>")
      $i++
    }
    [void]$sb.AppendLine("  <line class=""axis"" x1=""$left"" y1=""$top"" x2=""$left"" y2=""$bottom""/>")
    $prefix = if ($p.Title) { "$($p.Title): " } else { '' }
    $descParts += ($prefix + (($p.Rows | ForEach-Object { "$($_.Name) $(& $fmtP $_.Value)" }) -join "; "))
    $y = $bottom + $(if ($ticks) { 24 } else { 0 }) + 14
  }
  if ($scale -eq 'shared' -and $axisCaption) {
    [void]$sb.AppendLine("  <text class=""muted small mid"" x=""$(N ($left + $plotW / 2))"" y=""$($y - 6)"">$(X $axisCaption)</text>"); $y += 14
  }
  foreach ($fl in $footer.Split('|')) { [void]$sb.AppendLine("  <text class=""muted small"" x=""16"" y=""$($y + 8)"">$(X $fl)</text>"); $y += 16 }
  $H = $y + 14
  $desc = "$descIntro Every bar starts at zero. " + ($descParts -join '. ') + '.'
  New-Svg $id $title $desc $W $H $sb.ToString()
}

# ---------- data and sources ----------
$syn = Read-Data 'synthetic-overhead'; $synSrc = Read-Source 'synthetic-overhead'
$swp = Read-Data 'width-sweep';        $swpSrc = Read-Source 'width-sweep'
$map = Read-Data 'map-cost';           $mapSrc = Read-Source 'map-cost'
$rp = Read-Data 'real-providers';      $rpSrc = Read-Source 'real-providers'
$sa = Read-Data 'sync-async';          $saSrc = Read-Source 'sync-async'
function Footer($src, [string]$csv) { "Build $($src.Build), $($src.Date), one run per case. Numbers: $csv.csv" }
function Pick($rows, [string]$id) { $rows | Where-Object { $_.variant_id -eq $id } | Select-Object -First 1 }

# One plain name for the compiled mapping everywhere, whatever the source CSV calls it.
foreach ($set in $syn, $swp, $rp) { foreach ($r in $set) { if ($r.variant_id -eq 'V1') { $r.variant = 'Map<T>()' } } }

# ---------- 1. the summary chart ----------
# Four ways to read the same 20 columns and 1,000 rows from the synthetic reader (the library comparison run). Two panels, each on its own scale.
$ls = Read-Data 'library-synthetic'; $lsSrc = Read-Source 'library-synthetic'
$lr = Read-Data 'library-real';      $lrSrc = Read-Source 'library-real'
$v1 = Pick $ls 'V1'
$sumNames = @{ 'V2' = 'Hand-written selector'; 'V1' = 'Map<T>()'; 'V7a' = 'Dapper.AOT'; 'V7' = 'Dapper Query<T>' }
function Get-SumPanel([string]$title, [string]$col, [scriptblock]$fmt) {
  $rows = @('V2', 'V1', 'V7a', 'V7' | ForEach-Object { Pick $ls $_ } | Sort-Object { [double]$_.$col } -Stable | ForEach-Object {
    [pscustomobject]@{ Name = $sumNames[$_.variant_id]; Fam = $_.family; Value = [double]$_.$col
      Detail = "$($sumNames[$_.variant_id]): connection held $(Format-Micro ([double]$_.reader_open_us)), $(F ([double]$_.allocated_kb) 1) KB allocated per call." } })
  @{ Title = $title; Rows = $rows; Format = $fmt }
}
$sumPanels = @(
  (Get-SumPanel 'Connection held open (shorter is better)' 'reader_open_us' { param($v) Format-MicroX $v }),
  (Get-SumPanel 'Memory allocated (smaller is better)' 'allocated_kb' { param($v) (F $v 1) + ' KB' }))
Write-Lf (Join-Path $ImageDir 'perf-summary.svg') (New-BarSvg 'summary' 'Map<T>() against Dapper and hand-written code' 'One call reads 20 columns and 1,000 rows from a synthetic reader.' $sumPanels { param($v) "$v" } '' (Footer $lsSrc 'library-synthetic') 'panel' 150 'Two panels of four bars each, each panel on its own scale: how long the connection is held open, and how much memory is allocated, for four ways of reading 20 columns and 1,000 rows from a synthetic in-memory reader.' 520)

# ---------- 2. synthetic bars: reader-open, total, memory ----------
function Get-SynRows([string]$col, [scriptblock]$fmt) {
  $rows = @($syn | Sort-Object { [double]$_.$col } -Stable | ForEach-Object {
    [pscustomobject]@{ Name = $_.variant; Fam = $_.family; Value = [double]$_.$col
      Detail = "$($_.variant) ($($_.variant_id)): $($_.description). Reader open $(Format-Micro ([double]$_.reader_open_us)), total $(Format-Micro ([double]$_.total_us)), $(F ([double]$_.allocated_kb) 1) KB allocated." } })
  ,$rows
}
$synIntro = 'Ranked horizontal bars, shortest first, one per way of reading 20 columns and 1,000 rows from a synthetic in-memory reader, colored by family.'
Write-Lf (Join-Path $ImageDir 'perf-synthetic-open.svg') (New-BarSvg 'syn-open' 'Connection held open per call' 'Shorter is better: a shorter bar holds the connection for less time.' @(@{ Title = ''; Rows = (Get-SynRows 'reader_open_us') }) { param($v) Format-MicroX $v } ("microseconds from ExecuteReader until the reader closes") (Footer $synSrc 'synthetic-overhead') 'shared' 232 $synIntro)
Write-Lf (Join-Path $ImageDir 'perf-synthetic-total.svg') (New-BarSvg 'syn-total' 'Total time per call' 'Shorter is better. Where it is far above reader-open time, work happens after the reader closes.' @(@{ Title = ''; Rows = (Get-SynRows 'total_us') }) { param($v) Format-MicroX $v } 'microseconds per call, the whole call' (Footer $synSrc 'synthetic-overhead') 'shared' 232 $synIntro)
Write-Lf (Join-Path $ImageDir 'perf-synthetic-memory.svg') (New-BarSvg 'syn-memory' 'Memory allocated per call' 'Smaller is better.' @(@{ Title = ''; Rows = (Get-SynRows 'allocated_kb') }) { param($v) (F $v 1) + ' KB' } 'kilobytes allocated per call' (Footer $synSrc 'synthetic-overhead') 'shared' 232 $synIntro)

# ---------- 3. cost grows with columns: a line chart ----------
$short = @{ 'V2' = 'Hand-written selector'; 'V1' = 'Map<T>()'; 'V3' = 'Factory map'; 'V0' = 'object[] rows'; 'V7_V1' = 'Dapper' }
$ids = @($swp | ForEach-Object { $_.variant_id } | Select-Object -Unique)
$widths = @($swp | ForEach-Object { [int]$_.columns } | Select-Object -Unique | Sort-Object)
$maxUs = ($swp | ForEach-Object { [double]$_.total_us } | Measure-Object -Maximum).Maximum
$step = Get-NiceStep $maxUs 5; $axisMax = [math]::Ceiling($maxUs / $step - 1e-9) * $step
$W = 560; $H = 380; $left = 54; $right = 150; $top = 70; $bot = 58
$plotW = $W - $left - $right; $plotH = $H - $top - $bot
$lx = { param($c) $left + ($c - $widths[0]) / ($widths[-1] - $widths[0]) * $plotW }
$ly = { param($v) $top + (1 - $v / $axisMax) * $plotH }
$sb = New-Object Text.StringBuilder
[void]$sb.AppendLine('  <text class="title" x="16" y="28">Cost grows with the number of columns</text>')
[void]$sb.AppendLine('  <text class="muted" x="16" y="48">Total time per call at 1,000 rows. Lower is better; the slope is the cost per cell.</text>')
for ($t = 0.0; $t -le $axisMax + 1e-9; $t += $step) {
  $gy = & $ly $t
  [void]$sb.AppendLine("  <line class=""grid"" x1=""$left"" y1=""$(N $gy)"" x2=""$($left + $plotW)"" y2=""$(N $gy)""/>")
  [void]$sb.AppendLine("  <text class=""muted small"" x=""$($left - 8)"" y=""$(N ($gy + 4))"" text-anchor=""end"">$(F $t 0)</text>")
}
[void]$sb.AppendLine("  <text class=""muted small"" x=""16"" y=""$($top - 10)"">$($muX)s per call</text>")
foreach ($c in $widths) {
  $gx = & $lx $c
  [void]$sb.AppendLine("  <text class=""muted small mid"" x=""$(N $gx)"" y=""$($top + $plotH + 18)"">$c</text>")
}
[void]$sb.AppendLine("  <line class=""axis"" x1=""$left"" y1=""$($top + $plotH)"" x2=""$($left + $plotW)"" y2=""$($top + $plotH)""/>")
[void]$sb.AppendLine("  <text class=""muted small mid"" x=""$(N ($left + $plotW / 2))"" y=""$($top + $plotH + 36)"">columns</text>")
$ends = @(); $descParts = @()
foreach ($id in $ids) {
  $pts = @($swp | Where-Object { $_.variant_id -eq $id } | Sort-Object { [int]$_.columns })
  $cls = $famClass[$pts[0].family]
  $dash = if ($id -eq 'V2') { ' dash' } else { '' }
  $poly = ($pts | ForEach-Object { "$(N (& $lx ([int]$_.columns))),$(N (& $ly ([double]$_.total_us)))" }) -join ' '
  [void]$sb.AppendLine("  <polyline class=""l-$cls$dash"" points=""$poly""/>")
  foreach ($p in $pts) {
    [void]$sb.AppendLine("  <circle class=""m-$cls"" cx=""$(N (& $lx ([int]$p.columns)))"" cy=""$(N (& $ly ([double]$p.total_us)))"" r=""4.5""><title>$(X "$($short[$id]), $($p.columns) columns: $(Format-Micro ([double]$p.total_us)) per call")</title></circle>")
  }
  $last = $pts[-1]
  $ends += [pscustomobject]@{ Name = $short[$id]; Y = (& $ly ([double]$last.total_us)) }
  $descParts += "$($short[$id]): " + (($pts | ForEach-Object { "$($_.columns) columns $(Format-Micro ([double]$_.total_us))" }) -join ', ')
}
$ends = @($ends | Sort-Object Y -Stable)
for ($i = 1; $i -lt $ends.Count; $i++) { if ($ends[$i].Y - $ends[$i - 1].Y -lt 17) { $ends[$i].Y = $ends[$i - 1].Y + 17 } }
foreach ($e in $ends) { [void]$sb.AppendLine("  <text x=""$($left + $plotW + 10)"" y=""$(N ($e.Y + 4))"">$(X $e.Name)</text>") }
[void]$sb.AppendLine("  <text class=""muted small"" x=""16"" y=""$($H - 12)"">$(X (Footer $swpSrc 'width-sweep'))</text>")
$descW = 'A line chart of total time per call, in microseconds, against 1 to 20 columns, 1,000 rows, five approaches with labels at the line ends; the axis starts at zero. ' + ($descParts -join '. ') + '.'
Write-Lf (Join-Path $ImageDir 'perf-width.svg') (New-Svg 'width' 'Cost grows with the number of columns' $descW $W $H $sb.ToString())

# ---------- 4. a reused map against a fresh one ----------
$mapRows = @($map | Sort-Object { [int]$_.columns } | ForEach-Object {
  $cost = ([double]$_.built_every_call_total_us - [double]$_.reused_total_us) / 1000
  $kb = [double]$_.built_every_call_allocated_kb - [double]$_.reused_allocated_kb
  [pscustomobject]@{ Name = $(if ($_.columns -eq '1') { '1 column' } else { "$($_.columns) columns" }); Fam = 'odx-one'; Value = $cost
    Detail = "$($_.columns) columns: a map built on every call took $(F ([double]$_.built_every_call_total_us / 1000) 2) ms and allocated $(F ([double]$_.built_every_call_allocated_kb) 1) KB; a reused map took $(F ([double]$_.reused_total_us) 2) $muX`s and allocated $(F ([double]$_.reused_allocated_kb) 2) KB. The difference, $(F $cost 2) ms and $(F $kb 1) KB, is the one-time cost." } })
$reusedMin = ($map | ForEach-Object { [double]$_.reused_total_us } | Measure-Object -Minimum).Minimum
$reusedMax = ($map | ForEach-Object { [double]$_.reused_total_us } | Measure-Object -Maximum).Maximum
Write-Lf (Join-Path $ImageDir 'perf-map-cost.svg') (New-BarSvg 'map-cost' 'What building a map costs, once' "Shorter is better. A reused map costs $(F $reusedMin 2) to $(F $reusedMax 2) $($muX)s for a one-row call." @(@{ Title = ''; Rows = $mapRows }) { param($v) (F $v 2) + ' ms' } 'milliseconds to build and compile one map (the extra time of a call that builds it)' (Footer $mapSrc 'map-cost') 'shared' 100 'Ranked horizontal bars of the one-time cost of building and compiling a map, for 1, 5 and 20 columns; one row per call.')

# ---------- 5. real providers ----------
$provOrder = 'SQL Server LocalDB', 'SQLite file'
function Get-RpPanels([string]$col, [double]$div) {
  $panels = @()
  foreach ($prov in ($provOrder | Where-Object { $rp.provider -contains $_ })) {
    $rows = @($rp | Where-Object { $_.provider -eq $prov -and $_.columns -eq '20' } | Sort-Object { [double]$_.$col } -Stable | ForEach-Object {
      [pscustomobject]@{ Name = $_.variant; Fam = $_.family; Value = [double]$_.$col / $div
        Detail = "$($_.variant): $($_.description). $prov, 20 columns, 10,000 rows: $(Format-Micro ([double]$_.total_us)) per call, $(F ([double]$_.allocated_kb / 1024) 2) MB allocated." } })
    $panels += @{ Title = $prov; Rows = $rows }
  }
  ,$panels
}
$rpIntro = 'Two panels on one scale, SQL Server LocalDB and a SQLite file, 20 columns and 10,000 rows; bars are ranked best first inside each panel.'
Write-Lf (Join-Path $ImageDir 'perf-real-time.svg') (New-BarSvg 'real-time' 'Real databases: time per call' 'Shorter is better. 20 columns, 10,000 rows, one scale for both panels.' (Get-RpPanels 'total_us' 1000) { param($v) (F $v 1) + ' ms' } 'milliseconds per call' (Footer $rpSrc 'real-providers') 'shared' 196 $rpIntro)
Write-Lf (Join-Path $ImageDir 'perf-real-memory.svg') (New-BarSvg 'real-memory' 'Real databases: memory per call' 'Smaller is better. 20 columns, 10,000 rows, one scale for both panels.' (Get-RpPanels 'allocated_kb' 1024) { param($v) (F $v 2) + ' MB' } 'megabytes allocated per call' (Footer $rpSrc 'real-providers') 'shared' 196 $rpIntro)

# ---------- 5b. the library comparison on real databases (two runs of each provider) ----------
$lrProv = 'SQL Server LocalDB', 'SQLite file'
function Get-LrPanels([string]$col, [double]$div, [scriptblock]$detailFmt) {
  $panels = @()
  foreach ($prov in $lrProv) {
    $rows = @('V2', 'V1', 'V7a', 'V7' | ForEach-Object {
      $id = $_
      $g = @($lr | Where-Object { $_.provider -eq $prov -and $_.variant_id -eq $id } | Sort-Object run)
      $mean = ($g | ForEach-Object { [double]$_.$col } | Measure-Object -Average).Average
      [pscustomobject]@{ Name = $sumNames[$id]; Fam = $g[0].family; Value = $mean / $div
        Detail = "$($sumNames[$id]), $prov, 20 columns, 10,000 rows: " + (($g | ForEach-Object { "run $($_.run) " + (& $detailFmt ([double]$_.$col)) }) -join ', ') + '.' } } | Sort-Object { $_.Value } -Stable)
    $panels += @{ Title = $prov; Rows = $rows }
  }
  ,$panels
}
$lrIntro = 'Two panels on one scale, SQL Server LocalDB and a SQLite file, 20 columns and 10,000 rows; each bar is the mean of two runs, ranked best first inside each panel.'
$lrFooter = "Build $($lrSrc.Build), $($lrSrc.Date), mean of 2 runs. Numbers: library-real.csv"
Write-Lf (Join-Path $ImageDir 'perf-lib-real-time.svg') (New-BarSvg 'lib-real-time' 'Real databases: time per call' 'Shorter is better. Mean of two runs, 20 columns, 10,000 rows, one scale for both panels.' (Get-LrPanels 'total_us' 1000 { param($v) Format-Micro $v }) { param($v) (F $v 1) + ' ms' } 'milliseconds per call' $lrFooter 'shared' 196 $lrIntro)
Write-Lf (Join-Path $ImageDir 'perf-lib-real-memory.svg') (New-BarSvg 'lib-real-memory' 'Real databases: memory per call' 'Smaller is better. 20 columns, 10,000 rows, one scale for both panels.' (Get-LrPanels 'allocated_kb' 1024 { param($v) (F ($v / 1024) 2) + ' MB' }) { param($v) (F $v 2) + ' MB' } 'megabytes allocated per call' $lrFooter 'shared' 196 $lrIntro)

# ---------- 6. sync against async ----------
$saPanels = @()
foreach ($cond in @($sa | ForEach-Object { $_.condition } | Select-Object -Unique)) {
  $rows = @($sa | Where-Object { $_.condition -eq $cond } | ForEach-Object {
    $syncRow = $sa | Where-Object { $_.condition -eq $cond -and $_.path -eq 'Sync' } | Select-Object -First 1
    [pscustomobject]@{ Name = $_.path; Fam = 'odx-one'; Value = [double]$_.reader_open_us
      Detail = "$cond, $($_.path): reader open $(Format-Micro ([double]$_.reader_open_us)), $(F ([double]$_.reader_open_us / [double]$syncRow.reader_open_us) 2)$timesX the sync read; $(F ([double]$_.allocated_kb) 1) KB allocated." } })
  $saPanels += @{ Title = $cond; Rows = $rows }
}
Write-Lf (Join-Path $ImageDir 'perf-sync-async.svg') (New-BarSvg 'sync-async' 'Sync reads against async reads' 'Connection held open, Map<T>(), 20 columns, 1,000 rows. Each panel has its own scale.' $saPanels { param($v) Format-MicroX $v } '' (Footer $saSrc 'sync-async') 'panel' 110 'Four panels of three bars each, one panel per condition, each on its own scale: sync, async execute, and async read.')

# ---------- 7. slow link and large values (the hand-recorded probe runs) ----------
$lt = Read-Data 'link-time';   $ltSrc = Read-Source 'link-time'
$lm = Read-Data 'link-memory'; $lmSrc = Read-Source 'link-memory'
$lv = Read-Data 'large-value'; $lvSrc = Read-Source 'large-value'
function Link-Footer($src, [string]$csv) { "Run $($src.Date), mean of 3 operations. Numbers: $csv.csv" }

$ltPanels = @()
foreach ($sc in @($lt | ForEach-Object { $_.scenario } | Select-Object -Unique)) {
  $rows = @($lt | Where-Object { $_.scenario -eq $sc } | ForEach-Object {
    [pscustomobject]@{ Name = $(if ($_.family -eq 'raw') { 'Raw SqlClient, no ODX' } else { 'ODX Map<T>(), sync read' }); Fam = $_.family; Value = [double]$_.wall_ms
      Detail = "$($_.variant): $(F ([double]$_.wall_ms) 1) ms per operation, $($_.scenario_description)." } })
  $ltPanels += @{ Title = "$sc, $(($lt | Where-Object { $_.scenario -eq $sc } | Select-Object -First 1).scenario_description)"; Rows = $rows }
}
Write-Lf (Join-Path $ImageDir 'perf-link-time.svg') (New-BarSvg 'link-time' 'Time over a slow link' 'Shorter is better. 5 ms delay, 100 Mbit, SQL Server, 10,000 rows.' $ltPanels { param($v) (F $v 1) + ' ms' } '' (Link-Footer $ltSrc 'link-time') 'panel' 170 'Two panels of two bars each, each panel on its own scale: the time of one operation over a simulated 5 ms, 100 Mbit link, for the ODX sync read and for raw SqlClient with ReadAsync.' 520)

$lmPanels = @()
foreach ($sh in @($lm | ForEach-Object { $_.shape } | Select-Object -Unique)) {
  $rows = @($lm | Where-Object { $_.shape -eq $sh } | ForEach-Object {
    [pscustomobject]@{ Name = $_.read_path; Fam = $_.family; Value = [double]$_.allocated_kb
      Detail = "$sh, $($_.read_path): $(F ([double]$_.allocated_kb) 0) KB allocated per operation, $(F ([double]$_.wall_ms) 1) ms, $($_.incomplete_readasync_calls) of 10,001 ReadAsync calls incomplete." } })
  $lmPanels += @{ Title = $sh; Rows = $rows }
}
Write-Lf (Join-Path $ImageDir 'perf-link-memory.svg') (New-BarSvg 'link-memory' 'Memory with the sync read and with AsyncReads()' 'Smaller is better. SQL Server, 5 ms, 100 Mbit link, 10,000 rows, 20 columns.' $lmPanels { param($v) (F $v 0) + ' KB' } 'kilobytes allocated per operation' (Link-Footer $lmSrc 'link-memory') 'shared' 170 'Two panels on one scale, memory allocated per operation over a simulated slow link, for the sync read and for AsyncReads().' 520)

$lvRows = @($lv | Where-Object { $_.seconds } | ForEach-Object {
  [pscustomobject]@{ Name = ($_.variant -replace 'Raw SqlClient, ', 'Raw, ' -replace 'both SqlClient switches off', 'switches off'); Fam = $_.family; Value = [double]$_.seconds
    Detail = "$($_.variant): $(F ([double]$_.seconds) 2) s for one 100 MB value$(if ($_.peak_working_set_mb) { ", peak working set $($_.peak_working_set_mb) MB" })." } })
$lvDesc = 'Horizontal bars in seconds for one 100 MB value over a simulated 5 ms, 100 Mbit link, SQL Server. Every bar starts at zero. ' + (($lvRows | ForEach-Object { "$($_.Name) $(F $_.Value 2)" }) -join '; ') + '. ODX Map(Func) with AsyncReads() was not measured.'
Write-Lf (Join-Path $ImageDir 'perf-large-value.svg') (New-BarSvg 'large-value' 'One 100 MB value over a slow link' 'Shorter is better. SQL Server, 5 ms delay, 100 Mbit link.' @(@{ Title = ''; Rows = $lvRows }) { param($v) (F $v 2) + ' s' } 'seconds for one operation' ("ODX Map(Func) with AsyncReads() was not measured.|" + (Link-Footer $lvSrc 'large-value')) 'shared' 262 $lvDesc 560)

# ---------- 8. channels and dataflow (hand-copied from Benchmarks/RESULTS.md) ----------
$ch = Read-Data 'channels'; $chSrc = Read-Source 'channels'
$chPanels = @()
foreach ($feat in @($ch | ForEach-Object { $_.feature } | Select-Object -Unique)) {
  $r = $ch | Where-Object { $_.feature -eq $feat -and $_.rows -eq '1000' } | Select-Object -First 1
  $short = $feat.Split(' ')[0]
  $rows = @(
    [pscustomobject]@{ Name = 'Plain loop'; Fam = 'loop'; Value = [double]$r.loop_us; Detail = "A hand-written loop over 1,000 rows: $(F ([double]$r.loop_us) 0) $muX`s." },
    [pscustomobject]@{ Name = $short; Fam = 'odx'; Value = [double]$r.odx_us; Detail = "$feat over 1,000 rows: $(F ([double]$r.odx_us) 0) $muX`s, $($r.time_ratio)$timesX the loop." })
  $chPanels += @{ Title = $feat; Rows = $rows }
}
Write-Lf (Join-Path $ImageDir 'perf-channels.svg') (New-BarSvg 'channels' 'Draining 1,000 rows: a plain loop against ODX' 'Shorter is better. In-memory provider.' $chPanels { param($v) Format-MicroX $v } '' "Older run. Numbers: channels.csv" 'panel' 100 'Two panels of two bars each, each panel on its own scale: the time to drain 1,000 rows with a hand-written loop and with ODX, for a channel and for a BufferBlock.' 520)

# ---------- 9. pooling and clearing (the pool spike and the end-to-end pairs) ----------
$ps = Read-Data 'pool-store';      $psSrc = Read-Source 'pool-store'
$pr = Read-Data 'pool-rent';       $prSrc = Read-Source 'pool-rent'
$pb = Read-Data 'pool-bytes';      $pbSrc = Read-Source 'pool-bytes'
$prp = Read-Data 'pool-release-path';  $prpSrc = Read-Source 'pool-release-path'
$pcs = Read-Data 'pool-clear-speed';   $pcsSrc = Read-Source 'pool-clear-speed'
$ppe = Read-Data 'pool-bin-proof-e2e'; $ppeSrc = Read-Source 'pool-bin-proof-e2e'
$pcpu = Read-Data 'pool-bin-cpu';      $pcpuSrc = Read-Source 'pool-bin-cpu'
function Med([double[]]$v) { $s = @($v | Sort-Object); if ($s.Count % 2) { $s[[int](($s.Count - 1) / 2)] } else { ($s[$s.Count / 2 - 1] + $s[$s.Count / 2]) / 2 } }
$pe = Read-Data 'pool-e2e';        $peSrc = Read-Source 'pool-e2e'
$pbc = Read-Data 'pool-bin-cost';  $pbcSrc = Read-Source 'pool-bin-cost'
$pbcostSrc = Read-Source 'pooled-buffer-cost'
function Spike-Footer($src, [string]$csv) { "Pool spike, $($src.Date), mean of 12 iterations. Numbers: $csv.csv" }
$shortArm = @{ 'A0' = 'Memory<T> chunks'; 'E1' = 'Arrays, lease per chunk'; 'E2' = 'Arrays, owner object'; 'E3' = 'Arrays, one store'; 'M1' = 'MemoryPool, span per row'; 'M2' = 'MemoryPool, span per chunk' }

# The chunk store: 10,000 int rows, one panel per shape.
$storePanels = @()
foreach ($shape in 'collect', 'buffer', 'stream') {
  $rows = @($ps | Where-Object { $_.shape -eq $shape -and $_.element -eq 'int' -and $_.rows -eq '10000' } | Sort-Object { [double]$_.ns } -Stable | ForEach-Object {
    [pscustomobject]@{ Name = $shortArm[$_.arm]; Fam = $_.family; Value = [double]$_.ns / 1000
      Detail = "$($_.arm_name): $(F ([double]$_.ns / 1000) 1) $muX`s to $shape 10,000 int rows." } })
  $title = switch ($shape) { 'collect' { 'Collect: add 10,000 rows, copy out' } 'buffer' { 'Buffer: add 10,000 rows, read each one' } default { 'Stream: add 10,000 rows, read chunk by chunk' } }
  $storePanels += @{ Title = $title; Rows = $rows }
}
Write-Lf (Join-Path $ImageDir 'perf-pool-store.svg') (New-BarSvg 'pool-store' 'Where the rows live: arrays or Memory' 'Shorter is better. 10,000 int rows, pool spike, net10.' $storePanels { param($v) (F $v 1) + " ${muX}s" } '' (Spike-Footer $psSrc 'pool-store') 'panel' 200 'Three panels of six bars, each panel on its own scale: the time to collect, buffer or stream 10,000 int rows with six designs of the chunk store. The two array designs are fastest in every panel.' 540)

# Releasing a chunk of class rows, three ways (the bin proof, paced so the bin never fills): median of three runs, one panel per chunk size.
$binPanels = @()
foreach ($size in 4096, 16384, 65536) {
  $rows = @('none', 'inline', 'bin' | ForEach-Object {
    $way = $_
    $vals = @($prp | Where-Object { $_.element -eq 'class' -and $_.chunk_rows -eq "$size" -and $_.way -eq $way } | ForEach-Object { [double]$_.release_ns })
    $name = switch ($way) { 'none' { 'No clear' } 'inline' { 'Inline clear' } default { 'Hand to the bin' } }
    $fam = switch ($way) { 'none' { 'p-none' } 'inline' { 'p-inline' } default { 'p-bin' } }
    [pscustomobject]@{ Name = $name; Fam = $fam; Value = (Med $vals)
      Detail = "$(F $size 0) class rows, $name`: median $(F (Med $vals) 0) ns on the releasing thread (runs: $(($vals | ForEach-Object { F $_ 0 }) -join ', '))." } })
  $binPanels += @{ Title = "$(F $size 0) class rows"; Rows = $rows }
}
Write-Lf (Join-Path $ImageDir 'perf-pool-bin.svg') (New-BarSvg 'pool-bin' 'Releasing a chunk of class rows: clear it here, or hand it to the bin' 'Shorter is better. Time on the releasing thread, median of 3 runs, bin never full.' $binPanels { param($v) (F $v 0) + ' ns' } '' "Bin proof, $($prpSrc.Date), median of 3 runs. Numbers: pool-release-path.csv" 'panel' 130 'Three panels of three bars each, each panel on its own scale: the time on the releasing thread to release one chunk of 4,096, 16,384 and 65,536 class rows, with no clear, with an inline clear and by handing it to the recycle bin. Handing to the bin costs 95, 75 and 144 nanoseconds; clearing inline costs 550, 1,786 and 6,738.' 520)

# End to end: how much less time than the Memory<T> chunks build.
$e2eRows = @($pe | Sort-Object { -[double]$_.rows }, terminal -Stable | ForEach-Object {
  $gain = (1 - [double]$_.after_us / [double]$_.before_us) * 100
  [pscustomobject]@{ Name = "$($_.terminal), $(F ([double]$_.rows) 0) rows"; Fam = 'arr'; Value = $gain
    Detail = "$($_.terminal), $(F ([double]$_.rows) 0) rows: $(Format-Micro ([double]$_.before_us)) before, $(Format-Micro ([double]$_.after_us)) after, $(F $gain 1)% less time." } })
Write-Lf (Join-Path $ImageDir 'perf-pool-e2e.svg') (New-BarSvg 'pool-e2e' 'The final design against the Memory chunks build' 'Longer is better. How much less time the call takes, in percent.' @(@{ Title = ''; Rows = $e2eRows }) { param($v) (F $v 1) + '%' } 'percent less time than the Memory<T> chunks build' "Builds 3c05c47 (before) and 6a770e8 (after), runs: $($peSrc.Date), SQLite in memory, class rows. Numbers: pool-e2e.csv" 'shared' 250 'Eight bars, one per terminal and row count: how much less time the final arrays design takes than the Memory chunks build it replaced.' 560)

# ---------- the tables (markdown includes) ----------
# Each file is one table, with no links (a page links to the CSV and the source file itself), so it reads the same
# wherever it is included.
$genNote = '<!-- Generated by docsite/tools/perf/New-PerfCharts.ps1 from docsite/perf-data/*.csv. Do not edit by hand. -->'
function Write-Table([string]$name, [string[]]$lines) { Write-Lf (Join-Path $IncludeDir "$name.md") ($genNote + "`n`n" + ($lines -join "`n")) }
function Fmt-Ratio([double]$v, [double]$base) { (F ($v / $base) 2) + $times }

# The summary table: the four approaches of the summary chart, ranked by the time the connection is held.
$t = @("| Approach | Connection held open | Whole call (error margin) | Memory allocated | Time against ``Map<T>()`` | Memory against ``Map<T>()`` |", '|---|---:|---:|---:|---:|---:|')
foreach ($r in @('V2', 'V1', 'V7a', 'V7' | ForEach-Object { Pick $ls $_ } | Sort-Object { [double]$_.reader_open_us } -Stable)) {
  $t += "| $(M $sumNames[$r.variant_id]) | $(Format-Micro ([double]$r.reader_open_us)) | $(Format-Micro ([double]$r.total_us)) (±$(F (100 * [double]$r.error_us / [double]$r.total_us) 1)%) | $(F ([double]$r.allocated_kb) 1) KB | $(Fmt-Ratio ([double]$r.reader_open_us) ([double]$v1.reader_open_us)) | $(Fmt-Ratio ([double]$r.allocated_kb) ([double]$v1.allocated_kb)) |"
}
Write-Table 'summary' $t

# The real-database tables of the library comparison: each run on its own, with its own ratio to Map<T>().
$t = @("| Database | Approach | Run a | Run b | Run a against ``Map<T>()`` | Run b against ``Map<T>()`` |", '|---|---|---:|---:|---:|---:|')
foreach ($prov in $lrProv) {
  $at = @{}; foreach ($run in 'a', 'b') { $at[$run] = [double](($lr | Where-Object { $_.provider -eq $prov -and $_.variant_id -eq 'V1' -and $_.run -eq $run }).total_us) }
  foreach ($id in 'V2', 'V1', 'V7a', 'V7') {
    $cells = @{}; $ratios = @{}
    foreach ($run in 'a', 'b') {
      $r = $lr | Where-Object { $_.provider -eq $prov -and $_.variant_id -eq $id -and $_.run -eq $run }
      $cells[$run] = "$(F ([double]$r.total_us / 1000) 2) ms (±$(F ([double]$r.error_us / 1000) 2))"
      $ratios[$run] = Fmt-Ratio ([double]$r.total_us) $at[$run]
    }
    $t += "| $prov | $(M $sumNames[$id]) | $($cells['a']) | $($cells['b']) | $($ratios['a']) | $($ratios['b']) |"
  }
}
Write-Table 'lib-real-time' $t

$t = @("| Database | Approach | Memory allocated | Against ``Map<T>()`` |", '|---|---|---:|---:|')
foreach ($prov in $lrProv) {
  $base = [double](($lr | Where-Object { $_.provider -eq $prov -and $_.variant_id -eq 'V1' -and $_.run -eq 'a' }).allocated_kb)
  foreach ($id in 'V2', 'V1', 'V7a', 'V7') {
    $kbs = @($lr | Where-Object { $_.provider -eq $prov -and $_.variant_id -eq $id } | ForEach-Object { [double]$_.allocated_kb })
    if (($kbs | Measure-Object -Maximum).Maximum - ($kbs | Measure-Object -Minimum).Minimum -gt 1) { throw "memory differs between the runs: $prov $id" }
    $t += "| $prov | $(M $sumNames[$id]) | $(F ($kbs[0] / 1024) 2) MB | $(Fmt-Ratio $kbs[0] $base) |"
  }
}
Write-Table 'lib-real-memory' $t

# All 16 variants, ranked by the time the connection is held.
$t = @("| Rank | Variant | What it does | Connection held ($($mu)s) | Whole call ($($mu)s) | Allocated (KB) |", '|---:|---|---|---:|---:|---:|')
$rank = 0
foreach ($r in ($syn | Sort-Object { [double]$_.reader_open_us } -Stable)) {
  $rank++
  $t += "| $rank | $(M $r.variant) | $(M $r.description) | $(F ([double]$r.reader_open_us) 1) | $(F ([double]$r.total_us) 1) | $(F ([double]$r.allocated_kb) 1) |"
}
Write-Table 'variants' $t

# Cost per column.
$t = @(("| Approach | " + (($widths | ForEach-Object { if ($_ -eq 1) { "1 column" } else { "$_ columns" } }) -join ' | ') + " | Cost of one more cell (ns) |"), ("|---|" + (($widths | ForEach-Object { '---:' }) -join '|') + "|---:|"))
foreach ($id in $ids) {
  $pts = @($swp | Where-Object { $_.variant_id -eq $id } | Sort-Object { [int]$_.columns })
  $cell = ($widths | ForEach-Object { $c = $_; (F ([double]($pts | Where-Object { [int]$_.columns -eq $c }).total_us) 1) }) -join ' | '
  $rowsN = [double]$pts[0].rows
  $perCell = ([double]$pts[-1].total_us - [double]$pts[0].total_us) * 1000 / (([int]$pts[-1].columns - [int]$pts[0].columns) * $rowsN)
  $t += "| $(M $pts[0].variant) | $cell | $(F $perCell 2) |"
}
Write-Table 'width' $t

# What building a map costs.
$t = @("| Columns | Reused map, one-row call ($($mu)s) | Map built on every call (ms) | One-time cost (ms) | One-time memory (KB) |", '|---:|---:|---:|---:|---:|')
foreach ($m in ($map | Sort-Object { [int]$_.columns })) {
  $t += "| $($m.columns) | $(F ([double]$m.reused_total_us) 2) | $(F ([double]$m.built_every_call_total_us / 1000) 2) | $(F (([double]$m.built_every_call_total_us - [double]$m.reused_total_us) / 1000) 2) | $(F ([double]$m.built_every_call_allocated_kb - [double]$m.reused_allocated_kb) 1) |"
}
Write-Table 'map-cost' $t

# Real databases: one table per provider, time and memory side by side.
foreach ($pair in @(@('SQL Server LocalDB', 'real-sqlserver', '20'), @('SQLite file', 'real-sqlite', '20'), @('SQL Server LocalDB', 'real-sqlserver-w5', '5'), @('SQLite file', 'real-sqlite-w5', '5'))) {
  $rows = @($rp | Where-Object { $_.provider -eq $pair[0] -and $_.columns -eq $pair[2] } | Sort-Object { [double]$_.total_us } -Stable)
  if ($rows.Count -eq 0) { continue }
  $bestT = [double]$rows[0].total_us
  $bestM = ($rows | ForEach-Object { [double]$_.allocated_kb } | Measure-Object -Minimum).Minimum
  $t = @("| Rank | Variant | Time per call (ms) | Error margin | Against the best | Memory per call (MB) | Against the least |", "|---:|---|---:|---:|---:|---:|---:|")
  $rank = 0
  foreach ($r in $rows) { $rank++; $t += "| $rank | $(M $r.variant) | $(F ([double]$r.total_us / 1000) 2) | ±$(F (100 * [double]$r.error_us / [double]$r.total_us) 1)% | $(Format-Pct ([double]$r.total_us) $bestT) | $(F ([double]$r.allocated_kb / 1024) 2) | $(Format-Pct ([double]$r.allocated_kb) $bestM) |" }
  Write-Table $pair[1] $t
}

# Sync against async.
$t = @('| Condition | Sync | Async execute | Async read | Async read against sync |', '|---|---:|---:|---:|---:|')
foreach ($cond in @($sa | ForEach-Object { $_.condition } | Select-Object -Unique)) {
  $g = @($sa | Where-Object { $_.condition -eq $cond })
  $a = [double]($g | Where-Object { $_.path -eq 'Sync' }).reader_open_us
  $b = [double]($g | Where-Object { $_.path -eq 'Async execute' }).reader_open_us
  $c = [double]($g | Where-Object { $_.path -eq 'Async read' }).reader_open_us
  $t += "| $cond | $(Format-Micro $a) | $(Format-Micro $b) | $(Format-Micro $c) | $(F ($c / $a) 2)$times |"
}
Write-Table 'sync-async' $t

# The slow-link tables.
$t = @('| Scenario | Variant | Time per operation (ms) |', '|---|---|---:|')
foreach ($r in $lt) { $t += "| $(M "$($r.scenario): $($r.scenario_description)") | $(M $r.variant) | $(F ([double]$r.wall_ms) 1) |" }
Write-Table 'link-time' $t

$t = @('| Shape | Read path | Memory per operation (KB) | Time (ms) | ReadAsync calls that did not complete at once (of 10,001) |', '|---|---|---:|---:|---:|')
foreach ($r in $lm) { $t += "| $(M $r.shape) | $(M $r.read_path) | $(F ([double]$r.allocated_kb) 0) | $(F ([double]$r.wall_ms) 1) | $(([double]$r.incomplete_readasync_calls).ToString("0.#", $inv)) |" }
Write-Table 'link-memory' $t

$syncSeconds = [double](($lv | Where-Object { $_.variant -like '*sync Read' } | Select-Object -First 1).seconds)
$t = @('| Variant | Time (s) | Against the sync read | Peak working set (MB) |', '|---|---:|---:|---:|')
foreach ($r in $lv) {
  if ($r.seconds) { $t += "| $(M $r.variant) | $(F ([double]$r.seconds) 2) | $(Fmt-Ratio ([double]$r.seconds) $syncSeconds) | $(if ($r.peak_working_set_mb) { $r.peak_working_set_mb } else { 'not recorded' }) |" }
  else { $t += "| $(M $r.variant) | not measured | not measured | not measured |" }
}
Write-Table 'large-value' $t

$t = @('| Feature | Rows | Plain loop | ODX | ODX time against the loop | ODX memory against the loop |', '|---|---:|---:|---:|---:|---:|')
foreach ($r in $ch) { $t += "| $(M $r.feature) | $(F ([double]$r.rows) 0) | $(Format-Micro ([double]$r.loop_us)) | $(Format-Micro ([double]$r.odx_us)) | $($r.time_ratio)$times | $(F ([double]$r.alloc_ratio) 2)$times |" }
Write-Table 'channels' $t

# The pooling tables.
function Fmt-Ns([double]$ns) { if ($ns -ge 1000) { (F ($ns / 1000) 1) + " $mu" + "s" } else { (F $ns 1) + " ns" } }
function Fmt-Us([double]$us) { (F $us 1) + " $mu" + "s" }
$armNames = 'Memory<T> chunks', 'Arrays, lease per chunk', 'Arrays, owner object', 'Arrays, one store', 'MemoryPool, span per row', 'MemoryPool, span per chunk'
$t = @(('| Shape | Rows | ' + ($armNames -join ' | ') + ' |'), '|---|---:|---:|---:|---:|---:|---:|---:|')
foreach ($shape in 'collect', 'buffer', 'stream') { foreach ($el in 'int', 'class') { foreach ($n in '1', '100', '10000') {
  $cells = foreach ($arm in 'A0', 'E1', 'E2', 'E3', 'M1', 'M2') { $v = [double]($ps | Where-Object { $_.shape -eq $shape -and $_.element -eq $el -and $_.rows -eq $n -and $_.arm -eq $arm }).ns; Fmt-Ns $v }
  $t += "| $shape, $el rows | $(F ([double]$n) 0) | " + ($cells -join ' | ') + ' |' } } }
Write-Table 'pool-store' $t

$t = @('| Size (slots) | ArrayPool, int | MemoryPool, one span, int | MemoryPool, per row, int | ArrayPool, string | MemoryPool, string |', '|---:|---:|---:|---:|---:|---:|')
foreach ($s in 16, 1024, 8192) {
  $c = foreach ($case in 'ArrayShared int', 'MemoryShared int (span once)', 'MemoryShared int (per row)', 'ArrayShared string', 'MemoryShared string') { "$(F ([double]($pr | Where-Object { $_.size -eq "$s" -and $_.case -eq $case }).ns) 1) ns" }
  $t += "| $(F $s 0) | " + ($c -join ' | ') + ' |' }
Write-Table 'pool-rent' $t

$t = @('| What | Bytes |', '|---|---:|')
foreach ($r in $pb) { $t += "| $(M $r.item) | $($r.bytes) |" }
Write-Table 'pool-bytes' $t

# The bin proof tables.
function Rng([double[]]$v, [string]$fmt) { $m = ($v | Measure-Object -Minimum -Maximum); "$(F (Med $v) $fmt) ($(F $m.Minimum $fmt) to $(F $m.Maximum $fmt))" }
$t = @('| Rows in the chunk | Int, no clear | Int, inline clear | Class, no clear | Class, inline clear | Class, hand to the bin |', '|---:|---:|---:|---:|---:|---:|')
foreach ($n in 4096, 16384, 65536) {
  $c = foreach ($pair in @(@('int', 'none'), @('int', 'inline'), @('class', 'none'), @('class', 'inline'), @('class', 'bin'))) {
    $v = @($prp | Where-Object { $_.element -eq $pair[0] -and $_.chunk_rows -eq "$n" -and $_.way -eq $pair[1] } | ForEach-Object { [double]$_.release_ns })
    (Rng $v 0) + ' ns' }
  $t += "| $(F $n 0) | " + ($c -join ' | ') + ' |' }
Write-Table 'pool-release-path' $t

$t = @('| Rows | Array | Rows used | `AsSpan(0, used).Clear()` then `Return(false)` | `AsSpan().Clear()` then `Return(false)` | `Return(clearArray: true)` | `Array.Clear(array, 0, used)` then `Return(false)` |', '|---|---:|---:|---:|---:|---:|---:|')
foreach ($el in 'int', 'class') {
  foreach ($key in @($pcs | Where-Object { $_.element -eq $el } | ForEach-Object { "$($_.array_len)/$($_.used_rows)" } | Select-Object -Unique)) {
    $len, $used = $key.Split('/')
    $c = foreach ($m in @($pcs | ForEach-Object { $_.method } | Select-Object -Unique)) {
      $v = @($pcs | Where-Object { $_.element -eq $el -and $_.array_len -eq $len -and $_.used_rows -eq $used -and $_.method -eq $m } | ForEach-Object { [double]$_.ns })
      "$(F (Med $v) 0) ns" }
    $t += "| $el | $(F ([double]$len) 0) | $(F ([double]$used) 0) | " + ($c -join ' | ') + ' |' } }
Write-Table 'pool-clear-speed' $t

$t = @('| Call | Rows | Bin: time | Inline always: time | Time, bin against inline (3 runs) | Bin: CPU | Inline always: CPU | CPU, bin against inline (3 runs) |', '|---|---:|---:|---:|---:|---:|---:|---:|')
foreach ($term in 'ToListAsync', 'ToPooledBufferStream') { foreach ($n in 10000, 100000) {
  $g = @($ppe | Where-Object { $_.terminal -eq $term -and $_.rows -eq "$n" })
  $b = @($g | Where-Object { $_.arm -eq 'bin' } | Sort-Object { [int]$_.run }); $i = @($g | Where-Object { $_.arm -eq 'inline-always' } | Sort-Object { [int]$_.run })
  $tr = for ($k = 0; $k -lt 3; $k++) { [double]$b[$k].median_ms / [double]$i[$k].median_ms }
  $cr = for ($k = 0; $k -lt 3; $k++) { [double]$b[$k].cpu_ms / [double]$i[$k].cpu_ms }
  $t += "| ``$term`` | $(F $n 0) | $(F (Med ($b | ForEach-Object { [double]$_.median_ms })) 2) ms | $(F (Med ($i | ForEach-Object { [double]$_.median_ms })) 2) ms | $(F (($tr | Measure-Object -Minimum).Minimum) 2)$times to $(F (($tr | Measure-Object -Maximum).Maximum) 2)$times | $(F (Med ($b | ForEach-Object { [double]$_.cpu_ms })) 2) ms | $(F (Med ($i | ForEach-Object { [double]$_.cpu_ms })) 2) ms | $(F (($cr | Measure-Object -Minimum).Minimum) 2)$times to $(F (($cr | Measure-Object -Maximum).Maximum) 2)$times |" } }
Write-Table 'pool-bin-proof-e2e' $t

$t = @('| Chunk (class rows) | Way | Releasing thread | Total CPU, all threads |', '|---:|---|---:|---:|')
foreach ($n in 4096, 16384, 65536) { foreach ($w in 'none', 'inline', 'bin') {
  $v = @($pcpu | Where-Object { $_.element -eq 'class' -and $_.chunk_rows -eq "$n" -and $_.way -eq $w })
  $name = switch ($w) { 'none' { 'no clear' } 'inline' { 'inline clear' } default { 'hand to the bin' } }
  $t += "| $(F $n 0) | $name | $(Fmt-Ns (Med ($v | ForEach-Object { [double]$_.releasing_ns }))) | $(Fmt-Ns (Med ($v | ForEach-Object { [double]$_.total_cpu_ns }))) |" } }
Write-Table 'pool-bin-cpu' $t

foreach ($set in @(@('pool-e2e', $pe), @('pool-bin-cost', $pbc))) {
  $t = @('| Call | Rows | Before | After | Change | Error margins | Memory before | Memory after |', '|---|---:|---:|---:|---:|---:|---:|---:|')
  foreach ($r in ($set[1] | Sort-Object { [double]$_.rows }, terminal -Stable)) {
    $chg = ([double]$r.after_us / [double]$r.before_us - 1) * 100
    $word = if ($chg -le 0) { (F (-$chg) 1) + '% less' } else { (F $chg 1) + '% more' }
    $t += "| ``$(M $r.terminal)`` | $(F ([double]$r.rows) 0) | $(Fmt-Us ([double]$r.before_us)) | $(Fmt-Us ([double]$r.after_us)) | $word | ±$(F (100 * [double]$r.before_error_us / [double]$r.before_us) 1)%, ±$(F (100 * [double]$r.after_error_us / [double]$r.after_us) 1)% | $(F ([double]$r.before_allocated_kb) 2) KB | $(F ([double]$r.after_allocated_kb) 2) KB |" }
  Write-Table $set[0] $t }

# How fresh each data set is.
$t = @('| Data file | Used on | Build | Run date | Machine | Runtime |', '|---|---|---|---|---|---|')
foreach ($p in @(
    @('library-synthetic', 'library comparison, the summary', $lsSrc),
    @('library-real', 'library comparison, real databases', $lrSrc),
    @('synthetic-overhead', 'mapping variants (all variants)', $synSrc),
    @('width-sweep', 'cost per column', $swpSrc),
    @('map-cost', 'cost of building a map', $mapSrc),
    @('real-providers', 'real databases (five variants, two widths)', $rpSrc),
    @('sync-async', 'sync reads against async reads', $saSrc),
    @('link-time', 'slow links and large values', $ltSrc),
    @('link-memory', 'slow links and large values', $lmSrc),
    @('large-value', 'slow links and large values', $lvSrc),
    @('channels', 'channels and Dataflow', $chSrc),
    @('pool-store', 'pooling and clearing', $psSrc),
    @('pool-rent', 'pooling and clearing', $prSrc),
    @('pool-bytes', 'pooling and clearing', $pbSrc),
    @('pool-release-path', 'pooling and clearing', $prpSrc),
    @('pool-clear-speed', 'pooling and clearing', $pcsSrc),
    @('pool-bin-proof-e2e', 'pooling and clearing', $ppeSrc),
    @('pool-bin-cpu', 'pooling and clearing', $pcpuSrc),
    @('pool-e2e', 'pooling and clearing', $peSrc),
    @('pool-bin-cost', 'pooling and clearing', $pbcSrc),
    @('pooled-buffer-cost', 'async buffers and streams (what it costs)', $pbcostSrc))) {
  $bld = ($p[2].Build -split ' \(')[0]
  $t += "| ``$($p[0]).csv`` | $($p[1]) | ``$bld`` | $($p[2].Date) | $(M $p[2].Machine) | $(M $p[2].Runtime.Replace('[Host] : ', '')) |"
}
Write-Table 'freshness' $t

"wrote charts to $ImageDir and tables to $IncludeDir"
