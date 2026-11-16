<#
.SYNOPSIS
  Generates the performance charts (SVG) and the data page (markdown) from the CSV files in docsite/perf-data.

.DESCRIPTION
  One command regenerates everything:  pwsh -NoProfile -File docsite/tools/perf/New-PerfCharts.ps1

  Reads  docsite/perf-data/*.csv and *.source.txt
  Writes docsite/images/perf/*.svg and docsite/articles/performance-data.md

  The output is deterministic (invariant culture, LF line endings, UTF-8 without a BOM), so a second run
  over the same CSV files gives byte-identical files. Each SVG follows the operating system's light or
  dark setting and draws its own background, so it stays readable when the site's theme switch and the
  operating system disagree.
#>
param(
  [string]$DataDir = (Join-Path $PSScriptRoot '../../perf-data'),
  [string]$ImageDir = (Join-Path $PSScriptRoot '../../images/perf'),
  [string]$DataPage = (Join-Path $PSScriptRoot '../../articles/performance-data.md')
)
$ErrorActionPreference = 'Stop'
$inv = [Globalization.CultureInfo]::InvariantCulture
$DataDir = [IO.Path]::GetFullPath($DataDir)
$ImageDir = [IO.Path]::GetFullPath($ImageDir)
$DataPage = [IO.Path]::GetFullPath($DataPage)
New-Item -ItemType Directory -Force -Path $ImageDir | Out-Null

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
    Machine = $lines[[array]::IndexOf(@($lines | ForEach-Object { $_ -like 'BenchmarkDotNet v*' }), $true) + 1].Trim()
    Runtime = ($lines | Where-Object { $_ -match '\[Host\]' } | Select-Object -First 1).Trim()
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
$famClass = @{ 'odx-one' = 'one'; 'odx-select' = 'select'; 'odx-raw' = 'grey'; 'reference' = 'grey'; 'dapper' = 'dapper' }
$famName = @{ 'odx-one' = 'ODX: one object per row'; 'odx-select' = 'ODX: row class, then Select'; 'odx-raw' = 'ODX: raw object[] rows'; 'reference' = 'Hand loop on the provider reader (reference)'; 'dapper' = 'Dapper 2.1.89' }
$famOrder = 'odx-one', 'odx-select', 'odx-raw', 'reference', 'dapper'

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
  [string]$footer, [string]$scale, [int]$left, [string]$descIntro) {
  $W = 560; $rowH = 26; $plotW = $W - $left - 72
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
      [void]$sb.AppendLine("    <text class=""val"" x=""$(N ($left + $bw + 6))"" y=""$($ry + 17)"">$(X (& $format $r.Value))</text>")
      [void]$sb.AppendLine("  </g>")
      $i++
    }
    [void]$sb.AppendLine("  <line class=""axis"" x1=""$left"" y1=""$top"" x2=""$left"" y2=""$bottom""/>")
    $prefix = if ($p.Title) { "$($p.Title): " } else { '' }
    $descParts += ($prefix + (($p.Rows | ForEach-Object { "$($_.Name) $(& $format $_.Value)" }) -join '; '))
    $y = $bottom + $(if ($ticks) { 24 } else { 0 }) + 14
  }
  if ($scale -eq 'shared' -and $axisCaption) {
    [void]$sb.AppendLine("  <text class=""muted small mid"" x=""$(N ($left + $plotW / 2))"" y=""$($y - 6)"">$(X $axisCaption)</text>"); $y += 14
  }
  [void]$sb.AppendLine("  <text class=""muted small"" x=""16"" y=""$($y + 8)"">$(X $footer)</text>")
  $H = $y + 22
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

# ---------- 1. headline tiles ----------
$v1 = Pick $syn 'V1'; $v2 = Pick $syn 'V2'; $v0 = Pick $syn 'V0'; $dap = Pick $syn 'V7_V1'
$tiles = @(
  @{ Big = (F ([double]$dap.reader_open_us / [double]$v1.reader_open_us) 2) + $timesX; Text = 'less reader-open time than Dapper, for the RowMap plan'; Tip = "Dapper $(F ([double]$dap.reader_open_us) 1) $muX`s against the RowMap plan $(F ([double]$v1.reader_open_us) 1) ${muX}s" },
  @{ Big = (F ([double]$dap.allocated_kb / [double]$v1.allocated_kb) 2) + $timesX; Text = 'fewer bytes allocated than Dapper'; Tip = "Dapper $(F ([double]$dap.allocated_kb) 1) KB against the RowMap plan $(F ([double]$v1.allocated_kb) 1) KB" },
  @{ Big = (F ([double]$v0.reader_open_us / [double]$v1.reader_open_us) 2) + $timesX; Text = 'faster than dumping every row into object[]'; Tip = "object[] rows $(F ([double]$v0.reader_open_us) 1) ${muX}s against the RowMap plan $(F ([double]$v1.reader_open_us) 1) ${muX}s" },
  @{ Big = (F ([double]$v1.reader_open_us / [double]$v2.reader_open_us) 2) + $timesX; Text = 'how much slower the plan is than a hand-written selector'; Tip = "RowMap plan $(F ([double]$v1.reader_open_us) 1) ${muX}s against the hand-written selector $(F ([double]$v2.reader_open_us) 1) ${muX}s" }
)
$sb = New-Object Text.StringBuilder
[void]$sb.AppendLine('  <text class="title" x="16" y="28">The RowMap plan against Dapper and hand-written code</text>')
[void]$sb.AppendLine('  <text class="muted" x="16" y="48">20 columns, 1,000 rows, synthetic reader. Reader-open time and allocation.</text>')
$tw = 260; $th = 96
for ($i = 0; $i -lt 4; $i++) {
  $tx = 16 + ($i % 2) * ($tw + 8); $ty = 64 + [math]::Floor($i / 2) * ($th + 8)
  [void]$sb.AppendLine("  <g>")
  [void]$sb.AppendLine("    <title>$(X $tiles[$i].Tip)</title>")
  [void]$sb.AppendLine("    <rect class=""tile"" x=""$tx"" y=""$ty"" width=""$tw"" height=""$th"" rx=""8""/>")
  [void]$sb.AppendLine("    <text class=""big"" x=""$($tx + 14)"" y=""$($ty + 42)"">$($tiles[$i].Big)</text>")
  $ly = $ty + 64
  foreach ($line in (Get-Words $tiles[$i].Text 34)) { [void]$sb.AppendLine("    <text class=""muted"" x=""$($tx + 14)"" y=""$ly"">$(X $line)</text>"); $ly += 17 }
  [void]$sb.AppendLine("  </g>")
}
$fy = 64 + 2 * ($th + 8) + 14
[void]$sb.AppendLine("  <text class=""muted small"" x=""16"" y=""$fy"">$(X (Footer $synSrc 'synthetic-overhead'))</text>")
$descSummary = "Four headline ratios from the RowMap plan, in the synthetic runs. " + (($tiles | ForEach-Object { "$($_.Big) $($_.Text)" }) -join '. ') + '.'
Write-Lf (Join-Path $ImageDir 'perf-summary.svg') (New-Svg 'summary' 'The RowMap plan against Dapper and hand-written code' $descSummary 560 ($fy + 18) $sb.ToString())

# ---------- 2. synthetic bars: reader-open, total, memory ----------
function Get-SynRows([string]$col, [scriptblock]$fmt) {
  $rows = @($syn | Sort-Object { [double]$_.$col } -Stable | ForEach-Object {
    [pscustomobject]@{ Name = $_.variant; Fam = $_.family; Value = [double]$_.$col
      Detail = "$($_.variant) ($($_.variant_id)): $($_.description). Reader open $(Format-Micro ([double]$_.reader_open_us)), total $(Format-Micro ([double]$_.total_us)), $(F ([double]$_.allocated_kb) 1) KB allocated." } })
  ,$rows
}
$synIntro = 'Ranked horizontal bars, shortest first, one per way of reading 20 columns and 1,000 rows from a synthetic in-memory reader, colored by family.'
Write-Lf (Join-Path $ImageDir 'perf-synthetic-open.svg') (New-BarSvg 'syn-open' 'Reader-open time per call' 'Shorter is better: a shorter bar holds the connection for less time.' @(@{ Title = ''; Rows = (Get-SynRows 'reader_open_us') }) { param($v) Format-MicroX $v } ("microseconds from ExecuteReader until the reader closes") (Footer $synSrc 'synthetic-overhead') 'shared' 232 $synIntro)
Write-Lf (Join-Path $ImageDir 'perf-synthetic-total.svg') (New-BarSvg 'syn-total' 'Total time per call' 'Shorter is better. Where it is far above reader-open time, work happens after the reader closes.' @(@{ Title = ''; Rows = (Get-SynRows 'total_us') }) { param($v) Format-MicroX $v } 'microseconds per call, the whole call' (Footer $synSrc 'synthetic-overhead') 'shared' 232 $synIntro)
Write-Lf (Join-Path $ImageDir 'perf-synthetic-memory.svg') (New-BarSvg 'syn-memory' 'Memory allocated per call' 'Smaller is better.' @(@{ Title = ''; Rows = (Get-SynRows 'allocated_kb') }) { param($v) (F $v 1) + ' KB' } 'kilobytes allocated per call' (Footer $synSrc 'synthetic-overhead') 'shared' 232 $synIntro)

# ---------- 3. cost grows with columns: a line chart ----------
$short = @{ 'V2' = 'Hand-written selector'; 'V1' = 'RowMap plan'; 'V3' = 'Row class + Select'; 'V0' = 'object[] rows'; 'V7_V1' = 'Dapper' }
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

# ---------- 6. sync against async ----------
$saPanels = @()
foreach ($cond in @($sa | ForEach-Object { $_.condition } | Select-Object -Unique)) {
  $rows = @($sa | Where-Object { $_.condition -eq $cond } | ForEach-Object {
    $syncRow = $sa | Where-Object { $_.condition -eq $cond -and $_.path -eq 'Sync' } | Select-Object -First 1
    [pscustomobject]@{ Name = $_.path; Fam = 'odx-one'; Value = [double]$_.reader_open_us
      Detail = "$cond, $($_.path): reader open $(Format-Micro ([double]$_.reader_open_us)), $(F ([double]$_.reader_open_us / [double]$syncRow.reader_open_us) 2)$timesX the sync read; $(F ([double]$_.allocated_kb) 1) KB allocated." } })
  $saPanels += @{ Title = $cond; Rows = $rows }
}
Write-Lf (Join-Path $ImageDir 'perf-sync-async.svg') (New-BarSvg 'sync-async' 'Sync reads against async reads' 'Reader-open time, RowMap plan, 20 columns, 1,000 rows. Each panel has its own scale.' $saPanels { param($v) Format-MicroX $v } '' (Footer $saSrc 'sync-async') 'panel' 110 'Four panels of three bars each, one panel per condition, each on its own scale: sync, async execute, and async read.')

# ---------- the data page ----------
function Link-Data([string]$name) { "[$name.csv](../perf-data/$name.csv) ([source and conditions](../perf-data/$name.source.txt))" }
function Rank-Table($rows, [string]$valueHeader, [scriptblock]$valueText, [string]$col, [bool]$withFamily) {
  $sorted = @($rows | Sort-Object { [double]$_.$col } -Stable)
  $best = [double]$sorted[0].$col
  $out = @(); $r = 0
  $out += "| Rank | Variant | $(if ($withFamily) { 'Family | ' })$valueHeader | Against the best |"
  $out += "|---:|---|$(if ($withFamily) { '---|' })---:|---:|"
  foreach ($x in $sorted) { $r++; $out += "| $r | $(M $x.variant) | $(if ($withFamily) { (M $famName[$x.family]) + ' | ' })$(& $valueText ([double]$x.$col)) | $(Format-Pct ([double]$x.$col) $best) |" }
  $out -join "`n"
}
$md = New-Object Text.StringBuilder
function Add([string]$s) { [void]$md.AppendLine($s) }
Add '<!-- Generated by docsite/tools/perf/New-PerfCharts.ps1 from docsite/perf-data/*.csv. Do not edit by hand. -->'
Add '# Performance data'
Add ''
Add "The numbers behind every chart on [Performance](performance.md), as tables. Each section gives the same numbers as the CSV file next to it, which you can download, and a source file that names the benchmark, the build, the date, the machine and the conditions, and the result file each number came from. The tables and the charts are generated from the CSV files by one script, so nothing here is typed by hand."
Add ''

Add '## How fresh the data is'
Add ''
Add 'Each data set comes from one BenchmarkDotNet run (or a few runs of the same build). The build is the commit of the benchmark project that was run; the date is when the run finished.'
Add ''
Add '| Data | Charts | Build | Run date | Machine | Runtime |'
Add '|---|---|---|---|---|---|'
foreach ($p in @(
    @('synthetic-overhead', 'headline ratios, reader-open time, total time, memory', $synSrc),
    @('width-sweep', 'cost grows with columns', $swpSrc),
    @('map-cost', 'what building a map costs', $mapSrc),
    @('real-providers', 'real databases', $rpSrc),
    @('sync-async', 'sync against async reads', $saSrc))) {
  Add "| ``$($p[0]).csv`` | $($p[1]) | ``$($p[2].Build)`` | $($p[2].Date) | $(M $p[2].Machine) | $(M $p[2].Runtime.Replace('[Host] : ', '')) |"
}
Add ''
Add '<a id="headline"></a>'
Add '## Headline ratios'
Add ''
Add "[Back to the chart](performance.md#headline). Each ratio divides two numbers of the table [Reader-open time](#open-time) or [Memory allocated](#memory)."
Add ''
Add '| Ratio | Value | Computed from |'
Add '|---|---:|---|'
Add "| Dapper reader-open time over the RowMap plan | $(F ([double]$dap.reader_open_us / [double]$v1.reader_open_us) 2)$times | $(F ([double]$dap.reader_open_us) 1) $($mu)s / $(F ([double]$v1.reader_open_us) 1) $($mu)s |"
Add "| Dapper allocation over the RowMap plan | $(F ([double]$dap.allocated_kb / [double]$v1.allocated_kb) 2)$times | $(F ([double]$dap.allocated_kb) 1) KB / $(F ([double]$v1.allocated_kb) 1) KB |"
Add "| object[] rows reader-open time over the RowMap plan | $(F ([double]$v0.reader_open_us / [double]$v1.reader_open_us) 2)$times | $(F ([double]$v0.reader_open_us) 1) $($mu)s / $(F ([double]$v1.reader_open_us) 1) $($mu)s |"
Add "| RowMap plan reader-open time over the hand-written selector | $(F ([double]$v1.reader_open_us / [double]$v2.reader_open_us) 2)$times | $(F ([double]$v1.reader_open_us) 1) $($mu)s / $(F ([double]$v2.reader_open_us) 1) $($mu)s |"
Add ''
Add "Data: $(Link-Data 'synthetic-overhead')."
Add ''

$sections = @(
  @{ Id = 'open-time'; Title = 'Reader-open time'; Col = 'reader_open_us'; Head = "Reader open ($mu`s)"; Text = { param($v) F $v 1 }; Note = 'Mean time from ExecuteReader until the reader closes, 20 columns, 1,000 rows, synthetic reader.' },
  @{ Id = 'total-time'; Title = 'Total time per call'; Col = 'total_us'; Head = "Total ($mu`s)"; Text = { param($v) F $v 1 }; Note = 'Mean time of the whole call, same runs as above.' },
  @{ Id = 'memory'; Title = 'Memory allocated per call'; Col = 'allocated_kb'; Head = 'Allocated (KB)'; Text = { param($v) F $v 1 }; Note = 'Bytes allocated per call divided by 1,024, same runs as above.' }
)
foreach ($s in $sections) {
  Add "<a id=""$($s.Id)""></a>"
  Add "## $($s.Title)"
  Add ''
  Add "[Back to the chart](performance.md#$($s.Id)). $($s.Note) Build $($synSrc.Build), $($synSrc.Date)."
  Add ''
  Add (Rank-Table $syn $s.Head $s.Text $s.Col $true)
  Add ''
  Add "Data: $(Link-Data 'synthetic-overhead')."
  Add ''
}

Add '<a id="width"></a>'
Add '## Cost grows with the number of columns'
Add ''
Add "[Back to the chart](performance.md#width). Total time per call ($($mu)s) at 1,000 rows. The last column is the cost of one more cell: the time at the widest result minus the time at the narrowest, divided by the cells added. Build $($swpSrc.Build), $($swpSrc.Date)."
Add ''
Add ("| Approach | " + (($widths | ForEach-Object { "$_ columns" }) -join ' | ') + " | Per added cell (ns) |")
Add ("|---|" + (($widths | ForEach-Object { '---:' }) -join '|') + "|---:|")
foreach ($id in $ids) {
  $pts = @($swp | Where-Object { $_.variant_id -eq $id } | Sort-Object { [int]$_.columns })
  $cell = ($widths | ForEach-Object { $c = $_; (F ([double]($pts | Where-Object { [int]$_.columns -eq $c }).total_us) 1) }) -join ' | '
  $rowsN = [double]$pts[0].rows
  $perCell = ([double]$pts[-1].total_us - [double]$pts[0].total_us) * 1000 / (([int]$pts[-1].columns - [int]$pts[0].columns) * $rowsN)
  Add "| $(M $pts[0].variant) | $cell | $(F $perCell 2) |"
}
Add ''
Add "Data: $(Link-Data 'width-sweep')."
Add ''

Add '<a id="map-cost"></a>'
Add '## What building a map costs'
Add ''
Add "[Back to the chart](performance.md#map-cost). One row per call. The one-time cost is the time (and memory) of a call that builds and compiles a new map, minus a call that reuses the cached one. Build $($mapSrc.Build), $($mapSrc.Date)."
Add ''
Add "| Columns | Reused map ($($mu)s) | Map built every call (ms) | One-time cost (ms) | Reused map (KB) | Built every call (KB) | One-time memory (KB) |"
Add '|---:|---:|---:|---:|---:|---:|---:|'
foreach ($m in ($map | Sort-Object { [int]$_.columns })) {
  Add "| $($m.columns) | $(F ([double]$m.reused_total_us) 2) | $(F ([double]$m.built_every_call_total_us / 1000) 2) | $(F (([double]$m.built_every_call_total_us - [double]$m.reused_total_us) / 1000) 2) | $(F ([double]$m.reused_allocated_kb) 2) | $(F ([double]$m.built_every_call_allocated_kb) 1) | $(F ([double]$m.built_every_call_allocated_kb - [double]$m.reused_allocated_kb) 1) |"
}
Add ''
Add "Data: $(Link-Data 'map-cost')."
Add ''

foreach ($s in @(
    @{ Id = 'real-time'; Title = 'Real databases: time per call'; Col = 'total_us'; Head = 'Time (ms)'; Text = { param($v) F ($v / 1000) 2 } },
    @{ Id = 'real-memory'; Title = 'Real databases: memory per call'; Col = 'allocated_kb'; Head = 'Allocated (KB)'; Text = { param($v) F $v 1 } })) {
  Add "<a id=""$($s.Id)""></a>"
  Add "## $($s.Title)"
  Add ''
  Add "[Back to the chart](performance.md#$($s.Id)). 20 columns, 10,000 rows. The CSV also holds the 5-column runs of the same benchmark. Build $($rpSrc.Build), $($rpSrc.Date)."
  Add ''
  foreach ($prov in ($provOrder | Where-Object { $rp.provider -contains $_ })) {
    Add "### $prov"
    Add ''
    Add (Rank-Table @($rp | Where-Object { $_.provider -eq $prov -and $_.columns -eq '20' }) $s.Head $s.Text $s.Col $true)
    Add ''
  }
  Add "Data: $(Link-Data 'real-providers')."
  Add ''
}

Add '<a id="sync-async"></a>'
Add '## Sync reads against async reads'
Add ''
Add "[Back to the chart](performance.md#sync-async). Reader-open time ($($mu)s), RowMap plan, 20 columns, 1,000 rows. Buffer refills: every 100 rows the reader waits 20 $($mu)s. Busy pool: 8 work items kept queued on the thread pool. Build $($saSrc.Build), $($saSrc.Date)."
Add ''
Add '| Condition | Sync | Async execute | Async read | Async read against sync |'
Add '|---|---:|---:|---:|---:|'
foreach ($cond in @($sa | ForEach-Object { $_.condition } | Select-Object -Unique)) {
  $g = @($sa | Where-Object { $_.condition -eq $cond })
  $a = [double]($g | Where-Object { $_.path -eq 'Sync' }).reader_open_us
  $b = [double]($g | Where-Object { $_.path -eq 'Async execute' }).reader_open_us
  $c = [double]($g | Where-Object { $_.path -eq 'Async read' }).reader_open_us
  Add "| $cond | $(Format-Micro $a) | $(Format-Micro $b) | $(Format-Micro $c) | $(F ($c / $a) 2)$times |"
}
Add ''
Add "Data: $(Link-Data 'sync-async')."
Write-Lf $DataPage $md.ToString()
"wrote charts to $ImageDir and the data page to $DataPage"
