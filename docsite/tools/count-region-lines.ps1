<#
Counts the lines of code in the regions of docsite/samples/HowLittleCode.cs and prints the table of the "How little code" page.

One way of counting, for every cell: the lines of the region (the method as it would appear in a data-access class,
from its signature to its closing brace), without blank lines and without comment-only lines. `using` directives are
outside the regions. Setup that a library needs and the others do not (a DbContext) is a region of its own, counted apart.

To change a row (a different snippet for a task), change the region name in $Tasks and run the script again.
#>
param([string]$File = (Join-Path $PSScriptRoot '..\samples\HowLittleCode.cs'))

$ErrorActionPreference = 'Stop'

function Get-RegionLineCount {
	param([string[]]$Lines, [string]$Name)
	$inside = $false
	$found = $false
	$count = 0
	foreach ($line in $Lines) {
		if ($line -match "^\s*#region\s+$([regex]::Escape($Name))\s*$") { $inside = $true; $found = $true; continue }
		if ($inside -and $line -match '^\s*#endregion') { return $count }
		if ($inside) {
			$text = $line.Trim()
			if ($text -ne '' -and -not $text.StartsWith('//')) { $count++ }
		}
	}
	if (-not $found) { throw "Region '$Name' not found in $File" }
	throw "Region '$Name' is not closed in $File"
}

$lines = Get-Content -LiteralPath $File
$libraries = 'odx', 'dapper', 'ef', 'ado'
$headings = 'Open.Database.Extensions', 'Dapper', 'EF Core', 'plain ADO.NET'
$tasks = [ordered]@{
	'1. Rows to a list'                           = 'little_list'
	'2. Filter in SQL, project in C#, as an async stream'   = 'little_stream'
	'3. The rows of one author in memory, then LINQ'   = 'little_buffer'
	'4. A stored procedure with two parameters'   = 'little_proc'
}

'| Task | ' + ($headings -join ' | ') + ' |'
'|---|' + ((1..$libraries.Count | ForEach-Object { '---:|' }) -join '')
foreach ($task in $tasks.GetEnumerator()) {
	$cells = foreach ($library in $libraries) { Get-RegionLineCount $lines "$($task.Value)_$library" }
	'| ' + $task.Key + ' | ' + ($cells -join ' | ') + ' |'
}
''
'Setup counted apart: ' + (Get-RegionLineCount $lines 'little_setup_ef') + ' lines (the EF Core context).'
