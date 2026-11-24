using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Benchmarks;

// What the recycle bin hand-off costs in TIME. Rows that hold references
// are always cleared on the bin (off the read path) when their buffers go
// back; before, with no clearing asked for, the arrays were disposed
// directly. CLASS rows (an int and a string) on SQLite in memory, through
// the two buffer terminals that carry the difference:
//   Buffer_  ToPooledBuffer(ct).Items, then dispose the items
//   Stream_  ToPooledStream(ct), consumed whole with await foreach
// Rows come from ODX_SYN_ROWS (default 1,100,10000), as the synthetic
// suite does. Filters: *PoolBinCostBenchmarks.Buffer_*  *PoolBinCostBenchmarks.Stream_*
//
// This file is the AFTER arm. The BEFORE arm is the same file compiled
// against master f6942a7 with the overloads that took the flag
// (ToPooledBuffer(clearOnDispose: false, ct) and
// ToPooledStream(clearOnReturn: false, ct)).
// BytesPerCall and the work-item counts are measured by the PoolBinCostProbe
// test; here the MemoryDiagnoser only confirms the bytes (the
// ThreadingDiagnoser is refused by --inprocess, which bench-guard uses).
[MemoryDiagnoser]
public class PoolBinCostBenchmarks
{
	public sealed class BinRow
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	[ParamsSource(nameof(RowsValues))]
	public int Rows { get; set; }

	public static IEnumerable<int> RowsValues()
	{
		string? value = Environment.GetEnvironmentVariable("ODX_SYN_ROWS");
		return string.IsNullOrWhiteSpace(value)
			? [1, 100, 10_000]
			: value.Split(',').Select(static s => int.Parse(s.Trim()));
	}

	SqliteConnection _connection = null!;
	string _sql = null!;

	[GlobalSetup]
	public void Setup()
	{
		_connection = new SqliteConnection("Data Source=:memory:");
		_connection.Open();
		_sql = $"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {Rows}) SELECT x AS Id, 'name' || x AS Name FROM c";
	}

	[GlobalCleanup]
	public void Cleanup() => _connection.Dispose();

	[Benchmark]
	public async Task<int> Buffer_Items()
	{
		using PooledArray<BinRow> items
			= await _connection.Command(_sql).Map<BinRow>()
				.ToPooledBuffer(CancellationToken.None)
				.Items;

		return items.Count;
	}

	[Benchmark]
	public async Task<int> Stream_Buffered()
	{
		int count = 0;
		await foreach (BinRow _ in _connection.Command(_sql).Map<BinRow>()
			.ToPooledStream(CancellationToken.None))
		{
			++count;
		}

		return count;
	}
}
