using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Benchmarks;

// The collecting and the buffer terminals end to end, on SQLite in memory
// (no network latency), class rows (an int and a string): the same file is
// compiled against two commits of the library to compare them (it uses the
// public terminals that both have). Rows come from ODX_SYN_ROWS (default
// 100 and 10,000). Filter: *PoolEndToEndBenchmarks*
[MemoryDiagnoser]
public class PoolEndToEndBenchmarks
{
	public sealed class EndRow
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
			? [100, 10_000]
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
	public async Task<int> ToListAsync()
	{
		List<EndRow> rows = await _connection.Command(_sql).Map<EndRow>()
			.ToListAsync(CancellationToken.None);

		return rows.Count;
	}

	[Benchmark]
	public async Task<int> ToArrayAsync()
	{
		EndRow[] rows = await _connection.Command(_sql).Map<EndRow>()
			.ToArrayAsync(CancellationToken.None);

		return rows.Length;
	}

	[Benchmark]
	public async Task<int> ToPooledBuffer_Items()
	{
		using PooledArray<EndRow> items
			= await _connection.Command(_sql).Map<EndRow>()
				.ToPooledBuffer(CancellationToken.None)
				.Items;

		return items.Count;
	}

	[Benchmark]
	public async Task<int> ToPooledBufferStream()
	{
		int count = 0;
		await foreach (ReadOnlyMemory<EndRow> chunk in _connection.Command(_sql).Map<EndRow>()
			.ToPooledBufferStream(CancellationToken.None))
		{
			count += chunk.Length;
		}

		return count;
	}
}
