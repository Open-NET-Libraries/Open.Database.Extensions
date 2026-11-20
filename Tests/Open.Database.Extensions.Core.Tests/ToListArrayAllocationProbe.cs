using Microsoft.Data.Sqlite;
using Xunit.Sdk;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Bytes allocated per call by ToListAsync, ToArrayAsync, the buffered queue
// (ResultsBufferedAsync), ToHashSetAsync and ToDictionaryAsync (both forms) on SQLite in memory, for 1, 10,
// 100 and 10,000 rows, with a class row and a struct row. The same source
// runs on master and on the branch: the numbers are compared, and the
// branch must not allocate more than master does for a small result.
//
// Method: the calls complete on the test thread (SQLite answers
// synchronously, the library never hops), so GC.GetAllocatedBytesForCurrent
// Thread around GetAwaiter().GetResult() sees every byte. Warm-up 50 calls,
// then the minimum of 3 rounds of 20 calls, in bytes per call.
[ExcludeFromCodeCoverage]
[Collection("ChunkSizing")]
public sealed class ToListArrayAllocationProbe(ITestOutputHelper output)
{
	public sealed class ClassRow
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public long Score { get; set; }
	}

	public struct StructRow
	{
		public int Id { get; set; }
		public long Score { get; set; }
		public double Ratio { get; set; }
	}

	static readonly int[] Sizes = [1, 10, 100, 10_000];

	static readonly string[] Names =
	[
		"class ToList", "class ToArray", "struct ToList", "struct ToArray",
		"class Queue", "struct Queue", "class HashSet", "struct HashSet",
		"class Dict", "struct Dict", "class DictKV", "struct DictKV",
	];

	static string ClassSql(int n)
		=> $"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) SELECT x AS Id, 'name' || x AS Name, x * 2 AS Score FROM c";

	static string StructSql(int n)
		=> $"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) SELECT x AS Id, x * 2 AS Score, x / 3.0 AS Ratio FROM c";

	static long BytesPerCall(Func<object> call)
	{
		for (int i = 0; i < 50; ++i) call();

		long best = long.MaxValue;
		for (int round = 0; round < 3; ++round)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 20; ++i) call();
			long used = GC.GetAllocatedBytesForCurrentThread() - before;
			if (used < best) best = used;
		}

		return best / 20;
	}

	static long ListBytes<T>(SqliteConnection connection, string sql)
		=> BytesPerCall(() => connection.Command(sql).Map<T>().ToListAsync().AsTask().GetAwaiter().GetResult());

	static long ArrayBytes<T>(SqliteConnection connection, string sql)
		=> BytesPerCall(() => connection.Command(sql).Map<T>().ToArrayAsync().AsTask().GetAwaiter().GetResult());

	static long HashSetBytes<T>(SqliteConnection connection, string sql)
		=> BytesPerCall(() => connection.Command(sql).Map<T>().ToHashSetAsync().AsTask().GetAwaiter().GetResult());

	static long DictBytes<T>(SqliteConnection connection, string sql, Func<T, int> key)
		=> BytesPerCall(() => connection.Command(sql).Map<T>().ToDictionaryAsync(key).AsTask().GetAwaiter().GetResult());

	static long DictValueBytes<T>(SqliteConnection connection, string sql, Func<T, int> key, Func<T, long> value)
		=> BytesPerCall(() => connection.Command(sql).Map<T>().ToDictionaryAsync(key, value).AsTask().GetAwaiter().GetResult());

	// The queue is built when the call returns; the enumerable it hands
	// back is not enumerated.
	static long QueueBytes<T>(SqliteConnection connection, string sql)
		=> BytesPerCall(() =>
		{
			using SqliteCommand command = connection.CreateCommand();
			command.CommandText = sql;
			using SqliteDataReader reader = command.ExecuteReader();
			return reader.ResultsBufferedAsync<T>(null, true, default).AsTask().GetAwaiter().GetResult();
		});

#if !DEBUG
	// The recorded results of master (f6942a7) on these probes, per target
	// framework, in the order of Names for N = 1, 10, 100. The branch must stay at or under
	// them plus Slack.
	const long Slack = 64;

#if NET11_0_OR_GREATER
	static readonly long[][] Master = [[1424, 1368, 1416, 1312, 1272, 1280, 1512, 1528, 1552, 1528, 1552, 1480], [2320, 2096, 2040, 1528, 2168, 1904, 2752, 2856, 2984, 2688, 2984, 2256], [11392, 10024, 7488, 3688, 11240, 7352, 16576, 18240, 19392, 17056, 19392, 11456]];
#else
	static readonly long[][] Master = [[1480, 1424, 1472, 1368, 1328, 1336, 1568, 1584, 1608, 1584, 1608, 1536], [2376, 2152, 2096, 1584, 2224, 1960, 2808, 2912, 3040, 2744, 3040, 2312], [11448, 10080, 7544, 3744, 11296, 7408, 16632, 18296, 19448, 17112, 19448, 11512]];
#endif
#endif

	[Fact]
	public void Probe_BytesPerCall()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();

		var lines = new List<string>
		{
			"N      | " + string.Join(" | ", Names),
		};
		long[][] got = new long[Sizes.Length][];
		for (int i = 0; i < Sizes.Length; ++i)
		{
			int n = Sizes[i];
			string cls = ClassSql(n);
			string str = StructSql(n);
			got[i] =
			[
				ListBytes<ClassRow>(connection, cls),
				ArrayBytes<ClassRow>(connection, cls),
				ListBytes<StructRow>(connection, str),
				ArrayBytes<StructRow>(connection, str),
				QueueBytes<ClassRow>(connection, cls),
				QueueBytes<StructRow>(connection, str),
				HashSetBytes<ClassRow>(connection, cls),
				HashSetBytes<StructRow>(connection, str),
				DictBytes<ClassRow>(connection, cls, static r => r.Id),
				DictBytes<StructRow>(connection, str, static r => r.Id),
				DictValueBytes<ClassRow>(connection, cls, static r => r.Id, static r => r.Score),
				DictValueBytes<StructRow>(connection, str, static r => r.Id, static r => r.Score),
			];
			lines.Add($"{n,-6} | " + string.Join(" | ", got[i]));
		}

		lines.Add("per row (N=10000 vs N=100): "
			+ string.Join(", ", Enumerable.Range(0, Names.Length).Select(k => ((got[3][k] - got[2][k]) / 9900.0).ToString("F1"))));
		foreach (string line in lines) output.WriteLine(line);

		// The branch must not allocate more than master for a small result.
		// (Master was recorded in Release; a Debug build allocates a class per
		// async state machine, so the check is for Release only.)
#if !DEBUG
		for (int i = 0; i < 3; ++i)
		{
			for (int k = 0; k < Names.Length; ++k)
			{
				Assert.True(
					got[i][k] <= Master[i][k] + Slack,
					$"N={Sizes[i]} {Names[k]}: {got[i][k]} bytes, master {Master[i][k]} (+{Slack})");
			}
		}
#endif
	}
}
