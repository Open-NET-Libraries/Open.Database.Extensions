using Microsoft.Data.Sqlite;
using Xunit.Sdk;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Bytes allocated per call by ToListAsync and ToArrayAsync on a typed view
// (Command(sql).Map<T>()) over SQLite in memory, for 1, 10, 100 and 10,000
// rows, with a class row and a struct row. The same source runs on master
// and on the branch: the numbers are compared, and the branch must not
// allocate more than master does for a small result.
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

	// The recorded results of master (f6942a7) on these probes, per target
	// framework, in the order ClassList, ClassArray, StructList, StructArray
	// for N = 1, 10, 100. The branch must stay at or under them plus Slack.
#if !DEBUG
	const long Slack = 64;

	// bytes per call, [N index][ClassList, ClassArray, StructList, StructArray]
#if NET11_0_OR_GREATER
	static readonly long[][] Master = [[1424, 1368, 1416, 1312], [2320, 2096, 2040, 1528], [11392, 10024, 7488, 3688]];
#else
	static readonly long[][] Master = [[1480, 1424, 1472, 1368], [2376, 2152, 2096, 1584], [11448, 10080, 7544, 3744]];
#endif
#endif

	[Fact]
	public void Probe_BytesPerCall()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();

		var lines = new List<string>
		{
			"N      | class ToListAsync | class ToArrayAsync | struct ToListAsync | struct ToArrayAsync",
		};
		long[][] got = new long[Sizes.Length][];
		for (int i = 0; i < Sizes.Length; ++i)
		{
			int n = Sizes[i];
			got[i] =
			[
				ListBytes<ClassRow>(connection, ClassSql(n)),
				ArrayBytes<ClassRow>(connection, ClassSql(n)),
				ListBytes<StructRow>(connection, StructSql(n)),
				ArrayBytes<StructRow>(connection, StructSql(n)),
			];
			lines.Add($"{n,-6} | {got[i][0],17} | {got[i][1],18} | {got[i][2],18} | {got[i][3],19}");
		}

		lines.Add("per row (N=10000 vs N=100): "
			+ string.Join(", ", Enumerable.Range(0, 4).Select(k => ((got[3][k] - got[2][k]) / 9900.0).ToString("F1"))));
		foreach (string line in lines) output.WriteLine(line);

		// The branch must not allocate more than master for a small result.
		// (Master was recorded in Release; a Debug build allocates a class per
		// async state machine, so the check is for Release only.)
#if !DEBUG
		for (int i = 0; i < 3; ++i)
		{
			for (int k = 0; k < 4; ++k)
			{
				Assert.True(
					got[i][k] <= Master[i][k] + Slack,
					$"N={Sizes[i]} column {k}: {got[i][k]} bytes, master {Master[i][k]} (+{Slack})");
			}
		}
#endif
	}
}
