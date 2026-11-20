using Microsoft.Data.Sqlite;

#nullable enable

namespace Open.Database.Extensions.Tests;

// What does the recycle bin hand-off cost, per call, against giving the
// arrays straight back? Rows that hold references are always cleared on
// the bin (off the read path); before that, with no clearing asked for,
// the arrays were disposed directly. This probe measures the buffer
// terminals that carry the difference, with CLASS rows (an int and a
// string) on SQLite in memory, for 1, 100 and 10,000 rows:
//   ToAsyncBuffer(...).Items, then dispose the items;
//   ToBufferedAsyncStream(...), consumed whole with await foreach.
//
// Method: the buffer runs a producer on the thread pool, and the bin runs
// its clears there too, so a thread-local counter would miss both. The
// bytes are GC.GetTotalAllocatedBytes(precise: true) around a sequential
// awaited loop in an otherwise idle process: 50 warm-up calls, then 7
// rounds of 100 calls with a forced collection before each round, the
// minimum of the rounds, in bytes per call. After each round the process
// waits until the thread pool is quiet (no pending work, no completed
// work item for a few polls), so the bin's trailing work is counted in
// the round that caused it. Thread-pool work items per call are the
// delta of ThreadPool.CompletedWorkItemCount over the same rounds.
//
// The same source builds in two trees: the branch (the AFTER arm, the
// current overloads) and master f6942a7 (the BEFORE arm: the overloads
// with clearOnDispose / clearOnReturn: false, defined by BEFORE_API).
// Run it ALONE in its process:
//   --filter FullyQualifiedName~PoolBinCostProbe
// The probe is Explicit (CI never runs it) and prints a table.
[ExcludeFromCodeCoverage]
[Collection("ChunkSizing")]
public sealed class PoolBinCostProbe(ITestOutputHelper output)
{
	public sealed class ClassRow
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	static readonly int[] Sizes = [1, 100, 10_000];

	const int Warmup = 50;
	const int Rounds = 7;
	const int CallsPerRound = 100;

	static string Sql(int n)
		=> $"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) SELECT x AS Id, 'name' || x AS Name FROM c";

	static async Task<int> BufferOnce(SqliteConnection connection, string sql)
	{
#if BEFORE_API
		using PooledArray<ClassRow> items
			= await connection.Command(sql).Map<ClassRow>()
				.ToAsyncBuffer(clearOnDispose: false, CancellationToken.None)
				.Items;
#else
		using PooledArray<ClassRow> items
			= await connection.Command(sql).Map<ClassRow>()
				.ToAsyncBuffer(CancellationToken.None)
				.Items;
#endif
		return items.Count;
	}

	static async Task<int> StreamOnce(SqliteConnection connection, string sql)
	{
		int count = 0;
#if BEFORE_API
		await foreach (ClassRow _ in connection.Command(sql).Map<ClassRow>()
			.ToBufferedAsyncStream(clearOnReturn: false, CancellationToken.None))
#else
		await foreach (ClassRow _ in connection.Command(sql).Map<ClassRow>()
			.ToBufferedAsyncStream(CancellationToken.None))
#endif
		{
			++count;
		}

		return count;
	}

	// Waits until nothing is queued and no work item has completed for
	// a few polls in a row (the bin's loop parks in an await when idle).
	static void Quiet()
	{
		long last = -1;
		int steady = 0;
		for (int i = 0; i < 400 && steady < 4; ++i)
		{
			Thread.Sleep(5);
			long done = ThreadPool.CompletedWorkItemCount;
			steady = done == last && ThreadPool.PendingWorkItemCount == 0
				? steady + 1
				: 0;
			last = done;
		}
	}

	static async Task<(long Bytes, double WorkItems)> Measure(
		Func<Task<int>> call,
		int expectedRows)
	{
		for (int i = 0; i < Warmup; ++i)
			Assert.Equal(expectedRows, await call());

		long bestBytes = long.MaxValue;
		long bestItems = long.MaxValue;
		for (int round = 0; round < Rounds; ++round)
		{
			GC.Collect();
			GC.WaitForPendingFinalizers();
			GC.Collect();
			Quiet();

			long items = ThreadPool.CompletedWorkItemCount;
			long bytes = GC.GetTotalAllocatedBytes(precise: true);
			for (int i = 0; i < CallsPerRound; ++i)
				await call();

			Quiet();
			bytes = GC.GetTotalAllocatedBytes(precise: true) - bytes;
			items = ThreadPool.CompletedWorkItemCount - items;
			if (bytes < bestBytes) bestBytes = bytes;
			if (items < bestItems) bestItems = items;
		}

		return (bestBytes / CallsPerRound, (double)bestItems / CallsPerRound);
	}

	[Fact(Explicit = true)]
	public async Task Probe_BytesAndWorkItemsPerCall()
	{
#if BEFORE_API
		const string Arm = "BEFORE (clear: false)";
#else
		const string Arm = "AFTER (references clear on the bin)";
#endif
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();

		// Off the xunit synchronization context, so only the library's own
		// hops are counted.
		var lines = await Task.Run(async () =>
		{
			var table = new List<string>
			{
				$"arm: {Arm}",
				"terminal | N      | bytes/call | work items/call",
			};
			foreach (int n in Sizes)
			{
				string sql = Sql(n);
				(long b, double w) = await Measure(
					() => BufferOnce(connection, sql),
					n);
				table.Add($"buffer   | {n,6} | {b,10} | {w,6:F2}");
			}

			foreach (int n in Sizes)
			{
				string sql = Sql(n);
				(long b, double w) = await Measure(
					() => StreamOnce(connection, sql),
					n);
				table.Add($"stream   | {n,6} | {b,10} | {w,6:F2}");
			}

			return table;
		});

		foreach (string line in lines)
			output.WriteLine(line);
	}
}
