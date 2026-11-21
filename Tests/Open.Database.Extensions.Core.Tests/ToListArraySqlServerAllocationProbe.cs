using Microsoft.Data.SqlClient;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Bytes per call of ToListAsync and ToArrayAsync on a typed view over SQL
// Server LocalDB. Unlike SQLite this really suspends (the server answers
// over a pipe), so a thread-local counter would miss the continuations:
// the whole process is counted instead, GC.GetTotalAllocatedBytes(precise:
// true), around a sequential awaited loop. Run it alone in an idle process
// (dotnet test --filter) for the numbers to mean anything.
[ExcludeFromCodeCoverage]
[Collection("Heap measurements")]
public sealed class ToListArraySqlServerAllocationProbe(ITestOutputHelper output)
{
	public sealed class ClassRow
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public long Score { get; set; }
	}

	const int Calls = 100;
	const int Rounds = 7;

	static async Task<long[]> Measure(Func<Task> call)
	{
		for (int i = 0; i < 50; ++i) await call();

		long[] rounds = new long[Rounds];
		for (int r = 0; r < Rounds; ++r)
		{
			GC.Collect();
			GC.WaitForPendingFinalizers();
			long before = GC.GetTotalAllocatedBytes(precise: true);
			for (int i = 0; i < Calls; ++i) await call();
			rounds[r] = (GC.GetTotalAllocatedBytes(precise: true) - before) / Calls;
		}

		return rounds;
	}

	[Fact(Explicit = true)]
	public async Task Probe_BytesPerCall()
	{
		LocalDbFixture.SkipUnlessAvailable();

		string table = LocalDbFixture.UniqueName("ode_alloc");
		LocalDbFixture.Execute(
			$"create table dbo.[{table}] (Id int not null, Name nvarchar(20) not null, Score bigint not null)");
		using var scratch = new LocalDbFixture.Scratch("table", table);
		LocalDbFixture.Execute(
			$"insert into dbo.[{table}] select top (100) row_number() over (order by (select 1)), 'name', 2 * row_number() over (order by (select 1)) from sys.all_objects a cross join sys.all_columns b");

		await using SqlConnection connection = LocalDbFixture.Create();
		await connection.OpenAsync(TestContext.Current.CancellationToken);

		var lines = new List<string>();
		foreach (int n in new[] { 1, 100 })
		{
			string sql = $"select top ({n}) Id, Name, Score from dbo.[{table}] order by Id";
			long[] list = await Measure(async () =>
				await connection.Command(sql).Map<ClassRow>().ToListAsync());
			long[] array = await Measure(async () =>
				await connection.Command(sql).Map<ClassRow>().ToArrayAsync());
			lines.Add($"N={n}: ToListAsync min {list.Min()} (rounds {string.Join(",", list)}); ToArrayAsync min {array.Min()} (rounds {string.Join(",", array)})");
		}

		foreach (string line in lines) output.WriteLine(line);
	}
}
