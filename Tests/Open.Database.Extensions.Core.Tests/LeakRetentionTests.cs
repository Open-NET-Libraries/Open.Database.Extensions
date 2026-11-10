using System.Collections;
using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// What stays alive after a call. Three kinds of proof:
//   - weak references to the command, the reader, the connection, the connection factory and a mapped model must be dead after a collection, so no static
//     or cache keeps anything from a call alive;
//   - the caches that have a bound (the row-map layouts, the remembered procedure names) are driven past it, and stay at it;
//   - a few thousand calls of every terminal must not grow the heap: the live memory after the calls, measured with a full collection, is compared with
//     the live memory after a warm-up. The bound is a small constant, far below what a leak of even a hundred bytes per call would add.
// The heap is measured for the whole process, so the tests that measure it run alone: no other test class runs while they do.
[CollectionDefinition("Heap measurements", DisableParallelization = true)]
public sealed class HeapMeasurementsCollection;

[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class LeakRetentionTests
{
	public sealed class Row
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	static void Collect()
	{
		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();
	}

	static readonly object?[][] TwoRows = [[1, "a"], [2, "b"]];

	// ---- weak references ----

	[MethodImpl(MethodImplOptions.NoInlining)]
	static async Task<WeakReference[]> CallAndTrack(string terminal, LeakSource source, CancellationToken token)
	{
		var tally = new DbTally(new() { Rows = TwoRows });
		TrackedFactory? factory = null;
		TrackedPool pool = tally.Pool;
		ExpressiveDbCommand command;
		if (source == LeakSource.Factory)
		{
			factory = tally.Factory;
			command = factory.Command("select 1");
		}
		else
		{
			command = tally.Command(source, pool);
		}

		object? model = null;
		switch (terminal)
		{
			case "ToList":
				model = command.Map<Row>().ToList()[0];
				break;
			case "ToListAsync":
				model = (await command.Map<Row>().ToListAsync(token))[0];
				break;
			case "ResultsAsync":
				model = (await command.ResultsAsync<Row>()).First();
				break;
			case "ToChannelAsync":
				ChannelReader<Row> channel = command.ToChannelAsync<Row>(true, cancellationToken: token);
				model = await channel.ReadAsync(token);
				break;
			default:
				command.ExecuteScalar();
				break;
		}

		var tracked = new List<WeakReference> { new(tally), new(command), new(tally.Commands[0]), new(tally.Connections[0]) };
		if (tally.Readers.Count > 0) tracked.Add(new(tally.Readers[0]));
		if (factory is not null) tracked.Add(new(factory));
		if (model is not null) tracked.Add(new(model));
		return [.. tracked];
	}

	[Theory]
	[InlineData("ToList", LeakSource.Factory)]
	[InlineData("ToList", LeakSource.BareClosed)]
	[InlineData("ToList", LeakSource.BareOpen)]
	[InlineData("ToListAsync", LeakSource.Factory)]
	[InlineData("ToListAsync", LeakSource.BareClosed)]
	[InlineData("ToListAsync", LeakSource.BareOpen)]
	[InlineData("ResultsAsync", LeakSource.Factory)]
	[InlineData("ToChannelAsync", LeakSource.Factory)]
	[InlineData("ToChannelAsync", LeakSource.BareClosed)]
	[InlineData("ExecuteScalar", LeakSource.Factory)]
	[InlineData("ExecuteScalar", LeakSource.BareOpen)]
	public static async Task AfterTheCall_NothingFromItIsStillAlive(string terminal, LeakSource source)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		WeakReference[] tracked = await CallAndTrack(terminal, source, token);

		// The pipelines finish their last steps on pool threads, so collect until nothing is left: whatever is truly rooted never goes, and the wait ends with a failure.
		DateTime until = DateTime.UtcNow + TimeSpan.FromSeconds(20);
		while (true)
		{
			Collect();
			if (!tracked.Any(r => r.IsAlive)) return;
			if (DateTime.UtcNow > until) break;
			await Task.Delay(20, token);
		}

		Assert.All(tracked, r => Assert.False(r.IsAlive, "Still alive: " + r.Target?.GetType().Name));
	}

	// ---- the row-map layout cache ----

	sealed class LayoutRow
	{
		public int Id { get; set; }
	}

	// One RowMap remembers a reader per column layout and starts over when it holds 128 layouts (a database that returns a different column list
	// for every query would otherwise add one entry per query, for ever). Read through its field: nothing public shows it.
	[Fact]
	public static void RowMapLayoutCache_StaysBounded_With300DistinctLayouts()
	{
		var map = new RowMap<LayoutRow>(ImmutableArray<KeyValuePair<string, string?>>.Empty);
		FieldInfo layouts = typeof(RowMap<LayoutRow>).GetField("_layouts", BindingFlags.Instance | BindingFlags.NonPublic)
			?? throw new InvalidOperationException("RowMap<T> no longer has the _layouts field this test reads.");

		int largest = 0;
		for (int i = 0; i < 300; i++)
		{
			var tally = new DbTally(new() { Columns = [("id", typeof(int)), ("column" + i, typeof(string))], Rows = [[1, "x"]] });
			Assert.Single(tally.NewOpenConnection().Command("select 1").Map(map).ToList());
			largest = Math.Max(largest, ((ICollection?)layouts.GetValue(map))?.Count ?? 0);
		}

		Assert.InRange(largest, 1, 128);
		Assert.True(((ICollection)layouts.GetValue(map)!).Count < 300);
	}

	// ---- the remembered procedure names ----

	// A command whose provider "knows" the declared parameters of every procedure: the one the call uses, and a name that is different for every procedure.
	sealed class ProcCommand(IDbConnectionFactory<DbConnection> factory, string name, List<WeakReference> uniqueNames)
		: ExpressiveDbCommand(factory, CommandType.StoredProcedure, name)
	{
		protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure)
		{
			string unique = new string('x', 8) + procedure; // a new string, so a weak reference to it says whether the remembered names are still held
			uniqueNames.Add(new WeakReference(unique));
			return ["@CompanySerialNum", unique];
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static List<WeakReference> RunProcedures(TrackedFactory factory, int count)
	{
		var uniqueNames = new List<WeakReference>(count);
		const int companySerialNum = 7;
		for (int i = 0; i < count; i++)
			new ProcCommand(factory, "dbo.proc" + i, uniqueNames).AddParam(companySerialNum).ExecuteNonQuery();

		return uniqueNames;
	}

	// The names of a procedure are remembered for five minutes, per connection factory. Before a bound, a factory that was used with 10,000 different
	// procedure names kept all 10,000 until it was collected: an entry expired but was only dropped when the same procedure was read again.
	[Fact]
	public static void ProcedureNames_StayBounded_With10000DistinctProcedures()
	{
		var tally = new DbTally();
		TrackedFactory factory = tally.Factory;

		List<WeakReference> uniqueNames = RunProcedures(factory, 10_000);
		Collect();

		int remembered = uniqueNames.Count(r => r.IsAlive);
		Assert.Equal(10_000, uniqueNames.Count);
		Assert.InRange(remembered, 1, 1024);
		GC.KeepAlive(factory);
	}

	// ---- memory over a few thousand calls ----

	const int WarmUp = 300;
	const int Calls = 3000;
	const int Rounds = 6;

	// The growth allowed in each of the last rounds of calls, in bytes. Measured: 0 for nearly every terminal and round, and swings of up to about 13 KB
	// (no trend) for the dataflow block. A call that keeps one object, of at least 24 bytes, adds 72 KB a round.
	// (A zero bound would be unfair to the garbage collector: a round may happen to end a few KB higher.)
	const long MaxGrowth = 65_536;

	static async Task Once(string terminal, CancellationTokenSource longLived)
	{
		var tally = new DbTally(new() { Rows = TwoRows, FailOnRead = terminal.EndsWith("Fails", StringComparison.Ordinal) ? 2 : 0 });
		ExpressiveDbCommand command = tally.Factory.Command("select 1");
		CancellationToken none = CancellationToken.None;

		try
		{
			switch (terminal)
			{
				case "ToList": command.Map<Row>().ToList(); break;
				case "ToListAsync": await command.Map<Row>().ToListAsync(none); break;
				case "ToArrayAsync": await command.Map<Row>().ToArrayAsync(none); break;
				case "ToDictionaryAsync": await command.Map<Row>().ToDictionaryAsync(r => r.Id, cancellationToken: none); break;
				case "ReadAsyncAbandon":
					await command.Map<Row>().ReadAsync<Row, bool>(async seq =>
					{
						await using IAsyncEnumerator<Row> e = seq.GetAsyncEnumerator(none);
						return await e.MoveNextAsync();
					}, none);
					break;
				case "ExecuteScalar": command.ExecuteScalar(); break;
				case "ExecuteNonQueryAsync": await command.ExecuteNonQueryAsync(); break;
				case "Retrieve": command.Retrieve(); break;
				case "ToListAsyncFails": await command.Map<Row>().ToListAsync(none); break;
				case "ToArrayAsyncFails": await command.Map<Row>().ToArrayAsync(none); break;
				case "ToListAsyncLinkedToken":
					// The command has a long-lived token and the call another: the call links them, and must unlink them when it ends.
					using (var call = new CancellationTokenSource())
						await command.UseCancellationToken(longLived.Token).Map<Row>().ToListAsync(call.Token);
					break;
				case "ToChannelAsync":
					ChannelReader<Row> channel = command.ToChannelAsync<Row>(true, cancellationToken: token);
					await channel.ReadAllAsync(none).ToListAsync(none);
					break;
				case "ToTargetBlockAsync":
					var block = new BufferBlock<Row>();
					await command.ToTargetBlockAsync<Row>(block, true);
					block.TryReceiveAll(out _);
					break;
				default: throw new ArgumentOutOfRangeException(nameof(terminal), terminal, null);
			}
		}
		catch (InvalidOperationException)
		{
			// The "Fails" terminals fail on purpose.
		}
	}

	[Theory]
	[InlineData("ToList")]
	[InlineData("ToListAsync")]
	[InlineData("ToArrayAsync")]
	[InlineData("ToDictionaryAsync")]
	[InlineData("ReadAsyncAbandon")]
	[InlineData("ExecuteScalar")]
	[InlineData("ExecuteNonQueryAsync")]
	[InlineData("Retrieve")]
	[InlineData("ToListAsyncFails")]
	[InlineData("ToArrayAsyncFails")]
	[InlineData("ToListAsyncLinkedToken")]
	[InlineData("ToChannelAsync")]
	[InlineData("ToTargetBlockAsync")]
	public static async Task ManyCalls_TheHeapDoesNotGrow(string terminal)
	{
		using var longLived = new CancellationTokenSource();

		for (int i = 0; i < WarmUp; i++) await Once(terminal, longLived);
		long last = Settled();

		// Rounds of calls, each measured against the one before. What a warm-up leaves behind (lazy statics, the pool threads, a pooled buffer growing to its size)
		// shows in the first rounds and then stops; a leak shows in every round.
		long[] growth = new long[Rounds];
		for (int round = 0; round < Rounds; round++)
		{
			for (int i = 0; i < Calls; i++) await Once(terminal, longLived);
			long now = Settled();
			growth[round] = now - last;
			last = now;
		}

		long worst = growth.Skip(Rounds / 2).Max();
		Assert.True(worst <= MaxGrowth, $"Actual: {terminal}: bytes grown per round of {Calls} calls: {string.Join(", ", growth)}.");
	}

	static long Settled()
	{
		Collect();
		return GC.GetTotalMemory(true);
	}
}
