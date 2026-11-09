using System.Buffers;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A Dataflow call waits for room in the target and stops only when the target declines because it is complete. The synchronous overloads wait too (they used to
// Post, which refuses a bounded target that is merely full, and so cut the rows short).
[ExcludeFromCodeCoverage]
public static class ToTargetBlockFullTargetTests
{
	sealed class Row
	{
		public int Id { get; set; }
	}

	static readonly RowMap<Row> Overrides = new([new("Id", "Id")]);

	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	delegate long SyncCall<T>(DbTally tally, ITargetBlock<T> target, bool complete);

	static DbTally Tally(int rows)
		=> new(new DbScript { Rows = [.. Enumerable.Range(1, rows).Select(i => (object?[])[i, "n" + i])] });

	static TypedReader Reader(DbTally tally) => TypedReader.Create(tally.Script.Columns, tally.Script.Rows);

	// The connection is closed in every case, so a command level call has to open it.
	static DbCommand Db(DbTally tally) => tally.NewConnection().CreateCommand();

	static ExpressiveDbCommand Expressive(DbTally tally) => tally.NewConnection().Command("select 1");

	// Every synchronous overload that reads a reader or a command into a target block.
	static readonly Dictionary<string, SyncCall<object[]>> SyncArrayCalls = new()
	{
		["reader"] = (t, w, c) => Reader(t).ToTargetBlock(w, c),
		["reader, pool"] = (t, w, c) => Reader(t).ToTargetBlock(w, c, ArrayPool<object>.Shared),
		["IDbCommand"] = (t, w, c) => Db(t).ToTargetBlock(w, c),
		["IDbCommand, pool"] = (t, w, c) => Db(t).ToTargetBlock(w, c, ArrayPool<object>.Shared),
		["IExecuteReader"] = (t, w, c) => Expressive(t).ToTargetBlock(w, c),
		["IExecuteReader, pool"] = (t, w, c) => Expressive(t).ToTargetBlock(w, c, ArrayPool<object>.Shared),
	};

	static readonly Dictionary<string, SyncCall<Row>> SyncRowCalls = new()
	{
		["reader, transform"] = (t, w, c) => Reader(t).ToTargetBlock(w, c, r => new Row { Id = r.GetInt32(0) }),
		["reader, mapped"] = (t, w, c) => Reader(t).ToTargetBlock<Row>(w, c),
		["reader, mapped with map"] = (t, w, c) => Reader(t).ToTargetBlock(w, c, Overrides),
		["IDbCommand, transform"] = (t, w, c) => Db(t).ToTargetBlock(w, c, r => new Row { Id = r.GetInt32(0) }),
		["IDbCommand, mapped"] = (t, w, c) => Db(t).ToTargetBlock<Row>(w, c),
		["IDbCommand, mapped with map"] = (t, w, c) => Db(t).ToTargetBlock(w, c, Overrides),
		["IExecuteReader, transform"] = (t, w, c) => Expressive(t).ToTargetBlock(w, c, r => new Row { Id = r.GetInt32(0) }),
		["IExecuteReader, mapped"] = (t, w, c) => Expressive(t).ToTargetBlock<Row>(w, c),
		["IExecuteReader, mapped with map"] = (t, w, c) => Expressive(t).ToTargetBlock(w, c, Overrides),
	};

	public static TheoryData<string> SyncArrayNames() => Names(SyncArrayCalls.Keys);

	public static TheoryData<string> SyncRowNames() => Names(SyncRowCalls.Keys);

	static TheoryData<string> Names(IEnumerable<string> names)
	{
		var data = new TheoryData<string>();
		foreach (string name in names) data.Add(name);
		return data;
	}

	// A bounded target (capacity one) whose consumer is parked until the call has had its chance to read ahead and give up: every row must still arrive.
	static async Task BoundedSlowTarget_ReceivesEveryRow<T>(SyncCall<T> call)
	{
		const int rows = 3000; // More than the 1024 rows the internal buffer of a mapped pipeline holds.
		var tally = Tally(rows);
		var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		int received = 0;
		var target = new ActionBlock<T>(
			async _ =>
			{
				await gate.Task.ConfigureAwait(false);
				Interlocked.Increment(ref received);
			},
			new ExecutionDataflowBlockOptions { BoundedCapacity = 1 });

		Task<long> running = Task.Run(() => call(tally, target, true), Ct);
		await Task.WhenAny(running, Task.Delay(200, Ct)); // A call that gives up on the full target ends now; one that waits for room is still going.
		gate.SetResult(true);

		Assert.Equal(rows, await running.WaitAsync(Timeout, Ct));
		await target.Completion.WaitAsync(Timeout, Ct);
		Assert.Equal(rows, received);
	}

	[Theory]
	[MemberData(nameof(SyncArrayNames))]
	public static Task Sync_Array_BoundedSlowTarget_ReceivesEveryRow(string call)
		=> BoundedSlowTarget_ReceivesEveryRow(SyncArrayCalls[call]);

	[Theory]
	[MemberData(nameof(SyncRowNames))]
	public static Task Sync_Rows_BoundedSlowTarget_ReceivesEveryRow(string call)
		=> BoundedSlowTarget_ReceivesEveryRow(SyncRowCalls[call]);
}
