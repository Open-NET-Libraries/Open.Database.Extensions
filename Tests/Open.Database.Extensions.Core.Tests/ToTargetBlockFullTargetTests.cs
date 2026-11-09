using System.Buffers;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A Dataflow call waits for room in the target and stops only when the target declines because it is complete. The synchronous overloads wait too (they used to
// Post, which refuses a bounded target that is merely full, and so cut the rows short). A command level call raises an error that follows the rows,
// unless the target declined: then the rest of the result set is not read.
[ExcludeFromCodeCoverage]
public static class ToTargetBlockFullTargetTests
{
	sealed class Row
	{
		public int Id { get; set; }
	}

	sealed class LateException() : Exception("late");

	static readonly RowMap<Row> Overrides = new([new("Id", "Id")]);

	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	delegate long SyncCall<T>(DbTally tally, ITargetBlock<T> target, bool complete);

	delegate ValueTask<long> Call<T>(DbTally tally, ITargetBlock<T> target, bool complete);

	static DbTally Tally(int rows, Exception? lateError = null)
		=> new(new DbScript { Rows = [.. Enumerable.Range(1, rows).Select(i => (object?[])[i, "n" + i])], NextResultError = lateError });

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

	// Every command level overload, synchronous and asynchronous.
	static readonly Dictionary<string, Call<object[]>> CommandArrayCalls = new()
	{
		["IDbCommand"] = (t, w, c) => ValueTask.FromResult(Db(t).ToTargetBlock(w, c)),
		["IDbCommand, pool"] = (t, w, c) => ValueTask.FromResult(Db(t).ToTargetBlock(w, c, ArrayPool<object>.Shared)),
		["IExecuteReader"] = (t, w, c) => ValueTask.FromResult(Expressive(t).ToTargetBlock(w, c)),
		["IExecuteReader, pool"] = (t, w, c) => ValueTask.FromResult(Expressive(t).ToTargetBlock(w, c, ArrayPool<object>.Shared)),
		["IDbCommand, async"] = (t, w, c) => Db(t).ToTargetBlockAsync(w, c),
		["IDbCommand, pool, async"] = (t, w, c) => Db(t).ToTargetBlockAsync(w, c, ArrayPool<object>.Shared),
		["IExecuteReader, async"] = (t, w, c) => Expressive(t).ToTargetBlockAsync(w, c),
		["IExecuteReader, pool, async"] = (t, w, c) => Expressive(t).ToTargetBlockAsync(w, c, ArrayPool<object>.Shared),
	};

	static readonly Dictionary<string, Call<Row>> CommandRowCalls = new()
	{
		["IDbCommand, transform"] = (t, w, c) => ValueTask.FromResult(Db(t).ToTargetBlock(w, c, r => new Row { Id = r.GetInt32(0) })),
		["IDbCommand, mapped"] = (t, w, c) => ValueTask.FromResult(Db(t).ToTargetBlock<Row>(w, c)),
		["IDbCommand, mapped with map"] = (t, w, c) => ValueTask.FromResult(Db(t).ToTargetBlock(w, c, Overrides)),
		["IExecuteReader, transform"] = (t, w, c) => ValueTask.FromResult(Expressive(t).ToTargetBlock(w, c, r => new Row { Id = r.GetInt32(0) })),
		["IExecuteReader, mapped"] = (t, w, c) => ValueTask.FromResult(Expressive(t).ToTargetBlock<Row>(w, c)),
		["IExecuteReader, mapped with map"] = (t, w, c) => ValueTask.FromResult(Expressive(t).ToTargetBlock(w, c, Overrides)),
		["IDbCommand, transform, async"] = (t, w, c) => Db(t).ToTargetBlockAsync(w, c, r => new Row { Id = r.GetInt32(0) }),
		["IDbCommand, mapped, async"] = (t, w, c) => Db(t).ToTargetBlockAsync<Row>(w, c),
		["IDbCommand, mapped with map, async"] = (t, w, c) => Db(t).ToTargetBlockAsync(w, c, Overrides),
		["IExecuteReader, transform, async"] = (t, w, c) => Expressive(t).ToTargetBlockAsync(w, c, r => new Row { Id = r.GetInt32(0) }),
		["IExecuteReader, mapped, async"] = (t, w, c) => Expressive(t).ToTargetBlockAsync<Row>(w, c),
		["IExecuteReader, mapped with map, async"] = (t, w, c) => Expressive(t).ToTargetBlockAsync(w, c, Overrides),
	};

	public static TheoryData<string> SyncArrayNames() => Names(SyncArrayCalls.Keys);

	public static TheoryData<string> SyncRowNames() => Names(SyncRowCalls.Keys);

	public static TheoryData<string> CommandArrayNames() => Names(CommandArrayCalls.Keys);

	public static TheoryData<string> CommandRowNames() => Names(CommandRowCalls.Keys);

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

	// An error that follows the rows reaches the caller of a command level call, and with complete true faults the target.
	static async Task LateError_ReachesTheCaller<T>(Call<T> call)
	{
		var tally = Tally(2, new LateException());
		var target = new BufferBlock<T>();

		await Assert.ThrowsAnyAsync<LateException>(async () => await call(tally, target, true).AsTask().WaitAsync(Timeout, Ct));
		TrackedConnection connection = Assert.Single(tally.Connections);
		Assert.Equal(connection.Opens, connection.Closes);
		await Assert.ThrowsAnyAsync<LateException>(() => target.Completion.WaitAsync(Timeout, Ct));
	}

	[Theory]
	[MemberData(nameof(SyncArrayNames))]
	public static Task Sync_Array_BoundedSlowTarget_ReceivesEveryRow(string call)
		=> BoundedSlowTarget_ReceivesEveryRow(SyncArrayCalls[call]);

	[Theory]
	[MemberData(nameof(SyncRowNames))]
	public static Task Sync_Rows_BoundedSlowTarget_ReceivesEveryRow(string call)
		=> BoundedSlowTarget_ReceivesEveryRow(SyncRowCalls[call]);

	[Theory]
	[MemberData(nameof(CommandArrayNames))]
	public static Task Array_CommandLevel_LateError_ReachesTheCaller(string call)
		=> LateError_ReachesTheCaller(CommandArrayCalls[call]);

	[Theory]
	[MemberData(nameof(CommandRowNames))]
	public static Task Rows_CommandLevel_LateError_ReachesTheCaller(string call)
		=> LateError_ReachesTheCaller(CommandRowCalls[call]);

	// A target that declines because it is complete stops the read: the call returns the count, and the reader is not moved on (the rest of the result set is not read).
	[Theory]
	[MemberData(nameof(CommandArrayNames))]
	public static async Task Array_CommandLevel_ClosedTarget_StopsWithoutMovingTheReader(string call)
	{
		var tally = Tally(3, new LateException());
		var target = new BufferBlock<object[]>();
		target.Complete();

		Assert.Equal(0, await CommandArrayCalls[call](tally, target, false).AsTask().WaitAsync(Timeout, Ct));
		Assert.Equal(0, Assert.Single(tally.Readers).NextResultCalls);
	}
}
