using System.Buffers;
using System.Data.Common;
using BoundedChannel = System.Threading.Channels.Channel;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A command level ToChannel* call starts at once: it opens the connection (if needed), executes, reads, and each write waits for room in the target.
// A full target is only ever a reason to wait. The call ends when the rows run out, when it is cancelled, or when the target is closed
// (ChannelClosedException); and with complete true the target is completed however the call ends.
[ExcludeFromCodeCoverage]
public static class ToChannelFullTargetTests
{
	sealed class Row
	{
		public int Id { get; set; }
	}

	sealed class LateException() : Exception("late");

	static readonly RowMap<Row> Overrides = new([new("Id", "Id")]);

	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
	static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(200);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	delegate ValueTask<long> Call<T>(DbTally tally, ChannelWriter<T> target, bool complete, CancellationToken token);

	static DbTally Tally(int rows, Exception? lateError = null)
		=> new(new DbScript { Rows = [.. Enumerable.Range(1, rows).Select(i => (object?[])[i, "n" + i])], NextResultError = lateError });

	static ExpressiveDbCommand Expressive(DbTally tally, CancellationToken token)
	{
		ExpressiveDbCommand command = tally.NewConnection().Command("select 1");
		command.CancellationToken = token;
		return command;
	}

	// The connection is closed in every case, so a command level call has to open it.
	static DbCommand Db(DbTally tally) => tally.NewConnection().CreateCommand();

	static readonly Dictionary<string, Call<object[]>> ArrayCalls = new()
	{
		["IDbCommand"] = (t, w, complete, ct) => Db(t).ToChannel(w, complete, ct),
		["IDbCommand, pool"] = (t, w, complete, ct) => Db(t).ToChannel(w, complete, ArrayPool<object>.Shared, ct),
		["DbCommand"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, ct),
		["DbCommand, pool"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, ArrayPool<object>.Shared, ct),
		["IExecuteReader"] = (t, w, complete, ct) => Expressive(t, ct).ToChannel(w, complete),
		["IExecuteReader, pool"] = (t, w, complete, ct) => Expressive(t, ct).ToChannel(w, complete, ArrayPool<object>.Shared),
		["IExecuteReaderAsync"] = (t, w, complete, ct) => Expressive(t, ct).ToChannelAsync(w, complete),
		["IExecuteReaderAsync, pool"] = (t, w, complete, ct) => Expressive(t, ct).ToChannelAsync(w, complete, ArrayPool<object>.Shared),
	};

	static readonly Dictionary<string, Call<Row>> RowCalls = new()
	{
		["IDbCommand, transform"] = (t, w, complete, ct) => Db(t).ToChannel(w, complete, r => new Row { Id = r.GetInt32(0) }, ct),
		["IDbCommand, mapped"] = (t, w, complete, ct) => Db(t).ToChannel<Row>(w, complete, ct),
		["IDbCommand, mapped with map"] = (t, w, complete, ct) => Db(t).ToChannel(w, complete, Overrides, ct),
		["DbCommand, transform"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, r => new Row { Id = r.GetInt32(0) }, ct),
		["DbCommand, mapped"] = (t, w, complete, ct) => Db(t).ToChannelAsync<Row>(w, complete, ct),
		["DbCommand, mapped with map"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, Overrides, ct),
		["IExecuteReader, transform"] = (t, w, complete, ct) => Expressive(t, ct).ToChannel(w, complete, r => new Row { Id = r.GetInt32(0) }),
		["IExecuteReader, mapped"] = (t, w, complete, ct) => Expressive(t, ct).ToChannel<Row>(w, complete),
		["IExecuteReader, mapped with map"] = (t, w, complete, ct) => Expressive(t, ct).ToChannel(w, complete, Overrides),
		["IExecuteReaderAsync, transform"] = (t, w, complete, ct) => Expressive(t, ct).ToChannelAsync(w, complete, r => new Row { Id = r.GetInt32(0) }),
		["IExecuteReaderAsync, mapped"] = (t, w, complete, ct) => Expressive(t, ct).ToChannelAsync<Row>(w, complete),
		["IExecuteReaderAsync, mapped with map"] = (t, w, complete, ct) => Expressive(t, ct).ToChannelAsync(w, complete, Overrides),
	};

	public static TheoryData<string, bool> ArrayShapes() => Shapes(ArrayCalls.Keys);

	public static TheoryData<string, bool> RowShapes() => Shapes(RowCalls.Keys);

	static TheoryData<string, bool> Shapes(IEnumerable<string> names)
	{
		var data = new TheoryData<string, bool>();
		foreach (string name in names)
		{
			data.Add(name, true);
			data.Add(name, false);
		}

		return data;
	}

	static async Task WaitUntil(Func<bool> condition, string what)
	{
		var watch = System.Diagnostics.Stopwatch.StartNew();
		while (!condition())
		{
			Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), what);
			await Task.Delay(5, Ct);
		}
	}

	// Reads what is there until the channel is completed or the wait is over; tells whether it was completed (with or without an error).
	static async Task<bool> DrainUntilCompleted<T>(ChannelReader<T> reader, TimeSpan wait)
	{
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
		cts.CancelAfter(wait);
		try
		{
			while (await reader.WaitToReadAsync(cts.Token))
				while (reader.TryRead(out _)) { }
		}
		catch (Exception)
		{
			// Completed with an error, or the wait is over: the state of the reader tells.
		}

		return reader.Completion.IsCompleted;
	}

	// A bounded channel of capacity one that is full when the call is made. The command runs at once, every row is delivered as the consumer
	// drains, and the connection is closed at the end.
	static async Task FullTarget_RunsAtOnce<T>(Call<T> call, T filler, bool complete)
	{
		const int rows = 3;
		var tally = Tally(rows);
		Channel<T> channel = BoundedChannel.CreateBounded<T>(1);
		Assert.True(channel.Writer.TryWrite(filler));

		Task<long> task = call(tally, channel.Writer, complete, Ct).AsTask();

		// Nothing waits for room before the command runs: it has been executed, and its connection opened, while the target is still full.
		await WaitUntil(() => tally.Readers.Count == 1 && tally.Connections.Sum(c => c.Opens) == 1, "the command did not run while the target was full");

		for (int i = 0; i < rows + 1; i++) // The filler, then every row.
			await channel.Reader.ReadAsync(Ct).AsTask().WaitAsync(Timeout, Ct);

		Assert.Equal(rows, await task.WaitAsync(Timeout, Ct));
		TrackedConnection connection = Assert.Single(tally.Connections);
		Assert.Equal(1, connection.Opens);
		Assert.Equal(1, connection.Closes);
		Assert.Equal(1, Assert.Single(tally.Readers).Disposes);

		if (complete) await channel.Reader.Completion.WaitAsync(Timeout, Ct);
		else Assert.False(channel.Reader.Completion.IsCompleted);
	}

	// The same target, and a consumer that never drains: the call waits, and a cancel ends it with the cancellation, the connection closed and, with complete true, the target completed.
	static async Task FullTarget_NeverDrained_CancelEndsTheCall<T>(Call<T> call, T filler, bool complete)
	{
		var tally = Tally(5000); // More rows than the buffer of a mapped pipeline holds.
		Channel<T> channel = BoundedChannel.CreateBounded<T>(1);
		Assert.True(channel.Writer.TryWrite(filler));
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);

		Task<long> task = call(tally, channel.Writer, complete, cts.Token).AsTask();
		await WaitUntil(() => tally.Readers.Count == 1, "the command did not run while the target was full");
		Assert.False(task.IsCompleted);

		cts.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task.WaitAsync(Timeout, Ct));

		TrackedConnection connection = Assert.Single(tally.Connections);
		Assert.Equal(1, connection.Opens);
		Assert.Equal(1, connection.Closes);

		// Make room and look at the writer: a completed one reports it, an open one is left alone.
		Assert.Equal(complete, await DrainUntilCompleted(channel.Reader, complete ? Timeout : Quiet));
	}

	// A target that is already completed when the call is made: the call ends with ChannelClosedException (whether the command ran first does not matter),
	// and the connection it opened is closed. The mapped overloads read into a buffer and deliver on a second task: the failure of that task ends the call only
	// if it happens before the reading ends, so the result is larger than the buffer (1024 rows).
	static async Task ClosedTarget_Throws<T>(Call<T> call, bool complete)
	{
		var tally = Tally(5000);
		Channel<T> channel = BoundedChannel.CreateBounded<T>(1);
		channel.Writer.Complete();

		await Assert.ThrowsAnyAsync<ChannelClosedException>(async () => await call(tally, channel.Writer, complete, Ct).AsTask().WaitAsync(Timeout, Ct));
		Assert.All(tally.Connections, c => Assert.Equal(c.Opens, c.Closes));
	}

	// An error that follows the rows (a provider raises it when the reader moves on) reaches the caller of a command level call, and with complete true faults the target.
	static async Task LateError_ReachesTheCaller<T>(Call<T> call, bool complete)
	{
		var tally = Tally(2, new LateException());
		Channel<T> channel = BoundedChannel.CreateUnbounded<T>();

		await Assert.ThrowsAnyAsync<LateException>(async () => await call(tally, channel.Writer, complete, Ct).AsTask().WaitAsync(Timeout, Ct));
		TrackedConnection connection = Assert.Single(tally.Connections);
		Assert.Equal(connection.Opens, connection.Closes);

		if (complete)
			await Assert.ThrowsAnyAsync<LateException>(async () => { await foreach (T _ in channel.Reader.ReadAllAsync(Ct)) { } });
		else
			Assert.False(channel.Reader.Completion.IsCompleted);
	}

	[Theory]
	[MemberData(nameof(ArrayShapes))]
	public static Task Array_FullTarget_RunsAtOnce_DeliversEveryRow_ClosesTheConnection(string call, bool complete)
		=> FullTarget_RunsAtOnce(ArrayCalls[call], [0], complete);

	[Theory]
	[MemberData(nameof(RowShapes))]
	public static Task Rows_FullTarget_RunsAtOnce_DeliversEveryRow_ClosesTheConnection(string call, bool complete)
		=> FullTarget_RunsAtOnce(RowCalls[call], new Row(), complete);

	[Theory]
	[MemberData(nameof(ArrayShapes))]
	public static Task Array_FullTarget_NeverDrained_CancelEndsTheCall(string call, bool complete)
		=> FullTarget_NeverDrained_CancelEndsTheCall(ArrayCalls[call], [0], complete);

	[Theory]
	[MemberData(nameof(RowShapes))]
	public static Task Rows_FullTarget_NeverDrained_CancelEndsTheCall(string call, bool complete)
		=> FullTarget_NeverDrained_CancelEndsTheCall(RowCalls[call], new Row(), complete);

	[Theory]
	[MemberData(nameof(ArrayShapes))]
	public static Task Array_ClosedTarget_ThrowsChannelClosedException(string call, bool complete)
		=> ClosedTarget_Throws(ArrayCalls[call], complete);

	[Theory]
	[MemberData(nameof(RowShapes))]
	public static Task Rows_ClosedTarget_ThrowsChannelClosedException(string call, bool complete)
		=> ClosedTarget_Throws(RowCalls[call], complete);

	[Theory]
	[MemberData(nameof(ArrayShapes))]
	public static Task Array_LateError_ReachesTheCaller(string call, bool complete)
		=> LateError_ReachesTheCaller(ArrayCalls[call], complete);

	[Theory]
	[MemberData(nameof(RowShapes))]
	public static Task Rows_LateError_ReachesTheCaller(string call, bool complete)
		=> LateError_ReachesTheCaller(RowCalls[call], complete);
}
