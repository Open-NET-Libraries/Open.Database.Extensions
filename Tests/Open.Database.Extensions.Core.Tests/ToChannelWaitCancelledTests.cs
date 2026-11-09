using System.Buffers;
using System.Data.Common;
using BoundedChannel = System.Threading.Channels.Channel;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The rule for a target channel: with complete true it is completed (or faulted) after the call however the call ends, and with complete false
// it is never completed or faulted. A call waits for room in the target before it executes anything; when that wait is cancelled the call
// ends too, so it has to complete the target like any other end.
[ExcludeFromCodeCoverage]
public static class ToChannelWaitCancelledTests
{
	sealed class Row
	{
		public int Id { get; set; }
	}

	static readonly RowMap<Row> Overrides = new([new("Id", "Id")]);

	delegate ValueTask<long> Call<T>(DbTally tally, ChannelWriter<T> target, bool complete, CancellationToken token);

	static ExpressiveDbCommand Expressive(DbTally tally, CancellationToken token)
	{
		ExpressiveDbCommand command = tally.NewConnection().Command("select 1");
		command.CancellationToken = token;
		return command;
	}

	// The connection is closed in every case, so a command level call waits for room before it opens it.
	static DbCommand Db(DbTally tally) => tally.NewConnection().CreateCommand();

	static readonly Dictionary<string, Call<object[]>> ArrayCalls = new()
	{
		["IDbCommand"] = (t, w, complete, ct) => ((IDbCommand)Db(t)).ToChannel(w, complete, ct),
		["IDbCommand, pool"] = (t, w, complete, ct) => ((IDbCommand)Db(t)).ToChannel(w, complete, ArrayPool<object>.Shared, ct),
		["DbCommand"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, ct),
		["DbCommand, pool"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, ArrayPool<object>.Shared, ct),
		["IExecuteReader"] = (t, w, complete, ct) => ((IExecuteReader)Expressive(t, ct)).ToChannel(w, complete),
		["IExecuteReader, pool"] = (t, w, complete, ct) => ((IExecuteReader)Expressive(t, ct)).ToChannel(w, complete, ArrayPool<object>.Shared),
		["IExecuteReaderAsync"] = (t, w, complete, ct) => ((IExecuteReaderAsync)Expressive(t, ct)).ToChannelAsync(w, complete),
		["IExecuteReaderAsync, pool"] = (t, w, complete, ct) => ((IExecuteReaderAsync)Expressive(t, ct)).ToChannelAsync(w, complete, ArrayPool<object>.Shared),
	};

	static readonly Dictionary<string, Call<Row>> RowCalls = new()
	{
		["IDbCommand, transform"] = (t, w, complete, ct) => ((IDbCommand)Db(t)).ToChannel(w, complete, _ => new Row(), ct),
		["IDbCommand, mapped"] = (t, w, complete, ct) => ((IDbCommand)Db(t)).ToChannel<Row>(w, complete, ct),
		["IDbCommand, mapped with map"] = (t, w, complete, ct) => ((IDbCommand)Db(t)).ToChannel(w, complete, Overrides, ct),
		["DbCommand, transform"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, _ => new Row(), ct),
		["DbCommand, mapped"] = (t, w, complete, ct) => Db(t).ToChannelAsync<Row>(w, complete, ct),
		["DbCommand, mapped with map"] = (t, w, complete, ct) => Db(t).ToChannelAsync(w, complete, Overrides, ct),
		["IExecuteReader, transform"] = (t, w, complete, ct) => ((IExecuteReader)Expressive(t, ct)).ToChannel(w, complete, _ => new Row()),
		["IExecuteReader, mapped"] = (t, w, complete, ct) => ((IExecuteReader)Expressive(t, ct)).ToChannel<Row>(w, complete),
		["IExecuteReader, mapped with map"] = (t, w, complete, ct) => ((IExecuteReader)Expressive(t, ct)).ToChannel(w, complete, Overrides),
		["IExecuteReaderAsync, transform"] = (t, w, complete, ct) => ((IExecuteReaderAsync)Expressive(t, ct)).ToChannelAsync(w, complete, _ => new Row()),
		["IExecuteReaderAsync, mapped"] = (t, w, complete, ct) => ((IExecuteReaderAsync)Expressive(t, ct)).ToChannelAsync<Row>(w, complete),
		["IExecuteReaderAsync, mapped with map"] = (t, w, complete, ct) => ((IExecuteReaderAsync)Expressive(t, ct)).ToChannelAsync(w, complete, Overrides),
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

	// A bounded channel of capacity one that is full and has no reader, a closed connection, and a cancel while the call waits for room.
	static async Task CancelWhileWaitingForRoom<T>(Call<T> call, T filler, bool complete)
	{
		var tally = new DbTally();
		Channel<T> channel = BoundedChannel.CreateBounded<T>(1);
		Assert.True(channel.Writer.TryWrite(filler));
		using var cts = new CancellationTokenSource();

		Task<long> task = call(tally, channel.Writer, complete, cts.Token).AsTask();
		Assert.False(task.IsCompleted); // It is waiting for room.
		cts.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);

		// Nothing was executed.
		Assert.All(tally.Connections, c => Assert.Equal(0, c.Opens));
		Assert.Empty(tally.Readers);

		// Make room, so that the state of the writer shows: a completed one refuses the write, an open one accepts it.
		Assert.True(channel.Reader.TryRead(out _));
		Assert.Equal(!complete, channel.Writer.TryWrite(filler));
		Assert.Equal(complete, channel.Reader.Completion.IsCompleted);
	}

	[Theory]
	[MemberData(nameof(ArrayShapes))]
	public static Task Array_CancelledWhileWaitingForRoom_CompletesTheTargetOnlyWhenAsked(string call, bool complete)
		=> CancelWhileWaitingForRoom(ArrayCalls[call], [1], complete);

	[Theory]
	[MemberData(nameof(RowShapes))]
	public static Task Rows_CancelledWhileWaitingForRoom_CompletesTheTargetOnlyWhenAsked(string call, bool complete)
		=> CancelWhileWaitingForRoom(RowCalls[call], new Row(), complete);
}
