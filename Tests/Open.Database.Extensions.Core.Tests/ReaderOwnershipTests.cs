using System.Buffers;
using System.Threading.Tasks.Dataflow;
using BoundedChannel = System.Threading.Channels.Channel;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A reader the caller passes to a reader level pipeline overload belongs to the caller: the call reads the rows and leaves the reader where the rows ended,
// so the caller decides what the next result set is. (A command level call owns its reader, and moves it on itself so that an error that follows the rows is raised.)
[ExcludeFromCodeCoverage]
public static class ReaderOwnershipTests
{
	sealed class Row
	{
		public int Id { get; set; }
	}

	static readonly RowMap<Row> Overrides = new([new("Id", "Id")]);

	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static ChannelWriter<T> Unbounded<T>() => BoundedChannel.CreateUnbounded<T>().Writer;

	// Waits for the background work that AsSourceBlockAsync starts: a buffer block completes once everything it holds has been received.
	static async Task Completed<T>(IReceivableSourceBlock<T> block)
	{
		await foreach (T _ in block.ReceiveAllAsync(Ct)) { }
	}

	static readonly Dictionary<string, Func<TypedReader, Task>> Calls = new()
	{
		// Channel, synchronous reads.
		["ToChannel"] = async r => await r.ToChannel(Unbounded<object[]>(), true, Ct),
		["ToChannel, pool"] = async r => await r.ToChannel(Unbounded<object[]>(), true, ArrayPool<object>.Shared, Ct),
		["ToChannel, transform"] = async r => await r.ToChannel(Unbounded<Row>(), true, rec => new Row { Id = rec.GetInt32(0) }, Ct),
		["ToChannel, mapped"] = async r => await r.ToChannel<Row>(Unbounded<Row>(), true, Ct),
		["ToChannel, mapped with map"] = async r => await r.ToChannel(Unbounded<Row>(), true, Overrides, Ct),

		// Channel, asynchronous reads.
		["ToChannelAsync"] = async r => await r.ToChannelAsync(Unbounded<object[]>(), true, Ct),
		["ToChannelAsync, pool"] = async r => await r.ToChannelAsync(Unbounded<object[]>(), true, ArrayPool<object>.Shared, Ct),
		["ToChannelAsync, transform"] = async r => await r.ToChannelAsync(Unbounded<Row>(), true, rec => new Row { Id = rec.GetInt32(0) }, Ct),
		["ToChannelAsync, mapped"] = async r => await r.ToChannelAsync<Row>(Unbounded<Row>(), true, Ct),
		["ToChannelAsync, mapped with map"] = async r => await r.ToChannelAsync(Unbounded<Row>(), true, Overrides, Ct),

		// Dataflow, synchronous reads.
		["ToTargetBlock"] = r => { r.ToTargetBlock(new BufferBlock<object[]>(), true); return Task.CompletedTask; },
		["ToTargetBlock, pool"] = r => { r.ToTargetBlock(new BufferBlock<object[]>(), true, ArrayPool<object>.Shared); return Task.CompletedTask; },
		["ToTargetBlock, transform"] = r => { r.ToTargetBlock(new BufferBlock<Row>(), true, rec => new Row { Id = rec.GetInt32(0) }); return Task.CompletedTask; },
		["ToTargetBlock, mapped"] = r => { r.ToTargetBlock<Row>(new BufferBlock<Row>(), true); return Task.CompletedTask; },
		["ToTargetBlock, mapped with map"] = r => { r.ToTargetBlock(new BufferBlock<Row>(), true, Overrides); return Task.CompletedTask; },

		// Dataflow, asynchronous reads.
		["ToTargetBlockAsync"] = async r => await r.ToTargetBlockAsync(new BufferBlock<object[]>(), true, Ct),
		["ToTargetBlockAsync, pool"] = async r => await r.ToTargetBlockAsync(new BufferBlock<object[]>(), true, ArrayPool<object>.Shared, Ct),
		["ToTargetBlockAsync, transform"] = async r => await r.ToTargetBlockAsync(new BufferBlock<Row>(), true, rec => new Row { Id = rec.GetInt32(0) }, Ct),
		["ToTargetBlockAsync, mapped"] = async r => await r.ToTargetBlockAsync<Row>(new BufferBlock<Row>(), true, Ct),
		["ToTargetBlockAsync, mapped with map"] = async r => await r.ToTargetBlockAsync(new BufferBlock<Row>(), true, Overrides, Ct),

		// Dataflow, AsSourceBlock (synchronous: the rows have been read when it returns) and AsSourceBlockAsync (in the background).
		["AsSourceBlock"] = r => { r.AsSourceBlock(); return Task.CompletedTask; },
		["AsSourceBlock, pool"] = r => { r.AsSourceBlock(ArrayPool<object>.Shared); return Task.CompletedTask; },
		["AsSourceBlock, transform"] = r => { r.AsSourceBlock(rec => new Row { Id = rec.GetInt32(0) }); return Task.CompletedTask; },
		["AsSourceBlock, mapped"] = r => { r.AsSourceBlock<Row>(); return Task.CompletedTask; },
		["AsSourceBlock, mapped with map"] = r => { r.AsSourceBlock(Overrides); return Task.CompletedTask; },
		["AsSourceBlockAsync"] = async r => await Completed(r.AsSourceBlockAsync(cancellationToken: Ct)),
		["AsSourceBlockAsync, pool"] = async r => await Completed(r.AsSourceBlockAsync(ArrayPool<object>.Shared, cancellationToken: Ct)),
		["AsSourceBlockAsync, transform"] = async r => await Completed(r.AsSourceBlockAsync(rec => new Row { Id = rec.GetInt32(0) }, cancellationToken: Ct)),
		["AsSourceBlockAsync, mapped"] = async r => await Completed(r.AsSourceBlockAsync<Row>(cancellationToken: Ct)),
		["AsSourceBlockAsync, mapped with map"] = async r => await Completed(r.AsSourceBlockAsync(Overrides, cancellationToken: Ct)),
	};

	public static TheoryData<string> Shapes()
	{
		var data = new TheoryData<string>();
		foreach (string name in Calls.Keys) data.Add(name);
		return data;
	}

	// A reader over two result sets: after the call it is still at the end of the first, and the next one is there for the caller.
	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task ReaderLevel_LeavesTheNextResultSetToTheCaller(string call)
	{
		TypedReader reader = TypedReader.Create([("Id", typeof(int)), ("Name", typeof(string))], [1, "a"], [2, "b"]);
		reader.MoreResultSets = 1;

		await Calls[call](reader).WaitAsync(Timeout, Ct);

		Assert.Equal(0, reader.NextResultCalls);
		Assert.True(reader.NextResult());
	}
}
