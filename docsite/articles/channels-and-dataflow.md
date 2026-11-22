# Channels and Dataflow

Query results can be read straight into a pipeline instead of a collection. The core package streams a mapped query, and two companion packages put its rows into a channel or a Dataflow block.

| Package | Entry points | Pipeline type |
|---|---|---|
| `Open.Database.Extensions.Core` (the views behind `Map<T>()`) | `ToBufferedAsyncStream()`, `ToAsyncBufferStream()` | `IAsyncEnumerable<T>` (elements), `IAsyncEnumerable<ReadOnlyMemory<T>>` (chunks) |
| `Open.Database.Extensions.Channel` | `ToChannelReader()`, `ToChannel()`, `ToChannelHoldingConnection()` | `System.Threading.Channels` |
| `Open.Database.Extensions.Dataflow` | `ToTargetBlock()`, `AsSourceBlock()`, `ToTargetBlockHoldingConnection()` | `System.Threading.Tasks.Dataflow` |

Both packages take a mapped query (`Map<T>()`), so rows are mapped the same way as [`Map<T>()`](mapping.md).

## Streaming from a mapped query

To read a mapped query while a consumer works, use the buffer streams (`ToBufferedAsyncStream` and `ToAsyncBufferStream`) in the core package: the connection is released as soon as the last row is read.

A stream earns its place when a producer and its consumer run at different speeds. After `Map<T>()`, the query is the producer: it reads the rows at database speed on a background task, in chunks, and is finished with the connection as soon as the last row is read. The consumer does the slow work (a web call, a file write, a transform) while the query is still being read. The rows are typed, the later result sets are read through, and rows are read with `Read` unless you call `AsyncReads()`.

Which one to use:
- `ToBufferedAsyncStream(cancellationToken)` yields the elements one by one, as an `IAsyncEnumerable<T>` that works with `await foreach` and `System.Linq.Async`.
- `ToAsyncBufferStream(cancellationToken)` yields the chunks (`ReadOnlyMemory<T>`), for raw speed and the shortest connection time.

Both are lazy and single-use: nothing runs until the first read, a second enumeration throws, and a `break` or a dispose cancels the query. The pooled buffers are cleared by the rule in [Pooled memory](async-buffers.md#pooled-memory), and `UsePool(pool, clearOnReturn: ...)` after `Map` chooses the pool. The pool, clearing, cancellation and the cost in memory are in [Async buffers and streams](async-buffers.md), which also covers `ToAsyncBuffer`.

### One producer, one consumer

The query reads ahead into buffers, and the consumer does its slow work row by row:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_producer_consumer)]

### A pipeline

A stage can consume the stream and produce a channel of its own. Here the query feeds a transform, which feeds the final consumer, and each stage runs on its own task.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_pipeline)]

### When you do not need a stream

If there is one consumer, only a few rows, and nothing else to overlap, you do not need a stream: buffer the rows with `ToListAsync(cancellationToken)`. The connection is released as soon as the list is built, and the slow work happens after that.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_buffer)]

To read rows one at a time and stop early, see [`Stream`](mapping.md#terminals): it holds the connection while you pull rows, so it is not the way to do slow work per row.

### What it costs

The producer is unbounded. It never waits for the consumer, so the read finishes at full speed and frees the connection, while memory grows with the backlog of a slow consumer. A bounded channel caps the channel, not the memory: the query is still read at full speed. To cap memory by holding the connection, see [Channels](#channels).

## Channels

![Open.Database.Extensions.Channel: You write the SQL. ODX writes the other twenty lines.](../images/hero-channel.svg)

**Import [Open.ChannelExtensions](https://www.nuget.org/packages/Open.ChannelExtensions/) alongside `Open.Database.Extensions.Channel`.** The Channel package depends only on Core and `System.Threading.Channels`, and does one job: it puts the rows of a mapped query into a channel as fast as it can. Everything after the dump (`Transform`, `Batch`, `Join`, `ReadAll`) is what Open.ChannelExtensions is for.

`ToChannelReader()` and `ToChannel()` make an unbounded channel and start reading at the call:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_channel)]

The query is read at full speed into pooled chunks, and the connection is closed when the read ends, however slowly the consumer reads. The rows the consumer has not read yet wait in memory. The channel is always completed: after the last row, or with the exception that ended the read, a cancel included. `ToChannel(out Task<long> readCount)` also returns the number of rows written. `ToBoundedChannelReader(capacity)` and `ToBoundedChannel(capacity)` make a bounded channel; the bound limits the channel, not memory.

### A channel you own

`ToChannel(channel)` and `ToChannel(writer)` only write. They leave the channel open, so passes can share it, and the returned task is the done signal: it completes with the number of rows written, and a failure or a cancel comes back through it. `complete: true` completes the channel when the pass ends, with the exception that ended it if there was one.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_channel_owned)]

When the channel is full, the write waits for room, before the command runs and between rows. The rows wait in memory meanwhile, so the channel does not limit memory. If the channel is closed, the pass stops, without an exception, and returns the rows written so far; a channel completed with an error throws that error.

### Capping memory

`ToChannelHoldingConnection` pulls the rows one at a time from [`Stream`](xref:Open.Database.Extensions.IMappedQuery`1.Stream*), so a full channel limits memory. `Stream` says when to use it and what the required `stallTimeout` does.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_channel_holding)]

## Dataflow

![Open.Database.Extensions.Dataflow: You write the SQL. ODX writes the other twenty lines.](../images/hero-dataflow.svg)

The Dataflow package exists to feed whatever pipeline you build, and to start it as fast as the query can be read. It reads a mapped query at full speed into pooled chunks and sends each row to a block, so the blocks, their task schedulers and their priorities stay yours.

`ToTargetBlock` sends the rows to a block you own and leaves it open, so passes can share it. The returned task is the done signal: it completes with the number of rows the block accepted, and a failure or a cancel comes back through it. `complete: true` completes the block when the pass ends, or faults it with the exception that ended it, a cancel included (a faulted block drops the rows it holds that were not received yet).

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_target_block)]

Each row is sent with `SendAsync`, so a bounded block that is full makes the pass wait and no row is dropped. The rows wait in memory meanwhile, and the connection is closed when the read ends. A block has no way to ask for room, so only a block that is already finished is known before the command runs. If the block declines a row for good (completed or faulted), the pass stops without an exception and returns the rows accepted so far.

`AsSourceBlock` makes the block for you: an unbounded `BufferBlock<T>`, or one made from the `DataflowBlockOptions` you pass (its task scheduler, its bound, its name). It starts reading at the call and always finishes the block, completed after the last row or faulted with the exception that ended the read.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#as_source_block)]

`ToTargetBlockHoldingConnection` pulls the rows one at a time from [`Stream`](xref:Open.Database.Extensions.IMappedQuery`1.Stream*), so a full block limits memory. `Stream` says when to use it and what the required `stallTimeout` does.

## Honest limits

- These are conveniences for pipelines, not speed-ups. In the published benchmarks, draining a channel or a `BufferBlock` is slower than reading rows in a plain loop; see [Channels and Dataflow against a plain loop](benchmarks/pipelines.md).
