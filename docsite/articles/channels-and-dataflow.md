# Channels and Dataflow

Query results can be read straight into a pipeline instead of a collection. The core package does it for a mapped query, and two companion packages do it for commands and readers.

| Package | Entry points | Pipeline type |
|---|---|---|
| `Open.Database.Extensions.Core` (the views behind `Map<T>()`) | `ToBufferedAsyncStream()`, `ToAsyncBufferStream()` | `IAsyncEnumerable<T>` (elements), `IAsyncEnumerable<ReadOnlyMemory<T>>` (chunks) |
| `Open.Database.Extensions.Channel` | `ToChannel<T>()`, `ToChannelAsync<T>()` | `System.Threading.Channels` |
| `Open.Database.Extensions.Dataflow` | `ToTargetBlock<T>()`, `ToTargetBlockAsync<T>()`, `AsSourceBlock<T>()` | `System.Threading.Tasks.Dataflow` |

The packages work on expressive commands, on `IDbCommand` and on readers, and map rows the same way as [`Map<T>()`](mapping.md).

## Streaming from a mapped query

The channel terminals on the typed views (`ToChannelReader()` and `ToChannel()`) left Core in 11.0.0-rc.8. They are planned to come back in the Channel library. Until then, the buffer streams do the same job with a shorter connection time, and they are in the core package.

A stream earns its place when a producer and its consumer run at different speeds. After `Map<T>()`, the query is the producer: it reads the rows at database speed on a background task, in chunks, and is finished with the connection as soon as the last row is read. The consumer does the slow work (a web call, a file write, a transform) while the query is still being read. The rows are typed, the later result sets are read through, and rows are read with `Read` unless you call `AsyncReads()`.

Which one to use:
- `ToBufferedAsyncStream(clearOnReturn)` yields the elements one by one, as an `IAsyncEnumerable<T>` that works with `await foreach` and `System.Linq.Async`.
- `ToAsyncBufferStream(clearOnReturn)` yields the chunks (`ReadOnlyMemory<T>`), for raw speed and the shortest connection time.

Both are lazy and single-use: nothing runs until the first read, a second enumeration throws, and a `break` or a dispose cancels the query. `clearOnReturn` has no default on the shared pool, so you decide whether the pooled buffers are cleared. Pass the cancellation token last. The overloads, clearing, cancellation and the cost in memory are in [Async buffers and streams](async-buffers.md), which also covers `ToAsyncBuffer`.

### One producer, one consumer

The query reads ahead into buffers, and the consumer does its slow work row by row:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_producer_consumer)]

### A pipeline

A stage can consume the stream and produce a channel of its own. Here the query feeds a transform, which feeds the final consumer, and each stage runs on its own task.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_pipeline)]

### When you do not need a stream

If there is one consumer, only a few rows, and nothing else to overlap, you do not need a stream: buffer the rows with `ToListAsync(cancellationToken)`. The connection is released as soon as the list is built, and the slow work happens after that.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_buffer)]

Iterating the query directly (the `ReadAsync` handler) holds the connection for the whole loop, so it is not the way to do slow work per row. The caution about streaming is [documented in the mapping article](mapping.md#streaming-holds-the-connection).

### What it costs

The producer is unbounded. It never waits for the consumer, so the read finishes at full speed and frees the connection, while memory grows with the backlog of a slow consumer. A bounded channel would cap the memory, but only by holding the connection at the consumer's pace, so the bounded forms are planned for the Channel library, under their own names.

## Channels

![Open.Database.Extensions.Channel: You write the SQL. ODX writes the other twenty lines.](../images/hero-channel.svg)

`ToChannel<T>` returns a `ChannelReader<T>` immediately. The channel is completed when every row has been written, or completed with the exception if reading fails:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_channel)]

The overloads that take a `ChannelWriter` as the target return the number of rows read and handed to the internal buffer, not the number delivered. With `complete: true` the writer is completed after the last row has been delivered and the reader released, which can be after the call has returned.

## Dataflow

![Open.Database.Extensions.Dataflow: You write the SQL. ODX writes the other twenty lines.](../images/hero-dataflow.svg)

`ToTargetBlock<T>` posts each row to a block. The `complete` argument controls the target: with `true`, the block is completed after every row has been delivered and the reader has been released, or faulted with the exception if reading or mapping failed. With `false`, the block is left open and is never completed or faulted by the call. The number `ToTargetBlock<T>` returns counts the rows read and handed to the internal buffer; delivery to the target can still be in progress.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_target_block)]

## Honest limits

- These are conveniences for pipelines, not speed-ups. In the published benchmarks, draining a channel or a `BufferBlock` is slower than reading rows in a plain loop; see [Performance](performance.md).
- The mapped overloads read raw rows into a bounded buffer of about 1,024 rows and map them in a second stage. The channel study in the [benchmark results](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md) explains the trade-off.
- The unmapped `ToChannel` overloads can still lose the row that was in flight when a read fails while the target is full. This is a known limitation of version 11.
- The synchronous `ToTargetBlock` overloads post each row without waiting. When the target is complete, or bounded and full, or when the internal 1,024-row buffer of the mapped overloads is full, reading stops without an error and the number the call returns is the only sign of the shortfall. To feed a bounded target, use `ToTargetBlockAsync`, which waits.
