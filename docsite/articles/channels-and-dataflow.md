# Channels and Dataflow

Query results can be read straight into a pipeline instead of a collection. The core package does it for a mapped query, and two companion packages do it for commands and readers.

| Package | Entry points | Pipeline type |
|---|---|---|
| `Open.Database.Extensions.Core` (the views behind `Map<T>()`) | `ToChannelReader()`, `ToChannel()`, `ToChannel(out var rows)` | `System.Threading.Channels` |
| `Open.Database.Extensions.Channel` | `ToChannel<T>()`, `ToChannelAsync<T>()` | `System.Threading.Channels` |
| `Open.Database.Extensions.Dataflow` | `ToTargetBlock<T>()`, `ToTargetBlockAsync<T>()`, `AsSourceBlock<T>()` | `System.Threading.Tasks.Dataflow` |

The packages work on expressive commands, on `IDbCommand` and on readers, and map rows the same way as [`Map<T>()`](mapping.md).

## Streaming from a mapped query

After `Map<T>()`, a query can stream its rows into a channel that ODX makes and completes. The rows are typed, the later result sets are read through, and rows are read with `Read` unless you call `AsyncReads()`. There is nothing more to install: this is in the core package.

Which one to use:
- `ToChannelReader()` is for streaming. It returns the `ChannelReader<T>`; consume it with `await foreach` over `ReadAllAsync(cancellationToken)`.
- `ToChannel()` hands you both ends of a channel that ODX still completes.
- `ToChannel(out var rows)` does the same and also gives the row count as a `Task<long>`, completed after the read-through. It is a `Task`, so it can be awaited more than once.

The default is an unbounded channel with a deferred start:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_default)]

The chain reads as one expression. Here it is next to iterating the query directly, which gives the same rows:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_chained)]

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_direct)]

Every rung takes the cancellation token last, so it can be passed positionally after the options: `ToChannelReader(ct)`, `ToChannelReader(options, ct)`, `ToChannelReader(startImmediately: true, ct)`. A token that is already cancelled does not throw at the call; the channel ends with the cancel, and the consumer's read throws it.

A bound, and the start, are arguments of the same call:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_bounded)]

`ToChannel(out var rows)` returns the channel and the count:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#stream_count)]

### What each choice costs

- **Unbounded** (the default): the read finishes at full speed and frees the connection, while memory grows with the backlog.
- **Bounded:** memory is capped, but the write waits for room, so the reader, and with it the connection, stays open for as long as the consumer takes. A `Drop*` `FullMode` is allowed, and it loses rows when the channel is full.
- **Deferred** (the default) **against `startImmediately: true`:** a deferred feed opens the connection and runs the command at the first read; an immediate one at the call. Either way the read itself runs on a thread-pool thread, so the call returns at once.

### Things to know

- A deferred feed that is never read never runs the command. Awaiting `Completion` alone does not start it, so a consumer that only awaits `Completion` waits forever: read, or use `startImmediately: true`. For `ToChannel(out var rows)` the same holds for `rows`.
- With a connection you opened, a deferred feed runs on it later, at the first read, so do not use that connection until the feed ends. A connection ODX opened is closed when the feed ends.
- With `ToChannel()` and `ToChannel(out var rows)`, writes you make through the returned writer interleave with ODX's rows, and fail once ODX has completed the channel. A write does not start a deferred feed; only a read does.
- If you complete the channel yourself, the read stops and the rows written so far stand. If you complete it with an error, that error is the one the feed reports.
- Continuations that run synchronously (`AllowSynchronousContinuations`) run on the producer's thread, which holds the connection.
- The options are the BCL's `BoundedChannelOptions` and `UnboundedChannelOptions` (or `null`); anything else throws `ArgumentException` at the call.

The Channel package's own `ToChannel<T>()` (below) works on a command or a reader rather than on a mapped query.

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
