# Channels and Dataflow

Two companion packages read query results straight into a pipeline instead of a collection.

| Package | Entry points | Pipeline type |
|---|---|---|
| `Open.Database.Extensions.Channel` | `ToChannel<T>()`, `ToChannelAsync<T>()` | `System.Threading.Channels` |
| `Open.Database.Extensions.Dataflow` | `ToTargetBlock<T>()`, `ToTargetBlockAsync<T>()`, `AsSourceBlock<T>()` | `System.Threading.Tasks.Dataflow` |

They work on expressive commands, on `IDbCommand` and on readers, and map rows the same way as [`Map<T>()`](mapping.md).

## Channels

![Open.Database.Extensions.Channel: You write the SQL. The library writes the other twenty lines.](../images/hero-channel.svg)

`ToChannel<T>` returns a `ChannelReader<T>` immediately. The channel is completed when every row has been written, or completed with the exception if reading fails:

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_channel)]

The overloads that take a `ChannelWriter` as the target return the number of rows read and handed to the internal buffer, not the number delivered. With `complete: true` the writer is completed after the last row has been delivered and the reader released, which can be after the call has returned.

## Dataflow

![Open.Database.Extensions.Dataflow: You write the SQL. The library writes the other twenty lines.](../images/hero-dataflow.svg)

`ToTargetBlock<T>` posts each row to a block. The `complete` argument controls the target: with `true`, the block is completed after every row has been delivered and the reader has been released, or faulted with the exception if reading or mapping failed. With `false`, the block is left open and is never completed or faulted by the call. The number `ToTargetBlock<T>` returns counts the rows read and handed to the internal buffer; delivery to the target can still be in progress.

[!code-csharp[](../samples/ChannelsAndDataflow.cs#to_target_block)]

## Honest limits

- These are conveniences for pipelines, not speed-ups. In the published benchmarks, draining a channel or a `BufferBlock` is slower than reading rows in a plain loop; see [Performance](performance.md).
- The mapped overloads read raw rows into a bounded buffer of about 1,024 rows and map them in a second stage. The channel study in the [benchmark results](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md) explains the trade-off.
- The unmapped `ToChannel` overloads can still lose the row that was in flight when a read fails while the target is full. This is a known limitation of version 11.
- The synchronous `ToTargetBlock` overloads post each row without waiting. When the target is complete, or bounded and full, or when the internal 1,024-row buffer of the mapped overloads is full, reading stops without an error and the number the call returns is the only sign of the shortfall. To feed a bounded target, use `ToTargetBlockAsync`, which waits.
