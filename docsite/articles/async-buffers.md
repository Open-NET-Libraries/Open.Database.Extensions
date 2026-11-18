# Async buffers and streams

`ToAsyncBuffer`, `ToAsyncBufferStream` and `ToBufferedAsyncStream` read a query as fast as the provider allows, into pooled chunks, on their own background producer. The connection is released when the last row is read. What happens to the rows after that, and how slowly, is the consumer's business.

They are on the typed views: `Map<T>()` of a command, and the same view after `Skip` and `Take`. Every example on this page is async, and every one is compiled and checked in the samples project.

## Why: the connection is the scarce thing

The keystone is minimum connection-open time. A connection held while your code serializes a response, calls a web service or writes a file is a connection no one else can use. The producer reads the query into pooled chunks without ever waiting for the consumer, closes the connection, and the consumer works at its own pace afterwards.

The cost is memory, and it is better to say so plainly. The rows are held, not the connection.

- An unbounded stream holds a whole result in memory when its consumer is slow, because the producer never waits.
- A buffer holds everything until it is disposed.

The query is read with the same read as every other terminal: `Read()` by default, `ReadAsync` after [`AsyncReads()`](async-reads.md).

<div style="overflow-x: auto; margin: 1rem 0;"><img src="../images/async-buffers-reader-open.svg" alt="Three panels of paired bars: reader-open time in milliseconds for 10,000 rows. Consumer at 39 ns per row: chunk stream 6.29, no buffer 6.62. At 1 microsecond per row: 6.27 against 16.15. Waiting 20 microseconds per 100 rows: 6.21 against 8.54." style="display: block; width: 100%; min-width: 600px; height: auto;"></div>

Three panels, each a pair of bars that start at zero, on one scale: how long the reader is held open for a result of 10,000 rows, as the consumer gets slower. The left bar of each pair is `ToAsyncBufferStream`; the right bar is no buffer, the consumer working inside the read. At 39 ns per row the pair is 6.29 and 6.62 ms. At 1 µs per row it is 6.27 and 16.15 ms. With a 20 µs wait per 100 rows it is 6.21 and 8.54 ms. The chunk stream holds the reader the same time however slow the consumer is; without a buffer the reader is held as long as the consumer takes. The source is the PooledBuffer final benchmark round (job J3a): a synthetic provider on net11 with a 50 µs refill every 100 rows, one run. It is not a real database.

## The three terminals

| Terminal | Returns | Starts | Use it for |
|---|---|---|---|
| `ToAsyncBuffer` | `PooledBuffer<T>` | At the call, on its own producer, even if nobody reads yet | A result you hand to another layer, or work with for a while: a reusable list, with an indexer |
| `ToAsyncBufferStream` | `IAsyncEnumerable<ReadOnlyMemory<T>>`, one chunk per step | At the first `MoveNextAsync`. Single-use | Sequential, read-only access to the rows without copying them twice |
| `ToBufferedAsyncStream` | `IAsyncEnumerable<T>`, one row per step | At the first `MoveNextAsync`. Single-use | The same, made easy: `await foreach` and `System.Linq.Async` work as they do on any stream |

`ToAsyncBuffer` returns at once. The read may finish before anyone asks for the rows. The buffer is not readable while it fills: its rows are reached through [`Items`](#the-items-view), once the read is done. To read while the query is still running, use one of the streams.

The streams do nothing until the first `MoveNextAsync`, and they can be enumerated once. A second `GetAsyncEnumerator` on the chunk stream throws `InvalidOperationException`; on the element stream the same exception comes at the first `MoveNextAsync`.

The same terminals are on a view with `Skip` and `Take`:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_skip_take)]

## The controller story

The use case that shaped `ToAsyncBuffer` is a data layer that hands rows up to a layer that does not know about the database. A command factory method returns `PooledBuffer<T>`:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_data_layer)]

The method returns at once and the read is already running. The layer above, a controller say, awaits the rows when it needs them, serializes them and disposes the buffer:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_controller)]

The connection was released when the read ended, however long the serialization took. Everything here is async: `await using` for the buffer, `await buffer.Items` for the rows, and no synchronous wait anywhere. Do not block on `Items.Result`. The producer runs on the ambient task scheduler, so blocking the one thread of a single-thread scheduler (a UI thread, an old synchronization context) can deadlock it.

### The Items view

`Items` is a `Task<PooledArray<T>>`: the same task on every access, and the view is built only if someone asks. A `PooledArray<T>` is a read-only list over the pooled arrays:

- It is an `IReadOnlyList<T>`, and an explicit read-only `ICollection<T>` (`Add`, `Clear` and `Remove` throw `NotSupportedException`).
- `Count` and the indexer are O(1).
- `foreach` binds to a struct enumerator, so walking the rows allocates nothing.
- LINQ's `ToArray` and `ToList` and the `List<T>` constructor see the count, and copy it one array at a time.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_items_view)]

`Items` faults with the read's exception. It is cancelled when the read ended with an `OperationCanceledException` of any origin, or when the buffer was disposed first. See [Cancellation and disposal](#cancellation-and-disposal).

The view is valid until it is disposed. Disposing it disposes the buffer it came from, and disposing the buffer returns the arrays to the pool. After that every member of the view throws `ObjectDisposedException`, an enumeration in progress included. Disposing while the read is still running cancels the query.

## The streams

`ToAsyncBufferStream` yields one chunk at a time as a `ReadOnlyMemory<T>`. A chunk holds exactly the rows of one chunk and is valid until the next step of the loop, when its array goes back to the pool. Copy what you keep, and do not collect the chunks themselves (with `ToListAsync` or `Buffer`, say): nothing guards a memory held longer.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_chunk_stream)]

`ToBufferedAsyncStream` flattens each chunk into its rows. Each row is copied out before it is yielded, so a row has no lifetime to mind. A `break` out of `await foreach` disposes the enumerator, which cancels the query and returns every array still queued.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_element_stream)]

Always dispose the enumerator, which `await foreach` does. An enumerator that is abandoned gives nothing back: the producer keeps reading the whole query into its unbounded queue, so the connection is held until that read ends.

## The overloads

Each terminal comes in three overloads, and each ends with `CancellationToken cancellationToken = default`:

- The shared pool and the flag: `ToAsyncBuffer(clearOnDispose, cancellationToken)`. The `bool` is required; there is no flag-less form.
- Your own pool: `ToAsyncBuffer(pool, cancellationToken)`. Nothing is cleared.
- Your own pool and the flag: `ToAsyncBuffer(pool, clearOnDispose, cancellationToken)`.

On the two streams the flag is `clearOnReturn` instead of `clearOnDispose`. A null pool throws `ArgumentNullException` at the call.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_overloads)]

The flag has no default on the shared pool on purpose. The shared pool does not clear what it gets back, and any other code in the process can rent the same array next, so whether to scrub the rows is a decision to make, not a default to miss.

A pool of your own is any `MemoryPool<T>`. This one counts what it hands out and what comes back, and the samples use it to check the claims on this page:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_counting_pool)]

## Clearing

Nothing is cleared by default, which is the BCL's behavior. A pooled array goes back with its rows in it, and while it sits in the pool, or in a buffer that is not disposed, every reference-type row in it stays reachable.

Pass the flag for sensitive rows:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_clearing)]

- What is cleared is the rows the buffer holds, never the rest of a larger array, of every type.
- For a buffer, the clear takes effect when the buffer, or its `PooledArray<T>` view, is disposed. For a stream, it takes effect as each chunk goes back: at the next step, or when the enumeration ends or is disposed.
- The clear runs on one background loop, off the read path. Neither the read nor the consumer waits for it.
- If 10,240 clears are ever waiting, something is wrong, and further buffers are discarded to the garbage collector: not cleared, and not returned to the pool. A dirty array never reaches a pool. It costs the pool one array, which it allocates again.
- A custom pool must therefore tolerate an owner that is never disposed. It must not throw from `Return`, either. If it does, the arrays not yet returned stay with the garbage collector.

## Cancellation and disposal

- **Two sources, one token.** The caller's token and the dispose are combined, so either one cancels the database read itself, not only the loop around it.
- **Our own cancel is swallowed.** A cancel caused by disposing the buffer, or the stream's enumerator, is not reported: you asked for it.
- **Yours is delivered.** The caller's own cancel, or a fault in the read, is delivered after the rows that were already read. A stream's next step throws it after the chunks that were already delivered. `Items` is cancelled or faulted instead of producing a view.
- **`DisposeAsync` waits for the producer, with no timeout.** It cancels a running read, waits until the producer has stopped, and returns the arrays. A provider that ignores cancellation delays it by the call in progress. After that, the producer stops at the next chunk boundary at the latest. The synchronous `Dispose` does not wait. It cancels and returns at once, and the producer returns the arrays when it exits.
- **No finalizer.** A buffer that is never disposed is not a leak: it holds its arrays until the garbage collector takes it, and the pool does not get them back.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_cancel)]

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_dispose_early)]

## Memory shape

Chunks start at 16 rows and double: 16, 32, 64 and so on. A buffer keeps doubling, and never moves a row once it is written. That is what makes the indexer O(1): the chunk of a row is found with a shift and a log. A stream stops doubling at 4,096 rows, so a long result is read in chunks of at most 4,096.

## What it costs

The numbers come from a synthetic provider on net11, not from a real database, and they are one run each.

- Reader-open time stays flat as the consumer slows. For the chunk stream at 10,000 rows it was 6.29, 6.27 and 6.21 ms for the three consumers in the figure above.
- On a simulated link it is within a few percent of `ToArrayAsync`. At a 50 µs refill the buffer held the reader 6.27 ms and `ToArrayAsync` 6.29 ms.
- At zero latency and 10,000 rows, dumping a result into a buffer holds the reader about 22% longer than `ToArrayAsync`: 225 against 185 µs. The buffer pays that to be able to hand the rows to someone else.

## When to use something else

For a small result, or one that nobody else needs, `ToArrayAsync` or `ToListAsync` is the better tool. The connection is released as soon as the array or list is built, and there is nothing to dispose.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_small_result)]

For a projection or a filter over a stream, use `System.Linq.Async` over `ToBufferedAsyncStream`. It is in the box on net10 and later.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_linq_async)]

The channel terminals (`ToChannelReader()` and `ToChannel()`) left the typed views of Core in 11.0.0-rc.8. The buffer streams do the same job with a shorter connection time. See [Channels and Dataflow](channels-and-dataflow.md).

## Sources

| Claim | Value | Run |
|---|---|---|
| Reader-open time, chunk stream, 10,000 rows | 6.29, 6.27 and 6.21 ms at 39 ns per row, 1 µs per row and a 20 µs wait per 100 rows | PooledBuffer final benchmark round, job J3a, synthetic provider, net11, a 50 µs refill every 100 rows; one run |
| Reader-open time, no buffer (the figure) | 6.62, 16.15 and 8.54 ms for the same consumers | same |
| Buffer against `ToArrayAsync`, simulated link | 6.27 and 6.29 ms, at a 50 µs refill every 100 rows | PooledBuffer final benchmark round, job J2a, same provider; one run |
| Buffer against `ToArrayAsync`, zero latency | 225 and 185 µs at 10,000 rows (about 22% longer) | PooledBuffer final benchmark round, job J1a, same provider; one run |
| Chunk sizes, the cleanup queue, the single-use rule | 16 doubling to 4,096 for a stream; 10,240 waiting clears | the library source (`ChunkSizing`, `RecycleBin`, `ChunkStream`); not a measurement |
