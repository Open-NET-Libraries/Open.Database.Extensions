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

Three panels, each a pair of bars that start at zero, on one scale: how long the reader is held open for a result of 10,000 rows, as the consumer gets slower. The left bar of each pair is `ToAsyncBufferStream`; the right bar is no buffer, the consumer working inside the read. At 39 ns per row the pair is 6.29 and 6.62 ms. At 1 µs per row it is 6.27 and 16.15 ms. With a 20 µs wait per 100 rows it is 6.21 and 8.54 ms. The chunk stream holds the reader the same time however slow the consumer is; without a buffer the reader is held as long as the consumer takes. The source is the library's unpublished pooled-buffer benchmark round: a synthetic provider on net11 with a 50 µs refill every 100 rows, one run. It is not a real database.

## The three terminals

| Terminal | Returns | Starts | Use it for |
|---|---|---|---|
| `ToAsyncBuffer` | `PooledBuffer<T>` | At the call, on its own producer, even if nobody reads yet | A result you hand to another layer, or work with for a while: a reusable list, with an indexer |
| `ToAsyncBufferStream` | `IAsyncEnumerable<ReadOnlyMemory<T>>`, one chunk per step | At the first `MoveNextAsync`. Single-use | Sequential, read-only access to the rows without copying them twice |
| `ToBufferedAsyncStream` | `IAsyncEnumerable<T>`, one row per step | At the first `MoveNextAsync`. Single-use | The same, made easy: `await foreach` and `System.Linq.Async` work as they do on any stream |

`ToAsyncBuffer` returns at once. (A cancellation token that is already cancelled starts nothing.) The read may finish before anyone asks for the rows. The buffer is not readable while it fills: its rows are reached through [`Items`](#the-items-view), once the read is done. To read while the query is still running, use one of the streams. If the command is on a connection you opened yourself, do not use that connection for anything else until the read has finished: the producer reads on it from the call (`ToAsyncBuffer`) or from the first step (the streams).

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

`Items` faults with the read's exception. It is cancelled when the read ended with an `OperationCanceledException` of any origin, or when the buffer was disposed first. Reading `Items` after the buffer is disposed throws `ObjectDisposedException`; a task you already hold that is still pending is cancelled. See [Cancellation and disposal](#cancellation-and-disposal).

The view is valid until it is disposed. Disposing it disposes the buffer it came from, and disposing the buffer returns the arrays to the pool (a large clear goes to the cleanup queue, and a full queue clears them where they are released: see [Pooled memory](#pooled-memory)). After that every member of the view except `Dispose` throws `ObjectDisposedException`, an enumeration in progress included. Disposing again does nothing. Disposing the buffer while the read is still running cancels the query.

## Lifetime and ownership

A buffer is a loan of pooled arrays, and a loan has rules. These are the risks the design accepts, said plainly, so that you can accept them with your eyes open.

- **One owner disposes.** Whoever ends the use of the rows disposes the buffer, once. The `PooledArray<T>` from `Items` is the same loan, so disposing the view disposes the buffer. Do not hand the view to a second owner that also disposes it, and do not dispose while something else still reads it.
- **A lazy query runs when it is enumerated.** `Where`, `Select` and the rest of LINQ run nothing when you call them. If the query is enumerated after the buffer is disposed, it reads arrays that are back in the pool, and the view throws `ObjectDisposedException`. Finish the query (`ToList()`, `ToArray()`) before the buffer goes.
- **Copy out to keep the rows longer.** `ToArray()` or `ToList()` on the view copies the rows. The copy belongs to you and outlives the buffer.
- **One thread at a time.** Do not use the view from another thread while something may dispose it. The view checks that it is alive before each read, but a check is not a lock: a dispose on another thread can land just after it, with the arrays on their way back to the pool.
- **A buffer that is never disposed is not a leak, but it costs the pool.** There is no finalizer. The arrays stay with the buffer until the garbage collector takes it, and the pool never gets them back. The cost is the allocations the pool would have saved. This includes `await LatestPosts(...).Items` when `Items` faults or is cancelled: you never receive the view, so nothing disposes the buffer, and its arrays wait for the garbage collector.

To shape the rows as they arrive, use async LINQ over the stream and return it: no `async`, no `await`, no `using`. Filter in the SQL; shape in C#. The work overlaps the read, only what you keep is held, and there is nothing to dispose. It is in the box on net10 and later; net472 and netstandard need the `System.Linq.Async` package.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_linq_async)]

To keep every row, call `ToArrayAsync` or `ToListAsync` on the query directly: they collect the rows for you, and there is no buffer to dispose.

Take the buffer, or its `Items` view, when you need the whole set at once: `Count`, the indexer, several passes, or handing it to a serializer. Finish any LINQ query over the view before the view is disposed:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_finish_query)]

The mistake to avoid is a query that has not run when the buffer is disposed. This one throws:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_lazy_trap)]

## Returning it from a controller

The controller above serializes the rows itself, inside the method, so it can dispose the buffer in the same method. The other way is to hand the rows to the framework and let the framework serialize them after the action has returned. The buffer must then outlive the action, and the framework has to dispose it, not your method.

The pattern is to register the buffer with the response, and then return the rows:

```csharp
// Not compiled by the docs build: the samples project has no ASP.NET
// reference. Checked against ASP.NET Core 11.0.0, with TestServer and
// with Kestrel.
[ApiController]
[Route("posts")]
public class PostsController(
	IDbConnectionFactory<SqliteConnection> connectionFactory)
	: ControllerBase
{
	[HttpGet]
	public async Task<IReadOnlyList<Post>> GetAsync()
	{
		// LatestPosts is the data-layer method shown above (call it as you
		// have named it): the read starts here.
		PooledBuffer<Post> buffer = LatestPosts(
			connectionFactory,
			HttpContext.RequestAborted);

		// The framework disposes it when the request is over.
		Response.RegisterForDispose(buffer);

		// A PooledArray<Post> is an IReadOnlyList<Post>:
		// System.Text.Json writes it as a JSON array.
		return await buffer.Items;
	}
}
```

What was observed, on ASP.NET Core 11.0.0 against SQLite:

- The response is `200` with a JSON array of the rows, equal to the array the same query gives with `ToListAsync`. The declared return type can be `IReadOnlyList<Post>` or `PooledArray<Post>`.
- Every array went back to the pool once the request was over.
- `HttpResponse.RegisterForDispose` and `HttpResponse.RegisterForDisposeAsync` both exist. A `PooledBuffer<T>` is both `IDisposable` and `IAsyncDisposable`, and the framework disposed it with `DisposeAsync` whichever of the two was used, so `RegisterForDispose` is enough.
- Register the buffer before you await `Items`. When the client aborted while the read ran, Kestrel still disposed the buffer and the arrays went back. When the action threw, Kestrel disposed an object registered the same way (a spy that was both `IDisposable` and `IAsyncDisposable`). A client abort cancels `HttpContext.RequestAborted`, which cancels the query: on a 1,000,000-row query aborted after 30 ms the read stopped early, having rented 12 arrays (at most 65,520 rows, worked out from the chunk sizes, not measured) where a full read rents 16.

> [!NOTE]
> TestServer did not dispose what the request had registered when the action threw or the client aborted before the response started. Kestrel did. If a test of your own controller counts the arrays that come back, it sees them only on a successful request, or on a real server.

### The trap: dispose before the framework serializes

The tempting version disposes the buffer in the action, the way the earlier controller does:

```csharp
// Not compiled by the docs build. Do not do this.
[HttpGet]
public async Task<IReadOnlyList<Post>> GetAsync()
{
	await using PooledBuffer<Post> buffer = LatestPosts(
		connectionFactory,
		HttpContext.RequestAborted);

	// The buffer is disposed as this method returns,
	// before the framework has serialized anything.
	return await buffer.Items;
}
```

The framework serializes the rows after the action has returned, and by then the buffer is disposed. System.Text.Json calls the view's enumerator and gets `ObjectDisposedException`. The response had not started, so on Kestrel it is a `500` with an empty body and the exception in the log; on TestServer the exception reaches the caller. The body is empty, not partial. Nothing in the compiler stops it, so it is the first thing to look for in a code review.

### A very large result: return the stream

A buffer holds the whole result until the request is over. For a very large result, return the stream and let the framework walk it:

```csharp
// Not compiled by the docs build. Checked against ASP.NET Core 11.0.0.
[HttpGet("stream")]
public IAsyncEnumerable<Post> StreamAsync()
	=> connectionFactory
		.Command("select * from posts order by id")
		.Map<Post>()
		.ToBufferedAsyncStream(HttpContext.RequestAborted);
```

There is nothing to register: the framework enumerates the stream and disposes the enumerator, which returns every array still queued. What was observed:

- System.Text.Json wrote it as a JSON array, equal to the list the same query gives, with status `200`.
- The response was streamed. With the same stream wrapped in an `async IAsyncEnumerable` method, it had already started when the 4,000th of 5,000 rows was reached, so nothing waited for the whole result.
- Every array went back to the pool, including when the client aborted in the middle of the body or before reading any of it. An abort stopped the query early: it had rented 9 or 13 arrays (at most about 8,000 or 24,000 rows, worked out from the chunk sizes, not measured) where a full 1,000,000-row read rents 252.

A stream still reads ahead without waiting for the client, as [described above](#why-the-connection-is-the-scarce-thing). The read does not wait for a slow client: it is not held up by one, and the rows the client has not taken yet wait in memory. That follows from the read-ahead above; it was not measured with a slow client.

## The streams

`ToAsyncBufferStream` yields one chunk at a time as a `ReadOnlyMemory<T>`. A chunk holds exactly the rows of one chunk and is valid until the next step of the loop, when its array goes back to the pool. Copy what you keep, and do not collect the chunks themselves (with `ToListAsync` or `Buffer`, say): nothing guards a memory held longer.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_chunk_stream)]

`ToBufferedAsyncStream` flattens each chunk into its rows. Each row is copied out before it is yielded, so a row has no lifetime to mind. A `break` out of `await foreach` disposes the enumerator, which cancels the query and returns every array still queued.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_element_stream)]

Always dispose the enumerator, which `await foreach` does. An enumerator that is abandoned gives nothing back: the producer keeps reading the whole query into its unbounded queue, so the connection is held until that read ends.

## Pooled memory

The rows of a buffer or a stream, and the chunks a collector (`ToListAsync`, `ToArrayAsync` and the rest) reads into before it copies the result once, live in arrays rented from a pool. By default the pool is `ArrayPool<T>.Shared`. An array goes back to its pool when the call, the buffer or the stream is done, and what is in it then is the question this section answers.

The measurements behind these choices (which pool, how rows are cleared, and where) are on [Pooling and clearing](benchmarks/pooling-and-clearing.md).

### What is cleared

A pooled array that goes back with rows in it keeps them reachable while it sits in the pool, and any other code in the process can rent the same array next. So ODX decides, for every array it rents, with one rule:

1. **Rows that hold references are always cleared.** A class, a string, a struct that contains either: the used rows are cleared before the array goes back, so a pooled array never keeps a row alive. Nothing turns this off.
2. **Rows with no references are cleared when asked.** An `int` or a plain struct holds nothing to keep alive, so clearing them costs time for no gain unless the values are sensitive. The default is not to clear them.

Two settings raise that default for rows with no references, and the more specific one wins.

**The app-wide switch** `Open.Database.Extensions.ClearPooledArrays` clears every array ODX rents, value rows included, for every query that does not say otherwise. It is off by default and is read once, so set it before the first query. The way to set it is in the application's project file, which puts it in `runtimeconfig.json`, where an operator can also change it:

```xml
<ItemGroup>
  <RuntimeHostConfigurationOption Include="Open.Database.Extensions.ClearPooledArrays" Value="true" />
</ItemGroup>
```

Or in code, before the first query: `AppContext.SetSwitch("Open.Database.Extensions.ClearPooledArrays", true)`.

**`UsePool(pool, clearOnReturn)`** decides for one query. It goes after `Map`, and both arguments are required, so the choice is always visible at the call. Write the second by name:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_clearing_shared)]

An explicit `clearOnReturn: true` clears value rows whatever the switch says. An explicit `clearOnReturn: false` keeps them as they are, also when the switch is on: this is the opt-out, and it is per query. It cannot lower the floor: `clear` controls rows with no references, and rows that hold references are always cleared.

### Your own pool

`UsePool` takes any `ArrayPool<T>`. Every pooling terminal of that query rents from it: the collectors and the three buffer terminals. `ArrayPool<T>.Shared` is what a query without `UsePool` uses.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_overloads)]

The rules of a pool of your own:

- **It is yours.** ODX rents from it and gives back to it, and never disposes it. A null pool throws `ArgumentNullException` at the call.
- **It decides how long its arrays are.** ODX takes what the pool gives, uses the whole array, writes only the rows it has, and clears only those, not the rest. A pool that gives a shorter array than was asked for makes the read throw `InvalidOperationException`; that array, and every array before it, is given back.
- **It may round up.** A pool that returns a longer array than asked for is fine, as `ArrayPool<T>.Shared` does; the extra room is used, and only the rows are copied.
- **Returning an array must not throw.** If it does, every other array is still returned, and the first exception is thrown after the last. In the cleanup queue (below) a return that throws is swallowed and costs only that array.
This pool counts what it hands out and what comes back, and the samples use it to check the claims on this page:

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_counting_pool)]

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_clearing)]

### When the clear happens

- What is cleared is the rows that were written, never the rest of a longer array. When every slot of an array was written, the pool is asked to clear it (`Return(array, clearArray: true)`); otherwise only the written slots are cleared and the array goes back without the flag.
- For a buffer, the clear takes effect when the buffer, or its `PooledArray<T>` view, is disposed. For a stream, as each chunk goes back: at the next step, or when the enumeration ends or is disposed. For a collector, when the call ends, after the result is built.
- A chunk under 128 KB of rows is cleared where it is released, which costs about as much as handing it over. A chunk of 128 KB or more of rows, value rows included, is cleared on one background loop instead, off the read path, and the loop also returns the array to the pool: neither the read nor the consumer waits for either.
- If 10,240 clears are ever waiting (a stalled custom pool, or an extreme burst), the next chunk is cleared and returned by the code that releases it, as a small one is. A dirty array never reaches a pool, and none is left behind with its rows.
- A custom pool's `Return` that blocks stalls the loop shared by every pool; the chunks that then pile up are cleared and returned by the code that releases them.

## Cancellation and disposal

- **Two sources, one token.** The caller's token and the dispose are combined, so either one cancels the database read itself, not only the loop around it.
- **Our own cancel is swallowed.** A cancel caused by disposing the buffer, or the stream's enumerator, is not reported: you asked for it. The read ends quietly, and an `Items` task you were already awaiting is cancelled.
- **Yours is delivered.** The caller's own cancel, or a fault in the read, is delivered: to a stream after the rows that were already read, and to a buffer through `Items`. A stream's next step throws it after the chunks that were already delivered. `Items` is cancelled or faulted instead of producing a view.
- **`DisposeAsync` waits for the producer, with no timeout.** It cancels a running read, waits until the producer has stopped, and returns the arrays (a large chunk that is to be cleared goes to the cleanup queue, so it is back in the pool shortly after `DisposeAsync` completes). A provider that ignores cancellation delays it by the call in progress. After that, the producer stops at the next chunk boundary at the latest. The synchronous `Dispose` does not wait. It cancels and returns at once. If the read is still running, the producer returns the arrays when it exits; if it had finished, `Dispose` returns them itself.
- **No finalizer.** A buffer that is never disposed is not a leak: it holds its arrays until the garbage collector takes it, and the pool does not get them back.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_cancel)]

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_dispose_early)]

## Memory shape

Chunks start at 16 rows and double: 16, 32, 64 and so on. A buffer keeps doubling, and never moves a row once it is written. That is what makes the indexer O(1): the chunk of a row is found with a shift and a log. A stream stops doubling at 4,096 rows, so a long result is read in chunks of at most 4,096.

## What it costs

The numbers come from a synthetic provider on net11, not from a real database, and they are one run each. The figure and the simulated-link numbers were measured with `AsyncReads()` on for every variant, so the refill waits are real awaits; with the default `Read()` the buffer and `ToArrayAsync` held the reader 5.26 and 5.23 ms at a 50 µs refill. The refill timer busy-waits on one of the four cores the run used, so the overlap ran on about three.

- Reader-open time stays flat as the consumer slows. For the chunk stream at 10,000 rows it was 6.29, 6.27 and 6.21 ms for the three consumers in the figure above.
- On a simulated link it is within a few percent of `ToArrayAsync`. At a 50 µs refill the buffer held the reader 6.27 ms and `ToArrayAsync` 6.29 ms.
- At zero latency and 10,000 rows, dumping a result into a buffer holds the reader about 22% longer than `ToArrayAsync`: 225 against 185 µs. The buffer pays that to be able to hand the rows to someone else.

## When to use something else

For a small result, or one that nobody else needs, `ToArrayAsync` or `ToListAsync` is the better tool. The connection is released as soon as the array or list is built, and there is nothing to dispose.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_small_result)]

To shape a stream's rows, use `System.Linq.Async` over `ToBufferedAsyncStream`, and leave the filtering to the SQL. It is in the box on net10 and later.

[!code-csharp[](../samples/AsyncBuffers.cs#buffers_linq_async)]

The channel terminals (`ToChannelReader()` and `ToChannel()`) left the typed views of Core in 11.0.0-rc.8. The buffer streams do the same job. At a million rows they held the connection for a shorter time than `ToChannelReader`; at 10,000 rows the two were about the same (the benchmark round is unpublished). See [Channels and Dataflow](channels-and-dataflow.md).

## Sources

| Claim | Value | Run |
|---|---|---|
| Reader-open time, chunk stream, 10,000 rows | 6.29, 6.27 and 6.21 ms at 39 ns per row, 1 µs per row and a 20 µs wait per 100 rows | the library's unpublished pooled-buffer benchmark round, synthetic provider, net11, a 50 µs refill every 100 rows; one run |
| Reader-open time, no buffer (the figure) | 6.62, 16.15 and 8.54 ms for the same consumers | same |
| Buffer against `ToArrayAsync`, simulated link | 6.27 and 6.29 ms, at a 50 µs refill every 100 rows | same round, same provider; one run |
| Buffer against `ToArrayAsync`, zero latency | 225 and 185 µs at 10,000 rows (about 22% longer) | same round, same provider; one run |
| The controller checks: status, body, pool balance, abort | RegisterForDispose, a dispose before serialization, a returned stream, a client abort: 5,000 rows, and 1,000,000 rows for the aborts | ASP.NET Core 11.0.0 with TestServer and with Kestrel on loopback, Microsoft.Data.Sqlite on a file database, a counting `MemoryPool<T>`; one run or a few. A scratch project, not part of the docs build |
| Chunk sizes, the cleanup queue, the single-use rule | 16 doubling to 4,096 for a stream; 10,240 waiting clears | the library source (`ChunkSizing`, `RecycleBin`, `ChunkStream`); not a measurement |
