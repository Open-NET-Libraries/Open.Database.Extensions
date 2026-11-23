# A controller with no service layer

The usual stack reads rows into entities, copies the entities into DTOs, collects the DTOs in a list and hands the list to the serializer. With Open.Database.Extensions the controller calls a query that a command factory defines once, takes the rows in a pooled buffer and returns them. ASP.NET Core writes the JSON from those rows, and the buffer's arrays go back to the pool after the response.

You define the command; the controller picks the shape of the result.

<div style="overflow-x: auto; margin: 1rem 0;"><img src="../images/no-service-layer-copies.svg" alt="Two lanes. Classic: database, repository with a list of entities, service with a list of DTOs, serializer, JSON. Two sets of row objects and two lists, all left to the garbage collector. ODX: database, command factory with Post rows in pooled arrays, serializer, JSON. One set of row objects; after the response the arrays go back to the pool." style="display: block; width: 100%; min-width: 600px; height: auto;"></div>

## Side by side

Both versions read the posts of one author and write the same JSON. `PostEntity` and `PostDto` (not shown) have the same four properties as `Post`. The empty rows on the right are where the service was.

<div class="side-by-side">
<div>

**Classic: repository, service, DTOs**

[!code-csharp[](../samples/NoServiceLayerClassic.cs#nsl_classic)]

</div>
<div>

**ODX: a command factory, and nothing in between**

[!code-csharp[](../samples/NoServiceLayer.cs#nsl_odx)]

</div>
</div>

What is gone:

- **The repository** becomes one definition, `DbQuery.Text(...).Map<Post>()`, made once in the [command factory](#the-command-factory).
- **The service** and its mapping code: there is nothing to copy.
- **The second set of objects.** The rows exist once, as `Post` objects in pooled arrays. The classic version builds a list of entities, then a list of DTOs, and leaves both to the garbage collector.

## The command factory

A command factory is a class that holds the queries of one area of the app. Each query is defined once: the SQL and the `Map<Post>()`, with no connection, immutable, safe in a static field.

`Post` is the model of [Getting started](getting-started.md#your-first-query), and `ListSql` is `select * from posts where author = @author order by id`.

The definition, `ByAuthorQuery`, is a `MappedDbQuery<Post>`. `ByAuthor` binds it to a connection factory, the author and a pool, and returns a `MappedQuery<Post>`: a query with everything it needs. Nothing runs yet: the call that reads the rows (the terminal) is the caller's choice. The same definition gives a pooled buffer to the controller above, a list to the one below, and a list to a test.

The entry point of the app provides the connection factory (SQLite in these samples) and the pool:

[!code-csharp[](../samples/NoServiceLayer.cs#nsl_registration)]

`Program.cs` reads the connection string from configuration and calls it next to `AddControllers()`: `builder.Services.AddPostCommands(connectionString);`, then maps the controllers with `app.MapControllers()`. [With minimal APIs](no-service-layer-minimal-apis.md) shows those lines.

`UsePool(pool, clearOnReturn)` hands the query its pool; both arguments are required. `clearOnReturn` decides only for rows with no references: pass `true` when those values are sensitive, `false` to skip the clear. A `Post` holds strings, so its rows are always cleared before an array goes back, whatever you pass. See [What is cleared](async-buffers.md#what-is-cleared).

## The controller

Read the controller of the side-by-side from the top:

1. `ToAsyncBuffer(HttpContext.RequestAborted)` starts the read and returns the buffer at once; the read stops when the client leaves.
2. `Response.RegisterForDispose(buffer)` hands the buffer to the framework, which disposes it when the request is over. Register it before you return; [Caveats](#caveats) says what was observed when a request fails.
3. The action returns `buffer.Items`, the task of the rows, as it is: nothing in the action runs after the rows are ready, so there is no `async` and no `await`. The framework awaits it. The task completes when the read is over and the connection released, so nothing waits on the database while the JSON is written.
4. The declared type is `Task<PooledArray<Post>>`. A `PooledArray<Post>` is an `IReadOnlyList<Post>`, so the JSON is the same: `System.Text.Json` writes it as an array.
5. After the response is written, the framework disposes the buffer, and the arrays go back to the pool.

<div style="overflow-x: auto; margin: 1rem 0;"><img src="../images/no-service-layer-lifetime.svg" alt="A timeline of one request. The action starts the read, registers the buffer and returns at once. The connection is open only while the read runs. The rows stay in pooled arrays from the read until the response is over. The framework awaits the rows, then writes the JSON. When the response is over the framework disposes the buffer and the arrays go back to the pool." style="display: block; width: 100%; min-width: 600px; height: auto;"></div>

What was observed on a real server, with a client abort and an action that throws, is on [Returning it from a controller](async-buffers.md#returning-it-from-a-controller). The use case behind the buffer is on [The controller story](async-buffers.md#the-controller-story).

### Or a list

The buffer is the default, whatever the size of the result: the rows exist once, as `Post` objects in pooled arrays, no list is built, and the arrays are reused by the next request. It is not faster: on a synthetic provider, dumping a result into a buffer held the reader longer than `ToArrayAsync` at zero latency, and about as long on a simulated link. See [What it costs](async-buffers.md#what-it-costs).

Take a `List<Post>` when you need a list that you keep or change. The same command factory gives one (here the action only returns it, to show the shape):

[!code-csharp[](../samples/NoServiceLayer.cs#nsl_list)]

`ToListAsync` returns a `ValueTask<List<Post>>`, and the action returns it as it is, like the buffer's task. There is nothing to register, and the list is yours. What it costs: the rows are read into pooled chunks and then copied once into the list, and the list is garbage after the response. See [When to use something else](async-buffers.md#when-to-use-something-else).

## ETag: read the rows twice

The rows of a buffer (`buffer.Items`) stay in memory as an `IReadOnlyList<Post>`, so they can be read twice: once to compute an ETag, then again by the serializer. When the client already has those rows, the answer is `304 Not Modified` and nothing is serialized.

[!code-csharp[](../samples/NoServiceLayer.cs#nsl_etag)]

The hash covers every field the response carries, in order, and it encodes the text as UTF-8, so it is the same on every server:

[!code-csharp[](../samples/NoServiceLayer.cs#nsl_etag_hash)]

- When `Post` gains a field that the response carries, add it to the hash, or clients keep a `304` for rows that changed.
- A stream from `ToBufferedAsyncStream` can be read once. To hash it and then write it, you would query again or copy the rows first.
- The query still runs. A `304` saves the serialization and the bytes on the wire, not the read.
- The `304` carries the ETag, as a `200` would.
- If you change the JSON options (property names, say), clients keep ETags that still match: put a version string into the hash when you do.

## What to test

With no service layer there is no pass-through code to mock. What is left to test is the SQL with its mapping, and the endpoint. The tests below are written for xUnit v3, and the samples project compiles and runs them against its in-memory SQLite database.

**The command factory, on a real database.** The SQL and the mapping are the logic, so test them where they run. `DatabaseFixture` is yours to write: its constructor creates and fills the test database, and its `ConnectionFactory` property is an `IDbConnectionFactory<SqliteConnection>` for that database. Every test class that touches that database joins one xUnit collection, so the database is filled once and one class does not read while another writes.

[!code-csharp[](../samples/NoServiceLayerTests.cs#nsl_test_commands)]

**The controller, end to end**, with `WebApplicationFactory` from `Microsoft.AspNetCore.Mvc.Testing`, and **one lifetime guard**: every array the request rented came back to the pool.

[!code-csharp[](../samples/NoServiceLayerTests.cs#nsl_test_endpoint)]

- `CountingPool<T>` is the pool on [Your own pool](async-buffers.md#your-own-pool). The test hands it in through the services.
- The client can see the response before the framework disposes the buffer, so the guard waits for the arrays, with a time limit.
- The guard is the test that fails when the action on the route it requests does not call `RegisterForDispose`: the response is the same and no array comes back.
- Run the guard on a request that succeeds. When the action threw, or the client aborted before the response started, the test server did not dispose what the request had registered; Kestrel did. See the note on [Returning it from a controller](async-buffers.md#returning-it-from-a-controller).

## Caveats

- **Nothing may hold the rows past the response.** Do not keep the `PooledArray<Post>` in a field or a cache, and do not hand it to work that outlives the request: once the buffer is disposed, the view throws `ObjectDisposedException`. Copy what you keep with `ToList()`. See [Lifetime and ownership](async-buffers.md#lifetime-and-ownership).
- **Register before you return the task.** Call `RegisterForDispose` before `return buffer.Items`, so that the framework disposes it when the request is over. On Kestrel, a registered buffer was still disposed when the client aborted, and a registered object when the action threw; that was observed with an action that awaited `Items`, not yet with this form.
- **Do not dispose it in the action.** A `using` or an `await using` on a buffer whose rows the action returns disposes it before the framework serializes them; with `await using`, the response on Kestrel was a `500`. See [The trap](async-buffers.md#the-trap-dispose-before-the-framework-serializes).
- **The buffer holds the whole result until the response is written.** For a very large result, return the stream: see [A very large result](async-buffers.md#a-very-large-result-return-the-stream).
