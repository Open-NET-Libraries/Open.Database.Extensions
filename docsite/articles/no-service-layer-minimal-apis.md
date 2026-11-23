# With minimal APIs

The [command factory](no-service-layer.md#the-command-factory) does not care what calls it. With minimal APIs, the endpoints ask for `PostCommands` as a parameter, and the rest is the same as in a controller: start the read, register the buffer, return the task of the rows.

[!code-csharp[](../samples/NoServiceLayerMinimal.cs#nsl_minimal_endpoints)]

`Program.cs` reads the connection string from configuration (`ConnectionStrings:Posts` in `appsettings.json`), registers the command factory and maps the endpoints. `MapPostEndpoints` is the extension method above, in the static class `PostEndpoints`:

[!code-csharp[](../samples/NoServiceLayerMinimal.cs#nsl_minimal_program)]

What each endpoint does:

- **The buffer.** `HttpContext` is a parameter, for `RequestAborted` and the response. [`RegisterForDispose(http.Response)`](no-service-layer.md#registerfordispose) registers the buffer with the response and returns the task of its rows, which the lambda returns without awaiting it, as the controller does. The framework awaits the rows and writes them as a JSON array, and the arrays go back to the pool after the response. The rule is the controller's: never dispose a buffer whose rows you return (no `using`, no `await using`). See [The controller](no-service-layer.md#the-controller) and [Caveats](no-service-layer.md#caveats).
- **The list.** `ToListAsync` returns a `ValueTask<List<Post>>`, and the endpoint returns it as it is, with nothing to register.

The tests on [What to test](no-service-layer.md#what-to-test) work the same way: point the `WebApplicationFactory` client at the endpoint's route.
