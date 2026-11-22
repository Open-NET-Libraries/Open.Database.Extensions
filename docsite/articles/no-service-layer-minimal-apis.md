# With minimal APIs

The [command factory](no-service-layer.md#the-command-factory) does not care what calls it. With minimal APIs, the endpoints ask for `PostCommands` as a parameter, and the rest is the same as in a controller: start the read, register the buffer, await the rows, return them.

[!code-csharp[](../samples/NoServiceLayerMinimal.cs#nsl_minimal_endpoints)]

`Program.cs` reads the connection string from configuration (`ConnectionStrings:Posts` in `appsettings.json`), registers the command factory and maps the endpoints. `MapPostEndpoints` is the extension method above, in the static class `PostEndpoints`:

[!code-csharp[](../samples/NoServiceLayerMinimal.cs#nsl_minimal_program)]

What each endpoint does:

- **The buffer.** `HttpContext` is a parameter, for `RequestAborted` and `Response.RegisterForDispose`. The rows are written as a JSON array after the lambda returns, and the arrays go back to the pool after the response. The rules are the controller's: register before you await, and never `await using` a buffer whose rows you return. See [The controller](no-service-layer.md#the-controller) and [Caveats](no-service-layer.md#caveats).
- **The list.** `ToListAsync` returns a `ValueTask<List<Post>>`, and the endpoint returns it as it is: no `async`, no `await`, nothing to register.

The samples project runs both endpoints next to the controllers: they write the same JSON, and every array the buffer endpoint rents comes back. The tests on [What do you test now](no-service-layer.md#what-do-you-test-now) work the same way: point the `WebApplicationFactory` client at the endpoint's route.
