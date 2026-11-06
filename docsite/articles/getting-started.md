# Getting started

## Install

Most applications need one package. The others add integrations.

| Package | Use it for | Targets |
|---|---|---|
| `Open.Database.Extensions.Core` | Everything in this guide except channels, dataflow and SQL Server helpers. Provider-agnostic. | `netstandard2.0`, `netstandard2.1`, `net10.0` |
| `Open.Database.Extensions.Channel` | [Reading into channels](channels-and-dataflow.md) | same |
| `Open.Database.Extensions.Dataflow` | [Reading into dataflow blocks](channels-and-dataflow.md) | same |
| `Open.Database.Extensions.MSSqlClient` | `SqlConnectionFactory` and SQL Server specializations on `Microsoft.Data.SqlClient` | `net472`, `net10.0` |

.NET 8 and .NET 9 applications use the `netstandard2.1` build; .NET Framework applications use `netstandard2.0` (or `net472` for the SQL Server package). Version 11 is a preview, so ask for prereleases:

```sh
dotnet add package Open.Database.Extensions.Core --prerelease
```

Core does not reference any database provider. Bring your own: the examples in this guide use `Microsoft.Data.Sqlite` so that you can run them without a server.

## Your first query

A model needs a public parameterless constructor and public settable properties. Columns are matched to property names, ignoring case.

[!code-csharp[](../samples/Models.cs#post_model)]

There are two ways to start a command. Both end the same way, with `AddParam` for parameters and a terminal that runs the query.

**From a connection factory.** The command takes a connection from the factory when a terminal runs, opens it, and disposes it afterwards:

[!code-csharp[](../samples/ExpressiveCommands.cs#factory_form)]

**From a connection you already have.** You own the connection. The command opens it only if it is closed and closes only what it opened:

[!code-csharp[](../samples/ExpressiveCommands.cs#connection_form)]

## With SQL Server

`Open.Database.Extensions.MSSqlClient` provides `SqlConnectionFactory`. Everything above works the same way, and stored procedures are one call away:

[!code-csharp[](../samples/SqlServer.cs#sql_server_factory)]

## Next steps

- [The foundation](foundation.md) explains the extensions underneath, and who owns a connection.
- [Expressive commands](expressive-commands.md) covers parameters and every terminal.
- [Mapping rows to objects](mapping.md) lists exactly what `Map<T>()` does and does not do.
