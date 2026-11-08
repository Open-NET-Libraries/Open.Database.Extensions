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

Core does not reference any database provider. Bring your own: the examples in this guide use `Microsoft.Data.Sqlite` so that you can run them without a server:

```sh
dotnet add package Microsoft.Data.Sqlite
```

The `connectionString` in the examples is `Db.ConnectionString`, defined in [`Db.cs`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/samples/Db.cs): a shared in-memory SQLite database.

## Your first query

A model is a class with public settable properties, as below, or a positional record or a class with one public constructor, which is built through that constructor (see [Records and constructors](mapping.md#records-and-constructors)). Columns are matched to property and constructor parameter names exactly, else ignoring case, else ignoring case and underscores (`first_name` feeds `FirstName`); see [Mapping rows to objects](mapping.md).

[!code-csharp[](../samples/Models.cs#post_model)]

There are two ways to start a command. Both end the same way, with `AddParam` for parameters and a terminal (the call that runs the query, such as `ToList()`). `connectionFactory` is created once per application, as shown after the table. Side by side, row against row ("nothing" marks a step the factory makes unnecessary):

<div class="side-by-side">
<div>

**From a connection factory**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_list_call)]

</div>
<div>

**From a connection you own**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_list_own)]

</div>
</div>

The factory is created once per application:

[!code-csharp[](../samples/ConnectionPattern.cs#ode_setup_once)]

What is different:

- **Who creates the connection.** The factory, for each execution; or you.
- **Open and close.** In both forms the command opens the connection only if it is closed. The factory form disposes the connection when the terminal returns. In the connection form the command closes only what it opened and never disposes it.

## Before and after

The same query with plain ADO.NET and with the extensions, row against row: the connection, the command, the parameter, then the read loop and the mapping.

<div class="side-by-side">
<div>

**Plain ADO.NET**

[!code-csharp[](../samples/ConnectionPattern.cs#plain_list_aligned)]

</div>
<div>

**Open.Database.Extensions (factory)**

[!code-csharp[](../samples/ConnectionPattern.cs#ode_list_aligned)]

</div>
</div>

Lines you write per call in the two samples above, not counting blank lines or the `// nothing` comment: plain ADO.NET 20, Open.Database.Extensions 4 (plus one line of one-time setup per application).

What is different:

- **Plain ADO.NET** needs you to open the connection, create and dispose the command and the reader, add each parameter by hand and copy each column by ordinal, with an `IsDBNull` check for nullable columns.
- **With the extensions** the factory opens and disposes the connection, the command is one call, and the terminal does the read loop and the mapping. Matching columns to properties is by name, ignoring case and underscores.

## With SQL Server

`Open.Database.Extensions.MSSqlClient` provides `SqlConnectionFactory`. Everything above works the same way, and stored procedures are one call away:

[!code-csharp[](../samples/SqlServer.cs#sql_server_factory)]

## Next steps

- [The foundation](foundation.md) explains the extensions underneath, and who owns a connection.
- [Expressive commands](expressive-commands.md) covers parameters and every terminal.
- [Mapping rows to objects](mapping.md) lists exactly what `Map<T>()` does and does not do.
