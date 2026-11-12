# Getting started

## Install

Most applications need one package. The others add integrations.

| Package | Use it for | Targets |
|---|---|---|
| `Open.Database.Extensions.Core` | Everything in this guide except channels, dataflow and SQL Server helpers. Provider-agnostic. | `netstandard2.0`, `netstandard2.1`, `net10.0`, `net11.0` |
| `Open.Database.Extensions.Channel` | [Reading into channels](channels-and-dataflow.md) | same |
| `Open.Database.Extensions.Dataflow` | [Reading into dataflow blocks](channels-and-dataflow.md) | same |
| `Open.Database.Extensions.MSSqlClient` | `SqlConnectionFactory` and SQL Server specializations on `Microsoft.Data.SqlClient` | `net472`, `net10.0`, `net11.0` |

.NET 8 and .NET 9 applications use the `netstandard2.1` build; .NET Framework applications use `netstandard2.0` (or `net472` for the SQL Server package). Version 11 is a release candidate, so ask for prereleases:

```sh
dotnet add package Open.Database.Extensions.Core --prerelease
```

Core does not reference any database provider. Bring your own: the examples in this guide use `Microsoft.Data.Sqlite` so that you can run them without a server:

```sh
dotnet add package Microsoft.Data.Sqlite
```

`connectionString` in the examples is your own, such as `"Data Source=blog.db"`. The runnable samples use a shared in-memory SQLite database that [`Db.cs`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/samples/Db.cs) creates.

## Your first query

A model is a class with public properties, as below. They can be read-only after construction: `init` properties work (C# 9), and so does `required` (C# 11), which here makes a query without an `id` column fail on its first row. A positional record or a class with one public constructor works too: it is built through that constructor (see [Records and constructors](mapping.md#records-and-constructors)).

[!code-csharp[](../samples/Models.cs#post_model)]

Columns are matched to property and constructor parameter names exactly, else ignoring case, else ignoring case and underscores (`first_name` feeds `FirstName`); see [Mapping rows to objects](mapping.md).

A command needs a connection. A connection factory creates them, and you create it once per application:

[!code-csharp[](../samples/ConnectionPattern.cs#ode_setup_once)]

There are two ways to start a command: from the factory, or from a connection you own. Both end the same way, with `AddParam` for parameters and a terminal (the call that runs the query, such as `ToListAsync()`). In the samples `author` is a variable (`int author = 42;`) and `AddParam(author)` names the parameter `author` after it; `AddParam("author", 42)` names it yourself. Side by side, row against row ("nothing" marks a step the factory makes unnecessary):

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

Lines you write, counted from `plain_list_aligned` and `ode_list_aligned` (`ConnectionPattern.cs`; not counting blank lines, `// nothing` comments or the `int author = 42;` that declares the value): plain ADO.NET 20, Open.Database.Extensions 5, plus one line of one-time setup per application. The README table counts the same job.

What is different:

- **Plain ADO.NET** needs you to open the connection, create and dispose the command and the reader, add each parameter by hand and copy each column by ordinal, with an `IsDBNull` check for nullable columns.
- **With the extensions** the factory opens and disposes the connection, the command is one call, and the terminal does the read loop and the mapping. Matching columns to properties is by name, ignoring case and underscores.

## With SQL Server

![Open.Database.Extensions.MSSqlClient: You write the SQL. ODX writes the other twenty lines.](../images/hero-mssqlclient.svg)

`Open.Database.Extensions.MSSqlClient` provides `SqlConnectionFactory`. Everything above works the same way, and stored procedures are one call away:

[!code-csharp[](../samples/SqlServer.cs#sql_server_factory)]

## Next steps

- [The foundation](foundation.md) explains the extensions underneath, and who owns a connection.
- [Expressive commands](expressive-commands.md) covers parameters and every terminal.
- [Mapping rows to objects](mapping.md) lists exactly what `Map<T>()` does and does not do.
