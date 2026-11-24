# How little code

The same four tasks, written with Open.Database.Extensions (ODX), Dapper, EF Core and plain ADO.NET. This page is about the size of the code and nothing else: it makes no claim about speed or safety. Every snippet is compiled in the samples project, and the samples run all four versions of a task and check that they return the same rows.

Everything here is async. ODX, Dapper and plain ADO.NET share the same SQL string in each of tasks 1 to 3 (tasks 2 and 3 add `where author = @author`); EF Core writes the same query as LINQ. The tasks read the `posts` table of the [sample database](getting-started.md#your-first-query) into the `Post` class.

## How the lines are counted

Each cell counts the lines of one method as it would appear in a data-access class: the signature through the closing brace, without blank lines and without comments. `using` directives are not counted. The numbers below are the output of `docsite/tools/count-region-lines.ps1`. The code is written vertically, one argument and one call per line, the same way for all four libraries.

| Task | Open.Database.Extensions | Dapper | EF Core | plain ADO.NET |
|---|---:|---:|---:|---:|
| 1. Rows to a list | 7 | 9 | 6 | 22 |
| 2. Filter in SQL, project in C#, as an async stream | 10 | 14 | 8 | 22 |
| 3. The rows of one author in memory, then LINQ | 15 | 15 | 13 | 26 |
| 4. A stored procedure with two parameters | 11 | 14 | 12 | 29 |

What is outside the count, and what each cell includes:

- **ODX** needs a connection factory, created once per application and passed in (`db` below). Setup is `new DbConnectionFactory<SqliteConnection>(...)` for any provider (three lines) or `new SqlConnectionFactory(connectionString)` for SQL Server (one line). It is not counted.
- **EF Core** needs a `DbContext` that maps the model. The one in these samples is five lines (`little_setup_ef` below) and is not counted. The context is also passed in, and EF Core reads `Post` through it.
- **Dapper and plain ADO.NET** have no setup, so each method creates its own connection from a connection string: that line is counted. An application that already has a connection factory for them would save that line.
- The methods take the connection source (`db`, `connectionString` or the context) and a `CancellationToken` as arguments, so the signatures are comparable. Dapper's `QueryAsync` only takes a token inside a `CommandDefinition`, and the Dapper snippets pass it that way; the Dapper stream passes it through `WithCancellation`.
- EF Core tracks the entities it returns by default. The snippets do not call `AsNoTracking()`, which would add one line to each EF Core method that reads a table.

The setup that EF Core needs:

[!code-csharp[](../samples/HowLittleCode.cs#little_setup_ef)]

## Task 1: rows to a list

All of the rows of the table, in order, as a `List<Post>`.

**Open.Database.Extensions**

[!code-csharp[](../samples/HowLittleCode.cs#little_list_odx)]

**Dapper**

[!code-csharp[](../samples/HowLittleCode.cs#little_list_dapper)]

**EF Core**

[!code-csharp[](../samples/HowLittleCode.cs#little_list_ef)]

**Plain ADO.NET**

[!code-csharp[](../samples/HowLittleCode.cs#little_list_ado)]

Dapper opens a closed connection for the call, so it has no `OpenAsync`. ADO.NET does everything by hand: the connection, the command, the reader, and copying each column into a property.

## Task 2: filter in SQL, project in C#, as an async stream

One author's posts, each turned into a headline string by a small C# method (`Headline`, below), returned as an `IAsyncEnumerable<string>` that the caller enumerates. The filter is in the SQL (`where author = @author`) for all four. The headline is built in C# because SQL does not build it, and it is shared by all four, so it is not counted:

[!code-csharp[](../samples/HowLittleCode.cs#little_headline)]

**Open.Database.Extensions**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_odx)]

The method has no `async`, no `await` and no `using`. The stream opens the connection when it is first enumerated, reads the rows and releases the connection when the last row is read, so there is nothing for the method to dispose. A stream is single-use: it can be enumerated once, and a second enumeration throws `InvalidOperationException`. See [Pooled buffers and streams](async-buffers.md).

**Dapper**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_dapper)]

`QueryUnbufferedAsync` streams the rows, but the connection must stay open while they are read, so the method has to be an async iterator that owns and disposes the connection.

**EF Core**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_ef)]

The `Where` is translated to SQL, so the database does the same filtering as in the other three. `AsAsyncEnumerable()` streams the entities, and `Headline` runs over them in C#. Two differences are not in the line count: the context must stay alive until the enumeration ends, and the token is passed by whoever enumerates (`WithCancellation`), so the method has no token argument.

**Plain ADO.NET**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_ado)]

This is an async iterator too, and it builds a `Post` from each row before calling `Headline`.

## Task 3: one author's rows in memory, then LINQ over them

Read one author's rows (the filter is in the SQL again), then build the headlines from the rows already in memory with ordinary LINQ.

**Open.Database.Extensions**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_odx)]

`ToPooledBuffer` reads into pooled arrays and `Items` is a read-only list over them. Disposing the view disposes the buffer and returns the arrays to the pool. See [Lifetime and ownership](async-buffers.md#lifetime-and-ownership).

**Dapper**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_dapper)]

**EF Core**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_ef)]

**Plain ADO.NET**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_ado)]

## Task 4: a stored procedure with two parameters

A procedure on SQL Server that takes `@author` and `@title_contains`. SQLite has no stored procedures, so this task uses SQL Server types; the samples run it only where SQL Server LocalDB is reachable.

**Open.Database.Extensions**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_odx)]

The parameter names come from the arguments (`author` and `titleContains` find `@author` and `@title_contains`). See [Name a parameter from its argument](expressive-commands.md#name-a-parameter-from-its-argument) and [Expressive commands](expressive-commands.md).

**Dapper**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_dapper)]

**EF Core**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_ef)]

**Plain ADO.NET**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_ado)]

## What the numbers say

ODX is not the smallest in every cell. Once its context, which the table does not count, is in place, EF Core is shorter than ODX in tasks 1 to 3 (by 1, 2 and 2 lines), and ODX is shorter in task 4 (by 1). ODX is shorter than Dapper in tasks 1, 2 and 4, equal to it in task 3 (15 lines each), and shorter than plain ADO.NET in all four. Where ODX needs less is the plumbing: no connection to create, open or dispose in the method, and none of the column copying that ADO.NET needs.

Each cell names a region of [`HowLittleCode.cs`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/samples/HowLittleCode.cs).

See also [Coming from Dapper](coming-from-dapper.md) for the steps side by side.
