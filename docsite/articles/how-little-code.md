# How little code

The same four tasks, written with Open.Database.Extensions (ODX), Dapper, EF Core and plain ADO.NET. This page is about the size of the code and nothing else: it makes no claim about speed or safety. Every snippet is compiled in the samples project, and the samples run all four versions of a task and check that they return the same rows.

Everything here is async, and the SQL is the same string for all four libraries in tasks 1 to 3. The tasks read the `posts` table of the [sample database](getting-started.md#your-first-query) into the `Post` class.

## How the lines are counted

Each cell counts the lines of one method as it would appear in a data-access class: the signature through the closing brace, without blank lines and without comments. `using` directives are not counted. The same script counts every cell (`docsite/tools/count-region-lines.ps1`), and the numbers below are its output, not a hand count. The code is written vertically, one argument and one call per line, the same way for all four libraries.

| Task | Open.Database.Extensions | Dapper | EF Core | plain ADO.NET |
|---|---:|---:|---:|---:|
| 1. Rows to a list | 7 | 9 | 6 | 22 |
| 2. Filter and project, as an async stream | 9 | 14 | 7 (closest equivalent) | 15 (closest equivalent) |
| 3. The whole set, then LINQ over it | 14 | 12 | 12 | 25 |
| 4. A stored procedure with two parameters | 11 | 14 | 12 | 29 |

What is outside the count, and what each cell includes:

- **ODX** needs a connection factory, created once per application and passed in (`db` below). Setup is `new DbConnectionFactory<SqliteConnection>(...)` for any provider (three lines) or `new SqlConnectionFactory(connectionString)` for SQL Server (one line). It is not counted.
- **EF Core** needs a `DbContext` that maps the model. The one in these samples is five lines (`little_setup_ef` below) and is not counted. The context is also passed in, and EF Core reads `Post` through it.
- **Dapper and plain ADO.NET** have no setup, so each method creates its own connection from a connection string: that line is counted. An application that already has a connection factory for them would save that line.
- The methods take the connection source (`db`, `connectionString` or the context) and a `CancellationToken` as arguments, so the signatures are comparable. Dapper's `QueryAsync` only takes a token inside a `CommandDefinition`, and the Dapper snippets use it, so the token is not silently dropped.
- EF Core tracks the entities it returns by default. The snippets do not call `AsNoTracking()`, which would add one line to each EF Core method that reads a table.

The setup that EF Core needs:

[!code-csharp[](../samples/HowLittleCode.cs#little_setup_ef)]

A cell marked "closest equivalent" cannot do the task the same way. The two cases are under task 2.

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

## Task 2: filter and project with async LINQ

The titles of one author's posts, returned as an `IAsyncEnumerable<string>` that the caller enumerates.

**Open.Database.Extensions**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_odx)]

The method has no `async`, no `await` and no `using`. The stream opens the connection when it is first enumerated, reads the rows and releases the connection when the last row is read, so there is nothing for the method to dispose. A stream is single-use: it can be enumerated once, and a second enumeration throws `InvalidOperationException` (the samples check this). See [Async buffers and streams](async-buffers.md).

**Dapper**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_dapper)]

`QueryUnbufferedAsync` streams the rows, but the connection must stay open while they are read, so the method has to be an async iterator that owns and disposes the connection.

**EF Core (closest equivalent)**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_ef)]

`AsAsyncEnumerable()` streams the entities, and the filter and the projection run over them in memory, as in the other three. The context must stay alive until the enumeration ends, and the token is passed by whoever enumerates (`WithCancellation`), so the method has no token argument. EF Core usually does this differently: it puts the filter and the projection into the SQL, so the database sends only the titles. That is not the same work, so it is not in the table. It is also compiled and checked in the samples (`little_stream_ef_sql`, 7 lines):

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_ef_sql)]

**Plain ADO.NET (closest equivalent)**

[!code-csharp[](../samples/HowLittleCode.cs#little_stream_ado)]

This is an async iterator too. It reads the two columns it needs straight from the reader and never builds a `Post`, which is shorter than building one and is why this is the closest equivalent and not the same work.

## Task 3: the whole set, then LINQ over it

Read every row, then filter and project the rows already in memory with ordinary LINQ. This is the one place the page uses a synchronous call, and it runs over a collection that was already read.

**Open.Database.Extensions**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_odx)]

`ToAsyncBuffer` reads into pooled arrays and `Items` is a read-only list over them. Disposing the view disposes the buffer and returns the arrays to the pool. See [Lifetime and ownership](async-buffers.md#the-items-view).

**Dapper**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_dapper)]

**EF Core**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_ef)]

**Plain ADO.NET**

[!code-csharp[](../samples/HowLittleCode.cs#little_buffer_ado)]

## Task 4: a stored procedure with two parameters

A procedure on SQL Server that takes `@author` and `@title_contains`. SQLite has no stored procedures, so this task uses SQL Server types, and the samples run it only when SQL Server LocalDB can be reached. Where it cannot, the snippets are compiled and the run reports the task as skipped.

**Open.Database.Extensions**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_odx)]

The parameter names come from the arguments (`author` and `titleContains` find `@author` and `@title_contains`). See [Name a parameter from its argument](expressive-commands.md#name-a-parameter-from-its-argument) and [Stored procedures](expressive-commands.md).

**Dapper**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_dapper)]

**EF Core**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_ef)]

**Plain ADO.NET**

[!code-csharp[](../samples/HowLittleCode.cs#little_proc_ado)]

## What the numbers say

ODX is not the smallest in every cell. EF Core is shorter in tasks 1 and 2 once its context, which the table does not count, is in place. Dapper and EF Core are two lines shorter in task 3, where ODX spends lines on the buffer and on disposing it. ODX is the smallest in task 4, and it is shorter than Dapper and plain ADO.NET in tasks 1 and 2. Where ODX needs less is the plumbing: no connection to create, open or dispose in the method, and none of the column copying that ADO.NET needs.

The table is a table of task by library, and each cell names a region of [`HowLittleCode.cs`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/samples/HowLittleCode.cs), so a cell can be replaced by another snippet and the script run again.

See also [Coming from Dapper](coming-from-dapper.md) for the steps side by side.
