---
title: Open.Database.Extensions (ODX)
---

![Open.Database.Extensions: You write the SQL. ODX writes the other twenty lines.](images/hero-core.svg)

Extensions on the standard ADO.NET types that make everyday database work short to write and robust by default. Version 11 is a release candidate.

[!code-csharp[](samples/BeforeAfter.cs#ode_list_factory)]

Here `author` is an `int` variable: the parameter name `author` comes from the argument.

## The idea

1. **A foundation of best-practice extensions.** Most of the library is a provider-agnostic collection of extensions on `IDbConnection`, `IDbCommand`, `IDataReader` and friends. They apply the usual best practices (open late, close early, dispose what you create, honor cancellation). See [The foundation](articles/foundation.md).
2. **Expressive commands on top.** Build a command from a connection factory or a connection, add parameters, and call a terminal. See [Expressive commands](articles/expressive-commands.md).
3. **Row mapping on top of that.** `Map<T>()` reads rows into your classes. See [Mapping rows to objects](articles/mapping.md).
4. **Performance.** See [Performance](articles/performance.md) for what is measured and what is not.

## How little code

The same four tasks written with ODX, Dapper, EF Core and plain ADO.NET are on [How little code](articles/how-little-code.md): every snippet is compiled and run, and this table counts the lines of each method, from the signature to the closing brace, without blank lines or comments.

| Task | ODX | Dapper | EF Core | plain ADO.NET |
|---|---:|---:|---:|---:|
| 1. Rows to a list | 7 | 9 | 6 | 22 |
| 2. Filter in SQL, project in C#, as an async stream | 10 | 14 | 8 | 22 |
| 3. The rows of one author in memory, then LINQ | 15 | 15 | 13 | 26 |
| 4. A stored procedure with two parameters | 11 | 14 | 12 | 29 |

ODX is not the smallest in every row: EF Core is shorter in the first three. The connection factory (ODX) and the context (EF Core) are set up once and are not counted: see the page for what each count includes. The ODX versions of the first two tasks:

[!code-csharp[](samples/HowLittleCode.cs#little_list_odx)]

[!code-csharp[](samples/HowLittleCode.cs#little_stream_odx)]

The stream method has no `async`, `await` or `using`: the stream opens the connection when it is first enumerated, and it can be enumerated once.

## Start here

| I want to... | Read |
|---|---|
| Install it and run a first query | [Getting started](articles/getting-started.md) |
| Stream rows into a channel or a dataflow block | [Channels and Dataflow](articles/channels-and-dataflow.md) |
| Return rows from an ASP.NET Core controller with no service layer | [A controller with no service layer](articles/no-service-layer.md) |
| See how little code it takes, against Dapper, EF Core and ADO.NET | [How little code](articles/how-little-code.md) |
| See how it differs from Dapper | [Coming from Dapper](articles/coming-from-dapper.md) |
| Look up a type or a method | [API reference](api/index.md) |

Every code sample in this guide is compiled by the build, and the ones that do not need SQL Server run against SQLite before the site is published.

## How it fits together

<img src="images/architecture.svg" alt="Four layers: a foundation of best-practice ADO.NET extensions, expressive commands on top of it, row mapping on top of that, all resting on a base of performance." style="max-width: 520px; width: 100%;" />

*Coming from earlier versions? See [Coming from earlier versions](articles/whats-new-11.md).*
