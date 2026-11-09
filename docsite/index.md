---
title: Open.Database.Extensions (ODX)
---

![Open.Database.Extensions: You write the SQL. The library writes the other twenty lines.](images/hero-core.svg)

Extensions on the standard ADO.NET types that make everyday database work short to write and robust by default. Version 11 is a release candidate.

[!code-csharp[](samples/BeforeAfter.cs#ode_list_factory)]

Here `author` is an `int` variable: the parameter name `author` comes from the argument.

## The idea

1. **A foundation of best-practice extensions.** Most of the library is a provider-agnostic collection of extensions on `IDbConnection`, `IDbCommand`, `IDataReader` and friends. They apply the usual best practices (open late, close early, dispose what you create, honor cancellation) so common database work is almost effortless. See [The foundation](articles/foundation.md).
2. **Expressive commands on top.** Build a command from a connection factory or a connection, add parameters, and call a terminal. See [Expressive commands](articles/expressive-commands.md).
3. **Row mapping on top of that.** `Map<T>()` reads rows into your classes. See [Mapping rows to objects](articles/mapping.md).
4. **All of it rests on performance.** The goal is to keep a connection open for as short a time as possible and to allocate as little as possible. See [Performance](articles/performance.md) for what is measured and what is not.

## Start here

| I want to... | Read |
|---|---|
| Install it and run a first query | [Getting started](articles/getting-started.md) |
| Stream rows into a channel or a dataflow block | [Channels and Dataflow](articles/channels-and-dataflow.md) |
| See how it differs from Dapper | [Coming from Dapper](articles/coming-from-dapper.md) |
| See what changed in 11 | [What's new in 11](articles/whats-new-11.md) |
| Look up a type or a method | [API reference](api/index.md) |

Every code sample in this guide is compiled by the build and run against SQLite before the site is published.

## How it fits together

<img src="images/architecture.svg" alt="Four layers: a foundation of best-practice ADO.NET extensions, expressive commands on top of it, row mapping on top of that, all resting on a base of performance." style="max-width: 520px; width: 100%;" />
