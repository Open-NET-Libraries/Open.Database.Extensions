# Open.Database.Extensions

[![NuGet](https://img.shields.io/nuget/vpre/Open.Database.Extensions.Core.svg?style=flat)](https://www.nuget.org/packages/Open.Database.Extensions.Core/)
[![Docs](https://github.com/Open-NET-Libraries/Open.Database.Extensions/actions/workflows/docs.yml/badge.svg)](https://github.com/Open-NET-Libraries/Open.Database.Extensions/actions/workflows/docs.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/LICENSE)

**You write the SQL. This library writes the other twenty lines.**

Open.Database.Extensions turns the ADO.NET boilerplate around your own SQL into one fluent command: create the connection, add the parameters, run it, map the rows, dispose everything, the way ADO.NET best practice says. It works with any ADO.NET provider, is built for short connection times and low allocations, and sits on the standard types you already use.

**[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/)** · [Getting started](https://open-net-libraries.github.io/Open.Database.Extensions/articles/getting-started.html) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Benchmarks](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md)

## Before and after

Fetch one author's posts, with plain ADO.NET:

```cs
var posts = new List<Post>();

using var connection = new SqliteConnection(Db.ConnectionString);
connection.Open();

using var command = connection.CreateCommand();
command.CommandText = "select id, author, title, body from posts where author = @author order by id";
var parameter = command.CreateParameter();
parameter.ParameterName = "@author";
parameter.Value = 42;
command.Parameters.Add(parameter);

using var reader = command.ExecuteReader();
while (reader.Read())
{
    posts.Add(new Post
    {
        Id = reader.GetInt32(0),
        Author = reader.GetInt32(1),
        Title = reader.GetString(2),
        Body = reader.IsDBNull(3) ? null : reader.GetString(3),
    });
}
```

And with Open.Database.Extensions (`connectionFactory`, created once per application, is explained just below):

```cs
Post[] posts = await connectionFactory
    .Command("select * from posts where author = @author order by id")
    .AddParam("author", 42)
    .Map<Post>()
    .ToArrayAsync();
```

`connectionFactory` is created once per application, for example `new DbConnectionFactory<SqliteConnection>(() => new SqliteConnection(connectionString))`; see [Registering the factory once](https://open-net-libraries.github.io/Open.Database.Extensions/articles/coming-from-dapper.html#registering-the-factory-once).

The same four jobs, counted from the `plain_*` and `ode_*` regions of [`docsite/samples/BeforeAfter.cs`](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/samples/BeforeAfter.cs) (lines of code, not counting blank lines):

| Job | Plain ADO.NET | Open.Database.Extensions |
|---|---:|---:|
| A list of mapped objects | 20 | 5 |
| One row | 20 | 5 |
| A scalar | 9 | 4 |
| A command with no result set | 9 | 4 |

What you no longer write: opening and closing the connection, creating the command and setting its text, creating and adding every parameter, the read loop, copying columns into properties (ordinals, `IsDBNull`), and disposing all of it.

## Why it is fast

- **Measured against Dapper 2.1.89 on an in-memory provider:** `Map<T>()` is roughly 2 to 3 times faster at 100 or more rows, with about a third of the allocations (0.33 to 0.38 of Dapper's on multi-row results).
- **With a real database the database dominates:** against SQL Server LocalDB it is on par to a few percent faster (ratios 0.89 to 1.08, mostly ties) with about 0.55 to 0.6 times the allocations on multi-row results.
- **Built to hold a connection briefly:** the row mapping is compiled once per result shape and cached, values are read with typed getters (nothing is boxed), `Skip` advances without mapping, and a command built from a factory opens its connection when it runs and disposes it afterwards.

These figures come from the 11.0.0-preview01 build, re-run independently on a noisy machine, so read the timing ratios as indicative. The SQLite results are published but were not independently re-run, and a re-baseline is pending. Channels, Dataflow and `Retrieve().DequeueAs<T>()` are conveniences, not speed-ups. See [Performance](https://open-net-libraries.github.io/Open.Database.Extensions/articles/performance.html).

## Expressive commands

Start from a connection factory and the command opens, runs and disposes its own connection. The call that runs the command (`ToArrayAsync()` here) is called the terminal:

```cs
// The command takes a connection from the factory when a terminal runs, opens it, and disposes it afterwards.
Post[] posts = await connectionFactory
    .Command("select * from posts where author = @author order by id")
    .AddParam("author", 42)
    .Map<Post>()
    .ToArrayAsync();
```

Or start from a connection you already have. You own it; the command opens it only if it is closed and closes only what it opened:

```cs
// You own the connection. The command opens it only if it is closed, and closes only what it opened.
Post[] posts = connection
    .Command("select * from posts where author = @author order by id")
    .AddParam("author", 42)
    .Map<Post>()
    .ToArray();
```

Stored procedures are `StoredProcedure(name)`. Terminals include `ExecuteNonQuery`, `ExecuteScalar` and, after `Map<T>()`, `ToArray`, `ToList` and `FirstOrDefault`. On a `DbConnection` or an `IDbConnectionFactory<TConnection>` each has an `Async` twin. See [Expressive commands](https://open-net-libraries.github.io/Open.Database.Extensions/articles/expressive-commands.html).

## Row mapping without configuration

`Map<T>()` matches columns to public settable properties by name, ignoring case and underscores (`first_name` feeds `FirstName`), and ignores the rest:

```cs
// Columns are matched to public settable properties by name, ignoring case and underscores.
// The extra "views" column has no property, so it is ignored.
Post[] posts = connection
    .Command("select * from posts order by id")
    .Map<Post>()
    .ToArray();
```

Models need `new T()` and settable properties: there is no constructor binding. See [Mapping rows to objects](https://open-net-libraries.github.io/Open.Database.Extensions/articles/mapping.html) for the rules and limits, including the `RowMap<T>` for columns with other names.

### Renamed columns

When your columns are named differently from your properties, declare the mapping once as a `RowMap<T>` and reuse it. The map is made from key/value pairs of property name and column name; write the property name with `nameof` so the compiler catches a rename. Create it on first use with `field ??=` (C# 14), so only the maps you use are built. A declared map adds nothing per call beyond a check of the result-set layout:

```cs
static class PersonMaps
{
    public static RowMap<Person> PersonMap => field ??= [
        new(nameof(Person.Id), "person_id"),
        new(nameof(Person.FirstName), "first_name"),
        new(nameof(Person.LastName), "last_name"),
    ];
}

Person[] people = connection
    .Command("select person_id, first_name, last_name from people order by person_id")
    .Map<Person>(PersonMaps.PersonMap)
    .ToArray();
```

Declare a map once and reuse it: a map built at the call site is rebuilt, and its row reader recompiled, on every call. The constructor takes one pair or several (`new RowMap<T>(new("A", "b"))`), and an `ImmutableArray` or any `IEnumerable` of pairs also works. A `null` column in a pair means "do not map this property": it stops the by-name match too, so the property keeps the value `new T()` gave it. Older compilers have no `field` keyword; use an explicit backing field.

More in [Columns named differently](https://open-net-libraries.github.io/Open.Database.Extensions/articles/mapping.html#columns-named-differently).

## Transactions in one expression

`Using` gives your action a connection from the factory and disposes it afterwards.

```cs
// The factory creates the connection; ExecuteTransactionConditional opens it if it is closed and starts the transaction. Returning true commits; false (or a throw) rolls back.
bool committed = connectionFactory.Using(connection =>
    connection.ExecuteTransactionConditional(transaction =>
    {
        int inserted = transaction
            .Command("insert into posts (id, author, title, body, views) values (100, 1, 'Temporary', null, 0)")
            .ExecuteNonQuery();

        return inserted == 1;
    }));
```

## And more

- **Channels and Dataflow:** stream rows into a `Channel<T>` or a dataflow block. [Guide](https://open-net-libraries.github.io/Open.Database.Extensions/articles/channels-and-dataflow.html)
- **SQL Server:** `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. [Guide](https://open-net-libraries.github.io/Open.Database.Extensions/articles/getting-started.html)
- **Coming from Dapper:** the same jobs side by side. [Guide](https://open-net-libraries.github.io/Open.Database.Extensions/articles/coming-from-dapper.html)

## Install

| Package | What it is |
|---|---|
| `Open.Database.Extensions.Core` | The foundation, expressive commands and `Map<T>()`. Provider-agnostic. |
| `Open.Database.Extensions.Channel` | Rows into `System.Threading.Channels`. |
| `Open.Database.Extensions.Dataflow` | Rows into TPL Dataflow blocks. |
| `Open.Database.Extensions.MSSqlClient` | `SqlConnectionFactory` and SQL Server specializations on `Microsoft.Data.SqlClient`. |
| `Open.Database.Extensions.SqlClient` | The same API on the older `System.Data.SqlClient`. Deprecated: use `MSSqlClient`. |
| `Open.Database.Extensions` | A meta-package for Core. The last published version is 9.0.3: use Core. |

```sh
dotnet add package Open.Database.Extensions.Core --prerelease
```

Targets: `netstandard2.0`, `netstandard2.1` and `net10.0` (`MSSqlClient`: `net472` and `net10.0`). Version 11 is a preview (`11.0.0-preview06`): see [What's new in 11](https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html) for the changes, the breaking changes and the known limitations, and the [release notes](https://open-net-libraries.github.io/Open.Database.Extensions/articles/release-notes.html) for earlier versions. Please try it and tell us what you find.

## How it fits together

![Four layers: a foundation of best-practice ADO.NET extensions, expressive commands, row mapping, all resting on performance.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/architecture.svg)

## Contributing and license

Documentation lives next to the code and is rebuilt and published from `master` automatically; see [where each kind of documentation lives](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/docsite/CONTRIBUTING-DOCS.md). Released under the [MIT license](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/LICENSE).