# Open.Database.Extensions.Core

**You write the SQL. This library writes the other twenty lines.**

Core is the foundation of Open.Database.Extensions: best-practice ADO.NET extension methods, expressive commands that open, run and dispose their own connection, and `Map<T>()` to turn rows into objects. It works with any ADO.NET provider and has no database dependency of its own.

```sh
dotnet add package Open.Database.Extensions.Core --prerelease
```

```cs
// The command takes a connection from the factory when a terminal runs, opens it, and disposes it afterwards.
Post[] posts = await connectionFactory
    .Command("select * from posts where author = @author order by id")
    .AddParam("author", 42)
    .Map<Post>()
    .ToArrayAsync();
```

`connectionFactory` is created once per application, for example `new DbConnectionFactory<SqliteConnection>(() => new SqliteConnection(connectionString))`. Already have a connection? Start from it instead: `connection.Command("...")`. You own the connection; the command opens it only if it is closed and closes only what it opened.

## Where to go next

- [Getting started](https://open-net-libraries.github.io/Open.Database.Extensions/articles/getting-started.html)
- [Expressive commands](https://open-net-libraries.github.io/Open.Database.Extensions/articles/expressive-commands.html): parameters, stored procedures, timeouts, terminals
- [Mapping rows to objects](https://open-net-libraries.github.io/Open.Database.Extensions/articles/mapping.html): the matching rules, `RowMap<T>`, NULL handling
- [Coming from Dapper](https://open-net-libraries.github.io/Open.Database.Extensions/articles/coming-from-dapper.html): the same jobs side by side
- [Performance](https://open-net-libraries.github.io/Open.Database.Extensions/articles/performance.html)

## The packages

| Package | What it adds | Targets |
|---|---|---|
| **Open.Database.Extensions.Core** (this package) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Legacy: prefer MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

Every other package depends on Core, so installing one brings Core with it.

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
