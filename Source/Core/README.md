# Open.Database.Extensions.Core

![Open.Database.Extensions.Core: You write the SQL. The library writes the other twenty lines.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/hero-core.svg)

Core is the foundation of Open.Database.Extensions: ADO.NET extension methods with best-practice defaults, expressive commands that open, run and dispose their own connection, and `Map<T>()` to turn rows into objects. It has no database dependency of its own. **Use it for any ADO.NET database;** most applications need only this package.

```sh
dotnet add package Open.Database.Extensions.Core --prerelease
dotnet add package Microsoft.Data.Sqlite   # your provider; SQLite is used below
```

```cs
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

public record Post(int Id, string Title);

// connectionString is yours, for example "Data Source=blog.db"; the query assumes a posts table (id, author, title, ...).
// Create the factory once per application. Use your provider's connection type.
var connectionFactory = new DbConnectionFactory<SqliteConnection>(() => new SqliteConnection(connectionString));
int author = 42;

// The command opens a connection when the terminal runs and disposes it afterwards.
Post[] posts = await connectionFactory
    .Command("select * from posts where author = @author order by id")
    .AddParam(author) // name and type inferred from the variable
    .Map<Post>()
    .ToArrayAsync();
```

`AddParam(author)` names the parameter `author` after the variable; `AddParam("author", 42)` names it yourself. Already have a connection? Start from it instead: `connection.Command("...")`. You own the connection; the command opens it only if it is closed and closes only what it opened.

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
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Deprecated: use MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

Every other package depends on Core, so installing one brings Core with it.

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
