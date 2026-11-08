# Open.Database.Extensions.MSSqlClient

![Open.Database.Extensions.MSSqlClient: You write the SQL. The library writes the other twenty lines.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/hero-mssqlclient.svg)

SQL Server for Open.Database.Extensions: `SqlConnectionFactory`, SQL-specific commands, and stored procedures on `Microsoft.Data.SqlClient`. **Use it when** your database is SQL Server. It includes [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) and the `Microsoft.Data.SqlClient` provider, so there is nothing else to install.

```sh
dotnet add package Open.Database.Extensions.MSSqlClient --prerelease
```

```cs
using Open.Database.Extensions;

public record Post(int Id, string Title);

// Create the factory once per application.
var connectionFactory = new SqlConnectionFactory(@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");
int author = 42;

Post[] posts = await connectionFactory
    .Command("select * from posts where author = @author")
    .AddParam(author)
    .Map<Post>()
    .ToArrayAsync();
```

## Stored procedures: name the argument, not the parameter

`AddParam(value)` takes the parameter name from the argument expression and matches it to the procedure's real parameters (exactly, then ignoring case, then ignoring case and underscores), or on a text command to the `@name` markers in the SQL, so `search.TitleContains` finds `@title_contains`. `AddParams(search)` sends every member of the object that the procedure declares.

```cs
// search holds the arguments: any object with the members you want to send, here an anonymous one.
var search = new { Author = 42, TitleContains = "ode" };

Post[] posts = await connectionFactory
    .StoredProcedure("[dbo].[SearchPosts]")
    .AddParam(search.Author)
    .AddParam(search.TitleContains)
    .Map<Post>()
    .ToArrayAsync();

// Or give it the whole object: the members the procedure declares are sent, the rest are ignored.
posts = await connectionFactory
    .StoredProcedure("[dbo].[SearchPosts]")
    .AddParams(search)
    .Map<Post>()
    .ToArrayAsync();
```

Parameter names are looked up once per connection factory and remembered for five minutes (a connection or factory you create for each call looks them up for every command). The return value of a procedure is readable with `ExecuteReturn<int>()`; output parameters can be sent, but their values cannot yet be read back. See [Expressive commands](https://open-net-libraries.github.io/Open.Database.Extensions/articles/expressive-commands.html) for the rules and limits.

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| **Open.Database.Extensions.MSSqlClient** (this package) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Deprecated: use MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
