# Open.Database.Extensions.MSSqlClient

SQL Server for Open.Database.Extensions: `SqlConnectionFactory`, SQL-specific commands, and stored procedures on `Microsoft.Data.SqlClient`. It includes [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/), so the whole Core API is available too.

```sh
dotnet add package Open.Database.Extensions.MSSqlClient --prerelease
```

```cs
var connectionFactory = new SqlConnectionFactory(@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");

Post[] posts = await connectionFactory
    .Command("select * from posts where author = @author")
    .AddParam("author", 42)
    .Map<Post>()
    .ToArrayAsync();
```

## Stored procedures: name the argument, not the parameter

`AddParam(value)` takes the parameter name from the argument expression and matches it to the procedure's real parameters (exactly, then ignoring case, then ignoring case and underscores), so `search.TitleContains` finds `@title_contains`. `AddParams(search)` sends every member of the object that the procedure declares.

```cs
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

Parameter names are looked up once per connection factory and remembered for five minutes. Output parameters can be sent, but their values cannot yet be read back. See [Expressive commands](https://open-net-libraries.github.io/Open.Database.Extensions/articles/expressive-commands.html) for the rules and limits.

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| **Open.Database.Extensions.MSSqlClient** (this package) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Legacy: prefer MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
