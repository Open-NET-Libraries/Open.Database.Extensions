# Open.Database.Extensions.SqlClient

[![Open.Database.Extensions.SqlClient: You write the SQL. ODX writes the other twenty lines.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/hero-sqlclient.svg)](https://open-net-libraries.github.io/Open.Database.Extensions/)

**This package is deprecated in favor of [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/)**, which uses the actively maintained `Microsoft.Data.SqlClient` and has the same API. Use this package only if your project still needs the older `System.Data.SqlClient`; otherwise install MSSqlClient. It includes [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/).

```sh
dotnet add package Open.Database.Extensions.SqlClient --prerelease
```

```cs
using Open.Database.Extensions;

public record Post(int Id, string Title);

// Create the factory once per application.
// connectionString is yours. System.Data.SqlClient comes with this package.
var connectionFactory = new SqlConnectionFactory(connectionString);
int author = 42;

Post[] posts = await connectionFactory
    .Command("select * from posts where author = @author")
    .AddParam(author) // name and type inferred from the variable
    .Map<Post>()
    .ToArrayAsync();
```

It is used the same way as MSSqlClient: `Command(...)`, `StoredProcedure(...)`, `AddParam(...)` and `Map<T>()`. The [MSSqlClient README](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) has the stored-procedure examples, including parameter names taken from the argument.

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| **Open.Database.Extensions.SqlClient** (this package) | The same API on the older `System.Data.SqlClient`. Deprecated: use MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
