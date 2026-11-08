# Open.Database.Extensions.SqlClient

![Open.Database.Extensions.SqlClient: You write the SQL. The library writes the other twenty lines.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/hero-sqlclient.svg)

The Open.Database.Extensions SQL Server API on the older `System.Data.SqlClient` provider. **This package is deprecated in favor of [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/)**, which uses the actively maintained `Microsoft.Data.SqlClient` and has the same API. It stays available for projects that still need `System.Data.SqlClient`. It includes [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/).

```sh
dotnet add package Open.Database.Extensions.SqlClient --prerelease
```

Use it the same way as MSSqlClient: `new SqlConnectionFactory(connectionString)`, then `Command(...)`, `StoredProcedure(...)`, `AddParam(...)` and `Map<T>()`. The [MSSqlClient README](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) has the examples, including stored-procedure parameter names taken from the argument.

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| **Open.Database.Extensions.SqlClient** (this package) | The same API on the older `System.Data.SqlClient`. Deprecated: use MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
