# API reference

The reference is generated from the code and its XML documentation on every build, using the `net10.0` target. The `netstandard2.0` and `netstandard2.1` builds expose the same public API except where the shims differ (see the PublicAPI files in the repository).

| Package | What it adds |
|---|---|
| `Open.Database.Extensions.Core` | The foundation, expressive commands and `Map<T>()`. Provider-agnostic. |
| `Open.Database.Extensions.Channel` | Reading into `System.Threading.Channels` channels. |
| `Open.Database.Extensions.Dataflow` | Reading into TPL Dataflow blocks. |
| `Open.Database.Extensions.MSSqlClient` | `SqlConnectionFactory` and SQL Server specializations on `Microsoft.Data.SqlClient`. |

`Open.Database.Extensions.SqlClient` (built on `System.Data.SqlClient`) declares the same type names as `MSSqlClient` in the same namespace, so only `MSSqlClient`, the package on `Microsoft.Data.SqlClient`, is documented here.

Start from the [namespaces](xref:Open.Database.Extensions) in the table of contents.
