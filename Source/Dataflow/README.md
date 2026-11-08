# Open.Database.Extensions.Dataflow

Read query results straight into a TPL Dataflow pipeline. `ToTargetBlock<T>()` posts each row to a block, and `AsSourceBlock<T>()` exposes the results as a source block. It includes [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/), and rows are mapped the same way as `Map<T>()`.

```sh
dotnet add package Open.Database.Extensions.Dataflow --prerelease
```

```cs
var block = new BufferBlock<Post>();

// complete: true completes the block after every row has been delivered (or faults it if reading fails).
long count = await connectionFactory
    .Command("select * from posts order by id")
    .ToTargetBlockAsync<Post>(block, complete: true);

// The block completes once every row has been delivered and received.
var received = new List<Post>();
while (await block.OutputAvailableAsync())
    received.Add(await block.ReceiveAsync());
```

It works on expressive commands, on `IDbCommand` and on readers. See [Channels and Dataflow](https://open-net-libraries.github.io/Open.Database.Extensions/articles/channels-and-dataflow.html).

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Legacy: prefer MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| **Open.Database.Extensions.Dataflow** (this package) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
