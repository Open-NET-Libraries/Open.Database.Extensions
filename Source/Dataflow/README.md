# Open.Database.Extensions.Dataflow

[![Open.Database.Extensions.Dataflow: You write the SQL. ODX writes the other twenty lines.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/hero-dataflow.svg)](https://open-net-libraries.github.io/Open.Database.Extensions/)

Rows from a mapped query into a TPL Dataflow pipeline you build: the query is read at full speed into your blocks. `ToTargetBlock` sends the rows to a block you own, so its scheduler and priorities stay yours; `AsSourceBlock` makes a block for you. The query is read at full speed and the connection is closed when the read ends. This package depends only on [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) and `System.Threading.Tasks.Dataflow`.

```sh
dotnet add package Open.Database.Extensions.Dataflow --prerelease
```

```cs
using System.Threading.Tasks.Dataflow;
using Open.Database.Extensions;

public record Post(int Id, string Title);

// connectionFactory is a DbConnectionFactory<TConnection>, created once per application (see the Core package page, which also names the provider to install).
var block = new BufferBlock<Post>();

// BlockEnd.Complete completes the block after the last row, or faults it if reading fails.
long count = await connectionFactory
    .Command("select * from posts order by id")
    .Map<Post>()
    .ToTargetBlock(block, BlockEnd.Complete);

// The block completes once every row has been received.
var received = new List<Post>();
while (await block.OutputAvailableAsync())
    received.Add(await block.ReceiveAsync());
```

Each row is sent with `SendAsync`, so a bounded block that is full makes the pass wait. See [Channels and Dataflow](https://open-net-libraries.github.io/Open.Database.Extensions/articles/channels-and-dataflow.html).

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0, net11.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0, net11.0 |
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Deprecated: use MSSqlClient. | netstandard2.0, netstandard2.1, net10.0, net11.0 |
| [Open.Database.Extensions.Channel](https://www.nuget.org/packages/Open.Database.Extensions.Channel/) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0, net11.0 |
| **Open.Database.Extensions.Dataflow** (this package) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0, net11.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
