# Open.Database.Extensions.Channel

[![Open.Database.Extensions.Channel: You write the SQL. ODX writes the other twenty lines.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/hero-channel.svg)](https://open-net-libraries.github.io/Open.Database.Extensions/)

Rows from a mapped query into a `System.Threading.Channels` channel, as fast as it can. **Import [Open.ChannelExtensions](https://www.nuget.org/packages/Open.ChannelExtensions/) alongside this package** for everything after the dump (`Transform`, `Batch`, `Join`, `ReadAll`). This package depends only on [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) and `System.Threading.Channels`.

```sh
dotnet add package Open.Database.Extensions.Channel --prerelease
dotnet add package Open.ChannelExtensions
```

```cs
using System.Threading.Channels;
using Open.Database.Extensions;

public record Post(int Id, string Title);

// connectionFactory is a DbConnectionFactory<TConnection>, created once per application (see the Core package page, which also names the provider to install).
// Returns right away: the rows are read in the background and the channel completes when they are all written.
ChannelReader<Post> reader = connectionFactory
    .Command("select * from posts order by id")
    .Map<Post>()
    .ToChannelReader();

await foreach (Post post in reader.ReadAllAsync())
{
    // Use the post.
}
```

`ToChannelReader()` and `ToChannel()` make an unbounded channel. The query is read at full speed and the connection is closed when the read ends, however slowly you read the channel. `ToChannel(channel)` writes into a channel you own and leaves it open; `ToChannelHoldingConnection` pulls the rows one at a time to cap memory. See [Channels and Dataflow](https://open-net-libraries.github.io/Open.Database.Extensions/articles/channels-and-dataflow.html).

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0, net11.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0, net11.0 |
| **Open.Database.Extensions.Channel** (this package) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0, net11.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0, net11.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
