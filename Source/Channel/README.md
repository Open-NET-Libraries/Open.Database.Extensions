# Open.Database.Extensions.Channel

![Open.Database.Extensions.Channel: You write the SQL. The library writes the other twenty lines.](https://raw.githubusercontent.com/Open-NET-Libraries/Open.Database.Extensions/master/docsite/images/hero-channel.svg)

Read query results straight into a `System.Threading.Channels` channel instead of a collection. `ToChannel<T>()` returns a `ChannelReader<T>` immediately; rows are produced in the background, and the channel completes when every row has been written (or completes with the exception if reading fails). **Use it when** another task consumes the rows while they are still being read. It includes [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/), and rows are mapped the same way as `Map<T>()`.

```sh
dotnet add package Open.Database.Extensions.Channel --prerelease
```

```cs
using Open.Database.Extensions;

public record Post(int Id, string Title);

// connectionFactory is a DbConnectionFactory<TConnection>, created once per application (see the Core package page, which also names the provider to install).
// Returns right away; rows are produced in the background and the channel completes when they are all written.
ChannelReader<Post> reader = connectionFactory
    .Command("select * from posts order by id")
    .ToChannel<Post>(singleReader: true);

var received = new List<Post>();
await foreach (Post post in reader.ReadAllAsync())
    received.Add(post);
```

It works on expressive commands, on `IDbCommand` and on readers; `ToChannelAsync<T>()` is the asynchronous twin. Reading into a channel is a convenience for pipelines, not a speed-up. See [Channels and Dataflow](https://open-net-libraries.github.io/Open.Database.Extensions/articles/channels-and-dataflow.html).

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Deprecated: use MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| **Open.Database.Extensions.Channel** (this package) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
