# Open.Database.Extensions.Channel

Read query results straight into a `System.Threading.Channels` channel instead of a collection. `ToChannel<T>()` returns a `ChannelReader<T>` immediately; rows are produced in the background, and the channel completes when every row has been written (or completes with the exception if reading fails). It includes [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/), and rows are mapped the same way as `Map<T>()`.

```sh
dotnet add package Open.Database.Extensions.Channel --prerelease
```

```cs
// Returns right away; rows are produced in the background and the channel completes when they are all written.
ChannelReader<Post> reader = connectionFactory
    .Command("select * from posts order by id")
    .ToChannel<Post>(singleReader: true);

await foreach (Post post in reader.ReadAllAsync())
    received.Add(post);
```

It works on expressive commands, on `IDbCommand` and on readers; `ToChannelAsync<T>()` is the asynchronous twin. See [Channels and Dataflow](https://open-net-libraries.github.io/Open.Database.Extensions/articles/channels-and-dataflow.html).

## The packages

| Package | What it adds | Targets |
|---|---|---|
| [Open.Database.Extensions.Core](https://www.nuget.org/packages/Open.Database.Extensions.Core/) | The foundation, expressive commands and `Map<T>()`. Provider-agnostic: start here. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.MSSqlClient](https://www.nuget.org/packages/Open.Database.Extensions.MSSqlClient/) | `SqlConnectionFactory` and stored procedures on `Microsoft.Data.SqlClient`. | net472, net10.0 |
| [Open.Database.Extensions.SqlClient](https://www.nuget.org/packages/Open.Database.Extensions.SqlClient/) | The same API on the older `System.Data.SqlClient`. Legacy: prefer MSSqlClient. | netstandard2.0, netstandard2.1, net10.0 |
| **Open.Database.Extensions.Channel** (this package) | Rows into `System.Threading.Channels`. | netstandard2.0, netstandard2.1, net10.0 |
| [Open.Database.Extensions.Dataflow](https://www.nuget.org/packages/Open.Database.Extensions.Dataflow/) | Rows into TPL Dataflow blocks. | netstandard2.0, netstandard2.1, net10.0 |

[Documentation](https://open-net-libraries.github.io/Open.Database.Extensions/) · [API reference](https://open-net-libraries.github.io/Open.Database.Extensions/api/index.html) · [Source and issues](https://github.com/Open-NET-Libraries/Open.Database.Extensions) · MIT license
