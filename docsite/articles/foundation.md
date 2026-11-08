# The foundation

Most of the library is a provider-agnostic set of extension methods on the standard ADO.NET types (`IDbConnection`, `DbConnection`, `IDbCommand`, `IDataReader`, `IDbTransaction`). They apply common best practices so that everyday database work is short to write and safe by default. Some helpers are deliberately thin and unopinionated; some are deliberately opinionated, for example a command built from a factory always disposes the connection it created.

![The layers of the library.](../images/architecture.svg)

The expressive commands and the row mapping are built on this layer.

## Connection factories

A connection factory creates connections without the caller knowing a connection string:

- `IDbConnectionFactory` and `IDbConnectionFactory<TConnection>` are the abstractions, which fit dependency injection. The `Async` terminals need a `DbConnection`, so a service that awaits should depend on `IDbConnectionFactory<DbConnection>` (or a factory of a specific `DbConnection` type); commands from the non-generic `IDbConnectionFactory` have no `Async` twins.
- `DbConnectionFactory<TConnection>` wraps any `Func<TConnection>`.
- `SqlConnectionFactory` (in the `MSSqlClient` package) wraps a connection string for SQL Server.

[!code-csharp[](../samples/Db.cs#factory_definition)]

Helpers on a factory: `Using` creates a connection, runs your action, and disposes the connection (it does not open it). `Open` does the same after making sure the connection is open. `Command` and `StoredProcedure` start an [expressive command](expressive-commands.md).

## Who owns the connection

| You start from | The connection is... | When a terminal runs |
|---|---|---|
| A connection factory (`factory.Command(sql)`) | Created by the command | Created, opened if needed, then disposed |
| A connection (`connection.Command(sql)`) | Yours | Opened only if it was closed, then closed again only if the command opened it. Never disposed |
| `factory.Using(connection => ...)` | Created by `Using` | Disposed when your action returns. Not opened for you |

[!code-csharp[](../samples/ExpressiveCommands.cs#ownership)]

A command built from a single connection refuses concurrent terminals: a second call while one is running throws `InvalidOperationException`.

## Open late, close early

Several helpers exist to keep the time a connection is open short:

- `EnsureOpen()` and `EnsureOpenAsync()` open a closed (or broken) connection and return its prior state, so you can restore it.
- Buffering helpers such as `Results<T>()` queue the rows first and apply the transformation as you enumerate them.
- `Map<T>()` streams rows straight out of the reader while the connection is open; the connection is released as soon as the terminal returns.

## Transactions

`ExecuteTransactionConditional` begins a transaction, runs your action, and commits only if the action returns `true`, nothing throws and the cancellation token was not cancelled. Otherwise it rolls back.

[!code-csharp[](../samples/Transactions.cs#transaction)]

## Reader and command helpers

The foundation also includes helpers you can use without the expressive syntax: `ForEach` and `ForEachAsync` over a reader, `Select` and `SelectAsync`, `AsEnumerable` and `AsAsyncEnumerable` (rows as `object[]`), `ToList`, `ToArray`, `ToDataTable` and friends on commands. Buffered reads copy the rows out of the reader first, so the connection can be released early: `Retrieve()` copies every row into a queue of `object[]`, and `DequeueAs<T>()` maps that queue to `T` as you enumerate it. `Results<T>()` buffers and maps in one call, and `FirstOrdinalResults<T>()` reads the first column of every row as `T`. For typed rows `Map<T>()` is the better route (see [Performance](performance.md#where-it-is-not-faster)).

See the [API reference](xref:Open.Database.Extensions) for the full list.
