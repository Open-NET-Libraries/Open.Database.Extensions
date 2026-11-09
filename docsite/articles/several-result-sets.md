# Several result sets

A *result set* is one table of rows that a query sends back. Most queries send one. A batch such as `select ...; select ...;` or a stored procedure can send several, one after another.

This library does not manage several result sets for you. A call on a command reads the first result set and stops. Only your SQL knows how many sets there are and what shape each one has, so the library does not guess. Several sets are uncommon. When you have them, you read them the way ADO.NET always has: you take the *reader* (the object that walks through the rows), read a set, call `NextResult` to move to the next set, and read again. The library maps the rows of each set for you.

The library never calls `NextResult` itself.

## The pattern

`ExecuteReader(reader => ...)` opens the connection, runs the command, hands you the reader and closes the connection when your function returns. Inside it, `reader.Results<T>()` maps the rows of the set the reader is on. `NextResult()` moves to the next set and returns `false` when there is none.

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_sync)]

Each set has its own columns, so each set gets its own type: `Post` for the first set and `Person` for the second.

## The same, async

In async code the reader is a `DbDataReader`, which has `NextResultAsync`. `ResultsBufferedAsync<T>()` reads and maps every row of the current set during the call.

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_async)]

## When you set up the connection yourself

You do not need the fluent command. Open the connection, create the command, call `ExecuteReader`, and use the same `Results<T>()` on that reader. You own the connection, the command and the reader, so you close them (here, with `using`).

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_own_connection)]

## An unknown number of sets

If you do not know how many sets come back, loop. Use `do ... while`, because the reader starts on the first set.

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_loop)]

## An unknown shape

The reader describes the set it is on: `FieldCount` is the number of columns and `GetName(i)` is the name of column `i`.

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_shape)]

`AsEnumerable()` (used in the loop above) gives each row as an `object[]` of values, with no type at all. When you just want every set and do not need to map them, `ToDataTables()` returns one `DataTable` per set:

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_tables)]

## The rules

- Do not pass `CommandBehavior.SingleResult`. It tells the provider there is only one set, so there is no second set to reach.
- Call `ToList()` (or `ToArray()`) before `NextResult`. `reader.Results<T>()` is lazy: it maps rows only as you enumerate it.
- Check what `NextResult` returns. `false` means there are no more sets.
- Each set has its own columns. Use a type that fits that set.

The second rule is the easy one to miss, so here it is broken. The people are read as if they were posts: no error, and every property keeps its default value.

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_lazy_mistake)]

## An error that comes after the rows

On SQL Server, a `RAISERROR` that runs after a `SELECT` is sent after the rows. It reaches you only when `NextResult` moves past that set. A call on a command (`Map<T>()`, `Results`, `Retrieve`, Channel, Dataflow) does not raise it, and neither does closing the reader.

If a batch can fail after its rows, read it with your own handler and make one more call past the last set you expect:

[!code-csharp[](../samples/SeveralResultSets.cs#result_sets_error_check)]

On SQLite, which the samples run on, there is no `RAISERROR`, so this code runs without an error there. The behavior below is SQL Server's.

An error after a later set needs a call past that set too: with two sets and an error after them, one `NextResult` does not raise it, and moving past every set does. These cases were measured on SQL Server with Microsoft.Data.SqlClient:

| Batch | What you do | Result |
|---|---|---|
| `SELECT 1; RAISERROR(...)` | read the rows, then dispose or close the reader | no error |
| `SELECT 1; RAISERROR(...)` | read the rows, then call `NextResult` | `SqlException` |
| same, with `CommandBehavior.SingleResult` | read the rows, then call `NextResult` | no error: there is no second set |
| two sets, then an error | read the first set, call `NextResult` once, read the second set | no error: that call moved to the second set |
| two sets, then an error | call `NextResult` until it has moved past every set | `SqlException` |
| a set, an error, a set | read the first, call `NextResult` once | `SqlException` |

If you come from Dapper: its `Query` calls `NextResult` over every remaining set and does not send `SingleResult` by default, which is why it raises such an error. Its `QueryMultiple` is its explicit form for several sets.

## Why the library does not do this for you

To move on by itself, the library would have to assume what the next result is: how many sets there are, and the type of each. It cannot know. A wrong guess would silently drop rows or map them to the wrong type. So the call reads one set, and what comes after it is yours to decide.
