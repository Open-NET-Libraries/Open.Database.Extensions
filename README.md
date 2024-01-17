# Open.Database.Extensions

[![NuGet](https://img.shields.io/nuget/v/Open.Database.Extensions.Core.svg?style=flat)](https://www.nuget.org/packages/Open.Database.Extensions.Core/)

Useful set of utilities and abstractions for simplifying modern database operations and ensuring dependency injection compatibility.

## Connection Factories

Connection factories facilitate creation and disposal of connections without the concern of a connection reference or need for awareness of a connection string.
A `SqlConnectionFactory` is provided and can be overridden to provide more specific dependency injection configurations.

## Expressive Commands

The provided expressive command classes allow for an expressive means to append parameters and execute the results without lengthy complicated setup.

Extensions are provided to create commands from connection factories.

## 8.0 Release Notes

  - All `.ConfigureAwait(true)` are now `.ConfigureAwait(false)` as they should be.  The caller will need to `.ConfigureAwait(true)` if they need to resume on the calling context.
  - Added `Open.Database.Extensions.MSSqlClient` for `Microsoft.Data.SqlClient` support.
  - .NET 8.0 added to targets to ensure potential compliation and performance improvements are available.
  - Improved nullable integrity.

### Example

```cs
var result = connectionFactory
   .StoredProcedure("[procedure name]")
   .AddParam("a",1)
   .AddParam("b",true)
   .AddParam("c","hello")
   .ExecuteScalar();
```

## Extensions

Instead of writing this:

```cs
var myResult = new List<T>();
using(var reader = await mySqlCommand.ExecuteReaderAsync(CommandBehavior.CloseConnection))
{
    while(await reader.ReadAsync())
        myResult.Add(transform(reader));
}
```

Is now simplified to this:

```cs
var myResult = await mySqlCommand.ToListAsync(transform);
```

## Deferred Transformation

In order to keep connection open time to a minimum, some methods cache data before closing the connection and then subsequently applying the transformations as needed.

### `Results<T>()` and `ResultsAsync<T>()`

Queues all the data.  Then using the provided type `T` entity, the data is coerced by which properties intersect with the ones available to the `IDataReader`.

Optionally a field to column override map can be passed as a parameter.  If a column is set as `null` then that field is ignored (not applied to the model).

### Examples

If all the columns in the database map exactly to a field: (A column that has no associated field/property is ignored.)

```cs
var people = cmd.Results<Person>();
```

If the database fields don't map exactly:

```cs
var people = cmd.Results<Person>(
 (Field:"FirstName", Column:"first_name"),
 (Field:"LastName", Column:"last_name")));
```

or

```cs
var people = cmd.Results<Person>(
 ("FirstName", "first_name"),
 ("LastName", "last_name"));
```

or

```cs
var people = cmd.Results<Person>(new Dictionary<string,string>{
 {"FirstName", "first_name"},
 {"LastName", "last_name"});
```

### `Retrieve()` and `RetrieveAsync()`

Queues all the data.  Returns a `QueryResult<Queue<object[]>>` containing the requested data and column information.  The `.AsDequeueingMappedEnumerable()` extension will iteratively convert the results to dictionaries for ease of access.

### `ResultsAsync<T>`

`ResultsAsync<T>()` is fully asynchronous from end-to-end but returns an `IEnumerable<T>` that although has fully buffered the all the data into memory, has deferred the transformation until enumerated.  This way, the asynchronous data pipeline is fully complete before synchronously transforming the data.

## Transactions

Example:

```cs
// Returns true if the transaction is successful.
public static bool TryTransaction()
=> ConnectionFactory.Using(connection =>
    // Open a connection and start a transaction.
    connection.ExecuteTransactionConditional(transaction => {

        // First procedure does some updates.
        var count = transaction
            .StoredProcedure("[Updated Procedure]")
            .ExecuteNonQuery();

        // Second procedure validates the results.
        // If it returns true, then the transaction is committed.
        // If it returns false, then the transaction is rolled back.
        return transaction
            .StoredProcedure("[Validation Procedure]")
            .AddParam("@ExpectedCount", count)
            .ExecuteScalar<bool>();
    }));
```
