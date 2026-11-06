# Open.Database.Extensions

Useful set of utilities and abstractions for simplifying modern database operations and ensuring dependency injection compatibility.

[Full API Documentation Click Here](https://electricessence.github.io/Open.Database.Extensions/api)

## Connection Factories

Connection factories facilitate creation and disposal of connections without the concern of a connection reference or need for awareness of a connection string.  A `SqlConnectionFactory` is provided and can be overridden to provide more specific dependency injection configurations.

## Expressive Commands

The provided expressive command classes allow for an expressive means to append parameters and execute the results without lengthy complicated setup.

Extensions are provided to create commands from connection factories.

### Example

```cs
var result = connectionFactory
   .StoredProcedure("[procedure name]")
   .AddParam("a",1)
   .AddParam("b",true)
   .AddParam("c","hello")
   .ExecuteScalar();
```

## Asynchronous

End-to-end asynchronous methods suffixed with `Async`.

When using the SQL Client, asynchronous methods are available as well as `.ToTargetBlockAsync<T>(target)` and `.AsSourceBlockAsync<T>()` Dataflow methods.

## Extensions

Instead of writing this:

```cs
var myResult = new List<T>();
using(var reader = await mySqlCommand.ExecuteReaderAsync(CommandBehavior.CloseConnection))
{
    while(await reader.ReadAsync())
        list.Add(transform(reader));
}
```

Is now simplified to this:

```cs
var myResult = await cmd.ToListAsync(transform);
```

## Deferred Transformation

In order to keep connection open time to a minimum, some methods cache data before closing the connection and then subsequently applying the transformations as needed.

### `Results<T>()` and `ResultsAsync<T>()`

Queues all the data.  Then using the provided type `T` entity, the data is coerced by which properties intersect with the ones available to the `IDataReader`.

Optionally a `RowMap<T>` can be passed as the `map` parameter to name the column for a property. A `null` column means "do not map this property": it stops the by-name match, so the property keeps its default value even if the result set has a column with that name.

### Examples

If all the columns in the database map exactly to a field: (A column that has no associated field/property is ignored.)

```cs
var people = cmd.Results<Person>();
```

If the database fields don't map exactly, declare a `RowMap<T>` once and pass it each time. The property name is written with `nameof`, so a rename is caught by the compiler:

```cs
static class PersonMaps
{
    public static RowMap<Person> PersonMap => field ??= [
        new(nameof(Person.FirstName), "first_name"),
        new(nameof(Person.LastName), "last_name"),
    ];
}

var people = cmd.Results<Person>(PersonMaps.PersonMap);
```

Tuples are accepted as a convenience for a one-off. They build a new map, and compile the row reader, on every call:

```cs
var people = cmd.Results<Person>(
 (nameof(Person.FirstName), "first_name"),
 (nameof(Person.LastName), "last_name"));
```

#### `Retrieve()` and `RetrieveAsync()`

Queues all the data.  Returns a `QueryResult<Queue<object[]>>` containing the requested data and column information.  The `.AsDequeueingMappedEnumerable()` extension will iteratively convert the results to dictionaries for ease of access.

#### `AsSourceBlockAsync<T>()`

*Include the `Open.Database.Extensions.Dataflow` package for Dataflow extensions.*

(Fully asynchronous.) Returns a Dataflow source block.  Then asynchronously buffers and transforms the results allowing for any possible Dataflow configuration.  The source block is marked as complete when there are no more results.  If the block is somehow marked as complete externally, the flow of data will stop and the connection will close.

### `AsSourceBlockAsync<T>()` versus `ResultsAsync<T>`

Depending on the level of asynchrony in your application, you may want to avoid too much buffering of data.

`AsSourceBlockAsync<T>()` is fully asynchronous from end-to-end and can keep total buffering to a minimum by consuming (receiving) results as fast as possible, but may incur additional latency between reads.

`ResultsAsync<T>()` is fully asynchronous from end-to-end but returns an `IEnumerable<T>` that although has fully buffered the all the data into memory, has deferred the transformation until enumerated.  This way, the asynchronous data pipeline is fully complete before synchronously transforming the data.

Both methods ultimately are using a `Queue<object[]>` or `ConcurrentQueue<object[]>` (Dataflow) to buffer the data, but `ResultsAsync<T>()` buffers the entire data set before dequeuing and transforming the results.

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
