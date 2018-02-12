# Open.Database.Extensions
 
Useful set of utilities and abstractions for simplifying modern database operations and ensuring dependency injection compatibility.

## Connection Factories

Connection factories facilitate creation and disposal of connections without the concern of a connection reference or need for awareness of a connection string.  A ```SqlConnectionFactory``` is provided and can be overriden to provide more specific dependency injection configurations.

## Expressive Commands

The provided expressive command classes allow for an expressive means to append parameters and execute the results without lenghty complicated setup.

Extensions are provied to create commands from connection factories.

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

When using the SQL Client, asychronous methods are available as well as `.ToTargetBlockAsync<T>(target)` and `.AsSourceBlockAsync<T>()` Dataflow methods.

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

Is now simplifed to this:
```cs
var myResult = await cmd.ToListAsync(transform);
```

## Deferred Transformation

In order to keep connection open time to a minimum, some methods cache data before closing the connection and then subsequently applying the transformations as needed.

#### `Results<T>()`

Synchronously queries (pulls all the data).  Then using the provided type `T` entity, the data is coerced by which properties intersect with the ones available to the ```IDataReader```.

#### `Retrieve()`

Synchronously queries (pulls all the data).  Returns a `QueryResult<Queue<object[]>>` containing the requested data and column mappings.  The `.AsDequeueingMappedEnumerable()` extension will iteratively convert the results to dictionaries for ease of access.

#### `AsSourceBlockAsync<T>()`

(Fully asynchronous.) Retuns a Dataflow source block.  Then asynchronously buffers and transforms the results allowing for any possible Dataflow configuration.  The source block is marked as complete when there are no more results.  If the block is somehow marked as complete externally, the flow of data will stop and the connection will close.
