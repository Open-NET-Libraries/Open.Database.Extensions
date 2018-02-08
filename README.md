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

When using the SQL Client, asychronous methods are available as well as `.ToTargetBlock(target)` and `.AsSourceBlock()` methods.

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

#### `Results<T>` &amp; `ResultsAsync<T>`

Using the provided type `T` entity, the data is coerced by which properties intersect with the ones available to the ```IDataReader```.

#### `Retrieve` &amp; `RetrieveAsync`

Returns a `List<Dictionary<string, object>>` containing the requested data.  Takes parameters to isolate the desired columns.
