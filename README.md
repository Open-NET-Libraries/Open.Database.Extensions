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

## Asynchronous

When using the SQL Client, asychronous methods are available.

Subsequently, Open.Linq.AsyncExtensions is available to help with method chaining:

https://www.nuget.org/packages/Open.Linq.AsyncExtensions
