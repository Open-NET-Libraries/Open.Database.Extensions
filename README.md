# Open.Database.Extensions
 
Useful set of utilities and abstractions for simplifying modern database operations and ensuring dependency injection compatibility.

## Connection Factories

Connection factories facilitate creation and disposal of connections without the concern of a connection reference or need for awareness of a connection string.  A ```SqlConnectionFactory``` is provided and can be overriden to provide more specific dependency injection configurations.

## Stored Procedures

The provided ```StoredProcedure``` classes allow for an expressive means to append parameters and execute the results without lenghty complicated setup.
