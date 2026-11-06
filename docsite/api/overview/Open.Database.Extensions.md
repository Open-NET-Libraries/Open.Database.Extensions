---
uid: Open.Database.Extensions
summary: *content
---
Extensions on the standard ADO.NET types, expressive commands and row mapping. This is the namespace most applications use.

- **The foundation:** extension methods on connections, commands, readers and transactions, for example <xref:Open.Database.Extensions.ConnectionExtensions>, <xref:Open.Database.Extensions.CommandExtensions> and <xref:Open.Database.Extensions.TransactionExtensions>.
- **Connection factories:** <xref:Open.Database.Extensions.IDbConnectionFactory> and <xref:Open.Database.Extensions.DbConnectionFactory>.
- **Expressive commands:** <xref:Open.Database.Extensions.ExpressiveCommand> and <xref:Open.Database.Extensions.ExpressiveDbCommand>, started with `Command(sql)` or `StoredProcedure(name)`.
- **Row mapping:** `Map<T>()` in <xref:Open.Database.Extensions.CoreExtensions> returns a deferred <xref:Open.Database.Extensions.MappedQuery`1>.
- **Companion packages:** the Channel, Dataflow and MSSqlClient packages add their types to this namespace.

The guide has the narrative: [The foundation](../../articles/foundation.md), [Expressive commands](../../articles/expressive-commands.md) and [Mapping rows to objects](../../articles/mapping.md).
