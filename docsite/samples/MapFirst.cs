using Microsoft.Data.Sqlite;
using Open.Database.Extensions;
using static Samples.Sql;

namespace Samples;

// A command factory before and after: the same queries, the same rows. The "before" builds the command and its map
// on every call; the "after" declares each query once, mapped, with no connection, and only binds the source and the
// values where it runs.

#region mapfirst_before
public class PostQueriesBefore(IDbConnectionFactory<SqliteConnection> connectionFactory)
{
	public ValueTask<List<Post>> ByAuthorAsync(
		int author,
		CancellationToken cancellationToken)
		=> connectionFactory
			.Command(ListSql)
			.AddParam(author)
			.Map<Post>()
			.ToListAsync(cancellationToken);

	public ValueTask<Post?> FirstByAuthorAsync(
		int author,
		CancellationToken cancellationToken)
		=> connectionFactory
			.Command(ListSql)
			.AddParam(author)
			.Map<Post>()
			.FirstOrDefaultAsync(cancellationToken);
}
#endregion

#region mapfirst_after
public class PostQueriesAfter(IDbConnectionFactory<SqliteConnection> connectionFactory)
{
	// Declared once: immutable, no connection, safe in a static field.
	// The map, and the reader compiled from it, live with the definition.
	static readonly MappedDbQuery<Post> ByAuthor
		= DbQuery.Text(ListSql).Map<Post>();

	public ValueTask<List<Post>> ByAuthorAsync(
		int author,
		CancellationToken cancellationToken)
		=> ByAuthor
			.Using(connectionFactory)
			.AddParam(author)
			.ToListAsync(cancellationToken);

	public ValueTask<Post?> FirstByAuthorAsync(
		int author,
		CancellationToken cancellationToken)
		=> ByAuthor
			.Using(connectionFactory)
			.AddParam(author)
			.FirstOrDefaultAsync(cancellationToken);
}
#endregion

public static class MapFirstSamples
{
	// The values can come first, and the source last: the same command either way.
	public static ValueTask<List<Post>> ValuesFirstAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		int author,
		CancellationToken cancellationToken)
	{
		#region mapfirst_values_first
		MappedDbQuery<Post> byAuthor
			= DbQuery.Text(ListSql).Map<Post>();

		return byAuthor
			.AddParam(author)
			.Using(connectionFactory)
			.ToListAsync(cancellationToken);
		#endregion
	}

	// Paging is chosen where the query runs, not where it is defined.
	#region mapfirst_paged
	public static ValueTask<List<Post>> PagedAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		MappedDbQuery<Post> byAuthor,
		int author,
		CancellationToken cancellationToken)
		=> byAuthor
			.Using(connectionFactory)
			.AddParam(author)
			.Skip(1).Take(1)
			.ToListAsync(cancellationToken);
	#endregion
}
