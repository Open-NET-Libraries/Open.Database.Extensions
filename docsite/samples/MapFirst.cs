using Microsoft.Data.Sqlite;
using Open.Database.Extensions;
using static Samples.Sql;

namespace Samples;

// A command factory: the same queries and the same rows, written two ways. The recommended way (mapfirst_after)
// declares each query once, mapped, with no connection, and only binds the source and the values where it runs.
// The other way (mapfirst_before) builds the command and its map on every call.

#region mapfirst_after
public class PostQueriesAfter(IDbConnectionFactory<SqliteConnection> connectionFactory)
{
	// Recommended. Declared once: immutable, no connection, safe in a static field.
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

public static class MapFirstSamples
{
	// The values can come first, and the source last: the same command either way.
	public static ValueTask<List<Post>> ValuesFirstAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		int author,
		CancellationToken cancellationToken)
	{
		#region mapfirst_values_first
		// Costs the same as adding the values after Using: binding the source allocates nothing.
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
