using Dapper;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class DapperComparison
{
	public static async Task<List<Post>> DapperListAsync(SqliteConnection connection)
	{
		#region dapper_list
		List<Post> posts = (await connection
			.QueryAsync<Post>(
				"select * from posts where author = @author order by id",
				new { author = 42 }))
			.AsList();
		#endregion

		return posts;
	}

	public static async Task<List<Post>> OdeListAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region ode_list
		List<Post> posts = await connection
			.Command("select * from posts where author = @author order by id")
			.AddParam("author", 42)
			.Map<Post>()
			.ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<Post?> DapperFirstAsync(SqliteConnection connection)
	{
		#region dapper_first
		Post? post = await connection.QueryFirstOrDefaultAsync<Post>(
			"select * from posts where id = @id",
			new { id = 3 });
		#endregion

		return post;
	}

	public static async Task<Post?> OdeFirstAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region ode_first
		Post? post = await connection
			.Command("select * from posts where id = @id")
			.AddParam("id", 3)
			.Map<Post>()
			.FirstOrDefaultAsync(cancellationToken);
		#endregion

		return post;
	}

	public static async Task<long> DapperScalarAsync(SqliteConnection connection)
	{
		#region dapper_scalar
		long count = await connection.ExecuteScalarAsync<long>(
			"select count(*) from posts");
		#endregion

		return count;
	}

	public static async Task<long> OdeScalarAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region ode_scalar
		long count = await connection
			.Command("select count(*) from posts")
			.ExecuteScalarAsync<long>(cancellationToken);
		#endregion

		return count;
	}

	public static async Task<int> DapperExecuteAsync(SqliteConnection connection)
	{
		#region dapper_execute
		int updated = await connection.ExecuteAsync(
			"update posts set views = views where author = @author",
			new { author = 42 });
		#endregion

		return updated;
	}

	public static async Task<int> OdeExecuteAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region ode_execute
		int updated = await connection
			.Command("update posts set views = views where author = @author")
			.AddParam("author", 42)
			.ExecuteNonQueryAsync(cancellationToken);
		#endregion

		return updated;
	}

	public static async Task<List<PostRecord>> DapperRecordsAsync(SqliteConnection connection)
	{
		#region dapper_record
		// Dapper binds constructor parameters, so a positional record works.
		List<PostRecord> posts = (await connection
			.QueryAsync<PostRecord>(
				"select id, author, title, body from posts order by id"))
			.AsList();
		#endregion

		return posts;
	}

	public static async Task<List<PostRecord>> OdeRecordsAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region ode_record
		// Map<T>() binds the one public constructor, so the same record works.
		List<PostRecord> posts = await connection
			.Command("select id, author, title, body from posts order by id")
			.Map<PostRecord>()
			.ToListAsync(cancellationToken);
		#endregion

		return posts;
	}
}
