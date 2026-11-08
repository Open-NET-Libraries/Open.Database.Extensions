using Dapper;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class DapperComparison
{
	public static List<Post> DapperList(SqliteConnection connection)
	{
		#region dapper_list
		List<Post> posts = connection
			.Query<Post>("select * from posts where author = @author order by id", new { author = 42 })
			.AsList();
		#endregion

		return posts;
	}

	public static List<Post> OdeList(SqliteConnection connection)
	{
		#region ode_list
		List<Post> posts = connection
			.Command("select * from posts where author = @author order by id")
			.AddParam("author", 42)
			.Map<Post>()
			.ToList();
		#endregion

		return posts;
	}

	public static Post? DapperFirst(SqliteConnection connection)
	{
		#region dapper_first
		Post? post = connection.QueryFirstOrDefault<Post>("select * from posts where id = @id", new { id = 3 });
		#endregion

		return post;
	}

	public static Post? OdeFirst(SqliteConnection connection)
	{
		#region ode_first
		Post? post = connection.Command("select * from posts where id = @id").AddParam("id", 3).Map<Post>().FirstOrDefault();
		#endregion

		return post;
	}

	public static long DapperScalar(SqliteConnection connection)
	{
		#region dapper_scalar
		long count = connection.ExecuteScalar<long>("select count(*) from posts");
		#endregion

		return count;
	}

	public static long OdeScalar(SqliteConnection connection)
	{
		#region ode_scalar
		long count = connection.Command("select count(*) from posts").ExecuteScalar<long>();
		#endregion

		return count;
	}

	public static int DapperExecute(SqliteConnection connection)
	{
		#region dapper_execute
		int updated = connection.Execute("update posts set views = views where author = @author", new { author = 42 });
		#endregion

		return updated;
	}

	public static int OdeExecute(SqliteConnection connection)
	{
		#region ode_execute
		int updated = connection.Command("update posts set views = views where author = @author").AddParam("author", 42).ExecuteNonQuery();
		#endregion

		return updated;
	}

	public static List<PostRecord> DapperRecords(SqliteConnection connection)
	{
		#region dapper_record
		// Dapper binds constructor parameters, so a positional record works.
		List<PostRecord> posts = connection.Query<PostRecord>("select id, author, title, body from posts order by id").AsList();
		#endregion

		return posts;
	}

	public static List<PostRecord> OdeRecords(SqliteConnection connection)
	{
		#region ode_record
		// Map<T>() binds the one public constructor, so the same record works.
		List<PostRecord> posts = connection.Command("select id, author, title, body from posts order by id").Map<PostRecord>().ToList();
		#endregion

		return posts;
	}
}
