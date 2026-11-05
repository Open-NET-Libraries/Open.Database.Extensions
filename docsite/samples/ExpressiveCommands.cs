using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class ExpressiveCommands
{
	public static async Task<Post[]> FactoryForm(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region factory_form
		// The command takes a connection from the factory when a terminal runs, opens it, and disposes it afterwards.
		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @author order by id")
			.AddParam("author", 42)
			.Map<Post>()
			.ToArrayAsync();
		#endregion

		return posts;
	}

	public static Post[] ConnectionForm(SqliteConnection connection)
	{
		#region connection_form
		// You own the connection. The command opens it only if it is closed, and closes only what it opened.
		Post[] posts = connection
			.Command("select * from posts where author = @author order by id")
			.AddParam("author", 42)
			.Map<Post>()
			.ToArray();
		#endregion

		return posts;
	}

	public static Post? FirstPost(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region first_post
		Post? post = connectionFactory
			.Command("select * from posts where id = @id")
			.AddParam("id", 3)
			.Map<Post>()
			.FirstOrDefault();
		#endregion

		return post;
	}

	public static (long Count, int Updated) ScalarAndNonQuery(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region scalar_and_nonquery
		// ExecuteScalar<T> casts the value the provider returns, so T must be that exact type
		// (SQLite counts as long; SQL Server counts as int). Use the Func overload to convert instead.
		long count = connectionFactory
			.Command("select count(*) from posts where author = @author")
			.AddParam("author", 42)
			.ExecuteScalar<long>();

		int updated = connectionFactory
			.Command("update posts set views = views + 1 where author = @author")
			.AddParam("author", 42)
			.ExecuteNonQuery();
		#endregion

		return (count, updated);
	}

	public static int ConvertedScalar(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region scalar_convert
		int count = connectionFactory
			.Command("select count(*) from posts")
			.ExecuteScalar(value => Convert.ToInt32(value));
		#endregion

		return count;
	}

	public static Post[] ConditionalParam(IDbConnectionFactory<SqliteConnection> connectionFactory, int? author)
	{
		#region param_if
		// AddParamIf adds the parameter only when the condition is true: use it when the SQL text varies with the same condition.
		string sql = author.HasValue
			? "select * from posts where author = @author order by id"
			: "select * from posts order by id";

		Post[] posts = connectionFactory
			.Command(sql)
			.AddParamIf(author.HasValue, "author", author)
			.Map<Post>()
			.ToArray();
		#endregion

		return posts;
	}

	public static (ConnectionState ClosedBefore, ConnectionState ClosedAfter, ConnectionState OpenBefore, ConnectionState OpenAfter) Ownership(SqliteConnection connection)
	{
		#region ownership
		// A closed connection is opened for the call and closed again afterwards.
		connection.Close();
		ConnectionState closedBefore = connection.State;
		connection.Command("select 1").ExecuteScalar();
		ConnectionState closedAfter = connection.State;

		// A connection that was already open stays open.
		connection.Open();
		ConnectionState openBefore = connection.State;
		connection.Command("select 1").ExecuteScalar();
		ConnectionState openAfter = connection.State;
		#endregion

		return (closedBefore, closedAfter, openBefore, openAfter);
	}
}
