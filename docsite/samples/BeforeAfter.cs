using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The same four jobs written with plain ADO.NET and with Open.Database.Extensions.
// The README counts the lines inside these regions.
public static class BeforeAfter
{
	public static List<Post> PlainList()
	{
		#region plain_list
		var posts = new List<Post>();

		using var connection = new SqliteConnection(Db.ConnectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "select id, author, title, body from posts where author = @author order by id";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = 42;
		command.Parameters.Add(parameter);

		using var reader = command.ExecuteReader();
		while (reader.Read())
		{
			posts.Add(new Post
			{
				Id = reader.GetInt32(0),
				Author = reader.GetInt32(1),
				Title = reader.GetString(2),
				Body = reader.IsDBNull(3) ? null : reader.GetString(3),
			});
		}
		#endregion

		return posts;
	}

	public static async Task<Post[]> OdeList(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region ode_list_factory
		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @author order by id")
			.AddParam("author", 42)
			.Map<Post>()
			.ToArrayAsync();
		#endregion

		return posts;
	}

	public static Post? PlainFirst()
	{
		#region plain_first
		Post? post = null;

		using var connection = new SqliteConnection(Db.ConnectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "select id, author, title, body from posts where id = @id";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@id";
		parameter.Value = 3;
		command.Parameters.Add(parameter);

		using var reader = command.ExecuteReader();
		if (reader.Read())
		{
			post = new Post
			{
				Id = reader.GetInt32(0),
				Author = reader.GetInt32(1),
				Title = reader.GetString(2),
				Body = reader.IsDBNull(3) ? null : reader.GetString(3),
			};
		}
		#endregion

		return post;
	}

	public static Post? OdeFirst(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region ode_first_factory
		Post? post = connectionFactory
			.Command("select * from posts where id = @id")
			.AddParam("id", 3)
			.Map<Post>()
			.FirstOrDefault();
		#endregion

		return post;
	}

	public static long PlainScalar()
	{
		#region plain_scalar
		using var connection = new SqliteConnection(Db.ConnectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "select count(*) from posts where author = @author";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = 42;
		command.Parameters.Add(parameter);

		long count = (long)command.ExecuteScalar()!;
		#endregion

		return count;
	}

	public static long OdeScalar(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region ode_scalar_factory
		long count = connectionFactory
			.Command("select count(*) from posts where author = @author")
			.AddParam("author", 42)
			.ExecuteScalar<long>();
		#endregion

		return count;
	}

	public static int PlainNonQuery()
	{
		#region plain_nonquery
		using var connection = new SqliteConnection(Db.ConnectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "update posts set views = views + 1 where author = @author";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = 42;
		command.Parameters.Add(parameter);

		int updated = command.ExecuteNonQuery();
		#endregion

		return updated;
	}

	public static int OdeNonQuery(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region ode_nonquery
		int updated = connectionFactory
			.Command("update posts set views = views + 1 where author = @author")
			.AddParam("author", 42)
			.ExecuteNonQuery();
		#endregion

		return updated;
	}
}
