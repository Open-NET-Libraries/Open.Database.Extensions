using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The same four jobs written with plain ADO.NET and with Open.Database.Extensions.
// The README counts the lines inside these regions. The values (author is 42, id is 3) are declared above each region, so the counts cover only the call.
public static class BeforeAfter
{
	public static List<Post> PlainList(string connectionString)
	{
		const int author = 42;
		#region plain_list
		var posts = new List<Post>();

		using var connection = new SqliteConnection(connectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "select id, author, title, body from posts where author = @author order by id";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
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
		const int author = 42;
		#region ode_list_factory
		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @author order by id")
			.AddParam(author)
			.Map<Post>()
			.ToArrayAsync();
		#endregion

		return posts;
	}

	public static Post? PlainFirst(string connectionString)
	{
		const int id = 3;
		#region plain_first
		Post? post = null;

		using var connection = new SqliteConnection(connectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "select id, author, title, body from posts where id = @id";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@id";
		parameter.Value = id;
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
		const int id = 3;
		#region ode_first_factory
		Post? post = connectionFactory
			.Command("select * from posts where id = @id")
			.AddParam(id)
			.Map<Post>()
			.FirstOrDefault();
		#endregion

		return post;
	}

	public static long PlainScalar(string connectionString)
	{
		const int author = 42;
		#region plain_scalar
		using var connection = new SqliteConnection(connectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "select count(*) from posts where author = @author";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
		command.Parameters.Add(parameter);

		long count = (long)command.ExecuteScalar()!;
		#endregion

		return count;
	}

	public static long OdeScalar(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		const int author = 42;
		#region ode_scalar_factory
		long count = connectionFactory
			.Command("select count(*) from posts where author = @author")
			.AddParam(author)
			.ExecuteScalar<long>();
		#endregion

		return count;
	}

	public static int PlainNonQuery(string connectionString)
	{
		const int author = 42;
		#region plain_nonquery
		using var connection = new SqliteConnection(connectionString);
		connection.Open();

		using var command = connection.CreateCommand();
		command.CommandText = "update posts set views = views + 1 where author = @author";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
		command.Parameters.Add(parameter);

		int updated = command.ExecuteNonQuery();
		#endregion

		return updated;
	}

	public static int OdeNonQuery(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		const int author = 42;
		#region ode_nonquery
		int updated = connectionFactory
			.Command("update posts set views = views + 1 where author = @author")
			.AddParam(author)
			.ExecuteNonQuery();
		#endregion

		return updated;
	}
}
