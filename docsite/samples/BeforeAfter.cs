using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The same four jobs written with plain ADO.NET and with Open.Database.Extensions.
// The README counts the lines inside these regions. The values (author is 42, id is 3) are declared above each region, so the counts cover only the call.
public static class BeforeAfter
{
	public static async Task<List<Post>> PlainListAsync(
		string connectionString,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region plain_list
		var posts = new List<Post>();

		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(cancellationToken);

		await using var command = connection.CreateCommand();
		command.CommandText =
			"select id, author, title, body from posts "
			+ "where author = @author order by id";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
		command.Parameters.Add(parameter);

		await using var reader = await command.ExecuteReaderAsync(cancellationToken);
		while (await reader.ReadAsync(cancellationToken))
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

	public static async Task<Post[]> OdeList(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_list_factory
		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @author order by id")
			.AddParam(author) // name and type inferred from the variable
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<Post?> PlainFirstAsync(
		string connectionString,
		CancellationToken cancellationToken)
	{
		const int id = 3;
		#region plain_first
		Post? post = null;

		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(cancellationToken);

		await using var command = connection.CreateCommand();
		command.CommandText =
			"select id, author, title, body from posts where id = @id";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@id";
		parameter.Value = id;
		command.Parameters.Add(parameter);

		await using var reader = await command.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
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

	public static async Task<Post?> OdeFirstAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int id = 3;
		#region ode_first_factory
		Post? post = await connectionFactory
			.Command("select * from posts where id = @id")
			.AddParam(id) // name and type inferred from the variable
			.Map<Post>()
			.FirstOrDefaultAsync(cancellationToken);
		#endregion

		return post;
	}

	public static async Task<long> PlainScalarAsync(
		string connectionString,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region plain_scalar
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(cancellationToken);

		await using var command = connection.CreateCommand();
		command.CommandText = "select count(*) from posts where author = @author";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
		command.Parameters.Add(parameter);

		long count = (long)(await command.ExecuteScalarAsync(cancellationToken))!;
		#endregion

		return count;
	}

	public static async Task<long> OdeScalarAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_scalar_factory
		long count = await connectionFactory
			.Command("select count(*) from posts where author = @author")
			.AddParam(author) // name and type inferred from the variable
			.ExecuteScalarAsync<long>(cancellationToken);
		#endregion

		return count;
	}

	public static async Task<int> PlainNonQueryAsync(
		string connectionString,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region plain_nonquery
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(cancellationToken);

		await using var command = connection.CreateCommand();
		command.CommandText =
			"update posts set views = views + 1 where author = @author";
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
		command.Parameters.Add(parameter);

		int updated = await command.ExecuteNonQueryAsync(cancellationToken);
		#endregion

		return updated;
	}

	public static async Task<int> OdeNonQueryAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_nonquery
		int updated = await connectionFactory
			.Command("update posts set views = views + 1 where author = @author")
			.AddParam(author) // name and type inferred from the variable
			.ExecuteNonQueryAsync(cancellationToken);
		#endregion

		return updated;
	}
}
