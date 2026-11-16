using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Open.Database.Extensions;
using static Samples.Sql;

namespace Samples;

// The comparisons in the docs: every pair has the same number of lines, so the rows line up side by side.
// Row 1 gets the connection, row 2 is the command or query, row 3 the parameters, row 4 the terminal and mapping.
public static class Sql
{
	#region shared_sql
	public const string ListSql = "select * from posts where author = @author order by id";
	public const string OneSql = "select * from posts where id = @id";
	public const string CountSql = "select count(*) from posts where author = @author";
	public const string TouchSql = "update posts set views = views where author = @author";
	public const string InsertSql = "insert into posts (id, author, title, body, views) values (@id, 1, 'Temporary', null, 0)";
	#endregion
}

public static class ConnectionPattern
{
	public static DbConnectionFactory<SqliteConnection> Setup(string connectionString)
	{
		#region ode_setup_once
		var connectionFactory = new DbConnectionFactory<SqliteConnection>(() => new SqliteConnection(connectionString));
		#endregion

		return connectionFactory;
	}

	public static async Task<List<Post>> DapperList(string connectionString)
	{
		const int author = 42;
		#region dapper_list_call
		await using var connection = new SqliteConnection(connectionString);
		List<Post> posts = (await connection.QueryAsync<Post>(ListSql,
			new { author }))
			.AsList();
		#endregion

		return posts;
	}

	public static async Task<List<Post>> OdeList(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_list_call
		// nothing: the factory creates, opens and disposes the connection
		List<Post> posts = await connectionFactory.Command(ListSql)
			.AddParam(author) // name and type inferred from the variable
			.Map<Post>().ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<List<Post>> OdeListOwn(
		string connectionString,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_list_own
		await using var connection = new SqliteConnection(connectionString);
		List<Post> posts = await connection.Command(ListSql)
			.AddParam(author) // name and type inferred from the variable
			.Map<Post>().ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<Post?> DapperFirstAsync(string connectionString)
	{
		const int id = 3;
		#region dapper_first_call
		await using var connection = new SqliteConnection(connectionString);
		Post? post = await connection.QueryFirstOrDefaultAsync<Post>(OneSql,
			new { id });
		// nothing: "first or default" and Post are in the method name above
		#endregion

		return post;
	}

	public static async Task<Post?> OdeFirstAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int id = 3;
		#region ode_first_call
		// nothing: the factory creates, opens and disposes the connection
		Post? post = await connectionFactory.Command(OneSql)
			.AddParam(id) // name and type inferred from the variable
			.Map<Post>().FirstOrDefaultAsync(cancellationToken);
		#endregion

		return post;
	}

	public static async Task<long> DapperScalarAsync(string connectionString)
	{
		const int author = 42;
		#region dapper_scalar_call
		await using var connection = new SqliteConnection(connectionString);
		long count = await connection.ExecuteScalarAsync<long>(CountSql,
			new { author });
		// nothing: the terminal is the method name above
		#endregion

		return count;
	}

	public static async Task<long> OdeScalarAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_scalar_call
		// nothing: the factory creates, opens and disposes the connection
		long count = await connectionFactory.Command(CountSql)
			.AddParam(author) // name and type inferred from the variable
			.ExecuteScalarAsync<long>(cancellationToken);
		#endregion

		return count;
	}

	public static async Task<int> DapperNonQueryAsync(string connectionString)
	{
		const int author = 42;
		#region dapper_nonquery_call
		await using var connection = new SqliteConnection(connectionString);
		int updated = await connection.ExecuteAsync(TouchSql,
			new { author });
		// nothing: the terminal is the method name above
		#endregion

		return updated;
	}

	public static async Task<int> OdeNonQueryAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_nonquery_call
		// nothing: the factory creates, opens and disposes the connection
		int updated = await connectionFactory.Command(TouchSql)
			.AddParam(author) // name and type inferred from the variable
			.ExecuteNonQueryAsync(cancellationToken);
		#endregion

		return updated;
	}

	public static async Task DapperTransactionAsync(string connectionString)
	{
		#region dapper_transaction_call
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync();
		await using var transaction = await connection.BeginTransactionAsync();
		try
		{
			await connection.ExecuteAsync(InsertSql, new { id = 100 }, transaction);
			await connection.ExecuteAsync(InsertSql, new { id = 101 }, transaction);
			await transaction.CommitAsync();
		}
		catch
		{
			await transaction.RollbackAsync();
			throw;
		}
		#endregion
	}

	public static ValueTask OdeTransactionAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
		#region ode_transaction_call
		=> connectionFactory.UsingAsync(connection =>
			// nothing: ExecuteTransactionAsync opens a closed connection
			// nothing: ExecuteTransactionAsync begins the transaction
			connection.ExecuteTransactionAsync(async transaction =>
			{
				await transaction.Command(InsertSql).AddParam("id", 100).ExecuteNonQueryAsync(cancellationToken);
				await transaction.Command(InsertSql).AddParam("id", 101).ExecuteNonQueryAsync(cancellationToken);
				// nothing: it commits when the action returns
			}, cancellationToken));
		// nothing: it rolls back if the action throws
		// nothing: UsingAsync disposes the connection
		#endregion

	public static async Task<List<Post>> PlainListAligned(
		string connectionString,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region plain_list_aligned
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(cancellationToken);
		await using var command = connection.CreateCommand();
		command.CommandText = ListSql;
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
		command.Parameters.Add(parameter);
		var posts = new List<Post>();
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

	public static async Task<List<Post>> OdeListAligned(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region ode_list_aligned
		// nothing: the factory creates, opens and disposes the connection

		List<Post> posts = await connectionFactory
			.Command(ListSql)
			.AddParam(author) // name and type inferred from the variable



			.Map<Post>()
			.ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<Person[]> RenamedColumnsAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region ode_renamed_columns
		// PersonMaps.PersonMap is the RowMap<Person>
		// declared once in the map_declared sample (Mapping.cs).
		Person[] people = await connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(PersonMaps.PersonMap)
			.ToArrayAsync(cancellationToken);
		#endregion

		return people;
	}

	// Compile-only: needs a SQL Server.
	public static IServiceCollection RegisterSqlServer(IServiceCollection services, string connectionString)
	{
		#region sql_server_registration
		services.AddSingleton<IDbConnectionFactory<SqlConnection>>(new SqlConnectionFactory(connectionString));
		#endregion

		return services;
	}
}

#region di_consumer
public class PostService(IDbConnectionFactory<SqliteConnection> connectionFactory)
{
	public ValueTask<List<Post>> ByAuthorAsync(int author, CancellationToken cancellationToken)
		=> connectionFactory.Command(ListSql)
			.AddParam(author) // name and type inferred from the variable
			.Map<Post>().ToListAsync(cancellationToken);
}
#endregion

public static class DependencyInjectionSample
{
	public static async Task<List<Post>> ResolveAndUseAsync(CancellationToken cancellationToken)
	{
		#region di_registration
		var services = new ServiceCollection();

		// A singleton is fine: the factory holds no connection,
		// it only creates them.
		services.AddSingleton<IDbConnectionFactory<SqliteConnection>>(
			new DbConnectionFactory<SqliteConnection>(static () => new SqliteConnection(Db.ConnectionString)));
		services.AddTransient<PostService>();

		using ServiceProvider provider = services.BuildServiceProvider();
		var service = provider.GetRequiredService<PostService>();
		#endregion

		return await service.ByAuthorAsync(42, cancellationToken);
	}
}
