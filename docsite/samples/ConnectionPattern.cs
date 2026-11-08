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

	public static List<Post> DapperList(string connectionString)
	{
		const int author = 42;
		#region dapper_list_call
		using var connection = new SqliteConnection(connectionString);
		List<Post> posts = connection.Query<Post>(ListSql,
			new { author })
			.AsList();
		#endregion

		return posts;
	}

	public static List<Post> OdeList(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		const int author = 42;
		#region ode_list_call
		// nothing: the factory creates, opens and disposes the connection
		List<Post> posts = connectionFactory.Command(ListSql)
			.AddParam(author)
			.Map<Post>().ToList();
		#endregion

		return posts;
	}

	public static List<Post> OdeListOwn(string connectionString)
	{
		const int author = 42;
		#region ode_list_own
		using var connection = new SqliteConnection(connectionString);
		List<Post> posts = connection.Command(ListSql)
			.AddParam(author)
			.Map<Post>().ToList();
		#endregion

		return posts;
	}

	public static Post? DapperFirst(string connectionString)
	{
		const int id = 3;
		#region dapper_first_call
		using var connection = new SqliteConnection(connectionString);
		Post? post = connection.QueryFirstOrDefault<Post>(OneSql,
			new { id });
		// nothing: "first or default" and Post are in the method name above
		#endregion

		return post;
	}

	public static Post? OdeFirst(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		const int id = 3;
		#region ode_first_call
		// nothing: the factory creates, opens and disposes the connection
		Post? post = connectionFactory.Command(OneSql)
			.AddParam(id)
			.Map<Post>().FirstOrDefault();
		#endregion

		return post;
	}

	public static long DapperScalar(string connectionString)
	{
		const int author = 42;
		#region dapper_scalar_call
		using var connection = new SqliteConnection(connectionString);
		long count = connection.ExecuteScalar<long>(CountSql,
			new { author });
		// nothing: the terminal is the method name above
		#endregion

		return count;
	}

	public static long OdeScalar(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		const int author = 42;
		#region ode_scalar_call
		// nothing: the factory creates, opens and disposes the connection
		long count = connectionFactory.Command(CountSql)
			.AddParam(author)
			.ExecuteScalar<long>();
		#endregion

		return count;
	}

	public static int DapperNonQuery(string connectionString)
	{
		const int author = 42;
		#region dapper_nonquery_call
		using var connection = new SqliteConnection(connectionString);
		int updated = connection.Execute(TouchSql,
			new { author });
		// nothing: the terminal is the method name above
		#endregion

		return updated;
	}

	public static int OdeNonQuery(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		const int author = 42;
		#region ode_nonquery_call
		// nothing: the factory creates, opens and disposes the connection
		int updated = connectionFactory.Command(TouchSql)
			.AddParam(author)
			.ExecuteNonQuery();
		#endregion

		return updated;
	}

	public static void DapperTransaction(string connectionString)
	{
		#region dapper_transaction_call
		using var connection = new SqliteConnection(connectionString);
		connection.Open();
		using var transaction = connection.BeginTransaction();
		try
		{
			connection.Execute(InsertSql, new { id = 100 }, transaction);
			connection.Execute(InsertSql, new { id = 101 }, transaction);
			transaction.Commit();
		}
		catch
		{
			transaction.Rollback();
			throw;
		}
		#endregion
	}

	public static void OdeTransaction(IDbConnectionFactory<SqliteConnection> connectionFactory)
		#region ode_transaction_call
		=> connectionFactory.Using(connection =>
			// nothing: ExecuteTransaction opens the connection if it is closed
			// nothing: ExecuteTransaction begins the transaction
			connection.ExecuteTransaction(transaction =>
			{
				transaction.Command(InsertSql).AddParam("id", 100).ExecuteNonQuery();
				transaction.Command(InsertSql).AddParam("id", 101).ExecuteNonQuery();
				// nothing: it commits when the action returns
			}));
		// nothing: it rolls back if the action throws
		// nothing: Using disposes the connection
		#endregion
	public static List<Post> PlainListAligned(string connectionString)
	{
		const int author = 42;
		#region plain_list_aligned
		using var connection = new SqliteConnection(connectionString);
		connection.Open();
		using var command = connection.CreateCommand();
		command.CommandText = ListSql;
		var parameter = command.CreateParameter();
		parameter.ParameterName = "@author";
		parameter.Value = author;
		command.Parameters.Add(parameter);
		var posts = new List<Post>();
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

	public static List<Post> OdeListAligned(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		const int author = 42;
		#region ode_list_aligned
		// nothing: the factory creates, opens and disposes the connection

		List<Post> posts = connectionFactory
			.Command(ListSql)
			.AddParam(author)



			.Map<Post>()
			.ToList();
		#endregion

		return posts;
	}

	public static Person[] RenamedColumns(SqliteConnection connection)
	{
		#region ode_renamed_columns
		// PersonMaps.PersonMap is the RowMap<Person> declared once in the map_declared sample (Mapping.cs).
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(PersonMaps.PersonMap)
			.ToArray();
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
	public List<Post> ByAuthor(int author) => connectionFactory.Command(ListSql)
		.AddParam(author)
		.Map<Post>().ToList();
}
#endregion

public static class DependencyInjectionSample
{
	public static List<Post> ResolveAndUse()
	{
		#region di_registration
		var services = new ServiceCollection();

		// A singleton is fine: the factory holds no connection, it only creates them.
		services.AddSingleton<IDbConnectionFactory<SqliteConnection>>(
			new DbConnectionFactory<SqliteConnection>(() => new SqliteConnection(Db.ConnectionString)));
		services.AddTransient<PostService>();

		using ServiceProvider provider = services.BuildServiceProvider();
		var service = provider.GetRequiredService<PostService>();
		#endregion

		return service.ByAuthor(42);
	}
}