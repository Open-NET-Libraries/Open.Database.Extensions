using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class ExpressiveCommands
{
	public static async Task<Post[]> FactoryForm(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region factory_form
		// The command takes a connection from the factory
		// when a terminal runs, opens it, and disposes it afterwards.
		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @author order by id")
			.AddParam(author) // name and type inferred from the variable
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<Post[]> ConnectionFormAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region connection_form
		// You own the connection.
		// The command opens it only if it is closed,
		// and closes only what it opened.
		Post[] posts = await connection
			.Command("select * from posts where author = @author order by id")
			.AddParam(author) // name and type inferred from the variable
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<int[]> ColumnOfIds(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		#region single_value_async
		// The terminals take the type:
		// ToArrayAsync<int>(token) reads the first column of each row,
		// as Map<int>().ToArrayAsync(token) does.
		int[] ids = await connectionFactory
			.Command("select id from posts order by id")
			.ToArrayAsync<int>(cancellationToken);
		#endregion

		return ids;
	}

	public static async Task<Post?> FirstPostAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int id = 3;
		#region first_post
		Post? post = await connectionFactory
			.Command("select * from posts where id = @id")
			.AddParam(id) // name and type inferred from the variable
			.Map<Post>()
			.FirstOrDefaultAsync(cancellationToken);
		#endregion

		return post;
	}

	public static async Task<(long Count, int Updated)> ScalarAndNonQueryAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;
		#region scalar_and_nonquery
		// ExecuteScalarAsync<T> converts the value the provider returns,
		// as Map<T>() does: a count can be read as long or as int
		// (SQLite counts as long; SQL Server counts as int).
		// Use the Func overload to convert in your own way.
		long count = await connectionFactory
			.Command("select count(*) from posts where author = @author")
			.AddParam(author) // name and type inferred from the variable
			.ExecuteScalarAsync<long>(cancellationToken);

		int updated = await connectionFactory
			.Command("update posts set views = views + 1 where author = @author")
			.AddParam(author)
			.ExecuteNonQueryAsync(cancellationToken);
		#endregion

		return (count, updated);
	}

	public static async Task<long> InsertAndReadIdAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		const int author = 42;

		#region generated_id
		// The insert and a select of the new id in one command;
		// ExecuteScalarAsync<T>() reads the id
		// (SQLite: last_insert_rowid()).
		long newId = await connectionFactory
			.Command("insert into posts (author, title, body, views) values (@author, 'New', null, 0); select last_insert_rowid()")
			.AddParam(author) // name and type inferred from the variable
			.ExecuteScalarAsync<long>(cancellationToken);
		#endregion

		return newId;
	}

	public static async Task<int> ConvertedScalarAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		#region scalar_convert
		int count = await connectionFactory
			.Command("select count(*) from posts")
			.ExecuteScalarAsync(static value => Convert.ToInt32(value), cancellationToken);
		#endregion

		return count;
	}

	public static async Task<int> ScalarReadAsInt(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		#region scalar_read_as_int
		// SQLite returns count(*) as a long (a bigint).
		// ExecuteScalarAsync<int> converts it, as Map<int>() does:
		// a value that does not fit in an int throws ValueMappingException.
		int count = await connectionFactory
			.Command("select count(*) from posts")
			.ExecuteScalarAsync<int>(cancellationToken);
		#endregion

		return count;
	}

	public static async Task<Post[]> ConditionalParamAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		int? author,
		CancellationToken cancellationToken)
	{
		#region param_if
		// AddParamIf adds the parameter only when the condition is true:
		// use it when the SQL text varies with the same condition.
		string sql = author.HasValue
			? "select * from posts where author = @author order by id"
			: "select * from posts order by id";

		Post[] posts = await connectionFactory
			.Command(sql)
			.AddParamIf(author.HasValue, author)
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<Post[]> ParamNamedFromTheArgumentAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		var search = new PostSearch { Author = 7 };

		#region param_from_argument
		// The parameter is named from the argument:
		// search.Author adds a parameter named Author.
		// A text command has no procedure to ask,
		// so the name is used as written.
		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @Author order by id")
			.AddParam(search.Author) // name and type inferred from the member
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<(ConnectionState ClosedBefore, ConnectionState ClosedAfter, ConnectionState OpenBefore, ConnectionState OpenAfter)> OwnershipAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region ownership
		// A closed connection is opened for the call
		// and closed again afterwards.
		await connection.CloseAsync();
		ConnectionState closedBefore = connection.State;
		await connection.Command("select 1").ExecuteScalarAsync(cancellationToken);
		ConnectionState closedAfter = connection.State;

		// A connection that was already open stays open.
		await connection.OpenAsync(cancellationToken);
		ConnectionState openBefore = connection.State;
		await connection.Command("select 1").ExecuteScalarAsync(cancellationToken);
		ConnectionState openAfter = connection.State;
		#endregion

		return (closedBefore, closedAfter, openBefore, openAfter);
	}
}
