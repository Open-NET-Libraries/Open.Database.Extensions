using System.Data;
using Open.Database.Extensions;

namespace Samples;

// Compile-only: this needs a SQL Server, so Program.cs never calls it.
public static class SqlServer
{
	public static async Task<Post[]> Example(CancellationToken cancellationToken)
	{
		const int author = 42;
		#region sql_server_factory
		// Open.Database.Extensions.MSSqlClient wraps Microsoft.Data.SqlClient.
		var connectionFactory = new SqlConnectionFactory(
			@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");

		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @author")
			.AddParam(author) // name and type inferred from the variable
			.Map<Post>()
			.ToArrayAsync(cancellationToken);

		int touched = await connectionFactory
			.StoredProcedure("[dbo].[TouchPosts]")
			.AddParam(author)
			.ExecuteNonQueryAsync(cancellationToken);
		#endregion

		_ = touched;
		return posts;
	}

	public static async Task<Post[]> DerivedNames(CancellationToken cancellationToken)
	{
		var connectionFactory = new SqlConnectionFactory(@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");
		var search = new PostSearch { Author = 42, TitleContains = "ode" };

		#region sql_server_derived_names
		// The procedure declares @author and @title_contains.
		// Each argument is matched to a real parameter,
		// so search.TitleContains finds @title_contains.
		Post[] byArguments = await connectionFactory
			.StoredProcedure("[dbo].[SearchPosts]")
			.AddParam(search.Author) // name and type inferred from the member
			.AddParam(search.TitleContains)
			.Map<Post>()
			.ToArrayAsync(cancellationToken);

		// Or give it the whole object:
		// the members the procedure declares are sent, the rest are ignored.
		Post[] posts = await connectionFactory
			.StoredProcedure("[dbo].[SearchPosts]")
			.AddParams(search)
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		_ = byArguments;
		return posts;
	}

	public static async Task<int> ReturnValue(CancellationToken cancellationToken)
	{
		var connectionFactory = new SqlConnectionFactory(@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");
		const int author = 42;

		#region sql_server_return
		// ExecuteReturnAsync<T>() adds the return-value parameter itself,
		// runs the procedure and returns its value:
		// T must be the type the provider returns
		// (a SQL Server procedure returns an int, its RETURN code).
		int returned = await connectionFactory
			.StoredProcedure("[dbo].[TouchPosts]")
			.AddParam(author)
			.ExecuteReturnAsync<int>(cancellationToken);
		#endregion

		return returned;
	}

	public static async Task<Post[]> NullByName(
		string? titleContains,
		CancellationToken cancellationToken)
	{
		var connectionFactory = new SqlConnectionFactory(@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");

		#region sql_server_null_by_name
		// A value of type object has no type to use, so pass one.
		Post[] posts = await connectionFactory
			.StoredProcedure("[dbo].[SearchPosts]")
			.AddParam(
				"title_contains",
				titleContains ?? (object)DBNull.Value,
				SqlDbType.NVarChar)
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		return posts;
	}
}
