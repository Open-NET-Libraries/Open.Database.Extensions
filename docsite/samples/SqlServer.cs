using Open.Database.Extensions;

namespace Samples;

// Compile-only: this needs a SQL Server, so Program.cs never calls it.
public static class SqlServer
{
	public static async Task<Post[]> Example()
	{
		#region sql_server_factory
		// Open.Database.Extensions.MSSqlClient wraps Microsoft.Data.SqlClient.
		var connectionFactory = new SqlConnectionFactory(@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");

		Post[] posts = await connectionFactory
			.Command("select * from posts where author = @author")
			.AddParam("author", 42)
			.Map<Post>()
			.ToArrayAsync();

		int touched = await connectionFactory
			.StoredProcedure("[dbo].[TouchPosts]")
			.AddParam("author", 42)
			.ExecuteNonQueryAsync();
		#endregion

		_ = touched;
		return posts;
	}

	public static async Task<Post[]> DerivedNames()
	{
		var connectionFactory = new SqlConnectionFactory(@"Server=(localdb)\MSSQLLocalDB;Database=Blog;Integrated Security=true");
		var search = new PostSearch { Author = 42, TitleContains = "ode" };

		#region sql_server_derived_names
		// The procedure declares @author and @title_contains. Each argument is matched to a real parameter, so search.TitleContains finds @title_contains.
		Post[] byArguments = await connectionFactory
			.StoredProcedure("[dbo].[SearchPosts]")
			.AddParam(search.Author)
			.AddParam(search.TitleContains)
			.Map<Post>()
			.ToArrayAsync();

		// Or give it the whole object: the members the procedure declares are sent, the rest are ignored.
		Post[] posts = await connectionFactory
			.StoredProcedure("[dbo].[SearchPosts]")
			.AddParams(search)
			.Map<Post>()
			.ToArrayAsync();
		#endregion

		_ = byArguments;
		return posts;
	}
}
