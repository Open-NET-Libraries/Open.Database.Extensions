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
}
