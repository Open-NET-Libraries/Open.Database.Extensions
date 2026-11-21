using System.Data;
using System.Runtime.CompilerServices;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Open.Database.Extensions;

namespace Samples;

// The "How little code" page: the same four tasks written with Open.Database.Extensions (ODX), Dapper, EF Core and plain ADO.NET.
// Every method is async-only. Docs\tools\count-region-lines.ps1 counts the lines of each region (blank lines and comments are not counted), and the page prints those numbers.
// Tasks 1 to 3 run against the shared SQLite database (Db.cs); task 4 needs SQL Server (LittleCodeSqlServer.cs), so Program.cs runs it only when LocalDB can be reached.
// The `ct` token and the connection (a factory, a connection string or a context) are what a data-access class already holds, so they come in as arguments.
public static class HowLittleCode
{
	// The SQL of tasks 1 to 3 is the same string for all four libraries.
	public const string AllPostsSql = "select * from posts order by id";
	public const string AuthorPostsSql
		= "select * from posts where author = @author order by id";

	// Tasks 2 and 3 filter in the SQL and then build a value in C# that SQL
	// does not build: this method, shared by all four, is not counted.
	#region little_headline
	public static string Headline(Post post)
		=> $"{post.Title.ToUpperInvariant()}: {post.Body ?? "(no body)"}";
	#endregion

	#region little_setup_ef
	// What EF Core needs and the other three do not: a context, with the
	// model it maps. (Post is the same class the other three read into.)
	public sealed class BlogContext(DbContextOptions<BlogContext> options)
		: DbContext(options)
	{
		public DbSet<Post> Posts => Set<Post>();
	}
	#endregion

	// ---- Task 1: rows to a list ----

	#region little_list_odx
	public static ValueTask<List<Post>> ListPostsOdxAsync(
		IDbConnectionFactory<SqliteConnection> db,
		CancellationToken ct)
		=> db
			.Command(AllPostsSql)
			.Map<Post>()
			.ToListAsync(ct);
	#endregion

	#region little_list_dapper
	public static async Task<List<Post>> ListPostsDapperAsync(
		string connectionString,
		CancellationToken ct)
	{
		await using var connection = new SqliteConnection(connectionString);
		return (await connection.QueryAsync<Post>(
			new CommandDefinition(AllPostsSql, cancellationToken: ct)))
			.AsList();
	}
	#endregion

	#region little_list_ef
	public static Task<List<Post>> ListPostsEfAsync(
		BlogContext db,
		CancellationToken ct)
		=> db.Posts
			.OrderBy(static p => p.Id)
			.ToListAsync(ct);
	#endregion

	#region little_list_ado
	public static async Task<List<Post>> ListPostsAdoAsync(
		string connectionString,
		CancellationToken ct)
	{
		var posts = new List<Post>();
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(ct);
		await using var command = connection.CreateCommand();
		command.CommandText = AllPostsSql;
		await using var reader = await command.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			posts.Add(new Post
			{
				Id = reader.GetInt32(0),
				Author = reader.GetInt32(1),
				Title = reader.GetString(2),
				Body = reader.IsDBNull(3) ? null : reader.GetString(3),
			});
		}

		return posts;
	}
	#endregion

	// ---- Task 2: filter in SQL, project in C#, as an async stream ----

	#region little_stream_odx
	public static IAsyncEnumerable<string> AuthorHeadlinesOdxAsync(
		IDbConnectionFactory<SqliteConnection> db,
		int author,
		CancellationToken ct)
		=> db
			.Command(AuthorPostsSql)
			.AddParam(author)
			.Map<Post>()
			.ToBufferedAsyncStream(ct)
			.Select(static p => Headline(p));
	#endregion

	#region little_stream_dapper
	public static async IAsyncEnumerable<string> AuthorHeadlinesDapperAsync(
		string connectionString,
		int author,
		[EnumeratorCancellation] CancellationToken ct)
	{
		await using var connection = new SqliteConnection(connectionString);
		await foreach (string headline in connection
			.QueryUnbufferedAsync<Post>(AuthorPostsSql, new { author })
			.Select(static p => Headline(p))
			.WithCancellation(ct))
		{
			yield return headline;
		}
	}
	#endregion

	// EF Core translates the Where to SQL, as the other three do with their
	// query. The context must outlive the enumeration, and the token goes to
	// whoever enumerates (WithCancellation), so there is no token argument.
	#region little_stream_ef
	public static IAsyncEnumerable<string> AuthorHeadlinesEfAsync(
		BlogContext db,
		int author)
		=> db.Posts
			.Where(p => p.Author == author)
			.OrderBy(static p => p.Id)
			.AsAsyncEnumerable()
			.Select(static p => Headline(p));
	#endregion

	#region little_stream_ado
	public static async IAsyncEnumerable<string> AuthorHeadlinesAdoAsync(
		string connectionString,
		int author,
		[EnumeratorCancellation] CancellationToken ct)
	{
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(ct);
		await using var command = connection.CreateCommand();
		command.CommandText = AuthorPostsSql;
		command.Parameters.AddWithValue("@author", author);
		await using var reader = await command.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			yield return Headline(new Post
			{
				Id = reader.GetInt32(0),
				Author = reader.GetInt32(1),
				Title = reader.GetString(2),
				Body = reader.IsDBNull(3) ? null : reader.GetString(3),
			});
		}
	}
	#endregion

	// ---- Task 3: the rows of one author in memory, then sync LINQ over them ----

	#region little_buffer_odx
	public static async Task<List<string>> AuthorHeadlinesBufferOdxAsync(
		IDbConnectionFactory<SqliteConnection> db,
		int author,
		CancellationToken ct)
	{
		using PooledArray<Post> posts = await db
			.Command(AuthorPostsSql)
			.AddParam(author)
			.Map<Post>()
			.ToAsyncBuffer(ct)
			.Items;

		return posts
			.Select(static p => Headline(p))
			.ToList();
	}
	#endregion

	#region little_buffer_dapper
	public static async Task<List<string>> AuthorHeadlinesBufferDapperAsync(
		string connectionString,
		int author,
		CancellationToken ct)
	{
		await using var connection = new SqliteConnection(connectionString);
		var posts = await connection.QueryAsync<Post>(
			new CommandDefinition(
				AuthorPostsSql,
				new { author },
				cancellationToken: ct));

		return posts
			.Select(static p => Headline(p))
			.ToList();
	}
	#endregion

	#region little_buffer_ef
	public static async Task<List<string>> AuthorHeadlinesBufferEfAsync(
		BlogContext db,
		int author,
		CancellationToken ct)
	{
		List<Post> posts = await db.Posts
			.Where(p => p.Author == author)
			.OrderBy(static p => p.Id)
			.ToListAsync(ct);

		return posts
			.Select(static p => Headline(p))
			.ToList();
	}
	#endregion

	#region little_buffer_ado
	public static async Task<List<string>> AuthorHeadlinesBufferAdoAsync(
		string connectionString,
		int author,
		CancellationToken ct)
	{
		var posts = new List<Post>();
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(ct);
		await using var command = connection.CreateCommand();
		command.CommandText = AuthorPostsSql;
		command.Parameters.AddWithValue("@author", author);
		await using var reader = await command.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			posts.Add(new Post
			{
				Id = reader.GetInt32(0),
				Author = reader.GetInt32(1),
				Title = reader.GetString(2),
				Body = reader.IsDBNull(3) ? null : reader.GetString(3),
			});
		}

		return posts
			.Select(static p => Headline(p))
			.ToList();
	}
	#endregion

	// ---- Task 4: a stored procedure with two parameters (SQL Server) ----

	#region little_proc_odx
	public static ValueTask<List<Post>> SearchPostsOdxAsync(
		IDbConnectionFactory<SqlConnection> db,
		int author,
		string titleContains,
		CancellationToken ct)
		=> db
			.StoredProcedure("[dbo].[SearchPosts]")
			.AddParam(author)
			.AddParam(titleContains)
			.Map<Post>()
			.ToListAsync(ct);
	#endregion

	#region little_proc_dapper
	public static async Task<List<Post>> SearchPostsDapperAsync(
		string connectionString,
		int author,
		string titleContains,
		CancellationToken ct)
	{
		await using var connection = new SqlConnection(connectionString);
		return (await connection.QueryAsync<Post>(new CommandDefinition(
			"[dbo].[SearchPosts]",
			new { author, title_contains = titleContains },
			commandType: CommandType.StoredProcedure,
			cancellationToken: ct)))
			.AsList();
	}
	#endregion

	#region little_proc_ef
	public static Task<List<Post>> SearchPostsEfAsync(
		BlogContext db,
		int author,
		string titleContains,
		CancellationToken ct)
		=> db.Posts
			.FromSql($"""
				exec [dbo].[SearchPosts]
					@author = {author},
					@title_contains = {titleContains}
				""")
			.ToListAsync(ct);
	#endregion

	#region little_proc_ado
	public static async Task<List<Post>> SearchPostsAdoAsync(
		string connectionString,
		int author,
		string titleContains,
		CancellationToken ct)
	{
		var posts = new List<Post>();
		await using var connection = new SqlConnection(connectionString);
		await connection.OpenAsync(ct);
		await using var command = connection.CreateCommand();
		command.CommandText = "[dbo].[SearchPosts]";
		command.CommandType = CommandType.StoredProcedure;
		command.Parameters.Add("@author", SqlDbType.Int).Value = author;
		command.Parameters
			.Add("@title_contains", SqlDbType.NVarChar, 100)
			.Value = titleContains;
		await using var reader = await command.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			posts.Add(new Post
			{
				Id = reader.GetInt32(0),
				Author = reader.GetInt32(1),
				Title = reader.GetString(2),
				Body = reader.IsDBNull(3) ? null : reader.GetString(3),
			});
		}

		return posts;
	}
	#endregion

	/// <summary>Every column of every post, so the checks compare rows and not just ids.</summary>
	public static string Fingerprint(IEnumerable<Post> posts)
		=> string.Join(
			"|",
			posts.Select(static p
				=> $"{p.Id}:{p.Author}:{p.Title}:{p.Body ?? "<null>"}"));

	/// <summary>Reads a stream to a list, for the checks (the snippets return the stream).</summary>
	public static async Task<List<T>> CollectAsync<T>(
		IAsyncEnumerable<T> stream,
		CancellationToken ct)
	{
		List<T> items = [];
		await foreach (T item in stream.WithCancellation(ct))
			items.Add(item);

		return items;
	}
}
