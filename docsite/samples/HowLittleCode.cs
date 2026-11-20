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

	// ---- Task 2: filter and project with async LINQ, returned as a stream ----

	#region little_stream_odx
	public static IAsyncEnumerable<string> AuthorTitlesOdxAsync(
		IDbConnectionFactory<SqliteConnection> db,
		CancellationToken ct)
		=> db
			.Command(AllPostsSql)
			.Map<Post>()
			.ToBufferedAsyncStream(clearOnReturn: false, ct)
			.Where(static p => p.Author == 42)
			.Select(static p => p.Title);
	#endregion

	#region little_stream_dapper
	public static async IAsyncEnumerable<string> AuthorTitlesDapperAsync(
		string connectionString,
		[EnumeratorCancellation] CancellationToken ct)
	{
		await using var connection = new SqliteConnection(connectionString);
		await foreach (string title in connection
			.QueryUnbufferedAsync<Post>(AllPostsSql)
			.Where(static p => p.Author == 42)
			.Select(static p => p.Title)
			.WithCancellation(ct))
		{
			yield return title;
		}
	}
	#endregion

	#region little_stream_ef
	public static IAsyncEnumerable<string> AuthorTitlesEfAsync(
		BlogContext db)
		=> db.Posts
			.OrderBy(static p => p.Id)
			.AsAsyncEnumerable()
			.Where(static p => p.Author == 42)
			.Select(static p => p.Title);
	#endregion

	// What EF Core usually does instead: the filter and the projection are
	// part of the SQL, so the database sends only the titles.
	// It is not the same work, so it is not in the table.
	#region little_stream_ef_sql
	public static IAsyncEnumerable<string> AuthorTitlesEfSqlAsync(
		BlogContext db)
		=> db.Posts
			.Where(static p => p.Author == 42)
			.OrderBy(static p => p.Id)
			.Select(static p => p.Title)
			.AsAsyncEnumerable();
	#endregion

	#region little_stream_ado
	public static async IAsyncEnumerable<string> AuthorTitlesAdoAsync(
		string connectionString,
		[EnumeratorCancellation] CancellationToken ct)
	{
		await using var connection = new SqliteConnection(connectionString);
		await connection.OpenAsync(ct);
		await using var command = connection.CreateCommand();
		command.CommandText = AllPostsSql;
		await using var reader = await command.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			if (reader.GetInt32(1) == 42)
				yield return reader.GetString(2);
		}
	}
	#endregion

	// ---- Task 3: the whole set in memory, then sync LINQ over it ----

	#region little_buffer_odx
	public static async Task<List<string>> AuthorTitlesBufferOdxAsync(
		IDbConnectionFactory<SqliteConnection> db,
		CancellationToken ct)
	{
		using PooledArray<Post> posts = await db
			.Command(AllPostsSql)
			.Map<Post>()
			.ToAsyncBuffer(clearOnDispose: false, ct)
			.Items;

		return posts
			.Where(static p => p.Author == 42)
			.Select(static p => p.Title)
			.ToList();
	}
	#endregion

	#region little_buffer_dapper
	public static async Task<List<string>> AuthorTitlesBufferDapperAsync(
		string connectionString,
		CancellationToken ct)
	{
		await using var connection = new SqliteConnection(connectionString);
		var posts = await connection.QueryAsync<Post>(
			new CommandDefinition(AllPostsSql, cancellationToken: ct));

		return posts
			.Where(static p => p.Author == 42)
			.Select(static p => p.Title)
			.ToList();
	}
	#endregion

	#region little_buffer_ef
	public static async Task<List<string>> AuthorTitlesBufferEfAsync(
		BlogContext db,
		CancellationToken ct)
	{
		List<Post> posts = await db.Posts
			.OrderBy(static p => p.Id)
			.ToListAsync(ct);

		return posts
			.Where(static p => p.Author == 42)
			.Select(static p => p.Title)
			.ToList();
	}
	#endregion

	#region little_buffer_ado
	public static async Task<List<string>> AuthorTitlesBufferAdoAsync(
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

		return posts
			.Where(static p => p.Author == 42)
			.Select(static p => p.Title)
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
