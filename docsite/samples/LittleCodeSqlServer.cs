using Microsoft.Data.SqlClient;

namespace Samples;

// The SQL Server side of task 4 in "How little code".
// Not every machine has SQL Server LocalDB. Program.cs calls TryStartAsync: it returns null
// (and says so) when LocalDB cannot be reached, so the task is compiled but not run there.
// Where a skip would hide a gap, set ODX_REQUIRE_SQL: a missing server is then a failure.
// The table and the procedure live in tempdb and are dropped when the scope is disposed.
public sealed class LittleCodeSqlServer : IAsyncDisposable
{
	public const string RequireVariable = "ODX_REQUIRE_SQL";

	public const string ConnectionString
		= @"Server=(localdb)\MSSQLLocalDB;Database=tempdb;Integrated Security=true;Encrypt=False;Connect Timeout=20";

	const string Table = "dbo.LittleCodePosts";

	LittleCodeSqlServer() { }

	/// <summary>Creates the table and the SearchPosts procedure, or returns null when LocalDB is not available (and ODX_REQUIRE_SQL is not set).</summary>
	public static async Task<LittleCodeSqlServer?> TryStartAsync(
		CancellationToken cancellationToken)
	{
		try
		{
			await ExecuteAsync(
				$"drop procedure if exists dbo.SearchPosts; drop table if exists {Table};",
				cancellationToken);
		}
		catch (Exception ex) when (ex is SqlException or InvalidOperationException or PlatformNotSupportedException or TimeoutException or ArgumentException)
		{
			if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable(RequireVariable)))
			{
				throw new InvalidOperationException(
					$"{RequireVariable} is set, so a missing SQL Server is a failure, not a skip.",
					ex);
			}

			Console.WriteLine(
				$"skipped - SQL Server LocalDB is not available ({ex.GetType().Name}): task 4 of How little code was compiled, not run");
			return null;
		}

		// The same five posts as Db.Seed() writes to SQLite.
		await ExecuteAsync(
			$"""
			create table {Table} (
				id int not null primary key,
				author int not null,
				title nvarchar(100) not null,
				body nvarchar(100) null,
				views int not null);
			insert into {Table} (id, author, title, body, views) values
				(1, 42, N'Post 1', N'Body 1', 10),
				(2, 42, N'Post 2', null, 20),
				(3, 42, N'Post 3', N'Body 3', 30),
				(4, 7, N'Post 4', null, 40),
				(5, 7, N'Post 5', N'Body 5', 50);
			""",
			cancellationToken);

		await ExecuteAsync(
			$"""
			create procedure dbo.SearchPosts
				@author int,
				@title_contains nvarchar(100) = null
			as
			select id, author, title, body
			from {Table}
			where author = @author
				and (@title_contains is null
					or title like N'%' + @title_contains + N'%')
			order by id;
			""",
			cancellationToken);

		return new LittleCodeSqlServer();
	}

	public async ValueTask DisposeAsync()
		=> await ExecuteAsync(
			$"drop procedure if exists dbo.SearchPosts; drop table if exists {Table};",
			CancellationToken.None);

	static async Task ExecuteAsync(
		string sql,
		CancellationToken cancellationToken)
	{
		await using var connection = new SqlConnection(ConnectionString);
		await connection.OpenAsync(cancellationToken);
		await using var command = connection.CreateCommand();
		command.CommandText = sql;
		await command.ExecuteNonQueryAsync(cancellationToken);
	}
}
