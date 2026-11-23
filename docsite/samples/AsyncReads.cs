using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The samples behind the "Row reads" article.
// SQLite completes ReadAsync synchronously,
// so these show the calls and that the rows are the same,
// not the cost of either read.
public static class AsyncReads
{
	public static async Task<List<Post>> DefaultAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region asyncreads_default
		// The same query, two reads.
		// Rows are read with Read().
		List<Post> posts = await connection
			.Command("select * from posts order by id")
			// No AsyncReads(): the default.
			.Map<Post>()
			.ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<List<Post>> OptInAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region asyncreads_optin
		// The same query, two reads.
		// Rows are read with ReadAsync.
		List<Post> posts = await connection
			.Command("select * from posts order by id")
			.AsyncReads()
			.Map<Post>()
			.ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<List<Post>> ViewAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region asyncreads_view
		// On the mapped view, after Map<T>():
		// the same opt-in as on the command.
		List<Post> posts = await connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.AsyncReads()
			.ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<List<Post>> DbCommandAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region asyncreads_dbcommand
		// A plain IDbCommand you already have
		// takes the same call on its mapped view.
		await using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "select * from posts order by id";

		List<Post> posts = await command
			.Map<Post>()
			.AsyncReads()
			.ToListAsync(cancellationToken);
		#endregion

		return posts;
	}

	public static async Task<List<string>> FuncAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region asyncreads_func
		// Map(Func) is the one to think twice about: with AsyncReads()
		// it allocated 73% more in the measured run (16,812 KB
		// against 9,713 KB).
		List<string> titles = await connection
			.Command("select title from posts order by id")
			.Map(static record
				=> record.GetString(0))
			.AsyncReads()
			.ToListAsync(cancellationToken);
		#endregion

		return titles;
	}

	public static async Task<(List<Post> Before, List<Post> After)> OrderAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region asyncreads_order
		// AsyncReads() goes before or after Skip and Take.
		List<Post> before = await connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.AsyncReads()
			.Skip(1)
			.Take(2)
			.ToListAsync(cancellationToken);

		List<Post> after = await connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.Skip(1)
			.Take(2)
			.AsyncReads()
			.ToListAsync(cancellationToken);
		#endregion

		return (before, after);
	}
}
