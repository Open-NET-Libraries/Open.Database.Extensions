using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

/// <summary>The buffered Results calls return the queue they build; Program.cs checks what the docs say about it.</summary>
public static class BufferedQueue
{
	public static (int Count, string FirstPass, string SecondPass, int AfterDequeue) Replay(SqliteConnection connection)
	{
		#region buffered_queue
		using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "select * from posts order by id";

		// The rows are read and mapped before the call returns; the queue is yours.
		Queue<Post> posts = command.Results<Post>();

		int count = posts.Count;
		string firstPass = string.Join(",", posts.Select(post => post.Id));
		string secondPass = string.Join(",", posts.Select(post => post.Id)); // enumerating did not empty it

		// To release each row as you go, dequeue it.
		while (posts.TryDequeue(out Post? post))
			Console.WriteLine(post.Title);
		#endregion

		return (count, firstPass, secondPass, posts.Count);
	}

	public static async Task<Queue<object?>> FirstColumn(SqliteConnection connection)
	{
		#region buffered_queue_first_column
		using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "select body from posts order by id";

		// A NULL column is already null here, never DBNull.
		Queue<object?> bodies = await command.FirstOrdinalResultsAsync();
		#endregion

		return bodies;
	}
}