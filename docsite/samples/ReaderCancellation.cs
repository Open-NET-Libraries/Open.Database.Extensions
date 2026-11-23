using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class ReaderCancellation
{
	public static async Task<(int[] Seen, bool Canceled)> SelectAsyncCanceled(
		SqliteConnection connection)
	{
		var seen = new List<int>();
		bool canceled = false;

		#region reader_cancellation
		// The token is checked before each read.
		// The token goes to ReadAsync; a cancel throws
		// OperationCanceledException and the read is not made.
		// The rows already handed out stay handed out.
		// Once a read has returned no row, a cancel is not an error.
		// To stop early without an exception, break out of the loop.
		using var cts = new CancellationTokenSource();
		await using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "select id from posts order by id";
		await using SqliteDataReader reader = await command.ExecuteReaderAsync();
		try
		{
			await foreach (int id in reader.SelectAsync(
				static record => record.GetInt32(0),
				cts.Token))
			{
				seen.Add(id);
				if (id == 2) cts.Cancel();
			}
		}
		catch (OperationCanceledException)
		{
			canceled = true;
		}
		#endregion

		return ([.. seen], canceled);
	}

	#region reader_cancellation_sync
	// The synchronous form of the same rule is in the Synchronous namespace.
	#endregion
}
