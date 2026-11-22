using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;
using Open.Database.Extensions.Core;

namespace Samples;

/// <summary>
/// The calls that were deprecated in 10.x and the rc series are removed in 11.0.
/// Each "before" region is the removed call as a comment (it no longer compiles);
/// each "after" region is the call to type instead, compiled and run by Program.cs.
/// </summary>
public static class Deprecated
{
	const string PostsSql = "select * from posts order by id";
	const string IdsSql = "select id from posts order by id";

	public static string Key(IEnumerable<Post> posts) => string.Join(",", posts.Select(static post => $"{post.Id}:{post.Title}"));

	public static string Key(IEnumerable<long> ids) => string.Join(",", ids);

	public static string Key(IEnumerable<Dictionary<string, object?>> rows)
		=> string.Join(";", rows.Select(static row => string.Join(",", row.OrderBy(static pair => pair.Key, StringComparer.Ordinal).Select(static pair => $"{pair.Key}={pair.Value}"))));

	// The selector the old calls and Map both take.
	static long Id(IDataRecord record) => record.GetInt64(0);

	public static string Results(SqliteConnection connection)
	{
		#region deprecated_results_before
		// Removed in 11.0, synchronous:
		// List<Post> before = connection
		//     .Command(PostsSql)
		//     .Results<Post>()
		//     .ToList();
		#endregion

		#region deprecated_results_after
		// Synchronous, like the call it replaces;
		// the async form is ToListAsync(cancellationToken).
		List<Post> after = connection
			.Command(PostsSql)
			.Map<Post>()
			.ToList();
		#endregion

		return Key(after);
	}

	public static async Task<string> ResultsAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region deprecated_resultsasync_before
		// Removed in 11.0:
		// List<Post> before = (await connection
		//     .Command(PostsSql)
		//     .ResultsAsync<Post>())
		//     .ToList();
		#endregion

		#region deprecated_resultsasync_after
		List<Post> after = await connection
			.Command(PostsSql)
			.Map<Post>()
			.ToListAsync(cancellationToken);
		#endregion

		return Key(after);
	}

	public static string ResultsFromDataTable(SqliteConnection connection)
	{
		using SqliteCommand newCommand = connection.CreateCommand();
		newCommand.CommandText = PostsSql;

		#region deprecated_resultsfromdatatable_before
		// Removed in 11.0, synchronous:
		// List<Post> before = oldCommand
		//     .ResultsFromDataTable<Post>()
		//     .ToList();
		#endregion

		#region deprecated_resultsfromdatatable_after
		// Synchronous, like the call it replaces:
		// ToDataTable has no async form.
		// Reading the map first keeps the old behavior of failing
		// before the command runs for a type that cannot be mapped.
		RowMap<Post> map = RowMap<Post>.Default;
		using var table = newCommand.ToDataTable();
		List<Post> after = table
			.Map(map)
			.ToList();
		#endregion

		return Key(after);
	}

	public static string SelectorForms(SqliteConnection connection)
	{
		IExecuteReader command = connection.Command(IdsSql);

		#region deprecated_selector_before
		// Removed in 11.0, synchronous:
		// long? first = command.FirstOrDefault(Id);
		// List<long> taken = command.Take(2, Id);
		// List<long> skipped = command.Skip(3, Id);
		// List<long> sliced = command.SkipThenTake(1, 2, Id);
		#endregion

		#region deprecated_selector_after
		// Synchronous, like the calls they replace;
		// each terminal has an async form that takes a token.
		long? firstAfter = command
			.Map(Id)
			.FirstOrDefault();
		List<long> takenAfter = command
			.Map(Id)
			.Take(2)
			.ToList();
		List<long> skippedAfter = command
			.Map(Id)
			.Skip(3)
			.ToList();
		List<long> slicedAfter = command
			.Map(Id)
			.Skip(1)
			.Take(2)
			.ToList();
		#endregion

		return $"{firstAfter}|{Key(takenAfter)}|{Key(skippedAfter)}|{Key(slicedAfter)}";
	}

	public static async Task<string> TakeAsyncAndToListAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region deprecated_takeasync_before
		// Removed in 11.0:
		// IList<long> taken = await connection
		//     .Command(IdsSql)
		//     .TakeAsync(Id, 2);
		// List<long> all = await connection
		//     .Command(IdsSql)
		//     .ToListAsync(Id);
		#endregion

		#region deprecated_takeasync_after
		List<long> takenAfter = await connection
			.Command(IdsSql)
			.Map(Id)
			.Take(2)
			.ToListAsync(cancellationToken);
		List<long> allAfter = await connection
			.Command(IdsSql)
			.Map(Id)
			.ToListAsync(cancellationToken);
		#endregion

		return $"{Key(takenAfter)}|{Key(allAfter)}";
	}

	/// <summary>Nothing in the library returned a QueryResultCollection, so there is nothing to compare.</summary>
	public static async Task<int> CollectionAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region deprecated_collection_before
		// Removed in 11.0:
		// var rows = new QueryResultCollection<long>([0], ["id"], ids);
		#endregion

		#region deprecated_collection_after
		List<long> ids = await connection
			.Command(IdsSql)
			.Map(Id)
			.ToListAsync(cancellationToken);
		#endregion

		return ids.Count;
	}

	public static async Task<(string After, string AfterAsync)> MappedDictionaries(SqliteConnection connection)
	{
		#region deprecated_mappeddictionaries_before
		// Removed in 11.0, synchronous:
		// IEnumerable<Dictionary<string, object?>> before = source.AsMappedDictionaries();
		#endregion

		#region deprecated_mappeddictionaries_after
		// The first line is synchronous, like the call it replaces;
		// the second is its async form.
		IEnumerable<Dictionary<string, object?>> after = connection
			.Command(IdsSql)
			.Retrieve()
			.DequeueAsMappedDictionaries();
		IEnumerable<Dictionary<string, object?>> afterAsync = (await connection
			.Command(IdsSql)
			.RetrieveAsync())
			.DequeueAsMappedDictionaries();
		#endregion

		return (Key(after), Key(afterAsync));
	}

	public static string DequeueAs(SqliteConnection connection)
	{
		#region deprecated_dequeueas_before
		// Removed in 11.0, synchronous:
		// List<Post> before = connection
		//     .Command(PostsSql)
		//     .Retrieve()
		//     .DequeueAs<Post>()
		//     .ToList();
		#endregion

		#region deprecated_dequeueas_after
		// Synchronous, like the call it replaces;
		// the async form is ToListAsync(cancellationToken).
		List<Post> after = connection
			.Command(PostsSql)
			.Map<Post>()
			.ToList();
		#endregion

		return Key(after);
	}
}
