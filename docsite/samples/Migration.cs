using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;
using Open.Database.Extensions.Synchronous;
using Open.Database.Extensions.Core;

namespace Samples;

/// <summary>
/// The calls that were removed in 11.0, and the calls to type instead.
/// The old calls appear only as text in the migration guide (they no longer compile);
/// each "after" region here is compiled and run by Program.cs.
/// </summary>
public static class Migration
{
	const string PostsSql = "select * from posts order by id";
	const string IdsSql = "select id from posts order by id";

	public static string Key(IEnumerable<Post> posts) => string.Join(",", posts.Select(static post => $"{post.Id}:{post.Title}"));

	public static string Key(IEnumerable<long> ids) => string.Join(",", ids);

	public static string Key(IEnumerable<Dictionary<string, object?>> rows)
		=> string.Join(";", rows.Select(static row => string.Join(",", row.OrderBy(static pair => pair.Key, StringComparer.Ordinal).Select(static pair => $"{pair.Key}={pair.Value}"))));

	// The selector the old calls and Map both take.
	static long Id(IDataRecord record) => record.GetInt64(0);

	public static async Task<string> Results(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region migration_results_after
		List<Post> after = await connection
			.Command(PostsSql)
			.Map<Post>()
			.ToListAsync(cancellationToken);
		#endregion

		return Key(after);
	}

	public static async Task<string> ResultsAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region migration_resultsasync_after
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

		#region migration_resultsfromdatatable_after
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

	public static async Task<string> SelectorForms(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		IExecuteReader command = connection.Command(IdsSql);

		#region migration_selector_after
		long? firstAfter = await command
			.Map(Id)
			.FirstOrDefaultAsync(cancellationToken);
		List<long> takenAfter = await command
			.Map(Id)
			.Take(2)
			.ToListAsync(cancellationToken);
		List<long> skippedAfter = await command
			.Map(Id)
			.Skip(3)
			.ToListAsync(cancellationToken);
		List<long> slicedAfter = await command
			.Map(Id)
			.Skip(1)
			.Take(2)
			.ToListAsync(cancellationToken);
		#endregion

		return $"{firstAfter}|{Key(takenAfter)}|{Key(skippedAfter)}|{Key(slicedAfter)}";
	}

	public static async Task<string> TakeAsyncAndToListAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region migration_takeasync_after
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
		#region migration_collection_after
		List<long> ids = await connection
			.Command(IdsSql)
			.Map(Id)
			.ToListAsync(cancellationToken);
		#endregion

		return ids.Count;
	}

	public static async Task<string> MappedDictionaries(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region migration_mappeddictionaries_after
		// One DictionaryRow for each row, with the names of the result set shared by all of them;
		// ToDictionary() is a copy that the caller owns.
		List<DictionaryRow> rows = await connection
			.Command(IdsSql)
			.AsDictionaryRows()
			.ToListAsync(cancellationToken);
		IEnumerable<Dictionary<string, object?>> after = rows.Select(static row => row.ToDictionary());
		#endregion

		return Key(after);
	}

	public static async Task<string> ObjectArrays(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region migration_objectarrays_after
		// A value for each column, in order; a database NULL is null.
		List<object?[]> rows = await connection
			.Command(PostsSql)
			.AsObjectArrays()
			.ToListAsync(cancellationToken);
		#endregion

		return string.Join(";", rows.Select(static row => string.Join(",", row)));
	}

	public static async Task<string> DequeueAs(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region migration_dequeueas_after
		List<Post> after = await connection
			.Command(PostsSql)
			.Map<Post>()
			.ToListAsync(cancellationToken);
		#endregion

		return Key(after);
	}
}
