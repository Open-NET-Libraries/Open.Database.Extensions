using System.Collections.Immutable;
using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;
using Open.Database.Extensions.Core;

namespace Samples;

/// <summary>
/// The "before" is the call that is obsolete in 11.0 (removed in 12), inside a pragma that hides its CS0618 warning;
/// the "after" is the call to type instead. Program.cs checks that both give the same result.
/// </summary>
public static class Deprecated
{
	const string PostsSql = "select * from posts order by id";
	const string IdsSql = "select id from posts order by id";

	public static string Key(IEnumerable<Post> posts) => string.Join(",", posts.Select(post => $"{post.Id}:{post.Title}"));

	public static string Key(IEnumerable<long> ids) => string.Join(",", ids);

	public static string Key(IEnumerable<Dictionary<string, object?>> rows)
		=> string.Join(";", rows.Select(row => string.Join(",", row.OrderBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => $"{pair.Key}={pair.Value}"))));

	// The selector the old calls and Map both take.
	static long Id(IDataRecord record) => record.GetInt64(0);

#pragma warning disable CS0618 // shown in the docs as the call to replace
	public static (string Before, string After) Results(SqliteConnection connection)
	{
		#region deprecated_results_before
		List<Post> before = connection.Command(PostsSql).Results<Post>().ToList();
		#endregion

		#region deprecated_results_after
		List<Post> after = connection.Command(PostsSql).Map<Post>().ToList();
		#endregion

		return (Key(before), Key(after));
	}

	public static async Task<(string Before, string After)> ResultsAsync(SqliteConnection connection)
	{
		#region deprecated_resultsasync_before
		List<Post> before = (await connection.Command(PostsSql).ResultsAsync<Post>()).ToList();
		#endregion

		#region deprecated_resultsasync_after
		List<Post> after = await connection.Command(PostsSql).Map<Post>().ToListAsync();
		#endregion

		return (Key(before), Key(after));
	}

	public static (string Before, string After) ResultsFromDataTable(SqliteConnection connection)
	{
		static SqliteCommand Create(SqliteConnection connection)
		{
			SqliteCommand command = connection.CreateCommand();
			command.CommandText = PostsSql;
			return command;
		}

		using SqliteCommand oldCommand = Create(connection);
		#region deprecated_resultsfromdatatable_before
		List<Post> before = oldCommand.ResultsFromDataTable<Post>().ToList();
		#endregion

		using SqliteCommand newCommand = Create(connection);
		#region deprecated_resultsfromdatatable_after
		// Reading the map first keeps the old behavior of failing before the command runs for a type that cannot be mapped.
		RowMap<Post> map = RowMap<Post>.Default;
		using var table = newCommand.ToDataTable();
		List<Post> after = table.To<Post>(map, true).ToList();
		#endregion

		return (Key(before), Key(after));
	}

	public static (string Before, string After) SelectorForms(SqliteConnection connection)
	{
		IExecuteReader command = connection.Command(IdsSql);

		#region deprecated_selector_before
		long? first = command.FirstOrDefault(Id);
		List<long> taken = command.Take(2, Id);
		List<long> skipped = command.Skip(3, Id);
		List<long> sliced = command.SkipThenTake(1, 2, Id);
		#endregion

		#region deprecated_selector_after
		long? firstAfter = command.Map(Id).FirstOrDefault();
		List<long> takenAfter = command.Map(Id).Take(2).ToList();
		List<long> skippedAfter = command.Map(Id).Skip(3).ToList();
		List<long> slicedAfter = command.Map(Id).Skip(1).Take(2).ToList();
		#endregion

		return (
			$"{first}|{Key(taken)}|{Key(skipped)}|{Key(sliced)}",
			$"{firstAfter}|{Key(takenAfter)}|{Key(skippedAfter)}|{Key(slicedAfter)}");
	}

	/// <summary>A selector that throws on the first row: the old Skip ran it on the skipped row, Map does not.</summary>
	public static (bool OldThrew, bool NewThrew) SkippedRowSelector(SqliteConnection connection)
	{
		static long ThrowsOnTheFirstRow(IDataRecord record) => record.GetInt64(0) == 1 ? throw new InvalidOperationException() : record.GetInt64(0);
		IExecuteReader command = connection.Command(IdsSql);

		bool oldThrew;
		try
		{
			command.Skip(1, ThrowsOnTheFirstRow);
			oldThrew = false;
		}
		catch (InvalidOperationException)
		{
			oldThrew = true;
		}

		bool newThrew;
		try
		{
			command.Map(ThrowsOnTheFirstRow).Skip(1).ToList();
			newThrew = false;
		}
		catch (InvalidOperationException)
		{
			newThrew = true;
		}

		return (oldThrew, newThrew);
	}

	/// <summary>A command that cannot run (a missing table) shows it: Take(0) did not run it, and Map(...).Take(0) does.</summary>
	public static (bool OldFailed, bool NewFailed) TakeOfZero(SqliteConnection connection)
	{
		IExecuteReader command = connection.Command("select id from no_such_table");

		bool oldFailed;
		try
		{
			command.Take(0, Id);
			oldFailed = false;
		}
		catch (SqliteException)
		{
			oldFailed = true;
		}

		bool newFailed;
		try
		{
			command.Map(Id).Take(0).ToList();
			newFailed = false;
		}
		catch (SqliteException)
		{
			newFailed = true;
		}

		return (oldFailed, newFailed);
	}
	public static async Task<(string Before, string After)> TakeAsyncAndToListAsync(SqliteConnection connection)
	{
		#region deprecated_takeasync_before
		IList<long> taken = await connection.Command(IdsSql).TakeAsync(Id, 2);
		List<long> all = await connection.Command(IdsSql).ToListAsync(Id);
		#endregion

		#region deprecated_takeasync_after
		List<long> takenAfter = await connection.Command(IdsSql).Map(Id).Take(2).ToListAsync();
		List<long> allAfter = await connection.Command(IdsSql).Map(Id).ToListAsync();
		#endregion

		return ($"{Key(taken)}|{Key(all)}", $"{Key(takenAfter)}|{Key(allAfter)}");
	}

	/// <summary>Nothing in the library returns a QueryResultCollection, so there is no runnable before and after.</summary>
	public static int Collection(SqliteConnection connection)
	{
		#region deprecated_collection_before
		var rows = new QueryResultCollection<long>(ImmutableArray.Create(0), ImmutableArray.Create("id"), connection.Command(IdsSql).Map(Id).ToList());
		#endregion

		#region deprecated_collection_after
		List<long> ids = connection.Command(IdsSql).Map(Id).ToList();
		#endregion

		return rows.Count() + ids.Count;
	}

	public static async Task<(string Before, string After, string BeforeAsync, string AfterAsync)> MappedDictionaries(SqliteConnection connection)
	{
		// Nothing returns the QueryResult<IEnumerable<object?[]>> that AsMappedDictionaries takes, so the sample builds one from a Retrieve.
		QueryResult<Queue<object[]>> retrieved = connection.Command(IdsSql).Retrieve();
		var source = new QueryResult<IEnumerable<object?[]>>(retrieved.Ordinals, retrieved.Names, retrieved.Result.ToArray());

		#region deprecated_mappeddictionaries_before
		IEnumerable<Dictionary<string, object?>> before = source.AsMappedDictionaries();
		#endregion

		#region deprecated_mappeddictionaries_after
		IEnumerable<Dictionary<string, object?>> after = connection.Command(IdsSql).Retrieve().DequeueAsMappedDictionaries();
		IEnumerable<Dictionary<string, object?>> afterAsync = (await connection.Command(IdsSql).RetrieveAsync()).DequeueAsMappedDictionaries();
		#endregion

		var beforeAsync = await Task.FromResult(new QueryResult<IEnumerable<object?[]>>(retrieved.Ordinals, retrieved.Names, source.Result)).AsMappedDictionaries();
		return (Key(before), Key(after), Key(beforeAsync), Key(afterAsync));
	}

	public static (string Before, string After) DequeueAs(SqliteConnection connection)
	{
		#region deprecated_dequeueas_before
		List<Post> before = connection.Command(PostsSql).Retrieve().DequeueAs<Post>().ToList();
		#endregion

		#region deprecated_dequeueas_after
		List<Post> after = connection.Command(PostsSql).Map<Post>().ToList();
		#endregion

		return (Key(before), Key(after));
	}
#pragma warning restore CS0618
}
