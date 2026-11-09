using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class SeveralResultSets
{
	// Two result sets of different shapes in one command: the posts, then the people.
	const string TwoSets = "select * from posts order by id; select person_id, first_name, last_name from people order by person_id";

	public static (int Posts, int People) Sync(IDbConnectionFactory<SqliteConnection> factory)
	{
		int posts = 0, people = 0;

		#region result_sets_sync
		// The connection is opened before the handler runs and closed after it returns.
		factory
			.Command(TwoSets)
			.ExecuteReader(reader =>
			{
				// Results<T>() is lazy, so ToList() reads the whole set while the reader is still on it.
				List<Post> postList = reader.Results<Post>().ToList();
				posts = postList.Count;

				// NextResult moves to the next set. It returns false when there is none.
				if (!reader.NextResult()) return;

				List<Person> personList = reader.Results<Person>().ToList();
				people = personList.Count;
			});
		#endregion

		return (posts, people);
	}

	public static async Task<(int Posts, int People)> Async(IDbConnectionFactory<SqliteConnection> factory, CancellationToken cancellationToken)
	{
		int posts = 0, people = 0;

		#region result_sets_async
		await factory
			.Command(TwoSets)
			.ExecuteReaderAsync(async (DbDataReader reader) =>
			{
				// ResultsBufferedAsync reads the whole set during the call, so there is nothing lazy to forget.
				IEnumerable<Post> postList = await reader.ResultsBufferedAsync<Post>(cancellationToken: cancellationToken);
				posts = postList.Count();

				if (!await reader.NextResultAsync(cancellationToken)) return;

				IEnumerable<Person> personList = await reader.ResultsBufferedAsync<Person>(cancellationToken: cancellationToken);
				people = personList.Count();
			});
		#endregion

		return (posts, people);
	}

	public static (int Posts, int People) OwnConnection(IDbConnectionFactory<SqliteConnection> factory)
	{
		#region result_sets_own_connection
		// You set up the connection, the command and the reader. The mapping calls are the same.
		using SqliteConnection connection = factory.Create();
		connection.Open();

		using SqliteCommand command = connection.CreateCommand();
		command.CommandText = TwoSets;

		using SqliteDataReader reader = command.ExecuteReader();

		List<Post> posts = reader.Results<Post>().ToList();
		reader.NextResult();
		List<Person> people = reader.Results<Person>().ToList();
		#endregion

		return (posts.Count, people.Count);
	}

	public static List<int> RowsPerSet(IDbConnectionFactory<SqliteConnection> factory, string sql)
	{
		List<int> rowCounts = [];

		#region result_sets_loop
		factory
			.Command(sql)
			.ExecuteReader(reader =>
			{
				// do-while: a reader starts on its first set, so use it before the first NextResult.
				do
				{
					// AsEnumerable() gives each row as an object[] of the values, with no type to write.
					rowCounts.Add(reader.AsEnumerable().Count());
				}
				while (reader.NextResult());
			});
		#endregion

		return rowCounts;
	}

	public static List<string> ColumnsPerSet(IDbConnectionFactory<SqliteConnection> factory, string sql)
	{
		List<string> columns = [];

		#region result_sets_shape
		factory
			.Command(sql)
			.ExecuteReader(reader =>
			{
				do
				{
					// The reader describes the set it is on.
					string[] names = new string[reader.FieldCount];
					for (int i = 0; i < names.Length; i++)
						names[i] = reader.GetName(i);

					columns.Add(string.Join(", ", names));
				}
				while (reader.NextResult());
			});
		#endregion

		return columns;
	}

	public static List<DataTable> AllSets(IDbConnectionFactory<SqliteConnection> factory)
	{
		#region result_sets_tables
		// One DataTable per result set, whatever the shapes are.
		List<DataTable> tables = factory
			.Command(TwoSets)
			.ExecuteReader(reader => reader.ToDataTables());
		#endregion

		return tables;
	}

	public static (int Posts, int People) Check(IDbConnectionFactory<SqliteConnection> factory)
	{
		int posts = 0, people = 0;

		#region result_sets_error_check
		factory
			.Command(TwoSets)
			.ExecuteReader(reader =>
			{
				posts = reader.Results<Post>().ToList().Count;
				reader.NextResult();

				people = reader.Results<Person>().ToList().Count;

				// A call past the last set. On SQL Server this is where an error raised after the last row (RAISERROR) reaches you.
				// It returns false: there is no third set.
				reader.NextResult();
			});
		#endregion

		return (posts, people);
	}

	public static List<Post> ForgotToList(IDbConnectionFactory<SqliteConnection> factory)
	{
		List<Post> posts = [];

		#region result_sets_lazy_mistake
		factory
			.Command(TwoSets)
			.ExecuteReader(reader =>
			{
				// Wrong: nothing has been read yet, because Results<T>() is lazy.
				IEnumerable<Post> postRows = reader.Results<Post>();

				reader.NextResult(); // The reader moves on to the people.

				posts = postRows.ToList(); // The people are read as if they were posts.
			});
		#endregion

		return posts;
	}
}
