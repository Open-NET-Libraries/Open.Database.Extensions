using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

static class Program
{
	static readonly DbNonQuery Create = DbNonQuery.Text("create table Posts (Id integer primary key, Title text, AuthorId integer)");
	static readonly DbNonQuery Insert = DbNonQuery.Text("insert into Posts (Id, Title, AuthorId) values (@id, @title, @authorId)");
	static readonly DbNonQuery SetTitle = DbNonQuery.Text("update Posts set Title = @title where Id = @id").SetTimeout(15);
	static readonly ScalarDbQuery<int> PostCount = DbQuery.Text("select count(*) from Posts where AuthorId = @authorId").Scalar<int>();
	static readonly ScalarDbQuery<long?> MaxId = DbQuery.Text("select max(Id) from Posts where AuthorId = @authorId").SetTimeout(30).Scalar<long?>();
	static readonly ScalarDbQuery<int> NoRow = DbQuery.Text("select 1 where 0").Scalar<int>();
	static readonly ScalarDbQuery<string?> TitleOf = DbQuery.Text("select Title from Posts where Id = @id").Scalar<string?>();

	static int failures;
	static void Check(bool ok, string what) { Console.WriteLine((ok ? "PASS " : "FAIL ") + what); if (!ok) failures++; }

	static async Task<int> Main()
	{
		var ct = CancellationToken.None;
		const string cs = "Data Source=shimtest;Mode=Memory;Cache=Shared";
		using var keepAlive = new SqliteConnection(cs);
		keepAlive.Open();
		var factory = new DbConnectionFactory<SqliteConnection>(() => new SqliteConnection(cs));

		await Create.Using(keepAlive).ExecuteNonQueryAsync(ct);

		int id = 1; string title = "First"; int authorId = 7;
		int rows = await Insert.Using(factory).AddParam(id).AddParam(title).AddParam(authorId).ExecuteNonQueryAsync(ct);
		Check(rows == 1, "non-query via factory, derived param names: rows=" + rows);

		rows = await Insert.Using(keepAlive).AddParams(new { id = 2, title = "Second", authorId }).ExecuteNonQueryAsync(ct);
		Check(rows == 1, "non-query via connection, AddParams object: rows=" + rows);

		rows = await SetTitle.Using(factory).AddParam("@title", "Renamed").AddParam("@id", 1).ExecuteNonQueryAsync(ct);
		Check(rows == 1, "non-query explicit names + SetTimeout: rows=" + rows);

		int count = await PostCount.Using(factory).AddParam(authorId).ExecuteScalarAsync(ct);
		Check(count == 2, "scalar<int> count=" + count);

		long? max = await MaxId.Using(keepAlive).AddParam(authorId).ExecuteScalarAsync(ct);
		Check(max == 2, "scalar<long?> max=" + max);

		int otherAuthor = 99;
		max = await MaxId.Using(factory).AddParam("@authorId", otherAuthor).ExecuteScalarAsync(ct);
		Check(max is null, "scalar<long?> NULL -> null");

		string? t = await TitleOf.Using(factory).AddParam("@id", 1).ExecuteScalarAsync(ct);
		Check(t == "Renamed", "scalar<string?> title=" + t);

		try { await NoRow.Using(factory).ExecuteScalarAsync(ct); Check(false, "no row into int throws"); }
		catch (ValueMappingException) { Check(true, "no row into int throws ValueMappingException"); }

		using (var tx = keepAlive.BeginTransaction())
		{
			rows = await Insert.Using(tx).AddParam("@id", 3).AddParam("@title", "Tx").AddParam("@authorId", authorId).ExecuteNonQueryAsync(ct);
			Check(rows == 1, "non-query via transaction");
			tx.Rollback();
		}
		count = await PostCount.Using(keepAlive).AddParam(authorId).ExecuteScalarAsync(ct);
		Check(count == 2, "rollback undid the transaction insert: count=" + count);

		Console.WriteLine(failures == 0 ? "ALL PASS" : failures + " FAILED");
		return failures;
	}
}
