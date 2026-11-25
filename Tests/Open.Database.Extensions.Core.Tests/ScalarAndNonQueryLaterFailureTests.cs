using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// What ExecuteScalarAsync and ExecuteNonQueryAsync do when a LATER statement of
// a batch fails: neither reads the results itself, so this is the provider's
// behavior, pinned for SQLite and SQL Server.
[ExcludeFromCodeCoverage]
[Collection("LocalDB DDL")]
public static class ScalarAndNonQueryLaterFailureTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	static async Task<string> Outcome(Func<ValueTask<int>> run)
	{
		try
		{
			return $"returned {await run()}";
		}
		catch (Exception ex) when (ex is SqliteException or SqlException)
		{
			return "threw";
		}
	}

	static Task<string> Scalar(DbConnection connection, string sql)
		=> Outcome(() => DbQuery.Text(sql).Scalar<int>().Using(connection).ExecuteScalarAsync(Token));

	static Task<string> Write(DbConnection connection, string sql)
		=> Outcome(() => DbNonQuery.Text(sql).Using(connection).ExecuteNonQueryAsync(Token));

	// The outcome of the terminal, and the rows of t after it (it starts with one, id 1).
	static async Task<(string Outcome, int Rows)> OnSqlite(
		Func<DbConnection, string, Task<string>> terminal,
		string sql)
	{
		await using var connection = new SqliteConnection("Data Source=:memory:");
		await connection.OpenAsync(Token);
		await DbNonQuery.Text("create table t (id integer primary key); insert into t values (1)")
			.Using(connection)
			.ExecuteNonQueryAsync(Token);

		string outcome = await terminal(connection, sql);
		int rows = await DbQuery.Text("select count(*) from t")
			.Scalar<int>()
			.Using(connection)
			.ExecuteScalarAsync(Token);
		return (outcome, rows);
	}

	// Microsoft.Data.Sqlite: after a statement that returns rows, a later failure
	// is not seen by either terminal.
	[Theory]
	[InlineData("select 7; insert into t values (1)", "returned 7", 1, "returned -1", 1)]
	[InlineData("select 7; select abs(-9223372036854775808)", "returned 7", 1, "returned -1", 1)]
	[InlineData("select 7; select * from no_such_table", "returned 7", 1, "returned -1", 1)]
	// A later statement that succeeds does run.
	[InlineData("select 7; insert into t values (2)", "returned 7", 2, "returned 1", 2)]
	// A failure after a statement that returns no rows is thrown; the earlier write stays.
	[InlineData("insert into t values (2); insert into t values (1)", "threw", 2, "threw", 2)]
	public static async Task OnSqlite_ALaterStatement(
		string sql,
		string scalar,
		int rowsAfterScalar,
		string write,
		int rowsAfterWrite)
	{
		Assert.Equal((scalar, rowsAfterScalar), await OnSqlite(Scalar, sql));
		Assert.Equal((write, rowsAfterWrite), await OnSqlite(Write, sql));
	}

	// SQL Server: both terminals throw.
	[Theory]
	[InlineData("select 7; raiserror('late failure', 16, 1);")]
	[InlineData("select 7; select 1/0;")]
	[InlineData("select 7; throw 50000, 'late failure', 1;")]
	[InlineData("select 7 where 1 = 0; raiserror('late failure', 16, 1);")]
	[InlineData("set nocount on; select 7; select 7; raiserror('late failure', 16, 1);")]
	public static async Task OnSqlServer_ALaterStatementThatFails_Throws(string sql)
	{
		LocalDbFixture.SkipUnlessAvailable();
		await using SqlConnection connection = LocalDbFixture.Create();

		Assert.Equal("threw", await Scalar(connection, sql));
		Assert.Equal("threw", await Write(connection, sql));
	}
}
