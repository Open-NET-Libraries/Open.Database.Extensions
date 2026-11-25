using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Tests;

#nullable enable

// DbQuery.Scalar<T>() and DbNonQuery on a real server: stored procedures called
// with derived names (@author_id found for authorId, @p_views for views), with
// the typed form and through a factory and a transaction.
// Skipped without LocalDB (see LocalDbFixture).
[ExcludeFromCodeCoverage]
[Collection("LocalDB DDL")]
public static class ScalarAndNonQueryLocalDbTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	readonly record struct AuthorViews(int AuthorId, int Views);

	static LocalDbFixture.Scratch Procedure(string body)
	{
		LocalDbFixture.Scratch procedure = LocalDbFixture.CreateProcedure(
			"@author_id int, @p_views int",
			"ode_nq");
		LocalDbFixture.Execute(
			$"alter procedure dbo.[{procedure.Name}] @author_id int, @p_views int as {body}");
		return procedure;
	}

	[Fact]
	public static async Task AProcedureWrite_FindsTheNamesFromTheVariables()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch table = LocalDbFixture.CreateTable("ode_nq_t");
		using LocalDbFixture.Scratch procedure = Procedure(
			$"insert into dbo.[{table.Name}] values (@author_id), (@p_views)");
		DbNonQuery write = DbNonQuery.Procedure("dbo." + procedure.Name);
		var factory = new DbConnectionFactory<SqlConnection>(LocalDbFixture.Create);
		const int authorId = 7;
		const int views = 50;

		// The first call reads the names; the second finds them remembered.
		for (int i = 0; i < 2; i++)
		{
			Assert.Equal(2, await write.Using(factory).AddParam(authorId).AddParam(views).ExecuteNonQueryAsync(Token));
		}

		Assert.Equal(2, await write.AddParams(new AuthorViews(8, 60)).Using(factory).ExecuteNonQueryAsync(Token));
		Assert.Equal(2, await write.Params<AuthorViews>().Using(factory).AddParams(new(9, 70)).ExecuteNonQueryAsync(Token));
		Assert.Equal(8, LocalDbFixture.Count(table.Name));
	}

	[Fact]
	public static async Task AProcedureScalar_FindsTheNamesFromTheVariables()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = Procedure("set nocount on; select @author_id * 1000 + @p_views");
		ScalarDbQuery<int> read = DbQuery.Procedure("dbo." + procedure.Name).Scalar<int>();
		await using SqlConnection connection = LocalDbFixture.Create();
		const int authorId = 7;
		const int views = 50;

		Assert.Equal(7050, await read.Using(connection).AddParam(authorId).AddParam(views).ExecuteScalarAsync(Token));
		Assert.Equal(7050, await read.AddParam(authorId).AddParam(views).Using(connection).ExecuteScalarAsync(Token));
		Assert.Equal(8060, await read.Params<AuthorViews>().Using(connection).AddParams(new(8, 60)).ExecuteScalarAsync(Token));
		Assert.Equal(System.Data.ConnectionState.Closed, connection.State);

		// A write of a procedure that sets NOCOUNT ON reports no count.
		DbNonQuery write = DbNonQuery.Procedure("dbo." + procedure.Name);
		Assert.Equal(-1, await write.Using(connection).AddParam(authorId).AddParam(views).ExecuteNonQueryAsync(Token));
	}

	[Fact]
	public static async Task AWrite_RunsInTheTransaction()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch table = LocalDbFixture.CreateTable("ode_nq_tx");
		DbNonQuery insert = DbNonQuery.Text($"insert into dbo.[{table.Name}] values (@id)");
		ScalarDbQuery<int> count = DbQuery.Text($"select count(*) from dbo.[{table.Name}]").Scalar<int>();
		await using SqlConnection connection = LocalDbFixture.Create();
		await connection.OpenAsync(Token);

		await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync(Token))
		{
			Assert.Equal(1, await insert.Using(transaction).AddParam("id", 1).ExecuteNonQueryAsync(Token));
			Assert.Equal(1, await count.Using(transaction).ExecuteScalarAsync(Token));
			await transaction.RollbackAsync(Token);
		}

		Assert.Equal(System.Data.ConnectionState.Open, connection.State);
		Assert.Equal(0, await count.Using(connection).ExecuteScalarAsync(Token));
	}
}
