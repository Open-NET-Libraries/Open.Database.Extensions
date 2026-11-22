using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The definition-first query on a real server: a stored procedure called with
// derived names (@author_id found for authorId, @p_views for views) and with
// the typed form, a transaction, a factory, and a typed NULL.
// Skipped without LocalDB (see LocalDbFixture).
[ExcludeFromCodeCoverage]
[Collection("LocalDB DDL")]
public static class DbQueryLocalDbTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	sealed record Row(int AuthorId, int Views);

	readonly record struct AuthorViews(int AuthorId, int Views);

	static LocalDbFixture.Scratch CreateProcedure()
		=> LocalDbFixture.CreateProcedure(
			"@author_id int, @p_views int",
			"ode_ds7");

	// CreateProcedure's body selects 1: replace it with one that echoes the values.
	static string Echo(LocalDbFixture.Scratch procedure)
	{
		LocalDbFixture.Execute(
			$"alter procedure dbo.[{procedure.Name}] @author_id int, @p_views int as select @author_id as AuthorId, @p_views as Views");
		return "dbo." + procedure.Name;
	}

	[Fact]
	public static async Task AProcedure_IsCalled_WithNamesFromTheVariables()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = CreateProcedure();
		var query = DbQuery.Procedure(Echo(procedure));
		await using SqlConnection connection = LocalDbFixture.Create();
		const int authorId = 7;
		const int views = 50;

		List<Row> rows = await query.Using(connection)
			.AddParam(authorId)
			.AddParam(views)
			.Map<Row>()
			.ToListAsync(Token);

		Assert.Equal([new Row(7, 50)], rows);
		Assert.Equal(System.Data.ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static async Task AProcedure_IsCalled_WithTheTypedForm()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = CreateProcedure();
		DbQuery<AuthorViews> query
			= DbQuery.Procedure(Echo(procedure)).Params<AuthorViews>();
		await using SqlConnection connection = LocalDbFixture.Create();
		const int authorId = 8;
		const int views = 60;

		List<Row> rows = await query.Using(connection)
			.AddParams(new(AuthorId: authorId, Views: views))
			.Map<Row>()
			.ToListAsync(Token);

		Assert.Equal([new Row(8, 60)], rows);
	}

	readonly record struct WithExtra(int AuthorId, int Views, int Unused);

	// The second call finds the names remembered (no schema lookup), and a member
	// the procedure does not declare is not sent.
	[Fact]
	public static async Task AProcedure_IsCalledAgain_FromTheRememberedNames()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = CreateProcedure();
		DbQuery<WithExtra> query
			= DbQuery.Procedure(Echo(procedure)).Params<WithExtra>();
		await using SqlConnection connection = LocalDbFixture.Create();

		for (int i = 0; i < 3; i++)
		{
			List<Row> rows = await query.Using(connection)
				.AddParams(new(AuthorId: 7, Views: 50 + i, Unused: 9))
				.Map<Row>()
				.ToListAsync(Token);

			Assert.Equal([new Row(7, 50 + i)], rows);
		}
	}

	[Fact]
	public static async Task ATransaction_RunsTheQueryInIt()
	{
		LocalDbFixture.SkipUnlessAvailable();
		await using SqlConnection connection = LocalDbFixture.Create();
		await connection.OpenAsync(Token);
		await using System.Data.Common.DbTransaction transaction
			= await connection.BeginTransactionAsync(Token);

		List<int> open = await DbQuery
			.Text("select @@trancount").Using(transaction)
			.Map<int>()
			.ToListAsync(Token);

		Assert.Equal([1], open);
		Assert.Equal(System.Data.ConnectionState.Open, connection.State);
	}

	[Fact]
	public static async Task AFactory_MakesAConnectionPerRun()
	{
		LocalDbFixture.SkipUnlessAvailable();
		var factory = new DbConnectionFactory<SqlConnection>(LocalDbFixture.Create);
		DbQueryUsing query = DbQuery
			.Text("select @@spid").Using(factory);

		List<int> first = await query.Map<int>().ToListAsync(Token);
		List<int> second = await query.Map<int>().ToListAsync(Token);

		Assert.Single(first);
		Assert.Single(second);
	}

	[Fact]
	public static async Task ANullOfATypedArgument_IsSentAsADatabaseNull()
	{
		LocalDbFixture.SkipUnlessAvailable();
		await using SqlConnection connection = LocalDbFixture.Create();
		int? none = null;

		List<int> flag = await DbQuery
			.Text("select case when @none is null then 1 else 0 end").Using(connection)
			.AddParam(none)
			.Map<int>()
			.ToListAsync(Token);

		Assert.Equal([1], flag);
	}
}
