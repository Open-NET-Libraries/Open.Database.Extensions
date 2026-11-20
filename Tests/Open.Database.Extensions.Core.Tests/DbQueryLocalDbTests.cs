using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The definition-first query on a real server: a stored procedure called with
// derived names and with the typed form (SQL Server matches the names
// without regard to case), a transaction, a factory, and a typed NULL.
// Skipped without LocalDB (see LocalDbFixture).
[ExcludeFromCodeCoverage]
public static class DbQueryLocalDbTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	sealed record Row(int AuthorId, int Views);

	readonly record struct AuthorViews(int AuthorId, int Views);

	static LocalDbFixture.Scratch CreateProcedure()
		=> LocalDbFixture.CreateProcedure(
			"@authorId int, @views int",
			"ode_ds7");

	// CreateProcedure's body selects 1: replace it with one that echoes the values.
	static string Echo(LocalDbFixture.Scratch procedure)
	{
		LocalDbFixture.Execute(
			$"alter procedure dbo.[{procedure.Name}] @authorId int, @views int as select @authorId as AuthorId, @views as Views");
		return "dbo." + procedure.Name;
	}

	[Fact]
	public static async Task AProcedure_IsCalled_WithNamesFromTheVariables()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = CreateProcedure();
		DbQuery query = DbQuery.Procedure(Echo(procedure));
		await using SqlConnection connection = LocalDbFixture.Create();
		int authorId = 7;
		int views = 50;

		List<Row> rows = await query
			.Using(connection)
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
		int authorId = 8;
		int views = 60;

		List<Row> rows = await query
			.Using(connection)
			.AddParams(new(AuthorId: authorId, Views: views))
			.Map<Row>()
			.ToListAsync(Token);

		Assert.Equal([new Row(8, 60)], rows);
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
			.Text("select @@trancount")
			.Using(transaction)
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
			.Text("select @@spid")
			.Using(factory);

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
			.Text("select case when @none is null then 1 else 0 end")
			.Using(connection)
			.AddParam(none)
			.Map<int>()
			.ToListAsync(Token);

		Assert.Equal([1], flag);
	}
}
