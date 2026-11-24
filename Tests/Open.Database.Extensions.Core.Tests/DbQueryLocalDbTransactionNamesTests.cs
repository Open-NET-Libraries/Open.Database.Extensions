using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Tests;

#nullable enable

// D2 part two: a DbQuery reads a stored procedure's parameter names through the ADO.NET schema collection (SqlConnection.GetSchema), which runs its own
// command on the connection. With a pending local transaction (the source is an open DbTransaction, with a write done in it) the first call, with a cold name
// cache, must still find the names and run: it must not throw (a command on a connection in a pending transaction needs the transaction) and must not block.
// Skipped without LocalDB (see LocalDbFixture).
[ExcludeFromCodeCoverage]
[Collection("LocalDB DDL")]
public static class DbQueryLocalDbTransactionNamesTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	sealed record Row(int AuthorId, int Views);

	[Fact]
	public static async Task AProcedureWithNamedParams_InAPendingTransaction_ColdNameCache_FindsItsNamesAndRuns()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = LocalDbFixture.CreateProcedure("@author_id int, @p_views int", "ode_d2");
		LocalDbFixture.Execute(
			$"alter procedure dbo.[{procedure.Name}] @author_id int, @p_views int as select @author_id as AuthorId, @p_views as Views");

		await using SqlConnection connection = LocalDbFixture.Create();
		await connection.OpenAsync(Token);
		await using System.Data.Common.DbTransaction transaction = await connection.BeginTransactionAsync(Token);

		// A write in the same transaction first, so the transaction holds locks and is a pending local transaction.
		await using (SqlCommand write = connection.CreateCommand())
		{
			write.Transaction = (SqlTransaction)transaction;
			write.CommandText = $"create table dbo.[ode_d2_{Guid.NewGuid():N}] (id int)";
			await write.ExecuteNonQueryAsync(Token);
		}

		const int authorId = 7;
		const int views = 50;

		// The first call: nothing is remembered for this procedure on this connection. Bounded, so a block fails the test instead of hanging it.
		Task<List<Row>> run = DbQuery.Procedure("dbo." + procedure.Name)
			.Using(transaction)
			.AddParam(authorId)
			.AddParam(views)
			.Map<Row>()
			.ToListAsync(Token)
			.AsTask();
		List<Row> rows = await run.WaitAsync(TimeSpan.FromSeconds(30), Token);

		Assert.Equal([new Row(7, 50)], rows);
	}
}
