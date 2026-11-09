using System.Data.Common;
using Microsoft.Data.SqlClient;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The real provider: a failure in a statement after the first result set is raised by NextResult, so a Map terminal throws it, and .SingleResult() does not see it.
// Needs SQL Server LocalDB; the tests are skipped on a machine without it. Procedures are created in tempdb under a unique name and dropped.
[ExcludeFromCodeCoverage]
public static class MappedQueryLaterFailureLiveTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	const string ConnectionString = @"Server=(localdb)\MSSQLLocalDB;Database=tempdb;Integrated Security=true;Encrypt=false;TrustServerCertificate=true;Connect Timeout=30";

	const string LateFailure = "select 1 as Id; raiserror('late failure', 16, 1);";
	const string TrailingStatus = "select 1 as Id; select 7 as Status;";

	static async Task<string> Prepare(string body)
	{
		string name = "dbo.ode_single_result_" + Guid.NewGuid().ToString("N");
		try
		{
			await using var connection = new SqlConnection(ConnectionString);
			await connection.OpenAsync(Ct);
			await using SqlCommand create = connection.CreateCommand();
			create.CommandText = $"create procedure {name} as begin set nocount on; {body} end";
			await create.ExecuteNonQueryAsync(Ct);
		}
		catch (SqlException ex)
		{
			Assert.Skip("SQL Server LocalDB is not available: " + ex.Message);
		}

		return name;
	}

	static async Task Drop(string name)
	{
		await using var connection = new SqlConnection(ConnectionString);
		await connection.OpenAsync(Ct);
		await using SqlCommand drop = connection.CreateCommand();
		drop.CommandText = $"drop procedure {name}";
		await drop.ExecuteNonQueryAsync(Ct);
	}

	static ExpressiveDbCommand Command(CommandType type, string text)
		=> new(() => new SqlConnection(ConnectionString), type, text);

	static MappedQuery<int> Ids(ExpressiveDbCommand command) => command.Map(r => r.GetInt32(0));

	[Fact]
	public static async Task AStoredProcedureWithALateFailure_Throws_ThroughEveryTerminal_UnlessSingleResult()
	{
		string name = await Prepare(LateFailure);
		try
		{
			ExpressiveDbCommand command = Command(CommandType.StoredProcedure, name);

			Assert.Throws<SqlException>(() => Ids(command).ToList());
			Assert.Throws<SqlException>(() => Ids(command).FirstOrDefault());
			Assert.Throws<SqlException>(() => Ids(command).Read(items => items.Count()));
			await Assert.ThrowsAsync<SqlException>(async () => await Ids(command).ToListAsync(Ct));
			await Assert.ThrowsAsync<SqlException>(async () => await Ids(command).SingleOrDefaultAsync(Ct));

			Assert.Equal([1], Ids(command).SingleResult().ToList());
			Assert.Equal(1, Ids(command).SingleResult().FirstOrDefault());
			Assert.Equal(1, Assert.Single(await Ids(command).SingleResult().ToListAsync(Ct)));
			Assert.Equal(1, await Ids(command).SingleResult().SingleOrDefaultAsync(Ct));
		}
		finally
		{
			await Drop(name);
		}
	}

	[Fact]
	public static async Task ATextBatchWithALateFailure_Throws_UnlessSingleResult()
	{
		ExpressiveDbCommand command = Command(CommandType.Text, LateFailure);
		try
		{
			Assert.Throws<SqlException>(() => Ids(command).ToList());
		}
		catch (Exception ex) when (ex is not Xunit.Sdk.XunitException && IsUnavailable(ex))
		{
			Assert.Skip("SQL Server LocalDB is not available: " + ex.Message);
		}

		await Assert.ThrowsAsync<SqlException>(async () => await Ids(command).ToArrayAsync(Ct));
		Assert.Equal([1], Ids(command).SingleResult().ToList());
		Assert.Equal(1, Assert.Single(await Ids(command).SingleResult().ToArrayAsync(Ct)));
	}

	[Fact]
	public static async Task ATrailingStatusSet_DoesNotThrow_AndAddsNoRows()
	{
		string name = await Prepare(TrailingStatus);
		try
		{
			ExpressiveDbCommand command = Command(CommandType.StoredProcedure, name);

			Assert.Equal([1], Ids(command).ToList());
			Assert.Equal(1, Assert.Single(await Ids(command).ToListAsync(Ct)));
			Assert.Equal(1, Ids(command).SingleOrDefault());
			Assert.Equal([1], Ids(command).SingleResult().ToList());
		}
		finally
		{
			await Drop(name);
		}
	}

	static bool IsUnavailable(Exception ex) => ex is not SqlException { Number: 50000 } and (SqlException or InvalidOperationException);
}
