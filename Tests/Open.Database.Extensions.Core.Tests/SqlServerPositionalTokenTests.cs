extern alias mssql;

using Microsoft.Data.SqlClient;
using SqlCmd = mssql::Open.Database.Extensions.SqlCommandExtensions;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The SQL Server package's ExecuteReaderAsync forms put the token straight after the handler or transform
// (the token is positional after any prefix of the optional parameters), as Core's do.
// A method group converts to a delegate of exactly one rung, so each line fails to compile if the rung is missing.
[ExcludeFromCodeCoverage]
public static class SqlServerPositionalTokenTests
{
	[Fact]
	public static void EveryExecuteReaderAsyncForm_HasATokenRungAfterItsHandler()
	{
		Func<SqlCommand, Action<SqlDataReader>, CancellationToken, ValueTask> action = SqlCmd.ExecuteReaderAsync;
		Func<SqlCommand, Func<SqlDataReader, ValueTask>, CancellationToken, ValueTask> asyncAction = SqlCmd.ExecuteReaderAsync;
		Func<SqlCommand, Func<SqlDataReader, int>, CancellationToken, ValueTask<int>> transform = SqlCmd.ExecuteReaderAsync<int>;
		Func<SqlCommand, Func<SqlDataReader, ValueTask<int>>, CancellationToken, ValueTask<int>> asyncTransform = SqlCmd.ExecuteReaderAsync<int>;

		Assert.NotNull(action);
		Assert.NotNull(asyncAction);
		Assert.NotNull(transform);
		Assert.NotNull(asyncTransform);
	}

	[Fact]
	public static void TheExpressiveCommandsStateTransform_HasATokenRungAfterItsTransform()
	{
		var rungs = typeof(mssql::Open.Database.Extensions.ExpressiveSqlCommand).GetMethods()
			.Where(static m => m.Name == "ExecuteReaderAsync" && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 2)
			.Select(static m => m.GetParameters().Select(static p => p.ParameterType.Name).ToArray())
			.ToList();

		Assert.Contains(rungs, static p => p is ["TState", "Func`3", "CancellationToken"]);
		Assert.Contains(rungs, static p => p is ["TState", "Func`3", "CommandBehavior", "CancellationToken"]);
	}

	[Fact]
	public static void TheBehaviorRung_StillTakesTheBehaviorBeforeTheToken()
	{
		Func<SqlCommand, Action<SqlDataReader>, System.Data.CommandBehavior, CancellationToken, ValueTask> action = SqlCmd.ExecuteReaderAsync;
		Func<SqlCommand, Func<SqlDataReader, int>, System.Data.CommandBehavior, CancellationToken, ValueTask<int>> transform = SqlCmd.ExecuteReaderAsync<int>;

		Assert.NotNull(action);
		Assert.NotNull(transform);
	}
}
