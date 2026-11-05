namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class EnsureOpenAsyncTests
{
	[Fact]
	public static async Task DbConnection_Closed_OpensAsyncAndReturnsPriorState()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed, dbConnection: true);

		ConnectionState prior = await probe.Db!.EnsureOpenAsync(TestContext.Current.CancellationToken);

		Assert.Equal(ConnectionState.Closed, prior);
		Assert.Equal("OpenAsync", probe.Log);
		Assert.Equal(ConnectionState.Open, probe.State);
	}

	[Fact]
	public static async Task DbConnection_Open_DoesNothing()
	{
		var probe = new ConnectionProbe(ConnectionState.Open, dbConnection: true);

		ConnectionState prior = await probe.Db!.EnsureOpenAsync(TestContext.Current.CancellationToken);

		Assert.Equal(ConnectionState.Open, prior);
		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static async Task DbConnection_Broken_ClosesThenOpensAndReportsBroken()
	{
		var probe = new ConnectionProbe(ConnectionState.Broken, dbConnection: true);

		ConnectionState prior = await probe.Db!.EnsureOpenAsync(TestContext.Current.CancellationToken);

		Assert.Equal(ConnectionState.Broken, prior);
		Assert.Equal("Close,OpenAsync", probe.Log);
	}

	[Fact]
	public static async Task DbConnection_AlreadyCancelled_ThrowsWithoutTouchingConnection()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed, dbConnection: true);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await probe.Db!.EnsureOpenAsync(cts.Token));

		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static async Task DbConnection_Null_Throws()
	{
		System.Data.Common.DbConnection connection = null!;

		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await connection.EnsureOpenAsync(TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task IDbConnection_NotDbConnection_FallsBackToSynchronousOpen()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);

		ConnectionState prior = await probe.Connection.EnsureOpenAsync(TestContext.Current.CancellationToken);

		Assert.Equal(ConnectionState.Closed, prior);
		Assert.Equal("Open", probe.Log);
	}

	[Fact]
	public static async Task IDbConnection_NotDbConnection_AlreadyCancelled_ThrowsWithoutOpening()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await probe.Connection.EnsureOpenAsync(cts.Token));

		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static async Task IDbConnection_ThatIsADbConnection_OpensAsynchronously()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed, dbConnection: true);
		IDbConnection connection = probe.Connection;

		await connection.EnsureOpenAsync(TestContext.Current.CancellationToken);

		// OpenAsync only; the synchronous Open() must not also be called.
		Assert.Equal("OpenAsync", probe.Log);
	}
}
