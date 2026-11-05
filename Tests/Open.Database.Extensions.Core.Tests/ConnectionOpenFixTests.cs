using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

[ExcludeFromCodeCoverage]
public static class ConnectionOpenFixTests
{
	// OpenAsync(IDbConnection) used to fall through from the DbConnection branch to a second, synchronous Open().
	[Fact]
	public static async Task IDbConnection_OpenAsync_OnADbConnection_OpensExactlyOnce()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed, dbConnection: true);

		await probe.Connection.OpenAsync(cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal("OpenAsync", probe.Log);
	}

	[Fact]
	public static async Task IDbConnection_OpenAsync_OnANonDbConnection_OpensSynchronouslyOnce()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);

		await probe.Connection.OpenAsync(cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal("Open", probe.Log);
	}

	// ConnectionState.Closed is zero, so the old HasFlag(Closed) test was always true and the cleanup never ran:
	// a connection opened just as cancellation arrived was left open while the caller got an exception.
	[Fact]
	public static async Task EnsureOpenAsync_CancelledWhileOpening_ClosesTheConnectionItOpenedAndThrows()
	{
		using var cts = new CancellationTokenSource();
		var calls = new List<string>();
		ConnectionState state = ConnectionState.Closed;

		DbConnection db = Substitute.For<DbConnection>();
		db.State.Returns(_ => state);
		db.OpenAsync(Arg.Any<CancellationToken>()).Returns(_ =>
		{
			calls.Add("OpenAsync");
			state = ConnectionState.Open;
			cts.Cancel(); // The open itself completes; cancellation arrived at the same time.
			return Task.CompletedTask;
		});
		db.When(c => c.Close()).Do(_ =>
		{
			calls.Add("Close");
			state = ConnectionState.Closed;
		});

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await db.EnsureOpenAsync(cts.Token));

		Assert.Equal("OpenAsync,Close", string.Join(",", calls));
		Assert.Equal(ConnectionState.Closed, state);
	}

	[Fact]
	public static async Task EnsureOpenAsync_NotCancelled_LeavesTheConnectionOpen()
	{
		var calls = new List<string>();
		ConnectionState state = ConnectionState.Closed;

		DbConnection db = Substitute.For<DbConnection>();
		db.State.Returns(_ => state);
		db.OpenAsync(Arg.Any<CancellationToken>()).Returns(_ =>
		{
			calls.Add("OpenAsync");
			state = ConnectionState.Open;
			return Task.CompletedTask;
		});
		db.When(c => c.Close()).Do(_ => calls.Add("Close"));

		ConnectionState prior = await db.EnsureOpenAsync(TestContext.Current.CancellationToken);

		Assert.Equal(ConnectionState.Closed, prior);
		Assert.Equal("OpenAsync", string.Join(",", calls));
		Assert.Equal(ConnectionState.Open, state);
	}
}
