using System.Threading.Channels;

namespace Open.Database.Extensions.Tests;

// ToChannel(IDbCommand, target, complete, transform) opens a closed connection itself. When the provider fails to execute there is no reader, so nothing closes
// the connection through CommandBehavior.CloseConnection: the call has to close what it opened (and never close one the caller opened).
[ExcludeFromCodeCoverage]
public static class ToChannelExecuteFailureTests
{
	[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors", Justification = "Test Only")]
	sealed class ProviderFailed : Exception;

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static async Task ProviderFails_ClosesTheConnectionTheCallOpened(bool complete)
	{
		var tally = new DbTally(new() { ExecuteFailure = new ProviderFailed() });
		TrackedConnection connection = tally.NewConnection();
		IDbCommand command = connection.CreateCommand();
		var channel = System.Threading.Channels.Channel.CreateUnbounded<int>();

		await Assert.ThrowsAsync<ProviderFailed>(async () => await command.ToChannel(channel.Writer, complete, r => r.GetInt32(0), TestContext.Current.CancellationToken));

		Assert.Equal(1, connection.Opens);
		Assert.Equal(1, connection.Closes);
		Assert.Equal(0, connection.Disposes);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Empty(tally.Readers);
	}

	[Fact]
	public static async Task ProviderFails_LeavesAConnectionTheCallerOpenedAsItWas()
	{
		var tally = new DbTally(new() { ExecuteFailure = new ProviderFailed() });
		TrackedConnection connection = tally.NewOpenConnection();
		IDbCommand command = connection.CreateCommand();
		var channel = System.Threading.Channels.Channel.CreateUnbounded<int>();

		await Assert.ThrowsAsync<ProviderFailed>(async () => await command.ToChannel(channel.Writer, false, r => r.GetInt32(0), TestContext.Current.CancellationToken));

		Assert.Equal(0, connection.Opens);
		Assert.Equal(0, connection.Closes);
		Assert.Equal(0, connection.Disposes);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Empty(tally.Readers);
	}
}
