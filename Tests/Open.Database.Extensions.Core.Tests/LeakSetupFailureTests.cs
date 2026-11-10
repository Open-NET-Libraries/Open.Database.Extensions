#nullable enable

namespace Open.Database.Extensions.Tests;

// A command the library creates and then fails to set up is never handed to anyone, so the library is the only one who can dispose it.
[ExcludeFromCodeCoverage]
public static class LeakSetupFailureTests
{
	// A provider that rejects a parameter (a name or value it cannot use) must not strand the command that was already created for the call.
	[Fact]
	public static void ParameterRejected_TheCommandIsStillDisposed_AndTheConnectionRestored()
	{
		var tally = new DbTally(new() { FailCreateParameter = true });
		TrackedConnection connection = tally.NewConnection();

		Assert.Throws<InvalidOperationException>(() => connection.Command("select 1").AddParam("x", 1).ExecuteScalar());

		Assert.Single(tally.Commands);
		LeakAssert.CommandsAndReadersDisposed(tally);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(connection.Opens, connection.Closes);
	}

	[Fact]
	public static async Task ParameterRejected_Async_TheCommandIsStillDisposed_AndTheConnectionRestored()
	{
		var tally = new DbTally(new() { FailCreateParameter = true });
		TrackedConnection connection = tally.NewConnection();

#pragma warning disable xUnit1051 // The leak check must see the same token the call has always had: none.
		await Assert.ThrowsAsync<InvalidOperationException>(async () => await connection.Command("select 1").AddParam("x", 1).ExecuteScalarAsync());
#pragma warning restore xUnit1051

		Assert.Single(tally.Commands);
		LeakAssert.CommandsAndReadersDisposed(tally);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(connection.Opens, connection.Closes);
	}

	// The commands the extension methods hand to the caller are the caller's to dispose; one that fails while being set up never reaches the caller.
	[Fact]
	public static void CreateCommand_ASetterThrows_TheCommandIsDisposed()
	{
		var tally = new DbTally();
		TrackedConnection connection = tally.NewOpenConnection();

		Assert.Throws<ArgumentOutOfRangeException>(() => connection.CreateCommand(CommandType.Text, "select 1", -1));

		Assert.Single(tally.Commands);
		LeakAssert.CommandsAndReadersDisposed(tally);
	}
}
