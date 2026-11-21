namespace Open.Database.Extensions.Tests;

#nullable enable

// A command that has no connection is not opened or closed by the view: the
// command itself says so (InvalidOperationException from the provider), not an
// ArgumentNullException from the scope, as it always has.
[ExcludeFromCodeCoverage]
public static class MappedCommandNoConnectionTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	[Fact]
	public static void Sync_TheCommandReportsItsMissingConnection()
	{
		var command = new FakeDbCommand(null, () => new ChannelTestReader(3));

		Assert.Throws<InvalidOperationException>(
			() => command.Map<int>().ToArray());

		Assert.Single(command.Behaviors);
	}

	[Fact]
	public static async Task Async_TheCommandReportsItsMissingConnection()
	{
		var command = new FakeDbCommand(null, () => new ChannelTestReader(3));

		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await command.Map<int>().ToArrayAsync(Ct));

		Assert.Single(command.Behaviors);
	}
}