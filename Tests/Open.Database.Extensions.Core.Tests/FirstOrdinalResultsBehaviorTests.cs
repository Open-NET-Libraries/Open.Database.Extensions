#nullable enable

namespace Open.Database.Extensions.Tests;

[ExcludeFromCodeCoverage]
public static class FirstOrdinalResultsBehaviorTests
{
	// FirstOrdinalResults<T>(IDbCommand, behavior) used to drop the caller's behavior when the command was a DbCommand.
	[Fact]
	public static void IDbCommand_ThatIsADbCommand_PassesTheCallersBehavior()
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		var command = new FakeDbCommand(connection, () => TypedReader.Create([("Id", typeof(int))], [1], [2]));
		IDbCommand asInterface = command;

		List<int> results = asInterface.FirstOrdinalResults<int>(CommandBehavior.KeyInfo).ToList();

		Assert.Equal(new[] { 1, 2 }, results);
		CommandBehavior used = Assert.Single(command.Behaviors);
		Assert.True(used.HasFlag(CommandBehavior.KeyInfo), $"the caller's behavior was dropped (used: {used})");
		Assert.True(used.HasFlag(CommandBehavior.SingleResult));
	}
}
