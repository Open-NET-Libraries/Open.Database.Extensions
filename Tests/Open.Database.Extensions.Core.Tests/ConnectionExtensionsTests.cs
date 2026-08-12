namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class ConnectionExtensionsTests
{
	[Fact]
	public static void EnsureOpen_ClosedConnection_OpensAndReturnsPriorState()
	{
		var connection = Substitute.For<IDbConnection>();
		connection.State.Returns(ConnectionState.Closed);

		ConnectionState prior = connection.EnsureOpen();

		Assert.Equal(ConnectionState.Closed, prior);
		connection.Received(1).Open();
	}

	[Fact]
	public static void EnsureOpen_OpenConnection_DoesNotReopen()
	{
		var connection = Substitute.For<IDbConnection>();
		connection.State.Returns(ConnectionState.Open);

		ConnectionState prior = connection.EnsureOpen();

		Assert.Equal(ConnectionState.Open, prior);
		connection.DidNotReceive().Open();
	}

	[Fact]
	public static void EnsureOpen_BrokenConnection_ClosesThenOpens()
	{
		var connection = Substitute.For<IDbConnection>();
		connection.State.Returns(ConnectionState.Broken);

		connection.EnsureOpen();

		connection.Received(1).Close();
		connection.Received(1).Open();
	}

	[Fact]
	public static void EnsureOpen_Null_Throws()
	{
		IDbConnection connection = null!;
		Assert.Throws<ArgumentNullException>(() => connection.EnsureOpen());
	}

	[Fact]
	public static void Factory_Create_InvokesUnderlyingFactory()
	{
		var connection = Substitute.For<IDbConnection>();
		int calls = 0;
		var factory = DbConnectionFactory.Create(() => { calls++; return connection; });

		IDbConnection created = factory.Create();

		Assert.Same(connection, created);
		Assert.Equal(1, calls);
	}

	[Fact]
	public static void Factory_Command_ProducesConfiguredExpressiveCommand()
	{
		var factory = DbConnectionFactory.Create(() => Substitute.For<IDbConnection>());

		ExpressiveCommand command = factory.Command("SELECT 1");

		Assert.Equal("SELECT 1", command.Command);
		Assert.Equal(CommandType.Text, command.Type);
	}
}
