using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// The <see cref="IDbCommand"/> / <see cref="DbCommand"/> read helpers each decide whether to open the connection,
/// whether to ask for <see cref="CommandBehavior.CloseConnection"/> and which other behavior flags to add.
/// </summary>
[ExcludeFromCodeCoverage]
public static class CommandExtensionsTests
{
	static ConnectionProbe ProbeWithRows(ConnectionState state, params int[] ids)
		=> new(state)
		{
			ReaderFactory = () => FakeReader.Create(["id"], ids.Select(i => new object?[] { i }).ToArray()),
		};

	static void Run(string operation, IDbCommand c)
	{
		switch (operation)
		{
			case "ExecuteReaderAction": c.ExecuteReader(_ => { }); break;
			case "ExecuteReaderFunc": c.ExecuteReader(r => r.Read()); break;
			default: throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
		}
	}

	public static TheoryData<string> Operations
		=> [
			"ExecuteReaderAction", "ExecuteReaderFunc",
		];

	[Theory]
	[MemberData(nameof(Operations))]
	public static void Operation_ClosedConnection_OpensItAndRequestsCloseConnection(string operation)
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Closed, 1);

		Run(operation, probe.Command);

		Assert.Equal("Open,ExecuteReader,DisposeReader", probe.Log);
		Assert.True(probe.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	[Theory]
	[MemberData(nameof(Operations))]
	public static void Operation_OpenConnection_LeavesItAlone(string operation)
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Open, 1);

		Run(operation, probe.Command);

		Assert.Equal("ExecuteReader,DisposeReader", probe.Log);
		Assert.False(probe.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	// The asynchronous reader helpers decide the same way: a closed connection is opened and closed again by the call,
	// an open one is left alone.
	[Fact]
	public static async Task ExecuteReaderAsync_ClosedConnection_OpensItAndRequestsCloseConnection()
	{
		var connection = new FakeDbConnection();
		var command = new FakeDbCommand(connection, () => FakeReader.CreateDb(["id"], [1]));

		await command.ExecuteReaderAsync(_ => { }, CommandBehavior.Default, TestContext.Current.CancellationToken);

		Assert.Equal(["open"], connection.Log);
		Assert.True(Assert.Single(command.Behaviors).HasFlag(CommandBehavior.CloseConnection));
	}

	[Fact]
	public static async Task ExecuteReaderAsync_OpenConnection_LeavesItAlone()
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		var command = new FakeDbCommand(connection, () => FakeReader.CreateDb(["id"], [1]));

		await command.ExecuteReaderAsync(_ => { }, CommandBehavior.Default, TestContext.Current.CancellationToken);

		Assert.Empty(connection.Log);
		Assert.False(Assert.Single(command.Behaviors).HasFlag(CommandBehavior.CloseConnection));
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Fact]
	public static void ExecuteReader_ANullHandler_IsRefusedBeforeTheConnectionIsTouched()
	{
		var probe = new ConnectionProbe();

		Assert.Equal("handler", Assert.Throws<ArgumentNullException>(
			() => probe.Command.ExecuteReader(null!)).ParamName);
		Assert.Equal("command", Assert.Throws<ArgumentNullException>(
			() => ((IDbCommand)null!).ExecuteReader(_ => { })).ParamName);
		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static async Task ExecuteReaderAsync_NonDbCommand_FallsBackToSynchronousPath()
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Closed, 1);
		ValueTask handler(IDataReader _)
		{
			probe.Calls.Add("Handler");
			return ValueTask.CompletedTask;
		}

		await probe.Command.ExecuteReaderAsync(handler, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal("Open,ExecuteReader,Handler,DisposeReader", probe.Log);
		Assert.True(probe.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	#region DbCommand (async)

	static FakeDbCommand NewDbCommand(ConnectionProbe probe, params int[] ids)
		=> new(probe.Db, () => FakeReader.CreateDb(["id"], ids.Select(i => new object?[] { i }).ToArray()));

	[Fact]
	public static async Task ExecuteReaderAsync_ClosedConnection_RequestsCloseConnectionAndKeepsCallerBehavior()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed, dbConnection: true);
		FakeDbCommand command = NewDbCommand(probe, 1);

		bool read = await command.ExecuteReaderAsync(r => r.Read(), CommandBehavior.SequentialAccess, cancellationToken: TestContext.Current.CancellationToken);

		Assert.True(read);
		Assert.Equal("OpenAsync", probe.Log);
		Assert.Equal(
			CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection,
			command.Behaviors.Single());
	}

	#endregion
}
