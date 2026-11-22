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

	[Fact]
	public static void FirstOrdinalResults_ReadsFirstColumn_ConvertsDBNullToNull_AndRequestsSequentialSingleResult()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed)
		{
			ReaderFactory = () => FakeReader.Create(["id"], [1], [null], [3]),
		};

		var results = probe.Command.FirstOrdinalResults().ToList();

		Assert.Equal([1, null, 3], results);
		Assert.Equal(
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.CloseConnection,
			probe.Behaviors.Single());
		Assert.Contains("DisposeReader", probe.Log);
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
		=> new(probe.Db!, () => FakeReader.CreateDb(["id"], ids.Select(i => new object?[] { i }).ToArray()));

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
