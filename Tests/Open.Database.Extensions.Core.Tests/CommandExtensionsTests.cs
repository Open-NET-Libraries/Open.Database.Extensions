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
			case "ToList": c.ToList(r => r.GetValue(0)); break;
			case "ToArray": c.ToArray(r => r.GetValue(0)); break;
			case "ToImmutableArray": c.ToImmutableArray(r => r.GetValue(0)); break;
			case "ExecuteReaderAction": c.ExecuteReader((IDataReader r) => { }); break;
			case "ExecuteReaderFunc": c.ExecuteReader(r => r.Read()); break;
			case "IterateReader": c.IterateReader((IDataRecord r) => { }); break;
			case "IterateReaderWhile": c.IterateReaderWhile(r => true); break;
			case "First": c.First(r => r.GetValue(0)); break;
			case "FirstOrDefault": c.FirstOrDefault(r => r.GetValue(0)); break;
			case "Single": c.Single(r => r.GetValue(0)); break;
			case "SingleOrDefault": c.SingleOrDefault(r => r.GetValue(0)); break;
			case "Take": c.Take(1, r => r.GetValue(0)); break;
			case "Skip": c.Skip(0, r => r.GetValue(0)); break;
			case "SkipThenTake": c.SkipThenTake(0, 1, r => r.GetValue(0)); break;
			default: throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
		}
	}

	public static TheoryData<string> Operations => new()
	{
		"ToList", "ToArray", "ToImmutableArray", "ExecuteReaderAction", "ExecuteReaderFunc", "IterateReader",
		"IterateReaderWhile", "First", "FirstOrDefault", "Single", "SingleOrDefault", "Take", "Skip", "SkipThenTake",
	};

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

	[Theory]
	[InlineData("First", true)]
	[InlineData("FirstOrDefault", true)]
	[InlineData("Single", false)] // Single must see a possible second row to be able to fail.
	[InlineData("SingleOrDefault", false)]
	[InlineData("ToList", false)]
	public static void SingleRowBehavior_IsRequestedOnlyWhenFirstRowIsAllThatIsNeeded(string operation, bool expectSingleRow)
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Open, 1);

		Run(operation, probe.Command);

		Assert.Equal(expectSingleRow, probe.Behaviors.Single().HasFlag(CommandBehavior.SingleRow));
	}

	[Fact]
	public static void First_CombinesCallerBehavior_SingleRow_AndCloseConnection()
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Closed, 1);

		probe.Command.First(r => r.GetValue(0), CommandBehavior.SequentialAccess);

		Assert.Equal(
			CommandBehavior.SequentialAccess | CommandBehavior.SingleRow | CommandBehavior.CloseConnection,
			probe.Behaviors.Single());
	}

	[Fact]
	public static void Single_MoreThanOneRow_ThrowsAfterDisposingTheReader()
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Open, 1, 2);

		Assert.Throws<InvalidOperationException>(() => probe.Command.Single(r => r.GetValue(0)));

		Assert.Equal("ExecuteReader,DisposeReader", probe.Log);
	}

	[Fact]
	public static void First_NoRows_Throws_ButFirstOrDefaultReturnsDefault()
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Open);

		Assert.Throws<InvalidOperationException>(() => probe.Command.First(r => r.GetValue(0)));
		Assert.Null(probe.Command.FirstOrDefault(r => r.GetValue(0)));
	}

	[Fact]
	public static void Take_Skip_SkipThenTake_ApplyTheirCounts()
	{
		static int Id(IDataRecord r) => (int)r.GetValue(0);

		Assert.Equal([1, 2], ProbeWithRows(ConnectionState.Open, 1, 2, 3, 4, 5).Command.Take(2, Id));
		Assert.Equal([3, 4, 5], ProbeWithRows(ConnectionState.Open, 1, 2, 3, 4, 5).Command.Skip(2, Id));
		Assert.Equal([2, 3], ProbeWithRows(ConnectionState.Open, 1, 2, 3, 4, 5).Command.SkipThenTake(1, 2, Id));
		Assert.Empty(ProbeWithRows(ConnectionState.Open, 1, 2).Command.Skip(10, Id));
	}

	[Fact]
	public static void FirstOrdinalResults_ReadsFirstColumn_ConvertsDBNullToNull_AndRequestsSequentialSingleResult()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed)
		{
			ReaderFactory = () => FakeReader.Create(["id"], new object?[] { 1 }, new object?[] { null }, new object?[] { 3 }),
		};

		List<object?> results = probe.Command.FirstOrdinalResults().ToList();

		Assert.Equal([1, null, 3], results);
		Assert.Equal(
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.CloseConnection,
			probe.Behaviors.Single());
		Assert.Contains("DisposeReader", probe.Log);
	}

	[Fact]
	public static void NullArguments_ThrowArgumentNull()
	{
		var probe = new ConnectionProbe();

		Assert.Equal("command", Assert.Throws<ArgumentNullException>(
			() => ((IDbCommand)null!).ToList(r => r.GetValue(0))).ParamName);
		Assert.Equal("transform", Assert.Throws<ArgumentNullException>(
			() => probe.Command.ToList((Func<IDataRecord, object>)null!)).ParamName);
		Assert.Equal("handler", Assert.Throws<ArgumentNullException>(
			() => probe.Command.ExecuteReader((Action<IDataReader>)null!)).ParamName);
		Assert.Equal("predicate", Assert.Throws<ArgumentNullException>(
			() => probe.Command.IterateReaderWhile(null!)).ParamName);

		// Arguments are validated before the connection is touched.
		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static async Task ExecuteReaderAsync_NonDbCommand_FallsBackToSynchronousPath()
	{
		ConnectionProbe probe = ProbeWithRows(ConnectionState.Closed, 1);
		Func<IDataReader, ValueTask> handler = r =>
		{
			probe.Calls.Add("Handler");
			return ValueTask.CompletedTask;
		};

		await probe.Command.ExecuteReaderAsync(handler, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal("Open,ExecuteReader,Handler,DisposeReader", probe.Log);
		Assert.True(probe.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	#region DbCommand (async)

	static FakeDbCommand NewDbCommand(ConnectionProbe probe, params int[] ids)
		=> new(probe.Db!, () => FakeReader.CreateDb(["id"], ids.Select(i => new object?[] { i }).ToArray()));

	[Fact]
	public static async Task ToListAsync_ClosedConnection_OpensAsyncAndRequestsCloseConnection()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed, dbConnection: true);
		FakeDbCommand command = NewDbCommand(probe, 1, 2);

		List<int> results = await command.ToListAsync(r => (int)r.GetValue(0), cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal([1, 2], results);
		Assert.Equal("OpenAsync", probe.Log);
		Assert.True(command.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	[Fact]
	public static async Task ToListAsync_OpenConnection_IsNotReopenedOrClosed()
	{
		var probe = new ConnectionProbe(ConnectionState.Open, dbConnection: true);
		FakeDbCommand command = NewDbCommand(probe, 1, 2);

		List<int> results = await command.ToListAsync(r => (int)r.GetValue(0), cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal([1, 2], results);
		Assert.Equal(string.Empty, probe.Log);
		Assert.False(command.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	[Fact]
	public static async Task ToListAsync_WithoutReadAsync_ReadsTheSameRows()
	{
		var probe = new ConnectionProbe(ConnectionState.Open, dbConnection: true);
		FakeDbCommand command = NewDbCommand(probe, 1, 2, 3);

		List<int> results = await command.ToListAsync(r => (int)r.GetValue(0), useReadAsync: false, cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal([1, 2, 3], results);
	}

	[Fact]
	public static async Task ForEachAsync_AlreadyCancelled_ThrowsWithoutOpeningOrExecuting()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed, dbConnection: true);
		FakeDbCommand command = NewDbCommand(probe, 1);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			await command.ForEachAsync((Action<IDataRecord>)(r => { }), cancellationToken: cts.Token));

		Assert.Equal(string.Empty, probe.Log);
		Assert.Empty(command.Behaviors);
	}

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
