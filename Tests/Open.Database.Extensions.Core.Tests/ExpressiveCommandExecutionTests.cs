namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// Execution-time behavior of <see cref="ExpressiveCommand"/>: connection ownership, ordering of
/// open / create / execute / dispose / close, and how commands are configured.
/// </summary>
[ExcludeFromCodeCoverage]
public static class ExpressiveCommandExecutionTests
{
	[Fact]
	public static void ExecuteReader_ClosedConnection_OpensFirst_RequestsCloseConnection_AndRestoresClosed()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");

		command.ExecuteReader(r => probe.Calls.Add("Handler"));

		// "Open MUST occur before command creation as some DbCommands require it."
		Assert.Equal("Open,CreateCommand,ExecuteReader,Handler,DisposeReader,DisposeCommand,Close", probe.Log);
		Assert.True(probe.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	[Fact]
	public static void ExecuteReader_OpenConnection_LeavesItOpenAndUndisposed()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");

		command.ExecuteReader(r => probe.Calls.Add("Handler"));

		Assert.Equal("CreateCommand,ExecuteReader,Handler,DisposeReader,DisposeCommand", probe.Log);
		Assert.False(probe.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
		Assert.Equal(ConnectionState.Open, probe.State);
	}

	[Fact]
	public static void ExecuteReader_KeepsCallerBehavior_WhenAddingCloseConnection()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");

		command.ExecuteReader(r => r.Read(), CommandBehavior.SequentialAccess);

		Assert.Equal(
			CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection,
			probe.Behaviors.Single());
	}

	[Fact]
	public static void ExecuteReader_HandlerThrows_StillRestoresClosedConnection()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");

		Assert.Throws<InvalidOperationException>(
			() => command.ExecuteReader(r => throw new InvalidOperationException()));

		Assert.Equal("Open,CreateCommand,ExecuteReader,DisposeReader,DisposeCommand,Close", probe.Log);
		Assert.Equal(ConnectionState.Closed, probe.State);
	}

	[Fact]
	public static async Task ExecuteReaderAsync_ClosedConnection_OpensFirst_RequestsCloseConnection_AndRestoresClosed()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");
		ValueTask handler(IDataReader r)
		{
			probe.Calls.Add("Handler");
			return ValueTask.CompletedTask;
		}

		await command.ExecuteReaderAsync(handler);

		Assert.Equal("Open,CreateCommand,ExecuteReader,Handler,DisposeReader,DisposeCommand,Close", probe.Log);
		Assert.True(probe.Behaviors.Single().HasFlag(CommandBehavior.CloseConnection));
	}

	[Fact]
	public static void Execute_NestedOnSameConnection_ThrowsConcurrentUse_ThenConnectionIsReusable()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");

		var ex = Assert.Throws<InvalidOperationException>(
			() => command.Execute(c => command.Execute(_ => { })));

		Assert.Contains("Concurrent", ex.Message);
		Assert.Equal("Open,CreateCommand,DisposeCommand,Close", probe.Log);

		// The failed attempt must not leave the single-connection provider stuck as "taken".
		Assert.Equal(7, command.ExecuteNonQuery());
	}

	[Fact]
	public static void ExecuteNonQuery_AppliesTextTypeAndTimeoutToTheCreatedCommand()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		ExpressiveCommand command = probe.Connection.StoredProcedure("do_thing").SetTimeout(45);

		int result = command.ExecuteNonQuery();

		Assert.Equal(7, result);
		Assert.Equal("do_thing", probe.Command.CommandText);
		Assert.Equal(CommandType.StoredProcedure, probe.Command.CommandType);
		Assert.Equal(45, probe.Command.CommandTimeout);
	}

	[Fact]
	public static void ExecuteNonQuery_UntouchedTimeout_AppliesTheCommandsOwnDefault()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");

		command.ExecuteNonQuery();

		Assert.True(command.Timeout > 0);
		Assert.Equal(command.Timeout, probe.Command.CommandTimeout);
	}

	[Fact]
	public static void Execute_AppliesParamsToTheCommand_InOrder_WithDbTypeOnlyWhenGiven()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		ExpressiveCommand command = probe.Connection.Command("SELECT @a, @b")
			.AddParam("@a", 1)
			.AddParam("@b", (object)"x", DbType.String);

		command.ExecuteNonQuery();

		Assert.Equal(2, probe.Parameters.Count);
		Assert.Equal("@a", probe.Parameters[0].ParameterName);
		Assert.Equal(1, probe.Parameters[0].Value);
		Assert.Equal("@b", probe.Parameters[1].ParameterName);
		Assert.Equal("x", probe.Parameters[1].Value);
		Assert.Equal(DbType.String, probe.Parameters[1].DbType);
	}

	[Fact]
	public static void Transaction_IsAssignedToTheCreatedCommand()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var transaction = Substitute.For<IDbTransaction>();
		transaction.Connection.Returns(probe.Connection);

		transaction.Command("UPDATE t SET x = 1").ExecuteNonQuery();

		Assert.Same(transaction, probe.Command.Transaction);
	}

	[Fact]
	public static void Transaction_WithoutConnection_Throws()
	{
		var transaction = Substitute.For<IDbTransaction>();
		transaction.Connection.Returns((IDbConnection)null!);

		Assert.Throws<ArgumentNullException>(() => transaction.Command("UPDATE t SET x = 1"));
	}

	[Fact]
	public static void FactoryBased_CreatesAndDisposesAConnectionPerExecution()
	{
		var probes = new List<ConnectionProbe>();
		IDbConnectionFactory factory = DbConnectionFactory.Create<IDbConnection>(() =>
		{
			var p = new ConnectionProbe(ConnectionState.Closed);
			probes.Add(p);
			return p.Connection;
		});
		ExpressiveCommand command = factory.Command("SELECT 1");

		command.ExecuteNonQuery();
		command.ExecuteNonQuery();

		Assert.Equal(2, probes.Count);
		foreach (ConnectionProbe p in probes)
		{
			// Disposed (not just closed): a factory has no use for the connection afterwards.
			Assert.Equal("Open,CreateCommand,ExecuteNonQuery,DisposeCommand,DisposeConnection", p.Log);
		}
	}

	[Fact]
	public static void PoolBased_TakesAndGivesBack_EvenWhenExecutionFails_WithoutClosingItself()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		ExpressiveCommand command = probe.CreatePool().Command("SELECT 1");

		Assert.Throws<InvalidOperationException>(
			() => command.Execute(c => throw new InvalidOperationException()));

		Assert.Equal("Take,Open,CreateCommand,DisposeCommand,Give", probe.Log);
	}

	[Fact]
	public static void ExecuteScalar_ReturnsValueAndSupportsTypedAndTransformedForms()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		ExpressiveCommand command = probe.Connection.Command("SELECT 1");

		Assert.Equal(42, command.ExecuteScalar());
		Assert.Equal(42, command.ExecuteScalar<int>());
		Assert.Equal("v42", command.ExecuteScalar(o => $"v{o}"));
	}

	[Fact]
	public static async Task ExecuteAsync_AlreadyCancelled_ThrowsBeforeOpeningAnything()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		ExpressiveCommand command = probe.Connection.Command("SELECT 1").UseCancellationToken(cts.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			await command.ExecuteAsync(c => ValueTask.CompletedTask));

		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static void Constructor_RejectsNullAndBlankCommandText_AndNullConnection()
	{
		var probe = new ConnectionProbe();

		Assert.Throws<ArgumentNullException>(
			() => new ExpressiveCommand(probe.Connection, CommandType.Text, null!));
		Assert.Throws<ArgumentException>(
			() => new ExpressiveCommand(probe.Connection, CommandType.Text, "   "));
		Assert.Throws<ArgumentNullException>(
			() => new ExpressiveCommand((IDbConnection)null!, CommandType.Text, "SELECT 1"));
	}
}
