namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// The Open/Using helpers decide who opens, who disposes and who gives a connection back.
/// The recorded call sequences below are those decisions.
/// </summary>
[ExcludeFromCodeCoverage]
public static class ConnectionOpenUsingTests
{
	#region Factory

	[Fact]
	public static void FactoryOpen_ClosedConnection_OpensRunsActionThenDisposes()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionFactory factory = probe.CreateFactory();

		factory.Open(_ => probe.Calls.Add("Action"));

		Assert.Equal("Create,Open,Action,DisposeConnection", probe.Log);
	}

	[Fact]
	public static void FactoryOpen_AlreadyOpenConnection_IsNotReopened()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		IDbConnectionFactory factory = probe.CreateFactory();

		factory.Open(_ => probe.Calls.Add("Action"));

		Assert.Equal("Create,Action,DisposeConnection", probe.Log);
	}

	[Fact]
	public static void FactoryOpen_ActionThrows_StillDisposesAndPropagates()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionFactory factory = probe.CreateFactory();

		var ex = Assert.Throws<InvalidOperationException>(
			() => factory.Open(_ => throw new InvalidOperationException("boom")));

		Assert.Equal("boom", ex.Message);
		Assert.Equal("Create,Open,DisposeConnection", probe.Log);
	}

	[Fact]
	public static void FactoryOpen_ReturnsActionValue()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionFactory factory = probe.CreateFactory();

		int result = factory.Open(_ => 5);

		Assert.Equal(5, result);
		Assert.Equal("Create,Open,DisposeConnection", probe.Log);
	}

	[Fact]
	public static void FactoryUsing_DoesNotOpenButDisposes()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionFactory factory = probe.CreateFactory();

		factory.Using(_ => probe.Calls.Add("Action"));

		Assert.Equal("Create,Action,DisposeConnection", probe.Log);
	}

	[Fact]
	public static void FuncOpen_OpensAndDisposes_ButFuncUsingOnlyDisposes()
	{
		var openProbe = new ConnectionProbe(ConnectionState.Closed);
		Func<IDbConnection> openFactory = () => openProbe.Connection;
		openFactory.Open(_ => openProbe.Calls.Add("Action"));
		Assert.Equal("Open,Action,DisposeConnection", openProbe.Log);

		var usingProbe = new ConnectionProbe(ConnectionState.Closed);
		Func<IDbConnection> usingFactory = () => usingProbe.Connection;
		usingFactory.Using(_ => usingProbe.Calls.Add("Action"));
		Assert.Equal("Action,DisposeConnection", usingProbe.Log);
	}

	#endregion

	#region Pool

	[Theory]
	[InlineData(ConnectionState.Closed, "Take,Open,Action,Give")]
	[InlineData(ConnectionState.Open, "Take,Action,Give")]
	public static void PoolOpen_OpensOnlyIfNeeded_AndAlwaysGivesBack(ConnectionState initial, string expected)
	{
		var probe = new ConnectionProbe(initial);
		IDbConnectionPool pool = probe.CreatePool();
		ConnectionState? received = null;

		pool.Open((_, state) =>
		{
			received = state;
			probe.Calls.Add("Action");
		});

		Assert.Equal(expected, probe.Log);
		Assert.Equal(initial, received); // The action is told what state the connection was in beforehand.
	}

	[Fact]
	public static void PoolOpen_ActionThrows_StillGivesBack()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionPool pool = probe.CreatePool();

		Assert.Throws<InvalidOperationException>(
			() => pool.Open((_, __) => throw new InvalidOperationException()));

		Assert.Equal("Take,Open,Give", probe.Log);
	}

	[Fact]
	public static void PoolUsing_DoesNotOpen_AndGivesBack()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionPool pool = probe.CreatePool();

		pool.Using(_ => probe.Calls.Add("Action"));

		Assert.Equal("Take,Action,Give", probe.Log);
		Assert.Equal(ConnectionState.Closed, probe.State);
	}

	[Fact]
	public static void FactoryAsPool_TakeCreatesEachTime_AndGiveDisposes()
	{
		var created = new List<IDbConnection>();
		IDbConnectionFactory factory = DbConnectionFactory.Create(() =>
		{
			var c = Substitute.For<IDbConnection>();
			created.Add(c);
			return c;
		});

		IDbConnection first = factory.AsPool().Take();
		IDbConnection second = factory.AsPool().Take();
		factory.AsPool().Give(first);

		Assert.NotSame(first, second);
		Assert.Equal(2, created.Count);
		first.Received(1).Dispose();
		second.DidNotReceive().Dispose();
	}

	#endregion

	#region Async

	[Fact]
	public static async Task FactoryOpenAsync_OpensRunsActionThenDisposes()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionFactory factory = probe.CreateFactory();

		await factory.OpenAsync(_ =>
		{
			probe.Calls.Add("Action");
			return ValueTask.CompletedTask;
		}, TestContext.Current.CancellationToken);

		Assert.Equal("Create,Open,Action,DisposeConnection", probe.Log);
	}

	[Fact]
	public static async Task FactoryOpenAsync_AlreadyCancelled_NeverRunsActionButStillDisposes()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionFactory factory = probe.CreateFactory();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			await factory.OpenAsync(_ =>
			{
				probe.Calls.Add("Action");
				return ValueTask.CompletedTask;
			}, cts.Token));

		Assert.Equal("Create,DisposeConnection", probe.Log);
	}

	[Fact]
	public static async Task PoolOpenAsync_ActionThrows_StillGivesBack()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionPool pool = probe.CreatePool();

		await Assert.ThrowsAsync<InvalidOperationException>(async () =>
			await pool.OpenAsync((_, __) =>
			{
				probe.Calls.Add("Action");
				throw new InvalidOperationException();
			}, TestContext.Current.CancellationToken));

		Assert.Equal("Take,Open,Action,Give", probe.Log);
	}

	[Fact]
	public static async Task PoolOpenAsync_AlreadyCancelled_GivesBackWithoutRunningAction()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionPool pool = probe.CreatePool();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			await pool.OpenAsync((_, __) =>
			{
				probe.Calls.Add("Action");
				return ValueTask.CompletedTask;
			}, cts.Token));

		Assert.Equal("Take,Give", probe.Log);
	}

	[Fact]
	public static async Task PoolUsingAsync_DoesNotOpen_AndGivesBackEvenWhenActionThrows()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionPool pool = probe.CreatePool();

		await Assert.ThrowsAsync<InvalidOperationException>(async () =>
			await pool.UsingAsync(_ =>
			{
				probe.Calls.Add("Action");
				throw new InvalidOperationException();
			}));

		Assert.Equal("Take,Action,Give", probe.Log);
	}

	[Fact]
	public static async Task FactoryUsingAsync_DisposesEvenWhenActionThrows()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		IDbConnectionFactory factory = probe.CreateFactory();

		await Assert.ThrowsAsync<InvalidOperationException>(async () =>
			await factory.UsingAsync(_ => throw new InvalidOperationException()));

		Assert.Equal("Create,DisposeConnection", probe.Log);
	}

	#endregion

	#region Argument validation

	[Fact]
	public static void Open_NullArguments_Throw()
	{
		var probe = new ConnectionProbe();
		IDbConnectionFactory factory = probe.CreateFactory();
		IDbConnectionPool pool = probe.CreatePool();

		Assert.Equal("connectionFactory", Assert.Throws<ArgumentNullException>(
			() => ((IDbConnectionFactory)null!).Open(_ => { })).ParamName);
		Assert.Equal("action", Assert.Throws<ArgumentNullException>(
			() => factory.Open(null!)).ParamName);
		Assert.Equal("connectionPool", Assert.Throws<ArgumentNullException>(
			() => ((IDbConnectionPool)null!).Open((_, __) => { })).ParamName);
		Assert.Equal("action", Assert.Throws<ArgumentNullException>(
			() => pool.Open(null!)).ParamName);

		// Validation happens before anything is created or taken.
		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static void Using_NullArguments_Throw()
	{
		var probe = new ConnectionProbe();
		IDbConnectionFactory factory = probe.CreateFactory();
		IDbConnectionPool pool = probe.CreatePool();

		Assert.Equal("connectionFactory", Assert.Throws<ArgumentNullException>(
			() => ((IDbConnectionFactory)null!).Using(_ => { })).ParamName);
		Assert.Equal("action", Assert.Throws<ArgumentNullException>(
			() => factory.Using(null!)).ParamName);
		Assert.Equal("connectionPool", Assert.Throws<ArgumentNullException>(
			() => ((IDbConnectionPool)null!).Using(_ => { })).ParamName);
		Assert.Equal("action", Assert.Throws<ArgumentNullException>(
			() => pool.Using(null!)).ParamName);

		Assert.Equal(string.Empty, probe.Log);
	}

	#endregion
}
