namespace Open.Database.Extensions.Tests;

#nullable enable

// EnsureOpenScope / EnsureOpenScopeAsync on a factory and on a pool follow the Open / OpenAsync
// callbacks: a factory's connection is created, opened if needed, and disposed; a
// pool's is taken, opened if needed, and given back, and the pool decides what that
// means. If the open fails the connection is released the same way first.
[ExcludeFromCodeCoverage]
public static class ConnectionSourceScopeTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// Counts what the source was asked, and writes "give" into the connection's own
	// log so that the order against open and dispose is visible in one list.
	sealed class CountingFactory(ConnectionScopeTests.ScopeConnection connection)
		: IDbConnectionFactory<ConnectionScopeTests.ScopeConnection>
	{
		public int Creates { get; private set; }

		public ConnectionScopeTests.ScopeConnection Create()
		{
			Creates++;
			return connection;
		}

		IDbConnection IDbConnectionFactory.Create() => Create();
	}

	sealed class CountingPool(ConnectionScopeTests.ScopeConnection connection)
		: IDbConnectionPool<ConnectionScopeTests.ScopeConnection>
	{
		public int Takes { get; private set; }

		public int Gives { get; private set; }

		public ConnectionScopeTests.ScopeConnection Take()
		{
			Takes++;
			return connection;
		}

		IDbConnection IDbConnectionPool.Take() => Take();

		// Does not close or dispose: what giving back means is the pool's, and
		// this one only records it.
		public void Give(IDbConnection given)
		{
			Gives++;
			Assert.Same(connection, given);
			connection.Log.Add("give");
		}
	}

	static ConnectionScopeTests.ScopeConnection Failing()
		=> new() { OpenException = new InvalidOperationException("open failed") };

	// ---- a factory: created, opened, disposed ----------------------------------------

	[Fact]
	public static void AFactory_CreatesOpensAndDisposes()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var factory = new CountingFactory(connection);

		using (var scope = factory.EnsureOpenScope())
		{
			Assert.Same(connection, scope.Connection);
			Assert.Equal(ConnectionState.Open, connection.State);
			Assert.Equal(ConnectionState.Closed, scope.PriorState);
		}

		Assert.Equal(1, factory.Creates);
		Assert.Equal(["open", "dispose"], connection.Log);
		Assert.True(connection.WasDisposed);
	}

	[Fact]
	public static async Task AFactory_Async_OpensWithOpenAsync_AndDisposesWithDisposeAsync()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var factory = new CountingFactory(connection);

		await using (var scope = await factory.EnsureOpenScopeAsync(Ct))
		{
			Assert.Same(connection, scope.Connection);
			Assert.Equal(ConnectionState.Open, connection.State);
		}

		Assert.Equal(1, factory.Creates);
		Assert.Equal(["openAsync", "disposeAsync"], connection.Log);
	}

	[Fact]
	public static async Task AFactory_NotGeneric_GivesTheInterface()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		IDbConnectionFactory factory = new CountingFactory(connection);

		await using (ConnectionScope<IDbConnection> scope = factory.EnsureOpenScope())
			Assert.Same(connection, scope.Connection);

		// The fake keeps its state when disposed; a real connection would not be open.
		connection.SetState(ConnectionState.Closed);

		await using (ConnectionScope<IDbConnection> scope = await factory.EnsureOpenScopeAsync(Ct))
			Assert.Same(connection, scope.Connection);

		Assert.Equal(["open", "dispose", "openAsync", "disposeAsync"], connection.Log);
	}

	[Fact]
	public static void AFactory_DisposesAConnectionItFoundOpen_TooAndSaysSo()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		connection.SetState(ConnectionState.Open);

		using (var scope = new CountingFactory(connection).EnsureOpenScope())
			Assert.Equal(ConnectionState.Open, scope.PriorState);

		// The factory's connection is the scope's to dispose, however it was found.
		Assert.Equal(["dispose"], connection.Log);
	}

	[Fact]
	public static async Task AFactory_PlainConnection_HasNoAsyncDispose_ButIsStillDisposed()
	{
		var connection = new ConnectionScopeTests.PlainConnection();
		var factory = new DbConnectionFactory<ConnectionScopeTests.PlainConnection>(() => connection);

		await using (await factory.EnsureOpenScopeAsync(Ct)) { }

		Assert.Equal(["open"], connection.Log);
		Assert.True(connection.WasDisposed);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task AFactory_WhenOpenThrows_DisposesTheConnection_AndRethrows(
		bool async)
	{
		var connection = Failing();
		var factory = new CountingFactory(connection);

		var ex = async
			? await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await factory.EnsureOpenScopeAsync(Ct))
			: Assert.Throws<InvalidOperationException>(() => factory.EnsureOpenScope());

		Assert.Equal("open failed", ex.Message);
		Assert.Equal(1, factory.Creates);
		Assert.Equal(async ? ["openAsync", "disposeAsync"] : ["open", "dispose"], connection.Log);
		Assert.True(connection.WasDisposed);
	}

	[Fact]
	public static async Task AFactory_WhenCancelled_DisposesTheConnection_AndNothingIsOpened()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var factory = new CountingFactory(connection);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await factory.EnsureOpenScopeAsync(cts.Token));

		Assert.Equal(["disposeAsync"], connection.Log);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static async Task AFactory_WhenCancelledWhileOpening_ClosesAndDisposesTheConnection()
	{
		using var cts = new CancellationTokenSource();
		var connection = new ConnectionScopeTests.ScopeConnection { CancelWhenOpened = cts };
		var factory = new CountingFactory(connection);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await factory.EnsureOpenScopeAsync(cts.Token));

		// Opened, then closed by EnsureOpenAsync, then disposed by the scope: not left open.
		Assert.Equal(["openAsync", "close", "disposeAsync"], connection.Log);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static void AFactory_ASecondDisposeOfACopy_DisposesAgain()
	{
		// The documented hazard of a readonly struct: pinned so a change is deliberate.
		var connection = new ConnectionScopeTests.ScopeConnection();
		var scope = new CountingFactory(connection).EnsureOpenScope();
		var copy = scope;

		scope.Dispose();
		copy.Dispose();

		Assert.Equal(["open", "dispose", "dispose"], connection.Log);
	}

	// ---- a pool: taken, opened, given back --------------------------------------------

	[Fact]
	public static void APool_TakesOpensAndGivesBackOnce()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var pool = new CountingPool(connection);

		using (var scope = pool.EnsureOpenScope())
		{
			Assert.Same(connection, scope.Connection);
			Assert.Equal(ConnectionState.Open, connection.State);
			Assert.Equal(ConnectionState.Closed, scope.PriorState);
			Assert.Equal(0, pool.Gives);
		}

		// What the callback does: give it back, and leave the state to the pool.
		Assert.Equal(1, pool.Takes);
		Assert.Equal(1, pool.Gives);
		Assert.Equal(["open", "give"], connection.Log);
		Assert.False(connection.WasDisposed);
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Fact]
	public static async Task APool_Async_OpensWithOpenAsync_AndGivesBackOnce()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var pool = new CountingPool(connection);

		await using (var scope = await pool.EnsureOpenScopeAsync(Ct))
			Assert.Same(connection, scope.Connection);

		Assert.Equal(1, pool.Takes);
		Assert.Equal(1, pool.Gives);
		Assert.Equal(["openAsync", "give"], connection.Log);
	}

	[Fact]
	public static async Task APool_NotGeneric_GivesTheInterface()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var counting = new CountingPool(connection);
		IDbConnectionPool pool = counting;

		await using (ConnectionScope<IDbConnection> scope = pool.EnsureOpenScope())
			Assert.Same(connection, scope.Connection);

		await using (ConnectionScope<IDbConnection> scope = await pool.EnsureOpenScopeAsync(Ct))
			Assert.Same(connection, scope.Connection);

		Assert.Equal(2, counting.Gives);
	}

	[Fact]
	public static void APool_LeavesAnOpenConnectionToThePool_AndSaysSo()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		connection.SetState(ConnectionState.Open);
		var pool = new CountingPool(connection);

		using (var scope = pool.EnsureOpenScope())
			Assert.Equal(ConnectionState.Open, scope.PriorState);

		Assert.Equal(["give"], connection.Log);
	}

	[Fact]
	public static async Task APoolThatRestoresTheState_ClosesWhatTheScopeOpened_AndLeavesTheRest()
	{
		// The pool a single connection makes (DbConnectionProvider): giving back closes it
		// again if it was found closed, and leaves it open if it was found open.
		var closed = new ConnectionScopeTests.ScopeConnection();
		var pool = Open.Database.Extensions.Core.DbConnectionProvider.Create(closed);

		using (pool.EnsureOpenScope())
			Assert.Equal(ConnectionState.Open, closed.State);

		Assert.Equal(ConnectionState.Closed, closed.State);

		var open = new ConnectionScopeTests.ScopeConnection();
		open.SetState(ConnectionState.Open);
		pool = Open.Database.Extensions.Core.DbConnectionProvider.Create(open);

		await using (await pool.EnsureOpenScopeAsync(Ct)) { }

		Assert.Equal(ConnectionState.Open, open.State);
		Assert.Equal(["open", "close"], closed.Log);
		Assert.Empty(open.Log);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task APool_WhenOpenThrows_GivesTheConnectionBackOnce_AndRethrows(
		bool async)
	{
		var connection = Failing();
		var pool = new CountingPool(connection);

		var ex = async
			? await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await pool.EnsureOpenScopeAsync(Ct))
			: Assert.Throws<InvalidOperationException>(() => pool.EnsureOpenScope());

		Assert.Equal("open failed", ex.Message);
		Assert.Equal(1, pool.Takes);
		Assert.Equal(1, pool.Gives);
		Assert.Equal(async ? ["openAsync", "give"] : ["open", "give"], connection.Log);
	}

	[Fact]
	public static async Task APool_WhenCancelled_GivesTheConnectionBack_AndNothingIsOpened()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var pool = new CountingPool(connection);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await pool.EnsureOpenScopeAsync(cts.Token));

		Assert.Equal(1, pool.Takes);
		Assert.Equal(1, pool.Gives);
		Assert.Equal(["give"], connection.Log);
	}

	[Fact]
	public static async Task APool_WhenCancelledWhileOpening_ClosesAndGivesTheConnectionBack()
	{
		using var cts = new CancellationTokenSource();
		var connection = new ConnectionScopeTests.ScopeConnection { CancelWhenOpened = cts };
		var pool = new CountingPool(connection);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await pool.EnsureOpenScopeAsync(cts.Token));

		Assert.Equal(1, pool.Takes);
		Assert.Equal(1, pool.Gives);
		Assert.Equal(["openAsync", "close", "give"], connection.Log);
	}

	[Fact]
	public static void APool_ASecondDisposeOfACopy_GivesBackAgain()
	{
		// The documented hazard of a readonly struct: pinned so a change is deliberate.
		var pool = new CountingPool(new ConnectionScopeTests.ScopeConnection());
		var scope = pool.EnsureOpenScope();
		var copy = scope;

		scope.Dispose();
		copy.Dispose();

		Assert.Equal(2, pool.Gives);
	}

	// ---- the Open / Using callbacks are these scopes -------------------------------------

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task PoolOpen_Callback_WhenOpenThrows_GivesBackOnce_AndNeverRunsTheAction(
		bool async)
	{
		var connection = Failing();
		var pool = new CountingPool(connection);
		bool ran = false;

		if (async)
		{
			await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await pool.OpenAsync(
					(__, _) =>
					{
						ran = true;
						return default;
					},
					Ct));
		}
		else
		{
			Assert.Throws<InvalidOperationException>(
				() => pool.Open((__, _) => ran = true));
		}

		Assert.False(ran);
		Assert.Equal(1, pool.Takes);
		Assert.Equal(1, pool.Gives);
	}

	[Fact]
	public static async Task PoolOpen_Callback_GivesTheConnectionAndThePriorState()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var pool = new CountingPool(connection);

		ConnectionState prior = await pool.OpenAsync(
			(c, state) =>
			{
				Assert.Same(connection, c);
				Assert.Equal(ConnectionState.Open, c.State);
				return new ValueTask<ConnectionState>(state);
			},
			Ct);

		Assert.Equal(ConnectionState.Closed, prior);
		Assert.Equal(["openAsync", "give"], connection.Log);
		Assert.Equal(1, pool.Gives);
	}

	[Fact]
	public static async Task FactoryOpen_Callbacks_DisposeSynchronously_AndAsynchronously()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var factory = new CountingFactory(connection);

		factory.Open(c => Assert.Equal(ConnectionState.Open, c.State));
		connection.SetState(ConnectionState.Closed);
		await factory.OpenAsync(_ => default, Ct);

		Assert.Equal(["open", "dispose", "openAsync", "disposeAsync"], connection.Log);
	}

	[Fact]
	public static async Task FactoryOpen_Callback_WhenOpenThrows_DisposesTheConnection()
	{
		var connection = Failing();

		Assert.Throws<InvalidOperationException>(
			() => new CountingFactory(connection).Open(_ => { }));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await new CountingFactory(connection).OpenAsync(_ => default, Ct));

		Assert.Equal(["open", "dispose", "openAsync", "disposeAsync"], connection.Log);
	}

	[Fact]
	public static async Task FuncOpen_Callbacks_OpenAndDispose()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		Func<ConnectionScopeTests.ScopeConnection> create = () => connection;

		create.Open(c => Assert.Equal(ConnectionState.Open, c.State));
		connection.SetState(ConnectionState.Closed);
		await create.OpenAsync(_ => default, Ct);

		Assert.Equal(["open", "dispose", "openAsync", "disposeAsync"], connection.Log);
	}

	[Fact]
	public static async Task PoolUsing_NeverOpens_AndGivesBackOnce()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();
		var pool = new CountingPool(connection);

		pool.Using(c => Assert.Equal(ConnectionState.Closed, c.State));
		await pool.UsingAsync(_ => default);
		Assert.Throws<NotSupportedException>(
			() => pool.Using(_ => throw new NotSupportedException()));

		// Not opened, so nothing to close; given back each time, also when the action throws.
		Assert.Equal(3, pool.Takes);
		Assert.Equal(3, pool.Gives);
		Assert.Equal(["give", "give", "give"], connection.Log);
	}
	// ---- one name for a connection, a factory and a pool --------------------------------

	[Fact]
	public static async Task EnsureOpenScope_BindsToTheRightSource_ForEveryReceiver()
	{
		var connection = new ConnectionScopeTests.ScopeConnection();

		// A connection (the generic form keeps its type) ...
		ConnectionScope<ConnectionScopeTests.ScopeConnection> ofConnection = connection.EnsureOpenScope();
		ofConnection.Dispose();

		// ... a factory, typed and not ...
		var factory = new CountingFactory(connection);
		connection.SetState(ConnectionState.Closed);
		ConnectionScope<ConnectionScopeTests.ScopeConnection> ofFactory = factory.EnsureOpenScope();
		ofFactory.Dispose();
		IDbConnectionFactory untypedFactory = factory;
		connection.SetState(ConnectionState.Closed);
		ConnectionScope<IDbConnection> ofUntypedFactory = untypedFactory.EnsureOpenScope();
		ofUntypedFactory.Dispose();
		connection.SetState(ConnectionState.Closed);
		var adapterPool = new DbConnectionFactory<ConnectionScopeTests.ScopeConnection>(() => connection).AsPool();
		ConnectionScope<ConnectionScopeTests.ScopeConnection> ofAdapter = adapterPool.EnsureOpenScope();
		ofAdapter.Dispose();

		// ... and a pool, typed and not.
		var pool = new CountingPool(connection);
		connection.SetState(ConnectionState.Closed);
		ConnectionScope<ConnectionScopeTests.ScopeConnection> ofPool = pool.EnsureOpenScope();
		ofPool.Dispose();
		IDbConnectionPool untypedPool = pool;
		connection.SetState(ConnectionState.Closed);
		ConnectionScope<IDbConnection> ofUntypedPool = await untypedPool.EnsureOpenScopeAsync(Ct);
		await ofUntypedPool.DisposeAsync();

		// Each ended the way its source calls for: closed (the caller's connection),
		// disposed (a factory's), or given back (a pool's).
		Assert.Equal(
			[
				"open", "close",
				"open", "dispose",
				"open", "dispose",
				"open", "dispose",
				"open", "give",
				"openAsync", "give",
			],
			connection.Log);
	}
	// ---- shared ------------------------------------------------------------------------

	[Fact]
	public static async Task ASourceThatIsNull_IsRefused()
	{
		IDbConnectionFactory factory = null!;
		IDbConnectionPool pool = null!;

		Assert.Throws<ArgumentNullException>(() => factory.EnsureOpenScope());
		Assert.Throws<ArgumentNullException>(() => pool.EnsureOpenScope());
		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await factory.EnsureOpenScopeAsync(Ct));
		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await pool.EnsureOpenScopeAsync(Ct));
	}

	[Fact]
	public static async Task ADefaultScope_FromAFactoryOrPool_IsInert()
	{
		ConnectionScope<ConnectionScopeTests.ScopeConnection> scope = default;

		Assert.Null(Record.Exception(scope.Dispose));
		await scope.DisposeAsync();
	}
}
