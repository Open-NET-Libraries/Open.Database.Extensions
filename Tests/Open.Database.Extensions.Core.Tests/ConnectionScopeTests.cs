using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// EnsureOpenScope / EnsureOpenScopeAsync open a connection if it needs it and
// return a scope that puts it back: closed again if this call opened it, left
// open if it was already open, never disposed.
[ExcludeFromCodeCoverage]
public static class ConnectionScopeTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A DbConnection that tells Close from CloseAsync, and can fail to open.
	internal sealed class ScopeConnection : DbConnection
	{
		ConnectionState _state = ConnectionState.Closed;

		public List<string> Log { get; } = [];

		public bool WasDisposed { get; private set; }

		public Exception? OpenException { get; set; }

		public Exception? CloseException { get; set; }

		public void SetState(ConnectionState state) => _state = state;

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "scope";
		public override string DataSource => "scope";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => _state;
		public override void ChangeDatabase(string databaseName) { }

		public override void Open()
		{
			Log.Add("open");
			if (OpenException is not null) throw OpenException;

			_state = ConnectionState.Open;
		}

		public override Task OpenAsync(CancellationToken cancellationToken)
		{
			Log.Add("openAsync");
			if (OpenException is not null) throw OpenException;

			_state = ConnectionState.Open;
			return Task.CompletedTask;
		}

		public override void Close()
		{
			Log.Add("close");
			_state = ConnectionState.Closed;
			if (CloseException is not null) throw CloseException;
		}

		public override Task CloseAsync()
		{
			Log.Add("closeAsync");
			_state = ConnectionState.Closed;
			return Task.CompletedTask;
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing) Log.Add("dispose");
			WasDisposed = true;
			base.Dispose(disposing);
		}

		public override ValueTask DisposeAsync()
		{
			Log.Add("disposeAsync");
			WasDisposed = true;
			return default;
		}

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
			=> throw new NotSupportedException();

		protected override DbCommand CreateDbCommand()
			=> throw new NotSupportedException();
	}

	// An IDbConnection that is not a DbConnection: no async at all.
	internal sealed class PlainConnection : IDbConnection
	{
		public List<string> Log { get; } = [];

		public bool WasDisposed { get; private set; }

		[AllowNull]
		public string ConnectionString { get; set; } = "";
		public int ConnectionTimeout => 0;
		public string Database => "plain";
		public ConnectionState State { get; private set; } = ConnectionState.Closed;
		public IDbTransaction BeginTransaction() => throw new NotSupportedException();
		public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotSupportedException();
		public void ChangeDatabase(string databaseName) { }
		public IDbCommand CreateCommand() => throw new NotSupportedException();

		public void Open()
		{
			Log.Add("open");
			State = ConnectionState.Open;
		}

		public void Close()
		{
			Log.Add("close");
			State = ConnectionState.Closed;
		}

		public void Dispose() => WasDisposed = true;
	}

	// ---- a closed connection: opened, then closed again ----------------------------

	[Fact]
	public static void AClosedConnection_IsOpenedByTheScope_AndClosedWhenItIsDisposed()
	{
		var connection = new ScopeConnection();

		using (var scope = connection.EnsureOpenScope())
		{
			Assert.Equal(ConnectionState.Open, connection.State);
			Assert.Equal(ConnectionState.Closed, scope.PriorState);
			Assert.Same(connection, scope.Connection);
		}

		Assert.Equal(["open", "close"], connection.Log);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.False(connection.WasDisposed);
	}

	[Fact]
	public static async Task AClosedConnection_Async_IsOpenedWithOpenAsync_AndClosedWithCloseAsync()
	{
		var connection = new ScopeConnection();

		await using (var scope = await connection.EnsureOpenScopeAsync(Ct))
		{
			Assert.Equal(ConnectionState.Open, connection.State);
			Assert.Equal(ConnectionState.Closed, scope.PriorState);
		}

		Assert.Equal(["openAsync", "closeAsync"], connection.Log);
		Assert.False(connection.WasDisposed);
	}

	[Fact]
	public static async Task AScopeMadeAsync_AndDisposedSynchronously_UsesClose()
	{
		var connection = new ScopeConnection();

		var scope = await connection.EnsureOpenScopeAsync(Ct);
		scope.Dispose();

		Assert.Equal(["openAsync", "close"], connection.Log);
	}

	[Fact]
	public static async Task APlainConnection_HasNoAsync_ButStillOpensAndCloses()
	{
		var connection = new PlainConnection();

		await using (var scope = await connection.EnsureOpenScopeAsync(Ct))
			Assert.Equal(ConnectionState.Open, connection.State);

		Assert.Equal(["open", "close"], connection.Log);
		Assert.False(connection.WasDisposed);
	}

	// ---- an open connection: left open ----------------------------------------------

	[Theory]
	[InlineData(ConnectionState.Open)]
	[InlineData(ConnectionState.Open | ConnectionState.Executing)]
	[InlineData(ConnectionState.Open | ConnectionState.Fetching)]
	public static async Task AnOpenConnection_IsLeftOpen_ByBothDisposes(
		ConnectionState state)
	{
		var connection = new ScopeConnection();
		connection.SetState(state);

		using (var scope = connection.EnsureOpenScope())
			Assert.Equal(state, scope.PriorState);

		await using (await connection.EnsureOpenScopeAsync(Ct)) { }

		Assert.Empty(connection.Log);
		Assert.Equal(state, connection.State);
	}

	// ---- a broken connection: closed and opened again, so closed again --------------

	[Fact]
	public static void ABrokenConnection_IsClosedFirst_OpenedAgain_AndClosedAfterwards()
	{
		var connection = new ScopeConnection();
		connection.SetState(ConnectionState.Broken);

		using (var scope = connection.EnsureOpenScope())
		{
			Assert.Equal(ConnectionState.Broken, scope.PriorState);
			Assert.Equal(ConnectionState.Open, connection.State);
		}

		Assert.Equal(["close", "open", "close"], connection.Log);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static async Task ABrokenOpenConnection_IsClosedAgainAfterwards_Async()
	{
		var connection = new ScopeConnection();
		connection.SetState(ConnectionState.Open | ConnectionState.Broken);

		await using (await connection.EnsureOpenScopeAsync(Ct)) { }

		Assert.Equal(["close", "openAsync", "closeAsync"], connection.Log);
	}

	// ---- default, and what the scope never does --------------------------------------

	[Fact]
	public static async Task ADefaultScope_DisposesToNothing_AndHasNoConnection()
	{
		ConnectionScope<ScopeConnection> scope = default;

		scope.Dispose();
		await scope.DisposeAsync();

		Assert.Throws<InvalidOperationException>(() => scope.Connection);
		Assert.Throws<InvalidOperationException>(() =>
		{
			(ScopeConnection connection, ConnectionState state) = scope;
			return connection;
		});
		Assert.Equal(ConnectionState.Closed, scope.PriorState);
	}

	[Fact]
	public static void ADeconstructedScope_GivesTheConnectionAndThePriorState()
	{
		var connection = new ScopeConnection();
		using var scope = connection.EnsureOpenScope();

		(ScopeConnection held, ConnectionState was) = scope;

		Assert.Same(connection, held);
		Assert.Equal(ConnectionState.Closed, was);
		Assert.Equal(scope.PriorState, was);
	}

	[Fact]
	public static void NestedScopes_TheInnerLeavesItOpen_TheOuterClosesIt()
	{
		var connection = new ScopeConnection();

		using (connection.EnsureOpenScope())
		{
			using (var inner = connection.EnsureOpenScope())
				Assert.Equal(ConnectionState.Open, inner.PriorState);

			Assert.Equal(ConnectionState.Open, connection.State);
		}

		Assert.Equal(["open", "close"], connection.Log);
	}

	[Fact]
	public static void ASecondDisposeOfACopy_ClosesAConnectionSomeoneOpenedInBetween()
	{
		// The documented hazard of a readonly struct: it cannot remember that it
		// was disposed. Pinned, so that a change to it is deliberate.
		var connection = new ScopeConnection();
		var scope = connection.EnsureOpenScope();
		var copy = scope;

		scope.Dispose();
		connection.Open();
		copy.Dispose();

		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(["open", "close", "open", "close"], connection.Log);
	}

	[Fact]
	public static void ASecondDispose_OfAClosedConnection_IsHarmless()
	{
		var connection = new ScopeConnection();
		var scope = connection.EnsureOpenScope();

		scope.Dispose();
		Assert.Null(Record.Exception(scope.Dispose));
		Assert.False(connection.WasDisposed);
	}

	// ---- the connection keeps its type; the scope may be ignored ------------------------

	[Fact]
	public static async Task TheScope_KeepsTheTypeOfTheConnection_ItWasCalledOn()
	{
		var connection = new ScopeConnection();

		await using var scope = await connection.EnsureOpenScopeAsync(Ct);
		ScopeConnection same = scope.Connection;
		var (held, was) = scope;

		Assert.Same(connection, same);
		Assert.Same(connection, held);
		Assert.Equal(ConnectionState.Closed, was);
	}

	public static IDbConnection Create(Func<IDbConnection> make) => make();

	[Fact]
	public static async Task AConnection_HeldAsTheInterface_StillGetsAScope()
	{
		// Held as the interface, as a caller who was handed one would hold it.
		IDbConnection connection = Create(() => new ScopeConnection());

		await using ConnectionScope<IDbConnection> scope
			= await connection.EnsureOpenScopeAsync(Ct);

		Assert.Same(connection, scope.Connection);
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Fact]
	public static async Task TheScope_CanBeIgnored_AndStillClosesTheConnection()
	{
		var connection = new ScopeConnection();

		await using (await connection.EnsureOpenScopeAsync(Ct))
			Assert.Equal(ConnectionState.Open, connection.State);

		using (connection.EnsureOpenScope())
			Assert.Equal(ConnectionState.Open, connection.State);

		Assert.Equal(
			["openAsync", "closeAsync", "open", "close"],
			connection.Log);
	}
	// ---- failures ---------------------------------------------------------------------

	[Fact]
	public static void ACloseThatThrows_PropagatesFromDispose()
	{
		var connection = new ScopeConnection
		{
			CloseException = new InvalidOperationException("close failed"),
		};
		var scope = connection.EnsureOpenScope();

		var ex = Assert.Throws<InvalidOperationException>(scope.Dispose);

		Assert.Equal("close failed", ex.Message);
		Assert.False(connection.WasDisposed);
	}

	[Fact]
	public static async Task AnOpenThatThrows_ReturnsNoScope_AndDoesNotDisposeTheConnection()
	{
		var connection = new ScopeConnection
		{
			OpenException = new InvalidOperationException("open failed"),
		};

		Assert.Throws<InvalidOperationException>(
			() => connection.EnsureOpenScope());
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await connection.EnsureOpenScopeAsync(Ct));

		Assert.False(connection.WasDisposed);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static async Task ACancelledToken_ThrowsBeforeAnythingIsOpened()
	{
		var connection = new ScopeConnection();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await connection.EnsureOpenScopeAsync(cts.Token));

		Assert.Empty(connection.Log);
	}

	[Fact]
	public static async Task ANullConnection_IsRefused()
	{
		IDbConnection connection = null!;

		Assert.Throws<ArgumentNullException>(() => connection.EnsureOpenScope());
		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await connection.EnsureOpenScopeAsync(Ct));
	}

	[Fact]
	public static async Task TheConnection_IsStillUsable_AfterTheScope()
	{
		var connection = new ScopeConnection();

		await using (await connection.EnsureOpenScopeAsync(Ct)) { }
		await using (await connection.EnsureOpenScopeAsync(Ct)) { }

		Assert.Equal(
			["openAsync", "closeAsync", "openAsync", "closeAsync"],
			connection.Log);
		Assert.False(connection.WasDisposed);
	}
}
