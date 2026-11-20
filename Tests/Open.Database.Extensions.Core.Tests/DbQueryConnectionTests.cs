using System.Data.Common;
using System.Reflection;

namespace Open.Database.Extensions.Tests;

#nullable enable

// Who owns the connection of a DbQuery run, on a fake provider whose every
// open, close and dispose is counted, and whose connection can break.
// A transaction's connection is never opened, closed or reopened by the query.
// A connection that needed opening (closed, or broken: EnsureOpen closes a
// broken one and opens it again) is closed again afterwards, by the query and by
// the command builder alike. A factory connection is disposed and a pool
// connection is given back, also when the run throws.
[ExcludeFromCodeCoverage]
public static class DbQueryConnectionTests
{
	static readonly DbQuery Query = DbQuery.Text("select 1");

	static async Task Run(IExecuteReader source, bool async)
	{
		if (async)
		{
			await source.ExecuteReaderAsync(
				static r => new ValueTask<int>(r.FieldCount));
		}
		else
		{
			source.ExecuteReader(static r => r.FieldCount);
		}
	}

	static async Task Run(DbQueryUsing source, bool async)
		=> await Run((IExecuteReader)source, async);

	static (DbTally Tally, TrackedConnection Connection, TrackedTransaction Transaction) Transaction()
	{
		var tally = new DbTally();
		TrackedConnection connection = tally.NewOpenConnection();
		var transaction = (TrackedTransaction)connection.BeginTransaction();
		return (tally, connection, transaction);
	}

	// ---- a transaction's connection is the caller's ----

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ATransaction_NeverOpensOrClosesItsConnection_AndAsksNoCloseConnection(
		bool async)
	{
		var (tally, connection, transaction) = Transaction();

		await Run(Query.Using(transaction), async);

		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Equal(0, connection.Opens);
		Assert.Equal(0, connection.Closes);
		Assert.Equal(0, connection.Disposes);
		TrackedReader reader = Assert.Single(tally.Readers);
		Assert.Equal(CommandBehavior.Default, reader.Behavior);
		Assert.Same(transaction, Assert.Single(tally.Commands).Transaction);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ACompletedTransaction_IsRefused(bool async)
	{
		var (tally, connection, transaction) = Transaction();
		transaction.Commit();

		InvalidOperationException ex
			= await Assert.ThrowsAsync<InvalidOperationException>(
				() => Run(Query.Using(transaction), async));

		Assert.Contains("completed", ex.Message);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Equal(0, connection.Opens);
		Assert.Equal(0, connection.Closes);
		Assert.Empty(tally.Commands);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ATransactionWhoseConnectionWasClosed_IsRefused_NotReopened(
		bool async)
	{
		var (tally, connection, transaction) = Transaction();
		connection.Close();
		connection.ResetCounters();

		InvalidOperationException ex
			= await Assert.ThrowsAsync<InvalidOperationException>(
				() => Run(Query.Using(transaction), async));

		Assert.Contains("not open", ex.Message);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(0, connection.Opens);
		Assert.Equal(0, connection.Closes);
		Assert.Empty(tally.Commands);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ATransactionWhoseConnectionIsBroken_IsRefused_NotReopened(
		bool async)
	{
		var (tally, connection, transaction) = Transaction();
		connection.Break();

		InvalidOperationException ex
			= await Assert.ThrowsAsync<InvalidOperationException>(
				() => Run(Query.Using(transaction), async));

		Assert.Contains("not open", ex.Message);
		Assert.Equal(ConnectionState.Broken, connection.State);
		Assert.Equal(0, connection.Opens);
		Assert.Equal(0, connection.Closes);
		Assert.Empty(tally.Commands);
	}

	// ---- a broken connection that was reopened is closed again ----

	static void ClosedAgain(TrackedConnection connection)
	{
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(1, connection.Opens);
		Assert.True(connection.Closes >= 2); // once to reopen it, once when finished
		Assert.Equal(0, connection.Disposes);
	}

	static TrackedConnection Broken(DbTally tally)
	{
		TrackedConnection connection = tally.NewOpenConnection();
		connection.Break();
		return connection;
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task Query_ABrokenConnection_IsClosedAgain(bool async)
	{
		var tally = new DbTally();
		TrackedConnection connection = Broken(tally);

		await Run(Query.Using(connection), async);

		ClosedAgain(connection);
		Assert.True(
			Assert.Single(tally.Readers).Behavior.HasFlag(CommandBehavior.CloseConnection));
	}

	sealed class BrokenPool(DbTally tally) : IDbConnectionPool<TrackedConnection>
	{
		public List<TrackedConnection> Given { get; } = [];

		public TrackedConnection Take() => Broken(tally);

		IDbConnection IDbConnectionPool.Take() => Take();

		public void Give(IDbConnection connection)
			=> Given.Add((TrackedConnection)connection);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task Query_ABrokenPoolConnection_IsGivenBackClosed(bool async)
	{
		var tally = new DbTally();
		var pool = new BrokenPool(tally);

		await Run(Query.Using(pool), async);

		TrackedConnection connection = Assert.Single(pool.Given);
		ClosedAgain(connection);
	}

	[Fact]
	public static async Task Builder_Execute_ABrokenConnection_IsClosedAgain()
	{
		var tally = new DbTally();
		TrackedConnection connection = Broken(tally);
		connection.Command("select 1").Execute(c => c.CommandText = "select 2");

		ClosedAgain(connection);

		TrackedConnection again = Broken(tally);
		await again.Command("select 1").ExecuteAsync(static _ => default);

		ClosedAgain(again);
	}

	[Fact]
	public static void Builder_ExecuteReader_ABrokenConnection_IsClosedAgain()
	{
		var tally = new DbTally();
		TrackedConnection connection = Broken(tally);

		((IExecuteReader)connection.Command("select 1"))
			.ExecuteReader(static r => r.FieldCount);

		ClosedAgain(connection);
		Assert.True(
			Assert.Single(tally.Readers).Behavior.HasFlag(CommandBehavior.CloseConnection));
	}

	[Fact]
	public static async Task Builder_ExecuteReaderAsync_ABrokenConnection_IsClosedAgain()
	{
		var tally = new DbTally();
		TrackedConnection connection = Broken(tally);

		await ((IExecuteReader)connection.Command("select 1"))
			.ExecuteReaderAsync(static r => new ValueTask<int>(r.FieldCount));

		ClosedAgain(connection);
		Assert.True(
			Assert.Single(tally.Readers).Behavior.HasFlag(CommandBehavior.CloseConnection));
	}

	// ---- the source is let go when the run throws ----

	static DbTally Failing()
		=> new(new() { ExecuteFailure = new TimeoutException("The provider failed.") });

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task AFactoryConnection_IsDisposed_WhenTheRunThrows(bool async)
	{
		DbTally tally = Failing();

		await Assert.ThrowsAsync<TimeoutException>(
			() => Run(Query.Using(tally.Factory), async));

		TrackedConnection connection = Assert.Single(tally.Connections);
		Assert.Equal(1, connection.Disposes);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(connection.Opens, connection.Closes);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task APoolConnection_IsGivenBack_WhenTheRunThrows(bool async)
	{
		DbTally tally = Failing();
		TrackedPool pool = tally.Pool;

		await Assert.ThrowsAsync<TimeoutException>(
			() => Run(Query.Using(pool), async));

		Assert.Equal(1, pool.Takes);
		Assert.Equal(1, pool.Gives);
		TrackedConnection connection = Assert.Single(tally.Connections);
		Assert.Equal(0, connection.Disposes);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	// ---- the source arguments are checked by the call that takes them ----

	[Fact]
	public static void Using_ANullSource_NamesItsParameter()
	{
		DbQuery<Values> typed = Query.Params<Values>();
		var tally = new DbTally();

		Assert.Equal(
			"connection",
			Assert.Throws<ArgumentNullException>(
				() => Query.Using((DbConnection)null!)).ParamName);
		Assert.Equal(
			"transaction",
			Assert.Throws<ArgumentNullException>(
				() => Query.Using((DbTransaction)null!)).ParamName);
		Assert.Equal(
			"pool",
			Assert.Throws<ArgumentNullException>(
				() => Query.Using((IDbConnectionPool<TrackedConnection>)null!)).ParamName);
		Assert.Equal(
			"factory",
			Assert.Throws<ArgumentNullException>(
				() => Query.Using((IDbConnectionFactory<TrackedConnection>)null!)).ParamName);

		Assert.Equal(
			"connection",
			Assert.Throws<ArgumentNullException>(
				() => typed.Using((DbConnection)null!)).ParamName);
		Assert.Equal(
			"transaction",
			Assert.Throws<ArgumentNullException>(
				() => typed.Using((DbTransaction)null!)).ParamName);
		Assert.Equal(
			"pool",
			Assert.Throws<ArgumentNullException>(
				() => typed.Using((IDbConnectionPool<TrackedConnection>)null!)).ParamName);
		Assert.Equal(
			"factory",
			Assert.Throws<ArgumentNullException>(
				() => typed.Using((IDbConnectionFactory<TrackedConnection>)null!)).ParamName);

		Assert.Empty(tally.Connections);
	}

	// ---- a repeat typed call matches no names ----

	readonly record struct Values(int Id, string Name);

	const string ValuesSql = "select @id, @name";

	static readonly DbQuery<Values> ValuesQuery
		= DbQuery.Text(ValuesSql).Params<Values>();

	const BindingFlags Hidden
		= BindingFlags.Instance | BindingFlags.NonPublic;

	// Runs it on another thread while this one holds the lock; whether it finished.
	static bool CompletesWhileHeld(object gate, Func<Task> run)
	{
		lock (gate)
		{
			return Task.Run(run).Wait(TimeSpan.FromSeconds(10));
		}
	}

	// What a definition remembers of the members it matched (internal).
	static object?[] Remembered(DbQuery definition)
		=> (object?[]?)typeof(DbQuery)
			.GetProperty("Matches", Hidden)!
			.GetValue(definition) ?? [];

	[Fact]
	public static async Task ARepeatTypedCall_ReusesTheMatchedMembers_AndLooksUpNoName()
	{
		var tally = new DbTally();
		TrackedConnection connection = tally.NewOpenConnection();
		DbQuery definition = DbQuery.Text(ValuesSql);
		DbQueryCall call = definition
			.Params<Values>().Using(connection)
			.AddParams(new(7, "x"));

		await Run(call, async: false);
		object?[] first = Remembered(definition);
		object matched = Assert.Single(first)!;

		// Every name lookup of a text query takes this lock: held here, a run
		// that looked up a name would wait for it.
		object names = typeof(DbQuery)
			.GetProperty("TextNames", Hidden)!
			.GetValue(definition)!;
		object gate = names.GetType().GetField("_matches", Hidden)!.GetValue(names)!;
		Assert.True(
			CompletesWhileHeld(gate, () => Run(call, async: false)),
			"The second run waited for a name lookup.");
		Assert.Same(matched, Assert.Single(Remembered(definition)));
		Assert.Equal(
			["@id", "@name"],
			tally.Commands[^1].Parameters.Cast<DbParameter>()
				.Select(static p => p.ParameterName));
	}
}
