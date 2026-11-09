extern alias mssql;

using Microsoft.Data.SqlClient;
using Expressive = mssql::Open.Database.Extensions.ExpressiveSqlCommand;
using SqlCmd = mssql::Open.Database.Extensions.SqlCommandExtensions;
using SqlTx = mssql::Open.Database.Extensions.SqlTransactionExtensions;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The SQL Server package (Open.Database.Extensions.MSSqlClient, Microsoft.Data.SqlClient) keeps typed copies of Core's command, parameter and transaction code so that SqlCommand, SqlDataReader and SqlTransaction flow through.
// Core's copies are tested with fakes; SqlConnection and SqlTransaction are concrete, so a fake cannot stand in for these. The tests that need no server (a SqlCommand and its parameters never touch a connection) run everywhere.
// The tests that need one run on SQL Server LocalDB in tempdb (LocalDbFixture) and are reported as skipped when LocalDB is missing, so the suite stays green on a machine or build agent without it.
[ExcludeFromCodeCoverage]
public static class SqlServerPackageTests
{
	[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors", Justification = "Test Only")]
	sealed class ProbeException : Exception;

	// A connection that is never opened: nothing here touches the network.
	static CancellationToken Token => TestContext.Current.CancellationToken;

	const string UnopenedConnectionString = "Server=.;Integrated Security=true;Encrypt=False";

	// ---- ExpressiveSqlCommand.AddParams: a three-way branch (value, no value, and which type wins) on a real SqlCommand ----

	// AddParams is protected and the lookup of the procedure's parameter names is a protected override: a subclass is how to reach them without a server.
	sealed class Probe : Expressive
	{
		public Probe(SqlConnection connection) : base(connection, CommandType.StoredProcedure, "dbo.p") { }

		public Probe(SqlConnection connection, SqlTransaction transaction) : base(connection, transaction, CommandType.StoredProcedure, "dbo.p") { }

		public void AddTo(SqlCommand command) => AddParams(command);

		public ImmutableArray<string> Names(SqlConnection connection, string procedure) => GetProcedureParameterNames(connection, procedure);

		public ValueTask<ImmutableArray<string>> NamesAsync(SqlConnection connection, string procedure, CancellationToken cancellationToken)
			=> GetProcedureParameterNamesAsync(connection, procedure, cancellationToken);
	}

	// What the command would send for the parameters the callback adds to the expressive command.
	static SqlParameter[] Sent(Action<Probe> add)
	{
		using var connection = new SqlConnection(UnopenedConnectionString);
		var probe = new Probe(connection);
		add(probe);
		using var command = new SqlCommand();
		probe.AddTo(command);
		return [.. command.Parameters.Cast<SqlParameter>()];
	}

	[Fact]
	public static void AddParams_AValue_IsAnInputParameterTheProviderTypes()
	{
		SqlParameter p = Assert.Single(Sent(c => c.AddParam("@n", 5)));

		Assert.Equal("@n", p.ParameterName);
		Assert.Equal(5, p.Value);
		Assert.Equal(ParameterDirection.Input, p.Direction);
		Assert.Equal(SqlDbType.Int, p.SqlDbType);
	}

	[Fact]
	public static void AddParams_NoValue_StillReachesTheCommandWithItsTypeAndDirection()
	{
		SqlParameter[] sent = Sent(c => c
			.AddParam("@o", SqlDbType.Int, ParameterDirection.Output)
			.AddParam("@r", SqlDbType.BigInt, ParameterDirection.ReturnValue));

		Assert.Equal(["@o", "@r"], sent.Select(p => p.ParameterName));
		Assert.All(sent, p => Assert.Null(p.Value));
		Assert.Equal([SqlDbType.Int, SqlDbType.BigInt], sent.Select(p => p.SqlDbType));
		Assert.Equal([ParameterDirection.Output, ParameterDirection.ReturnValue], sent.Select(p => p.Direction));
	}

	[Fact]
	public static void AddParams_InputOutput_KeepsItsValueAndDirection()
	{
		SqlParameter p = Assert.Single(Sent(c => c.AddParam("@io", 7, ParameterDirection.InputOutput)));

		Assert.Equal(7, p.Value);
		Assert.Equal(ParameterDirection.InputOutput, p.Direction);
	}

	[Fact]
	public static void AddParams_ANullFromANullableArgument_IsADatabaseNullOfThatType()
	{
		int? count = null;
		char? letter = null;
		Guid? id = null;

		SqlParameter[] sent = Sent(c => c.AddParam(count).AddParam(letter).AddParam(id));

		Assert.Equal(["count", "letter", "id"], sent.Select(p => p.ParameterName));
		Assert.All(sent, p => Assert.Equal(DBNull.Value, p.Value));
		Assert.Equal([DbType.Int32, DbType.StringFixedLength, DbType.Guid], sent.Select(p => p.DbType));
		Assert.Equal([SqlDbType.Int, SqlDbType.NChar, SqlDbType.UniqueIdentifier], sent.Select(p => p.SqlDbType));
	}

	[Fact]
	public static void AddParams_ADatabaseNullWithNoClrType_LeavesTheTypeToTheProvider()
	{
		SqlParameter p = Assert.Single(Sent(c => c.AddParam("@d", DBNull.Value)));

		Assert.Equal(DBNull.Value, p.Value);
		Assert.Equal(new SqlParameter("@d", DBNull.Value).DbType, p.DbType);
	}

	[Fact]
	public static void AddParams_AnExplicitType_WinsOverWhatTheValueOrTheClrTypeWouldGive()
	{
		SqlParameter[] sent = Sent(c => c
			.AddParam("@big", 5, SqlDbType.BigInt)                     // the provider would infer Int from the value
			.AddParam("@null", DBNull.Value, SqlDbType.SmallInt));     // and a database null has no type of its own

		Assert.Equal([SqlDbType.BigInt, SqlDbType.SmallInt], sent.Select(p => p.SqlDbType));
		Assert.Equal(5, sent[0].Value);
		Assert.Equal(DBNull.Value, sent[1].Value);
	}

	[Fact]
	public static void AddParams_KeepsTheOrderTheParametersWereAdded()
		=> Assert.Equal(["@c", "@a", "@b", "@o"], Sent(c => c
			.AddParam("@c", 1)
			.AddParam("@a", 2)
			.AddParam("@b", "three")
			.AddParam("@o", SqlDbType.Int, ParameterDirection.Output)).Select(p => p.ParameterName));

	[Fact]
	public static void AddParams_ANullCommand_ThrowsArgumentNull()
	{
		using var connection = new SqlConnection(UnopenedConnectionString);

		Assert.Equal("command", Assert.Throws<ArgumentNullException>(() => new Probe(connection).AddTo(null!)).ParamName);
	}

	// ---- SqlCommandExtensions.AddParameter and AddParameterType: what decides whether a parameter is added ----

	[Fact]
	public static void AddParameterType_ANullNameForAnInputParameter_ThrowsAndAddsNothing()
	{
		using var command = new SqlCommand();

		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => SqlCmd.AddParameterType(command, null, SqlDbType.Int));

		Assert.Equal("name", ex.ParamName);
		Assert.Empty(command.Parameters);
	}

	[Fact]
	public static void AddParameterType_ABlankNameIsRejectedForEveryDirection_AndAddsNothing()
	{
		using var command = new SqlCommand();

		foreach (ParameterDirection direction in new[] { ParameterDirection.Input, ParameterDirection.Output, ParameterDirection.ReturnValue })
		{
			ArgumentException ex = Assert.Throws<ArgumentException>(() => SqlCmd.AddParameterType(command, "  ", SqlDbType.Int, direction));
			Assert.Equal("name", ex.ParamName);
		}

		Assert.Empty(command.Parameters);
	}

	[Fact]
	public static void AddParameterType_ANullNameIsAllowedForTheReturnValue()
	{
		using var command = new SqlCommand();

		SqlParameter p = SqlCmd.AddReturnParameter(command, SqlDbType.Int);

		Assert.Same(p, Assert.Single(command.Parameters));
		Assert.Equal(ParameterDirection.ReturnValue, p.Direction);
		Assert.Equal(SqlDbType.Int, p.SqlDbType);
		Assert.StartsWith("Parameter", p.ParameterName); // the collection names a parameter that has no name
	}

	[Fact]
	public static void AddParameterType_SetsTheTypeAndDirection_AndReturnsTheParameterInTheCollection()
	{
		using var command = new SqlCommand();

		SqlParameter p = SqlCmd.AddParameterType(command, "@text", SqlDbType.NVarChar, ParameterDirection.InputOutput);

		Assert.Same(p, command.Parameters["@text"]);
		Assert.Equal(SqlDbType.NVarChar, p.SqlDbType);
		Assert.Equal(ParameterDirection.InputOutput, p.Direction);
		Assert.Null(p.Value);
	}

	[Fact]
	public static void AddParameterType_OnAnIDbCommandThatIsNotASqlCommand_ThrowsInvalidCast()
	{
		IDbCommand command = Substitute.For<IDbCommand>();

		Assert.Throws<InvalidCastException>(() => SqlCmd.AddParameterType(command, "@x", SqlDbType.Int));
		Assert.Throws<ArgumentNullException>(() => SqlCmd.AddParameterType((SqlCommand)null!, "@x", SqlDbType.Int));
	}

	[Fact]
	public static void AddParameter_AssignsTheValueAsGiven_SoANullStaysNullAndADatabaseNullStaysOne()
	{
		using var command = new SqlCommand();

		SqlParameter none = SqlCmd.AddParameter(command, "@none", null, SqlDbType.Int);
		SqlParameter database = SqlCmd.AddParameter(command, "@database", DBNull.Value, SqlDbType.Int);
		SqlParameter value = SqlCmd.AddParameter(command, "@value", 42, SqlDbType.BigInt, ParameterDirection.InputOutput);

		Assert.Equal(3, command.Parameters.Count);
		Assert.Null(none.Value);
		Assert.Equal(DBNull.Value, database.Value);
		Assert.Equal(42, value.Value);
		Assert.Equal(SqlDbType.BigInt, value.SqlDbType);
		Assert.Equal(ParameterDirection.InputOutput, value.Direction);
		Assert.Equal(ParameterDirection.Input, none.Direction);
	}

	[Fact]
	public static void AddParameter_ABlankName_ThrowsBeforeAnythingIsAdded()
	{
		using var command = new SqlCommand();

		Assert.Throws<ArgumentException>(() => SqlCmd.AddParameter(command, " ", 1, SqlDbType.Int));
		Assert.Throws<ArgumentNullException>(() => SqlCmd.AddParameter(command, null!, 1, SqlDbType.Int));
		Assert.Throws<ArgumentNullException>(() => SqlCmd.AddParameter(null!, "@x", 1, SqlDbType.Int));
		Assert.Empty(command.Parameters);
	}

	// ---- SqlCommandExtensions.ExecuteReader: it opens the connection it needs, and the reader's disposal closes it ----

	public enum Overload { Sync, SyncValue, Async, AsyncValueTask, AsyncValue, AsyncValueTaskValue }

	public static TheoryData<Overload> Overloads { get; } = new(Enum.GetValues<Overload>());

	// Runs the work on the reader through one of the six overloads.
	static async Task<int> Run(SqlCommand command, Overload overload, Func<SqlDataReader, int> work)
	{
		int result = 0;
		switch (overload)
		{
			case Overload.Sync:
				SqlCmd.ExecuteReader(command, (Action<SqlDataReader>)(r => result = work(r)));
				break;
			case Overload.SyncValue:
				result = SqlCmd.ExecuteReader(command, work);
				break;
			case Overload.Async:
				await SqlCmd.ExecuteReaderAsync(command, (Action<SqlDataReader>)(r => result = work(r)));
				break;
			case Overload.AsyncValueTask:
				await SqlCmd.ExecuteReaderAsync(command, (Func<SqlDataReader, ValueTask>)(r =>
				{
					result = work(r);
					return default;
				}));
				break;
			case Overload.AsyncValue:
				result = await SqlCmd.ExecuteReaderAsync(command, work);
				break;
			default:
				result = await SqlCmd.ExecuteReaderAsync(command, (Func<SqlDataReader, ValueTask<int>>)(r => new ValueTask<int>(work(r))));
				break;
		}

		return result;
	}

	const string ThreeRows = "select v from (values (1), (2), (3)) as t(v) order by v";

	static int ReadInto(SqlDataReader reader, List<int> seen)
	{
		while (reader.Read()) seen.Add(reader.GetInt32(0));
		return seen.Count;
	}

	[Theory]
	[MemberData(nameof(Overloads))]
	public static async Task ExecuteReader_AClosedConnection_IsOpenedForTheReader_ClosedAfterIt_AndTheCommandRunsAgain(Overload overload)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using SqlConnection connection = LocalDbFixture.Create();
		using var command = new SqlCommand(ThreeRows, connection);
		var seen = new List<int>();

		Assert.Equal(3, await Run(command, overload, r => ReadInto(r, seen)));
		Assert.Equal([1, 2, 3], seen);
		Assert.Equal(ConnectionState.Closed, connection.State);

		seen.Clear();
		Assert.Equal(3, await Run(command, overload, r => ReadInto(r, seen)));
		Assert.Equal([1, 2, 3], seen);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Theory]
	[MemberData(nameof(Overloads))]
	public static async Task ExecuteReader_AConnectionTheCallerOpened_StaysOpen(Overload overload)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using SqlConnection connection = LocalDbFixture.Create();
		connection.Open();
		using var command = new SqlCommand(ThreeRows, connection);
		var seen = new List<int>();

		Assert.Equal(3, await Run(command, overload, r => ReadInto(r, seen)));

		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Theory]
	[MemberData(nameof(Overloads))]
	public static async Task ExecuteReader_AHandlerThatThrows_PropagatesUnchanged_AndTheConnectionIsLeftAsItWas(Overload overload)
	{
		LocalDbFixture.SkipUnlessAvailable();
		var failure = new ProbeException();
		using SqlConnection connection = LocalDbFixture.Create();
		using var command = new SqlCommand(ThreeRows, connection);

		// Opened by the call: closed again, although the handler threw.
		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(() => Run(command, overload, _ => throw failure)));
		Assert.Equal(ConnectionState.Closed, connection.State);

		// Opened by the caller: left open.
		connection.Open();
		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(() => Run(command, overload, _ => throw failure)));
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	[Theory]
	[MemberData(nameof(Overloads))]
	public static async Task ExecuteReader_AStatementThatFailsToStart_ThrowsTheSqlException_AndDoesNotLeaveTheConnectionItOpenedOpen(Overload overload)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using SqlConnection connection = LocalDbFixture.Create();
		using var command = new SqlCommand($"select * from dbo.{LocalDbFixture.UniqueName("ode_missing")}", connection);

		// There is no reader to dispose when ExecuteReader itself throws, so nothing but the provider closes a connection that was opened for the call.
		await Assert.ThrowsAsync<SqlException>(() => Run(command, overload, _ => 0));
		Assert.Equal(ConnectionState.Closed, connection.State);

		// A connection the caller opened is not the call's to close.
		connection.Open();
		await Assert.ThrowsAsync<SqlException>(() => Run(command, overload, _ => 0));
		Assert.Equal(ConnectionState.Open, connection.State);
	}

	// A command with no connection: Core's EnsureOpen guards throw ArgumentNullException; in Debug builds the SQL Server package checks first and throws its own exception, a different type for the synchronous and the asynchronous overloads.
	// The tests pin what each configuration does today and that the handler never runs and no NullReferenceException escapes.
#if DEBUG
	static readonly Type NoConnectionSync = typeof(InvalidOperationException);
	static readonly Type NoConnectionAsync = typeof(ArgumentException);
#else
	static readonly Type NoConnectionSync = typeof(ArgumentNullException);
	static readonly Type NoConnectionAsync = typeof(ArgumentNullException);
#endif

	[Fact]
	public static void ExecuteReader_ACommandWithNoConnection_Throws_AndNeverCallsTheHandler()
	{
		using var command = new SqlCommand("select 1");
		bool called = false;

		Assert.Throws(NoConnectionSync, () => SqlCmd.ExecuteReader(command, (Action<SqlDataReader>)(_ => called = true)));
		Assert.Throws(NoConnectionSync, () => SqlCmd.ExecuteReader(command, (Func<SqlDataReader, int>)(_ =>
		{
			called = true;
			return 1;
		})));

		Assert.False(called);
	}

	[Fact]
	public static async Task ExecuteReaderAsync_ACommandWithNoConnection_Throws_AndNeverCallsTheHandler()
	{
		using var command = new SqlCommand("select 1");
		bool called = false;

		await Assert.ThrowsAsync(NoConnectionAsync, async () => await SqlCmd.ExecuteReaderAsync(command, (Action<SqlDataReader>)(_ => called = true), cancellationToken: Token));
		await Assert.ThrowsAsync(NoConnectionAsync, async () => await SqlCmd.ExecuteReaderAsync(command, (Func<SqlDataReader, ValueTask>)(_ =>
		{
			called = true;
			return default;
		}), cancellationToken: Token));
		await Assert.ThrowsAsync(NoConnectionAsync, async () => await SqlCmd.ExecuteReaderAsync(command, (Func<SqlDataReader, int>)(_ =>
		{
			called = true;
			return 1;
		}), cancellationToken: Token));
		await Assert.ThrowsAsync(NoConnectionAsync, async () => await SqlCmd.ExecuteReaderAsync(command, (Func<SqlDataReader, ValueTask<int>>)(_ =>
		{
			called = true;
			return new ValueTask<int>(1);
		}), cancellationToken: Token));

		Assert.False(called);
	}

	// ---- ExpressiveSqlCommand.GetProcedureParameterNames: the lookup that parameters added by their argument depend on ----

	public enum Flavor { Sync, Async }

	public static TheoryData<Flavor> Flavors { get; } = new(Enum.GetValues<Flavor>());

	static async Task<ImmutableArray<string>> NamesOf(Probe probe, SqlConnection connection, string procedure, Flavor flavor, CancellationToken cancellationToken)
		=> flavor == Flavor.Sync
			? probe.Names(connection, procedure)
			: await probe.NamesAsync(connection, procedure, cancellationToken);

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ProcedureParameterNames_AreTheDeclaredOnesInDeclarationOrderWithTheirPrefix(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = LocalDbFixture.CreateProcedure("@zeta int, @alpha varchar(10) = 'x', @mid int output");
		using SqlConnection connection = LocalDbFixture.Create();
		connection.Open();

		ImmutableArray<string> names = await NamesOf(new Probe(connection), connection, $"dbo.{procedure.Name}", flavor, Token);

		Assert.Equal(["@zeta", "@alpha", "@mid"], names); // not alphabetical: ordered by parameter_id
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ProcedureParameterNames_AMissingProcedure_GivesNoNamesAndNoException(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using SqlConnection connection = LocalDbFixture.Create();
		connection.Open();

		ImmutableArray<string> names = await NamesOf(new Probe(connection), connection, $"dbo.{LocalDbFixture.UniqueName("ode_missing")}", flavor, Token);

		Assert.False(names.IsDefault);
		Assert.Empty(names);
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ProcedureParameterNames_ANameWithADatabasePart_IsNotLookedUp(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = LocalDbFixture.CreateProcedure("@a int, @b int");
		using SqlConnection connection = LocalDbFixture.Create();
		connection.Open();
		var probe = new Probe(connection);

		// The same procedure, which lives in the current database (tempdb): found by its two-part name, not by its three-part one.
		Assert.Equal(["@a", "@b"], await NamesOf(probe, connection, $"dbo.{procedure.Name}", flavor, Token));
		Assert.Empty(await NamesOf(probe, connection, $"tempdb.dbo.{procedure.Name}", flavor, Token));
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ProcedureParameterNames_RunInTheCommandsTransaction(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch procedure = LocalDbFixture.CreateProcedure("@a int");
		using SqlConnection connection = LocalDbFixture.Create();
		connection.Open();
		using SqlTransaction transaction = connection.BeginTransaction();

		// SQL Server refuses a command with no transaction on a connection that has one pending; one that was given it runs.
		Assert.Equal(["@a"], await NamesOf(new Probe(connection, transaction), connection, $"dbo.{procedure.Name}", flavor, Token));
		await Assert.ThrowsAsync<InvalidOperationException>(() => NamesOf(new Probe(connection), connection, $"dbo.{procedure.Name}", flavor, Token));
	}

	[Fact]
	public static void ProcedureParameterNames_AFailureIsRethrownUnchanged_WithTheProcedureNameAdded()
	{
		using var connection = new SqlConnection(UnopenedConnectionString); // closed: the provider refuses to run a command on it, without any I/O

		InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => new Probe(connection).Names(connection, "dbo.some_procedure"));

		Assert.Equal("dbo.some_procedure", ex.Data["Procedure"]);
	}

	[Fact]
	public static async Task ProcedureParameterNames_AnAsyncFailureIsRethrownUnchanged_WithTheProcedureNameAdded()
	{
		using var connection = new SqlConnection(UnopenedConnectionString);

		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await new Probe(connection).NamesAsync(connection, "dbo.some_procedure", CancellationToken.None));

		Assert.Equal("dbo.some_procedure", ex.Data["Procedure"]);
	}

	[Fact]
	public static async Task ProcedureParameterNames_ACancellation_IsNotAFailureOfTheLookup_SoItCarriesNoProcedureName()
	{
		using var connection = new SqlConnection(UnopenedConnectionString);
		using var cancelled = new CancellationTokenSource();
		await cancelled.CancelAsync();

		OperationCanceledException ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await new Probe(connection).NamesAsync(connection, "dbo.some_procedure", cancelled.Token));

		Assert.False(ex.Data.Contains("Procedure"));
	}

	// ---- SqlTransactionExtensions.ExecuteTransactionConditional and its async twin: a typed copy of Core's, which BeginTransaction keeps synchronous so the SqlTransaction type flows ----

	static void Insert(SqlTransaction transaction, string table, int id)
	{
		using SqlCommand command = transaction.Connection!.CreateCommand();
		command.Transaction = transaction;
		command.CommandText = $"insert into dbo.[{table}] (id) values ({id})";
		command.ExecuteNonQuery();
	}

	static async Task<(bool Commit, int Value)> Transact(
		SqlConnection connection, Flavor flavor,
		Func<SqlTransaction, (bool Commit, int Value)> action,
		CancellationToken cancellationToken)
		=> flavor == Flavor.Sync
			? SqlTx.ExecuteTransactionConditional(connection, action, IsolationLevel.Unspecified, cancellationToken)
			: await SqlTx.ExecuteTransactionConditionalAsync(connection, (t, _) => new ValueTask<(bool Commit, int Value)>(action(t)), IsolationLevel.Unspecified, cancellationToken);

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ExecuteTransactionConditional_Commit_MakesTheRowVisibleToAnotherConnection_AndLeavesTheOpenedConnectionClosed(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch table = LocalDbFixture.CreateTable();
		using SqlConnection connection = LocalDbFixture.Create();

		(bool commit, int value) = await Transact(connection, flavor, t =>
		{
			Insert(t, table.Name, 1);
			return (true, 7);
		}, Token);

		Assert.True(commit);
		Assert.Equal(7, value);
		Assert.Equal(1, LocalDbFixture.Count(table.Name));
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ExecuteTransactionConditional_NoCommit_RollsBack_AndReturnsTheResultUnchanged(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch table = LocalDbFixture.CreateTable();
		using SqlConnection connection = LocalDbFixture.Create();

		(bool commit, int value) = await Transact(connection, flavor, t =>
		{
			Insert(t, table.Name, 1);
			return (false, 7);
		}, Token);

		Assert.False(commit);
		Assert.Equal(7, value);
		Assert.Equal(0, LocalDbFixture.Count(table.Name));
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ExecuteTransactionConditional_AnActionThatThrowsAfterTheInsert_RollsBack_ThrowsTheOriginal_AndClosesTheConnection(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch table = LocalDbFixture.CreateTable();
		using SqlConnection connection = LocalDbFixture.Create();
		var failure = new ProbeException();

		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(() => Transact(connection, flavor, t =>
		{
			Insert(t, table.Name, 1);
			throw failure;
		}, Token)));

		Assert.Equal(0, LocalDbFixture.Count(table.Name));
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ExecuteTransactionConditional_ATokenCancelledByTheAction_NeverCommits(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch table = LocalDbFixture.CreateTable();
		using SqlConnection connection = LocalDbFixture.Create();
		using var cancellation = new CancellationTokenSource();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Transact(connection, flavor, t =>
		{
			Insert(t, table.Name, 1);
			cancellation.Cancel();
			return (true, 7);
		}, cancellation.Token));

		Assert.Equal(0, LocalDbFixture.Count(table.Name));
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ExecuteTransactionConditional_AnAlreadyCancelledToken_StopsBeforeOpeningTheConnection(Flavor flavor)
	{
		using SqlConnection connection = LocalDbFixture.Create();
		using var cancelled = new CancellationTokenSource();
		await cancelled.CancelAsync();
		bool called = false;

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Transact(connection, flavor, _ =>
		{
			called = true;
			return (true, 7);
		}, cancelled.Token));

		Assert.False(called);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Theory]
	[MemberData(nameof(Flavors))]
	public static async Task ExecuteTransactionConditional_AConnectionTheCallerOpened_StaysOpen_WhateverTheOutcome(Flavor flavor)
	{
		LocalDbFixture.SkipUnlessAvailable();
		using LocalDbFixture.Scratch table = LocalDbFixture.CreateTable();
		using SqlConnection connection = LocalDbFixture.Create();
		connection.Open();

		await Transact(connection, flavor, t =>
		{
			Insert(t, table.Name, 1);
			return (true, 0);
		}, Token);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Equal(1, LocalDbFixture.Count(table.Name));

		await Transact(connection, flavor, t =>
		{
			Insert(t, table.Name, 2);
			return (false, 0);
		}, Token);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Equal(1, LocalDbFixture.Count(table.Name));

		await Assert.ThrowsAsync<ProbeException>(() => Transact(connection, flavor, _ => throw new ProbeException(), Token));
		Assert.Equal(ConnectionState.Open, connection.State);
	}
}
