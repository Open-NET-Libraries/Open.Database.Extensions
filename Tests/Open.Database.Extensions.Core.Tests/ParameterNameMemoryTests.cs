using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// What the remembered names of a procedure do, and do not do. The names are read once and trusted for five minutes; these tests pin the outcomes the docs describe,
// so that a change to any of them is a decision: a stale list, a transaction that rolls back, and what a name is remembered under.
[ExcludeFromCodeCoverage]
public static class ParameterNameMemoryTests
{
	// One procedure on one connection: every Run is a new command whose provider reports `declared` right now, so a test can change what the procedure declares between two runs.
	sealed class Procedure(string name)
	{
		public ConnectionProbe Probe { get; } = new(ConnectionState.Open);

		public int Lookups { get; private set; }

		public string[] Run(string[] declared, Action<ExpressiveCommand> add)
		{
			Probe.Parameters.Clear();
			var command = new DerivedParameterNameTests.ProcCommand(Probe.Connection, CommandType.StoredProcedure, name, declared);
			add(command);
			try { command.ExecuteNonQuery(); }
			finally { Lookups += command.Lookups; }

			return [.. Probe.Parameters.Select(p => p.ParameterName)];
		}
	}

	// The asynchronous twin: the same connection for every run, and the names the provider reports right now.
	sealed class AsyncProcedure
	{
		sealed class Command(DbConnection connection, string procedureName, string[] declared)
			: ExpressiveDbCommand(connection, CommandType.StoredProcedure, procedureName)
		{
			readonly ImmutableArray<string> _declared = [.. declared];

			public int Lookups { get; private set; }

			protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure)
			{
				Lookups++;
				return _declared;
			}

			protected override ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(DbConnection connection, string procedure, CancellationToken cancellationToken)
			{
				Lookups++;
				return new(_declared);
			}
		}

		readonly string _name;
		readonly DerivedParameterNameTests.RecordingDbCommand _db = new();
		readonly DbConnection _connection;

		public AsyncProcedure(string name)
		{
			_name = name;
			_connection = new DerivedParameterNameTests.OpenDbConnection(_db);
		}

		public int Lookups { get; private set; }

		public async Task<string[]> RunAsync(string[] declared, Action<ExpressiveDbCommand> add)
		{
			_db.Sent.Clear();
			var command = new Command(_connection, _name, declared);
			add(command);
			try { await command.ExecuteNonQueryAsync(); }
			finally { Lookups += command.Lookups; }

			return [.. _db.Sent.Select(p => p.ParameterName)];
		}
	}

	static void CustomerId(ExpressiveCommand c) => c.AddParam(value: 1, expression: "order.Customer.Id");

	static void Region(ExpressiveCommand c) => c.AddParam(value: 1, expression: "filter.Region");

	static void Company(ExpressiveCommand c) => c.AddParam(value: 1, expression: "filter.Company");

	static void Author(ExpressiveCommand c) => c.AddParam(value: 1, expression: "search.Author");

	static void TwoRegions(ExpressiveCommand c) => c.AddParam(value: 1, expression: "filter.Region").AddParam(value: 2, expression: "other.Region");

	// ---- the four outcomes of a remembered list that went stale (the procedure changed, the five minutes have not passed) ----

	[Fact]
	public static void AStaleList_ANewOptionalParameter_IsNotSentByAddParams_AMemberTheNamesDoNotDeclareIsNotAReasonToReadAgain()
	{
		var procedure = new Procedure("dbo.stale_optional");
		var args = new { A = 1, B = 2 };

		Assert.Equal(["@a"], procedure.Run(["@a"], c => c.AddParams(args)));
		Assert.Equal(1, procedure.Lookups);

		// The procedure now declares @b as well. A member that the remembered names do not declare is skipped, and skipping is not a mismatch: nothing is read again.
		Assert.Equal(["@a"], procedure.Run(["@a", "@b"], c => c.AddParams(args)));
		Assert.Equal(1, procedure.Lookups);
	}

	[Fact]
	public static void AStaleList_ALessSpecificNameIsKept_WhileAMoreSpecificOneNowExists()
	{
		var procedure = new Procedure("dbo.stale_specific");

		Assert.Equal(["@id"], procedure.Run(["@id"], CustomerId));
		Assert.Equal(1, procedure.Lookups);

		// order.Customer.Id would now find @customer_id, but the remembered names already have an answer (@id), so they are not read again.
		Assert.Equal(["@id"], procedure.Run(["@id", "@customer_id"], CustomerId));
		Assert.Equal(1, procedure.Lookups);
	}

	[Fact]
	public static void AStaleList_AnAmbiguity_IsReportedFromMemory_EvenThoughTheProcedureNoLongerHasIt()
	{
		var procedure = new Procedure("dbo.stale_ambiguous");

		Assert.Throws<InvalidOperationException>(() => procedure.Run(["@p_author", "@in_author"], Author));
		Assert.Equal(1, procedure.Lookups);

		var ex = Assert.Throws<InvalidOperationException>(() => procedure.Run(["@p_author"], Author));
		Assert.Contains("matches both '@p_author' and '@in_author'", ex.Message); // what the remembered names say, not what the procedure declares now
		Assert.Equal(1, procedure.Lookups);
	}

	[Fact]
	public static void AStaleList_ACollision_IsReportedFromMemory_EvenThoughTheProcedureNowHasASecondParameter()
	{
		var procedure = new Procedure("dbo.stale_collision");

		Assert.Throws<InvalidOperationException>(() => procedure.Run(["@region"], TwoRegions));
		Assert.Equal(1, procedure.Lookups);

		// With a fresh read other.Region would find @other_region first; from memory both arguments still resolve to @region.
		var ex = Assert.Throws<InvalidOperationException>(() => procedure.Run(["@region", "@other_region"], TwoRegions));
		Assert.StartsWith("Two parameters resolve to '@region': the argument 'filter.Region' and the argument 'other.Region'.", ex.Message);
		Assert.Equal(1, procedure.Lookups);
	}

	[Fact]
	public static void AStaleList_WhenTheSecondReadFindsNothing_TheArgumentIsSentAsWritten_AndTheOldEntryStays()
	{
		var procedure = new Procedure("dbo.stale_empty");

		Assert.Equal(["@region"], procedure.Run(["@region"], Region));
		Assert.Equal(1, procedure.Lookups);

		// filter.Company matches none of the remembered names, so the names are read once more. The provider now answers "not known" (the procedure is not visible
		// to the login, say): the argument is sent as written, and an empty answer is never remembered, so the old entry is left where it was.
		Assert.Equal(["Company"], procedure.Run([], Company));
		Assert.Equal(2, procedure.Lookups);

		Assert.Equal(["Company"], procedure.Run([], Company));
		Assert.Equal(3, procedure.Lookups); // read again, because the stale entry is still there and still does not fit

		Assert.Equal(["@region"], procedure.Run([], Region));
		Assert.Equal(3, procedure.Lookups); // and it still answers what it does fit
	}

	[Fact]
	public static async Task AStaleList_OnTheAsyncPath_IsReadAgainOnce_ForAnArgumentItDoesNotFit_ButNeverForAnAmbiguity()
	{
		var procedure = new AsyncProcedure("dbo.async_stale");

		Assert.Equal(["@region"], await procedure.RunAsync(["@region"], c => c.AddParam(value: 1, expression: "filter.Region")));
		Assert.Equal(1, procedure.Lookups);

		Assert.Equal(["@company_serial_num"], await procedure.RunAsync(["@company_serial_num"], c => c.AddParam(value: 1, expression: "filter.CompanySerialNum")));
		Assert.Equal(2, procedure.Lookups); // the argument fitted none of the remembered names: one more read

		var ambiguous = new AsyncProcedure("dbo.async_ambiguous");
		await Assert.ThrowsAsync<InvalidOperationException>(() => ambiguous.RunAsync(["@p_author", "@in_author"], c => c.AddParam(value: 1, expression: "search.Author")));
		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ambiguous.RunAsync(["@p_author"], c => c.AddParam(value: 1, expression: "search.Author")));
		Assert.Contains("matches both '@p_author' and '@in_author'", ex.Message);
		Assert.Equal(1, ambiguous.Lookups);
	}

	// ---- a transaction that rolls back ----

	sealed class TransactionCommand(IDbTransaction transaction, string procedureName, params string[] declared)
		: ExpressiveCommand(transaction, CommandType.StoredProcedure, procedureName)
	{
		readonly ImmutableArray<string> _declared = [.. declared];

		public int Lookups { get; private set; }
		public IDbTransaction? LookedUpIn { get; private set; }

		protected override ImmutableArray<string> GetProcedureParameterNames(IDbConnection connection, string procedure)
		{
			Lookups++;
			LookedUpIn = Transaction;
			return _declared;
		}
	}

	[Fact]
	public static void NamesReadInsideATransaction_AreRememberedByTheConnection_EvenIfItRollsBack()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var transaction = Substitute.For<IDbTransaction>();
		transaction.Connection.Returns(probe.Connection);

		var inside = new TransactionCommand(transaction, "dbo.rolled_back", "@region");
		inside.AddParam(value: 1, expression: "filter.Region").ExecuteNonQuery();
		Assert.Equal(1, inside.Lookups);
		Assert.Same(transaction, inside.LookedUpIn); // the lookup ran inside the transaction
		transaction.Rollback();                     // whatever the transaction created or changed is gone, and nothing here is told

		// The next command on that connection finds the names in memory: the provider is not asked, and the answer is what the rolled-back transaction saw.
		var after = new DerivedParameterNameTests.ProcCommand(probe.Connection, CommandType.StoredProcedure, "dbo.rolled_back", "@something_else");
		after.AddParam(value: 1, expression: "filter.Region").ExecuteNonQuery();

		Assert.Equal(0, after.Lookups);
		Assert.Equal(["@region", "@region"], probe.Parameters.Select(p => p.ParameterName));
	}

	// ---- what a name is remembered under ----

	sealed class ServerConnection(string dataSource, string database, DbCommand command) : DbConnection
	{
		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => database;
		public override string DataSource => dataSource;
		public override string ServerVersion => "1.0";
		public override ConnectionState State => ConnectionState.Open;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() { }
		public override void Close() { }
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
		protected override DbCommand CreateDbCommand() => command;
	}

	sealed class FactoryCommand(IDbConnectionFactory<DbConnection> factory, string procedureName)
		: ExpressiveDbCommand(factory, CommandType.StoredProcedure, procedureName)
	{
		public int Lookups { get; private set; }

		protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure)
		{
			Lookups++;
			return ["@region"];
		}
	}

	static int Lookups(IDbConnectionFactory<DbConnection> factory, string procedure)
	{
		var command = new FactoryCommand(factory, procedure);
		command.AddParam(value: 1, expression: "filter.Region").ExecuteNonQuery();
		return command.Lookups;
	}

	[Fact]
	public static void RememberedNames_AreKeyedByServerDatabaseAndProcedure_IgnoringCase_OnOneFactory()
	{
		string server = "alpha", database = "sales";
		var db = new DerivedParameterNameTests.RecordingDbCommand();
		var factory = DbConnectionFactory.Create<DbConnection>(() => new ServerConnection(server, database, db));

		Assert.Equal(1, Lookups(factory, "dbo.p")); // the first read
		Assert.Equal(0, Lookups(factory, "dbo.p")); // remembered

		server = "beta";
		Assert.Equal(1, Lookups(factory, "dbo.p")); // the same name on another server is another procedure
		Assert.Equal(0, Lookups(factory, "dbo.p"));

		server = "alpha";
		database = "hr";
		Assert.Equal(1, Lookups(factory, "dbo.p")); // another database on the first server

		database = "sales";
		Assert.Equal(0, Lookups(factory, "dbo.p")); // back on the first: still remembered

		server = "ALPHA";
		database = "SALES";
		Assert.Equal(0, Lookups(factory, "DBO.P")); // a difference of case alone is not a difference

		Assert.Equal(1, Lookups(factory, "dbo.q")); // another procedure
	}

	[Fact]
	public static void ACommandThatIsGivenANewFactoryEachTime_ReadsTheNamesEachTime()
	{
		var db = new DerivedParameterNameTests.RecordingDbCommand();

		for (int i = 0; i < 3; i++)
		{
			var factory = DbConnectionFactory.Create<DbConnection>(() => new ServerConnection("alpha", "sales", db));
			Assert.Equal(1, Lookups(factory, "dbo.per_call_factory"));
		}
	}
}
