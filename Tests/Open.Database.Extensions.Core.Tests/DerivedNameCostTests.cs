using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// What a repeat execution costs when the parameter names are derived (<c>AddParam(value)</c>, <c>AddParams(object)</c>) compared with the hand-written
/// <c>AddParam("@name", value)</c> measured in the same run, and the behaviour that the cost work must not change. Everything runs on in-memory fakes.
/// </summary>
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class DerivedNameCostTests
{
	// ---- measuring ----

	// Bytes per call: after a warm-up, the smallest of three rounds of 1,000 calls (the allocated-bytes counter of this thread is exact, so only a stray JIT or GC adds noise).
	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 100; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 1000; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 1000);
		}

		return best;
	}

	// What the derived version may cost beyond the hand-written one: the two differ by no more than a rounding of the measurement.
	const int Slack = 16;

	// The numbers are carried by the failure message; when ODE_ALLOC_LOG names a file they are also appended there, so a passing run can be read too.
	static void Report(string label, long derived, long explicitName)
	{
		string line = $"{label}: derived {derived} B, explicit {explicitName} B, difference {derived - explicitName} B";
		if (Environment.GetEnvironmentVariable("ODE_ALLOC_LOG") is { Length: > 0 } path) File.AppendAllText(path, line + Environment.NewLine);
	}

	static void AssertWithinBudget(string label, long derived, long explicitName)
	{
		Report(label, derived, explicitName);
		Assert.True(derived <= explicitName + Slack, $"{label}: derived {derived} B per call, explicit {explicitName} B, budget explicit + {Slack}.");
	}

	// ---- the fakes ----

	sealed class Filter
	{
		public int OrderId { get; set; } = 5;
		public int CustomerId { get; set; } = 7;
		public DateTime StartDate { get; set; } = new(2026, 1, 2);
	}

	sealed class Args
	{
		public int OrderId { get; set; } = 5;
		public int CustomerId { get; set; } = 7;
		public DateTime StartDate { get; set; } = new(2026, 1, 2);
	}

	sealed class NamedDbCommand(DbConnection connection, CommandType type, string text, ImmutableArray<string> names)
		: ExpressiveDbCommand(connection, type, text)
	{
		protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure) => names;

		protected override ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(DbConnection connection, string procedure, CancellationToken cancellationToken) => new(names);
	}

	// One connection and one procedure for the whole measurement, so the names are read once and every later run is a repeat.
	sealed class Rig
	{
		static readonly ImmutableArray<string> Real =
		[
			"@order_id", "@customer_id", "@start_date", "@end_date", "@status", "@page", "@page_size", "@sort", "@region", "@flag",
		];

		readonly DerivedParameterNameTests.RecordingDbCommand _db = new();
		readonly DbConnection _connection;
		readonly CommandType _type;
		readonly string _procedure = ParameterNameHarness.NewProcedure();

		public Rig(CommandType type)
		{
			_type = type;
			_connection = new DerivedParameterNameTests.OpenDbConnection(_db);
		}

		public ExpressiveDbCommand Command() => new NamedDbCommand(_connection, _type, _procedure, Real);

		public ExpressiveDbCommand Command(string text) => new NamedDbCommand(_connection, _type, text, Real);

		public string[] Sent => [.. _db.Sent.Select(p => p.ParameterName)];

		public void Run(ExpressiveDbCommand command)
		{
			command.ExecuteNonQuery();
			_db.Sent.Clear();
		}

		public void RunAsync(ExpressiveDbCommand command)
		{
			command.ExecuteNonQueryAsync().GetAwaiter().GetResult();
			_db.Sent.Clear();
		}
	}

	const string Sql = "select * from orders where order_id = @order_id and customer_id = @customer_id and start_date >= @start_date";

	// ---- stored procedure, AddParam(value) ----

	[Fact]
	public static void StoredProcedure_PlainLocals_DerivedCostsNoMoreThanExplicit()
	{
		var rig = new Rig(CommandType.StoredProcedure);
		int orderId = 5, customerId = 7;
		var startDate = new DateTime(2026, 1, 2);

		long derived = BytesPerCall(() => rig.Run(rig.Command().AddParam(orderId).AddParam(customerId).AddParam(startDate)));
		long explicitName = BytesPerCall(() => rig.Run(rig.Command().AddParam("@order_id", orderId).AddParam("@customer_id", customerId).AddParam("@start_date", startDate)));

		AssertWithinBudget("stored procedure, three plain locals", derived, explicitName);
	}

	[Fact]
	public static void StoredProcedure_MemberChains_DerivedCostsNoMoreThanExplicit()
	{
		var rig = new Rig(CommandType.StoredProcedure);
		var filter = new Filter();

		long derived = BytesPerCall(() => rig.Run(rig.Command().AddParam(filter.OrderId).AddParam(filter.CustomerId).AddParam(filter.StartDate)));
		long explicitName = BytesPerCall(() => rig.Run(rig.Command().AddParam("@order_id", filter.OrderId).AddParam("@customer_id", filter.CustomerId).AddParam("@start_date", filter.StartDate)));

		AssertWithinBudget("stored procedure, three member chains", derived, explicitName);
	}

	// ---- stored procedure, AddParams(object) ----

	[Fact]
	public static void StoredProcedure_AddParamsObject_CostsNoMoreThanExplicit()
	{
		var rig = new Rig(CommandType.StoredProcedure);
		var args = new Args();

		long derived = BytesPerCall(() => rig.Run(rig.Command().AddParams(args)));
		long explicitName = BytesPerCall(() => rig.Run(rig.Command().AddParam("@order_id", args.OrderId).AddParam("@customer_id", args.CustomerId).AddParam("@start_date", args.StartDate)));

		AssertWithinBudget("stored procedure, AddParams with three members", derived, explicitName);
	}

	// ---- text command ----

	[Fact]
	public static void Text_LiteralSql_DerivedCostsNoMoreThanExplicit()
	{
		var rig = new Rig(CommandType.Text);
		int orderId = 5, customerId = 7;
		var startDate = new DateTime(2026, 1, 2);

		long derived = BytesPerCall(() => rig.Run(rig.Command(Sql).AddParam(orderId).AddParam(customerId).AddParam(startDate)));
		long explicitName = BytesPerCall(() => rig.Run(rig.Command(Sql).AddParam("@order_id", orderId).AddParam("@customer_id", customerId).AddParam("@start_date", startDate)));

		AssertWithinBudget("text command, literal SQL", derived, explicitName);
	}

	// The SQL text is a new string object on every call (same characters). Nothing is asserted about the cost beyond "it works": it is reported.
	// A weak-table entry also costs a GC handle, which the allocated bytes do not show.
	[Fact]
	public static void Text_SqlBuiltOnEveryCall_Works_AndTheCostIsReported()
	{
		var rig = new Rig(CommandType.Text);
		int orderId = 5, customerId = 7;
		var startDate = new DateTime(2026, 1, 2);

		long derived = BytesPerCall(() => rig.Run(rig.Command(new string(Sql.AsSpan())).AddParam(orderId).AddParam(customerId).AddParam(startDate)));
		long explicitName = BytesPerCall(() => rig.Run(rig.Command(new string(Sql.AsSpan())).AddParam("@order_id", orderId).AddParam("@customer_id", customerId).AddParam("@start_date", startDate)));
		Report("text command, SQL built on every call", derived, explicitName);

		ExpressiveDbCommand command = rig.Command(new string(Sql.AsSpan())).AddParam(orderId).AddParam(customerId).AddParam(startDate);
		command.ExecuteNonQuery();
		Assert.Equal(["@order_id", "@customer_id", "@start_date"], rig.Sent);
	}

	// ---- the asynchronous twins (the fake completes synchronously, so the per-thread counter sees all of it) ----

	[Fact]
	public static void StoredProcedureAsync_PlainLocals_DerivedCostsNoMoreThanExplicit()
	{
		var rig = new Rig(CommandType.StoredProcedure);
		int orderId = 5, customerId = 7;
		var startDate = new DateTime(2026, 1, 2);

		long derived = BytesPerCall(() => rig.RunAsync(rig.Command().AddParam(orderId).AddParam(customerId).AddParam(startDate)));
		long explicitName = BytesPerCall(() => rig.RunAsync(rig.Command().AddParam("@order_id", orderId).AddParam("@customer_id", customerId).AddParam("@start_date", startDate)));

		AssertWithinBudget("stored procedure async, three plain locals", derived, explicitName);
	}

	[Fact]
	public static void StoredProcedureAsync_AddParamsObject_CostsNoMoreThanExplicit()
	{
		var rig = new Rig(CommandType.StoredProcedure);
		var args = new Args();

		long derived = BytesPerCall(() => rig.RunAsync(rig.Command().AddParams(args)));
		long explicitName = BytesPerCall(() => rig.RunAsync(rig.Command().AddParam("@order_id", args.OrderId).AddParam("@customer_id", args.CustomerId).AddParam("@start_date", args.StartDate)));

		AssertWithinBudget("stored procedure async, AddParams with three members", derived, explicitName);
	}

	// A plain command object: the size of what conn.Command(sql) allocates, reported for the record (the field that marks derived parameters must not grow it).
	[Fact]
	public static void PlainCommand_SizeIsReported()
	{
		var db = new DerivedParameterNameTests.RecordingDbCommand();
		var connection = new DerivedParameterNameTests.OpenDbConnection(db);
		long bytes = BytesPerCall(() => connection.Command("select 1"));
		if (Environment.GetEnvironmentVariable("ODE_ALLOC_LOG") is { Length: > 0 } path)
			File.AppendAllText(path, $"plain command, bytes per conn.Command(sql): {bytes}" + Environment.NewLine);

		Assert.True(bytes > 0);
	}

	// ---- behaviour that must not change ----

	// A fake connection that can be switched to another server or database, as one pooled connection or one factory can be.
	sealed class SwitchableConnection(DerivedParameterNameTests.RecordingDbCommand command) : DbConnection
	{
		public string Server = "";
		public string CurrentDatabase = "";

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => CurrentDatabase;
		public override string DataSource => Server;
		public override string ServerVersion => "1.0";
		public override ConnectionState State => ConnectionState.Open;
		public override void ChangeDatabase(string databaseName) => CurrentDatabase = databaseName;
		public override void Open() { }
		public override void Close() { }
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
		protected override DbCommand CreateDbCommand() => command;
	}

	// A procedure whose names the test can change, on a connection the test can switch.
	sealed class ProcRig(params string[] initial)
	{
		readonly DerivedParameterNameTests.RecordingDbCommand _db = new();
		SwitchableConnection? _connection;
		public string[] Names = initial;
		public int Lookups;
		public string Procedure { get; } = ParameterNameHarness.NewProcedure();
		public SwitchableConnection Connection => _connection ??= new(_db);

		sealed class Cmd(DbConnection connection, string procedure, ProcRig owner) : ExpressiveDbCommand(connection, CommandType.StoredProcedure, procedure)
		{
			protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure)
			{
				owner.Lookups++;
				return [.. owner.Names];
			}
		}

		public string[] Run(Action<ExpressiveDbCommand> add)
		{
			_db.Sent.Clear();
			var command = new Cmd(Connection, Procedure, this);
			add(command);
			command.ExecuteNonQuery();
			return [.. _db.Sent.Select(p => p.ParameterName)];
		}
	}

	// The same procedure name on another database, and on another server, is read on its own: the parts of the key are not mixed.
	[Fact]
	public static void TheSameProcedureOnAnotherDatabaseOrServer_ResolvesAgainstItsOwnNames()
	{
		var rig = new ProcRig("@order_id");
		int orderId = 1;

		rig.Connection.Server = "srv1";
		rig.Connection.CurrentDatabase = "dbA";
		Assert.Equal(["@order_id"], rig.Run(c => c.AddParam(orderId)));

		rig.Names = ["@orderid"];
		rig.Connection.CurrentDatabase = "dbB";
		Assert.Equal(["@orderid"], rig.Run(c => c.AddParam(orderId)));

		rig.Names = ["@p_order_id"];
		rig.Connection.Server = "srv2";
		rig.Connection.CurrentDatabase = "dbA";
		Assert.Equal(["@p_order_id"], rig.Run(c => c.AddParam(orderId)));
		Assert.Equal(3, rig.Lookups);

		// Back on the first server and database (written in another case): its own names, remembered, not read again.
		rig.Names = ["@never_read"];
		rig.Connection.Server = "SRV1";
		rig.Connection.CurrentDatabase = "DBA";
		Assert.Equal(["@order_id"], rig.Run(c => c.AddParam(orderId)));
		Assert.Equal(3, rig.Lookups);
	}

	// The remembered names no longer fit: they are read again and the call succeeds, and what the old set answered is not used for the new one.
	[Fact]
	public static void StaleNames_AreReadAgain_AndTheOldSetsAnswersAreNotReused()
	{
		var rig = new ProcRig("@order_id");
		int orderId = 1, customerId = 2;

		Assert.Equal(["@order_id"], rig.Run(c => c.AddParam(orderId)));
		Assert.Equal(1, rig.Lookups);

		rig.Names = ["@orderid", "@customer_id"]; // the procedure changed after the first read
		Assert.Equal(["@orderid", "@customer_id"], rig.Run(c => c.AddParam(orderId).AddParam(customerId)));
		Assert.Equal(2, rig.Lookups);

		// The new set is remembered now: no more reads.
		Assert.Equal(["@orderid", "@customer_id"], rig.Run(c => c.AddParam(orderId).AddParam(customerId)));
		Assert.Equal(2, rig.Lookups);
	}

	// An ambiguous argument throws on every call, not only the first.
	[Fact]
	public static void AmbiguousArgument_ThrowsOnEveryCall()
	{
		var rig = new ProcRig("@company_serial_num", "@companyserial_num");
		int companySerialNum = 1;

		for (int i = 0; i < 3; i++)
		{
			var ex = Assert.Throws<InvalidOperationException>(() => rig.Run(c => c.AddParam(companySerialNum)));
			Assert.Equal(
				$"The argument 'companySerialNum' matches both '@company_serial_num' and '@companyserial_num' of '{rig.Procedure}'. Its candidate name 'companySerialNum' fits each of them ignoring case and underscores. "
				+ "The procedure declares: @company_serial_num, @companyserial_num. Name the parameter explicitly: AddParam(\"name\", value).",
				ex.Message);
		}
	}

	sealed class Item
	{
		public int Id { get; set; } = 1;
		public string Junk { get; set; } = "x";
		public string Name { get; set; } = "n";
	}

	// A member of an object that the procedure does not take is not sent, on every call.
	[Fact]
	public static void ObjectMemberTheProcedureDoesNotDeclare_IsDropped_OnEveryCall()
	{
		var rig = new ProcRig("@id", "@name");
		var item = new Item();

		for (int i = 0; i < 3; i++)
			Assert.Equal(["@id", "@name"], rig.Run(c => c.AddParams(item)));
	}

	// A parameter named by the caller wins over an object member that resolves to the same name.
	[Fact]
	public static void CallerNamedParameter_WinsOverAnObjectMember()
	{
		var rig = new ProcRig("@id", "@name");
		var item = new Item();

		for (int i = 0; i < 3; i++)
			Assert.Equal(["@ID", "@name"], rig.Run(c => c.AddParam("@ID", 99).AddParams(item)));
	}

	sealed class Regional
	{
		public int Region { get; set; } = 3;
	}

	// Two arguments that resolve to one parameter: the messages are pinned.
	[Fact]
	public static void Collision_MessagesArePinned()
	{
		var rig = new ProcRig("@region");
		int region = 1, Region = 2;
		string tail = $" The procedure '{rig.Procedure}' declares: @region. Name one of them explicitly: AddParam(\"name\", value).";

		var twoArguments = Assert.Throws<InvalidOperationException>(() => rig.Run(c => c.AddParam(region).AddParam(Region)));
		Assert.Equal("Two parameters resolve to '@region': the argument 'region' and the argument 'Region'." + tail, twoArguments.Message);

		var named = Assert.Throws<InvalidOperationException>(() => rig.Run(c => c.AddParam("@REGION", 1).AddParam(region)));
		Assert.Equal("Two parameters resolve to '@REGION': the name '@REGION' you passed and the argument 'region'." + tail, named.Message);

		var member = Assert.Throws<InvalidOperationException>(() => rig.Run(c => c.AddParams(new Regional()).AddParams(new Regional())));
		Assert.Equal("Two parameters resolve to '@region': the member 'Region' of the AddParams object and the member 'Region' of the AddParams object." + tail, member.Message);
	}

	// The message of an argument that matches nothing.
	[Fact]
	public static void NoMatch_MessageIsPinned()
	{
		var rig = new ProcRig("@a", "@b");
		int orderId = 1;

		var ex = Assert.Throws<InvalidOperationException>(() => rig.Run(c => c.AddParam(orderId)));
		Assert.Equal($"The argument 'orderId' matches none of the parameters of '{rig.Procedure}' (tried orderId). The procedure declares: @a, @b. Name the parameter explicitly: AddParam(\"name\", value).", ex.Message);
	}

	// ---- text commands ----

	[Fact]
	public static void TwoStringObjectsWithTheSameSql_BothResolve()
	{
		var db = new DerivedParameterNameTests.RecordingDbCommand();
		var connection = new DerivedParameterNameTests.OpenDbConnection(db);
		int orderId = 1;

		for (int i = 0; i < 3; i++)
		{
			string sql = new("select * from t where order_id = @order_id".AsSpan());
			db.Sent.Clear();
			new ExpressiveDbCommand(connection, CommandType.Text, sql).AddParam(orderId).ExecuteNonQuery();
			Assert.Equal(["@order_id"], db.Sent.Select(p => p.ParameterName));
		}

		for (int i = 0; i < 3; i++) // a literal is one object for every call
		{
			db.Sent.Clear();
			new ExpressiveDbCommand(connection, CommandType.Text, "select * from t where order_id = @order_id").AddParam(orderId).ExecuteNonQuery();
			Assert.Equal(["@order_id"], db.Sent.Select(p => p.ParameterName));
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static WeakReference RunWithADynamicText()
	{
		var db = new DerivedParameterNameTests.RecordingDbCommand();
		var connection = new DerivedParameterNameTests.OpenDbConnection(db);
		string sql = new("select * from t where order_id = @order_id and x = @leak_check_marker".AsSpan());
		int orderId = 1;
		new ExpressiveDbCommand(connection, CommandType.Text, sql).AddParam(orderId).ExecuteNonQuery();
		Assert.Equal(["@order_id"], db.Sent.Select(p => p.ParameterName));
		return new WeakReference(sql);
	}

	// What is remembered about a text command does not keep its text alive.
	[Fact]
	public static void ATextCommandsSql_IsNotKeptAliveByWhatIsRemembered()
	{
		WeakReference weak = RunWithADynamicText();
		for (int i = 0; i < 3 && weak.IsAlive; i++)
		{
			GC.Collect();
			GC.WaitForPendingFinalizers();
			GC.Collect();
		}

		Assert.False(weak.IsAlive);
	}

	// ---- concurrency ----

	// One connection object serves every thread (so they share one remembered name set); each thread has its own fake database command.
	sealed class ThreadConnection : DbConnection
	{
		public readonly ThreadLocal<DerivedParameterNameTests.RecordingDbCommand> Commands = new(() => new());

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "fake";
		public override string DataSource => "fake";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => ConnectionState.Open;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() { }
		public override void Close() { }
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
		protected override DbCommand CreateDbCommand() => Commands.Value!;
	}

	sealed class SharedCommand(DbConnection connection, string procedure, ImmutableArray<string> names) : ExpressiveDbCommand(connection, CommandType.StoredProcedure, procedure)
	{
		protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure) => names;
	}

	[Fact]
	public static void ConcurrentExecutions_ShareOneNameSet_AndAllResolveTheSame()
	{
		string procedure = ParameterNameHarness.NewProcedure();
		ImmutableArray<string> names = ["@order_id", "@customer_id", "@start_date"];
		var results = new System.Collections.Concurrent.ConcurrentBag<string>();
		using var connection = new ThreadConnection();

		Parallel.For(0, 1000, i =>
		{
			DerivedParameterNameTests.RecordingDbCommand db = connection.Commands.Value!;
			db.Sent.Clear();
			int orderId = i, customerId = i, startDate = i;
			new SharedCommand(connection, procedure, names).AddParam(orderId).AddParam(customerId).AddParam(startDate).ExecuteNonQuery();
			results.Add(string.Join(",", db.Sent.Select(p => p.ParameterName)));
		});

		Assert.Equal(1000, results.Count);
		Assert.All(results, r => Assert.Equal("@order_id,@customer_id,@start_date", r));
	}
}
