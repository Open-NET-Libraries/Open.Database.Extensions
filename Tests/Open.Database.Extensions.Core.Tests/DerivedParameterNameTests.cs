using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// AddParam(value) / AddParamIf(condition, value): the parameter name comes from the argument expression, so filter.CompanySerialNum adds @company_serial_num.
// The call sites below are real, so the compiler itself captures the text; properties, fields, and members of members are all exercised.
[ExcludeFromCodeCoverage]
public static class DerivedParameterNameTests
{
	sealed class Customer
	{
		public int Id { get; set; } = 7;
		public string Name = "ann"; // a field
		public string? Email { get; set; } = "a@b"; // a property
	}

	sealed class Filter
	{
		public int CompanySerialNum { get; set; } = 42;
		public string? Region = "west"; // a field
		public DateTime? EndDate { get; set; }
		public int? Limit { get; set; } = 10;
		public int Offset = 20; // a field
		public Customer Customer { get; set; } = new();
	}

	sealed class Order
	{
		public Customer Customer { get; set; } = new();
		public Filter Filter = new(); // a field holding an object with properties and fields
	}

	// A command whose provider "knows" the real parameter names of the procedure.
	sealed class ProcCommand : ExpressiveCommand
	{
		readonly ImmutableArray<string> _names;

		public ProcCommand(IDbConnection connection, CommandType type, string command, params string[] names)
			: base(connection, type, command) => _names = [.. names];

		public ProcCommand(IDbConnectionFactory factory, CommandType type, string command, params string[] names)
			: base(factory, type, command) => _names = [.. names];

		public int Lookups { get; private set; }
		public string? LookedUp { get; private set; }

		protected override ImmutableArray<string> GetProcedureParameterNames(IDbConnection connection, string procedure)
		{
			Lookups++;
			LookedUp = procedure;
			return _names;
		}
	}

	sealed class ProcDbCommand(DbConnection connection, string command, params string[] names)
		: ExpressiveDbCommand(connection, CommandType.StoredProcedure, command)
	{
		readonly ImmutableArray<string> _names = [.. names];

		public int SyncLookups { get; private set; }
		public int AsyncLookups { get; private set; }

		protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure)
		{
			SyncLookups++;
			return _names;
		}

		protected override ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(DbConnection connection, string procedure, CancellationToken cancellationToken)
		{
			AsyncLookups++;
			return new(_names);
		}
	}

	static int _procedureCounter;

	// A unique procedure name per call: remembered names are shared by every command that uses the same connection source.
	static ProcCommand Proc(ConnectionProbe probe, params string[] names)
		=> new(probe.Connection, CommandType.StoredProcedure, "dbo.search" + Interlocked.Increment(ref _procedureCounter), names);

	static string[] SentNames(ConnectionProbe probe) => [.. probe.Parameters.Select(p => p.ParameterName)];

	// What was sent, by name: the order members of an object are read in is not part of the contract.
	static Dictionary<string, object?> Sent(ConnectionProbe probe) => probe.Parameters.ToDictionary(p => p.ParameterName, p => p.Value);

	// ---- the name as written, when the real names are not known ----

	[Fact]
	public static void NoRealNames_TheLastMemberIsUsedAsWritten_ForVariablesPropertiesFieldsAndMembersOfMembers()
	{
		var filter = new Filter();
		var order = new Order();
		int startDate = 5;
		int pageSize = 25;
		string @class = "c";
		Filter? maybe = filter;

		var probe = new ConnectionProbe(ConnectionState.Open);
		Proc(probe) // the provider supplies no names, so every argument keeps the last member as written (and each must be distinct)
			.AddParam(startDate)                    // a variable
			.AddParam(filter.CompanySerialNum)      // a property
			.AddParam(filter.Region)                // a field
			.AddParam(order.Customer.Id)            // a property of a property
			.AddParam(order.Customer.Name)          // a field of a property
			.AddParam(order.Filter.EndDate)         // a property of a field
			.AddParam(order.Filter.Customer.Email)  // a property of a property of a field
			.AddParam(maybe?.Limit)                 // null-conditional
			.AddParam(maybe!.Offset)                // null-forgiving, a field
			.AddParam((pageSize))                   // parentheses
			.AddParam(@class)                       // a verbatim identifier
			.ExecuteNonQuery();

		Assert.Equal(
			["startDate", "CompanySerialNum", "Region", "Id", "Name", "EndDate", "Email", "Limit", "Offset", "pageSize", "class"],
			SentNames(probe));
		Assert.Equal(
			[5, 42, "west", 7, "ann", DBNull.Value, "a@b", 10, 20, 25, "c"],
			probe.Parameters.Select(p => p.Value));
	}

	[Fact]
	public static void ThisMember_UsesTheMemberName()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		new Holder().Add(Proc(probe)).ExecuteNonQuery();

		Assert.Equal(["Quantity"], SentNames(probe));
		Assert.Equal([3], probe.Parameters.Select(p => p.Value));
	}

	sealed class Holder
	{
		public int Quantity { get; set; } = 3;
		public ExpressiveCommand Add(ExpressiveCommand command) => command.AddParam(this.Quantity);
	}

	// ---- matched to the real parameter names ----

	[Fact]
	public static void RealNames_AreMatchedExactly_IgnoringCase_ThenIgnoringCaseAndUnderscores()
	{
		var filter = new Filter();

		var exact = new ConnectionProbe(ConnectionState.Open);
		Proc(exact, "@CompanySerialNum").AddParam(filter.CompanySerialNum).ExecuteNonQuery();
		Assert.Equal(["@CompanySerialNum"], SentNames(exact));

		var case_ = new ConnectionProbe(ConnectionState.Open);
		Proc(case_, "@companyserialnum").AddParam(filter.CompanySerialNum).ExecuteNonQuery();
		Assert.Equal(["@companyserialnum"], SentNames(case_));

		var underscores = new ConnectionProbe(ConnectionState.Open);
		Proc(underscores, "@company_serial_num").AddParam(filter.CompanySerialNum).ExecuteNonQuery();
		Assert.Equal(["@company_serial_num"], SentNames(underscores));
		Assert.Equal([42], underscores.Parameters.Select(p => p.Value));
	}

	[Fact]
	public static void RealNames_ExactBeatsCaseBeatsUnderscores_WhenSeveralWouldMatch()
	{
		var filter = new Filter();

		// A case-sensitive provider can declare all three; the exact one wins, then the case-insensitive one.
		var all = new ConnectionProbe(ConnectionState.Open);
		Proc(all, "@company_serial_num", "@companyserialnum", "@CompanySerialNum").AddParam(filter.CompanySerialNum).ExecuteNonQuery();
		Assert.Equal(["@CompanySerialNum"], SentNames(all));

		var noExact = new ConnectionProbe(ConnectionState.Open);
		Proc(noExact, "@company_serial_num", "@companyserialnum").AddParam(filter.CompanySerialNum).ExecuteNonQuery();
		Assert.Equal(["@companyserialnum"], SentNames(noExact));
	}

	[Fact]
	public static void RealNames_FieldsAndMembersOfMembers_FindTheirParameters()
	{
		var filter = new Filter();
		var order = new Order();

		var probe = new ConnectionProbe(ConnectionState.Open);
		Proc(probe, "@region", "@customer_id", "@customer_name", "@end_date", "@company_serial_num")
			.AddParam(filter.Region)              // a field
			.AddParam(order.Customer.Id)          // CustomerId finds @customer_id
			.AddParam(order.Customer.Name)        // a field of a property: CustomerName finds @customer_name
			.AddParam(filter.EndDate)             // a nullable property
			.AddParam(order.Filter.CompanySerialNum) // a property of a field: tries OrderFilterCompanySerialNum, FilterCompanySerialNum, then CompanySerialNum
			.ExecuteNonQuery();

		Assert.Equal(["@region", "@customer_id", "@customer_name", "@end_date", "@company_serial_num"], SentNames(probe));
		Assert.Equal(["west", 7, "ann", DBNull.Value, 42], probe.Parameters.Select(p => p.Value));
	}

	[Fact]
	public static void RealNames_AChain_TriesItsMoreSpecificEndingFirst()
	{
		var order = new Order();

		// Both @customer_id and @id exist: order.Customer.Id means the customer's id, not a bare @id (a tier-first search would pick @id).
		var both = new ConnectionProbe(ConnectionState.Open);
		Proc(both, "@Id", "@customer_id").AddParam(order.Customer.Id).ExecuteNonQuery();
		Assert.Equal(["@customer_id"], SentNames(both));

		// Only @id exists: the shorter ending is used.
		var bare = new ConnectionProbe(ConnectionState.Open);
		Proc(bare, "@id").AddParam(order.Customer.Id).ExecuteNonQuery();
		Assert.Equal(["@id"], SentNames(bare));

		// The whole chain, root included, is the most specific candidate.
		var whole = new ConnectionProbe(ConnectionState.Open);
		Proc(whole, "@order_customer_id", "@customer_id", "@id").AddParam(order.Customer.Id).ExecuteNonQuery();
		Assert.Equal(["@order_customer_id"], SentNames(whole));
	}

	[Fact]
	public static void RealNames_ExplicitAndDerivedParams_KeepTheirOrder()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		Proc(probe, "@mode", "@company_serial_num", "@region")
			.AddParam("@mode", 1)
			.AddParam(filter.CompanySerialNum)
			.AddParam("@extra", "x")
			.AddParam(filter.Region)
			.ExecuteNonQuery();

		Assert.Equal(["@mode", "@company_serial_num", "@extra", "@region"], SentNames(probe));
	}

	[Fact]
	public static void RealNames_AreLookedUpOnceForTheProcedure_AndOnlyForStoredProcedures()
	{
		var filter = new Filter();

		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@company_serial_num");
		proc.AddParam(filter.CompanySerialNum);
		proc.ExecuteNonQuery();
		proc.ExecuteNonQuery(); // the names are already settled
		Assert.Equal(1, proc.Lookups);
		Assert.StartsWith("dbo.search", proc.LookedUp);

		// A text command has no procedure to ask about: the last member is used as written.
		var textProbe = new ConnectionProbe(ConnectionState.Open);
		var text = new ProcCommand(textProbe.Connection, CommandType.Text, "select @CompanySerialNum", "@never");
		text.AddParam(filter.CompanySerialNum).ExecuteNonQuery();
		Assert.Equal(0, text.Lookups);
		Assert.Equal(["CompanySerialNum"], SentNames(textProbe));
	}


	[Fact]
	public static void RealNames_AreRemembered_PerConnectionSource_AndNotShared_ByDifferentSources()
	{
		var filter = new Filter();
		const string Procedure = "dbo.remembered_names";

		// Two commands on the same connection: the second reads the names from memory, so the provider is asked once.
		var probe = new ConnectionProbe(ConnectionState.Open);
		var first = new ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@region");
		var second = new ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@region");
		first.AddParam(filter.Region).ExecuteNonQuery();
		second.AddParam(filter.Region).ExecuteNonQuery();

		Assert.Equal(1, first.Lookups);
		Assert.Equal(0, second.Lookups);
		Assert.Equal(["@region", "@region"], SentNames(probe));

		// Another connection is another source (a different factory, pool or connection): it asks for itself.
		var other = new ConnectionProbe(ConnectionState.Open);
		var third = new ProcCommand(other.Connection, CommandType.StoredProcedure, Procedure, "@region");
		third.AddParam(filter.Region).ExecuteNonQuery();
		Assert.Equal(1, third.Lookups);
	}

	[Fact]
	public static void RealNames_AFactorysCommands_ShareOneMemory()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		DbConnectionFactory<IDbConnection> factory = DbConnectionFactory.Create(() => probe.Connection);

		// Each command wraps the factory in its own pool object; what is remembered belongs to the factory, so the second command does not ask again.
		var first = new ProcCommand(factory, CommandType.StoredProcedure, "dbo.factory_names", "@region");
		var second = new ProcCommand(factory, CommandType.StoredProcedure, "dbo.factory_names", "@region");
		first.AddParam(filter.Region).ExecuteNonQuery();
		second.AddParam(filter.Region).ExecuteNonQuery();

		Assert.Equal(1, first.Lookups + second.Lookups);
	}

	[Fact]
	public static void RealNames_AnEmptyAnswerIsNotRemembered()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		var first = new ProcCommand(probe.Connection, CommandType.StoredProcedure, "dbo.unknown_names" /* no names */);
		var second = new ProcCommand(probe.Connection, CommandType.StoredProcedure, "dbo.unknown_names");
		first.AddParam(filter.Region).ExecuteNonQuery();
		second.AddParam(filter.Region).ExecuteNonQuery();

		Assert.Equal(1, first.Lookups);
		Assert.Equal(1, second.Lookups); // asked again, because "not known" may only be a missing permission or a procedure created later
	}

	[Fact]
	public static void RealNames_ARememberedListThatNoLongerFits_IsReadAgainOnceBeforeFailing()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		const string Procedure = "dbo.changed_procedure";

		new ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@region")
			.AddParam(filter.Region)
			.ExecuteNonQuery(); // remembers [@region]

		// The procedure now takes @company_serial_num instead: the remembered list does not fit, is read again once, and the call succeeds.
		var after = new ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@company_serial_num");
		after.AddParam(filter.CompanySerialNum).ExecuteNonQuery();
		Assert.Equal(1, after.Lookups);
		Assert.Equal(["@region", "@company_serial_num"], SentNames(probe));

		// A name that still matches nothing after the re-read throws, and says what the procedure declares now.
		var wrong = new ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@company_serial_num");
		wrong.AddParam(filter.Region);
		var ex = Assert.Throws<InvalidOperationException>(() => wrong.ExecuteNonQuery());
		Assert.Equal(1, wrong.Lookups);
		Assert.Contains("@company_serial_num", ex.Message);
	}

	[Fact]
	public static void RealNames_ProviderThatKnowsNothing_FallsBackToTheNameAsWritten()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);

		// An empty answer means "not known" (for example no permission to see the procedure), not "no parameters".
		Proc(probe /* no names */).AddParam(filter.CompanySerialNum).ExecuteNonQuery();

		Assert.Equal(["CompanySerialNum"], SentNames(probe));
	}

	[Fact]
	public static void NoMatch_Throws_NamingTheProcedureWhatWasTriedAndWhatItExpects()
	{
		var order = new Order();
		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@a", "@b");
		proc.AddParam(order.Customer.Id);

		var ex = Assert.Throws<InvalidOperationException>(() => proc.ExecuteNonQuery());

		Assert.Contains("order.Customer.Id", ex.Message);
		Assert.Contains("dbo.search", ex.Message);
		Assert.Contains("orderCustomerId, CustomerId, Id", ex.Message);
		Assert.Contains("@a, @b", ex.Message);
		Assert.Contains("AddParam(\"name\", value)", ex.Message);
		Assert.Empty(probe.Parameters); // nothing was sent
	}

	[Fact]
	public static void Ambiguous_Throws()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@company_serial_num", "@companyserial_num"); // both read as companyserialnum
		proc.AddParam(filter.CompanySerialNum);

		var ex = Assert.Throws<InvalidOperationException>(() => proc.ExecuteNonQuery());
		Assert.Contains("matches both", ex.Message);
	}

	[Fact]
	public static void TwoArgumentsResolvingToOneParameter_Throws()
	{
		var filter = new Filter();
		var other = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@region");
		proc.AddParam(filter.Region).AddParam(other.Region);

		var ex = Assert.Throws<InvalidOperationException>(() => proc.ExecuteNonQuery());
		Assert.Contains("Two parameters resolve to '@region'", ex.Message);
	}

	[Fact]
	public static void ExplicitNameThatMatchesADerivedOne_Throws()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@region");
		proc.AddParam("@REGION", "x").AddParam(filter.Region);

		Assert.Throws<InvalidOperationException>(() => proc.ExecuteNonQuery());
	}

	// ---- async ----

	sealed class OpenDbConnection(DbCommand command) : DbConnection
	{
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
		protected override DbCommand CreateDbCommand() => command;
	}

	[Fact]
	public static async Task Async_UsesTheAsyncLookup_AndTheNamesReachTheCommand()
	{
		var filter = new Filter();
		var order = new Order();

		var dbCommand = new RecordingDbCommand();
		var proc = new ProcDbCommand(new OpenDbConnection(dbCommand), "dbo.search", "@company_serial_num", "@customer_name");
		await proc
			.AddParam(filter.CompanySerialNum)
			.AddParam(order.Customer.Name)
			.ExecuteNonQueryAsync();

		Assert.Equal(1, proc.AsyncLookups);
		Assert.Equal(0, proc.SyncLookups);
		Assert.Equal(["@company_serial_num", "@customer_name"], dbCommand.Sent.Select(p => p.ParameterName));
		Assert.Equal([42, "ann"], dbCommand.Sent.Select(p => p.Value));
	}

	// A concrete command that records the parameters it is given (a substitute cannot create parameters).
	sealed class RecordingDbCommand : DbCommand
	{
		public List<DbParameter> Sent { get; } = [];

		[AllowNull]
		public override string CommandText { get; set; } = "";
		public override int CommandTimeout { get; set; }
		public override CommandType CommandType { get; set; }
		public override bool DesignTimeVisible { get; set; }
		public override UpdateRowSource UpdatedRowSource { get; set; }
		protected override DbConnection? DbConnection { get; set; }
		protected override DbParameterCollection DbParameterCollection => _parameters ??= new RecordedParameters(Sent);
		protected override DbTransaction? DbTransaction { get; set; }
		RecordedParameters? _parameters;

		public override void Cancel() { }
		public override int ExecuteNonQuery() => 1;
		public override object? ExecuteScalar() => null;
		public override void Prepare() { }
		protected override DbParameter CreateDbParameter() => new Parameter();
		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

		sealed class Parameter : DbParameter
		{
			public override DbType DbType { get; set; }
			public override ParameterDirection Direction { get; set; }
			public override bool IsNullable { get; set; }
			[AllowNull]
			public override string ParameterName { get; set; } = "";
			[AllowNull]
			public override string SourceColumn { get; set; } = "";
			public override object? Value { get; set; }
			public override bool SourceColumnNullMapping { get; set; }
			public override int Size { get; set; }
			public override void ResetDbType() { }
		}

		sealed class RecordedParameters(List<DbParameter> list) : DbParameterCollection
		{
			public override int Count => list.Count;
			public override object SyncRoot => list;
			public override int Add(object value) { list.Add((DbParameter)value); return list.Count - 1; }
			public override void AddRange(Array values) { foreach (object v in values) Add(v); }
			public override void Clear() => list.Clear();
			public override bool Contains(object value) => list.Contains((DbParameter)value);
			public override bool Contains(string value) => list.Exists(p => p.ParameterName == value);
			public override void CopyTo(Array array, int index) => throw new NotSupportedException();
			public override System.Collections.IEnumerator GetEnumerator() => list.GetEnumerator();
			public override int IndexOf(object value) => list.IndexOf((DbParameter)value);
			public override int IndexOf(string parameterName) => list.FindIndex(p => p.ParameterName == parameterName);
			public override void Insert(int index, object value) => list.Insert(index, (DbParameter)value);
			public override void Remove(object value) => list.Remove((DbParameter)value);
			public override void RemoveAt(int index) => list.RemoveAt(index);
			public override void RemoveAt(string parameterName) => list.RemoveAt(IndexOf(parameterName));
			protected override DbParameter GetParameter(int index) => list[index];
			protected override DbParameter GetParameter(string parameterName) => list[IndexOf(parameterName)];
			protected override void SetParameter(int index, DbParameter value) => list[index] = value;
			protected override void SetParameter(string parameterName, DbParameter value) => list[IndexOf(parameterName)] = value;
		}
	}

	// ---- a name that cannot be derived ----

	[Fact]
	public static void ArgumentsThatAreNotMemberChains_ThrowAtTheCallSite_TellingYouToNameTheParameter()
	{
		var filter = new Filter { EndDate = DateTime.Today };
		int[] values = [1, 2];
		int a = 1, b = 2;
		string Text() => "x";
		var cmd = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		void Throws(Action add, string expression)
		{
			var ex = Assert.Throws<ArgumentException>(add);
			Assert.Equal("value", ex.ParamName);
			Assert.Contains(expression, ex.Message);
			Assert.Contains("AddParam(\"name\", value)", ex.Message);
		}

		Throws(() => cmd.AddParam(filter.EndDate?.AddDays(1)), "filter.EndDate?.AddDays(1)"); // a call
		Throws(() => cmd.AddParam(values[0]), "values[0]");                                    // an index
		Throws(() => cmd.AddParam(a + b), "a + b");                                            // an operator
		Throws(() => cmd.AddParam(1), "1");                                                    // a literal
		Throws(() => cmd.AddParam("@name"), "\"@name\"");                                      // a string literal: this used to be read as a name
		Throws(() => cmd.AddParam(nameof(a)), "nameof(a)");
		Throws(() => cmd.AddParam(Text()), "Text()");
		Throws(() => cmd.AddParam((long)a), "(long)a");                                        // a cast
		Throws(() => cmd.AddParam(filter.Region ?? "none"), "filter.Region ?? \"none\"");
		Throws(() => cmd.AddParam(default(int)), "default(int)");
		Throws(() => cmd.AddParam(true), "true");

		// AddParamIf checks the argument even when the condition is false, so a mistake is not hidden by the data.
		Throws(() => cmd.AddParamIf(false, a + b), "a + b");

		Assert.Empty(cmd.Params);
	}

	[Theory]
	[InlineData("startDate", "startDate")]
	[InlineData("filter.CompanySerialNum", "CompanySerialNum")]
	[InlineData("  filter . CompanySerialNum  ", "CompanySerialNum")]
	[InlineData("maybe?.X", "X")]
	[InlineData("maybe!.X", "X")]
	[InlineData("maybe?.Inner?.X", "X")]
	[InlineData("maybe!.Inner!.X", "X")]
	[InlineData("x!", "x")]
	[InlineData("(filter.X)", "X")]
	[InlineData("((filter.X))", "X")]
	[InlineData("( filter.X )", "X")]
	[InlineData("@class", "class")]
	[InlineData("@class.@event", "event")]
	[InlineData("this.Name", "Name")]
	[InlineData("order.Customer.Id", "Id")]
	[InlineData("a.b.c.d.e", "e")]
	[InlineData("_private", "_private")]
	[InlineData("Pedido.Cliente.Código", "Código")]
	public static void DerivableExpressions_GiveTheLastMember(string expression, string last)
	{
		var cmd = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		cmd.AddParam(value: 1, expression: expression);

		Assert.Equal(last, Assert.Single(cmd.Params).Name);
	}

	[Theory]
	[InlineData("")]
	[InlineData("   ")]
	[InlineData("a.")]
	[InlineData(".a")]
	[InlineData("a..b")]
	[InlineData("a b")]
	[InlineData("this")]
	[InlineData("a.this")]
	[InlineData("null")]
	[InlineData("true")]
	[InlineData("false")]
	[InlineData("default")]
	[InlineData("default(int)")]
	[InlineData("new Foo()")]
	[InlineData("typeof(int)")]
	[InlineData("nameof(x)")]
	[InlineData("x()")]
	[InlineData("x.Y()")]
	[InlineData("x[0]")]
	[InlineData("x?[0]")]
	[InlineData("a + b")]
	[InlineData("-a")]
	[InlineData("!a")]
	[InlineData("a ?? b")]
	[InlineData("a ? b : c")]
	[InlineData("(int)a")]
	[InlineData("(a)b")]
	[InlineData("(a.b")]
	[InlineData("a.b)")]
	[InlineData("x => x")]
	[InlineData("1")]
	[InlineData("1.5")]
	[InlineData("\"text\"")]
	[InlineData("'c'")]
	[InlineData("$\"{a}\"")]
	[InlineData("a<b>")]
	[InlineData("a.B<int>")]
	[InlineData("a++")]
	[InlineData("await x")]
	[InlineData("a.b ?")]
	public static void NonDerivableExpressions_Throw(string expression)
	{
		var cmd = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		var ex = Assert.Throws<ArgumentException>(() => cmd.AddParam(value: 1, expression: expression));

		Assert.Equal("value", ex.ParamName);
		Assert.Empty(cmd.Params);
	}

	[Fact]
	public static void NoExpression_Throws()
	{
		var cmd = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		Assert.Throws<ArgumentException>(() => cmd.AddParam(1, expression: null));
	}

	// ---- AddParamIf ----

	[Fact]
	public static void AddParamIf_AddsADerivedParam_OnlyWhenTheConditionHolds()
	{
		var filter = new Filter { EndDate = new DateTime(2026, 1, 2) };
		var probe = new ConnectionProbe(ConnectionState.Open);

		Proc(probe, "@end_date", "@region")
			.AddParamIf(filter.EndDate.HasValue, filter.EndDate)
			.AddParamIf(filter.Region is null, filter.Region)
			.ExecuteNonQuery();

		Assert.Equal(["@end_date"], SentNames(probe));
		Assert.Equal([new DateTime(2026, 1, 2)], probe.Parameters.Select(p => p.Value));
	}

	// ---- the explicit-name overloads are untouched ----

	[Fact]
	public static void ExplicitNames_StillBindToTheNamedOverloads_NeverToTheDerivingOne()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@never");

		proc
			.AddParam("a", "text")                 // string value: used to be at risk of being read as (value, expression)
			.AddParam("b", 0)
			.AddParam("c", 5L)
			.AddParam("d", (object?)null)
			.AddParam("e", ParameterDirection.Output)
			.AddParamIf(true, "f", "text")
			.AddParamIf(true, "g", 0)
			.AddParam<int>("h", 3, null)
			.ExecuteNonQuery();

		Assert.Equal(["a", "b", "c", "d", "e", "f", "g", "h"], SentNames(probe));
		Assert.Equal(["text", 0, 5L, DBNull.Value, null, "text", 0, 3], probe.Parameters.Select(p => p.Value is DBNull ? DBNull.Value : p.Value));
		Assert.Equal(0, proc.Lookups); // nothing was derived, so the provider was never asked
	}

	// ---- a NULL carries no type, so the argument's type is sent with it ----

	[Fact]
	public static void NullArguments_AreSentWithTheDatabaseTypeOfTheArgument()
	{
		byte[]? blob = null;
		int? count = null;
		string? text = null;
		DateTime? stamp = null;
		Guid? key = null;
		decimal? price = null;
		bool? flag = null;
		DayOfWeek? day = null; // an enum is sent as its underlying type
		var probe = new ConnectionProbe(ConnectionState.Open);

		Proc(probe)
			.AddParam(blob).AddParam(count).AddParam(text).AddParam(stamp).AddParam(key).AddParam(price).AddParam(flag).AddParam(day)
			.ExecuteNonQuery();

		Assert.Equal(
			[DbType.Binary, DbType.Int32, DbType.String, DbType.DateTime, DbType.Guid, DbType.Decimal, DbType.Boolean, DbType.Int32],
			probe.Parameters.Select(p => p.DbType));
		Assert.All(probe.Parameters, p => Assert.Equal(DBNull.Value, p.Value));
	}

	[Fact]
	public static void ValuesAndExplicitTypes_AreLeftAlone()
	{
		var filter = new Filter();
		int? present = 5;
		var probe = new ConnectionProbe(ConnectionState.Open);

		Proc(probe)
			.AddParam(filter.CompanySerialNum)                 // a value: the provider types it
			.AddParam(present)                                 // a nullable with a value
			.AddParam("@by_name", (object?)null)               // by name: no static type to use
			.AddParam("@typed", (object)null!, DbType.Int64)    // an explicit type always wins
			.ExecuteNonQuery();

		Assert.Equal(default, probe.Parameters[0].DbType);
		Assert.Equal(default, probe.Parameters[1].DbType);
		Assert.Equal(default, probe.Parameters[2].DbType);
		Assert.Equal(DbType.Int64, probe.Parameters[3].DbType);
	}

	[Fact]
	public static void AddParams_NullMembers_AreSentWithTheTypeOfTheMember()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Proc(probe, "@end_date", "@region", "@company_serial_num").AddParams(new SearchArgs { Region = null }).ExecuteNonQuery();

		Assert.Equal(DbType.DateTime, probe.Parameters.Single(p => p.ParameterName == "@end_date").DbType);   // DateTime? property, null
		Assert.Equal(DbType.String, probe.Parameters.Single(p => p.ParameterName == "@region").DbType);        // string? field, null
		Assert.Equal(default, probe.Parameters.Single(p => p.ParameterName == "@company_serial_num").DbType);  // int, a value
	}

	[Fact]
	public static void ATypedNull_KeepsItsTypeThroughNameResolution()
	{
		DateTime? stamp = null;
		var probe = new ConnectionProbe(ConnectionState.Open);

		Proc(probe, "@stamp").AddParam(stamp).ExecuteNonQuery();

		Assert.Equal(["@stamp"], SentNames(probe));
		Assert.Equal(DbType.DateTime, probe.Parameters[0].DbType);
	}

	// ---- AddParams(object): the members of an object ----

	sealed class SearchArgs
	{
		public int CompanySerialNum { get; set; } = 42;     // a property
		public string? Region = "west";                     // a field
		public DateTime? EndDate { get; set; }              // a nullable property
		public string Unrelated = "not a parameter";        // the procedure does not declare it
		public static int Shared = 1;                       // static: ignored
		public int this[int index] => index;                // an indexer: ignored
#pragma warning disable CS0414 // never used: it is private, which is the point
		int _hidden = 5;                                    // not public: ignored
#pragma warning restore CS0414
		public override string ToString() => _hidden.ToString();
	}

	readonly record struct RecordArgs(int CompanySerialNum, string Region);

	class BaseArgs { public int CompanySerialNum { get; set; } = 1; }

	sealed class DerivedArgs : BaseArgs { public string Extra = "x"; }

	[Fact]
	public static void AddParams_SendsOnlyTheMembersTheProcedureDeclares_FoundByTheSameTiers()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		Proc(probe, "@company_serial_num", "@REGION", "@end_date", "@something_else")
			.AddParams(new SearchArgs())
			.ExecuteNonQuery();

		// Property, field and nullable property found by underscores, case and underscores; Unrelated is not sent; @something_else is left to its default.
		Assert.Equal(new Dictionary<string, object?> { ["@company_serial_num"] = 42, ["@REGION"] = "west", ["@end_date"] = DBNull.Value }, Sent(probe));
	}

	[Fact]
	public static void AddParams_AParameterNamedByTheCaller_WinsOverTheMember()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var filter = new Filter { Region = "east" };
		Proc(probe, "@company_serial_num", "@region")
			.AddParams(new SearchArgs())
			.AddParam("@region", "override")      // by name
			.ExecuteNonQuery();

		Assert.Equal(new Dictionary<string, object?> { ["@company_serial_num"] = 42, ["@region"] = "override" }, Sent(probe));
		Assert.Equal(2, probe.Parameters.Count);

		// The same when the caller's parameter is derived from an argument.
		var probe2 = new ConnectionProbe(ConnectionState.Open);
		Proc(probe2, "@company_serial_num", "@region")
			.AddParams(new SearchArgs())
			.AddParam(filter.Region)               // derived: finds @region
			.ExecuteNonQuery();

		Assert.Equal(new Dictionary<string, object?> { ["@company_serial_num"] = 42, ["@region"] = "east" }, Sent(probe2));
		Assert.Equal(2, probe2.Parameters.Count);
	}

	[Fact]
	public static void AddParams_WithoutRealNames_SendsEveryPublicInstanceMemberAsWritten()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		Proc(probe /* the provider supplies no names */).AddParams(new SearchArgs()).ExecuteNonQuery();

		Assert.Equal(
			new Dictionary<string, object?> { ["CompanySerialNum"] = 42, ["Region"] = "west", ["EndDate"] = DBNull.Value, ["Unrelated"] = "not a parameter" },
			Sent(probe));

		// A text command has no procedure to ask: the same.
		var textProbe = new ConnectionProbe(ConnectionState.Open);
		var text = new ProcCommand(textProbe.Connection, CommandType.Text, "select @CompanySerialNum", "@never");
		text.AddParams(new RecordArgs(7, "r")).ExecuteNonQuery();
		Assert.Equal(new Dictionary<string, object?> { ["CompanySerialNum"] = 7, ["Region"] = "r" }, Sent(textProbe));
		Assert.Equal(0, text.Lookups);
	}

	[Fact]
	public static void AddParams_Members_AreReadWhenTheMethodIsCalled()
	{
		var args = new SearchArgs { CompanySerialNum = 1 };
		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@company_serial_num");
		proc.AddParams(args);
		args.CompanySerialNum = 2; // too late

		proc.ExecuteNonQuery();

		Assert.Equal([1], probe.Parameters.Select(p => p.Value));
	}

	[Fact]
	public static void AddParams_AcceptsARecordStruct_AndUsesTheStaticTypesMembers()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		Proc(probe, "@company_serial_num", "@region", "@extra").AddParams(new RecordArgs(7, "r")).ExecuteNonQuery();
		Assert.Equal(new Dictionary<string, object?> { ["@company_serial_num"] = 7, ["@region"] = "r" }, Sent(probe));

		// The members are those of the static type: a Derived held as a Base sends only Base's members.
		var baseProbe = new ConnectionProbe(ConnectionState.Open);
		BaseArgs asBase = new DerivedArgs();
		Proc(baseProbe, "@company_serial_num", "@extra").AddParams(asBase).ExecuteNonQuery();
		Assert.Equal(new Dictionary<string, object?> { ["@company_serial_num"] = 1 }, Sent(baseProbe));

		var derivedProbe = new ConnectionProbe(ConnectionState.Open);
		Proc(derivedProbe, "@company_serial_num", "@extra").AddParams(new DerivedArgs()).ExecuteNonQuery();
		Assert.Equal(new Dictionary<string, object?> { ["@company_serial_num"] = 1, ["@extra"] = "x" }, Sent(derivedProbe));
	}

	[Fact]
	public static void AddParams_TwoMembersThatResolveToOneParameter_Throws()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		ProcCommand proc = Proc(probe, "@foo_bar");
		proc.AddParams(new TwoSpellings());

		var ex = Assert.Throws<InvalidOperationException>(() => proc.ExecuteNonQuery());
		Assert.Contains("Two parameters resolve to '@foo_bar'", ex.Message);
	}

	sealed class TwoSpellings
	{
		public int FooBar = 1;
		public int Foo_Bar = 2;
	}

	[Fact]
	public static void AddParams_Null_Throws_AndAnObjectWithNoMembersAddsNothing()
	{
		var cmd = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		var ex = Assert.Throws<ArgumentNullException>(() => cmd.AddParams<SearchArgs>(null!));
		Assert.Equal("source", ex.ParamName);

		cmd.AddParams(new object());
		Assert.Empty(cmd.Params);
	}

	[Fact]
	public static void AddParams_MixesWithDerivedAndNamedParams_InAnyOrder()
	{
		var filter = new Filter();
		var probe = new ConnectionProbe(ConnectionState.Open);
		Proc(probe, "@mode", "@company_serial_num", "@region", "@limit")
			.AddParam("@mode", 1)
			.AddParams(new SearchArgs())
			.AddParam(filter.Limit)
			.ExecuteNonQuery();

		Assert.Equal(new Dictionary<string, object?> { ["@mode"] = 1, ["@company_serial_num"] = 42, ["@region"] = "west", ["@limit"] = 10 }, Sent(probe));
	}
}
