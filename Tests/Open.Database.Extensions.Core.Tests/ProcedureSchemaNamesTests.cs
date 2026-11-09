using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// A command made over any DbConnection (ExpressiveDbCommand, and ExpressiveCommand when the connection it holds is a DbConnection) looks up the parameter names of a stored procedure
// through the ADO.NET schema collection "ProcedureParameters", so AddParam(orderId) finds @order_id without a SQL-specific command. The fake provider below answers that collection
// with a DataTable (the columns INFORMATION_SCHEMA.PARAMETERS has); real SQL Server is covered by the probe, which also runs it inside a transaction.
[ExcludeFromCodeCoverage]
public static class ProcedureSchemaNamesTests
{
	sealed class ProviderFailure(string message) : DbException(message);

	sealed class SchemaConnection(DataTable? table, Exception? failure) : DbConnection
	{
		public DerivedParameterNameTests.RecordingDbCommand Command { get; } = new();
		public int SchemaCalls { get; private set; }
		public string? Collection { get; private set; }
		public string?[]? Restrictions { get; private set; }

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
		protected override DbCommand CreateDbCommand() => Command;

		public override DataTable GetSchema(string collectionName, string?[] restrictionValues)
		{
			SchemaCalls++;
			Collection = collectionName;
			Restrictions = restrictionValues;
			if (failure is not null) throw failure;
			return table ?? throw new NotSupportedException();
		}
	}

	// The columns of INFORMATION_SCHEMA.PARAMETERS that matter: a row per parameter, ordinal 0 is the return value.
	static DataTable Table(params object?[][] rows)
	{
		var table = new DataTable("ProcedureParameters");
		table.Columns.Add("SPECIFIC_SCHEMA", typeof(string));
		table.Columns.Add("SPECIFIC_NAME", typeof(string));
		table.Columns.Add("ORDINAL_POSITION", typeof(int));
		table.Columns.Add("PARAMETER_NAME", typeof(string));
		foreach (object?[] row in rows) table.Rows.Add([.. row.Select(v => v ?? DBNull.Value)]);
		return table;
	}

	static object?[] Row(int ordinal, string? name, string schema = "dbo") => [schema, "proc", ordinal, name];

	static string Proc() => ParameterNameHarness.NewProcedure(); // dbo.namesN: a name per test, as remembered names are shared by every command on one connection

	static string[] Sent(SchemaConnection connection) => [.. connection.Command.Sent.Select(p => p.ParameterName)];

	static string[] Run(SchemaConnection connection, string procedure, Action<ExpressiveDbCommand> add)
	{
		connection.Command.Sent.Clear();
		var command = new ExpressiveDbCommand(connection, CommandType.StoredProcedure, procedure);
		add(command);
		command.ExecuteNonQuery();
		return Sent(connection);
	}

	static async Task<string[]> RunAsync(SchemaConnection connection, string procedure, Action<ExpressiveDbCommand> add)
	{
		connection.Command.Sent.Clear();
		var command = new ExpressiveDbCommand(connection, CommandType.StoredProcedure, procedure);
		add(command);
		await command.ExecuteNonQueryAsync();
		return Sent(connection);
	}

	// ---- the names ----

	[Fact]
	public static void TheNamesOfTheSchemaCollection_AreMatched_ReturnValueSkipped_InOrdinalOrder_AndSentAsTheProviderSpellsThem()
	{
		const int orderId = 5;
		const string name = "ann";
		var connection = new SchemaConnection(Table(Row(2, "@p_name"), Row(0, ""), Row(3, null), Row(1, "@order_id"), Row(0, "@RETURN_VALUE")), null);

		Assert.Equal(["@order_id", "@p_name"], Run(connection, Proc(), c => c.AddParam(orderId).AddParam(name)));
		Assert.Equal([5, "ann"], connection.Command.Sent.Select(p => p.Value));
		Assert.Equal(1, connection.SchemaCalls);
		Assert.Equal("ProcedureParameters", connection.Collection);
	}

	[Fact]
	public static void ANameWithoutAnAt_IsSentWithoutOne()
	{
		const int orderId = 5;
		const string name = "ann";
		var connection = new SchemaConnection(Table(Row(1, "order_id"), Row(2, "@p_name")), null);

		Assert.Equal(["order_id", "@p_name"], Run(connection, Proc(), c => c.AddParam(orderId).AddParam(name)));
	}

	[Fact]
	public static void ANameThatMatchesNothing_ThrowsLikeAStoredProcedureOfASqlCommand()
	{
		const int unrelated = 1;
		var connection = new SchemaConnection(Table(Row(1, "@order_id")), null);

		var ex = Assert.Throws<InvalidOperationException>(() => Run(connection, Proc(), c => c.AddParam(unrelated)));

		Assert.StartsWith("The argument 'unrelated' matches none of the parameters of 'dbo.names", ex.Message);
		Assert.Contains("The procedure declares: @order_id.", ex.Message);
	}

	[Fact]
	public static void AMemberOfAnObjectTheProcedureDoesNotDeclare_IsNotSent()
	{
		var connection = new SchemaConnection(Table(Row(1, "@order_id"), Row(2, "@p_name")), null);

		Assert.Equal(["@order_id", "@p_name"], Run(connection, Proc(), c => c.AddParams(new { OrderId = 5, Name = "ann", Extra = 1 })));
	}

	[Fact]
	public static void AnExplicitName_NeedsNoLookupResult_AndIsKept()
	{
		var connection = new SchemaConnection(Table(Row(1, "@order_id")), null);

		Assert.Equal(["@anything"], Run(connection, Proc(), c => c.AddParam("@anything", 1)));
	}

	[Fact]
	public static async Task Async_FindsTheSameNames()
	{
		const int orderId = 5;
		const string name = "ann";
		var connection = new SchemaConnection(Table(Row(1, "@order_id"), Row(2, "@p_name")), null);

		Assert.Equal(["@order_id", "@p_name"], await RunAsync(connection, Proc(), c => c.AddParam(orderId).AddParam(name)));
		Assert.Equal(1, connection.SchemaCalls);
	}

	// ---- the procedure name: which restrictions are asked ----

	[Theory]
	[InlineData("names1", null, "names1")]
	[InlineData("dbo.names1", "dbo", "names1")]
	[InlineData("[dbo].[names1]", "dbo", "names1")]
	[InlineData("\"dbo\".\"names1\"", "dbo", "names1")]
	[InlineData("[my.schema].[my proc]", "my.schema", "my proc")]
	[InlineData("\"dbo\".\"my.proc\"", "dbo", "my.proc")]
	[InlineData("[sales].[a]]b]", "sales", "a]b")]
	[InlineData("names1;1", null, "names1;1")] // nothing is stripped that the provider could still resolve
	public static void TheProcedureName_IsSplitIntoSchemaAndName_ForTheRestrictions(string procedure, string? schema, string name)
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(1, "@order_id")), null);

		Assert.Equal(["@order_id"], Run(connection, procedure, c => c.AddParam(orderId)));
		Assert.Equal(new string?[] { null, schema, name, null }, connection.Restrictions);
	}

	[Theory]
	[InlineData("otherdb.dbo.names1")] // a database part: the lookup would run in the current database
	[InlineData("[otherdb].[dbo].[names1]")]
	[InlineData("srv.otherdb.dbo.names1")]
	[InlineData("dbo..names1")]
	[InlineData("dbo.")]
	[InlineData(".names1")]
	public static void ADatabasePartOrAnEmptyPart_IsNotLookedUp_TheNameIsUsedAsWritten(string procedure)
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(1, "@order_id")), null);

		Assert.Equal(["orderId"], Run(connection, procedure, c => c.AddParam(orderId)));
		Assert.Equal(0, connection.SchemaCalls);
	}

	// ---- not known ----

	[Fact]
	public static void WithoutASchema_RowsFromMoreThanOneSchema_AreAmbiguous_TheNameIsUsedAsWritten()
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(1, "@order_id", "dbo"), Row(1, "@order_id", "sales")), null);

		Assert.Equal(["orderId"], Run(connection, "names1", c => c.AddParam(orderId)));
		Assert.Equal(1, connection.SchemaCalls);
	}

	[Fact]
	public static void WithoutASchema_AProcedureOfTheSameNameWithoutParametersInAnotherSchema_IsAmbiguousToo()
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(0, "", "sales"), Row(1, "@order_id", "dbo")), null);

		Assert.Equal(["orderId"], Run(connection, "names1", c => c.AddParam(orderId)));
	}

	[Fact]
	public static void WithoutASchema_RowsFromOneSchema_AreUsed()
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(1, "@order_id", "sales"), Row(2, "@p_name", "sales")), null);

		Assert.Equal(["@order_id"], Run(connection, "names1", c => c.AddParam(orderId)));
	}

	[Fact]
	public static void ATableWithoutParameters_IsNotKnown_AndNotRemembered()
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(0, "")), null);
		string procedure = Proc();

		Assert.Equal(["orderId"], Run(connection, procedure, c => c.AddParam(orderId)));
		Assert.Equal(["orderId"], Run(connection, procedure, c => c.AddParam(orderId)));
		Assert.Equal(2, connection.SchemaCalls); // an empty answer is never remembered
	}

	public static TheoryData<Exception> Unsupported => new()
	{
		new NotSupportedException(),
		new NotImplementedException(),
		new ArgumentException("The requested collection (ProcedureParameters) is not defined."),
		new InvalidOperationException("The schema collection 'ProcedureParameters' is not supported by this provider."),
	};

	[Theory]
	[MemberData(nameof(Unsupported), DisableDiscoveryEnumeration = true)]
	public static void AProviderThatDoesNotSupportTheCollection_IsNotKnown_TheNameIsUsedAsWritten(Exception failure)
	{
		const int orderId = 5;
		var connection = new SchemaConnection(null, failure);

		Assert.Equal(["orderId"], Run(connection, Proc(), c => c.AddParam(orderId)));
		Assert.Equal(1, connection.SchemaCalls);
	}

	[Fact]
	public static async Task AProviderThatDoesNotSupportTheCollection_IsNotKnown_OnTheAsyncPathToo()
	{
		const int orderId = 5;
		var connection = new SchemaConnection(null, new NotSupportedException());

		Assert.Equal(["orderId"], await RunAsync(connection, Proc(), c => c.AddParam(orderId)));
	}

	[Fact]
	public static void ADatabaseError_PropagatesUnchanged_WithTheProcedureNameInData()
	{
		const int orderId = 5;
		var failure = new ProviderFailure("the server said no");
		var connection = new SchemaConnection(null, failure);
		string procedure = Proc();

		var ex = Assert.Throws<ProviderFailure>(() => Run(connection, procedure, c => c.AddParam(orderId)));

		Assert.Same(failure, ex);
		Assert.Equal(procedure, ex.Data["Procedure"]);
	}

	[Fact]
	public static async Task ADatabaseError_PropagatesUnchanged_OnTheAsyncPathToo()
	{
		const int orderId = 5;
		var failure = new ProviderFailure("the server said no");
		var connection = new SchemaConnection(null, failure);
		string procedure = Proc();

		var ex = await Assert.ThrowsAsync<ProviderFailure>(() => RunAsync(connection, procedure, c => c.AddParam(orderId)));

		Assert.Same(failure, ex);
		Assert.Equal(procedure, ex.Data["Procedure"]);
	}

	[Fact]
	public static void AnInvalidOperationThatDoesNotSayTheCollectionIsUnsupported_PropagatesWithTheProcedureName()
	{
		const int orderId = 5;
		var failure = new InvalidOperationException("The connection is busy.");
		var connection = new SchemaConnection(null, failure);
		string procedure = Proc();

		var ex = Assert.Throws<InvalidOperationException>(() => Run(connection, procedure, c => c.AddParam(orderId)));

		Assert.Same(failure, ex);
		Assert.Equal(procedure, ex.Data["Procedure"]);
	}

	// ---- remembered like the other lookups ----

	[Fact]
	public static void TheNames_AreRemembered_ForTheNextCommandOnTheSameConnection()
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(1, "@order_id")), null);
		string procedure = Proc();

		Assert.Equal(["@order_id"], Run(connection, procedure, c => c.AddParam(orderId)));
		Assert.Equal(["@order_id"], Run(connection, procedure, c => c.AddParam(orderId)));
		Assert.Equal(1, connection.SchemaCalls);
	}

	// ---- ExpressiveCommand: through the IDbConnection it holds, when that is a DbConnection ----

	[Fact]
	public static void ExpressiveCommand_OverADbConnection_LooksTheNamesUpToo()
	{
		const int orderId = 5;
		var connection = new SchemaConnection(Table(Row(1, "@order_id")), null);
		IDbConnection asInterface = connection;

		new ExpressiveCommand(asInterface, CommandType.StoredProcedure, Proc()).AddParam(orderId).ExecuteNonQuery();

		Assert.Equal(["@order_id"], Sent(connection));
		Assert.Equal(1, connection.SchemaCalls);
	}

	[Fact]
	public static void ExpressiveCommand_OverAConnectionThatIsNotADbConnection_UsesTheNameAsWritten()
	{
		const int orderId = 5;
		var probe = new ConnectionProbe(ConnectionState.Open);

		new ExpressiveCommand(probe.Connection, CommandType.StoredProcedure, Proc()).AddParam(orderId).ExecuteNonQuery();

		Assert.Equal(["orderId"], probe.Parameters.Select(p => p.ParameterName));
	}
}
