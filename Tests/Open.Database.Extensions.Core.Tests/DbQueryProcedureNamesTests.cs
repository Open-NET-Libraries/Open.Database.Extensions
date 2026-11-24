using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// D2 probe: a stored procedure whose parameter names the provider answers with nothing is remembered as unknown by a command, so a repeat run does
// not ask the provider again until the short lifetime ends. A DbQuery remembers it the same way.
[ExcludeFromCodeCoverage]
public static class DbQueryProcedureNamesTests
{
	// An open connection whose schema lookup answers with an empty ProcedureParameters table (no names) and counts how often it is asked.
	sealed class Counting : DbConnection
	{
		public DerivedParameterNameTests.RecordingDbCommand Command { get; } = new();
		public int SchemaCalls { get; private set; }

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
			var table = new DataTable("ProcedureParameters");
			table.Columns.Add("SPECIFIC_SCHEMA", typeof(string));
			table.Columns.Add("SPECIFIC_NAME", typeof(string));
			table.Columns.Add("ORDINAL_POSITION", typeof(int));
			table.Columns.Add("PARAMETER_NAME", typeof(string));
			return table;
		}
	}

	[Fact]
	public static void ACommand_RemembersAnEmptyAnswer_SoARepeatRunDoesNotAskTheProviderAgain()
	{
		var connection = new Counting();
		string procedure = ParameterNameHarness.NewProcedure();
		const int orderId = 1; // a name to derive the parameter name from

		for (int i = 0; i < 2; i++)
		{
			var command = new ExpressiveDbCommand(connection, CommandType.StoredProcedure, procedure);
			command.AddParam(orderId);
			command.ExecuteNonQuery();
		}

		Assert.Equal(1, connection.SchemaCalls);
	}

	[Fact]
	public static void ADbQuery_RemembersAnEmptyAnswer_SoARepeatRunDoesNotAskTheProviderAgain()
	{
		var connection = new Counting();
		DbQuery query = DbQuery.Procedure(ParameterNameHarness.NewProcedure());
		const int orderId = 1; // a name to derive the parameter name from

		for (int i = 0; i < 2; i++)
		{
			// The fake command cannot read rows: the run ends in an exception after the names were resolved, which is all this probe needs.
			try { query.Map<int>().Using(connection).AddParam(orderId).FirstOrDefault(); }
			catch (Exception ex) when (ex is NotSupportedException or NotImplementedException or InvalidOperationException) { }
		}

		// As a command: one ask for two runs (the empty answer is remembered as unknown).
		Assert.Equal(1, connection.SchemaCalls);
	}

	[Fact]
	public static async Task ADbQuery_RemembersAnEmptyAnswer_OnTheAsyncPathToo()
	{
		var connection = new Counting();
		DbQuery query = DbQuery.Procedure(ParameterNameHarness.NewProcedure());
		const int orderId = 1; // a name to derive the parameter name from

		for (int i = 0; i < 2; i++)
		{
			try { await query.Map<int>().Using(connection).AddParam(orderId).FirstOrDefaultAsync(TestContext.Current.CancellationToken); }
			catch (Exception ex) when (ex is NotSupportedException or NotImplementedException or InvalidOperationException) { }
		}

		Assert.Equal(1, connection.SchemaCalls);
	}
}
