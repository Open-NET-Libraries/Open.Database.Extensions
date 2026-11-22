using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Tests;

#nullable enable

// R20 on a real server: one command object, run against two databases where
// the procedure of the same name takes differently named parameters.
// The first run's names used to be written into Params, so the second run
// sent them to a database that does not know them (SQL Server error 201 or
// 8145), and a member the first database did not take was gone for good.
// The two databases are tempdb and master of LocalDB, so no file is created.
// These tests never skip: a missing server is a failure.
[ExcludeFromCodeCoverage]
[Collection("LocalDB DDL")]
public static class DerivedNamesEveryRunSqlServerTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	const string First = "tempdb";
	const string Second = "master";

	static string ConnectionString(string database)
		=> new SqlConnectionStringBuilder(LocalDbFixture.ConnectionString)
		{
			InitialCatalog = database,
			Pooling = false,
		}.ConnectionString;

	static void Run(string database, string sql)
	{
		using var connection = new SqlConnection(ConnectionString(database));
		connection.Open();
		using SqlCommand command = connection.CreateCommand();
		command.CommandText = sql;
		command.ExecuteNonQuery();
	}

	// The same procedure name in both databases, each with its own
	// parameters and body, dropped when this goes.
	sealed class Procedures : IDisposable
	{
		public Procedures(
			string parametersOne,
			string bodyOne,
			string parametersTwo,
			string bodyTwo)
		{
			LocalDbFixture.RequireAvailable();
			Name = LocalDbFixture.UniqueName("ode_a6");
			Run(
				First,
				$"create procedure dbo.{Name} {parametersOne} as {bodyOne}");
			Run(
				Second,
				$"create procedure dbo.{Name} {parametersTwo} as {bodyTwo}");
		}

		public string Name { get; }

		public string Qualified => "dbo." + Name;

		public void Dispose()
		{
			Run(First, $"drop procedure if exists dbo.{Name}");
			Run(Second, $"drop procedure if exists dbo.{Name}");
		}
	}

	// The first database names the parameter @order_id, the second
	// @p_order_id; each returns what it was given.
	static Procedures DifferentlyNamed()
		=> new(
			"@order_id int",
			"select @order_id as one",
			"@p_order_id int",
			"select @p_order_id as one");

	static int Value(object? scalar) => Convert.ToInt32(scalar);

	[Fact]
	public static void AFactory_TwoDatabases_EachRunSendsItsOwnNames()
	{
		using Procedures procedures = DifferentlyNamed();
		string database = First;
		var command = new ExpressiveDbCommand(
			() => new SqlConnection(ConnectionString(database)),
			CommandType.StoredProcedure,
			procedures.Qualified);
		const int orderId = 5;
		command.AddParam(orderId);

		Assert.Equal(5, Value(command.ExecuteScalar()));

		database = Second;
		Assert.Equal(5, Value(command.ExecuteScalar()));

		database = First;
		Assert.Equal(5, Value(command.ExecuteScalar()));
		Assert.Equal(["orderId"], command.Params.Select(static p => p.Name));
	}

	[Fact]
	public static async Task AFactory_TwoDatabases_EachRunSendsItsOwnNames_Async()
	{
		using Procedures procedures = DifferentlyNamed();
		string database = First;
		var command = new ExpressiveDbCommand(
			() => new SqlConnection(ConnectionString(database)),
			CommandType.StoredProcedure,
			procedures.Qualified);
		const int orderId = 5;
		command.AddParam(orderId);

		Assert.Equal(5, Value(await command.ExecuteScalarAsync(Token)));

		database = Second;
		Assert.Equal(5, Value(await command.ExecuteScalarAsync(Token)));
	}

	[Fact]
	public static void ChangeDatabase_SameCommand_EachRunSendsItsOwnNames()
	{
		using Procedures procedures = DifferentlyNamed();
		using var connection = new SqlConnection(ConnectionString(First));
		connection.Open();
		var command = new ExpressiveDbCommand(
			connection,
			CommandType.StoredProcedure,
			procedures.Qualified);
		const int orderId = 5;
		command.AddParam(orderId);

		Assert.Equal(5, Value(command.ExecuteScalar()));

		connection.ChangeDatabase(Second);
		Assert.Equal(5, Value(command.ExecuteScalar()));

		connection.ChangeDatabase(First);
		Assert.Equal(5, Value(command.ExecuteScalar()));
		Assert.Equal(["orderId"], command.Params.Select(static p => p.Name));
	}

	[Fact]
	public static void ADroppedMember_ComesBack_OnTheDatabaseThatTakesIt()
	{
		// The first database does not take Region; the second does, and its
		// body shows whether it arrived.
		using var procedures = new Procedures(
			"@order_id int",
			"select @order_id as one",
			"@order_id int, @region nvarchar(20) = null",
			"select case when @region = N'west' then 1 else 0 end as one");
		string database = First;
		var command = new ExpressiveDbCommand(
			() => new SqlConnection(ConnectionString(database)),
			CommandType.StoredProcedure,
			procedures.Qualified);
		command.AddParams(new { OrderId = 5, Region = "west" });

		Assert.Equal(5, Value(command.ExecuteScalar()));

		database = Second;
		Assert.Equal(1, Value(command.ExecuteScalar()));
		Assert.Equal(
			["OrderId", "Region"],
			command.Params.Select(static p => p.Name));
	}
}
