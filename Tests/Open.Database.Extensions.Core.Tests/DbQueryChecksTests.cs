using Microsoft.Data.Sqlite;

using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The checks a DbQuery, a DbQueryUsing, a DbQueryCall and a command make of what they are given: which exception, which message, which argument name,
// in which order. They share their code, and these pin what a caller sees.
[ExcludeFromCodeCoverage]
public static class DbQueryChecksTests
{
	const string BlankName = "Parameter names cannot be empty or white space.";
	const string BlankCommand = "The command cannot be empty or white space.";
	const string BlankCommandOfACommand = "Cannot be null or whitespace.";

	static CancellationToken Token => TestContext.Current.CancellationToken;

	// The message of an argument exception, without the argument name the framework appends to it (" (Parameter 'name')").
	static string Text(ArgumentException ex)
	{
		string message = ex.Message;
		int end = message.IndexOf(" (Parameter '", StringComparison.Ordinal);
		return end < 0 ? message : message[..end];
	}

	[Theory]
	[InlineData("")]
	[InlineData("  ")]
	public static void DbQueryText_ABlankCommand_IsRefused_WithItsMessageAndName(string text)
	{
		ArgumentException ex = Assert.Throws<ArgumentException>(() => DbQuery.Text(text));
		Assert.Equal("sql", ex.ParamName);
		Assert.Equal(BlankCommand, Text(ex));
	}

	[Fact]
	public static void DbQueryText_ANullCommand_IsANullArgument_NotABlankOne()
	{
		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => DbQuery.Text(null!));
		Assert.Equal("sql", ex.ParamName);
	}

	[Theory]
	[InlineData("")]
	[InlineData("\t")]
	public static void DbQueryProcedure_ABlankName_IsRefused_WithTheCommandMessage(string name)
	{
		ArgumentException ex = Assert.Throws<ArgumentException>(() => DbQuery.Procedure(name));
		Assert.Equal("name", ex.ParamName);
		Assert.Equal(BlankCommand, Text(ex));
	}

	[Fact]
	public static void DbQueryProcedure_ANullName_IsANullArgument()
	{
		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => DbQuery.Procedure(null!));
		Assert.Equal("name", ex.ParamName);
	}

	static readonly DbQuery Q = DbQuery.Text("select 1");

	[Theory]
	[InlineData("")]
	[InlineData("   ")]
	public static void AParamName_Blank_IsRefused_OnEveryFormOfTheCall(string name)
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		DbQueryUsing bound = Q.Using(connection);
		DbQueryCall call = bound.AddParam("first", 1);

		foreach (Action add in new Action[]
		{
			() => bound.AddParam(name, 1),
			() => bound.AddParam(name, "text"),
			() => call.AddParam(name, 1),
			() => call.AddParam(name, "text"),
			() => Q.Map<int>().AddParam(name, 1),
		})
		{
			ArgumentException ex = Assert.Throws<ArgumentException>(add);
			Assert.Equal("name", ex.ParamName);
			Assert.Equal(BlankName, Text(ex));
		}
	}

	[Fact]
	public static void AParamName_Null_IsANullArgument_OnEveryFormOfTheCall()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		DbQueryUsing bound = Q.Using(connection);
		DbQueryCall call = bound.AddParam("first", 1);

		foreach (Action add in new Action[]
		{
			() => bound.AddParam(null!, 1),
			() => bound.AddParam(null!, "text"),
			() => call.AddParam(null!, 1),
			() => call.AddParam(null!, "text"),
		})
		{
			ArgumentNullException ex = Assert.Throws<ArgumentNullException>(add);
			Assert.Equal("name", ex.ParamName);
		}
	}

	[Theory]
	[InlineData("")]
	[InlineData("  ")]
	public static void ACommandText_Blank_IsRefused_ByEveryConstructorWithItsMessage(string text)
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		DbConnection db = connection;

		foreach (Action make in new Action[]
		{
			() => _ = new ExpressiveDbCommand(db, CommandType.Text, text),
			() => _ = new ExpressiveDbCommand(db, null, CommandType.Text, text),
			() => _ = new ExpressiveDbCommand(() => db, CommandType.Text, text),
		})
		{
			ArgumentException ex = Assert.Throws<ArgumentException>(make);
			Assert.Equal("command", ex.ParamName);
			Assert.Equal(BlankCommandOfACommand, Text(ex));
		}
	}

	[Fact]
	public static void ACommandText_Null_IsANullArgument_ByEveryConstructor()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		DbConnection db = connection;

		foreach (Action make in new Action[]
		{
			() => _ = new ExpressiveDbCommand(db, CommandType.Text, null!),
			() => _ = new ExpressiveDbCommand(db, null, CommandType.Text, null!),
			() => _ = new ExpressiveDbCommand(() => db, CommandType.Text, null!),
		})
		{
			ArgumentNullException ex = Assert.Throws<ArgumentNullException>(make);
			Assert.Equal("command", ex.ParamName);
		}
	}

	[Fact]
	public static void AConstructor_ChecksTheConnectionBeforeTheCommand()
	{
		// Both are wrong: the connection is named first.
		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
			() => _ = new ExpressiveDbCommand((DbConnection)null!, CommandType.Text, null!));
		Assert.Equal("connection", ex.ParamName);
	}

	[Fact]
	public static void ADefaultBoundQuery_Throws_FromEveryStateMember_AtTheCall()
	{
		DbQueryUsing unbound = default;
		var sync = (IExecuteReaderSync)unbound;
		var reader = (IExecuteReader)unbound;

		Assert.Throws<InvalidOperationException>(
			() => sync.ExecuteReader(0, static (_, state) => state, CommandBehavior.Default));

		// A faulted task would be a different contract: these throw before they return.
		Assert.Throws<InvalidOperationException>(
			() => { _ = reader.ExecuteReaderAsync(0, static (_, state) => new ValueTask<int>(state), CommandBehavior.Default, Token).AsTask(); });
		Assert.Throws<InvalidOperationException>(
			() => { _ = reader.ExecuteReaderAsync(0, static (_, state) => new ValueTask<int>(state), CommandBehavior.Default).AsTask(); });
	}

	[Fact]
	public static void ADefaultBoundQuery_Throws_FromEveryAddAndMap()
	{
		DbQueryUsing unbound = default;

		Assert.Throws<InvalidOperationException>(() => unbound.AddParam("a", 1));
		Assert.Throws<InvalidOperationException>(() => unbound.AddParam("a", "text"));
		Assert.Throws<InvalidOperationException>(() => unbound.AddParams(new { A = 1 }));
		Assert.Throws<InvalidOperationException>(() => unbound.Map<int>());
		Assert.Throws<InvalidOperationException>(() => unbound.AsObjectArrays());
		Assert.Throws<InvalidOperationException>(() => unbound.AsDictionaryRows());
		Assert.Throws<InvalidOperationException>(() => unbound.Source);
	}

	[Fact]
	public static void ABoundQuery_IsEqual_ToTheSameDefinitionOnTheSameSource_AndHashesAsBefore()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		using var other = new SqliteConnection("Data Source=:memory:");

		DbQueryUsing a = Q.Using(connection);
		DbQueryUsing b = Q.Using(connection);

		Assert.Equal(a, b);
		Assert.True(a == b);
		Assert.NotEqual(a, Q.Using(other));
		Assert.NotEqual(a, DbQuery.Text("select 2").Using(connection));

		int expected = unchecked(Q.GetHashCode() * 31 + connection.GetHashCode());
		Assert.Equal(expected, a.GetHashCode());
		Assert.Equal(0, default(DbQueryUsing).GetHashCode());
	}

	[Fact]
	public static void ACall_IsEqual_ByDefinitionSourceAndValues()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		using var other = new SqliteConnection("Data Source=:memory:");

		DbQueryCall a = Q.Using(connection).AddParam("x", 1);
		DbQueryCall b = Q.Using(connection).AddParam("x", 1);

		Assert.Equal(a, b);
		Assert.Equal(a.GetHashCode(), b.GetHashCode());

		// The target's hash, times 31, plus the values': as it was before the two calls shared it.
		int target = unchecked(Q.GetHashCode() * 31 + connection.GetHashCode());
		Assert.Equal(unchecked(target * 31 + a.Params.EntriesHash()), a.GetHashCode());
		Assert.NotEqual(a, Q.Using(connection).AddParam("x", 2));
		Assert.NotEqual(a, Q.Using(other).AddParam("x", 1));
		Assert.NotEqual(a, DbQuery.Text("select 2").Using(connection).AddParam("x", 1));
	}
}
