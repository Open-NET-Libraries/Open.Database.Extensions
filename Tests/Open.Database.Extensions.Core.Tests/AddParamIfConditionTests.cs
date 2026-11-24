extern alias mssql;

namespace Open.Database.Extensions.Tests;

#nullable enable

// AddParamIf(condition, value) derives the parameter's name from the argument text, but only when the condition is true:
// a false condition adds nothing and never looks at the argument, so an argument whose name cannot be derived (here the literal 1) is no error then.
// Each command type that has the overload is covered, for a false and a true condition and for a name that can be derived.
[ExcludeFromCodeCoverage]
public static class AddParamIfConditionTests
{
	static ExpressiveCommand Classic()
		=> new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

	static ExpressiveDbCommand Db()
		=> new ConnectionProbe(ConnectionState.Open, dbConnection: true).Db!.Command("select 1");

#pragma warning disable CS0618 // System.Data.SqlClient is obsolete; it is one of the two packages under test
	static ExpressiveSqlCommand Legacy()
		=> new(new System.Data.SqlClient.SqlConnection(), CommandType.Text, "select 1");
#pragma warning restore CS0618

	static mssql::Open.Database.Extensions.ExpressiveSqlCommand Microsoft()
		=> new(new Microsoft.Data.SqlClient.SqlConnection(), CommandType.Text, "select 1");

	static void AssertNone<T>(IReadOnlyCollection<T> sent) => Assert.Empty(sent);

	[Fact]
	public static void FalseCondition_AddsNothing_AndNeverLooksAtTheArgument()
	{
		var classic = Classic();
		var db = Db();
		var legacy = Legacy();
		var microsoft = Microsoft();

		// The literal 1 has no derivable name: a true condition would throw (see the next test).
		Assert.Same(classic, classic.AddParamIf(false, 1));
		Assert.Same(db, db.AddParamIf(false, 1));
		Assert.Same(legacy, legacy.AddParamIf(false, 1));
		Assert.Same(microsoft, microsoft.AddParamIf(false, 1));

		AssertNone(classic.Params);
		AssertNone(db.Params);
		AssertNone(legacy.Params);
		AssertNone(microsoft.Params);
	}

	[Fact]
	public static void FalseCondition_WithAnArgumentTheCompilerDidNotSupply_AddsNothing()
	{
		var classic = Classic();
		var db = Db();
		var legacy = Legacy();
		var microsoft = Microsoft();

		// A compiler before C# 10 passes no text; with a false condition that is no error either.
		classic.AddParamIf(false, value: 1, expression: null);
		db.AddParamIf(false, value: 1, expression: null);
		legacy.AddParamIf(false, value: 1, expression: null);
		microsoft.AddParamIf(false, value: 1, expression: null);

		AssertNone(classic.Params);
		AssertNone(db.Params);
		AssertNone(legacy.Params);
		AssertNone(microsoft.Params);
	}

	[Fact]
	public static void TrueCondition_WithAnArgumentThatHasNoName_Throws_AndAddsNothing()
	{
		var classic = Classic();
		var db = Db();
		var legacy = Legacy();
		var microsoft = Microsoft();

		static void Throws(Action add)
		{
			var ex = Assert.Throws<ArgumentException>(add);
			Assert.Equal("value", ex.ParamName);
			Assert.Contains("AddParam(\"name\", value)", ex.Message);
		}

		Throws(() => classic.AddParamIf(true, 1));
		Throws(() => db.AddParamIf(true, 1));
		Throws(() => legacy.AddParamIf(true, 1));
		Throws(() => microsoft.AddParamIf(true, 1));
		Throws(() => classic.AddParamIf(true, value: 1, expression: null));

		AssertNone(classic.Params);
		AssertNone(db.Params);
		AssertNone(legacy.Params);
		AssertNone(microsoft.Params);
	}

	[Fact]
	public static void TrueCondition_WithADerivableArgument_AddsTheParameter_UnderTheLastMember()
	{
		const int author = 42;
		var classic = Classic();
		var db = Db();
		var legacy = Legacy();
		var microsoft = Microsoft();

		classic.AddParamIf(true, author);
		db.AddParamIf(true, author);
		legacy.AddParamIf(true, author);
		microsoft.AddParamIf(true, author);

		Assert.Equal("author", Assert.Single(classic.Params).Name);
		Assert.Equal("author", Assert.Single(db.Params).Name);
		Assert.Equal("author", Assert.Single(legacy.Params).Name);
		Assert.Equal("author", Assert.Single(microsoft.Params).Name);
		Assert.Equal(42, classic.Params[0].Value);
		Assert.Equal(42, db.Params[0].Value);
		Assert.Equal(42, legacy.Params[0].Value);
		Assert.Equal(42, microsoft.Params[0].Value);
	}

	[Fact]
	public static void FalseCondition_WithADerivableArgument_AddsNothing()
	{
		const int author = 42;
		var classic = Classic();
		var db = Db();
		var legacy = Legacy();
		var microsoft = Microsoft();

		classic.AddParamIf(false, author);
		db.AddParamIf(false, author);
		legacy.AddParamIf(false, author);
		microsoft.AddParamIf(false, author);

		AssertNone(classic.Params);
		AssertNone(db.Params);
		AssertNone(legacy.Params);
		AssertNone(microsoft.Params);
	}
}
