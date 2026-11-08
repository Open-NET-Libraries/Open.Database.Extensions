namespace Open.Database.Extensions.Tests;

#nullable enable

// An error about a derived name must say what the caller wrote (the argument as written, not only the candidate that matched), which candidate and which tier
// matched more than one parameter, and what the procedure declares. The first sentence of each message stays what it was ("... matches both ...", "Two parameters resolve to ...").
[ExcludeFromCodeCoverage]
public static class ParameterNameMessageTests
{
	sealed class FooBars
	{
		public int FooBar = 1;
		public int Foo_Bar = 2;
	}

	// ---- an ambiguity ----

	[Fact]
	public static void Ambiguity_SaysWhatYouWrote_TheCandidate_TheTier_AndWhatTheProcedureDeclares()
	{
		// order.Customer.Id: the first candidate (orderCustomerId) matches nothing; the second (CustomerId) fits both parameters once underscores are ignored.
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@customer_id", "@customer__id", "@name").Add("order.Customer.Id"));

		Assert.StartsWith("The argument 'order.Customer.Id' matches both '@customer_id' and '@customer__id' of 'dbo.names", ex.Message);
		Assert.Contains("Its candidate name 'CustomerId' fits each of them ignoring case and underscores.", ex.Message);
		Assert.Contains("The procedure declares: @customer_id, @customer__id, @name.", ex.Message);
		Assert.Contains("Name the parameter explicitly: AddParam(\"name\", value).", ex.Message);
	}

	[Theory]
	[InlineData("exactly", new[] { "@x", "@x" }, "x")]
	[InlineData("ignoring case", new[] { "@customerid", "@CUSTOMERID" }, "CustomerId")]
	[InlineData("ignoring case and underscores", new[] { "@customer_id", "@customer__id" }, "CustomerId")]
	[InlineData("ignoring case and underscores once one parameter prefix (p_, in_, i_, prm_, par_, v_ or a lone p) is removed from the real name", new[] { "@p_author", "@in_author" }, "Author")]
	public static void Ambiguity_NamesTheTierThatMatchedMoreThanOneParameter(string tier, string[] real, string expression)
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(real).Add(expression));

		Assert.Contains($"fits each of them {tier}.", ex.Message);
		Assert.Contains($"matches both '{real[0]}' and '{real[1]}'", ex.Message);
	}

	[Fact]
	public static async Task Ambiguity_SaysTheSame_OnTheAsyncPath()
	{
		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => new AsyncNameRun("@p_author", "@in_author").AddAsync("search.Author"));

		Assert.StartsWith("The argument 'search.Author' matches both '@p_author' and '@in_author' of 'dbo.names", ex.Message);
		Assert.Contains("Its candidate name 'Author' fits each of them", ex.Message);
		Assert.Contains("The procedure declares: @p_author, @in_author.", ex.Message);
	}

	// ---- a collision: two arguments resolve to one parameter ----

	[Fact]
	public static void Collision_NamesBothArguments_AsTheyWereWritten_AndWhatTheProcedureDeclares()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@region", "@mode").Run(c => c
			.AddParam(value: 1, expression: "filter.Region")
			.AddParam(value: 2, expression: "other.Region")));

		Assert.StartsWith("Two parameters resolve to '@region': the argument 'filter.Region' and the argument 'other.Region'.", ex.Message);
		Assert.Contains("The procedure 'dbo.names", ex.Message);
		Assert.Contains("declares: @region, @mode.", ex.Message);
		Assert.Contains("Name one of them explicitly: AddParam(\"name\", value).", ex.Message);
	}

	[Fact]
	public static void Collision_OfANamedParameterAndAnArgument_SaysWhichWasWhich()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@region").Run(c => c
			.AddParam("@REGION", "x")
			.AddParam(value: 1, expression: "filter.Region")));

		Assert.StartsWith("Two parameters resolve to '@REGION': the name '@REGION' you passed and the argument 'filter.Region'.", ex.Message); // the name of the first of the two
	}

	[Fact]
	public static void Collision_OfTwoMembersOfAnObject_NamesTheMembers()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@foo_bar").Run(c => c.AddParams(new FooBars())));

		Assert.StartsWith("Two parameters resolve to '@foo_bar': the member '", ex.Message);
		Assert.Contains("the member 'FooBar' of the AddParams object", ex.Message);
		Assert.Contains("the member 'Foo_Bar' of the AddParams object", ex.Message);
	}

	[Fact]
	public static void Collision_WithoutTheProceduresNames_SaysTheyAreNotKnown()
	{
		// A text command has no names to read: both arguments are used as written (Id), and still collide.
		var run = new SyncNameRun(CommandType.Text, "@never");

		var ex = Assert.Throws<InvalidOperationException>(() => run.Run(c => c
			.AddParam(value: 1, expression: "order.Id")
			.AddParam(value: 2, expression: "customer.Id")));

		Assert.StartsWith("Two parameters resolve to 'Id': the argument 'order.Id' and the argument 'customer.Id'.", ex.Message);
		Assert.Contains("The parameter names are not known, so each argument is used under its last member as written.", ex.Message);
		Assert.DoesNotContain("declares", ex.Message);
		Assert.Equal(0, run.Lookups);
	}

	[Fact]
	public static async Task Collision_SaysTheSame_OnTheAsyncPath()
	{
		var run = new AsyncNameRun("@region");

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => run.RunAsync(c => c
			.AddParam(value: 1, expression: "filter.Region")
			.AddParam(value: 2, expression: "other.Region")));

		Assert.StartsWith("Two parameters resolve to '@region': the argument 'filter.Region' and the argument 'other.Region'.", ex.Message);
		Assert.Contains("declares: @region.", ex.Message);
	}

	// ---- no match, unchanged ----

	[Fact]
	public static void NoMatch_StillSaysWhatWasWritten_WhatWasTried_AndWhatTheProcedureDeclares()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@a", "@b").Add("order.Customer.Id"));

		Assert.StartsWith("The argument 'order.Customer.Id' matches none of the parameters of 'dbo.names", ex.Message);
		Assert.Contains("tried orderCustomerId, CustomerId, Id", ex.Message);
		Assert.Contains("The procedure declares: @a, @b.", ex.Message);
	}
}
