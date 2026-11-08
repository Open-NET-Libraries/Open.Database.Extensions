namespace Open.Database.Extensions.Tests;

#nullable enable

// The pattern matrix for parameter-name inference, at the level of the argument expression: the text the compiler captures from a call such as AddParam(search.Author).
// Every case goes through the public command (AddParam(value, expression) and AddParamIf) on a fake provider that reports the procedure's real parameter names,
// on both the synchronous and the asynchronous path, so what is pinned is what a consumer gets. The catalog of patterns is ParameterNameCatalog (read its header).
// The pairs that need the parameter-prefix tier or the field-prefix rule (p_author, pAuthor, m_author, ...) are in ParameterNamePrefixTests.
[ExcludeFromCodeCoverage]
public static class ParameterNameMatrixTests
{
	const string NoMatch = "matches none of the parameters";

	// ---- every plain pair: the expression finds the real name, as the provider reported it ----

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.PlainPairs), MemberType = typeof(ParameterNameCatalog))]
	public static void PlainPair_FindsTheRealName_AndFindsItAgainFromMemory(string expression, string real)
	{
		var run = new SyncNameRun(real);

		Assert.Equal([real], run.Add(expression));
		Assert.Equal(1, run.Probe.Parameters.Single().Value);
		Assert.Equal(1, run.Lookups);

		// The names are remembered now: the same answer for the same pattern, and for AddParamIf, without asking again.
		Assert.Equal([real], run.Add(expression));
		Assert.Equal([real], run.AddIf(expression));
		Assert.Empty(run.AddIf(expression, condition: false));
		Assert.Equal(1, run.Lookups);
	}

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.PlainPairs), MemberType = typeof(ParameterNameCatalog))]
	public static async Task PlainPair_FindsTheRealName_AndFindsItAgainFromMemory_Async(string expression, string real)
	{
		var run = new AsyncNameRun(real);

		Assert.Equal([real], await run.AddAsync(expression));
		Assert.Equal(1, run.Lookups);

		Assert.Equal([real], await run.AddAsync(expression));
		Assert.Equal([real], await run.AddIfAsync(expression));
		Assert.Empty(await run.AddIfAsync(expression, condition: false));
		Assert.Equal(1, run.Lookups);
		Assert.Equal(0, run.SyncLookups);
	}

	[Fact]
	public static void OneRememberedList_ServesEveryPattern()
	{
		var run = new SyncNameRun("@author_id");

		foreach (string expression in new[] { "authorId", "AuthorId", "AuthorID", "author_id", "AUTHOR_ID", "_authorId", "filter.Author.Id", "this.authorId" })
			Assert.Equal(["@author_id"], run.Add(expression));

		Assert.Equal(1, run.Lookups);
	}

	// ---- the real names are not known: the last member as written ----

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.Expressions), MemberType = typeof(ParameterNameCatalog))]
	public static void WithoutRealNames_TheLastMemberIsUsedAsWritten(string expression, string last)
	{
		// A provider that supplies no names, and a text command that is never asked.
		Assert.Equal([last], new SyncNameRun().Add(expression));
		Assert.Equal([last], new SyncNameRun().AddIf(expression));

		var text = new SyncNameRun(CommandType.Text, "@never", "@" + last + "_other");
		Assert.Equal([last], text.Add(expression));
		Assert.Equal(0, text.Lookups);
	}

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.Expressions), MemberType = typeof(ParameterNameCatalog))]
	public static async Task WithoutRealNames_TheLastMemberIsUsedAsWritten_Async(string expression, string last)
	{
		Assert.Equal([last], await new AsyncNameRun().AddAsync(expression));
		Assert.Equal([last], await new AsyncNameRun().AddIfAsync(expression));

		var text = new AsyncNameRun(CommandType.Text, "@never", "@" + last + "_other");
		Assert.Equal([last], await text.AddAsync(expression));
		Assert.Equal(0, text.Lookups);
	}

	// ---- names that must NOT match: a false match that silently binds is worse than a clear error ----

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.Negatives), MemberType = typeof(ParameterNameCatalog))]
	public static void Negative_Throws_AndSendsNothing(string expression, string real)
	{
		var run = new SyncNameRun(real);

		var ex = Assert.Throws<InvalidOperationException>(() => run.Add(expression));

		Assert.Contains(NoMatch, ex.Message);
		Assert.Contains(real, ex.Message);
		Assert.Empty(run.Probe.Parameters);
	}

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.Negatives), MemberType = typeof(ParameterNameCatalog))]
	public static async Task Negative_Throws_AndSendsNothing_Async(string expression, string real)
	{
		var run = new AsyncNameRun(real);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => run.AddAsync(expression));

		Assert.Contains(NoMatch, ex.Message);
		Assert.Empty(run.Values);
	}

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.Unmatched), MemberType = typeof(ParameterNameCatalog))]
	public static void PlausibleSpellingsThatAreDeliberatelyNotMatched_Throw(string expression, string real)
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(real).Add(expression));

		Assert.Contains(NoMatch, ex.Message);
	}

	// ---- precedence: exact, then ignoring case, then ignoring case and underscores ----

	static IEnumerable<string[]> Orders(string[] names)
	{
		if (names.Length <= 1)
		{
			yield return names;
			yield break;
		}

		for (int i = 0; i < names.Length; i++)
		{
			string[] rest = [.. names.Where((_, index) => index != i)];
			foreach (string[] tail in Orders(rest))
				yield return [names[i], .. tail];
		}
	}

	[Fact]
	public static void Tiers_ExactBeatsCaseBeatsUnderscores_WhateverTheOrderOfTheNames()
	{
		string[][] steps =
		[
			["@author", "@AUTHOR", "@author_"],  // exact wins
			["@AUTHOR", "@author_"],             // then ignoring case
			["@author_"],                        // then ignoring case and underscores
		];
		string[] winners = ["@author", "@AUTHOR", "@author_"];

		for (int step = 0; step < steps.Length; step++)
		{
			foreach (string[] order in Orders(steps[step]))
				Assert.Equal([winners[step]], new SyncNameRun(order).Add("author"));
		}
	}

	[Fact]
	public static async Task Tiers_ExactBeatsCaseBeatsUnderscores_Async()
	{
		Assert.Equal(["@author"], await new AsyncNameRun("@author_", "@AUTHOR", "@author").AddAsync("author"));
		Assert.Equal(["@AUTHOR"], await new AsyncNameRun("@author_", "@AUTHOR").AddAsync("author"));
		Assert.Equal(["@author_"], await new AsyncNameRun("@author_").AddAsync("author"));
	}

	[Fact]
	public static void Tiers_AMatchIsAmbiguousOnlyWithinTheTierThatMatched()
	{
		// Two names that read the same at tier 3 do not matter when tier 2 already found one.
		Assert.Equal(["@AUTHOR"], new SyncNameRun("@author_", "@AUTHOR", "@au_thor").Add("author"));
		Assert.Equal(["@author"], new SyncNameRun("@Author", "@AUTHOR", "@author").Add("author"));

		Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@author_", "@au_thor").Add("author"));
	}

	[Fact]
	public static void AChain_PrefersItsMoreSpecificEnding_EvenAtAWeakerTier()
	{
		// order.Customer.Id tries OrderCustomerId, CustomerId, then Id.
		Assert.Equal(["@CustomerID"], new SyncNameRun("@Id", "@CustomerID").Add("order.Customer.Id"));
		Assert.Equal(["@customer_id"], new SyncNameRun("@id", "@customer_id").Add("order.Customer.Id"));
		Assert.Equal(["@CUSTOMER_ID"], new SyncNameRun("@Id", "@CUSTOMER_ID").Add("order.Customer.Id"));
		Assert.Equal(["@order_customer_id"], new SyncNameRun("@id", "@customer_id", "@order_customer_id").Add("order.Customer.Id"));
		Assert.Equal(["@Id"], new SyncNameRun("@Id", "@unrelated").Add("order.Customer.Id"));

		// The same chain through members that are not "Id".
		Assert.Equal(["@customer_name"], new SyncNameRun("@name", "@customer_name").Add("order.Customer.Name"));
		Assert.Equal(["@name"], new SyncNameRun("@name", "@customer_id").Add("order.Customer.Name"));
	}

	[Fact]
	public static void AChain_AmbiguousAtItsMoreSpecificEnding_ThrowsInsteadOfFallingBack()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@customerid", "@CUSTOMERID", "@id").Add("order.Customer.Id"));

		Assert.Contains("'@customerid'", ex.Message);
		Assert.Contains("'@CUSTOMERID'", ex.Message);
	}

	// ---- ambiguity: two real parameters that one candidate matches at the same tier ----

	public static TheoryData<string, string, string> Ambiguous { get; } = new()
	{
		{ "author", "@Author", "@AUTHOR" },
		{ "Author", "@author", "@AUTHOR" },
		{ "author", "Author", "AUTHOR" },
		{ "author", "@Author_", "@AUTHOR_" },
		{ "authorId", "@author_id", "@Author_Id" },
		{ "author_id", "@authorid", "@AuthorId" },
		{ "AuthorID", "@author_id", "@auth_or_id" },
		{ "_author", "@author_", "@au_thor" },
		{ "m_author", "@m_author_", "@m_au_thor" },
		{ "author", "@a_uthor", "@au_thor" },
	};

	[Theory]
	[MemberData(nameof(Ambiguous))]
	public static void Ambiguous_Throws_NamingBothParameters(string expression, string first, string second)
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(first, second).Add(expression));
		Assert.Contains("matches both", ex.Message);
		Assert.Contains($"'{first}'", ex.Message);
		Assert.Contains($"'{second}'", ex.Message);
		Assert.Contains("AddParam(\"name\", value)", ex.Message);

		// The order the provider lists them in does not matter.
		var reversed = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(second, first).Add(expression));
		Assert.Contains($"'{first}'", reversed.Message);
		Assert.Contains($"'{second}'", reversed.Message);
	}

	[Theory]
	[MemberData(nameof(Ambiguous))]
	public static async Task Ambiguous_Throws_NamingBothParameters_Async(string expression, string first, string second)
	{
		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => new AsyncNameRun(first, second).AddAsync(expression));

		Assert.Contains($"'{first}'", ex.Message);
		Assert.Contains($"'{second}'", ex.Message);
	}

	[Fact]
	public static void Ambiguous_IsReportedAgainWhenTheNamesComeFromMemory()
	{
		var run = new SyncNameRun("@Author", "@AUTHOR");

		Assert.Throws<InvalidOperationException>(() => run.Add("author"));
		Assert.Throws<InvalidOperationException>(() => run.Add("author"));
		Assert.Equal(1, run.Lookups);
	}

	// ---- two arguments that resolve to one parameter ----

	public static TheoryData<string, string, string> Colliding { get; } = new()
	{
		{ "author", "Author", "@author" },
		{ "author", "AUTHOR", "@Author" },
		{ "authorId", "author_id", "@author_id" },
		{ "filter.Author", "search.Author", "@author" },
		{ "_author", "author", "@author" },
		{ "AuthorID", "Author_Id", "@authorid" },
	};

	[Theory]
	[MemberData(nameof(Colliding))]
	public static void TwoArgumentsResolvingToOneParameter_Throw(string first, string second, string real)
	{
		var run = new SyncNameRun(real);

		var ex = Assert.Throws<InvalidOperationException>(() => run.Run(c => c.AddParam(value: 1, expression: first).AddParam(value: 2, expression: second)));

		Assert.Contains($"Two parameters resolve to '{real}'", ex.Message);
		Assert.Empty(run.Probe.Parameters);
	}

	[Theory]
	[MemberData(nameof(Colliding))]
	public static async Task TwoArgumentsResolvingToOneParameter_Throw_Async(string first, string second, string real)
	{
		var run = new AsyncNameRun(real);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => run.RunAsync(c => c.AddParam(value: 1, expression: first).AddParam(value: 2, expression: second)));

		Assert.Contains($"Two parameters resolve to '{real}'", ex.Message);
	}

	// ---- optional parameters the call does not mention stay unsent ----

	[Fact]
	public static void ParametersTheCallDoesNotMention_AreNotSent()
	{
		var run = new SyncNameRun("@a", "@b", "@c", "@d_e", "@p_f");

		Assert.Equal(["@b", "@d_e"], run.Run(c => c.AddParam(value: 2, expression: "B").AddParam(value: 5, expression: "dE")));
		Assert.Equal(new Dictionary<string, object?> { ["@b"] = 2, ["@d_e"] = 5 }, run.Values);

		Assert.Empty(run.Run(_ => { }));
	}

	[Fact]
	public static async Task ParametersTheCallDoesNotMention_AreNotSent_Async()
	{
		var run = new AsyncNameRun("@a", "@b", "@c", "@d_e");

		Assert.Equal(["@b", "@d_e"], await run.RunAsync(c => c.AddParam(value: 2, expression: "B").AddParam(value: 5, expression: "dE")));
	}

	// ---- odd real names and odd expressions ----

	[Fact]
	public static void OddRealNames_NeverFailInAnyWayButNoMatch()
	{
		string[] odd = ["", "@", "@p", "@_", "@__", "@p_", "@in_", "@i_", "@v_", "@prm_", "@par_", "@pA_", "@P", "@IN_"];

		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(odd).Add("zzz"));
		Assert.Contains(NoMatch, ex.Message);

		// A name that is only a prefix is still a name: tier 3 ignores the underscore.
		Assert.Equal(["@p_"], new SyncNameRun("@p_", "@x").Add("p"));
		Assert.Equal(["@_"], new SyncNameRun("@_", "@x").Add("_"));
	}

	[Fact]
	public static void AVeryLongName_Matches()
	{
		string name = new('x', 5000);

		Assert.Equal(["@" + name], new SyncNameRun("@" + name).Add(name));
		Assert.Equal(["@" + name.ToUpperInvariant()], new SyncNameRun("@" + name.ToUpperInvariant()).Add(name));
		Assert.Equal([name], new SyncNameRun().Add(name));
		Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@" + name + "x").Add(name));
	}
}
