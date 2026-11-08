namespace Open.Database.Extensions.Tests;

#nullable enable

// The conventions that need more than tiers 1 to 3: the parameter prefixes of the procedure side (@p_author, @pAuthor, @in_author, @i_author, @prm_author, @par_author, @v_author)
// and the field prefixes of the C# side (m_author, s_author). A real consumer has them on both sides and should not have to rename anything.
// The rule, in the order it is applied:
//   candidates are tried most specific first, as before, and each candidate runs through the tiers in order: exact, ignoring case, ignoring case and underscores;
//   only when those three fail for THAT candidate, tier 4 removes ONE common prefix from the real name (p_, in_, i_, prm_, par_, v_ in any case, or a lone lower-case p before an upper-case letter)
//   and compares like tier 3. The real name that is sent is always the one the provider reported, prefix included.
//   On the C# side a last member that starts with m_, s_ or _ followed by a letter or digit also offers its name without the prefix, right after the name as written.
[ExcludeFromCodeCoverage]
public static class ParameterNamePrefixTests
{
	const string NoMatch = "matches none of the parameters";

	// ---- every pair of the catalog that needs a prefix rule ----

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.PrefixPairs), MemberType = typeof(ParameterNameCatalog))]
	public static void PrefixPair_FindsTheRealName_AsTheProviderReportedIt_AndFromMemory(string expression, string real)
	{
		var run = new SyncNameRun(real);

		Assert.Equal([real], run.Add(expression));
		Assert.Equal(1, run.Probe.Parameters.Single().Value);
		Assert.Equal(1, run.Lookups);

		Assert.Equal([real], run.Add(expression));
		Assert.Equal([real], run.AddIf(expression));
		Assert.Equal(1, run.Lookups);
	}

	[Theory]
	[MemberData(nameof(ParameterNameCatalog.PrefixPairs), MemberType = typeof(ParameterNameCatalog))]
	public static async Task PrefixPair_FindsTheRealName_AsTheProviderReportedIt_AndFromMemory_Async(string expression, string real)
	{
		var run = new AsyncNameRun(real);

		Assert.Equal([real], await run.AddAsync(expression));
		Assert.Equal([real], await run.AddAsync(expression));
		Assert.Equal([real], await run.AddIfAsync(expression));
		Assert.Equal(1, run.Lookups);
		Assert.Equal(0, run.SyncLookups);
	}

	public static TheoryData<string, string> PrefixCallSitePairs { get; } = ParameterNameEndToEndTests.CallSitePairs(prefixed: true);

	[Theory]
	[MemberData(nameof(PrefixCallSitePairs))]
	public static void CallSite_FindsTheRealName(string expression, string real)
	{
		ParameterNameEndToEndTests.CallSite site = ParameterNameEndToEndTests.CallSites[expression];
		var run = new SyncNameRun(real);

		Assert.Equal([real], run.Run(c => site.Sync(c)));
		Assert.Equal(1, run.Probe.Parameters.Single().Value);

		Assert.Equal([real], run.Run(c => site.Sync(c)));
		Assert.Equal(1, run.Lookups);
	}

	[Theory]
	[MemberData(nameof(PrefixCallSitePairs))]
	public static async Task CallSite_FindsTheRealName_Async(string expression, string real)
	{
		ParameterNameEndToEndTests.CallSite site = ParameterNameEndToEndTests.CallSites[expression];
		var run = new AsyncNameRun(real);

		Assert.Equal([real], await run.RunAsync(c => site.Async(c)));
		Assert.Equal(1, run.Values[real]);
		Assert.Equal(1, run.Lookups);
	}

	[Fact]
	public static void OneRememberedList_ServesEveryPattern()
	{
		var run = new SyncNameRun("@p_author_id");

		foreach (string expression in new[] { "authorId", "AuthorId", "AuthorID", "author_id", "AUTHOR_ID", "_authorId", "m_authorId", "s_author_id", "filter.Author.Id", "this.authorId" })
			Assert.Equal(["@p_author_id"], run.Add(expression));

		Assert.Equal(1, run.Lookups);
	}

	// ---- the members of an object ----

	sealed class FieldPrefixDto
	{
		public int m_author = 1;
		public int m_authorId = 2;
		public string? m_addressLine2 = "x";
		public DateTime? m_endDate = new(2026, 1, 2);
		public int? m_limit = null;
		public int s_httpRequestId = 6;
		public string m_unrelated = "u";
	}

	sealed class UnderscoreDto
	{
		public int _author = 1;
		public int _authorId = 2;
		public string? _addressLine2 = "x";
		public DateTime? _endDate = new(2026, 1, 2);
		public int? _limit = null;
		public int _httpRequestId = 6;
		public string _unrelated = "u";
	}

	static readonly Dictionary<string, ParameterNameEndToEndTests.Dto> PrefixedDtos = new()
	{
		["m_ and s_"] = new(["m_author", "m_authorId", "m_addressLine2", "m_endDate", "m_limit", "s_httpRequestId", "m_unrelated"], c => c.AddParams(new FieldPrefixDto()), c => c.AddParams(new FieldPrefixDto())),
		["_"] = new(["_author", "_authorId", "_addressLine2", "_endDate", "_limit", "_httpRequestId", "_unrelated"], c => c.AddParams(new UnderscoreDto()), c => c.AddParams(new UnderscoreDto())),
	};

	// The ordinary object spellings against prefixed procedure names, and the prefixed object spellings against every procedure spelling.
	public static TheoryData<string, string, bool> MemberCases { get; } = BuildMemberCases();

	static TheoryData<string, string, bool> BuildMemberCases()
	{
		var data = new TheoryData<string, string, bool>();
		foreach (string dto in ParameterNameEndToEndTests.Dtos.Keys)
		{
			foreach (string set in ParameterNameCatalog.MemberRealsPrefixed.Keys)
			{
				data.Add(dto, set, false);
				data.Add(dto, set, true);
			}
		}

		foreach (string dto in PrefixedDtos.Keys)
		{
			foreach (string set in ParameterNameCatalog.MemberReals.Keys.Concat(ParameterNameCatalog.MemberRealsPrefixed.Keys))
			{
				data.Add(dto, set, false);
				data.Add(dto, set, true);
			}
		}

		return data;
	}

	static ParameterNameEndToEndTests.Dto DtoOf(string name)
		=> ParameterNameEndToEndTests.Dtos.TryGetValue(name, out ParameterNameEndToEndTests.Dto? dto) ? dto : PrefixedDtos[name];

	static string[] RealsOf(string set, bool at)
		=> ParameterNameEndToEndTests.RealNames(
			ParameterNameCatalog.MemberReals.ContainsKey(set) ? ParameterNameCatalog.MemberReals : ParameterNameCatalog.MemberRealsPrefixed, set, at);

	[Theory]
	[MemberData(nameof(MemberCases))]
	public static void AddParams_MatchesEveryMemberToItsParameter(string dto, string set, bool at)
	{
		string[] real = RealsOf(set, at);
		ParameterNameEndToEndTests.Dto source = DtoOf(dto);
		var run = new SyncNameRun([.. real, "@something_else"]);

		run.Run(c => source.Sync(c));

		Assert.Equal(ParameterNameEndToEndTests.Expected(real), run.Values);
		Assert.Equal(6, run.Probe.Parameters.Count);

		run.Run(c => source.Sync(c)); // from memory
		Assert.Equal(ParameterNameEndToEndTests.Expected(real), run.Values);
		Assert.Equal(1, run.Lookups);
	}

	[Theory]
	[MemberData(nameof(MemberCases))]
	public static async Task AddParams_MatchesEveryMemberToItsParameter_Async(string dto, string set, bool at)
	{
		string[] real = RealsOf(set, at);
		ParameterNameEndToEndTests.Dto source = DtoOf(dto);
		var run = new AsyncNameRun([.. real, "@something_else"]);

		await run.RunAsync(c => source.Async(c));
		Assert.Equal(ParameterNameEndToEndTests.Expected(real), run.Values);

		await run.RunAsync(c => source.Async(c));
		Assert.Equal(1, run.Lookups);
		Assert.Equal(0, run.SyncLookups);
	}

	public static TheoryData<string> PrefixedDtoNames { get; } = new() { "m_ and s_", "_" };

	[Theory]
	[MemberData(nameof(PrefixedDtoNames))]
	public static void AddParams_WithoutRealNames_SendsEveryMemberAsWritten_PrefixIncluded(string dto)
	{
		ParameterNameEndToEndTests.Dto source = PrefixedDtos[dto];
		Dictionary<string, object?> expected = [];
		for (int i = 0; i < source.Members.Length; i++) expected[source.Members[i]] = ParameterNameEndToEndTests.ValuesAsWritten[i];

		var none = new SyncNameRun();
		none.Run(c => source.Sync(c));
		Assert.Equal(expected, none.Values);

		var text = new SyncNameRun(CommandType.Text, "@never");
		text.Run(c => source.Sync(c));
		Assert.Equal(expected, text.Values);
		Assert.Equal(0, text.Lookups);
	}

	sealed class PrefixCollide
	{
		public int Author = 1;
		public int m_author = 2;
	}

	[Fact]
	public static void AddParams_TwoMembersThatResolveToOneParameter_ThroughAPrefix_Throw()
	{
		// Author and m_author are both "author": one real @p_author is claimed twice.
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@p_author").Run(c => c.AddParams(new PrefixCollide())));

		Assert.Contains("Two parameters resolve to '@p_author'", ex.Message);
	}

	sealed class Source
	{
		public int Author = 1;
		public int AuthorId = 2;
		public int Region = 3;
	}

	[Theory]
	[InlineData("@p_author_id")]
	[InlineData("p_author_id")]
	[InlineData("@P_AUTHOR_ID")]
	public static void AddParams_SkipsTheMemberWhoseParameterWasAddedExplicitly_ThroughAPrefix(string name)
	{
		var run = new SyncNameRun("@p_author", "@p_author_id", "@p_region");
		var expected = new Dictionary<string, object?> { ["@p_author"] = 1, ["@p_region"] = 3, [name] = 99 };

		run.Run(c => c.AddParams(new Source()).AddParam(name, 99));
		Assert.Equal(expected, run.Values);

		run.Run(c => c.AddParam(name, 99).AddParams(new Source()));
		Assert.Equal(expected, run.Values);
	}

	// ---- precedence: the candidate decides first (most specific first), then the tier (the prefix tier is the last of the four) ----

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
	public static void Tiers_ExactBeatsCaseBeatsUnderscoresBeatsThePrefixTier_WhateverTheOrderOfTheNames()
	{
		string[][] steps =
		[
			["@author", "@AUTHOR", "@author_", "@p_author"],
			["@AUTHOR", "@author_", "@p_author"],
			["@author_", "@p_author"],
			["@p_author"],
		];
		string[] winners = ["@author", "@AUTHOR", "@author_", "@p_author"];

		for (int step = 0; step < steps.Length; step++)
		{
			foreach (string[] order in Orders(steps[step]))
				Assert.Equal([winners[step]], new SyncNameRun(order).Add("author"));
		}
	}

	[Fact]
	public static async Task Tiers_ExactBeatsCaseBeatsUnderscoresBeatsThePrefixTier_Async()
	{
		Assert.Equal(["@author"], await new AsyncNameRun("@p_author", "@author_", "@AUTHOR", "@author").AddAsync("author"));
		Assert.Equal(["@AUTHOR"], await new AsyncNameRun("@p_author", "@author_", "@AUTHOR").AddAsync("author"));
		Assert.Equal(["@author_"], await new AsyncNameRun("@p_author", "@author_").AddAsync("author"));
		Assert.Equal(["@p_author"], await new AsyncNameRun("@p_author", "@x").AddAsync("author"));
	}

	[Fact]
	public static void ThePrefixTier_RunsInsideEachCandidatesTurn_SpecificityDecidesFirst()
	{
		// order.Customer.Id tries OrderCustomerId, CustomerId, then Id, each through all four tiers before the next one.
		// A more specific candidate that matches only through a prefix beats a bare, less specific name: the customer's id, never the bare @id.
		Assert.Equal(["@p_customer_id"], new SyncNameRun("@p_customer_id", "@id").Add("order.Customer.Id"));
		Assert.Equal(["@p_customer_id"], new SyncNameRun("@id", "@p_customer_id").Add("order.Customer.Id"));
		Assert.Equal(["@p_order_customer_id"], new SyncNameRun("@p_order_customer_id", "@Id").Add("order.Customer.Id"));
		Assert.Equal(["@pCustomerId"], new SyncNameRun("@Id", "@pCustomerId").Add("order.Customer.Id"));

		// The mirror: a bare name that matches the more specific candidate wins over a prefixed name of a less specific one.
		Assert.Equal(["@customer_id"], new SyncNameRun("@customer_id", "@p_id").Add("order.Customer.Id"));
		Assert.Equal(["@CustomerID"], new SyncNameRun("@pId", "@CustomerID").Add("order.Customer.Id"));

		// Within one candidate the tiers still run in order: ignoring case beats the prefix.
		Assert.Equal(["@Id"], new SyncNameRun("@p_id", "@Id").Add("order.Customer.Id"));
		Assert.Equal(["@p_id"], new SyncNameRun("@p_id", "@x").Add("order.Customer.Id"));
	}

	[Fact]
	public static void ThePrefixTier_WithThreeCandidates_ChoosesTheMostSpecificOneThatMatchesAtAnyTier_WhateverTheOrder()
	{
		// The real names and the one that wins, for order.Customer.Id: OrderCustomerId, CustomerId, Id.
		KeyValuePair<string[], string>[] cases =
		[
			new(["@p_id", "@p_customer_id", "@p_order_customer_id"], "@p_order_customer_id"),  // all three through a prefix: the most specific
			new(["@id", "@p_customer_id", "@pOrderCustomerId"], "@pOrderCustomerId"),          // a bare @id does not outrank a prefixed, more specific name
			new(["@p_id", "@p_customer_id", "@order_customer_id"], "@order_customer_id"),      // matched without a prefix at the first candidate
			new(["@Id", "@p_customer_id", "@x"], "@p_customer_id"),                           // the middle candidate beats the bare last one
			new(["@p_id", "@customer_id", "@p_order_customer_id"], "@p_order_customer_id"),    // the first candidate beats a bare, less specific name
			new(["@p_id", "@x", "@y"], "@p_id"),                                                // only the last candidate finds anything
		];

		foreach (KeyValuePair<string[], string> expected in cases)
		{
			foreach (string[] order in Orders(expected.Key))
				Assert.Equal([expected.Value], new SyncNameRun(order).Add("order.Customer.Id"));
		}
	}

	[Fact]
	public static async Task ThePrefixTier_RunsInsideEachCandidatesTurn_Async()
	{
		Assert.Equal(["@p_customer_id"], await new AsyncNameRun("@id", "@p_customer_id").AddAsync("order.Customer.Id"));
		Assert.Equal(["@customer_id"], await new AsyncNameRun("@p_id", "@customer_id").AddAsync("order.Customer.Id"));
	}

	[Fact]
	public static void ThePrefixTier_AmbiguousForAMoreSpecificCandidate_ThrowsInsteadOfFallingBackToABareName()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@p_customer_id", "@i_customer_id", "@id").Add("order.Customer.Id"));

		Assert.Contains("'@p_customer_id'", ex.Message);
		Assert.Contains("'@i_customer_id'", ex.Message);
	}

	public static TheoryData<string, string, string> AmbiguousThroughPrefixes { get; } = new()
	{
		{ "author", "@p_author", "@i_author" },
		{ "author", "@p_author", "@pAuthor" },
		{ "author", "@in_author", "@v_author" },
		{ "author", "@prm_author", "@par_author" },
		{ "author", "@P_AUTHOR", "@p_Author" },
		{ "author", "p_author", "v_author" },
		{ "authorId", "@p_author_id", "@in_authorid" },
		{ "m_author", "@i_author", "@v_author" },
	};

	[Theory]
	[MemberData(nameof(AmbiguousThroughPrefixes))]
	public static void AmbiguityInThePrefixTier_Throws_NamingBothParameters(string expression, string first, string second)
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(first, second).Add(expression));
		Assert.Contains("matches both", ex.Message);
		Assert.Contains($"'{first}'", ex.Message);
		Assert.Contains($"'{second}'", ex.Message);

		var reversed = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(second, first).Add(expression));
		Assert.Contains($"'{first}'", reversed.Message);
		Assert.Contains($"'{second}'", reversed.Message);
	}

	[Theory]
	[MemberData(nameof(AmbiguousThroughPrefixes))]
	public static async Task AmbiguityInThePrefixTier_Throws_NamingBothParameters_Async(string expression, string first, string second)
	{
		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => new AsyncNameRun(first, second).AddAsync(expression));

		Assert.Contains($"'{first}'", ex.Message);
		Assert.Contains($"'{second}'", ex.Message);
	}

	[Fact]
	public static void AmbiguityInThePrefixTier_IsReportedAgainFromMemory_AndANameFoundEarlierIsNeverAmbiguous()
	{
		var run = new SyncNameRun("@p_author", "@i_author");

		Assert.Throws<InvalidOperationException>(() => run.Add("author"));
		Assert.Throws<InvalidOperationException>(() => run.Add("author"));
		Assert.Equal(1, run.Lookups);

		// Tiers 1 to 3 found one: the two prefixed names are never compared.
		Assert.Equal(["@AUTHOR"], new SyncNameRun("@p_author", "@i_author", "@AUTHOR").Add("author"));
		Assert.Equal(["@author"], new SyncNameRun("@p_author", "@i_author", "@author").Add("author"));
	}

	[Fact]
	public static void ThePrefixIsComparedIgnoringCase_AndOnlyOneIsRemoved()
	{
		foreach (string real in new[] { "@IN_author", "@In_Author", "@Prm_author", "@PAR_author", "@V_Author", "@I_AUTHOR", "@pAUTHOR", "@pAuthor_" })
			Assert.Equal([real], new SyncNameRun(real).Add("author"));

		// A prefix is removed once: what is left must be the whole name.
		Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@p_p_author").Add("author"));
		Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@in_p_author").Add("author"));
		Assert.Equal(["@p_p_author"], new SyncNameRun("@p_p_author").Add("p_author"));
		Assert.Equal(["@in_p_author"], new SyncNameRun("@in_p_author").Add("pAuthor"));
	}

	[Fact]
	public static void ALoneLowerCasePBeforeAnUpperCaseLetter_IsAPrefix_ButNotBeforeALowerCaseOne()
	{
		Assert.Equal(["@pAuthor"], new SyncNameRun("@pAuthor").Add("author"));
		Assert.Equal(["@pA"], new SyncNameRun("@pA").Add("a"));
		Assert.Equal(["@pAUTHOR"], new SyncNameRun("@pAUTHOR").Add("author"));
		Assert.Equal(["@pX_1"], new SyncNameRun("@pX_1").Add("x1"));

		foreach (string real in new[] { "@pauthor", "@PAuthor", "@Pauthor", "@pages", "@pAuthors" })
		{
			var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(real).Add("author"));
			Assert.Contains(NoMatch, ex.Message);
		}

		Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@pages").Add("ages"));
	}

	[Fact]
	public static void APrefixAloneIsNotAName()
	{
		foreach (string real in new[] { "@p_", "@in_", "@i_", "@prm_", "@par_", "@v_", "@p", "@pA_", "@IN_" })
			Assert.Throws<InvalidOperationException>(() => new SyncNameRun(real, "@unrelated").Add("zzz"));
	}

	// ---- the C# side: m_, s_ and _ in front of the last member ----

	[Fact]
	public static void AFieldPrefix_AlsoOffersTheNameWithoutIt_AfterTheNameAsWritten()
	{
		// As written is the most specific candidate, even at a weaker tier than the stripped name.
		Assert.Equal(["@m_author"], new SyncNameRun("@author", "@m_author").Add("m_author"));
		Assert.Equal(["@M_AUTHOR"], new SyncNameRun("@author", "@M_AUTHOR").Add("m_author"));
		Assert.Equal(["@m_author_"], new SyncNameRun("@author", "@m_author_").Add("m_author"));
		Assert.Equal(["@s_author"], new SyncNameRun("@author", "@s_author").Add("s_author"));
		Assert.Equal(["@_author"], new SyncNameRun("@author", "@_author").Add("_author"));

		// Otherwise the name without it.
		Assert.Equal(["@author"], new SyncNameRun("@author").Add("m_author"));
		Assert.Equal(["@Author"], new SyncNameRun("@Author", "@other").Add("s_author"));
		Assert.Equal(["@author"], new SyncNameRun("@author").Add("_author"));
		Assert.Equal(["@author_id"], new SyncNameRun("@author_id").Add("m_authorId"));
		Assert.Equal(["@x1"], new SyncNameRun("@x1").Add("m_x1"));
	}

	[Fact]
	public static void AFieldPrefix_OnAChain_OnlyAffectsTheLastMember()
	{
		Assert.Equal(["@author"], new SyncNameRun("@author").Add("search.m_author"));
		Assert.Equal(["@search_m_author"], new SyncNameRun("@author", "@search_m_author").Add("search.m_author"));
		Assert.Equal(["@m_author"], new SyncNameRun("@author", "@m_author").Add("search.m_author"));

		// A prefix on an earlier member is not removed: m_order.Customer.Id tries m_orderCustomerId, CustomerId, Id.
		Assert.Equal(["@m_order_customer_id"], new SyncNameRun("@m_order_customer_id", "@customer_id").Add("m_order.Customer.Id"));
		Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@order_customer_id").Add("m_order.Customer.Id"));
	}

	[Fact]
	public static void NotAFieldPrefix_IsNotRemoved()
	{
		// Upper case, no underscore, or an underscore that is not followed by a letter or a digit.
		foreach (string expression in new[] { "M_author", "S_author", "ms_author", "mauthor", "my_author", "m__author", "m_", "p_author", "g_author", "t_author" })
			Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@author", "@other").Add(expression));
	}

	[Fact]
	public static void NoMatch_ListsTheNameWithoutTheFieldPrefixAmongTheCandidates()
	{
		var run = new SyncNameRun("@x");

		var ex = Assert.Throws<InvalidOperationException>(() => run.Add("m_author"));

		Assert.Contains("tried m_author, author", ex.Message);
		Assert.Contains("@x", ex.Message);

		var chain = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@x").Add("search.m_author"));
		Assert.Contains("tried searchm_author, m_author, author", chain.Message);
	}

	[Fact]
	public static void WithoutRealNames_TheFieldPrefixIsKept()
	{
		Assert.Equal(["m_author"], new SyncNameRun().Add("m_author"));
		Assert.Equal(["s_author"], new SyncNameRun(CommandType.Text, "@author").Add("s_author"));
		Assert.Equal(["_author"], new SyncNameRun().Add("this._author"));
	}

	// ---- two arguments, and explicit names, through a prefix ----

	[Fact]
	public static void TwoArgumentsResolvingToOneParameter_ThroughAPrefix_Throw()
	{
		string[][] cases = [["author", "m_author", "@p_author"], ["author", "Author", "@p_author"], ["m_author", "s_author", "@p_author"], ["authorId", "m_author_id", "@p_author_id"]];

		foreach (string[] pair in cases)
		{
			string real = pair[2];
			var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(real).Run(c => c.AddParam(value: 1, expression: pair[0]).AddParam(value: 2, expression: pair[1])));

			Assert.Contains($"Two parameters resolve to '{real}'", ex.Message);
		}
	}

	[Fact]
	public static void ExplicitAndDerivedResolvingToTheSameParameter_ThroughAPrefix_Throw()
	{
		var run = new SyncNameRun("@p_author");

		// The explicit name is the real name, written out; the derived argument finds it through the prefix.
		var first = Assert.Throws<InvalidOperationException>(() => run.Run(c => c.AddParam("p_author", 1).AddParam(value: 2, expression: "author")));
		Assert.Contains("Two parameters resolve to", first.Message);
		Assert.Contains("p_author", first.Message);

		var second = Assert.Throws<InvalidOperationException>(() => run.Run(c => c.AddParam(value: 2, expression: "author").AddParam("@P_AUTHOR", 1)));
		Assert.Contains("Two parameters resolve to", second.Message);
		Assert.Contains("p_author", second.Message);
	}

	[Fact]
	public static void AnExplicitNameThatIsNotThePrefixedOne_IsNeverCorrected_AndIsSentBesideTheDerivedParameter()
	{
		var run = new SyncNameRun("@p_author");

		// Today's behavior for any two spellings that are not equal ignoring case and the @: both are sent, the explicit one exactly as given.
		Assert.Equal(["author", "@p_author"], run.Run(c => c.AddParam("author", 1).AddParam(value: 2, expression: "m_author")));
		Assert.Equal(new Dictionary<string, object?> { ["author"] = 1, ["@p_author"] = 2 }, run.Values);
	}

	// ---- the remembered names ----

	[Fact]
	public static void ARememberedListThatNoLongerFits_IsReadAgainOnce_AndThePrefixTierMatchesTheNewNames()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		const string Procedure = "dbo.prefix_changed_procedure";

		new DerivedParameterNameTests.ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@region")
			.AddParam(value: 1, expression: "region")
			.ExecuteNonQuery(); // remembers [@region]

		var after = new DerivedParameterNameTests.ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@p_region", "@p_author");
		after.AddParam(value: 2, expression: "m_author").ExecuteNonQuery();

		Assert.Equal(1, after.Lookups);
		Assert.Equal(["@region", "@p_author"], probe.Parameters.Select(p => p.ParameterName));

		// The list is remembered again: the next command asks nobody.
		var next = new DerivedParameterNameTests.ProcCommand(probe.Connection, CommandType.StoredProcedure, Procedure, "@never");
		next.AddParam(value: 3, expression: "region").ExecuteNonQuery();
		Assert.Equal(0, next.Lookups);
		Assert.Equal("@p_region", probe.Parameters[^1].ParameterName);
	}

	[Fact]
	public static void AddParamIf_WithAPrefix_FindsTheParameter_OnlyWhenTheConditionHolds()
	{
		var run = new SyncNameRun("@p_author", "@pAuthorId");

		Assert.Equal(["@p_author"], run.Run(c => c.AddParamIf(true, value: 1, expression: "m_author").AddParamIf(false, value: 2, expression: "authorId")));
		Assert.Equal(["@pAuthorId"], run.Run(c => c.AddParamIf(false, value: 1, expression: "author").AddParamIf(true, value: 2, expression: "authorId")));
		Assert.Equal(1, run.Lookups);
	}
}
