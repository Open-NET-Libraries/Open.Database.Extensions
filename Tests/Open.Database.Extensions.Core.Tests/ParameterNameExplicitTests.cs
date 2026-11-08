namespace Open.Database.Extensions.Tests;

#nullable enable

// A name the caller gives is ALWAYS used exactly as written: never matched, never "corrected" to the procedure's casing, underscores or prefix, never overridden by inference,
// and when every parameter is explicit the provider is not even asked for the procedure's names.
[ExcludeFromCodeCoverage]
public static class ParameterNameExplicitTests
{
	// Real names that a matcher could be tempted to "correct" an explicit name to.
	static readonly string[] Tempting =
		["@Author", "@AUTHOR", "@p_author", "@author_id", "@Author_Id", "@custom", "@Custom", "@mode", "@MODE", "@m_author", "@class", "@Region"];

	// The same without the two spellings of author id, so a derived AuthorId finds exactly one.
	static readonly string[] TemptingWithAuthorId =
		["@Author_Id", "@Author", "@AUTHOR", "@p_author", "@custom", "@Custom", "@mode", "@MODE", "@m_author", "@class", "@Region"];

	public static TheoryData<string> Names { get; } = new()
		{
			"custom", "@custom", "Custom", "author", "@author", "Author", "AUTHOR", "author_id", "@Author_Id", "authorId",
			"p_author", "@p_author", "pAuthor", "m_author", "_author", "@mode", "mode", "class", "@class", "region",
		};

	// The names that cannot collide with a derived AuthorId (see the collision tests below).
	public static TheoryData<string> NamesNextToAuthorId { get; } = new()
		{
			"custom", "@custom", "Custom", "author", "@author", "Author", "AUTHOR", "authorId",
			"p_author", "@p_author", "pAuthor", "m_author", "_author", "@mode", "mode", "class", "@class", "region",
		};

	static readonly Action<ExpressiveCommand, string>[] Overloads =
	[
		(c, name) => c.AddParam(name, 5),
		(c, name) => c.AddParam(name, 5L),
		(c, name) => c.AddParam(name, "text"),
		(c, name) => c.AddParam(name, (object?)null),
		(c, name) => c.AddParam(name, (object)5, DbType.Int32),
		(c, name) => c.AddParam(name, ParameterDirection.Output),
		(c, name) => c.AddParam<int>(name, 3, null),
		(c, name) => c.AddParamIf(true, name, 5),
		(c, name) => c.AddParamIf(true, name, "text"),
	];

	static readonly Action<ExpressiveDbCommand, string>[] AsyncOverloads =
	[
		(c, name) => c.AddParam(name, 5),
		(c, name) => c.AddParam(name, "text"),
		(c, name) => c.AddParam(name, (object?)null),
		(c, name) => c.AddParamIf(true, name, 5),
	];

	// ---- exactly as given, and the provider is not asked ----

	[Theory]
	[MemberData(nameof(Names))]
	public static void ExplicitName_IsSentExactlyAsGiven_AndTheProviderIsNotAsked(string name)
	{
		foreach (Action<ExpressiveCommand, string> add in Overloads)
		{
			var run = new SyncNameRun(Tempting);

			Assert.Equal([name], run.Run(c => add(c, name)));
			Assert.Equal(0, run.Lookups);
		}
	}

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task ExplicitName_IsSentExactlyAsGiven_AndTheProviderIsNotAsked_Async(string name)
	{
		foreach (Action<ExpressiveDbCommand, string> add in AsyncOverloads)
		{
			var run = new AsyncNameRun(Tempting);

			Assert.Equal([name], await run.RunAsync(c => add(c, name)));
			Assert.Equal(0, run.Lookups);
		}
	}

	[Fact]
	public static void ExplicitNames_OnATextCommand_AreSentAsGiven()
	{
		var run = new SyncNameRun(CommandType.Text, Tempting);

		Assert.Equal(["author", "@Mode", "p_author"], run.Run(c => c.AddParam("author", 1).AddParam("@Mode", 2).AddParam("p_author", 3)));
		Assert.Equal(0, run.Lookups);
	}

	[Fact]
	public static void ExplicitName_WithAnArgumentThatCouldBeANameItself_IsNeverDerived()
	{
		string region = "west";
		int limit = 10;
		object? thing = null;
		var search = new { Author = 5 };
		var run = new SyncNameRun(Tempting);

		Assert.Equal(
			["custom", "other", "third", "@forth"],
			run.Run(c => c.AddParam("custom", region).AddParam("other", limit).AddParam("third", thing).AddParam("@forth", search.Author)));

		Assert.Equal(new Dictionary<string, object?> { ["custom"] = "west", ["other"] = 10, ["third"] = DBNull.Value, ["@forth"] = 5 }, run.Values);
		Assert.Equal(0, run.Lookups);
	}

	[Fact]
	public static void AddParamIf_WithAnExplicitName_SendsItAsGiven_OnlyWhenTheConditionHolds()
	{
		var run = new SyncNameRun(Tempting);

		Assert.Equal(["author", "@Mode"], run.Run(c => c.AddParamIf(true, "author", 1).AddParamIf(false, "other", 2).AddParamIf(true, "@Mode", "m")));
		Assert.Equal(0, run.Lookups);
	}

	// ---- next to a derived parameter: the explicit name is still untouched ----

	[Theory]
	[MemberData(nameof(NamesNextToAuthorId))]
	public static void ExplicitName_NextToADerivedOne_IsStillExact_InEitherOrder(string name)
	{
		var run = new SyncNameRun(TemptingWithAuthorId);

		Assert.Equal([name, "@Author_Id"], run.Run(c => c.AddParam(name, 5).AddParam(value: 2, expression: "AuthorId")));
		Assert.Equal(["@Author_Id", name], run.Run(c => c.AddParam(value: 2, expression: "AuthorId").AddParam(name, 5)));
		Assert.Equal(1, run.Lookups);
	}

	[Theory]
	[MemberData(nameof(NamesNextToAuthorId))]
	public static async Task ExplicitName_NextToADerivedOne_IsStillExact_InEitherOrder_Async(string name)
	{
		var run = new AsyncNameRun(TemptingWithAuthorId);

		Assert.Equal([name, "@Author_Id"], await run.RunAsync(c => c.AddParam(name, 5).AddParam(value: 2, expression: "AuthorId")));
		Assert.Equal(["@Author_Id", name], await run.RunAsync(c => c.AddParam(value: 2, expression: "AuthorId").AddParam(name, 5)));
		Assert.Equal(1, run.Lookups);
	}

	[Fact]
	public static void ExplicitName_NextToADerivedOne_NeverChangesTheDerivedNameEither()
	{
		var run = new SyncNameRun("@Author_Id", "@MODE");

		// The explicit name is the caller's; the derived one is the provider's.
		Assert.Equal(["mode", "@Author_Id"], run.Run(c => c.AddParam("mode", 1).AddParam(value: 2, expression: "authorId")));
		Assert.Equal(new Dictionary<string, object?> { ["mode"] = 1, ["@Author_Id"] = 2 }, run.Values);
	}

	// ---- an explicit name and a derived one that resolve to the same real parameter ----

	public static TheoryData<string, string, string> SameParameter { get; } = new()
	{
		{ "@region", "region", "@region" },
		{ "region", "region", "@region" },
		{ "REGION", "Region", "@region" },
		{ "@Region", "region", "@region" },
		{ "author_id", "authorId", "@author_id" },
		{ "@AUTHOR_ID", "AuthorID", "@author_id" },
		{ "Author_Id", "author_id", "Author_Id" },
	};

	[Theory]
	[MemberData(nameof(SameParameter))]
	public static void ExplicitAndDerivedResolvingToTheSameParameter_Throw_InEitherOrder(string explicitName, string expression, string real)
	{
		// Today's behavior: the derived argument is matched to the real name and compared with the explicit name ignoring case and the leading @.
		// Neither is dropped and nothing is sent; the caller must keep one of them.
		var run = new SyncNameRun(real);

		var first = Assert.Throws<InvalidOperationException>(() => run.Run(c => c.AddParam(explicitName, 1).AddParam(value: 2, expression: expression)));
		Assert.Contains("Two parameters resolve to", first.Message);
		Assert.Contains("Name one of them explicitly", first.Message);

		var second = Assert.Throws<InvalidOperationException>(() => run.Run(c => c.AddParam(value: 2, expression: expression).AddParam(explicitName, 1)));
		Assert.Contains("Two parameters resolve to", second.Message);
		Assert.Empty(run.Probe.Parameters);
	}

	[Theory]
	[MemberData(nameof(SameParameter))]
	public static async Task ExplicitAndDerivedResolvingToTheSameParameter_Throw_InEitherOrder_Async(string explicitName, string expression, string real)
	{
		var run = new AsyncNameRun(real);

		var first = await Assert.ThrowsAsync<InvalidOperationException>(() => run.RunAsync(c => c.AddParam(explicitName, 1).AddParam(value: 2, expression: expression)));
		Assert.Contains("Two parameters resolve to", first.Message);

		var second = await Assert.ThrowsAsync<InvalidOperationException>(() => run.RunAsync(c => c.AddParam(value: 2, expression: expression).AddParam(explicitName, 1)));
		Assert.Contains("Two parameters resolve to", second.Message);
	}

	[Fact]
	public static void ExplicitAndDerivedResolvingToTheSameParameter_AreOnlyCaughtWhenTheNamesAreEqualIgnoringCaseAndAt()
	{
		// Today's behavior, pinned: the explicit name is never rewritten, so "authorId" and "@author_id" are different names to the check
		// and both are sent (the provider decides what two parameters bound to one declared parameter mean).
		var run = new SyncNameRun("@author_id");

		Assert.Equal(["authorId", "@author_id"], run.Run(c => c.AddParam("authorId", 1).AddParam(value: 2, expression: "author_id")));
		Assert.Equal(new Dictionary<string, object?> { ["authorId"] = 1, ["@author_id"] = 2 }, run.Values);
	}

	// ---- AddParams(object) skips a member whose parameter was added explicitly ----

	sealed class Source
	{
		public int Author = 1;
		public int AuthorId = 2;
		public int Region = 3;
	}

	public static TheoryData<string> NamesOfTheAuthorIdParameter { get; } = new() { "@author_id", "author_id", "@AUTHOR_ID", "@Author_Id", "AUTHOR_ID" };

	[Theory]
	[MemberData(nameof(NamesOfTheAuthorIdParameter))]
	public static void AddParams_SkipsTheMemberWhoseParameterWasAddedExplicitly_InEitherOrder(string name)
	{
		var run = new SyncNameRun("@author", "@author_id", "@region");
		var expected = new Dictionary<string, object?> { ["@author"] = 1, ["@region"] = 3, [name] = 99 };

		run.Run(c => c.AddParams(new Source()).AddParam(name, 99));
		Assert.Equal(expected, run.Values);
		Assert.Equal(3, run.Probe.Parameters.Count);

		run.Run(c => c.AddParam(name, 99).AddParams(new Source()));
		Assert.Equal(expected, run.Values);
		Assert.Equal(3, run.Probe.Parameters.Count);
	}

	[Theory]
	[MemberData(nameof(NamesOfTheAuthorIdParameter))]
	public static async Task AddParams_SkipsTheMemberWhoseParameterWasAddedExplicitly_InEitherOrder_Async(string name)
	{
		var run = new AsyncNameRun("@author", "@author_id", "@region");
		var expected = new Dictionary<string, object?> { ["@author"] = 1, ["@region"] = 3, [name] = 99 };

		await run.RunAsync(c => c.AddParams(new Source()).AddParam(name, 99));
		Assert.Equal(expected, run.Values);

		await run.RunAsync(c => c.AddParam(name, 99).AddParams(new Source()));
		Assert.Equal(expected, run.Values);
	}

	[Fact]
	public static void AddParams_AnExplicitNameThatDiffersFromTheMembersParameter_IsSentBesideIt()
	{
		// The same pin as for AddParam(value): "authorId" is not "@author_id" to the check, so the member is still sent.
		var run = new SyncNameRun("@author", "@author_id", "@region");

		run.Run(c => c.AddParams(new Source()).AddParam("authorId", 99));

		Assert.Equal(new Dictionary<string, object?> { ["@author"] = 1, ["@author_id"] = 2, ["@region"] = 3, ["authorId"] = 99 }, run.Values);
	}
}
