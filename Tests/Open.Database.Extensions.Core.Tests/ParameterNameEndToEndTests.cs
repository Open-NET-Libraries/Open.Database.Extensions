namespace Open.Database.Extensions.Tests;

#nullable enable

// The pattern matrix end to end: real call sites, so the compiler itself captures the argument text for AddParam(value) and AddParamIf(condition, value),
// and AddParams(object) reads the members of real objects. Every pair of the catalog that has a call site below is run on the synchronous and the asynchronous path.
// A call site is a lambda per path because the two command types are different types; the text between the parentheses is the catalog expression.
[ExcludeFromCodeCoverage]
public static class ParameterNameEndToEndTests
{
	internal sealed record CallSite(Func<ExpressiveCommand, ExpressiveCommand> Sync, Func<ExpressiveDbCommand, ExpressiveDbCommand> Async);

	internal sealed record Dto(string[] Members, Func<ExpressiveCommand, ExpressiveCommand> Sync, Func<ExpressiveDbCommand, ExpressiveDbCommand> Async);

	sealed class AuthorRef { public int Id = 1; }

	sealed class Filter
	{
		public int AuthorId = 1;
		public AuthorRef Author = new();
		public int A = 1;
	}

	sealed class Link
	{
		public Link b;
		public Link c;
		public int Author = 1;
		public int D = 1;

		public Link() => b = c = this;
	}

	// Every member a call site reads, spelled in the style under test.
	sealed class Styles
	{
		public int author = 1, Author = 1, AUTHOR = 1, _author = 1, m_author = 1;
		public static int s_author = 1;
		public int authorId = 1, AuthorId = 1, AuthorID = 1, author_id = 1, AUTHOR_ID = 1, Author_Id = 1, _authorId = 1, m_authorId = 1;
		public static int s_author_id = 1;
		public int address2 = 1, Address2 = 1, m_address2 = 1, line1 = 1, x1 = 1, a = 1, A = 1, d = 1, D = 1;
		public int HTTPRequestId = 1, XMLData = 1, UserID = 1, OrderIDs = 1, Código = 1, código = 1, _código = 1;
		public int @class = 1, @event = 1;
		public int CustomerBillingAddressLineSecondPostalCodeRegionCountryPhoneNumberFaxEmailNotes = 1;
		public int customerBillingAddressLineSecondPostalCodeRegionCountryPhoneNumberFaxEmailNotes = 1;

		public Styles search;
		public Filter filter = new();

		public Styles() => search = this;

		public Dictionary<string, CallSite> Sites() => new()
		{
			["author"] = new(cmd => cmd.AddParam(author), cmd => cmd.AddParam(author)),
			["Author"] = new(cmd => cmd.AddParam(Author), cmd => cmd.AddParam(Author)),
			["AUTHOR"] = new(cmd => cmd.AddParam(AUTHOR), cmd => cmd.AddParam(AUTHOR)),
			["_author"] = new(cmd => cmd.AddParam(_author), cmd => cmd.AddParam(_author)),
			["m_author"] = new(cmd => cmd.AddParam(m_author), cmd => cmd.AddParam(m_author)),
			["s_author"] = new(cmd => cmd.AddParam(s_author), cmd => cmd.AddParam(s_author)),
			["this.author"] = new(cmd => cmd.AddParam(this.author), cmd => cmd.AddParam(this.author)),
			["this._author"] = new(cmd => cmd.AddParam(this._author), cmd => cmd.AddParam(this._author)),
			["search.Author"] = new(cmd => cmd.AddParam(search.Author), cmd => cmd.AddParam(search.Author)),
			["search?.Author"] = new(cmd => cmd.AddParam(search?.Author), cmd => cmd.AddParam(search?.Author)),
			["search!.Author"] = new(cmd => cmd.AddParam(search!.Author), cmd => cmd.AddParam(search!.Author)),
			["(search.Author)"] = new(cmd => cmd.AddParam(search.Author), cmd => cmd.AddParam(search.Author)),
			["a.b.c.Author"] = new(cmd => { var a = new Link(); return cmd.AddParam(a.b.c.Author); }, cmd => { var a = new Link(); return cmd.AddParam(a.b.c.Author); }),

			["authorId"] = new(cmd => cmd.AddParam(authorId), cmd => cmd.AddParam(authorId)),
			["AuthorId"] = new(cmd => cmd.AddParam(AuthorId), cmd => cmd.AddParam(AuthorId)),
			["AuthorID"] = new(cmd => cmd.AddParam(AuthorID), cmd => cmd.AddParam(AuthorID)),
			["author_id"] = new(cmd => cmd.AddParam(author_id), cmd => cmd.AddParam(author_id)),
			["AUTHOR_ID"] = new(cmd => cmd.AddParam(AUTHOR_ID), cmd => cmd.AddParam(AUTHOR_ID)),
			["Author_Id"] = new(cmd => cmd.AddParam(Author_Id), cmd => cmd.AddParam(Author_Id)),
			["_authorId"] = new(cmd => cmd.AddParam(_authorId), cmd => cmd.AddParam(_authorId)),
			["m_authorId"] = new(cmd => cmd.AddParam(m_authorId), cmd => cmd.AddParam(m_authorId)),
			["s_author_id"] = new(cmd => cmd.AddParam(s_author_id), cmd => cmd.AddParam(s_author_id)),
			["this.authorId"] = new(cmd => cmd.AddParam(this.authorId), cmd => cmd.AddParam(this.authorId)),
			["filter.AuthorId"] = new(cmd => cmd.AddParam(filter.AuthorId), cmd => cmd.AddParam(filter.AuthorId)),
			["filter.Author.Id"] = new(cmd => cmd.AddParam(filter.Author.Id), cmd => cmd.AddParam(filter.Author.Id)),

			["address2"] = new(cmd => cmd.AddParam(address2), cmd => cmd.AddParam(address2)),
			["Address2"] = new(cmd => cmd.AddParam(Address2), cmd => cmd.AddParam(Address2)),
			["m_address2"] = new(cmd => cmd.AddParam(m_address2), cmd => cmd.AddParam(m_address2)),
			["line1"] = new(cmd => cmd.AddParam(line1), cmd => cmd.AddParam(line1)),
			["x1"] = new(cmd => cmd.AddParam(x1), cmd => cmd.AddParam(x1)),
			["a"] = new(cmd => cmd.AddParam(a), cmd => cmd.AddParam(a)),
			["A"] = new(cmd => cmd.AddParam(A), cmd => cmd.AddParam(A)),
			["filter.A"] = new(cmd => cmd.AddParam(filter.A), cmd => cmd.AddParam(filter.A)),
			["d"] = new(cmd => cmd.AddParam(d), cmd => cmd.AddParam(d)),
			["D"] = new(cmd => cmd.AddParam(D), cmd => cmd.AddParam(D)),
			["a.b.c.D"] = new(cmd => { var a = new Link(); return cmd.AddParam(a.b.c.D); }, cmd => { var a = new Link(); return cmd.AddParam(a.b.c.D); }),
			["HTTPRequestId"] = new(cmd => cmd.AddParam(HTTPRequestId), cmd => cmd.AddParam(HTTPRequestId)),
			["XMLData"] = new(cmd => cmd.AddParam(XMLData), cmd => cmd.AddParam(XMLData)),
			["UserID"] = new(cmd => cmd.AddParam(UserID), cmd => cmd.AddParam(UserID)),
			["OrderIDs"] = new(cmd => cmd.AddParam(OrderIDs), cmd => cmd.AddParam(OrderIDs)),
			["Código"] = new(cmd => cmd.AddParam(Código), cmd => cmd.AddParam(Código)),
			["código"] = new(cmd => cmd.AddParam(código), cmd => cmd.AddParam(código)),
			["_código"] = new(cmd => cmd.AddParam(_código), cmd => cmd.AddParam(_código)),
			["@class"] = new(cmd => cmd.AddParam(@class), cmd => cmd.AddParam(@class)),
			["@event"] = new(cmd => cmd.AddParam(@event), cmd => cmd.AddParam(@event)),
			[ParameterNameCatalog.LongPascal] = new(
				cmd => cmd.AddParam(CustomerBillingAddressLineSecondPostalCodeRegionCountryPhoneNumberFaxEmailNotes),
				cmd => cmd.AddParam(CustomerBillingAddressLineSecondPostalCodeRegionCountryPhoneNumberFaxEmailNotes)),
			[ParameterNameCatalog.LongCamel] = new(
				cmd => cmd.AddParam(customerBillingAddressLineSecondPostalCodeRegionCountryPhoneNumberFaxEmailNotes),
				cmd => cmd.AddParam(customerBillingAddressLineSecondPostalCodeRegionCountryPhoneNumberFaxEmailNotes)),
		};

		// AddParamIf at a call site: the argument is checked and named whether or not the condition holds.
		public ExpressiveCommand AddIf(ExpressiveCommand cmd, bool condition)
			=> cmd.AddParamIf(condition, Author).AddParamIf(!condition, search.AuthorId);

		public ExpressiveDbCommand AddIf(ExpressiveDbCommand cmd, bool condition)
			=> cmd.AddParamIf(condition, Author).AddParamIf(!condition, search.AuthorId);
	}

	/// <summary>The call sites by the text of their argument.</summary>
	internal static IReadOnlyDictionary<string, CallSite> CallSites { get; } = new Styles().Sites();

	/// <summary>The catalog pairs that have a call site.</summary>
	internal static TheoryData<string, string> CallSitePairs(bool prefixed)
	{
		var data = new TheoryData<string, string>();
		foreach (KeyValuePair<string, string> pair in ParameterNameCatalog.PairsOf(prefixed))
		{
			if (CallSites.ContainsKey(pair.Key)) data.Add(pair.Key, pair.Value);
		}

		return data;
	}

	public static TheoryData<string, string> PlainCallSitePairs { get; } = CallSitePairs(prefixed: false);

	public static TheoryData<string, string> CallSiteExpressions { get; } = BuildCallSiteExpressions();

	static TheoryData<string, string> BuildCallSiteExpressions()
	{
		var data = new TheoryData<string, string>();
		foreach (string expression in CallSites.Keys)
			data.Add(expression, ParameterNameCatalog.LastOf(expression));
		return data;
	}

	[Fact]
	public static void EveryCallSite_IsAPatternOfTheCatalog_SoTheTwoTablesCannotDrift()
	{
		var catalog = new HashSet<string>(ParameterNameCatalog.Groups.SelectMany(g => g.CSharp));

		string[] unknown = [.. CallSites.Keys.Where(key => !catalog.Contains(key))];

		Assert.True(unknown.Length == 0, "Not in the catalog: " + string.Join(", ", unknown));
	}

	// ---- AddParam(value) ----

	[Theory]
	[MemberData(nameof(PlainCallSitePairs))]
	public static void CallSite_FindsTheRealName(string expression, string real)
	{
		CallSite site = CallSites[expression];
		var run = new SyncNameRun(real);

		Assert.Equal([real], run.Run(c => site.Sync(c)));
		Assert.Equal(1, run.Probe.Parameters.Single().Value);

		Assert.Equal([real], run.Run(c => site.Sync(c))); // from memory
		Assert.Equal(1, run.Lookups);
	}

	[Theory]
	[MemberData(nameof(PlainCallSitePairs))]
	public static async Task CallSite_FindsTheRealName_Async(string expression, string real)
	{
		CallSite site = CallSites[expression];
		var run = new AsyncNameRun(real);

		Assert.Equal([real], await run.RunAsync(c => site.Async(c)));
		Assert.Equal(1, run.Values[real]);

		Assert.Equal([real], await run.RunAsync(c => site.Async(c)));
		Assert.Equal(1, run.Lookups);
		Assert.Equal(0, run.SyncLookups);
	}

	[Theory]
	[MemberData(nameof(CallSiteExpressions))]
	public static void CallSite_WithoutRealNames_UsesTheLastMemberAsWritten(string expression, string last)
	{
		CallSite site = CallSites[expression];

		Assert.Equal([last], new SyncNameRun().Run(c => site.Sync(c)));
		Assert.Equal([last], new SyncNameRun(CommandType.Text, "@never").Run(c => site.Sync(c)));
	}

	[Theory]
	[MemberData(nameof(CallSiteExpressions))]
	public static async Task CallSite_WithoutRealNames_UsesTheLastMemberAsWritten_Async(string expression, string last)
	{
		CallSite site = CallSites[expression];

		Assert.Equal([last], await new AsyncNameRun().RunAsync(c => site.Async(c)));
		Assert.Equal([last], await new AsyncNameRun(CommandType.Text, "@never").RunAsync(c => site.Async(c)));
	}

	// ---- AddParamIf(condition, value) ----

	[Fact]
	public static void AddParamIf_AtACallSite_AddsTheDerivedParam_OnlyWhenTheConditionHolds()
	{
		var styles = new Styles();
		var run = new SyncNameRun("@author", "@author_id", "@end_date");

		Assert.Equal(["@author"], run.Run(c => styles.AddIf(c, true)));
		Assert.Equal(["@author_id"], run.Run(c => styles.AddIf(c, false)));
		Assert.Equal(1, run.Lookups);
	}

	[Fact]
	public static async Task AddParamIf_AtACallSite_AddsTheDerivedParam_OnlyWhenTheConditionHolds_Async()
	{
		var styles = new Styles();
		var run = new AsyncNameRun("@author", "@author_id", "@end_date");

		Assert.Equal(["@author"], await run.RunAsync(c => styles.AddIf(c, true)));
		Assert.Equal(["@author_id"], await run.RunAsync(c => styles.AddIf(c, false)));
		Assert.Equal(1, run.Lookups);
	}

	// ---- AddParams(object): the members of an object, in each spelling ----

	sealed class PascalDto
	{
		public int Author = 1;
		public int AuthorId { get; set; } = 2;
		public string? AddressLine2 { get; set; } = "x";
		public DateTime? EndDate { get; set; } = new(2026, 1, 2);
		public int? Limit { get; set; } = null;
		public int HTTPRequestId = 6;
		public string Unrelated = "u";
	}

	sealed class CamelDto
	{
		public int author = 1;
		public int authorId = 2;
		public string? addressLine2 = "x";
		public DateTime? endDate = new(2026, 1, 2);
		public int? limit = null;
		public int httpRequestId = 6;
		public string unrelated = "u";
	}

	sealed class SnakeDto
	{
		public int author = 1;
		public int author_id = 2;
		public string? address_line_2 = "x";
		public DateTime? end_date = new(2026, 1, 2);
		public int? limit = null;
		public int http_request_id = 6;
		public string unrelated = "u";
	}

	sealed class UpperDto
	{
		public int AUTHOR = 1;
		public int AUTHOR_ID = 2;
		public string? ADDRESS_LINE_2 = "x";
		public DateTime? END_DATE = new(2026, 1, 2);
		public int? LIMIT = null;
		public int HTTP_REQUEST_ID = 6;
		public string UNRELATED = "u";
	}

	internal static IReadOnlyDictionary<string, Dto> Dtos { get; } = new Dictionary<string, Dto>
	{
		["Pascal"] = new(["Author", "AuthorId", "AddressLine2", "EndDate", "Limit", "HTTPRequestId", "Unrelated"], c => c.AddParams(new PascalDto()), c => c.AddParams(new PascalDto())),
		["camel"] = new(["author", "authorId", "addressLine2", "endDate", "limit", "httpRequestId", "unrelated"], c => c.AddParams(new CamelDto()), c => c.AddParams(new CamelDto())),
		["snake"] = new(["author", "author_id", "address_line_2", "end_date", "limit", "http_request_id", "unrelated"], c => c.AddParams(new SnakeDto()), c => c.AddParams(new SnakeDto())),
		["UPPER"] = new(["AUTHOR", "AUTHOR_ID", "ADDRESS_LINE_2", "END_DATE", "LIMIT", "HTTP_REQUEST_ID", "UNRELATED"], c => c.AddParams(new UpperDto()), c => c.AddParams(new UpperDto())),
	};

	// The values the members hold, by position: the six words and then Unrelated.
	internal static object?[] ValuesAsWritten { get; } = [.. ParameterNameCatalog.MemberValues, "u"];

	/// <summary>Every object spelling crossed with every spelling of the procedure's names, with and without the leading <c>@</c>.</summary>
	internal static TheoryData<string, string, bool> MemberCases(IReadOnlyDictionary<string, string[]> sets)
	{
		var data = new TheoryData<string, string, bool>();
		foreach (string dto in Dtos.Keys)
		{
			foreach (string set in sets.Keys)
			{
				data.Add(dto, set, false);
				data.Add(dto, set, true);
			}
		}

		return data;
	}

	public static TheoryData<string, string, bool> PlainMemberCases { get; } = MemberCases(ParameterNameCatalog.MemberReals);

	public static TheoryData<string> DtoNames { get; } = BuildDtoNames();

	static TheoryData<string> BuildDtoNames()
	{
		var data = new TheoryData<string>();
		foreach (string dto in Dtos.Keys) data.Add(dto);
		return data;
	}

	internal static string[] RealNames(IReadOnlyDictionary<string, string[]> sets, string set, bool at)
		=> [.. sets[set].Select(name => at ? "@" + name : name)];

	internal static Dictionary<string, object?> Expected(string[] real)
	{
		var expected = new Dictionary<string, object?>();
		for (int i = 0; i < real.Length; i++) expected[real[i]] = ParameterNameCatalog.MemberValues[i];
		return expected;
	}

	[Theory]
	[MemberData(nameof(PlainMemberCases))]
	public static void AddParams_MatchesEveryMemberToItsParameter(string dto, string set, bool at)
	{
		string[] real = RealNames(ParameterNameCatalog.MemberReals, set, at);
		Dto source = Dtos[dto];
		var run = new SyncNameRun([.. real, "@something_else"]);

		run.Run(c => source.Sync(c)); // Unrelated is not declared by the procedure, so it is not sent
		Assert.Equal(Expected(real), run.Values);
		Assert.Equal(6, run.Probe.Parameters.Count);

		run.Run(c => source.Sync(c)); // from memory
		Assert.Equal(Expected(real), run.Values);
		Assert.Equal(1, run.Lookups);
	}

	[Theory]
	[MemberData(nameof(PlainMemberCases))]
	public static async Task AddParams_MatchesEveryMemberToItsParameter_Async(string dto, string set, bool at)
	{
		string[] real = RealNames(ParameterNameCatalog.MemberReals, set, at);
		Dto source = Dtos[dto];
		var run = new AsyncNameRun([.. real, "@something_else"]);

		await run.RunAsync(c => source.Async(c));
		Assert.Equal(Expected(real), run.Values);

		await run.RunAsync(c => source.Async(c));
		Assert.Equal(Expected(real), run.Values);
		Assert.Equal(1, run.Lookups);
		Assert.Equal(0, run.SyncLookups);
	}

	[Theory]
	[MemberData(nameof(DtoNames))]
	public static void AddParams_WithoutRealNames_SendsEveryMemberAsWritten(string dto)
	{
		Dto source = Dtos[dto];
		Dictionary<string, object?> expected = [];
		for (int i = 0; i < source.Members.Length; i++) expected[source.Members[i]] = ValuesAsWritten[i];

		var none = new SyncNameRun();
		none.Run(c => source.Sync(c));
		Assert.Equal(expected, none.Values);

		var text = new SyncNameRun(CommandType.Text, "@never");
		text.Run(c => source.Sync(c));
		Assert.Equal(expected, text.Values);
		Assert.Equal(0, text.Lookups);
	}

	[Theory]
	[MemberData(nameof(DtoNames))]
	public static async Task AddParams_WithoutRealNames_SendsEveryMemberAsWritten_Async(string dto)
	{
		Dto source = Dtos[dto];
		Dictionary<string, object?> expected = [];
		for (int i = 0; i < source.Members.Length; i++) expected[source.Members[i]] = ValuesAsWritten[i];

		var none = new AsyncNameRun();
		await none.RunAsync(c => source.Async(c));
		Assert.Equal(expected, none.Values);

		var text = new AsyncNameRun(CommandType.Text, "@never");
		await text.RunAsync(c => source.Async(c));
		Assert.Equal(expected, text.Values);
		Assert.Equal(0, text.Lookups);
	}

	sealed class Collide
	{
		public int Author = 1;
		public int AUTHOR = 2;
	}

	sealed class CollideUnderscore
	{
		public int Author = 1;
		public int Author_ = 2;
	}

	sealed class Nothing
	{
		public int Unrelated = 1;
		public string? Other = "o";
	}

	[Fact]
	public static void AddParams_TwoMembersThatResolveToOneParameter_Throw()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@author").Run(c => c.AddParams(new Collide())));
		Assert.Contains("Two parameters resolve to '@author'", ex.Message);

		ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@author").Run(c => c.AddParams(new CollideUnderscore())));
		Assert.Contains("Two parameters resolve to '@author'", ex.Message);
	}

	[Fact]
	public static async Task AddParams_TwoMembersThatResolveToOneParameter_Throw_Async()
	{
		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => new AsyncNameRun("@author").RunAsync(c => c.AddParams(new Collide())));

		Assert.Contains("Two parameters resolve to '@author'", ex.Message);
	}

	[Fact]
	public static void AddParams_MembersThatMatchNothing_AreNotSent_AndTheProcedureKeepsItsDefaults()
	{
		var run = new SyncNameRun("@author", "@author_id");

		Assert.Empty(run.Run(c => c.AddParams(new Nothing())));
		Assert.Equal(["@author"], run.Run(c => c.AddParams(new OnlyAuthor())));
	}

	sealed class OnlyAuthor
	{
		public int Author = 1;
		public int Unrelated = 2;
	}
}
