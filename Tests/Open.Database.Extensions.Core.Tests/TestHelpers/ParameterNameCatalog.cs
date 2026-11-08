namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// The catalog of naming patterns the parameter-name inference is tested against: one table, read here at a glance.
/// </summary>
/// <remarks>
/// <para>Each <see cref="Group"/> is a set of names that mean the same words (author, author id, address 2, ...) spelled in the conventions a consumer really has on both sides:
/// the C# expression (<see cref="Group.CSharp"/>) and the real parameter names of the stored procedure (<see cref="Group.Plain"/> and <see cref="Group.Prefixed"/>).
/// Every C# expression of a group is crossed with every real name of the same group, written with and without the leading <c>@</c>, and each pair must resolve to that real name exactly as the provider reported it.</para>
/// <para>How a pair is matched (a chain such as <c>filter.Author.Id</c> tries its most specific ending first: <c>filterAuthorId</c>, <c>AuthorId</c>, <c>Id</c>):</para>
/// <code>
/// tier 1  exact                                 author        finds  @author
/// tier 2  ignoring case                         Author        finds  @AUTHOR
/// tier 3  ignoring case and underscores         AuthorID      finds  @author_id,   _author finds @author,   Address2 finds @address_2
/// tier 4  after removing ONE parameter prefix   author        finds  @p_author, @pAuthor, @in_author, @i_author, @prm_author, @par_author, @v_author
///         (inside each candidate's turn, after tiers 1-3 failed for that candidate)
/// field prefix on the C# side: the last member m_author or s_author (or _author) also tries author, after the name as written
/// </code>
/// <code>
/// group           C# expressions (Plain = tiers 1-3, Prefixed = tier 4 on the procedure side; m_/s_ = field prefix on the C# side)
/// author          author  Author  AUTHOR  _author  m_author  s_author  this.author  this._author  search.Author  search?.Author  search!.Author  (search.Author)  a.b.c.Author
///                 plain:    Author  author  AUTHOR
///                 prefixed: p_author  P_AUTHOR  p_Author  pAuthor  in_author  i_author  prm_author  par_author  v_author
/// author id       authorId  AuthorId  AuthorID  author_id  AUTHOR_ID  Author_Id  _authorId  m_authorId  s_author_id  this.authorId  filter.AuthorId  filter.Author.Id
///                 plain:    author_id  Author_Id  AuthorID  authorId  AUTHOR_ID  AuthorId  authorid
///                 prefixed: p_author_id  pAuthorId  in_author_id  i_author_id  prm_author_id  par_author_id  v_author_id
/// address 2       address2  Address2  Address_2  ADDRESS2  m_address2  order.Address2           plain: address_2 address2 Address2 ADDRESS_2 Address_2    prefixed: p_address_2 pAddress2 in_address2 v_address_2
/// line 1          line1  Line1  line_1  s_line1                                                  plain: line_1 LINE1 Line1                                    prefixed: p_line_1 pLine1
/// x 1             x1  X1  m_x1                                                                   plain: x1 X1 x_1                                             prefixed: p_x1 pX1 i_x1
/// a               a  A  _a  m_a  filter.A                                                        plain: a A                                                   prefixed: p_a pA i_a v_a
/// d               d  D  a.b.c.D  m_d                                                             plain: d D                                                   prefixed: p_d pD in_d
/// http request id HTTPRequestId  httpRequestId  http_request_id  m_HTTPRequestId  req.HTTPRequestId   plain: http_request_id HttpRequestId HTTPRequestID httprequestid   prefixed: p_http_request_id pHttpRequestId in_http_request_id
/// xml data        XMLData  xmlData  m_xmlData                                                    plain: xml_data XmlData XML_DATA                             prefixed: p_xml_data pXmlData v_xml_data
/// user id         UserID  userId  s_userID  user.UserID                                           plain: user_id UserId USER_ID userid                         prefixed: p_user_id pUserId i_user_id
/// order ids       OrderIDs  orderIds  m_orderIDs                                                 plain: order_ids OrderIds ORDER_IDS                          prefixed: p_order_ids pOrderIds
/// non-ASCII       Código  código  _código  m_código                                              plain: código CÓDIGO Código                                  prefixed: p_código pCódigo P_CÓDIGO
/// class (keyword) @class  this.@class  order.@class                                              plain: class Class CLASS                                     prefixed: p_class pClass in_class
/// event (keyword) @event  a.@event                                                               plain: event Event EVENT                                     prefixed: p_event pEvent
/// long (14 words) Pascal  camel  snake_case  m_camel  filter.Pascal                              plain: snake Pascal camel UPPER_SNAKE                        prefixed: p_snake pPascal in_snake
/// </code>
/// <para>A pair is "plain" when it needs nothing beyond tiers 1 to 3 (<see cref="PlainPairs"/>), and "prefixed" when the C# expression ends in an <c>m_</c> or <c>s_</c> member
/// or the real name carries a parameter prefix (<see cref="PrefixPairs"/>).</para>
/// </remarks>
[ExcludeFromCodeCoverage]
public static class ParameterNameCatalog
{
	/// <summary>Names that mean the same words, in the spellings of both sides.</summary>
	/// <param name="Words">What they mean, for reading.</param>
	/// <param name="CSharp">The argument expressions as a call site writes them.</param>
	/// <param name="Plain">Real parameter names matched by the first three tiers.</param>
	/// <param name="Prefixed">Real parameter names that carry a parameter prefix.</param>
	public sealed record Group(string Words, string[] CSharp, string[] Plain, string[] Prefixed);

	static readonly string[] LongWords =
		["Customer", "Billing", "Address", "Line", "Second", "Postal", "Code", "Region", "Country", "Phone", "Number", "Fax", "Email", "Notes"];

	static string Pascal => string.Concat(LongWords);

	static string Camel => char.ToLowerInvariant(Pascal[0]) + Pascal.Substring(1);

	static string Snake => string.Join("_", LongWords).ToLowerInvariant();

	/// <summary>The long names the call sites declare as members of that spelling (see the end-to-end tests).</summary>
	public static string LongPascal => Pascal;

	/// <summary>The same long name in camel case.</summary>
	public static string LongCamel => Camel;

	/// <summary>Every group, in table order.</summary>
	public static IReadOnlyList<Group> Groups { get; } =
	[
		new("author",
			["author", "Author", "AUTHOR", "_author", "m_author", "s_author", "this.author", "this._author", "search.Author", "search?.Author", "search!.Author", "(search.Author)", "a.b.c.Author"],
			["Author", "author", "AUTHOR"],
			["p_author", "P_AUTHOR", "p_Author", "pAuthor", "in_author", "i_author", "prm_author", "par_author", "v_author"]),
		new("author id",
			["authorId", "AuthorId", "AuthorID", "author_id", "AUTHOR_ID", "Author_Id", "_authorId", "m_authorId", "s_author_id", "this.authorId", "filter.AuthorId", "filter.Author.Id"],
			["author_id", "Author_Id", "AuthorID", "authorId", "AUTHOR_ID", "AuthorId", "authorid"],
			["p_author_id", "pAuthorId", "in_author_id", "i_author_id", "prm_author_id", "par_author_id", "v_author_id"]),
		new("address 2",
			["address2", "Address2", "Address_2", "ADDRESS2", "m_address2", "order.Address2"],
			["address_2", "address2", "Address2", "ADDRESS_2", "Address_2"],
			["p_address_2", "pAddress2", "in_address2", "v_address_2"]),
		new("line 1",
			["line1", "Line1", "line_1", "s_line1"],
			["line_1", "LINE1", "Line1"],
			["p_line_1", "pLine1"]),
		new("x 1",
			["x1", "X1", "m_x1"],
			["x1", "X1", "x_1"],
			["p_x1", "pX1", "i_x1"]),
		new("a",
			["a", "A", "_a", "m_a", "filter.A"],
			["a", "A"],
			["p_a", "pA", "i_a", "v_a"]),
		new("d",
			["d", "D", "a.b.c.D", "m_d"],
			["d", "D"],
			["p_d", "pD", "in_d"]),
		new("http request id",
			["HTTPRequestId", "httpRequestId", "http_request_id", "m_HTTPRequestId", "req.HTTPRequestId"],
			["http_request_id", "HttpRequestId", "HTTPRequestID", "httprequestid"],
			["p_http_request_id", "pHttpRequestId", "in_http_request_id"]),
		new("xml data",
			["XMLData", "xmlData", "m_xmlData"],
			["xml_data", "XmlData", "XML_DATA"],
			["p_xml_data", "pXmlData", "v_xml_data"]),
		new("user id",
			["UserID", "userId", "s_userID", "user.UserID"],
			["user_id", "UserId", "USER_ID", "userid"],
			["p_user_id", "pUserId", "i_user_id"]),
		new("order ids",
			["OrderIDs", "orderIds", "m_orderIDs"],
			["order_ids", "OrderIds", "ORDER_IDS"],
			["p_order_ids", "pOrderIds"]),
		new("non-ASCII",
			["Código", "código", "_código", "m_código"],
			["código", "CÓDIGO", "Código"],
			["p_código", "pCódigo", "P_CÓDIGO"]),
		new("class (keyword)",
			["@class", "this.@class", "order.@class"],
			["class", "Class", "CLASS"],
			["p_class", "pClass", "in_class"]),
		new("event (keyword)",
			["@event", "a.@event"],
			["event", "Event", "EVENT"],
			["p_event", "pEvent"]),
		new("long (14 words)",
			[Pascal, Camel, Snake, "m_" + Camel, "filter." + Pascal],
			[Snake, Pascal, Camel, Snake.ToUpperInvariant()],
			["p_" + Snake, "p" + Pascal, "in_" + Snake]),
	];

	/// <summary>The last member of an expression as written, which is the name sent when the real names are not known.</summary>
	public static string LastOf(string expression)
	{
		string e = expression.Trim('(', ')');
		int dot = e.LastIndexOf('.');
		if (dot >= 0) e = e.Substring(dot + 1);
		return e.TrimStart('@').TrimEnd('!');
	}

	/// <summary>True for <c>m_author</c> and <c>s_author</c>: a field prefix the C# side must strip (a lone <c>_</c> is already ignored by tier 3).</summary>
	public static bool HasFieldPrefix(string last)
		=> last.Length >= 3 && (last[0] == 'm' || last[0] == 's') && last[1] == '_' && char.IsLetterOrDigit(last[2]);

	/// <summary>Pairs (expression, real name) that match with tiers 1 to 3 alone.</summary>
	public static TheoryData<string, string> PlainPairs { get; } = Pairs(prefixed: false);

	/// <summary>Pairs (expression, real name) that need the parameter-prefix tier or the field-prefix rule.</summary>
	public static TheoryData<string, string> PrefixPairs { get; } = Pairs(prefixed: true);

	/// <summary>The same pairs as plain data, for tests that build their own cases.</summary>
	public static IEnumerable<KeyValuePair<string, string>> PairsOf(bool prefixed)
	{
		foreach (Group group in Groups)
		{
			foreach (string expression in group.CSharp)
			{
				bool fieldPrefix = HasFieldPrefix(LastOf(expression));
				if (fieldPrefix == prefixed)
				{
					foreach (string real in group.Plain)
					{
						yield return new(expression, real);
						yield return new(expression, "@" + real);
					}
				}

				if (!prefixed) continue;
				foreach (string real in group.Prefixed)
				{
					yield return new(expression, real);
					yield return new(expression, "@" + real);
				}
			}
		}
	}

	static TheoryData<string, string> Pairs(bool prefixed)
	{
		var data = new TheoryData<string, string>();
		foreach (KeyValuePair<string, string> pair in PairsOf(prefixed)) data.Add(pair.Key, pair.Value);
		return data;
	}

	/// <summary>Every C# expression of the catalog with its last member as written.</summary>
	public static TheoryData<string, string> Expressions { get; } = BuildExpressions();

	static TheoryData<string, string> BuildExpressions()
	{
		var data = new TheoryData<string, string>();
		foreach (Group group in Groups)
		{
			foreach (string expression in group.CSharp)
				data.Add(expression, LastOf(expression));
		}

		return data;
	}

	/// <summary>
	/// Pairs (expression, real name) that must NOT match: a false match that silently binds is worse than a clear error.
	/// They stay unmatched with and without the parameter-prefix tier.
	/// </summary>
	public static TheoryData<string, string> Negatives { get; } = Of(
		// the owner's list
		new("ages", "@pages"), new("Id", "@author_id"), new("author", "@authors"), new("a", "@ab"), new("name", "@username"), new("x1", "@x"), new("ate", "@date"),
		// a longer or shorter name is another name
		new("x", "@x1"), new("author_id", "@author"), new("authorId", "@author"), new("author", "@author_id"), new("authorId", "@authorIds"), new("Id", "@p_author_id"), new("author", "@p_authors"),
		// what looks like a prefix but is not one: a lower-case p must be followed by an upper-case letter, the others by an underscore
		new("id", "@pid"), new("ut", "@input"), new("put", "@input"), new("ame", "@name"), new("author", "@PAuthor"), new("author", "@Pauthor"), new("author", "@pauthor"), new("author", "@vauthor"), new("uthor", "@author"), new("age", "@page"),
		// only ONE prefix is removed
		new("author", "@p_p_author"), new("author", "@in_p_author"), new("author", "@p_in_author"),
		// the C# side has no prefix stripping beyond the field prefixes _, m_ and s_, in lower case, followed by a letter or digit
		new("p_author", "@author"), new("pAuthor", "@author"), new("in_author", "@author"), new("mauthor", "@author"), new("my_author", "@author"), new("ms_author", "@author"),
		new("M_author", "@author"), new("S_author", "@author"), new("m_author", "@xauthor"), new("m_author", "@author_x"), new("s_author", "@authors"), new("m_", "@x"),
		// real names that are only a prefix, or less
		new("x", "@"), new("x", "@p"), new("x", "@p_"), new("x", "@in_"));

	/// <summary>
	/// Plausible spellings that are deliberately NOT matched (see "Not done / for the owner" in the pull request): a camel-case prefix other than <c>p</c>, other prefixes, suffixes, plurals.
	/// If one of them is ever added, move its pair to <see cref="PlainPairs"/> data.
	/// </summary>
	public static TheoryData<string, string> Unmatched { get; } = Of(
		new("author", "@iAuthor"), new("author", "@vAuthor"), new("author", "@inAuthor"), new("author", "@prmAuthor"), new("author", "@parAuthor"),
		new("author", "@param_author"), new("author", "@arg_author"), new("author", "@author_out"), new("author", "@author_in"), new("authorId", "@author_id_param"),
		new("order", "@order_id"));

	static TheoryData<string, string> Of(params KeyValuePair<string, string>[] pairs)
	{
		var data = new TheoryData<string, string>();
		foreach (KeyValuePair<string, string> pair in pairs) data.Add(pair.Key, pair.Value);
		return data;
	}

	// ---- the members of an object (AddParams): six words, in the spellings a procedure may declare them ----

	/// <summary>The value each of the six members holds, in order: author, author id, address line 2, end date, limit (a null), http request id.</summary>
	public static object?[] MemberValues { get; } = [1, 2, "x", new DateTime(2026, 1, 2), DBNull.Value, 6];

	/// <summary>The procedure's parameter names for the six members, by convention (without the leading @).</summary>
	public static IReadOnlyDictionary<string, string[]> MemberReals { get; } = new Dictionary<string, string[]>
	{
		["snake"] = ["author", "author_id", "address_line_2", "end_date", "limit", "http_request_id"],
		["Pascal"] = ["Author", "AuthorId", "AddressLine2", "EndDate", "Limit", "HttpRequestId"],
		["camel"] = ["author", "authorId", "addressLine2", "endDate", "limit", "httpRequestId"],
		["UPPER"] = ["AUTHOR", "AUTHOR_ID", "ADDRESS_LINE_2", "END_DATE", "LIMIT", "HTTP_REQUEST_ID"],
		["flat"] = ["author", "authorid", "addressline2", "enddate", "limit", "httprequestid"],
	};

	/// <summary>The same with a parameter prefix (they need the prefix tier).</summary>
	public static IReadOnlyDictionary<string, string[]> MemberRealsPrefixed { get; } = new Dictionary<string, string[]>
	{
		["p_snake"] = ["p_author", "p_author_id", "p_address_line_2", "p_end_date", "p_limit", "p_http_request_id"],
		["pPascal"] = ["pAuthor", "pAuthorId", "pAddressLine2", "pEndDate", "pLimit", "pHttpRequestId"],
		["in_snake"] = ["in_author", "in_author_id", "in_address_line_2", "in_end_date", "in_limit", "in_http_request_id"],
		["i_snake"] = ["i_author", "i_author_id", "i_address_line_2", "i_end_date", "i_limit", "i_http_request_id"],
		["prm_snake"] = ["prm_author", "prm_author_id", "prm_address_line_2", "prm_end_date", "prm_limit", "prm_http_request_id"],
		["par_snake"] = ["par_author", "par_author_id", "par_address_line_2", "par_end_date", "par_limit", "par_http_request_id"],
		["v_snake"] = ["v_author", "v_author_id", "v_address_line_2", "v_end_date", "v_limit", "v_http_request_id"],
	};
}
