using System.Reflection;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The @name tokens of a SQL text: what a command of CommandType.Text "declares" for AddParam(value) to match a derived name against.
// The tokenizer is internal, so the table below reaches it through reflection (as the other tests of Core internals do).
[ExcludeFromCodeCoverage]
public static class SqlTextParametersTests
{
	static readonly Type Tokenizer = typeof(DataRecordExtensions).Assembly.GetType("Open.Database.Extensions.SqlTextParameters", throwOnError: true)!;

	static string[] Call(string method, string text)
	{
		object? result = Tokenizer.GetMethod(method, BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [text]);
		return [.. (ImmutableArray<string>)result!];
	}

	static string[] Parse(string text) => Call("Parse", text);

	// ---- the table: text, the tokens in order of first appearance ----

	[Theory]
	// plain tokens, in order, each once
	[InlineData("select * from t where a = @a", new[] { "@a" })]
	[InlineData("where a = @a and b = @b", new[] { "@a", "@b" })]
	[InlineData("@a @b @a @b @c", new[] { "@a", "@b", "@c" })]
	[InlineData("(@a,@b)", new[] { "@a", "@b" })]
	[InlineData("x=@a+@b-@c*@d/@e", new[] { "@a", "@b", "@c", "@d", "@e" })]
	[InlineData("@region_id", new[] { "@region_id" })]
	[InlineData("@a\t@b\r\n@c", new[] { "@a", "@b", "@c" })]
	// the same name in another case is the same parameter (SQL Server does not tell them apart): the first spelling is kept
	[InlineData("@Region @region @REGION", new[] { "@Region" })]
	// what a name is made of: a letter or an underscore first, then letters, digits, underscore, $ and #
	[InlineData("@p1 @_x @a$b @a#c @p_9 @x1y2", new[] { "@p1", "@_x", "@a$b", "@a#c", "@p_9", "@x1y2" })]
	[InlineData("@été @名前", new[] { "@été", "@名前" })]
	[InlineData("@a.b @c:d @e;f @g(h", new[] { "@a", "@c", "@e", "@g" })]
	[InlineData("@a@b", new[] { "@a", "@b" })] // a name ends at the first character that cannot be part of one, and an @ cannot
	// an @ that starts no name
	[InlineData("select @", new string[0])]
	[InlineData("select @a, @", new[] { "@a" })]
	[InlineData("a = @ b", new string[0])]
	[InlineData("@1 @#a @$a @-a @ @", new string[0])]
	[InlineData("data @> '{}'", new string[0])]
	// system variables are not parameters
	[InlineData("select @@rowcount", new string[0])]
	[InlineData("select @@identity, @@version, @a", new[] { "@a" })]
	[InlineData("@@a @b", new[] { "@b" })]
	[InlineData("@@", new string[0])]
	// a local variable cannot be told from a parameter: it is a token like any other
	[InlineData("declare @x int = 1; select @x", new[] { "@x" })]
	[InlineData("declare @x int; set @x = @p; select @x", new[] { "@x", "@p" })]
	// single-quoted strings, '' is an escaped quote
	[InlineData("select '@a', @b", new[] { "@b" })]
	[InlineData("select 'it''s @a', @b", new[] { "@b" })]
	[InlineData("select '''', @b", new[] { "@b" })]
	[InlineData("select '', @b", new[] { "@b" })]
	[InlineData("select 'a@b', @c", new[] { "@c" })] // an address-looking text inside a string
	[InlineData("where e = N'o''brien@a' and f = @f", new[] { "@f" })]
	[InlineData("select 'unterminated @a", new string[0])]
	[InlineData("select 'a' + @b + 'c' + @d", new[] { "@b", "@d" })]
	// double-quoted and bracketed identifiers, "" and ]] are escapes
	[InlineData("select \"@a\", @b", new[] { "@b" })]
	[InlineData("select \"a\"\"@c\", @d", new[] { "@d" })]
	[InlineData("select [@a], @b", new[] { "@b" })]
	[InlineData("select [a]]@c], @d", new[] { "@d" })]
	[InlineData("select [unterminated @a", new string[0])]
	[InlineData("select \"unterminated @a", new string[0])]
	// a quote character inside the other kinds of quoting or inside a comment does not open anything
	[InlineData("select [it's], @a", new[] { "@a" })]
	[InlineData("select \"it's\", @a", new[] { "@a" })]
	[InlineData("select 'a\"b', @a", new[] { "@a" })]
	[InlineData("select 'a[b', @a", new[] { "@a" })]
	[InlineData("-- it's\nselect @a", new[] { "@a" })]
	// line comments end at the line break
	[InlineData("-- @a\n@b", new[] { "@b" })]
	[InlineData("-- @a\r\n@b", new[] { "@b" })]
	[InlineData("-- @a\r@b", new[] { "@b" })]
	[InlineData("-- @a", new string[0])]
	[InlineData("@a--@b", new[] { "@a" })]
	[InlineData("@a -- @b\n@c -- @d", new[] { "@a", "@c" })]
	[InlineData("select 'a--b', @c", new[] { "@c" })] // two dashes inside a string are not a comment
	[InlineData("5-@a", new[] { "@a" })] // a single dash is a minus
	// block comments, not nested
	[InlineData("/* @a */ @b", new[] { "@b" })]
	[InlineData("@a/*x*/@b", new[] { "@a", "@b" })]
	[InlineData("/* @a\n@b\n*/ @c", new[] { "@c" })]
	[InlineData("/* @a /* @b */ @c */", new[] { "@c" })]
	[InlineData("/* unterminated @a", new string[0])]
	[InlineData("select 'a/*b', @c", new[] { "@c" })] // a slash-star inside a string opens nothing
	[InlineData("x/@a", new[] { "@a" })] // a single slash is a divide
	[InlineData("/**/@a", new[] { "@a" })]
	// all of it together
	[InlineData("update t set a = @a, b = 'x@y' /* @c */ where id = @id -- @d\n and [@e] = \"@f\" and g = @a", new[] { "@a", "@id" })]
	public static void TheTokens(string text, string[] expected) => Assert.Equal(expected, Parse(text));

	// ---- no token: "not known", the arguments keep the name they were given ----

	[Theory]
	[InlineData("select 1")]
	[InlineData("select * from orders")]
	[InlineData("select :a, $1, ?, %(a)s")] // the other providers' markers are not recognised
	[InlineData("select @@rowcount")]
	[InlineData("   ")]
	public static void NoTokens_IsAnEmptyArray_NotADefaultOne(string text)
	{
		object? result = Tokenizer.GetMethod("Parse", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [text]);

		var tokens = (ImmutableArray<string>)result!;
		Assert.False(tokens.IsDefault);
		Assert.True(tokens.IsEmpty);
	}

	// ---- the cache ----

	[Fact]
	public static void TheCachedLookup_GivesTheSameTokensAsParsing_EachTime()
	{
		const string text = "select 1 where a = @region_id and b = @p_name /* @x */";

		Assert.Equal(Parse(text), Call("Names", text));
		Assert.Equal(["@region_id", "@p_name"], Call("Names", text));
		Assert.Equal(["@region_id", "@p_name"], Call("Names", text)); // the second and third come from the cache
	}

	[Fact]
	public static void TheCache_IsBounded()
	{
		FieldInfo field = Tokenizer.GetField("Cache", BindingFlags.Static | BindingFlags.NonPublic)!;
		var cache = (System.Collections.IDictionary)field.GetValue(null)!;

		for (int i = 0; i < 700; i++)
			Assert.Equal(["@v" + i], Call("Names", "select @v" + i));

		Assert.InRange(cache.Count, 1, 400); // cleared when it reaches 256 entries (a few racing threads may add one or two meanwhile)
	}
}
