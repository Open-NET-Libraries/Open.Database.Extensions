using System.Collections.Concurrent;

namespace Open.Database.Extensions;

/// <summary>
/// Finds the <c>@name</c> parameter markers in a SQL text, so a command of <see cref="CommandType.Text"/> can match an argument's derived name against the names its text uses.
/// </summary>
/// <remarks>
/// Only <c>@</c> is recognised (another provider's <c>:name</c>, <c>$1</c> or <c>?</c> is not: no token means "not known"). A marker inside a single-quoted string, a double-quoted or
/// <c>[bracketed]</c> identifier, a <c>--</c> line comment or a <c>/* */</c> block comment (not nested) is skipped, and so is a <c>@@system</c> variable. A local variable
/// cannot be told from a parameter, so it is a token like any other. A name is a letter or an underscore, then letters, digits, <c>_</c>, <c>$</c> and <c>#</c>; the same
/// name in another case is the same parameter, so it is returned once, as it first appeared. The result for a text is remembered (at most 256 texts, then the cache starts again).
/// </remarks>
static class SqlTextParameters
{
	const int MaxTexts = 256;

	static readonly ConcurrentDictionary<string, ImmutableArray<string>> Cache = new(StringComparer.Ordinal);

	// The distinct tokens of the text in order of first appearance, each with its @; empty (never default) when there are none.
	internal static ImmutableArray<string> Names(string text)
	{
		if (Cache.TryGetValue(text, out ImmutableArray<string> names)) return names;

		names = Parse(text);
		if (Cache.Count >= MaxTexts) Cache.Clear();
		Cache[text] = names;
		return names;
	}

	internal static ImmutableArray<string> Parse(string text)
	{
		ImmutableArray<string>.Builder? tokens = null;
		HashSet<string>? seen = null;
		int i = 0;
		while (i < text.Length)
		{
			switch (text[i])
			{
				case '\'':
					i = SkipQuoted(text, i, '\'');
					break;
				case '"':
					i = SkipQuoted(text, i, '"');
					break;
				case '[':
					i = SkipQuoted(text, i, ']');
					break;
				case '-' when i + 1 < text.Length && text[i + 1] == '-':
					i = SkipLine(text, i + 2);
					break;
				case '/' when i + 1 < text.Length && text[i + 1] == '*':
					int end = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
					i = end < 0 ? text.Length : end + 2;
					break;
				case '@' when i + 1 < text.Length && text[i + 1] == '@':
					i = SkipWord(text, i + 2); // @@rowcount: a system variable
					break;
				case '@':
					int stop = i + 1 < text.Length && (char.IsLetter(text[i + 1]) || text[i + 1] == '_') ? SkipWord(text, i + 2) : i + 1;
					if (stop == i + 1)
					{
						i++; // an @ that starts no name
						break;
					}

					string token = text[i..stop];
					seen ??= new(StringComparer.OrdinalIgnoreCase);
					if (seen.Add(token)) (tokens ??= ImmutableArray.CreateBuilder<string>()).Add(token);
					i = stop;
					break;
				default:
					i++;
					break;
			}
		}

		return tokens?.ToImmutable() ?? [];
	}

	// The index after the closing character of a quoted part that starts at `open` (a doubled closing character is one character of the text); the end of the text when it is not closed.
	static int SkipQuoted(string text, int open, char close)
	{
		for (int i = open + 1; i < text.Length; i++)
		{
			if (text[i] != close) continue;
			if (i + 1 < text.Length && text[i + 1] == close)
			{
				i++;
				continue;
			}

			return i + 1;
		}

		return text.Length;
	}

	// The index of the line break that ends a comment, or the end of the text.
	static int SkipLine(string text, int from)
	{
		for (int i = from; i < text.Length; i++)
		{
			if (text[i] is '\n' or '\r') return i;
		}

		return text.Length;
	}

	// The index after the name characters (letters, digits, _, $ and #) that start at `from`.
	static int SkipWord(string text, int from)
	{
		int i = from;
		while (i < text.Length && (char.IsLetterOrDigit(text[i]) || text[i] is '_' or '$' or '#')) i++;
		return i;
	}
}
