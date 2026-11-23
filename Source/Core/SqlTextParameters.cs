namespace Open.Database.Extensions;

// Another provider's :name, $1 or ? is not recognised: no token means "not known".
// A local variable cannot be told from a parameter, so it is a token like any other.
/// <summary>
/// Finds the <c>@name</c> parameter markers in a SQL text, so a command of <see cref="CommandType.Text"/> can match an argument's derived name against the names its text uses.
/// </summary>
/// <remarks>
/// Only <c>@</c> markers are recognised.
/// A marker inside a single-quoted string, a double-quoted or <c>[bracketed]</c> identifier, a <c>--</c> line comment or a <c>/* */</c> block comment (not nested) is skipped, and so is a <c>@@system</c> variable.
/// A name is a letter or an underscore, then letters, digits, <c>_</c>, <c>$</c> and <c>#</c>; the same name in another case is the same parameter, so it is returned once, as it first appeared.
/// </remarks>
static class SqlTextParameters
{
	/// <summary>
	/// Gets the distinct tokens of the text in order of first appearance, each
	/// with its <c>@</c>.
	/// </summary>
	/// <param name="text">The SQL text.</param>
	/// <returns>The tokens; empty (never default) when there are none.</returns>
	internal static ImmutableArray<string> Parse(string text)
	{
		ImmutableArray<string>.Builder? tokens = null;
		HashSet<string>? seen = null;
		int position = 0;
		while (NextMarker(text, ref position, out int start, out int stop))
		{
			string token = text[start..stop];
			seen ??= new(StringComparer.OrdinalIgnoreCase);
			if (seen.Add(token)) (tokens ??= ImmutableArray.CreateBuilder<string>()).Add(token);
		}

		return tokens?.ToImmutable() ?? [];
	}

	/// <summary>
	/// Finds the next marker of the text at or after <paramref name="position"/>: <paramref name="start"/> is the index of its <c>@</c>,
	/// <paramref name="stop"/> the index after its last character.
	/// </summary>
	/// <param name="text">The SQL text.</param>
	/// <param name="position">Where to start looking; moves to <paramref name="stop"/> when a marker is found, otherwise to the end of the text.</param>
	/// <param name="start">The index of the marker's <c>@</c>.</param>
	/// <param name="stop">The index after the marker's last character.</param>
	/// <returns><see langword="true"/> if a marker was found; <see langword="false"/> when the text has no more markers.</returns>
	internal static bool NextMarker(ReadOnlySpan<char> text, ref int position, out int start, out int stop)
	{
		// It allocates nothing. Parse is built on this, so the markers a scan sees are exactly the tokens a parse makes
		// (a repeat, in any case, is another marker here and one token there).
		int i = position;
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
					int end = text[(i + 2)..].IndexOf("*/".AsSpan(), StringComparison.Ordinal);
					i = end < 0 ? text.Length : i + 2 + end + 2;
					break;
				case '@' when i + 1 < text.Length && text[i + 1] == '@':
					i = SkipWord(text, i + 2); // @@rowcount: a system variable
					break;
				case '@':
					int after = i + 1 < text.Length && (char.IsLetter(text[i + 1]) || text[i + 1] == '_') ? SkipWord(text, i + 2) : i + 1;
					if (after == i + 1)
					{
						i++; // an @ that starts no name
						break;
					}

					start = i;
					stop = position = after;
					return true;
				default:
					i++;
					break;
			}
		}

		position = text.Length;
		start = stop = 0;
		return false;
	}

	/// <summary>
	/// Gets the index after the closing character of a quoted part.
	/// </summary>
	/// <remarks>
	/// A doubled closing character is one character of the text.
	/// </remarks>
	/// <param name="text">The SQL text.</param>
	/// <param name="open">The index of the opening character.</param>
	/// <param name="close">The closing character.</param>
	/// <returns>The index after the closing character; the end of the text when it is not closed.</returns>
	static int SkipQuoted(ReadOnlySpan<char> text, int open, char close)
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

	/// <summary>
	/// Gets the index of the line break that ends a comment.
	/// </summary>
	/// <param name="text">The SQL text.</param>
	/// <param name="from">The index to start at.</param>
	/// <returns>The index of the line break, or the end of the text.</returns>
	static int SkipLine(ReadOnlySpan<char> text, int from)
	{
		for (int i = from; i < text.Length; i++)
		{
			if (text[i] is '\n' or '\r') return i;
		}

		return text.Length;
	}

	/// <summary>
	/// Gets the index after the name characters (letters, digits, <c>_</c>,
	/// <c>$</c> and <c>#</c>) that start at <paramref name="from"/>.
	/// </summary>
	/// <param name="text">The SQL text.</param>
	/// <param name="from">The index to start at.</param>
	/// <returns>The index after the name.</returns>
	static int SkipWord(ReadOnlySpan<char> text, int from)
	{
		int i = from;
		while (i < text.Length && (char.IsLetterOrDigit(text[i]) || text[i] is '_' or '$' or '#')) i++;
		return i;
	}
}
