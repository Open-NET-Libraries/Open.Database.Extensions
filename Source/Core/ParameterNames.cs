using System.Runtime.InteropServices;

namespace Open.Database.Extensions;

/// <summary>
/// Derives a parameter name from the text of an argument expression (what <c>[CallerArgumentExpression]</c> captures)
/// and matches it to the real parameter names of a stored procedure.
/// </summary>
/// <remarks>
/// A derivable expression is a chain of plain member accesses: <c>startDate</c>, <c>filter.AuthorId</c>, <c>post.Author.Id</c>,
/// <c>this.Name</c>, <c>maybe?.X</c>, <c>maybe!.X</c>, <c>@class</c> (properties and fields look the same), with optional outer parentheses.
/// Calls, indexers, operators, casts, literals and <c>nameof</c> are not derivable.
/// A chain gives candidates, most specific first: <c>post.Author.Id</c> tries <c>postAuthorId</c>, <c>AuthorId</c>, then <c>Id</c>.
/// A last member that carries a field prefix (<c>m_author</c>, <c>s_author</c>, <c>_author</c>) also offers its name without the prefix, right after the name as written.
/// </remarks>
// A wrong name that silently binds is worse than an error that says to pass the name, so only plain member chains are derived.
// A candidate is matched to the real parameter names in four tiers, each candidate in its turn (see MatchBare).
static class ParameterNames
{
	/// <summary>
	/// Words that start an expression but are not a variable or member name.
	/// </summary>
	static readonly string[] NotNames =
		["null", "true", "false", "default", "new", "typeof", "sizeof", "checked", "unchecked", "stackalloc", "base"];

	readonly struct Segment(int start, int length)
	{
		public readonly int Start = start;
		public readonly int Length = length;
	}

	/// <summary>The facts of a derivable expression, worked out once.</summary>
	internal sealed class DerivedExpression(string expression, string lastMember, ImmutableArray<string> candidates)
	{
		/// <summary>The text of the argument the facts were worked out from.</summary>
		public string Expression { get; } = expression;

		internal string LastMember { get; } = lastMember;
		internal ImmutableArray<string> Candidates { get; } = candidates;

		/// <summary>The name an expression resolved to in a set of names.</summary>
		/// <param name="names">The set of names.</param>
		/// <param name="real">The real name.</param>
		/// <param name="entry">The real name's place in <paramref name="names"/>.</param>
		internal sealed class Resolution(NameSet names, string real, NameSet.Entry? entry)
		{
			public NameSet Names { get; } = names;

			public string Real { get; } = real;

			public NameSet.Entry? Entry { get; } = entry;
		}

		/// <summary>Gets or sets the last resolution of this expression.</summary>
		// A repeat in the same set of names reads one field and takes no lock.
		// Every call site that writes the same text shares the expression, and its calls usually run against one set of names.
		// A thread that loses a race only makes one more resolution.
		public Resolution? Last
		{
			get => Volatile.Read(ref field);
			set => Volatile.Write(ref field, value);
		}

		/// <summary>
		/// The last real name this expression resolved to in a SQL text, so a
		/// repeat scan hands back the same string.
		/// </summary>
		// The names are almost always the same.
		// It belongs to the expression, which the call site holds, not to any text.
		// One slot; a thread that loses a race only makes one more string.
		string? _real;

		/// <summary>Gets the string for a marker found by a scan: the one remembered when it is the same characters, else a new one, which is remembered.</summary>
		/// <param name="marker">The marker, with its <c>@</c>.</param>
		/// <returns>The string for the marker.</returns>
		internal string Real(ReadOnlySpan<char> marker)
		{
			string? known = _real;
			if (known is not null && marker.SequenceEqual(known.AsSpan())) return known;

			return _real = marker.ToString();
		}
	}

	/// <summary>
	/// The facts of the derivable expressions, by the expression string itself.
	/// </summary>
	/// <remarks>
	/// An entry goes when its string is released.
	/// A text that is not derivable is not stored.
	/// </remarks>
	// The text of an argument comes from [CallerArgumentExpression], a literal at the call site, so it is the same object on every call.
	static readonly ConditionalWeakTable<string, DerivedExpression> Described = new();

	/// <summary>Gets the last member and the candidates of the expression, remembered on the string.</summary>
	/// <param name="expression">The text of the argument.</param>
	/// <returns>The facts of the expression; <see langword="null"/> when it is <see langword="null"/> or not derivable.</returns>
	internal static DerivedExpression? Describe(string? expression)
	{
		if (expression is null) return null;
		if (Described.TryGetValue(expression, out DerivedExpression? known)) return known;
		return IsDerivable(expression) ? Described.GetValue(expression, static e => new DerivedExpression(e, LastMember(e), Candidates(e))) : null;
	}

	/// <summary>Determines whether the expression is a chain of plain member accesses a name can be derived from.</summary>
	/// <param name="expression">The text of the argument.</param>
	/// <returns><see langword="true"/> if a name can be derived from the expression; otherwise, <see langword="false"/>.</returns>
	// Allocates nothing.
	internal static bool IsDerivable(string? expression) => TryScan(expression, null, out _, out _);

	/// <summary>Gets the last member of a derivable expression as written (<c>AuthorId</c> for <c>filter.AuthorId</c>).</summary>
	/// <param name="expression">The text of the argument.</param>
	/// <returns>The last member.</returns>
	/// <exception cref="ArgumentException">The expression is not derivable.</exception>
	internal static string LastMember(string expression)
		=> TryScan(expression, null, out int start, out int length)
			? expression.Substring(start, length)
			: throw new ArgumentException($"Cannot derive a parameter name from the expression '{expression}'.", nameof(expression));

	/// <summary>
	/// Gets the candidate names of a derivable expression, most specific (longest) first, then the final member name as written.
	/// </summary>
	/// <remarks>
	/// When that member starts with a field prefix (<c>m_</c>, <c>s_</c> or <c>_</c>, in lower case, followed by a letter or a digit) its name without the prefix follows as the last candidate.
	/// </remarks>
	/// <param name="expression">The text of the argument.</param>
	/// <returns>The candidates, most specific first.</returns>
	/// <exception cref="ArgumentException">The expression is not derivable (callers check with <see cref="IsDerivable"/> first).</exception>
	internal static ImmutableArray<string> Candidates(string expression)
	{
		var segments = new List<Segment>(4);
		if (!TryScan(expression, segments, out int lastStart, out int lastLength))
			throw new ArgumentException($"Cannot derive a parameter name from the expression '{expression}'.", nameof(expression));

		string[] names = new string[segments.Count];
		for (int i = 0; i < names.Length; i++)
			names[i] = expression.Substring(segments[i].Start, segments[i].Length);

		int fieldPrefix = FieldPrefixLength(expression, lastStart, lastLength);

		var candidates = ImmutableArray.CreateBuilder<string>(names.Length + (fieldPrefix > 0 ? 1 : 0));
		for (int from = 0; from < names.Length; from++)
			candidates.Add(from == names.Length - 1 ? names[from] : string.Join(string.Empty, names, from, names.Length - from));

		if (fieldPrefix > 0) candidates.Add(expression.Substring(lastStart + fieldPrefix, lastLength - fieldPrefix));

		return candidates.MoveToImmutable();
	}

	/// <summary>
	/// Gets the length of a field prefix at the start of the member name
	/// <c>s[start..start + length]</c>.
	/// </summary>
	/// <remarks>
	/// A field prefix is <c>_</c>, <c>m_</c> or <c>s_</c>, followed by a letter
	/// or a digit: <c>m_author</c> and <c>_author</c> have one;
	/// <c>M_author</c>, <c>m__author</c> and <c>mauthor</c> do not.
	/// </remarks>
	/// <param name="s">The expression.</param>
	/// <param name="start">The index where the member name starts.</param>
	/// <param name="length">The length of the member name.</param>
	/// <returns>The length of the prefix, or 0 when there is none.</returns>
	static int FieldPrefixLength(string s, int start, int length)
	{
		int prefix = s[start] == '_' ? 1 : length > 1 && (s[start] == 'm' || s[start] == 's') && s[start + 1] == '_' ? 2 : 0;
		return prefix > 0 && prefix < length && char.IsLetterOrDigit(s[start + prefix]) ? prefix : 0;
	}

	/// <summary>
	/// Scans the expression as <c>a.b.c</c> with optional <c>?.</c>, <c>!</c>
	/// and outer parentheses.
	/// </summary>
	/// <param name="expression">The text of the argument.</param>
	/// <param name="segments">The list to fill with the member names, or <see langword="null"/> for none.</param>
	/// <param name="lastStart">The index where the last member starts.</param>
	/// <param name="lastLength">The length of the last member.</param>
	/// <returns><see langword="true"/> if the expression is a chain of plain member accesses; otherwise, <see langword="false"/>.</returns>
	static bool TryScan(string? expression, List<Segment>? segments, out int lastStart, out int lastLength)
	{
		lastStart = lastLength = 0;
		if (expression is null) return false;

		int start = 0, end = expression.Length;
		Trim(expression, ref start, ref end);

		// Outer parentheses that wrap the whole expression: (filter.X) and ((filter.X)).
		while (end - start >= 2 && expression[start] == '(' && ClosesAt(expression, start, end - 1))
		{
			start++;
			end--;
			Trim(expression, ref start, ref end);
		}

		int i = start;
		int members = 0;
		while (true)
		{
			SkipWhite(expression, ref i, end);

			bool verbatim = i < end && expression[i] == '@';
			if (verbatim) i++;

			int nameStart = i;
			if (i >= end || !(char.IsLetter(expression[i]) || expression[i] == '_')) return false;
			i++;
			while (i < end && (char.IsLetterOrDigit(expression[i]) || expression[i] == '_')) i++;

			int length = i - nameStart;
			if (!verbatim && IsNotAName(expression, nameStart, length)) return false;

			if (!verbatim && length == 4 && string.CompareOrdinal(expression, nameStart, "this", 0, 4) == 0)
			{
				// A leading `this.` names the member, not the instance; `this` anywhere else is not a member name.
				if (members > 0 || nameStart != start) return false;
			}
			else
			{
				segments?.Add(new Segment(nameStart, length));
				lastStart = nameStart;
				lastLength = length;
				members++;
			}

			SkipWhite(expression, ref i, end);
			while (i < end && expression[i] == '!')
			{
				i++;
				SkipWhite(expression, ref i, end);
			}

			if (i >= end) break;

			if (expression[i] == '?')
			{
				i++;
				SkipWhite(expression, ref i, end);
			}

			if (i >= end || expression[i] != '.') return false;
			i++;
		}

		// A lone `this` has no member to name.
		return members > 0;
	}

	static bool IsNotAName(string expression, int start, int length)
	{
		foreach (string word in NotNames)
		{
			if (word.Length == length && string.CompareOrdinal(expression, start, word, 0, length) == 0)
				return true;
		}

		return false;
	}

	static void Trim(string s, ref int start, ref int end)
	{
		while (start < end && char.IsWhiteSpace(s[start])) start++;
		while (end > start && char.IsWhiteSpace(s[end - 1])) end--;
	}

	static void SkipWhite(string s, ref int i, int end)
	{
		while (i < end && char.IsWhiteSpace(s[i])) i++;
	}

	/// <summary>
	/// Determines whether the <c>(</c> at <paramref name="open"/> is closed by
	/// the <c>)</c> at <paramref name="close"/>, so the parentheses wrap
	/// everything in between.
	/// </summary>
	/// <param name="s">The expression.</param>
	/// <param name="open">The index of the opening parenthesis.</param>
	/// <param name="close">The index of the closing parenthesis.</param>
	/// <returns><see langword="true"/> if they are a pair; otherwise, <see langword="false"/>.</returns>
	static bool ClosesAt(string s, int open, int close)
	{
		if (s[close] != ')') return false;
		int depth = 0;
		for (int i = open; i <= close; i++)
		{
			if (s[i] == '(') depth++;
			else if (s[i] == ')' && --depth == 0) return i == close;
		}

		return false;
	}

	/// <summary>Finds the real parameter that a derived name stands for, trying the candidates most specific first.</summary>
	/// <remarks>
	/// For example <c>post.Author.Id</c> finds <c>@author_id</c> before a bare <c>@id</c>.
	/// Within a candidate the match is exact, else ignoring case, else ignoring case and underscores, else ignoring case and underscores after one common parameter prefix is removed from the real name
	/// (<c>p_</c>, <c>in_</c>, <c>i_</c>, <c>prm_</c>, <c>par_</c> or <c>v_</c> in any case, or a lone lower-case <c>p</c> before an upper-case letter):
	/// <c>author</c> finds <c>@p_author</c> and <c>@pAuthor</c>, but <c>ages</c> never finds <c>@pages</c>.
	/// </remarks>
	/// <param name="candidates">The candidates from <see cref="Candidates"/>.</param>
	/// <param name="parameterNames">The real names as the provider reports them (a leading <c>@</c> is allowed).</param>
	/// <param name="bareNames">The same names without a leading <c>@</c>.</param>
	/// <param name="expression">The text of the argument, for the message of an ambiguity.</param>
	/// <param name="procedure">The procedure's name, for the message of an ambiguity.</param>
	/// <returns>The real name as the provider reported it, or <see langword="null"/> when no candidate matches any parameter.</returns>
	/// <exception cref="InvalidOperationException">One candidate matches more than one parameter at the same tier.</exception>
	internal static string? MatchBare(ImmutableArray<string> candidates, ImmutableArray<string> parameterNames, ImmutableArray<string> bareNames, string expression, string procedure)
	{
		foreach (string candidate in candidates)
		{
			if (Find(expression, procedure, candidate, parameterNames, bareNames) is { } found) return found;
		}

		return null;
	}

	/// <summary>
	/// Describes how a tier compares a candidate with a real name, for the
	/// message of an ambiguity.
	/// </summary>
	/// <param name="tier">The tier, 1 to 4.</param>
	/// <returns>The description.</returns>
	static string TierText(int tier) => tier switch
	{
		1 => "exactly",
		2 => "ignoring case",
		3 => "ignoring case and underscores",
		_ => "ignoring case and underscores once one parameter prefix (p_, in_, i_, prm_, par_, v_ or a lone p) is removed from the real name",
	};

	static string? Find(string expression, string procedure, string candidate, ImmutableArray<string> parameterNames, ImmutableArray<string> bareNames)
	{
		// Tier 1: exact. Tier 2: ignoring case. Tier 3: ignoring case and underscores. Tier 4: the same without one common parameter prefix.
		for (int tier = 1; tier <= 4; tier++)
		{
			string? found = null;
			for (int i = 0; i < parameterNames.Length; i++)
			{
				if (!Same(candidate, bareNames[i].AsSpan(), tier)) continue;
				string real = parameterNames[i];
				if (found is not null)
				{
					throw new InvalidOperationException(
						$"The argument '{expression}' matches both '{found}' and '{real}' of '{procedure}'. "
						+ $"Its candidate name '{candidate}' fits each of them {TierText(tier)}. "
						+ $"The procedure declares: {string.Join(", ", parameterNames)}. Name the parameter explicitly: AddParam(\"name\", value).");
				}

				found = real;
			}

			if (found is not null) return found;
		}

		return null;
	}

	/// <summary>
	/// Gets the real names without a leading <c>@</c>.
	/// </summary>
	/// <param name="names">The real names.</param>
	/// <returns>The names without a leading <c>@</c>.</returns>
	internal static ImmutableArray<string> BareNames(ImmutableArray<string> names)
	{
		// A set with none is returned as it is; otherwise one copy holds the bare names, and a name without an @ is the same string in it.
		string[]? bare = null;
		for (int i = 0; i < names.Length; i++)
		{
			string name = names[i];
			if (name.Length == 0 || name[0] != '@') continue;
			bare ??= [.. names];
			bare[i] = name[1..];
		}

		return bare is null ? names : ImmutableCollectionsMarshal.AsImmutableArray(bare);
	}

	static bool Same(ReadOnlySpan<char> a, ReadOnlySpan<char> b, int tier) => tier switch
	{
		1 => a.SequenceEqual(b),
		2 => a.Equals(b, StringComparison.OrdinalIgnoreCase),
		3 => EqualIgnoringCaseAndUnderscores(a, b),
		_ => EqualWithoutParameterPrefix(a, b),
	};

	/// <summary>
	/// The prefixes real procedures put in front of a parameter name,
	/// compared ignoring case and followed by at least one more character.
	/// </summary>
	static readonly string[] ParameterPrefixes = ["p_", "in_", "i_", "prm_", "par_", "v_"];

	/// <summary>
	/// Gets the length of the parameter prefix at the start of a real name.
	/// </summary>
	/// <remarks>
	/// A lone lower-case <c>p</c> counts only before an upper-case letter:
	/// <c>pAuthor</c>, never <c>pages</c>.
	/// </remarks>
	/// <param name="name">The real name, with a leading <c>@</c> already removed.</param>
	/// <returns>The length of the prefix, or 0 when there is none.</returns>
	static int ParameterPrefixLength(ReadOnlySpan<char> name)
	{
		foreach (string prefix in ParameterPrefixes)
		{
			if (name.Length > prefix.Length && name[..prefix.Length].Equals(prefix.AsSpan(), StringComparison.OrdinalIgnoreCase))
				return prefix.Length;
		}

		return name.Length > 1 && name[0] == 'p' && char.IsUpper(name[1]) ? 1 : 0;
	}

	/// <summary>
	/// Tier 4: compares the candidate with the real name once one parameter
	/// prefix is removed, ignoring case and underscores.
	/// </summary>
	/// <param name="candidate">The candidate.</param>
	/// <param name="real">The real name, without its <c>@</c>.</param>
	/// <returns><see langword="true"/> if they match; otherwise, <see langword="false"/>.</returns>
	static bool EqualWithoutParameterPrefix(ReadOnlySpan<char> candidate, ReadOnlySpan<char> real)
	{
		int prefix = ParameterPrefixLength(real);
		return prefix > 0 && EqualIgnoringCaseAndUnderscores(candidate, real[prefix..]);
	}

	/// <summary>
	/// Compares <paramref name="a"/> with <paramref name="b"/>, ignoring case
	/// and underscores.
	/// </summary>
	/// <param name="a">The first name.</param>
	/// <param name="b">The second name.</param>
	/// <returns><see langword="true"/> if they match; otherwise, <see langword="false"/>.</returns>
	static bool EqualIgnoringCaseAndUnderscores(ReadOnlySpan<char> a, ReadOnlySpan<char> b)
	{
		int i = 0, j = 0;
		while (true)
		{
			while (i < a.Length && a[i] == '_') i++;
			while (j < b.Length && b[j] == '_') j++;
			if (i == a.Length || j == b.Length) return i == a.Length && j == b.Length;
			if (char.ToUpperInvariant(a[i]) != char.ToUpperInvariant(b[j])) return false;
			i++;
			j++;
		}
	}

	/// <summary>How a scan of a SQL text for a name ended.</summary>
	internal enum ScanResult
	{
		/// <summary>No candidate fits any marker of the text.</summary>
		None,

		/// <summary>One marker fits.</summary>
		Found,

		/// <summary>A candidate fits more than one marker at the same tier (the caller makes the message).</summary>
		Ambiguous,
	}

	/// <summary>
	/// Finds the marker of a SQL text that the candidates stand for, scanning the text itself.
	/// </summary>
	/// <param name="text">The SQL text.</param>
	/// <param name="candidates">The candidates from <see cref="Candidates"/>.</param>
	/// <param name="real">The marker as the text spells it, with its <c>@</c>, when <see cref="ScanResult.Found"/>.</param>
	/// <returns>How the scan ended.</returns>
	// The same answer as MatchBare over the tokens of SqlTextParameters.Parse (the same candidates in the same order,
	// the same four tiers, a marker repeated in another case being one name, as first written), and allocates nothing.
	internal static ScanResult Scan(string text, ImmutableArray<string> candidates, out ReadOnlySpan<char> real)
	{
		foreach (string candidate in candidates)
		{
			for (int tier = 1; tier <= 4; tier++)
			{
				ReadOnlySpan<char> found = default;
				int position = 0;
				while (SqlTextParameters.NextMarker(text, ref position, out int start, out int stop))
				{
					if (!Same(candidate, text.AsSpan(start + 1, stop - start - 1), tier)) continue;

					// A marker written again in another case is the same name: only its first writing counts.
					if (!IsFirst(text, start, stop)) continue;
					if (!found.IsEmpty)
					{
						real = default;
						return ScanResult.Ambiguous;
					}

					found = text.AsSpan(start, stop - start);
				}

				if (!found.IsEmpty)
				{
					real = found;
					return ScanResult.Found;
				}
			}
		}

		real = default;
		return ScanResult.None;
	}

	/// <summary>
	/// Determines whether no earlier marker of the text is the same name
	/// (ignoring case) as the one at <paramref name="start"/> to
	/// <paramref name="stop"/>.
	/// </summary>
	/// <param name="text">The SQL text.</param>
	/// <param name="start">The index of the marker's <c>@</c>.</param>
	/// <param name="stop">The index after the marker's last character.</param>
	/// <returns><see langword="true"/> if it is the first writing of the name; otherwise, <see langword="false"/>.</returns>
	static bool IsFirst(string text, int start, int stop)
	{
		ReadOnlySpan<char> marker = text.AsSpan(start, stop - start);
		int position = 0;
		while (SqlTextParameters.NextMarker(text, ref position, out int earlier, out int earlierStop) && earlier < start)
		{
			if (text.AsSpan(earlier, earlierStop - earlier).Equals(marker, StringComparison.OrdinalIgnoreCase)) return false;
		}

		return true;
	}
}
