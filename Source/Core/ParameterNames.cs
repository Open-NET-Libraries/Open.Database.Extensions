namespace Open.Database.Extensions;

/// <summary>
/// Derives a parameter name from the text of an argument expression (what <c>[CallerArgumentExpression]</c> captures)
/// and matches it to the real parameter names of a stored procedure.
/// </summary>
/// <remarks>
/// A derivable expression is a chain of plain member accesses: <c>startDate</c>, <c>filter.CompanySerialNum</c>, <c>order.Customer.Id</c>,
/// <c>this.Name</c>, <c>maybe?.X</c>, <c>maybe!.X</c>, <c>@class</c> (properties and fields look the same), with optional outer parentheses.
/// Calls, indexers, operators, casts, literals and <c>nameof</c> are not derivable: a wrong name that silently binds is worse than an error that says to pass the name.
/// A chain gives candidates, most specific first: <c>order.Customer.Id</c> tries <c>orderCustomerId</c>, <c>CustomerId</c>, then <c>Id</c>.
/// A last member that carries a field prefix (<c>m_author</c>, <c>s_author</c>, <c>_author</c>) also offers its name without the prefix, right after the name as written.
/// A candidate is matched to the real parameter names in four tiers, each candidate in its turn: exact, ignoring case, ignoring case and underscores, and,
/// failing those, ignoring case and underscores after removing one common parameter prefix from the real name (<c>p_author</c>, <c>pAuthor</c>, <c>in_author</c>, <c>i_author</c>, <c>prm_author</c>, <c>par_author</c>, <c>v_author</c>).
/// </remarks>
static class ParameterNames
{
	// Words that start an expression but are not a variable or member name.
	static readonly string[] NotNames =
		["null", "true", "false", "default", "new", "typeof", "sizeof", "checked", "unchecked", "stackalloc", "base"];

	readonly struct Segment(int start, int length)
	{
		public readonly int Start = start;
		public readonly int Length = length;
	}

	/// <summary>True when the expression is a chain of plain member accesses a name can be derived from. Allocates nothing.</summary>
	internal static bool IsDerivable(string? expression) => TryScan(expression, null, out _, out _);

	/// <summary>The last member of a derivable expression as written (<c>CompanySerialNum</c> for <c>filter.CompanySerialNum</c>).</summary>
	/// <exception cref="ArgumentException">The expression is not derivable.</exception>
	internal static string LastMember(string expression)
		=> TryScan(expression, null, out int start, out int length)
			? expression.Substring(start, length)
			: throw new ArgumentException($"Cannot derive a parameter name from the expression '{expression}'.", nameof(expression));

	/// <summary>
	/// The candidate names of a derivable expression, most specific (longest) first, then the final member name as written.
	/// When that member starts with a field prefix (<c>m_</c>, <c>s_</c> or <c>_</c>, in lower case, followed by a letter or a digit) its name without the prefix follows as the last candidate.
	/// </summary>
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

	// The length of a field prefix at the start of the member name s[start..start + length] (`_`, `m_` or `s_`, followed by a letter or a digit), or 0:
	// m_author and _author have one; M_author, m__author and mauthor do not.
	static int FieldPrefixLength(string s, int start, int length)
	{
		int prefix = s[start] == '_' ? 1 : length > 1 && (s[start] == 'm' || s[start] == 's') && s[start + 1] == '_' ? 2 : 0;
		return prefix > 0 && prefix < length && char.IsLetterOrDigit(s[start + prefix]) ? prefix : 0;
	}

	// Scans the expression as `a.b.c` with optional `?.`, `!` and outer parentheses. Fills `segments` (when given) with the member names.
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

	// True when the '(' at `open` is closed by the ')' at `close` (so the parentheses wrap everything in between).
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

	/// <summary>
	/// Finds the real parameter that a derived name stands for. Candidates are tried most specific first, so <c>order.Customer.Id</c> finds <c>@customer_id</c> before a bare <c>@id</c>;
	/// within a candidate the match is exact, else ignoring case, else ignoring case and underscores, the same tiers columns use.
	/// When a candidate matches in none of those three tiers, a fourth tier compares it with the real name after one common parameter prefix is removed
	/// (<c>p_</c>, <c>in_</c>, <c>i_</c>, <c>prm_</c>, <c>par_</c> or <c>v_</c> in any case, or a lone lower-case <c>p</c> before an upper-case letter), ignoring case and underscores:
	/// <c>author</c> finds <c>@p_author</c> and <c>@pAuthor</c>, but <c>ages</c> never finds <c>@pages</c>. The four tiers run inside each candidate's turn, so <c>order.Customer.Id</c> finds <c>@p_customer_id</c> before a bare <c>@id</c>.
	/// </summary>
	/// <param name="candidates">The candidates from <see cref="Candidates"/>.</param>
	/// <param name="parameterNames">The procedure's real parameter names, as the provider reports them (a leading <c>@</c> is allowed).</param>
	/// <returns>The real name as the provider reported it, or <see langword="null"/> when no candidate matches any parameter.</returns>
	/// <exception cref="InvalidOperationException">One candidate matches more than one parameter at the same tier.</exception>
	internal static string? Match(ImmutableArray<string> candidates, ImmutableArray<string> parameterNames)
	{
		foreach (string candidate in candidates)
		{
			if (Find(candidate, parameterNames) is { } found) return found;
		}

		return null;
	}

	static string? Find(string candidate, ImmutableArray<string> parameterNames)
	{
		// Tier 1: exact. Tier 2: ignoring case. Tier 3: ignoring case and underscores. Tier 4: the same without one common parameter prefix.
		for (int tier = 1; tier <= 4; tier++)
		{
			string? found = null;
			foreach (string real in parameterNames)
			{
				if (!Same(candidate, Bare(real), tier)) continue;
				if (found is not null)
					throw new InvalidOperationException($"The argument name '{candidate}' matches both '{found}' and '{real}'. Name the parameter explicitly: AddParam(\"name\", value).");
				found = real;
			}

			if (found is not null) return found;
		}

		return null;
	}

	static string Bare(string name) => name.Length > 0 && name[0] == '@' ? name[1..] : name;

	static bool Same(string a, string b, int tier) => tier switch
	{
		1 => string.Equals(a, b, StringComparison.Ordinal),
		2 => string.Equals(a, b, StringComparison.OrdinalIgnoreCase),
		3 => EqualIgnoringCaseAndUnderscores(a, b, 0),
		_ => EqualWithoutParameterPrefix(a, b),
	};

	// The prefixes real procedures put in front of a parameter name, compared ignoring case and followed by at least one more character.
	static readonly string[] ParameterPrefixes = ["p_", "in_", "i_", "prm_", "par_", "v_"];

	// The length of the parameter prefix at the start of a real name (a leading @ already removed), or 0. A lone lower-case p counts only before an upper-case letter: pAuthor, never pages.
	static int ParameterPrefixLength(string name)
	{
		foreach (string prefix in ParameterPrefixes)
		{
			if (name.Length > prefix.Length && string.Compare(name, 0, prefix, 0, prefix.Length, StringComparison.OrdinalIgnoreCase) == 0)
				return prefix.Length;
		}

		return name.Length > 1 && name[0] == 'p' && char.IsUpper(name[1]) ? 1 : 0;
	}

	// Tier 4: the candidate against the real name once one parameter prefix is removed, ignoring case and underscores.
	static bool EqualWithoutParameterPrefix(string candidate, string real)
	{
		int prefix = ParameterPrefixLength(real);
		return prefix > 0 && EqualIgnoringCaseAndUnderscores(candidate, real, prefix);
	}

	// Compares a with b from index bStart, ignoring case and underscores.
	static bool EqualIgnoringCaseAndUnderscores(string a, string b, int bStart)
	{
		int i = 0, j = bStart;
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
}
