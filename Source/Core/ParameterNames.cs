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

	/// <summary>The candidate names of a derivable expression, most specific (longest) first; the last is the final member name as written.</summary>
	/// <exception cref="ArgumentException">The expression is not derivable (callers check with <see cref="IsDerivable"/> first).</exception>
	internal static ImmutableArray<string> Candidates(string expression)
	{
		var segments = new List<Segment>(4);
		if (!TryScan(expression, segments, out _, out _))
			throw new ArgumentException($"Cannot derive a parameter name from the expression '{expression}'.", nameof(expression));

		string[] names = new string[segments.Count];
		for (int i = 0; i < names.Length; i++)
			names[i] = expression.Substring(segments[i].Start, segments[i].Length);

		var candidates = ImmutableArray.CreateBuilder<string>(names.Length);
		for (int from = 0; from < names.Length; from++)
			candidates.Add(from == names.Length - 1 ? names[from] : string.Join(string.Empty, names, from, names.Length - from));

		return candidates.MoveToImmutable();
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
		// Tier 1: exact. Tier 2: ignoring case. Tier 3: ignoring case and underscores.
		for (int tier = 1; tier <= 3; tier++)
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

	static string Bare(string name) => name.Length > 0 && name[0] == '@' ? name.Substring(1) : name;

	static bool Same(string a, string b, int tier) => tier switch
	{
		1 => string.Equals(a, b, StringComparison.Ordinal),
		2 => string.Equals(a, b, StringComparison.OrdinalIgnoreCase),
		_ => EqualIgnoringCaseAndUnderscores(a, b),
	};

	static bool EqualIgnoringCaseAndUnderscores(string a, string b)
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
}
