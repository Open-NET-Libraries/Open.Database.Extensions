using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// One set of real parameter names (a procedure's, or the <c>@name</c> markers of a SQL text) and what is worked out from it:
/// the names without their <c>@</c>, once, and the real name each argument expression matched, so a repeat execution allocates nothing to match.
/// </summary>
sealed class NameSet
{
	// The real name each expression matched (null: it matches nothing), by the expression as the caller wrote it.
	// Bounded by the call sites and member names that run against this set; the set is replaced when its procedure's names expire (5 minutes) or are read again, so no limit is needed.
	readonly Dictionary<string, string?> _matches = new(StringComparer.Ordinal);

	/// <param name="names">The names as the provider reported them (a leading <c>@</c> is fine). Never the default array; may be empty (a text without markers).</param>
	internal NameSet(ImmutableArray<string> names)
	{
		Debug.Assert(!names.IsDefault, "A set is made from names, not from 'not known'.");
		Names = names;
		BareNames = ParameterNames.BareNames(names);
	}

	/// <summary>The names as the provider reported them.</summary>
	internal ImmutableArray<string> Names { get; }

	/// <summary>The same names without a leading <c>@</c>.</summary>
	internal ImmutableArray<string> BareNames { get; }

	/// <summary>
	/// The real name that the argument expression stands for, or <see langword="null"/> when it matches none. The answer is remembered, so the next call with the same expression does no matching.
	/// An ambiguity throws (see <see cref="ParameterNames.MatchBare"/>) and is not remembered, so it throws on every call.
	/// </summary>
	/// <exception cref="ArgumentException">The expression is not derivable.</exception>
	/// <exception cref="InvalidOperationException">The expression fits more than one name equally well.</exception>
	internal string? Match(string expression, string procedure)
	{
		lock (_matches)
		{
			if (_matches.TryGetValue(expression, out string? remembered)) return remembered;
		}

		// An argument of AddParam(value) was described when it was added; the member of an AddParams object is not derivable if it is a verbatim keyword, which Candidates reports.
		ImmutableArray<string> candidates = ParameterNames.Describe(expression)?.Candidates ?? ParameterNames.Candidates(expression);

		// Two threads may both match and store: the answer is the same.
		string? found = ParameterNames.MatchBare(candidates, Names, BareNames, expression, procedure);
		lock (_matches)
		{
			_matches[expression] = found;
		}

		return found;
	}
}
