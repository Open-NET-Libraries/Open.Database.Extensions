using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// One set of real parameter names (a procedure's, or the <c>@name</c> markers of a SQL text) and what is worked out from it:
/// the names without their <c>@</c>, once, and the real name each argument expression matched, so a repeat execution allocates nothing to match.
/// </summary>
sealed class NameSet
{
	/// <summary>
	/// The real name each expression matched (<see langword="null"/>: it
	/// matches nothing), by the expression as the caller wrote it.
	/// </summary>
	/// <remarks>
	/// Bounded by the call sites and member names that run against this set;
	/// the set is replaced when its procedure's names expire (5 minutes;
	/// 15 seconds for a "not known" answer) or are read again, so no limit is
	/// needed.
	/// The shared "not known" set (no names) is never asked to match: callers
	/// check that a set has names first.
	/// </remarks>
	readonly Dictionary<string, string?> _matches = new(StringComparer.Ordinal);

	/// <summary>Initializes a new instance from the names as the provider reported them (a leading <c>@</c> is fine); never the default array, but may be empty (a text without markers).</summary>
	/// <param name="names">The names.</param>
	/// <param name="rechecked">Whether the names were read again because an earlier set did not fit (see <see cref="Rechecked"/>).</param>
	internal NameSet(ImmutableArray<string> names, bool rechecked = false)
	{
		Debug.Assert(!names.IsDefault, "A set is made from names, not from 'not known'.");
		Names = names;
		BareNames = ParameterNames.BareNames(names);
		Rechecked = rechecked;
	}

	/// <summary>
	/// Whether this set is the answer of a second look: the names were read again because the set before did not fit.
	/// A member of an <c>AddParams</c> object that such a set does not declare is dropped without another read (the procedure was just asked about it),
	/// so a member that the procedure never takes costs one extra read when the names are read, not one in every run.
	/// </summary>
	internal bool Rechecked { get; }

	/// <summary>The names as the provider reported them.</summary>
	internal ImmutableArray<string> Names { get; }

	/// <summary>The same names without a leading <c>@</c>.</summary>
	internal ImmutableArray<string> BareNames { get; }

	/// <summary>
	/// One name of the set, with its place in it, held once per set: a call that stores it allocates nothing, and a call that binds it
	/// knows its place without looking the name up.
	/// </summary>
	/// <param name="real">The name as the set holds it, with its <c>@</c> when it has one.</param>
	/// <param name="index">The place of the name in <see cref="Names"/>.</param>
	internal sealed class Entry(string real, int index)
	{
		/// <summary>Gets the name as the set holds it.</summary>
		public string Real { get; } = real;

		/// <summary>Gets the place of the name in the set.</summary>
		public int Index { get; } = index;
	}

	/// <summary>
	/// Compares names the way a command compares its parameters: without a leading <c>@</c>, ignoring case.
	/// </summary>
	sealed class BareComparer : IEqualityComparer<string>
	{
		internal static readonly BareComparer Instance = new();

		static ReadOnlySpan<char> Bare(string name)
			=> name.Length > 0 && name[0] == '@' ? name.AsSpan(1) : name.AsSpan();

		public bool Equals(string? x, string? y)
			=> Bare(x!).Equals(Bare(y!), StringComparison.OrdinalIgnoreCase);

		public int GetHashCode(string obj)
		{
			unchecked
			{
				int h = 17;
				foreach (char c in Bare(obj)) h = h * 31 + char.ToUpperInvariant(c);
				return h;
			}
		}
	}

	/// <summary>The entries by name, without the <c>@</c> and ignoring case; built when first asked, never changed.</summary>
	Dictionary<string, Entry>? _entries;

	/// <summary>
	/// Finds a name of the set, written with or without its <c>@</c> and in any case.
	/// </summary>
	/// <remarks>
	/// Allocates nothing once the set has been asked.
	/// When two names differ only in case or in the <c>@</c> the first is the one found.
	/// </remarks>
	/// <param name="name">The name to look for.</param>
	/// <returns>The entry, or <see langword="null"/> when the set has no such name.</returns>
	public Entry? Find(string name)
	{
		Dictionary<string, Entry> entries = Volatile.Read(ref _entries) ?? BuildEntries();
		return entries.TryGetValue(name, out Entry? found) ? found : null;
	}

	Dictionary<string, Entry> BuildEntries()
	{
		var entries = new Dictionary<string, Entry>(Names.Length, BareComparer.Instance);
		for (int i = 0; i < Names.Length; i++)
		{
			if (!entries.ContainsKey(Names[i])) entries.Add(Names[i], new Entry(Names[i], i));
		}

		// Two threads may both build: the entries are the same.
		Volatile.Write(ref _entries, entries);
		return entries;
	}

	/// <summary>
	/// The real name that the argument expression stands for, or <see langword="null"/> when it matches none. The answer is remembered, so the next call with the same expression does no matching.
	/// An ambiguity throws (see <see cref="ParameterNames.MatchBare"/>) and is not remembered, so it throws on every call.
	/// </summary>
	/// <param name="expression">The text of the argument, or the name of a member of an <c>AddParams</c> object.</param>
	/// <param name="procedure">The procedure's name or the command text, for the message of an ambiguity.</param>
	/// <returns>The real name, or <see langword="null"/> when the expression matches none.</returns>
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
