using System.Runtime.CompilerServices;

namespace Open.Database.Extensions;

/// <summary>
/// Finds the real names of the derived arguments of a text command in the <c>@name</c> markers of its SQL text.
/// </summary>
/// <remarks>
/// A text that is an interned string (a literal or a <see langword="const"/>: the same object on every call, and as many as the code has literals) is read once into a
/// <see cref="NameSet"/> remembered by the text object (weakly: the entry goes when the string is released).
/// Any other text, such as one put together at run time (a new string with the same characters on every call), is scanned for each argument on every run and
/// nothing about it is remembered: the scan allocates nothing, so such a text costs no more than a literal one, and no table gains an entry per call or holds a text.
/// </remarks>
static class TextNameCache
{
	// Only interned texts, by the text object itself: an entry goes when its string is released.
	static readonly ConditionalWeakTable<string, NameSet> ByReference = new();

	/// <summary>
	/// The remembered set of an interned text, made on first use; <see langword="null"/> for a text that is not interned, which is scanned.
	/// </summary>
	/// <remarks>
	/// The text itself must be the interned object: a string built at run time with the characters of a literal is another object, and is scanned.
	/// Looking it up allocates nothing.
	/// </remarks>
	internal static NameSet? Get(string text)
		=> ByReference.TryGetValue(text, out NameSet? known)
			? known
			: ReferenceEquals(string.IsInterned(text), text)
				? ByReference.GetValue(text, static t => new NameSet(SqlTextParameters.Parse(t)))
				: null;

	/// <summary>True when the text has at least one marker (otherwise its parameter names are not known).</summary>
	internal static bool HasMarkers(string text)
	{
		int position = 0;
		return SqlTextParameters.NextMarker(text, ref position, out _, out _);
	}

	/// <summary>The distinct markers of the text, for a message.</summary>
	internal static ImmutableArray<string> Names(string text) => SqlTextParameters.Parse(text);

	/// <summary>
	/// The marker (with its <c>@</c>) that the argument expression stands for, as the text spells it, or <see langword="null"/> when none fits:
	/// the answer <see cref="NameSet.Match"/> gives over the parsed tokens, found without parsing.
	/// Allocates nothing once the expression has been seen and its marker was found before.
	/// </summary>
	/// <exception cref="InvalidOperationException">The expression fits more than one marker equally well.</exception>
	internal static string? Scan(string text, string expression)
	{
		// An expression that is not derivable (or none the cache described) takes the slow way, which gives its own errors.
		if (ParameterNames.Describe(expression) is not { } described)
			return Slow(text, expression);

		switch (ParameterNames.Scan(text, described.Candidates, out ReadOnlySpan<char> real))
		{
			case ParameterNames.ScanResult.Found:
				return described.Real(real);
			case ParameterNames.ScanResult.None:
				return null;
			default:
				// An ambiguity throws with the names the text declares in its message: the parsed way makes it.
				return Slow(text, expression);
		}
	}

	static string? Slow(string text, string expression)
		=> new NameSet(SqlTextParameters.Parse(text)).Match(expression, text);
}
