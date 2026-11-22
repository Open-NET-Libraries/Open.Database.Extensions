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
	/// <summary>
	/// The remembered sets, only for interned texts, by the text object itself.
	/// </summary>
	/// <remarks>An entry goes when its string is released.</remarks>
	static readonly ConditionalWeakTable<string, NameSet> ByReference = new();

	/// <summary>
	/// Gets the remembered set of an interned text, made on first use; <see langword="null"/> for a text that is not interned, which is scanned.
	/// </summary>
	/// <remarks>
	/// The text itself must be the interned object: a string built at run time with the characters of a literal is another object, and is scanned.
	/// Looking it up allocates nothing.
	/// </remarks>
	/// <param name="text">The SQL text.</param>
	/// <returns>The set of its markers, or <see langword="null"/> when the text is not interned.</returns>
	internal static NameSet? Get(string text)
		=> ByReference.TryGetValue(text, out NameSet? known)
			? known
			: ReferenceEquals(string.IsInterned(text), text)
				? ByReference.GetValue(text, static t => new NameSet(SqlTextParameters.Parse(t)))
				: null;

	/// <summary>Determines whether the text has at least one marker (otherwise its parameter names are not known).</summary>
	/// <param name="text">The SQL text.</param>
	/// <returns><see langword="true"/> if the text has a marker; otherwise, <see langword="false"/>.</returns>
	internal static bool HasMarkers(string text)
	{
		int position = 0;
		return SqlTextParameters.NextMarker(text, ref position, out _, out _);
	}

	/// <summary>Gets the distinct markers of the text, for a message.</summary>
	/// <param name="text">The SQL text.</param>
	/// <returns>The markers, each with its <c>@</c>.</returns>
	internal static ImmutableArray<string> Names(string text) => SqlTextParameters.Parse(text);

	/// <summary>
	/// Gets the marker (with its <c>@</c>) that the argument expression stands for, as the text spells it, or <see langword="null"/> when none fits:
	/// the answer <see cref="NameSet.Match"/> gives over the parsed tokens, found without parsing.
	/// Allocates nothing once the expression has been seen and its marker was found before.
	/// </summary>
	/// <param name="text">The SQL text.</param>
	/// <param name="expression">The text of the argument.</param>
	/// <returns>The marker, or <see langword="null"/> when none fits.</returns>
	/// <exception cref="InvalidOperationException">The expression fits more than one marker equally well.</exception>
	internal static string? Scan(string text, string expression)
	{
		// An expression that is not derivable (or none the cache described) takes the slow way, which gives its own errors.
		if (ParameterNames.Describe(expression) is not { } described)
			return Slow(text, expression);

		// An ambiguity throws with the names the text declares in its message: the parsed way makes it.
		return ParameterNames.Scan(text, described.Candidates, out ReadOnlySpan<char> real) switch
		{
			ParameterNames.ScanResult.Found => described.Real(real),
			ParameterNames.ScanResult.None => null,
			_ => Slow(text, expression),
		};
	}

	static string? Slow(string text, string expression)
		=> new NameSet(SqlTextParameters.Parse(text)).Match(expression, text);
}
