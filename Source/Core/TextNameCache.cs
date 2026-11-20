using System.Runtime.CompilerServices;

namespace Open.Database.Extensions;

/// <summary>
/// Remembers the <c>@name</c> markers of SQL texts, and what is worked out
/// from them, so a command with derived parameter names reads its text once,
/// not on every execution.
/// </summary>
/// <remarks>
/// A text is found by its object first (a literal or a kept string is the same
/// object on every call, so that costs nothing), then by its content, so a
/// text put together at run time, a new string with the same characters on
/// every call, is not read again either. What is remembered by content is
/// bounded in count and in length, and the entry of a text that was forgotten
/// is simply made again.
/// </remarks>
static class TextNameCache
{
	// How many texts are remembered by content.
	const int MaxTexts = 1024;

	// A longer text is remembered by its object only, so the content table
	// cannot hold more than MaxTexts * MaxLength characters.
	const int MaxLength = 4096;

	// By the text object itself: an entry goes when its string is released.
	static readonly ConditionalWeakTable<string, NameSet> ByReference = new();

	// By the characters of the text. A text with no marker has a set with no
	// names, which means "not known".
	static readonly Dictionary<string, NameSet> ByContent
		= new(StringComparer.Ordinal);

	internal static NameSet Get(string text)
	{
		if (ByReference.TryGetValue(text, out NameSet? known)) return known;

		if (text.Length > MaxLength)
			return ByReference.GetValue(text, static t => Make(t));

		lock (ByContent)
		{
			if (ByContent.TryGetValue(text, out known)) return known;
		}

		return Remember(text);
	}

	// Apart from Get so that the closure it needs is made only here:
	// a repeat call allocates nothing.
	static NameSet Remember(string text)
	{
		// Two threads may both read the text and store: the sets are equal.
		NameSet made = Make(text);
		lock (ByContent)
		{
			if (ByContent.Count >= MaxTexts) ByContent.Clear();
			ByContent[text] = made;
		}

		return ByReference.GetValue(text, _ => made);
	}

	static NameSet Make(string text) => new(SqlTextParameters.Parse(text));
}
