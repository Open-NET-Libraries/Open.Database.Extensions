using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// Remembers the parameter names of stored procedures, so a command with derived parameter names asks the database once, not on every execution.
/// </summary>
/// <remarks>
/// An entry belongs to the object commands take their connections from (the connection factory, the pool, or the bare connection), held weakly,
/// so the application's own objects decide how long names are remembered and nothing here keeps anything alive.
/// Names expire so that a procedure whose parameters change is read again, and one object remembers at most 1024 procedures. An empty answer ("not known") is never remembered.
/// </remarks>
static class ProcedureNameCache
{
	// How long the names of a procedure are trusted.
	internal static readonly long LifetimeTicks = 5 * 60 * Stopwatch.Frequency;

	// How many procedures one source remembers.
	const int MaxProcedures = 1024;

	static readonly ConditionalWeakTable<object, Entries> Sources = new();

	sealed class Entry(long expires, ImmutableArray<string> names)
	{
		public readonly long Expires = expires;
		public readonly ImmutableArray<string> Names = names;
	}

	sealed class Entries
	{
		public readonly Dictionary<string, Entry> ByProcedure = [with(StringComparer.OrdinalIgnoreCase)];
	}

	internal static bool TryGet(object source, string key, out ImmutableArray<string> names)
	{
		if (Sources.TryGetValue(source, out Entries? entries))
		{
			lock (entries)
			{
				if (entries.ByProcedure.TryGetValue(key, out Entry? entry))
				{
					if (Stopwatch.GetTimestamp() < entry.Expires)
					{
						names = entry.Names;
						return true;
					}

					entries.ByProcedure.Remove(key);
				}
			}
		}

		names = default;
		return false;
	}

	internal static void Set(object source, string key, ImmutableArray<string> names)
	{
		Debug.Assert(!names.IsDefaultOrEmpty, "An empty answer means 'not known' and is not remembered.");

		Entries entries = Sources.GetValue(source, static _ => new Entries());
		lock (entries)
		{
			Dictionary<string, Entry> byProcedure = entries.ByProcedure;
			if (byProcedure.Count >= MaxProcedures && !byProcedure.ContainsKey(key)) Trim(byProcedure);
			byProcedure[key] = new Entry(Stopwatch.GetTimestamp() + LifetimeTicks, names);
		}
	}

	// An expired entry is dropped when it is read again, so a source used with a stream of different procedures would keep every one of them.
	// At the limit the expired entries go; if every entry is still fresh, they all go and are read again when needed (as the row-map layouts do).
	static void Trim(Dictionary<string, Entry> byProcedure)
	{
		long now = Stopwatch.GetTimestamp();
		List<string> expired = [];
		foreach (KeyValuePair<string, Entry> pair in byProcedure)
		{
			if (now >= pair.Value.Expires) expired.Add(pair.Key);
		}

		foreach (string key in expired) byProcedure.Remove(key);
		if (byProcedure.Count >= MaxProcedures) byProcedure.Clear();
	}
}
