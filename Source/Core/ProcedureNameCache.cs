using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// Remembers the parameter names of stored procedures, so a command with derived parameter names asks the database once, not on every execution.
/// </summary>
/// <remarks>
/// An entry belongs to the object commands take their connections from (the connection factory, the pool, or the bare connection), held weakly,
/// so the application's own objects decide how long names are remembered and nothing here keeps anything alive.
/// Names expire so that a procedure whose parameters change is read again. An empty answer ("not known") is never remembered.
/// </remarks>
static class ProcedureNameCache
{
	// How long the names of a procedure are trusted.
	internal static readonly long LifetimeTicks = 5 * 60 * Stopwatch.Frequency;

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
			entries.ByProcedure[key] = new Entry(Stopwatch.GetTimestamp() + LifetimeTicks, names);
	}
}
