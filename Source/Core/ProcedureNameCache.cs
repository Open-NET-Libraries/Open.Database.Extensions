using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// The server, the database and the procedure a set of names belongs to. A small struct compared part by part, so looking a procedure up builds no string.
/// </summary>
/// <remarks>
/// Each part is compared ignoring case. A part that is <see langword="null"/> is the empty string.
/// </remarks>
readonly struct ProcedureKey : IEquatable<ProcedureKey>
{
	internal ProcedureKey(string? server, string? database, string? procedure)
	{
		// StringComparer.GetHashCode(null) throws, and a missing server or database is the same key as an empty one.
		Server = server ?? string.Empty;
		Database = database ?? string.Empty;
		Procedure = procedure ?? string.Empty;
	}

	internal string Server { get; }
	internal string Database { get; }
	internal string Procedure { get; }

	public bool Equals(ProcedureKey other)
		=> string.Equals(Procedure, other.Procedure, StringComparison.OrdinalIgnoreCase)
		&& string.Equals(Database, other.Database, StringComparison.OrdinalIgnoreCase)
		&& string.Equals(Server, other.Server, StringComparison.OrdinalIgnoreCase);

	public override bool Equals(object? obj) => obj is ProcedureKey other && Equals(other);

	// System.HashCode is not in netstandard2.0.
	public override int GetHashCode()
	{
		StringComparer comparer = StringComparer.OrdinalIgnoreCase;
		unchecked
		{
			int hash = comparer.GetHashCode(Server);
			hash = (hash * 397) ^ comparer.GetHashCode(Database);
			hash = (hash * 397) ^ comparer.GetHashCode(Procedure);
			return hash;
		}
	}
}

/// <summary>
/// Remembers the parameter names of stored procedures, and what is worked out from them, so a command with derived parameter names asks the database once, not on every execution.
/// </summary>
/// <remarks>
/// An entry belongs to the object commands take their connections from (the connection factory, the pool, or the bare connection), held weakly,
/// so the application's own objects decide how long names are remembered and nothing here keeps anything alive.
/// An entry is a <see cref="NameSet"/>: the names, and the real name each argument expression has matched so far, so a repeat execution matches nothing again.
/// Names expire so that a procedure whose parameters change is read again (the matches go with them), and one object remembers at most 1024 procedures. An empty answer ("not known") is never remembered.
/// </remarks>
static class ProcedureNameCache
{
	// How long the names of a procedure are trusted.
	internal static readonly long LifetimeTicks = 5 * 60 * Stopwatch.Frequency;

	// How many procedures one source remembers.
	const int MaxProcedures = 1024;

	static readonly ConditionalWeakTable<object, Entries> Sources = new();

	sealed class Entry(long expires, NameSet names)
	{
		public readonly long Expires = expires;
		public readonly NameSet Names = names;
	}

	sealed class Entries
	{
		public readonly Dictionary<ProcedureKey, Entry> ByProcedure = [];
	}

	internal static bool TryGet(object source, ProcedureKey key, out NameSet? names)
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

		names = null;
		return false;
	}

	/// <summary>Remembers the names and returns the set made from them.</summary>
	internal static NameSet Set(object source, ProcedureKey key, ImmutableArray<string> names)
	{
		Debug.Assert(!names.IsDefaultOrEmpty, "An empty answer means 'not known' and is not remembered.");

		var set = new NameSet(names);
		Entries entries = Sources.GetValue(source, static _ => new Entries());
		lock (entries)
		{
			Dictionary<ProcedureKey, Entry> byProcedure = entries.ByProcedure;
			if (byProcedure.Count >= MaxProcedures && !byProcedure.ContainsKey(key)) Trim(byProcedure);
			byProcedure[key] = new Entry(Stopwatch.GetTimestamp() + LifetimeTicks, set);
		}

		return set;
	}

	// An expired entry is dropped when it is read again, so a source used with a stream of different procedures would keep every one of them.
	// At the limit the expired entries go; if every entry is still fresh, they all go and are read again when needed (as the row-map layouts do).
	static void Trim(Dictionary<ProcedureKey, Entry> byProcedure)
	{
		long now = Stopwatch.GetTimestamp();
		List<ProcedureKey> expired = [];
		foreach (KeyValuePair<ProcedureKey, Entry> pair in byProcedure)
		{
			if (now >= pair.Value.Expires) expired.Add(pair.Key);
		}

		foreach (ProcedureKey key in expired) byProcedure.Remove(key);
		if (byProcedure.Count >= MaxProcedures) byProcedure.Clear();
	}
}
