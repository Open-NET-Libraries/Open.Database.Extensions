using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

// V10: the code a Map<T>() SOURCE GENERATOR would emit for SynRow, written by hand.
//
//   1. The ordinals are resolved ONCE PER RESULT SET, by name, from the reader's schema (SynRowGenerated.Resolve): the three tiers of RowMap.Match
//      (exact, then ignoring case, then ignoring case and underscores; a column feeds one property; the first match wins) and the same default map
//      (a property reads the column of its own name). The result is a small struct of 20 ints (-1: no column feeds that property, the property keeps its default).
//   2. Every row is then read with TYPED getters at those ordinals, IsDBNull on the 7 nullable members only (the ones Hand(20) tests): no Func delegate,
//      no IDataRecord interface call, no per-row name or type lookup. The row method is one per reader type: DbDataReader (provider independent),
//      the sealed SyntheticReader, the sealed SqlDataReader and SqliteDataReader (direct calls).
//   3. The row method is written once for every width. A generator cannot know which columns a query returns, so each property is guarded by "its ordinal is
//      not -1" (the plan RowMap compiles per layout has no such test). That guard is the price of by-name matching in a generated method; at width 5 fifteen
//      of the twenty guards are false on every row.
//
// Where it runs (what is public at master, read from Source/Core/MappedQuery.cs, RowReader.cs, RowMap.cs):
//   V10   INSIDE ODX's read loop, through the nearest public entry: Map(Func<IDataRecord, T>) (the only public entry that runs caller code in RowReader's loop;
//         IRowSource<T>, the interface that would take a generated mapper without a delegate, is INTERNAL, and RowMap<T> has no public way to hand it one).
//         So V10 is NOT delegate-free: one delegate call per row and the selector's reader field. The selector is an object made per query, resolves the
//         ordinals on its first row (one result set: the first one, the only one Map reads) and then calls the same typed row method on the reader the
//         loop hands it (a DbDataReader, cast once). It runs with the access mode the library picks for a selector (default, not SequentialAccess).
//   V10r  the RAW LOOP: ExecuteReader's callback, while (reader.Read()) list.Add(Read(reader, in ordinals)), the later result sets read through as every hand
//         variant does (V9c's loop and drain), SequentialAccess. No delegate and no interface call per cell or per row. Static type DbDataReader.
//   V10c  V10r on the sealed SyntheticReader (synthetic provider only): the gap to V10r is what a concrete reader is worth over a virtual call.
//   V10i  V10 with the row method marked AggressiveInlining, called exactly as V10 calls it (through the selector delegate); the JIT cannot inline through a delegate except by guarded devirtualization under dynamic PGO, so what it did is the measurement.
//   V10ri V10r with the AggressiveInlining row method called DIRECTLY (no delegate); V10rn the same with a NoInlining row method ("called, not inlined", stated and not left to the JIT).
//   V10L  the WHOLE read loop generated for SynRow (the mapping inline in the loop), in ExecuteReader's callback (ODX opens and closes the reader). Its body is the row method's text, reading the same fields of the same ordinals struct as V10r, V10ri and V10rn,
//         so the only differences between V10rn, V10ri and V10L are the call (made / inlined by the attribute / fused); the one remaining difference is that ReadAll's struct is a local and the row methods get it as an `in` reference.
//   V10r on the real providers is written on the CONCRETE reader (SqlDataReader, SqliteDataReader) as V9c is, and takes its own stopwatch like V9c.
//
// SequentialAccess reads columns in ascending ordinal order only. The row method reads in property order, so V10r and V10c are right only while the
// ordinals ascend (true for every table here). A generator would have to read in column order (a switch per column) or use the default access mode.

/// <summary>The ordinal of each <see cref="SynRow"/> property in a result set (-1: no column feeds it).</summary>
public readonly struct SynRowOrdinals
{
	public readonly int Id, Created, Title, Price, Qty, IsActive, Key, Score, Note, Blob, Big, Body, Amount, Stamp, Tag, Ratio, Flag, Link, Count, Summary;

	public SynRowOrdinals(ReadOnlySpan<int> o)
	{
		Id = o[0];
		Created = o[1];
		Title = o[2];
		Price = o[3];
		Qty = o[4];
		IsActive = o[5];
		Key = o[6];
		Score = o[7];
		Note = o[8];
		Blob = o[9];
		Big = o[10];
		Body = o[11];
		Amount = o[12];
		Stamp = o[13];
		Tag = o[14];
		Ratio = o[15];
		Flag = o[16];
		Link = o[17];
		Count = o[18];
		Summary = o[19];
	}

	/// <summary>The ordinals of the properties that have a column, in property order: true when they ascend (what SequentialAccess needs).</summary>
	public bool Ascend()
	{
		int last = -1;
		foreach (int o in (ReadOnlySpan<int>)[Id, Created, Title, Price, Qty, IsActive, Key, Score, Note, Blob, Big, Body, Amount, Stamp, Tag, Ratio, Flag, Link, Count, Summary])
		{
			if (o < 0) continue;
			if (o <= last) return false;
			last = o;
		}

		return true;
	}
}

/// <summary>What the generator would emit for SynRow: the resolver and the row methods.</summary>
public static partial class SynRowGenerated
{
	public const int Count = 20;

	/// <summary>How many times <see cref="Resolve"/> ran: once per query, so --verify can show the ordinals are not resolved per row.</summary>
	public static long Resolves { get; private set; }

	static readonly string[] Properties =
	[
		"Id", "Created", "Title", "Price", "Qty", "IsActive", "Key", "Score", "Note", "Blob",
		"Big", "Body", "Amount", "Stamp", "Tag", "Ratio", "Flag", "Link", "Count", "Summary",
	];

	// Tier 1 as a string switch (the compiler makes it a hash jump): the property a column of exactly this name feeds, else -1.
	static int Exact(string name) => name switch
	{
		"Id" => 0,
		"Created" => 1,
		"Title" => 2,
		"Price" => 3,
		"Qty" => 4,
		"IsActive" => 5,
		"Key" => 6,
		"Score" => 7,
		"Note" => 8,
		"Blob" => 9,
		"Big" => 10,
		"Body" => 11,
		"Amount" => 12,
		"Stamp" => 13,
		"Tag" => 14,
		"Ratio" => 15,
		"Flag" => 16,
		"Link" => 17,
		"Count" => 18,
		"Summary" => 19,
		_ => -1,
	};

	// The tier 3 comparison of RowMap: equal ignoring case and every '_' in either name.
	static bool EqualIgnoringCaseAndUnderscores(string x, string y)
	{
		int i = 0, j = 0;
		while (true)
		{
			while (i < x.Length && x[i] == '_') i++;
			while (j < y.Length && y[j] == '_') j++;
			if (i == x.Length || j == y.Length) return i == x.Length && j == y.Length;
			if (char.ToUpperInvariant(x[i++]) != char.ToUpperInvariant(y[j++])) return false;
		}
	}

	/// <summary>Once per result set: the ordinal of each property, by the rules of RowMap.Match. Nothing is allocated for a result set of up to 256 columns.</summary>
	public static SynRowOrdinals Resolve(DbDataReader reader)
	{
		Resolves++;
		int n = reader.FieldCount;
		Span<int> ordinals = stackalloc int[Count];
		ordinals.Fill(-1);
		Span<bool> fed = n <= 256 ? stackalloc bool[n] : new bool[n];
		int unmatched = n;

		for (int i = 0; i < n; i++)
		{
			int p = Exact(reader.GetName(i));
			if (p < 0 || ordinals[p] >= 0) continue;

			ordinals[p] = i;
			fed[i] = true;
			unmatched--;
		}

		for (int tier = 2; tier <= 3 && unmatched > 0; tier++)
		{
			for (int i = 0; i < n; i++)
			{
				if (fed[i]) continue;

				string name = reader.GetName(i);
				for (int p = 0; p < Count; p++)
				{
					if (ordinals[p] >= 0) continue;
					if (!(tier == 2 ? string.Equals(name, Properties[p], StringComparison.OrdinalIgnoreCase) : EqualIgnoringCaseAndUnderscores(name, Properties[p]))) continue;

					ordinals[p] = i;
					fed[i] = true;
					unmatched--;
					break;
				}
			}
		}

		return new SynRowOrdinals(ordinals);
	}


	/// <summary>Resolve for a type whose first <paramref name="width"/> members are REQUIRED: the same resolver, then a missing column throws here, once per result set (V10k, V10kri, V10kL).</summary>
	public static SynRowOrdinals ResolveRequired(DbDataReader reader, int width)
	{
		var o = Resolve(reader);
		ReadOnlySpan<int> all = [o.Id, o.Created, o.Title, o.Price, o.Qty, o.IsActive, o.Key, o.Score, o.Note, o.Blob, o.Big, o.Body, o.Amount, o.Stamp, o.Tag, o.Ratio, o.Flag, o.Link, o.Count, o.Summary];
		for (int i = 0; i < width; i++)
		{
			if (all[i] < 0) throw new InvalidOperationException($"The required member '{Properties[i]}' has no column in the result set.");
		}

		return o;
	}

}

/// <summary>The variants V10, V10r, V10c, V10i and V10L over one connection (the synthetic one, or a real provider's).</summary>
public static class GeneratorShaped
{
	public const string Sql = SyntheticVariants.Sql;

	/// <summary>The per-query selector of V10: resolves the ordinals on the first row, then reads every row with the typed row method.</summary>
	sealed class Selector
	{
		DbDataReader? _reader;
		SynRowOrdinals _ordinals;

		public SynRow Read(IDataRecord record)
		{
			if (_reader is null)
			{
				_reader = (DbDataReader)record;
				_ordinals = SynRowGenerated.Resolve(_reader);
			}

			return SynRowGenerated.Read(_reader, in _ordinals);
		}
	}

	/// <summary>The per-query selector of V10i: the same as <see cref="Selector"/>, calling the AggressiveInlining copy of the row method.</summary>
	sealed class InlineSelector
	{
		DbDataReader? _reader;
		SynRowOrdinals _ordinals;

		public SynRow Read(IDataRecord record)
		{
			if (_reader is null)
			{
				_reader = (DbDataReader)record;
				_ordinals = SynRowGenerated.Resolve(_reader);
			}

			return SynRowGenerated.ReadInline(_reader, in _ordinals);
		}
	}

	/// <summary>V10: inside ODX's read loop through the nearest public entry, Map(Func&lt;IDataRecord, T&gt;). Not delegate-free (see the file comment).</summary>
	public static List<SynRow> V10(DbConnection connection, string sql)
	{
		Func<IDataRecord, SynRow> read = new Selector().Read;
		return connection.Command(sql).Map(read).ToList();
	}

	/// <summary>V10i: V10 with the row method marked AggressiveInlining, called exactly as V10 calls it (through the selector delegate). The JIT cannot inline through a delegate except by guarded devirtualization under dynamic PGO: what it did is the measurement.</summary>
	public static List<SynRow> V10i(DbConnection connection, string sql)
	{
		Func<IDataRecord, SynRow> read = new InlineSelector().Read;
		return connection.Command(sql).Map(read).ToList();
	}

	/// <summary>V10L: the whole loop generated for SynRow (ordinals once, the mapping inline in the loop), inside ExecuteReader's callback (ODX opens and closes the reader; the loop is the generated one). Static type DbDataReader.</summary>
	public static List<SynRow> V10L(DbConnection connection, string sql)
		=> connection.Command(sql).ExecuteReader(static reader =>
		{
			var db = reader;
			var list = SynRowGenerated.ReadAll(db);
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V10r: the raw loop, static type DbDataReader.</summary>
	public static List<SynRow> V10r(DbConnection connection, string sql)
		=> connection.Command(sql).ExecuteReader(static reader =>
		{
			var db = reader;
			var ordinals = SynRowGenerated.Resolve(db);
			var list = new List<SynRow>();
			while (db.Read()) list.Add(SynRowGenerated.Read(db, in ordinals));
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V10c: the raw loop on the sealed SyntheticReader.</summary>
	public static List<SynRow> V10c(DbConnection connection, string sql)
		=> connection.Command(sql).ExecuteReader(static reader =>
		{
			var db = (SyntheticReader)reader;
			var ordinals = SynRowGenerated.Resolve(db);
			var list = new List<SynRow>();
			while (db.Read()) list.Add(SynRowGenerated.Read(db, in ordinals));
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V10r on SQL Server: the concrete reader, its own stopwatch (as V9c: the reader is the provider's, not the timing wrapper's).</summary>
	public static List<SynRow> V10rSql(SqlConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			var ordinals = SynRowGenerated.Resolve(reader);
			while (reader.Read()) list.Add(SynRowGenerated.Read(reader, in ordinals));
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>V10L on SQL Server: the generated loop on the concrete reader, its own stopwatch.</summary>
	public static List<SynRow> V10LSql(SqlConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		List<SynRow> list;
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			list = SynRowGenerated.ReadAll(reader);
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>V10L on SQLite: the generated loop on the concrete reader, its own stopwatch.</summary>
	public static List<SynRow> V10LLite(SqliteConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		List<SynRow> list;
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			list = SynRowGenerated.ReadAll(reader);
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>V10r on SQLite: the concrete reader, its own stopwatch.</summary>
	public static List<SynRow> V10rLite(SqliteConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			var ordinals = SynRowGenerated.Resolve(reader);
			while (reader.Read()) list.Add(SynRowGenerated.Read(reader, in ordinals));
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>V10ri: the raw loop of V10r calling the AggressiveInlining (V10ri) row method directly (no delegate), static type DbDataReader.</summary>
	public static List<SynRow> V10ri(DbConnection connection, string sql)
		=> connection.Command(sql).ExecuteReader(static reader =>
		{
			var db = reader;
			var ordinals = SynRowGenerated.Resolve(db);
			var list = new List<SynRow>();
			while (db.Read()) list.Add(SynRowGenerated.ReadInline(db, in ordinals));
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V10ri on SQL Server: the concrete reader, its own stopwatch.</summary>
	public static List<SynRow> V10riSql(SqlConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			var ordinals = SynRowGenerated.Resolve(reader);
			while (reader.Read()) list.Add(SynRowGenerated.ReadInline(reader, in ordinals));
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>V10ri on SQLite: the concrete reader, its own stopwatch.</summary>
	public static List<SynRow> V10riLite(SqliteConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			var ordinals = SynRowGenerated.Resolve(reader);
			while (reader.Read()) list.Add(SynRowGenerated.ReadInline(reader, in ordinals));
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>V10rn: the raw loop of V10r calling the NoInlining (V10rn) row method directly (no delegate), static type DbDataReader.</summary>
	public static List<SynRow> V10rn(DbConnection connection, string sql)
		=> connection.Command(sql).ExecuteReader(static reader =>
		{
			var db = reader;
			var ordinals = SynRowGenerated.Resolve(db);
			var list = new List<SynRow>();
			while (db.Read()) list.Add(SynRowGenerated.ReadNoInline(db, in ordinals));
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V10rn on SQL Server: the concrete reader, its own stopwatch.</summary>
	public static List<SynRow> V10rnSql(SqlConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			var ordinals = SynRowGenerated.Resolve(reader);
			while (reader.Read()) list.Add(SynRowGenerated.ReadNoInline(reader, in ordinals));
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>V10rn on SQLite: the concrete reader, its own stopwatch.</summary>
	public static List<SynRow> V10rnLite(SqliteConnection connection, SyntheticRecorder recorder)
	{
		using var command = connection.CreateCommand();
		command.CommandText = RealProviderVariants.Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			var ordinals = SynRowGenerated.Resolve(reader);
			while (reader.Read()) list.Add(SynRowGenerated.ReadNoInline(reader, in ordinals));
			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	// ---- V10k, V10kri, V10kL: every member REQUIRED (GeneratorShapedRowsK.cs): no presence branch per row, a missing column throws at resolve time. Width w = the first w members of SynRow are the required ones. ----

	/// <summary>V10k: required members, the raw loop calling a NoInlining row method (the required-members twin of V10rn).</summary>
	public static List<SynRow> V10k(DbConnection connection, string sql, int width)
		=> connection.Command(sql).ExecuteReader(width, static (reader, w) =>
		{
			var db = reader;
			var list = w switch
			{
				1 => SynRowGenerated.LoopKn1(db),
				3 => SynRowGenerated.LoopKn3(db),
				5 => SynRowGenerated.LoopKn5(db),
				10 => SynRowGenerated.LoopKn10(db),
				20 => SynRowGenerated.LoopKn20(db),
				_ => throw new ArgumentOutOfRangeException(nameof(width), SyntheticVariantsC.WidthMessage),
			};
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V10kri: required members, the raw loop calling an AggressiveInlining row method (the twin of V10ri).</summary>
	public static List<SynRow> V10kri(DbConnection connection, string sql, int width)
		=> connection.Command(sql).ExecuteReader(width, static (reader, w) =>
		{
			var db = reader;
			var list = w switch
			{
				1 => SynRowGenerated.LoopKi1(db),
				3 => SynRowGenerated.LoopKi3(db),
				5 => SynRowGenerated.LoopKi5(db),
				10 => SynRowGenerated.LoopKi10(db),
				20 => SynRowGenerated.LoopKi20(db),
				_ => throw new ArgumentOutOfRangeException(nameof(width), SyntheticVariantsC.WidthMessage),
			};
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V10kL: required members, the whole loop generated, the mapping inline in the loop (the twin of V10L).</summary>
	public static List<SynRow> V10kL(DbConnection connection, string sql, int width)
		=> connection.Command(sql).ExecuteReader(width, static (reader, w) =>
		{
			var db = reader;
			var list = w switch
			{
				1 => SynRowGenerated.ReadAllK1(db),
				3 => SynRowGenerated.ReadAllK3(db),
				5 => SynRowGenerated.ReadAllK5(db),
				10 => SynRowGenerated.ReadAllK10(db),
				20 => SynRowGenerated.ReadAllK20(db),
				_ => throw new ArgumentOutOfRangeException(nameof(width), SyntheticVariantsC.WidthMessage),
			};
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);
}
