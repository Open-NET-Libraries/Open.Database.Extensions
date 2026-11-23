using System.Buffers;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

// BS-C: the DS1a candidate variants, the DateTimeOffset scenario and the one-off overload timing, over the synthetic provider.
// The per-width methods (one hand-written method per catalog width, on purpose) are in SyntheticVariantsC.Widths.cs.
//
// What each variant is, and what it is NOT (the equal-work flags E1 to E11 of the DS1a check are listed in BS-C.md; read the numbers with them):
//   V9c-Db   typed getters on the DbDataReader, constant ordinals, IsDBNull only on the columns that can be NULL: the ceiling of DS1a. (V2 is the
//            shipped Map(Func<IDataRecord,T>) recipe, NOT a ceiling.)
//   V9c-Syn  the same on the sealed SyntheticReader (direct calls); the gap to V9c-Db is what a concrete reader is worth over a virtual call.
//   V9d      the plan's content by hand: an index store before every cell, IsDBNull on every cell, one try/catch, new T() then the setters.
//   V9d-M    V9d's shape ending in the massaged object (the constructor path's shape: typed locals, index -1, then the construction).
//   V9e      V9c with GetFieldValue<T> in place of every typed getter.
//   V6b-F    K2 inline: typed locals, ONE call through a delegate that builds the massaged object.
//   V6b-E    K1: typed locals, the plain members from the locals, each computed member from its own delegate call.
//   V6b-K    K0: today's plan, Map<ShapedN>(), whose constructor does the massage.
//   V9a      K2 deferred: typed columns are captured into rented buffers while the reader is open, the objects are built after it closes.
//   V9b      Map<Cells>().ToArray() (struct rows), the projection and the massage after the close.

/// <summary>The four delegates V6b-E calls, as static abstract members so --verify can count calls without touching the measured path.</summary>
public interface IEDelegates
{
	static abstract Func<int, int> D1 { get; }

	static abstract Func<string, int, string> D2 { get; }

	static abstract Func<decimal, int?, decimal> D3 { get; }

	static abstract Func<string?, string> D4 { get; }
}

/// <summary>The delegates the measured V6b-E calls: static readonly, no counting.</summary>
public readonly struct ERealDelegates : IEDelegates
{
	public static Func<int, int> D1 { get; } = SyntheticVariantsC.Id1;

	public static Func<string, int, string> D2 { get; } = SyntheticVariantsC.Title2;

	public static Func<decimal, int?, decimal> D3 { get; } = SyntheticVariantsC.Price3;

	public static Func<string?, string> D4 { get; } = SyntheticVariantsC.Note4;
}

/// <summary>The same delegates, each counting its calls. Used by --verify only: the delegate-call count per row is 1, 2, 3, 4, 4 for widths 1, 3, 5, 10, 20.</summary>
public readonly struct ECountingDelegates : IEDelegates
{
	public static long Calls { get; set; }

	public static Func<int, int> D1 { get; } = static id =>
	{
		Calls++;
		return SyntheticVariantsC.Id1(id);
	};

	public static Func<string, int, string> D2 { get; } = static (title, id) =>
	{
		Calls++;
		return SyntheticVariantsC.Title2(title, id);
	};

	public static Func<decimal, int?, decimal> D3 { get; } = static (price, qty) =>
	{
		Calls++;
		return SyntheticVariantsC.Price3(price, qty);
	};

	public static Func<string?, string> D4 { get; } = static note =>
	{
		Calls++;
		return SyntheticVariantsC.Note4(note);
	};
}

/// <summary>Counts what V9a rents from and returns to ArrayPool, so --verify can show that every rented array came back.</summary>
public static class PoolTally
{
	public static long Rented { get; set; }

	public static long Returned { get; set; }

	public static void Reset()
	{
		Rented = 0;
		Returned = 0;
	}
}

/// <summary>
/// One captured column of V9a: a rented array that doubles when it is full (rent a bigger one, copy, return the old one), and, for a column of a nullable
/// value type, a rented bool array that says which rows were NULL. A nullable reference column holds null itself and needs no bitmap.
/// </summary>
public sealed class Col<T>(bool withNulls)
{
	const int MinimumLength = 16;

	public T[] Items { get; private set; } = Rent(MinimumLength);

	public bool[]? Nulls { get; private set; } = withNulls ? RentFlags(MinimumLength) : null;

	public int Count { get; private set; }

	static T[] Rent(int length)
	{
		PoolTally.Rented++;
		return ArrayPool<T>.Shared.Rent(length);
	}

	static bool[] RentFlags(int length)
	{
		PoolTally.Rented++;
		return ArrayPool<bool>.Shared.Rent(length);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T value)
	{
		if (Count == Items.Length) Grow();
		Items[Count] = value;
		Nulls?[Count] = false;
		Count++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddNull()
	{
		if (Count == Items.Length) Grow();
		Items[Count] = default!;
		Nulls![Count] = true;
		Count++;
	}

	void Grow()
	{
		var bigger = Rent(Items.Length * 2);
		Array.Copy(Items, bigger, Count);
		GiveBack(Items);
		Items = bigger;
		if (Nulls is null) return;

		var flags = RentFlags(bigger.Length);
		Array.Copy(Nulls, flags, Count);
		PoolTally.Returned++;
		ArrayPool<bool>.Shared.Return(Nulls);
		Nulls = flags;
	}

	// An array of references is cleared (the used prefix only) before it goes back, so the pool holds no object of ours.
	void GiveBack(T[] items)
	{
		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) Array.Clear(items, 0, Count);
		PoolTally.Returned++;
		ArrayPool<T>.Shared.Return(items);
	}

	/// <summary>Returns the arrays and says how many bytes were rented: the array lengths times the element size, plus the bitmap.</summary>
	public long Release()
	{
		long bytes = ((long)Items.Length * Unsafe.SizeOf<T>()) + (Nulls?.Length ?? 0);
		GiveBack(Items);
		Items = [];
		if (Nulls is not null)
		{
			PoolTally.Returned++;
			ArrayPool<bool>.Shared.Return(Nulls);
			Nulls = [];
		}

		return bytes;
	}
}

/// <summary>Everything hand-written for one catalog width. The delegates are settable so a self-test can wrap one.</summary>
public sealed partial class Hands
{
	public required int Width { get; init; }

	public required Func<DbDataReader, SynRow> CDb { get; set; }

	public required Func<SyntheticReader, SynRow> CSyn { get; set; }

	public required Func<DbDataReader, SynRow> D { get; set; }

	public required Func<DbDataReader, SynItem> DM { get; set; }

	public required Func<DbDataReader, SynRow> EGet { get; set; }

	public required Func<DbDataReader, SynItem> E { get; set; }

	/// <summary>V6b-E with the counting delegates; --verify only.</summary>
	public required Func<DbDataReader, SynItem> ECounting { get; set; }

	public required Func<DbDataReader, SynItem> F { get; set; }

	public required Func<DbDataReader, Cap> Capture { get; set; }

	public required Func<Cap, List<SynItem>> BuildOn { get; set; }

	public required Func<Cap, List<SynRow>> BuildOff { get; set; }
}

/// <summary>V9b's row: the same 20 members as <see cref="SynRow"/> as a plain mutable struct (built by the plan as new T(), then the setters).</summary>
public struct Cells
{
	public int Id { get; set; }
	public DateTime Created { get; set; }
	public string? Title { get; set; }
	public decimal Price { get; set; }
	public int? Qty { get; set; }
	public bool IsActive { get; set; }
	public Guid Key { get; set; }
	public double Score { get; set; }
	public string? Note { get; set; }
	public byte[]? Blob { get; set; }
	public long Big { get; set; }
	public string? Body { get; set; }
	public decimal? Amount { get; set; }
	public DateTime? Stamp { get; set; }
	public string? Tag { get; set; }
	public double Ratio { get; set; }
	public bool? Flag { get; set; }
	public Guid? Link { get; set; }
	public long Count { get; set; }
	public string? Summary { get; set; }
}

/// <summary>The DateTimeOffset scenario's row (a type with no typed getter: the plan reads it by GetValue and converts).</summary>
public sealed class DtoRow
{
	public int Id { get; set; }
	public string? Title { get; set; }
	public DateTimeOffset When { get; set; }
}

/// <summary>The control: the same row with a DateTime (the plan reads it with the typed getter).</summary>
public sealed class DtRow
{
	public int Id { get; set; }
	public string? Title { get; set; }
	public DateTime When { get; set; }
}

public static partial class SyntheticVariantsC
{
	public const string Sql = SyntheticVariants.Sql;
	public const string WidthMessage = "Hand-written variants exist for widths 1, 3, 5, 10 and 20.";

	/// <summary>BufferBytes of the last V9a operation: the lengths of the rented arrays times their element size, plus the bitmaps. ArrayPool memory is not in BenchmarkDotNet's Allocated.</summary>
	public static long LastBufferBytes { get; private set; }

	// ---- the massage as functions over typed values (one definition of each rule; the V6 rules of SynItem.Massage, R1 to R4) ----------------
	internal static int Id1(int id) => id % 10 == 0 ? -id : id;

	internal static string Title2(string title, int id) => title.Trim() + "|" + Id1(id);

	internal static decimal Price3(decimal price, int? qty) => (price * 2) + (qty ?? 0);

	internal static string Note4(string? note) => note ?? "-";

	// A local equivalent of the plan's internal RowMap<T>.ConvertValue, for the one case of the catalog (a byte[] cell that already is a byte[]). Not an exact replica (E11).
	internal static T ConvertValue<T>(object? value)
	{
		if (value is null or DBNull) return default!;
		if (value is T exact) return exact;
		throw new InvalidCastException($"Cannot convert '{value.GetType()}' to {typeof(T)}.");
	}

	// What the plan raises for a failing cell (it wraps in a ValueMappingException, which is internal): the shape of the handler, not its type.
	internal static Exception MapError(Exception ex, int index)
		=> new InvalidOperationException("Column " + index + " could not be read.", ex);

	// ---- the hand loop (DS1a.check 1.1) ----------------------------------------------------------------
	// Sequential access, no SingleResult; the reader is the provider's own; the later result sets are read through as the library does (RowReader.ReadThrough).
	// The map is a static delegate called per row, as the plan's mapper is. Nothing is paid per query beyond the list itself: the state overload keeps the lambda static.
	public static List<T> HandList<T>(SyntheticConnection connection, Func<DbDataReader, T> map)
		=> connection.Command(Sql).ExecuteReader(map, static (reader, read) =>
		{
			var db = (DbDataReader)reader;
			var list = new List<T>();
			while (db.Read())
				list.Add(read(db));

			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>The same loop on the sealed SyntheticReader: the cast is once per query, every call after it is direct.</summary>
	public static List<T> HandListSyn<T>(SyntheticConnection connection, Func<SyntheticReader, T> map)
		=> connection.Command(Sql).ExecuteReader(map, static (reader, read) =>
		{
			var db = (SyntheticReader)reader;
			var list = new List<T>();
			while (db.Read())
				list.Add(read(db));

			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	// ---- the variants (the benchmark class and --verify both call these) ----------------------------------------------------------------
	public static List<SynRow> V9cDb(SyntheticConnection connection, Hands hands) => HandList(connection, hands.CDb);

	public static List<SynRow> V9cSyn(SyntheticConnection connection, Hands hands) => HandListSyn(connection, hands.CSyn);

	public static List<SynRow> V9d(SyntheticConnection connection, Hands hands) => HandList(connection, hands.D);

	public static List<SynItem> V9dM(SyntheticConnection connection, Hands hands) => HandList(connection, hands.DM);

	public static List<SynRow> V9e(SyntheticConnection connection, Hands hands) => HandList(connection, hands.EGet);

	public static List<SynItem> V6bF(SyntheticConnection connection, Hands hands) => HandList(connection, hands.F);

	public static List<SynItem> V6bE(SyntheticConnection connection, Hands hands) => HandList(connection, hands.E);

	// V9a: capture while open, build after the close. The capture method reads the rows; the drain follows it inside the callback, as in every hand variant.
	static Cap CaptureRows(SyntheticConnection connection, Func<DbDataReader, Cap> capture)
		=> connection.Command(Sql).ExecuteReader(capture, static (reader, read) =>
		{
			var db = (DbDataReader)reader;
			var cap = read(db);
			while (db.NextResult()) { }
			return cap;
		}, CommandBehavior.SequentialAccess);

	/// <summary>V9a on: the V6b-F function builds the massaged objects after the close, from the captured columns.</summary>
	public static List<SynItem> V9aOn(SyntheticConnection connection, Hands hands)
	{
		var cap = CaptureRows(connection, hands.Capture);
		var list = hands.BuildOn(cap);
		LastBufferBytes = cap.Release();
		return list;
	}

	/// <summary>V9a off: the same capture and shape, no massage, building V1's row type.</summary>
	public static List<SynRow> V9aOff(SyntheticConnection connection, Hands hands)
	{
		var cap = CaptureRows(connection, hands.Capture);
		var list = hands.BuildOff(cap);
		LastBufferBytes = cap.Release();
		return list;
	}

	/// <summary>V9b: struct rows through the library, then the projection and the massage after the close.</summary>
	public static List<SynItem> V9b(SyntheticConnection connection, int width)
	{
		var cells = connection.Command(Sql).Map<Cells>().ToArray();
		var list = new List<SynItem>(cells.Length);
		foreach (ref readonly var cell in cells.AsSpan())
			list.Add(SynItem.Massage(FromCells(in cell), width));
		return list;
	}

	[System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1242:Do not pass non-read-only struct by read-only reference", Justification = "Cells is mutable on purpose (the plan sets it by its setters); it is read here through auto-property getters, which are readonly, so nothing is copied.")]
	public static SynItem FromCells(in Cells c) => new()
	{
		Id = c.Id,
		Created = c.Created,
		Title = c.Title,
		Price = c.Price,
		Qty = c.Qty,
		IsActive = c.IsActive,
		Key = c.Key,
		Score = c.Score,
		Note = c.Note,
		Blob = c.Blob,
		Big = c.Big,
		Body = c.Body,
		Amount = c.Amount,
		Stamp = c.Stamp,
		Tag = c.Tag,
		Ratio = c.Ratio,
		Flag = c.Flag,
		Link = c.Link,
		Count = c.Count,
		Summary = c.Summary,
	};

	// V1-fresh: a new RowMap<SynRow> inside every operation. A map holds its own cache of compiled plans, so every operation builds and compiles its plan (no cache hit): the cost V1 pays once.
	public static List<SynRow> V1Fresh(SyntheticConnection connection)
		=> connection.Command(Sql).Map(new RowMap<SynRow>(ReadOnlySpan<KeyValuePair<string, string?>>.Empty)).ToList();

	// ---- the DateTimeOffset scenario (table: Id, Title, When) ----------------------------------------------------------------
	public static List<DtoRow> DtoV1(SyntheticConnection connection) => connection.Command(Sql).Map<DtoRow>().ToList();

	public static List<DtRow> DtV1(SyntheticConnection connection) => connection.Command(Sql).Map<DtRow>().ToList();

	static readonly Func<DbDataReader, DtoRow> DtoBoxed = static r => new()
	{
		Id = r.GetInt32(0),
		Title = r.GetString(1),
		When = (DateTimeOffset)r.GetValue(2),
	};

	static readonly Func<DbDataReader, DtoRow> DtoTyped = static r => new()
	{
		Id = r.GetInt32(0),
		Title = r.GetString(1),
		When = r.GetFieldValue<DateTimeOffset>(2),
	};

	public static List<DtoRow> DtoHandBoxed(SyntheticConnection connection) => HandList(connection, DtoBoxed);

	public static List<DtoRow> DtoHandTyped(SyntheticConnection connection) => HandList(connection, DtoTyped);

	/// <summary>The row as the table holds it (no reader), so every variant of the scenario can be checked against it.</summary>
	public static DtoRow DtoFromTable(SyntheticTable t, int i) => new()
	{
		Id = ((int[])t.Columns[0])[i],
		Title = ((string[])t.Columns[1])[i],
		When = ((DateTimeOffset[])t.Columns[2])[i],
	};

	public static DtRow DtFromTable(SyntheticTable t, int i) => new()
	{
		Id = ((int[])t.Columns[0])[i],
		Title = ((string[])t.Columns[1])[i],
		When = ((DateTime[])t.Columns[2])[i],
	};

	// The Map<T>(params KeyValuePair...) one-off overloads are gone from Core: the same one-off path is a new RowMap<T> built from the pairs inside each call (the map is built and validated every time),
	// then Map(command, map). The plan is compiled and cached per T and layout, so the cost per call is the map, not the plan.
	// then Map(command, map). The single-pair overload (line 81) goes the same way (new RowMap<T>(mapping)). The plan is compiled and cached per T and layout, so the cost per call is the map, not the plan.
	public static List<SynRow> OneOffList(SyntheticConnection connection)
		=> connection.Command(Sql).Map(new RowMap<SynRow>(
			new KeyValuePair<string, string?>(nameof(SynRow.Id), "Id"),
			new KeyValuePair<string, string?>(nameof(SynRow.Created), "Created"),
			new KeyValuePair<string, string?>(nameof(SynRow.Title), "Title"))).ToList();

	public static SynRow OneOffFirst(SyntheticConnection connection)
		=> connection.Command(Sql).Map(new RowMap<SynRow>(
			new KeyValuePair<string, string?>(nameof(SynRow.Id), "Id"),
			new KeyValuePair<string, string?>(nameof(SynRow.Created), "Created"),
			new KeyValuePair<string, string?>(nameof(SynRow.Title), "Title"))).First();

	/// <summary>The same three pairs, declared once: the way the docs say to hold a map.</summary>
	static readonly RowMap<SynRow> Declared = new(
		new KeyValuePair<string, string?>(nameof(SynRow.Id), "Id"),
		new KeyValuePair<string, string?>(nameof(SynRow.Created), "Created"),
		new KeyValuePair<string, string?>(nameof(SynRow.Title), "Title"));

	public static List<SynRow> DeclaredList(SyntheticConnection connection) => connection.Command(Sql).Map(Declared).ToList();

	public static SynRow DeclaredFirst(SyntheticConnection connection) => connection.Command(Sql).Map(Declared).First();
}
