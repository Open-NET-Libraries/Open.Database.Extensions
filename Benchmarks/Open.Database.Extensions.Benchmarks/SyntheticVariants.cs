using System.Data;
using System.Data.Common;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>The typed row for the catalog: one property per column, in catalog order. A width below 20 leaves the later properties at their default.</summary>
public sealed class SynRow
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

	/// <summary>The row as the table holds it, read straight from the column arrays (no reader), so every variant can be checked against it.</summary>
	public static SynRow FromTable(SyntheticTable t, int i)
	{
		var c = t.Columns;
		var n = t.Nulls;
		var r = new SynRow();
		int w = t.Width;
		if (w > 0) r.Id = ((int[])c[0])[i];
		if (w > 1) r.Created = ((DateTime[])c[1])[i];
		if (w > 2) r.Title = ((string[])c[2])[i];
		if (w > 3) r.Price = ((decimal[])c[3])[i];
		if (w > 4) r.Qty = n[4]![i] ? null : ((int[])c[4])[i];
		if (w > 5) r.IsActive = ((bool[])c[5])[i];
		if (w > 6) r.Key = ((Guid[])c[6])[i];
		if (w > 7) r.Score = ((double[])c[7])[i];
		if (w > 8) r.Note = n[8]![i] ? null : ((string[])c[8])[i];
		if (w > 9) r.Blob = ((byte[][])c[9])[i];
		if (w > 10) r.Big = ((long[])c[10])[i];
		if (w > 11) r.Body = ((string[])c[11])[i];
		if (w > 12) r.Amount = n[12]![i] ? null : ((decimal[])c[12])[i];
		if (w > 13) r.Stamp = n[13]![i] ? null : ((DateTime[])c[13])[i];
		if (w > 14) r.Tag = ((string[])c[14])[i];
		if (w > 15) r.Ratio = ((double[])c[15])[i];
		if (w > 16) r.Flag = n[16]![i] ? null : ((bool[])c[16])[i];
		if (w > 17) r.Link = n[17]![i] ? null : ((Guid[])c[17])[i];
		if (w > 18) r.Count = ((long[])c[18])[i];
		if (w > 19) r.Summary = n[19]![i] ? null : ((string[])c[19])[i];
		return r;
	}

	/// <summary>The members that differ, as text; empty when the two are equal.</summary>
	public static List<string> Diff(SynRow? expected, SynRow? actual)
	{
		var d = new List<string>();
		if (expected is null || actual is null)
		{
			if (!ReferenceEquals(expected, actual)) d.Add($"one row is null (expected {(expected is null ? "null" : "a row")}, actual {(actual is null ? "null" : "a row")})");
			return d;
		}

		Cmp(d, nameof(Id), expected.Id, actual.Id);
		Cmp(d, nameof(Created), expected.Created, actual.Created);
		Cmp(d, nameof(Title), expected.Title, actual.Title);
		Cmp(d, nameof(Price), expected.Price, actual.Price);
		Cmp(d, nameof(Qty), expected.Qty, actual.Qty);
		Cmp(d, nameof(IsActive), expected.IsActive, actual.IsActive);
		Cmp(d, nameof(Key), expected.Key, actual.Key);
		Cmp(d, nameof(Score), expected.Score, actual.Score);
		Cmp(d, nameof(Note), expected.Note, actual.Note);
		Cmp(d, nameof(Blob), expected.Blob, actual.Blob);
		Cmp(d, nameof(Big), expected.Big, actual.Big);
		Cmp(d, nameof(Body), expected.Body, actual.Body);
		Cmp(d, nameof(Amount), expected.Amount, actual.Amount);
		Cmp(d, nameof(Stamp), expected.Stamp, actual.Stamp);
		Cmp(d, nameof(Tag), expected.Tag, actual.Tag);
		Cmp(d, nameof(Ratio), expected.Ratio, actual.Ratio);
		Cmp(d, nameof(Flag), expected.Flag, actual.Flag);
		Cmp(d, nameof(Link), expected.Link, actual.Link);
		Cmp(d, nameof(Count), expected.Count, actual.Count);
		Cmp(d, nameof(Summary), expected.Summary, actual.Summary);
		return d;
	}

	static void Cmp<T>(List<string> d, string name, T expected, T actual)
	{
		if (!Same(expected, actual)) d.Add($"{name}: expected {Show(expected)}, actual {Show(actual)}");
	}

	/// <summary>Equal as values; two byte[] are equal when their contents are.</summary>
	public static bool Same(object? a, object? b)
		=> a is byte[] x && b is byte[] y ? x.AsSpan().SequenceEqual(y) : Equals(a, b);

	static string Show(object? o) => o switch
	{
		null => "null",
		byte[] b => "byte[" + b.Length + "] " + Convert.ToHexString(b),
		string s => "\"" + s + "\"",
		_ => o + " (" + o.GetType().Name + ")",
	};

	/// <summary>The row as <c>GetValues</c> returns it: boxed values, <see cref="DBNull"/> for NULL, the first <paramref name="width"/> columns.</summary>
	public object[] Boxed(int width)
	{
		object[] all =
		[
			Id, Created, Title!, Price, N(Qty), IsActive, Key, Score,
			N(Note), Blob!, Big, Body!,
			N(Amount), N(Stamp), Tag!, Ratio,
			N(Flag), N(Link), Count, N(Summary),
		];
		return all[..width];
	}

	static object N<T>(T? value) where T : struct => value.HasValue ? value.Value : DBNull.Value;
	static object N(string? value) => value is null ? DBNull.Value : value;
}

/// <summary>
/// The variants V0, V0b, V1 and V2 over one synthetic connection. The benchmark class and --verify both call these, so what is
/// checked for equal output is what is measured. Each returns the final objects.
/// </summary>
public static class SyntheticVariants
{
	public const string Sql = "select * from synthetic";

	// ---- V0: dump every row into object[] (GetValues), drain the later result sets as the library does, close ----------------------------------------------------------------
	public static List<object[]> V0(SyntheticConnection connection)
		=> connection.Command(Sql).ExecuteReader(static reader =>
		{
			var rows = new List<object[]>();
			while (reader.Read())
			{
				var row = new object[reader.FieldCount];
				reader.GetValues(row);
				rows.Add(row);
			}

			// The drain the library's default makes before it closes the reader (RowReader.ReadThrough), so a NextResult cost is charged to every variant alike.
			while (reader.NextResult()) { }
			return rows;
		}, CommandBehavior.SequentialAccess);

	public static ValueTask<List<object[]>> V0Async(SyntheticConnection connection)
		=> connection.Command(Sql).ExecuteReaderAsync<List<object[]>>(static async reader =>
		{
			var db = reader;
			var rows = new List<object[]>();
			while (await db.ReadAsync().ConfigureAwait(false))
			{
				var row = new object[db.FieldCount];
				db.GetValues(row);
				rows.Add(row);
			}

			// The same drain as the sync V0 and as the library's default (which calls NextResult, not NextResultAsync, here).
			while (db.NextResult()) { }
			return rows;
		}, CommandBehavior.SequentialAccess);

	// ---- V0b: V0, then convert the object[] rows to typed rows after the close ----------------------------------------------------------------
	public static List<SynRow> V0b(SyntheticConnection connection)
		=> ConvertAll(V0(connection));

	public static async ValueTask<List<SynRow>> V0bAsync(SyntheticConnection connection)
		=> ConvertAll(await V0Async(connection).ConfigureAwait(false));

	static List<SynRow> ConvertAll(List<object[]> rows)
	{
		var list = new List<SynRow>(rows.Count);
		foreach (var row in rows) list.Add(ToRow(row));
		return list;
	}

	// One conversion for every width: a length check guards each member the row may not have. NULL arrives as DBNull.
	static SynRow ToRow(object[] v)
	{
		int n = v.Length;
		var r = new SynRow();
		if (n > 0) r.Id = (int)v[0];
		if (n > 1) r.Created = (DateTime)v[1];
		if (n > 2) r.Title = (string)v[2];
		if (n > 3) r.Price = (decimal)v[3];
		if (n > 4) r.Qty = v[4] is DBNull ? null : (int)v[4];
		if (n > 5) r.IsActive = (bool)v[5];
		if (n > 6) r.Key = (Guid)v[6];
		if (n > 7) r.Score = (double)v[7];
		if (n > 8) r.Note = v[8] is DBNull ? null : (string)v[8];
		if (n > 9) r.Blob = (byte[])v[9];
		if (n > 10) r.Big = (long)v[10];
		if (n > 11) r.Body = (string)v[11];
		if (n > 12) r.Amount = v[12] is DBNull ? null : (decimal)v[12];
		if (n > 13) r.Stamp = v[13] is DBNull ? null : (DateTime)v[13];
		if (n > 14) r.Tag = (string)v[14];
		if (n > 15) r.Ratio = (double)v[15];
		if (n > 16) r.Flag = v[16] is DBNull ? null : (bool)v[16];
		if (n > 17) r.Link = v[17] is DBNull ? null : (Guid)v[17];
		if (n > 18) r.Count = (long)v[18];
		if (n > 19) r.Summary = v[19] is DBNull ? null : (string)v[19];
		return r;
	}

	// ---- V1: Map<T>() with the compiled RowMap plan (today's fast path) ----------------------------------------------------------------
	public static List<SynRow> V1(SyntheticConnection connection)
		=> connection.Command(Sql).Map<SynRow>().ToList();

	/// <summary>Rows pulled with Read() after an async execute (the library's default for async).</summary>
	public static ValueTask<List<SynRow>> V1Async(SyntheticConnection connection)
		=> connection.Command(Sql).Map<SynRow>().ToListAsync();

	/// <summary>Rows pulled with ReadAsync().</summary>
	public static ValueTask<List<SynRow>> V1AsyncRead(SyntheticConnection connection)
		=> connection.Command(Sql).AsyncReads().Map<SynRow>().ToListAsync();

// ---- V2: Map with a hand-written static delegate using the typed getters ----------------------------------------------------------------
	// What a caller writes today with the shipped Map(Func<IDataRecord, T>): a static selector over IDataRecord, as the library exposes it.
	// Not a ceiling (interface calls, the default access mode the library picks for a selector, NULL tests only where the member is nullable);
	// the ceiling on the concrete reader is a separate variant (V9c, BS-C).
	public static List<SynRow> V2(SyntheticConnection connection, Func<IDataRecord, SynRow> selector)
		=> connection.Command(Sql).Map(selector).ToList();

	public static ValueTask<List<SynRow>> V2Async(SyntheticConnection connection, Func<IDataRecord, SynRow> selector)
		=> connection.Command(Sql).Map(selector).ToListAsync();

	static readonly Func<IDataRecord, SynRow> Hand1 = Read1, Hand3 = Read3, Hand5 = Read5, Hand10 = Read10, Hand20 = Read20;

	/// <summary>The hand-written delegate for a catalog width (1, 3, 5, 10 or 20). Static, so a call allocates no delegate.</summary>
	public static Func<IDataRecord, SynRow> Hand(int width) => width switch
	{
		1 => Hand1,
		3 => Hand3,
		5 => Hand5,
		10 => Hand10,
		20 => Hand20,
		_ => throw new ArgumentOutOfRangeException(nameof(width), "Hand-written selectors exist for widths 1, 3, 5, 10 and 20."),
	};

	static SynRow Read1(IDataRecord r) => new()
	{
		Id = r.GetInt32(0),
	};

	static SynRow Read3(IDataRecord r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.GetString(2),
	};

	static SynRow Read5(IDataRecord r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.GetString(2),
		Price = r.GetDecimal(3),
		Qty = r.IsDBNull(4) ? null : r.GetInt32(4),
	};

	static SynRow Read10(IDataRecord r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.GetString(2),
		Price = r.GetDecimal(3),
		Qty = r.IsDBNull(4) ? null : r.GetInt32(4),
		IsActive = r.GetBoolean(5),
		Key = r.GetGuid(6),
		Score = r.GetDouble(7),
		Note = r.IsDBNull(8) ? null : r.GetString(8),
		Blob = (byte[])r.GetValue(9),
	};

	static SynRow Read20(IDataRecord r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.GetString(2),
		Price = r.GetDecimal(3),
		Qty = r.IsDBNull(4) ? null : r.GetInt32(4),
		IsActive = r.GetBoolean(5),
		Key = r.GetGuid(6),
		Score = r.GetDouble(7),
		Note = r.IsDBNull(8) ? null : r.GetString(8),
		Blob = (byte[])r.GetValue(9),
		Big = r.GetInt64(10),
		Body = r.GetString(11),
		Amount = r.IsDBNull(12) ? null : r.GetDecimal(12),
		Stamp = r.IsDBNull(13) ? null : r.GetDateTime(13),
		Tag = r.GetString(14),
		Ratio = r.GetDouble(15),
		Flag = r.IsDBNull(16) ? null : r.GetBoolean(16),
		Link = r.IsDBNull(17) ? null : r.GetGuid(17),
		Count = r.GetInt64(18),
		Summary = r.IsDBNull(19) ? null : r.GetString(19),
	};
}
