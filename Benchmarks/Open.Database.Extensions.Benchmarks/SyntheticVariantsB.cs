using System.Collections.Concurrent;
using System.Data;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// The domain object of V3 to V6: the same 20 members as <see cref="SynRow"/>, a different class, so the row-to-domain step allocates a second object per row
/// (the consumer's pattern). A copy is a plain member-by-member assignment for every width (width 1 pays the same 20 assignments).
/// </summary>
public sealed class SynItem
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

	/// <summary>The row-to-domain conversion every V3 to V6 variant uses (V6 then massages the result).</summary>
	public static SynItem From(SynRow r) => new()
	{
		Id = r.Id,
		Created = r.Created,
		Title = r.Title,
		Price = r.Price,
		Qty = r.Qty,
		IsActive = r.IsActive,
		Key = r.Key,
		Score = r.Score,
		Note = r.Note,
		Blob = r.Blob,
		Big = r.Big,
		Body = r.Body,
		Amount = r.Amount,
		Stamp = r.Stamp,
		Tag = r.Tag,
		Ratio = r.Ratio,
		Flag = r.Flag,
		Link = r.Link,
		Count = r.Count,
		Summary = r.Summary,
	};

	public SynRow ToRow() => new()
	{
		Id = Id,
		Created = Created,
		Title = Title,
		Price = Price,
		Qty = Qty,
		IsActive = IsActive,
		Key = Key,
		Score = Score,
		Note = Note,
		Blob = Blob,
		Big = Big,
		Body = Body,
		Amount = Amount,
		Stamp = Stamp,
		Tag = Tag,
		Ratio = Ratio,
		Flag = Flag,
		Link = Link,
		Count = Count,
		Summary = Summary,
	};

	/// <summary>The members that differ, as text; empty when the two are equal.</summary>
	public static List<string> Diff(SynItem? expected, SynItem? actual)
		=> SynRow.Diff(expected?.ToRow(), actual?.ToRow());

	/// <summary>
	/// The V6 massage, on the domain object, for a catalog width (rules gated by which columns exist):
	/// R1 an if check on Id; R2 (width 3 and up) a trim and concat; R3 (width 5 and up) a member computed from two columns;
	/// R4 (width 9 and up) a null check. V6a, V6c and V6d all do exactly this; the oracle runs it on the table's own values.
	/// </summary>
	public static SynItem Massage(SynItem x, int width)
	{
		if (x.Id % 10 == 0) x.Id = -x.Id;
		if (width >= 3) x.Title = x.Title!.Trim() + "|" + x.Id;
		if (width >= 5) x.Price = (x.Price * 2) + (x.Qty ?? 0);
		if (width >= 9 && x.Note is null) x.Note = "-";
		return x;
	}
}

/// <summary>
/// Variants V3 to V6 and the scalar variants V8 over one synthetic connection (V0 to V2 are in <see cref="SyntheticVariants"/>).
/// The benchmark class and --verify call these, so what is verified is what is measured. Consumer pace (a busy-wait of <c>pace</c>
/// iterations per item, 0 = off) is paid only by the variants that run caller code while the reader is open or stream:
/// V3, V4a, V4b, V6c, V3s. V5 and V6d do their work after the close and ignore it by design.
/// </summary>
public static class SyntheticVariantsB
{
	const string Sql = SyntheticVariants.Sql;

	// ---- the conversions ----------------------------------------------------------------
	static readonly Func<SynRow, SynItem> ToItem = static row => SynItem.From(row);

	// The row-to-domain selector run inside the open reader: the static delegate when there is no pace; a closure (one allocation per query) otherwise.
	static Func<SynRow, SynItem> Inline(int pace)
		=> pace <= 0 ? ToItem : row =>
		{
			Spin.Iterations(pace);
			return SynItem.From(row);
		};

	static Func<SynRow, SynItem> InlineMassaged(int width, int pace)
		=> row =>
		{
			if (pace > 0) Spin.Iterations(pace);
			return SynItem.Massage(SynItem.From(row), width);
		};

	// ---- the Select replacement (route a of the A2 migration line) ----------------------------------------------------------------
	// IMappedQuery<T>.Select is removed. A consumer who shaped each row with a Select now gives Map a factory map: the row is built straight into the result
	// type, with ONE object per row and no intermediate row object. The factory runs inside the read loop, like the Select selector did (so it pays the pace).
	// A consumer holds the map in a static field, so the maps are cached here (per pace, and per width for the massaged form) and the plan is built once.
	// Every parameter has a default: at a narrow catalog width the missing columns keep the member's default, as a SynRow member did.
	static readonly ConcurrentDictionary<long, RowMap<SynItem>> ItemMaps = new();

	/// <summary>The factory map to <see cref="SynItem"/>: the V3 shape (the conversion runs while the reader is open, one object per row).</summary>
	public static RowMap<SynItem> ItemMap(int pace) => ItemMaps.GetOrAdd(pace, static key => BuildItemMap((int)key, 0, false));

	/// <summary>The same with the V6 massage applied for <paramref name="width"/> (the V6c shape).</summary>
	public static RowMap<SynItem> MassagedItemMap(int width, int pace)
		=> ItemMaps.GetOrAdd(((long)(width + 1) << 32) | (uint)pace, static key => BuildItemMap((int)(uint)key, (int)(key >> 32) - 1, true));

	static RowMap<SynItem> BuildItemMap(int pace, int width, bool massage)
		=> RowMap<SynItem>.From((
			int id = 0,
			DateTime created = default,
			string? title = null,
			decimal price = 0,
			int? qty = null,
			bool isActive = false,
			Guid key = default,
			double score = 0,
			string? note = null,
			byte[]? blob = null,
			long big = 0,
			string? body = null,
			decimal? amount = null,
			DateTime? stamp = null,
			string? tag = null,
			double ratio = 0,
			bool? flag = null,
			Guid? link = null,
			long count = 0,
			string? summary = null) =>
		{
			if (pace > 0) Spin.Iterations(pace);
			var item = new SynItem
			{
				Id = id,
				Created = created,
				Title = title,
				Price = price,
				Qty = qty,
				IsActive = isActive,
				Key = key,
				Score = score,
				Note = note,
				Blob = blob,
				Big = big,
				Body = body,
				Amount = amount,
				Stamp = stamp,
				Tag = tag,
				Ratio = ratio,
				Flag = flag,
				Link = link,
				Count = count,
				Summary = summary,
			};
			return massage ? SynItem.Massage(item, width) : item;
		});

	// ---- V3: a factory map straight to the domain object, built while the reader is open (the Select migration target; one object per row) ----------------------------------------------------------------
	public static List<SynItem> V3(SyntheticConnection connection, int pace)
		=> connection.Command(Sql).Map(ItemMap(pace)).ToList();

	public static ValueTask<List<SynItem>> V3Async(SyntheticConnection connection, int pace)
		=> connection.Command(Sql).Map(ItemMap(pace)).ToListAsync();

	/// <summary>V3s: the same factory map streamed through the library's IAsyncEnumerable terminal; the pace is paid in the consumer's loop.</summary>
	public static ValueTask<List<SynItem>> V3s(SyntheticConnection connection, int pace)
		=> connection.Command(Sql).Map(ItemMap(0)).ReadAsync<SynItem, List<SynItem>>(async rows =>
		{
			var list = new List<SynItem>();
			await foreach (var item in rows.ConfigureAwait(false))
			{
				if (pace > 0) Spin.Iterations(pace);
				list.Add(item);
			}

			return list;
		});

	// ---- V4: the intermediate row reused or pooled ----------------------------------------------------------------
	// Map<Row>() always allocates a new Row (there is no overload that fills a given instance), so the reuse is built where a caller can build it:
	// the Map(Func<IDataRecord, T>) selector fills a held instance and returns it, and the conversion consumes it in the same selector, before the next row is read (the Select migration target for Map(selector).Select(f): Map(record => f(selector(record)))).
	sealed class Reuse(Action<IDataRecord, SynRow> fill)
	{
		readonly SynRow _row = new();

		public SynRow Next(IDataRecord record)
		{
			fill(record, _row);
			return _row;
		}
	}

	/// <summary>A pool of a few rows, one thread, no locking (the read is sequential).</summary>
	sealed class RowPool
	{
		readonly SynRow[] _items = new SynRow[4];
		int _count;

		public SynRow Rent() => _count > 0 ? _items[--_count] : new SynRow();

		public void Return(SynRow row)
		{
			if (_count < _items.Length) _items[_count++] = row;
		}
	}

	sealed class Pooled(Action<IDataRecord, SynRow> fill, RowPool pool)
	{
		public SynRow Next(IDataRecord record)
		{
			var row = pool.Rent();
			fill(record, row);
			return row;
		}
	}

	/// <summary>V4a: one reused instance, filled for every row.</summary>
	public static List<SynItem> V4a(SyntheticConnection connection, int width, int pace)
	{
		Func<IDataRecord, SynRow> selector = new Reuse(Filler(width)).Next;
		var inline = Inline(pace);
		return connection.Command(Sql).Map(record => inline(selector(record))).ToList();
	}

	public static ValueTask<List<SynItem>> V4aAsync(SyntheticConnection connection, int width, int pace)
	{
		Func<IDataRecord, SynRow> selector = new Reuse(Filler(width)).Next;
		var inline = Inline(pace);
		return connection.Command(Sql).Map(record => inline(selector(record))).ToListAsync();
	}

	/// <summary>V4b: a small pool; the conversion returns the row to the pool once it has converted it.</summary>
	public static List<SynItem> V4b(SyntheticConnection connection, int width, int pace)
	{
		var pool = new RowPool();
		Func<IDataRecord, SynRow> selector = new Pooled(Filler(width), pool).Next;
		var returning = Returning(pool, pace);
		return connection.Command(Sql).Map(record => returning(selector(record))).ToList();
	}

	public static ValueTask<List<SynItem>> V4bAsync(SyntheticConnection connection, int width, int pace)
	{
		var pool = new RowPool();
		Func<IDataRecord, SynRow> selector = new Pooled(Filler(width), pool).Next;
		var returning = Returning(pool, pace);
		return connection.Command(Sql).Map(record => returning(selector(record))).ToListAsync();
	}

	static Func<SynRow, SynItem> Returning(RowPool pool, int pace)
		=> row =>
		{
			if (pace > 0) Spin.Iterations(pace);
			var item = SynItem.From(row);
			pool.Return(row);
			return item;
		};

	// ---- V5: Map<Row>() buffered to completion (the reader closes), then Select after the close ----------------------------------------------------------------
	public static List<SynItem> V5(SyntheticConnection connection)
		=> connection.Command(Sql).Map<SynRow>().ToList().Select(ToItem).ToList();

	public static async ValueTask<List<SynItem>> V5Async(SyntheticConnection connection)
		=> (await connection.Command(Sql).Map<SynRow>().ToListAsync().ConfigureAwait(false)).Select(ToItem).ToList();

	// ---- V6: massaging (R1 to R4, see SynItem.Massage) ----------------------------------------------------------------
	/// <summary>V6a: inside the hand-written Map delegate (the V2 selector form), which produces the domain object directly.</summary>
	public static List<SynItem> V6a(SyntheticConnection connection, Func<IDataRecord, SynItem> selector)
		=> connection.Command(Sql).Map(selector).ToList();

	public static ValueTask<List<SynItem>> V6aAsync(SyntheticConnection connection, Func<IDataRecord, SynItem> selector)
		=> connection.Command(Sql).Map(selector).ToListAsync();

	// V6b: RESERVED for DS1a (the spike's candidate). Intentionally empty: no code until DS1a delivers it.

	/// <summary>V6c: the massage inside the factory map, while the reader is open (the V3 shape).</summary>
	public static List<SynItem> V6c(SyntheticConnection connection, int width, int pace)
		=> connection.Command(Sql).Map(MassagedItemMap(width, pace)).ToList();

	public static ValueTask<List<SynItem>> V6cAsync(SyntheticConnection connection, int width, int pace)
		=> connection.Command(Sql).Map(MassagedItemMap(width, pace)).ToListAsync();

	/// <summary>V6d: after the close (the V5 shape).</summary>
	public static List<SynItem> V6d(SyntheticConnection connection, int width)
		=> connection.Command(Sql).Map<SynRow>().ToList().Select(row => SynItem.Massage(SynItem.From(row), width)).ToList();

	public static async ValueTask<List<SynItem>> V6dAsync(SyntheticConnection connection, int width)
		=> (await connection.Command(Sql).Map<SynRow>().ToListAsync().ConfigureAwait(false)).Select(row => SynItem.Massage(SynItem.From(row), width)).ToList();

	// ---- V8: scalars (a width-1, 1-row table: one int) ----------------------------------------------------------------
	/// <summary>V8a: the typed ExecuteScalar&lt;T&gt;.</summary>
	public static int V8a(SyntheticConnection connection) => connection.Command(Sql).ExecuteScalar<int>();

	public static ValueTask<int> V8aAsync(SyntheticConnection connection) => connection.Command(Sql).ExecuteScalarAsync<int>();

	/// <summary>V8b: the object path: the provider's value as object (its GetValue(0)), then a plain conversion.</summary>
	public static int V8b(SyntheticConnection connection) => Convert.ToInt32(connection.Command(Sql).ExecuteScalar());

	public static async ValueTask<int> V8bAsync(SyntheticConnection connection) => Convert.ToInt32(await connection.Command(Sql).ExecuteScalarAsync().ConfigureAwait(false));

	/// <summary>V8c: Map&lt;int&gt;() reads the first column through the reader; First stops at the first row.</summary>
	public static int V8c(SyntheticConnection connection) => connection.Command(Sql).Map<int>().First();

	public static ValueTask<int> V8cAsync(SyntheticConnection connection) => connection.Command(Sql).Map<int>().FirstAsync();

	// ---- the hand-written delegates ----------------------------------------------------------------
	static readonly Func<IDataRecord, SynItem> M1 = Read1M, M3 = Read3M, M5 = Read5M, M10 = Read10M, M20 = Read20M;

	/// <summary>The hand-written V6a delegate (typed getters through IDataRecord, the massage inline) for a catalog width.</summary>
	public static Func<IDataRecord, SynItem> HandMassaged(int width) => width switch
	{
		1 => M1,
		3 => M3,
		5 => M5,
		10 => M10,
		20 => M20,
		_ => throw new ArgumentOutOfRangeException(nameof(width), "Hand-written selectors exist for widths 1, 3, 5, 10 and 20."),
	};

	static SynItem Read1M(IDataRecord r)
	{
		int id = r.GetInt32(0);
		if (id % 10 == 0) id = -id;
		return new SynItem { Id = id };
	}

	static SynItem Read3M(IDataRecord r)
	{
		int id = r.GetInt32(0);
		if (id % 10 == 0) id = -id;
		var created = r.GetDateTime(1);
		string title = r.GetString(2).Trim() + "|" + id;
		return new SynItem { Id = id, Created = created, Title = title };
	}

	static SynItem Read5M(IDataRecord r)
	{
		int id = r.GetInt32(0);
		if (id % 10 == 0) id = -id;
		var created = r.GetDateTime(1);
		string title = r.GetString(2).Trim() + "|" + id;
		decimal price = r.GetDecimal(3);
		int? qty = r.IsDBNull(4) ? null : r.GetInt32(4);
		return new SynItem { Id = id, Created = created, Title = title, Price = (price * 2) + (qty ?? 0), Qty = qty };
	}

	static SynItem Read10M(IDataRecord r)
	{
		int id = r.GetInt32(0);
		if (id % 10 == 0) id = -id;
		var created = r.GetDateTime(1);
		string title = r.GetString(2).Trim() + "|" + id;
		decimal price = r.GetDecimal(3);
		int? qty = r.IsDBNull(4) ? null : r.GetInt32(4);
		var isActive = r.GetBoolean(5);
		var key = r.GetGuid(6);
		var score = r.GetDouble(7);
		string? note = r.IsDBNull(8) ? null : r.GetString(8);
		note ??= "-";
		return new SynItem
		{
			Id = id,
			Created = created,
			Title = title,
			Price = (price * 2) + (qty ?? 0),
			Qty = qty,
			IsActive = isActive,
			Key = key,
			Score = score,
			Note = note,
			Blob = (byte[])r.GetValue(9),
		};
	}

	static SynItem Read20M(IDataRecord r)
	{
		int id = r.GetInt32(0);
		if (id % 10 == 0) id = -id;
		var created = r.GetDateTime(1);
		string title = r.GetString(2).Trim() + "|" + id;
		decimal price = r.GetDecimal(3);
		int? qty = r.IsDBNull(4) ? null : r.GetInt32(4);
		var isActive = r.GetBoolean(5);
		var key = r.GetGuid(6);
		var score = r.GetDouble(7);
		string? note = r.IsDBNull(8) ? null : r.GetString(8);
		note ??= "-";
		return new SynItem
		{
			Id = id,
			Created = created,
			Title = title,
			Price = (price * 2) + (qty ?? 0),
			Qty = qty,
			IsActive = isActive,
			Key = key,
			Score = score,
			Note = note,
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

	// The fillers of V4: the V2 hand-written reads, written into a given row. Every member up to the width is assigned (NULL as null),
	// so a reused row holds nothing of the one before; the members past the width are never touched and stay at their defaults.
	static readonly Action<IDataRecord, SynRow> F1 = Fill1, F3 = Fill3, F5 = Fill5, F10 = Fill10, F20 = Fill20;

	static Action<IDataRecord, SynRow> Filler(int width) => width switch
	{
		1 => F1,
		3 => F3,
		5 => F5,
		10 => F10,
		20 => F20,
		_ => throw new ArgumentOutOfRangeException(nameof(width), "Hand-written selectors exist for widths 1, 3, 5, 10 and 20."),
	};

	static void Fill1(IDataRecord r, SynRow x)
		=> x.Id = r.GetInt32(0);

	static void Fill3(IDataRecord r, SynRow x)
	{
		x.Id = r.GetInt32(0);
		x.Created = r.GetDateTime(1);
		x.Title = r.GetString(2);
	}

	static void Fill5(IDataRecord r, SynRow x)
	{
		x.Id = r.GetInt32(0);
		x.Created = r.GetDateTime(1);
		x.Title = r.GetString(2);
		x.Price = r.GetDecimal(3);
		x.Qty = r.IsDBNull(4) ? null : r.GetInt32(4);
	}

	static void Fill10(IDataRecord r, SynRow x)
	{
		x.Id = r.GetInt32(0);
		x.Created = r.GetDateTime(1);
		x.Title = r.GetString(2);
		x.Price = r.GetDecimal(3);
		x.Qty = r.IsDBNull(4) ? null : r.GetInt32(4);
		x.IsActive = r.GetBoolean(5);
		x.Key = r.GetGuid(6);
		x.Score = r.GetDouble(7);
		x.Note = r.IsDBNull(8) ? null : r.GetString(8);
		x.Blob = (byte[])r.GetValue(9);
	}

	static void Fill20(IDataRecord r, SynRow x)
	{
		x.Id = r.GetInt32(0);
		x.Created = r.GetDateTime(1);
		x.Title = r.GetString(2);
		x.Price = r.GetDecimal(3);
		x.Qty = r.IsDBNull(4) ? null : r.GetInt32(4);
		x.IsActive = r.GetBoolean(5);
		x.Key = r.GetGuid(6);
		x.Score = r.GetDouble(7);
		x.Note = r.IsDBNull(8) ? null : r.GetString(8);
		x.Blob = (byte[])r.GetValue(9);
		x.Big = r.GetInt64(10);
		x.Body = r.GetString(11);
		x.Amount = r.IsDBNull(12) ? null : r.GetDecimal(12);
		x.Stamp = r.IsDBNull(13) ? null : r.GetDateTime(13);
		x.Tag = r.GetString(14);
		x.Ratio = r.GetDouble(15);
		x.Flag = r.IsDBNull(16) ? null : r.GetBoolean(16);
		x.Link = r.IsDBNull(17) ? null : r.GetGuid(17);
		x.Count = r.GetInt64(18);
		x.Summary = r.IsDBNull(19) ? null : r.GetString(19);
	}
}
