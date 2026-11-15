using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Benchmarks;

// V10k, V10kri, V10kL (GeneratorShaped.cs explains the variants): what a generator emits when every member of the type is REQUIRED. Generated text: one block per width; edit the copies together.
// The ordinals are resolved by name once per result set (the same resolver as V10), a missing column throws at resolve time (ResolveRequired), and there is NO presence branch per row.
// A catalog width w is "a SynRow whose first w members are required" (the later members are not mapped, as V2's Hand(w) does not map them), so one set of methods exists per width: 1, 3, 5, 10, 20.
//   ReadKn<w>  [NoInlining] row method          (V10k, called like V10rn)
//   ReadKi<w>  [AggressiveInlining] row method  (V10kri, inlined like V10ri)
//   ReadAllK<w> the whole loop, the mapping inline (V10kL, fused like V10L)
public static partial class SynRowGenerated
{
	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadKn1(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
		};

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadKi1(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
		};

	public static List<SynRow> LoopKn1(DbDataReader r)
	{
		var o = ResolveRequired(r, 1);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKn1(r, in o));
		return list;
	}

	public static List<SynRow> LoopKi1(DbDataReader r)
	{
		var o = ResolveRequired(r, 1);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKi1(r, in o));
		return list;
	}

	public static List<SynRow> ReadAllK1(DbDataReader r)
	{
		var o = ResolveRequired(r, 1);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow
			{
				Id = r.GetInt32(o.Id),
			};
			list.Add(row);
		}

		return list;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadKn3(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
		};

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadKi3(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
		};

	public static List<SynRow> LoopKn3(DbDataReader r)
	{
		var o = ResolveRequired(r, 3);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKn3(r, in o));
		return list;
	}

	public static List<SynRow> LoopKi3(DbDataReader r)
	{
		var o = ResolveRequired(r, 3);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKi3(r, in o));
		return list;
	}

	public static List<SynRow> ReadAllK3(DbDataReader r)
	{
		var o = ResolveRequired(r, 3);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow
			{
				Id = r.GetInt32(o.Id),
				Created = r.GetDateTime(o.Created),
				Title = r.GetString(o.Title),
			};
			list.Add(row);
		}

		return list;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadKn5(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
			Price = r.GetDecimal(o.Price),
			Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
		};

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadKi5(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
			Price = r.GetDecimal(o.Price),
			Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
		};

	public static List<SynRow> LoopKn5(DbDataReader r)
	{
		var o = ResolveRequired(r, 5);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKn5(r, in o));
		return list;
	}

	public static List<SynRow> LoopKi5(DbDataReader r)
	{
		var o = ResolveRequired(r, 5);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKi5(r, in o));
		return list;
	}

	public static List<SynRow> ReadAllK5(DbDataReader r)
	{
		var o = ResolveRequired(r, 5);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow
			{
				Id = r.GetInt32(o.Id),
				Created = r.GetDateTime(o.Created),
				Title = r.GetString(o.Title),
				Price = r.GetDecimal(o.Price),
				Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
			};
			list.Add(row);
		}

		return list;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadKn10(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
			Price = r.GetDecimal(o.Price),
			Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
			IsActive = r.GetBoolean(o.IsActive),
			Key = r.GetGuid(o.Key),
			Score = r.GetDouble(o.Score),
			Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note),
			Blob = (byte[])r.GetValue(o.Blob),
		};

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadKi10(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
			Price = r.GetDecimal(o.Price),
			Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
			IsActive = r.GetBoolean(o.IsActive),
			Key = r.GetGuid(o.Key),
			Score = r.GetDouble(o.Score),
			Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note),
			Blob = (byte[])r.GetValue(o.Blob),
		};

	public static List<SynRow> LoopKn10(DbDataReader r)
	{
		var o = ResolveRequired(r, 10);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKn10(r, in o));
		return list;
	}

	public static List<SynRow> LoopKi10(DbDataReader r)
	{
		var o = ResolveRequired(r, 10);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKi10(r, in o));
		return list;
	}

	public static List<SynRow> ReadAllK10(DbDataReader r)
	{
		var o = ResolveRequired(r, 10);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow
			{
				Id = r.GetInt32(o.Id),
				Created = r.GetDateTime(o.Created),
				Title = r.GetString(o.Title),
				Price = r.GetDecimal(o.Price),
				Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
				IsActive = r.GetBoolean(o.IsActive),
				Key = r.GetGuid(o.Key),
				Score = r.GetDouble(o.Score),
				Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note),
				Blob = (byte[])r.GetValue(o.Blob),
			};
			list.Add(row);
		}

		return list;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadKn20(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
			Price = r.GetDecimal(o.Price),
			Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
			IsActive = r.GetBoolean(o.IsActive),
			Key = r.GetGuid(o.Key),
			Score = r.GetDouble(o.Score),
			Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note),
			Blob = (byte[])r.GetValue(o.Blob),
			Big = r.GetInt64(o.Big),
			Body = r.GetString(o.Body),
			Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount),
			Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp),
			Tag = r.GetString(o.Tag),
			Ratio = r.GetDouble(o.Ratio),
			Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag),
			Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link),
			Count = r.GetInt64(o.Count),
			Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary),
		};

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadKi20(DbDataReader r, in SynRowOrdinals o)
		=> new()
		{
			Id = r.GetInt32(o.Id),
			Created = r.GetDateTime(o.Created),
			Title = r.GetString(o.Title),
			Price = r.GetDecimal(o.Price),
			Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
			IsActive = r.GetBoolean(o.IsActive),
			Key = r.GetGuid(o.Key),
			Score = r.GetDouble(o.Score),
			Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note),
			Blob = (byte[])r.GetValue(o.Blob),
			Big = r.GetInt64(o.Big),
			Body = r.GetString(o.Body),
			Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount),
			Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp),
			Tag = r.GetString(o.Tag),
			Ratio = r.GetDouble(o.Ratio),
			Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag),
			Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link),
			Count = r.GetInt64(o.Count),
			Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary),
		};

	public static List<SynRow> LoopKn20(DbDataReader r)
	{
		var o = ResolveRequired(r, 20);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKn20(r, in o));
		return list;
	}

	public static List<SynRow> LoopKi20(DbDataReader r)
	{
		var o = ResolveRequired(r, 20);
		var list = new List<SynRow>();
		while (r.Read()) list.Add(ReadKi20(r, in o));
		return list;
	}

	public static List<SynRow> ReadAllK20(DbDataReader r)
	{
		var o = ResolveRequired(r, 20);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow
			{
				Id = r.GetInt32(o.Id),
				Created = r.GetDateTime(o.Created),
				Title = r.GetString(o.Title),
				Price = r.GetDecimal(o.Price),
				Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty),
				IsActive = r.GetBoolean(o.IsActive),
				Key = r.GetGuid(o.Key),
				Score = r.GetDouble(o.Score),
				Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note),
				Blob = (byte[])r.GetValue(o.Blob),
				Big = r.GetInt64(o.Big),
				Body = r.GetString(o.Body),
				Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount),
				Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp),
				Tag = r.GetString(o.Tag),
				Ratio = r.GetDouble(o.Ratio),
				Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag),
				Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link),
				Count = r.GetInt64(o.Count),
				Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary),
			};
			list.Add(row);
		}

		return list;
	}
}
