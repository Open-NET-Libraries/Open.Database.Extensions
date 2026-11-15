using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Benchmarks;

// The row methods and the fused loops of the generated code for SynRow (GeneratorShaped.cs explains the variants). Every method below has the SAME body text;
// only the reader type and the inlining attribute differ (written once, copied by a script into this file; edit the copies together):
//   Read          no attribute: the JIT decides (V10, V10r, V10c)
//   ReadInline    [MethodImpl(AggressiveInlining)] (V10i, V10ri)
//   ReadNoInline  [MethodImpl(NoInlining)]: "called, not inlined" stated, not left to the JIT (V10rn)
//   ReadAll       the whole loop, the mapping INLINE in the loop (V10L): the same body, reading the same fields of the same ordinals struct
public static partial class SynRowGenerated
{
	public static SynRow Read(DbDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadInline(DbDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadNoInline(DbDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	public static SynRow Read(SyntheticReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	public static SynRow Read(SqlDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadInline(SqlDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadNoInline(SqlDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	public static SynRow Read(SqliteDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static SynRow ReadInline(SqliteDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public static SynRow ReadNoInline(SqliteDataReader r, in SynRowOrdinals o)
	{
		var row = new SynRow();
		if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
		if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
		if (o.Title >= 0) row.Title = r.GetString(o.Title);
		if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
		if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
		if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
		if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
		if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
		if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
		if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
		if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
		if (o.Body >= 0) row.Body = r.GetString(o.Body);
		if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
		if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
		if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
		if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
		if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
		if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
		if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
		if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
		return row;
	}

	/// <summary>V10L: the whole read loop of SynRow: the ordinals once before the loop, the mapping inline in the loop (no call boundary), over the DbDataReader.</summary>
	public static List<SynRow> ReadAll(DbDataReader r)
	{
		var o = Resolve(r);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow();
			if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
			if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
			if (o.Title >= 0) row.Title = r.GetString(o.Title);
			if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
			if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
			if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
			if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
			if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
			if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
			if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
			if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
			if (o.Body >= 0) row.Body = r.GetString(o.Body);
			if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
			if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
			if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
			if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
			if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
			if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
			if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
			if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
			list.Add(row);
		}

		return list;
	}

	/// <summary>V10L: the whole read loop of SynRow: the ordinals once before the loop, the mapping inline in the loop (no call boundary), over the SqlDataReader.</summary>
	public static List<SynRow> ReadAll(SqlDataReader r)
	{
		var o = Resolve(r);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow();
			if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
			if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
			if (o.Title >= 0) row.Title = r.GetString(o.Title);
			if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
			if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
			if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
			if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
			if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
			if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
			if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
			if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
			if (o.Body >= 0) row.Body = r.GetString(o.Body);
			if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
			if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
			if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
			if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
			if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
			if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
			if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
			if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
			list.Add(row);
		}

		return list;
	}

	/// <summary>V10L: the whole read loop of SynRow: the ordinals once before the loop, the mapping inline in the loop (no call boundary), over the SqliteDataReader.</summary>
	public static List<SynRow> ReadAll(SqliteDataReader r)
	{
		var o = Resolve(r);
		var list = new List<SynRow>();
		while (r.Read())
		{
			var row = new SynRow();
			if (o.Id >= 0) row.Id = r.GetInt32(o.Id);
			if (o.Created >= 0) row.Created = r.GetDateTime(o.Created);
			if (o.Title >= 0) row.Title = r.GetString(o.Title);
			if (o.Price >= 0) row.Price = r.GetDecimal(o.Price);
			if (o.Qty >= 0) row.Qty = r.IsDBNull(o.Qty) ? null : r.GetInt32(o.Qty);
			if (o.IsActive >= 0) row.IsActive = r.GetBoolean(o.IsActive);
			if (o.Key >= 0) row.Key = r.GetGuid(o.Key);
			if (o.Score >= 0) row.Score = r.GetDouble(o.Score);
			if (o.Note >= 0) row.Note = r.IsDBNull(o.Note) ? null : r.GetString(o.Note);
			if (o.Blob >= 0) row.Blob = (byte[])r.GetValue(o.Blob);
			if (o.Big >= 0) row.Big = r.GetInt64(o.Big);
			if (o.Body >= 0) row.Body = r.GetString(o.Body);
			if (o.Amount >= 0) row.Amount = r.IsDBNull(o.Amount) ? null : r.GetDecimal(o.Amount);
			if (o.Stamp >= 0) row.Stamp = r.IsDBNull(o.Stamp) ? null : r.GetDateTime(o.Stamp);
			if (o.Tag >= 0) row.Tag = r.GetString(o.Tag);
			if (o.Ratio >= 0) row.Ratio = r.GetDouble(o.Ratio);
			if (o.Flag >= 0) row.Flag = r.IsDBNull(o.Flag) ? null : r.GetBoolean(o.Flag);
			if (o.Link >= 0) row.Link = r.IsDBNull(o.Link) ? null : r.GetGuid(o.Link);
			if (o.Count >= 0) row.Count = r.GetInt64(o.Count);
			if (o.Summary >= 0) row.Summary = r.IsDBNull(o.Summary) ? null : r.GetString(o.Summary);
			list.Add(row);
		}

		return list;
	}
}
