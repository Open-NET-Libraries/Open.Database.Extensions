using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// The hand-written twins of the NULL-policy rungs (SynRowPolicy.cs), the bars the runtime rungs V1_Throw_Sync and V1_Default_Sync are measured against.
/// They build the SAME row types as the runtime twins, over the same typed getters as V2 (<see cref="SyntheticVariants.Hand"/>), so the only difference from V2
/// is the NULL test and what a NULL does. They read the 20 catalog columns at fixed ordinals: width 20 only (WidthTwenty in SyntheticBenchmarks).
/// </summary>
public static class SyntheticVariantsNull
{
	public static readonly Func<IDataRecord, SynRowThrow> ThrowSelector = ReadThrow;
	public static readonly Func<IDataRecord, SynRowDefault> DefaultSelector = ReadDefault;

	// Out of line and never inlined, so the check is a compare and a not-taken branch per member and the exception costs nothing unless a NULL appears.
	[DoesNotReturn]
	[MethodImpl(MethodImplOptions.NoInlining)]
	static void Null(string member, string column)
		=> throw new ValueMappingException($"{member}: column {column} is NULL.", member, column, typeof(SynRowThrow));

	// Throw on every non-nullable member: each of the 13 (Id, Created, Title, Price, IsActive, Key, Score, Blob, Big, Body, Tag, Ratio, Count) is tested for NULL
	// before it is read, and a NULL throws; the 7 nullable members are read as V2 reads them (the NULL test is the member's own, null stays null).
	static SynRowThrow ReadThrow(IDataRecord r)
	{
		if (r.IsDBNull(0)) Null(nameof(SynRowThrow.Id), "Id");
		if (r.IsDBNull(1)) Null(nameof(SynRowThrow.Created), "Created");
		if (r.IsDBNull(2)) Null(nameof(SynRowThrow.Title), "Title");
		if (r.IsDBNull(3)) Null(nameof(SynRowThrow.Price), "Price");
		if (r.IsDBNull(5)) Null(nameof(SynRowThrow.IsActive), "IsActive");
		if (r.IsDBNull(6)) Null(nameof(SynRowThrow.Key), "Key");
		if (r.IsDBNull(7)) Null(nameof(SynRowThrow.Score), "Score");
		if (r.IsDBNull(9)) Null(nameof(SynRowThrow.Blob), "Blob");
		if (r.IsDBNull(10)) Null(nameof(SynRowThrow.Big), "Big");
		if (r.IsDBNull(11)) Null(nameof(SynRowThrow.Body), "Body");
		if (r.IsDBNull(14)) Null(nameof(SynRowThrow.Tag), "Tag");
		if (r.IsDBNull(15)) Null(nameof(SynRowThrow.Ratio), "Ratio");
		if (r.IsDBNull(18)) Null(nameof(SynRowThrow.Count), "Count");
		return new()
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

	// Default on every member: all 20 are tested for NULL, and a NULL leaves the member as the new row made it (its default). A nullable member then stays null.
	static SynRowDefault ReadDefault(IDataRecord r)
	{
		var x = new SynRowDefault();
		if (!r.IsDBNull(0)) x.Id = r.GetInt32(0);
		if (!r.IsDBNull(1)) x.Created = r.GetDateTime(1);
		if (!r.IsDBNull(2)) x.Title = r.GetString(2);
		if (!r.IsDBNull(3)) x.Price = r.GetDecimal(3);
		if (!r.IsDBNull(4)) x.Qty = r.GetInt32(4);
		if (!r.IsDBNull(5)) x.IsActive = r.GetBoolean(5);
		if (!r.IsDBNull(6)) x.Key = r.GetGuid(6);
		if (!r.IsDBNull(7)) x.Score = r.GetDouble(7);
		if (!r.IsDBNull(8)) x.Note = r.GetString(8);
		if (!r.IsDBNull(9)) x.Blob = (byte[])r.GetValue(9);
		if (!r.IsDBNull(10)) x.Big = r.GetInt64(10);
		if (!r.IsDBNull(11)) x.Body = r.GetString(11);
		if (!r.IsDBNull(12)) x.Amount = r.GetDecimal(12);
		if (!r.IsDBNull(13)) x.Stamp = r.GetDateTime(13);
		if (!r.IsDBNull(14)) x.Tag = r.GetString(14);
		if (!r.IsDBNull(15)) x.Ratio = r.GetDouble(15);
		if (!r.IsDBNull(16)) x.Flag = r.GetBoolean(16);
		if (!r.IsDBNull(17)) x.Link = r.GetGuid(17);
		if (!r.IsDBNull(18)) x.Count = r.GetInt64(18);
		if (!r.IsDBNull(19)) x.Summary = r.GetString(19);
		return x;
	}

	public static List<SynRowThrow> V2Throw(SyntheticConnection connection)
		=> connection.Command(SyntheticVariants.Sql).Map(ThrowSelector).ToList();

	public static List<SynRowDefault> V2Default(SyntheticConnection connection)
		=> connection.Command(SyntheticVariants.Sql).Map(DefaultSelector).ToList();
}
