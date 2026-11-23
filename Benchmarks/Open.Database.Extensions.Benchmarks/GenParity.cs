using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

// The decomposition of the gap between V2 (the hand-written selector) and the generated reader (Gen), at width 20: each step adds one thing the generated
// reader does and V2 does not, through the same Map(Func<IDataRecord, SynRow>) entry V2 uses (SyntheticVariants.V2).
//   V2n  V2 plus IsDBNull on every member (the NULL rule of the runtime map: a NULL leaves the member's default), constant ordinals: the hand-written
//        selector with the generated reader's former NULL semantics (named GP1 in the first decomposition run; the generator now tests nullable members only).
//   GP2  V2n with the ordinals read from a layout object (resolved once per result set), as the generated reader's mode 1 reads them. THE STEP BUNDLES
//        TWO CHANGES: Read2 also takes the reader as a DbDataReader (a cast, then virtual calls on the abstract class) where V2n calls the IDataRecord
//        interface, so the V2n -> GP2 difference is the ordinal source AND the interface-versus-virtual call, not the layout alone.
//   GP3  GP2 inside try/catch with the error-position stores (s = k) in front of every member.
//   GP4  GP3 behind the binder: the bound-layout check and the read-mode switch of the generated Row method, on an instance delegate.
//   GP5  V2n with each member in its own try region (no position store): a former row shape of the generator (per-member error attribution).
//   GP6  V2's own read (SyntheticVariants.Read20: no NULL test on a non-nullable member, IsDBNull on a nullable one) inside ONE try/catch per row
//        with a cold throw: the row shape the generator emits now (one try region per row reader). Equal to V2 if entering a try region is free.
//   GP6r GP6 plus a row counter: one increment of an int field on a heap object per row, as Bound.Rows is. The counter here is a STATIC object shared by
//        every query and thread and never reset, so it measures the increment only (a static-field load added to the heap-field increment, and a shared
//        cache line if queries ran in parallel); the row a failure reports is cumulative across queries, not the row of its own query as the generated
//        reader's per-binding counter gives. Nothing here checks the reported index.
//   GP7  the raw loop (as V10r): ExecuteReader's callback, while (db.Read()) list.Add(Read(db)), V2's read at constant ordinals on DbDataReader,
//        with ONE try/catch around the whole loop (the row number a loop local). Measurable only: a Map(Func) selector cannot put a try around
//        Core's loop without a Core hook, so the generator does not use this placement. GP7 and GP7c open the reader with CommandBehavior.SequentialAccess
//        (as V10r does), where V2 and every Map(Func) selector run with the default behavior: so GP7 against V2 or GP6 changes the access mode as well as
//        the loop placement. Only GP7 against GP7c isolates the try.
//   GP7c GP7 with no try: the bar of the loop placement (SequentialAccess too).
//
// WIDTH 20 ONLY. V2n, GP2 to GP7c and (SyntheticBenchmarks.cs) Ctl, Ctl_T and Gen_T read the 20 catalog columns at fixed ordinals. At widths 1, 3, 5 and 10
// the provider has fewer columns, so each of these methods throws NotSupportedException from its own width check, and BenchmarkDotNet reports the case as
// failed. Running these: set ODX_SYN_WIDTHS=20 and filter to the methods, as the diagnostic run does:
//   $env:ODX_SYN_WIDTHS = '20'; $env:ODX_SYN_ROWS = '1000'
//   --filter "*SyntheticBenchmarks.V2_Sync" "*.V2n_Sync" "*.Gen_Sync" "*.GenD_Sync" "*.Ctl_Sync" "*.Gen_T_Sync" "*.Ctl_T_Sync" "*.GP5_Sync" "*.GP6_Sync" "*.GP6r_Sync" "*.GP7_Sync" "*.GP7c_Sync"
// (add GP2_Sync, GP3_Sync and GP4_Sync to see the earlier steps). The `--verify` sweep skips them at the other widths on its own.
public static class GenParity
{
	sealed class Layout
	{
		public int O0 = 0, O1 = 1, O2 = 2, O3 = 3, O4 = 4, O5 = 5, O6 = 6, O7 = 7, O8 = 8, O9 = 9;
		public int O10 = 10, O11 = 11, O12 = 12, O13 = 13, O14 = 14, O15 = 15, O16 = 16, O17 = 17, O18 = 18, O19 = 19;
		public string[] Names = new string[20];
		public int Mode = 1;
	}

	static readonly Layout s_layout = new();

	public static readonly Func<IDataRecord, SynRow> V2n = Read1;
	public static readonly Func<IDataRecord, SynRow> GP2 = Read2;
	public static readonly Func<IDataRecord, SynRow> GP3 = Read3;
	public static readonly Func<IDataRecord, SynRow> GP5 = Read5;
	public static readonly Func<IDataRecord, SynRow> GP6 = Read6;
	public static readonly Func<IDataRecord, SynRow> GP6r = Read6r;

	/// <summary>A new instance per query, as the generator's interceptor makes one (the delegate is an instance method).</summary>
	public static Func<IDataRecord, SynRow> GP4() => new Binder().Row;

	static SynRow Read1(IDataRecord r)
	{
		var item = new SynRow();
		if (!r.IsDBNull(0)) item.Id = r.GetInt32(0);
		if (!r.IsDBNull(1)) item.Created = r.GetDateTime(1);
		item.Title = r.IsDBNull(2) ? null : r.GetString(2);
		if (!r.IsDBNull(3)) item.Price = r.GetDecimal(3);
		item.Qty = r.IsDBNull(4) ? null : r.GetInt32(4);
		if (!r.IsDBNull(5)) item.IsActive = r.GetBoolean(5);
		if (!r.IsDBNull(6)) item.Key = r.GetGuid(6);
		if (!r.IsDBNull(7)) item.Score = r.GetDouble(7);
		item.Note = r.IsDBNull(8) ? null : r.GetString(8);
		{ object c = r.GetValue(9); item.Blob = c is null || c is DBNull ? null : (byte[])c; }
		if (!r.IsDBNull(10)) item.Big = r.GetInt64(10);
		item.Body = r.IsDBNull(11) ? null : r.GetString(11);
		item.Amount = r.IsDBNull(12) ? null : r.GetDecimal(12);
		item.Stamp = r.IsDBNull(13) ? null : r.GetDateTime(13);
		item.Tag = r.IsDBNull(14) ? null : r.GetString(14);
		if (!r.IsDBNull(15)) item.Ratio = r.GetDouble(15);
		item.Flag = r.IsDBNull(16) ? null : r.GetBoolean(16);
		item.Link = r.IsDBNull(17) ? null : r.GetGuid(17);
		if (!r.IsDBNull(18)) item.Count = r.GetInt64(18);
		item.Summary = r.IsDBNull(19) ? null : r.GetString(19);
		return item;
	}

	static SynRow Read2(IDataRecord record) => Fill2((DbDataReader)record, s_layout);

	static SynRow Fill2(DbDataReader r, Layout l)
	{
		var item = new SynRow();
		if (!r.IsDBNull(l.O0)) item.Id = r.GetInt32(l.O0);
		if (!r.IsDBNull(l.O1)) item.Created = r.GetDateTime(l.O1);
		item.Title = r.IsDBNull(l.O2) ? null : r.GetString(l.O2);
		if (!r.IsDBNull(l.O3)) item.Price = r.GetDecimal(l.O3);
		item.Qty = r.IsDBNull(l.O4) ? null : r.GetInt32(l.O4);
		if (!r.IsDBNull(l.O5)) item.IsActive = r.GetBoolean(l.O5);
		if (!r.IsDBNull(l.O6)) item.Key = r.GetGuid(l.O6);
		if (!r.IsDBNull(l.O7)) item.Score = r.GetDouble(l.O7);
		item.Note = r.IsDBNull(l.O8) ? null : r.GetString(l.O8);
		{ object c = r.GetValue(l.O9); item.Blob = c is null || c is DBNull ? null : (byte[])c; }
		if (!r.IsDBNull(l.O10)) item.Big = r.GetInt64(l.O10);
		item.Body = r.IsDBNull(l.O11) ? null : r.GetString(l.O11);
		item.Amount = r.IsDBNull(l.O12) ? null : r.GetDecimal(l.O12);
		item.Stamp = r.IsDBNull(l.O13) ? null : r.GetDateTime(l.O13);
		item.Tag = r.IsDBNull(l.O14) ? null : r.GetString(l.O14);
		if (!r.IsDBNull(l.O15)) item.Ratio = r.GetDouble(l.O15);
		item.Flag = r.IsDBNull(l.O16) ? null : r.GetBoolean(l.O16);
		item.Link = r.IsDBNull(l.O17) ? null : r.GetGuid(l.O17);
		if (!r.IsDBNull(l.O18)) item.Count = r.GetInt64(l.O18);
		item.Summary = r.IsDBNull(l.O19) ? null : r.GetString(l.O19);
		return item;
	}

	static SynRow Read3(IDataRecord record) => Fill3((DbDataReader)record, s_layout);

	static SynRow Fill3(DbDataReader r, Layout l)
	{
		int s = -1;
		try
		{
			var item = new SynRow();
			s = 0; if (!r.IsDBNull(l.O0)) item.Id = r.GetInt32(l.O0);
			s = 1; if (!r.IsDBNull(l.O1)) item.Created = r.GetDateTime(l.O1);
			s = 2; item.Title = r.IsDBNull(l.O2) ? null : r.GetString(l.O2);
			s = 3; if (!r.IsDBNull(l.O3)) item.Price = r.GetDecimal(l.O3);
			s = 4; item.Qty = r.IsDBNull(l.O4) ? null : r.GetInt32(l.O4);
			s = 5; if (!r.IsDBNull(l.O5)) item.IsActive = r.GetBoolean(l.O5);
			s = 6; if (!r.IsDBNull(l.O6)) item.Key = r.GetGuid(l.O6);
			s = 7; if (!r.IsDBNull(l.O7)) item.Score = r.GetDouble(l.O7);
			s = 8; item.Note = r.IsDBNull(l.O8) ? null : r.GetString(l.O8);
			s = 9; { object c = r.GetValue(l.O9); item.Blob = c is null || c is DBNull ? null : (byte[])c; }
			s = 10; if (!r.IsDBNull(l.O10)) item.Big = r.GetInt64(l.O10);
			s = 11; item.Body = r.IsDBNull(l.O11) ? null : r.GetString(l.O11);
			s = 12; item.Amount = r.IsDBNull(l.O12) ? null : r.GetDecimal(l.O12);
			s = 13; item.Stamp = r.IsDBNull(l.O13) ? null : r.GetDateTime(l.O13);
			s = 14; item.Tag = r.IsDBNull(l.O14) ? null : r.GetString(l.O14);
			s = 15; if (!r.IsDBNull(l.O15)) item.Ratio = r.GetDouble(l.O15);
			s = 16; item.Flag = r.IsDBNull(l.O16) ? null : r.GetBoolean(l.O16);
			s = 17; item.Link = r.IsDBNull(l.O17) ? null : r.GetGuid(l.O17);
			s = 18; if (!r.IsDBNull(l.O18)) item.Count = r.GetInt64(l.O18);
			s = 19; item.Summary = r.IsDBNull(l.O19) ? null : r.GetString(l.O19);
			return item;
		}
		catch (Exception ex) when (ex is not DbException && ex is not OperationCanceledException)
		{
			throw new InvalidOperationException("member " + s, ex);
		}
	}

	static SynRow Read5(IDataRecord record) => Fill5((DbDataReader)record);

	static SynRow Fill5(DbDataReader r)
	{
		SynRow item;
		try { item = new SynRow(); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, -1); }
		try { if (!r.IsDBNull(0)) item.Id = r.GetInt32(0); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 0); }
		try { if (!r.IsDBNull(1)) item.Created = r.GetDateTime(1); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 1); }
		try { item.Title = r.IsDBNull(2) ? null : r.GetString(2); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 2); }
		try { if (!r.IsDBNull(3)) item.Price = r.GetDecimal(3); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 3); }
		try { item.Qty = r.IsDBNull(4) ? null : r.GetInt32(4); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 4); }
		try { if (!r.IsDBNull(5)) item.IsActive = r.GetBoolean(5); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 5); }
		try { if (!r.IsDBNull(6)) item.Key = r.GetGuid(6); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 6); }
		try { if (!r.IsDBNull(7)) item.Score = r.GetDouble(7); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 7); }
		try { item.Note = r.IsDBNull(8) ? null : r.GetString(8); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 8); }
		try { object c = r.GetValue(9); item.Blob = c is null || c is DBNull ? null : (byte[])c; } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 9); }
		try { if (!r.IsDBNull(10)) item.Big = r.GetInt64(10); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 10); }
		try { item.Body = r.IsDBNull(11) ? null : r.GetString(11); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 11); }
		try { item.Amount = r.IsDBNull(12) ? null : r.GetDecimal(12); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 12); }
		try { item.Stamp = r.IsDBNull(13) ? null : r.GetDateTime(13); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 13); }
		try { item.Tag = r.IsDBNull(14) ? null : r.GetString(14); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 14); }
		try { if (!r.IsDBNull(15)) item.Ratio = r.GetDouble(15); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 15); }
		try { item.Flag = r.IsDBNull(16) ? null : r.GetBoolean(16); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 16); }
		try { item.Link = r.IsDBNull(17) ? null : r.GetGuid(17); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 17); }
		try { if (!r.IsDBNull(18)) item.Count = r.GetInt64(18); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 18); }
		try { item.Summary = r.IsDBNull(19) ? null : r.GetString(19); } catch (Exception ex) when (Wraps(ex)) { throw Fail(ex, 19); }
		return item;
	}

	static SynRow Read6(IDataRecord r)
	{
		try
		{
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
		catch (Exception ex) when (Wraps(ex))
		{
			throw RowFail(ex, -1);
		}
	}

	/// <summary>The row counter of GP6r: a field on a heap object, as Bound.Rows is, but one static instance, shared and never reset (see the header: the increment only).</summary>
	sealed class RowCount
	{
		public int Rows;
	}

	static readonly RowCount s_rows = new();

	static SynRow Read6r(IDataRecord r)
	{
		RowCount b = s_rows;
		b.Rows++;
		try
		{
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
		catch (Exception ex) when (Wraps(ex))
		{
			throw RowFail(ex, b.Rows - 1);
		}
	}

	/// <summary>GP7: the raw loop with one try around it.</summary>
	public static List<SynRow> GP7(DbConnection connection)
		=> connection.Command(SyntheticVariants.Sql).ExecuteReader(static reader =>
		{
			var db = reader;
			var list = new List<SynRow>();
			int row = 0;
			try
			{
				while (db.Read())
				{
					list.Add(Read7(db));
					row++;
				}
			}
			catch (Exception ex) when (Wraps(ex))
			{
				throw RowFail(ex, row);
			}

			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	/// <summary>GP7c: the raw loop of GP7 with no try.</summary>
	public static List<SynRow> GP7c(DbConnection connection)
		=> connection.Command(SyntheticVariants.Sql).ExecuteReader(static reader =>
		{
			var db = reader;
			var list = new List<SynRow>();
			while (db.Read()) list.Add(Read7(db));
			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	// V2's read on DbDataReader (the loop's static type, as V10r's row method takes it).
	static SynRow Read7(DbDataReader r) => new()
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

	// The cold report of GP6, GP6r and GP7 (out of the row method, as the generated Fail is).
	[MethodImpl(MethodImplOptions.NoInlining)]
	static InvalidOperationException RowFail(Exception ex, int row)
		=> new("Unable to map row " + row + ".", ex);

	static bool Wraps(Exception ex) => ex is not DbException && ex is not OperationCanceledException;

	static InvalidOperationException Fail(Exception ex, int member) => new("member " + member, ex);

	sealed class Bound(IDataRecord record, DbDataReader reader, Layout layout)
	{
		public readonly IDataRecord Record = record;
		public readonly DbDataReader Reader = reader;
		public readonly Layout Layout = layout;
	}

	sealed class Binder
	{
		Bound? _bound;

		public SynRow Row(IDataRecord record)
		{
			Bound? b = _bound;
			if (b is null || !ReferenceEquals(b.Record, record) || record.FieldCount != b.Layout.Names.Length) _bound = b = new Bound(record, (DbDataReader)record, s_layout);
			switch (b.Layout.Mode)
			{
				case 1: return Fill3(b.Reader, b.Layout);
				default: throw new InvalidOperationException();
			}
		}
	}
}
