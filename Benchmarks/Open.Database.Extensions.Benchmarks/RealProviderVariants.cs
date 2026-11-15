using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

// BS-RP: the variants over a REAL provider (SQL Server LocalDB through SqlClient, a SQLite file through Microsoft.Data.Sqlite). All sync.
// BS-RP ranks the mapping paths; it does not compare sync with async (on both providers ReadAsync completes synchronously, see the IN6 probe).
//
//   V1   Command(sql).Map<SynRow>().ToList()                       the compiled plan
//   V2   Command(sql).Map(Func<IDataRecord,SynRow>).ToList()       the shipped selector recipe (typed getters through IDataRecord)
//   V3   Command(sql).Map<SynRow>().Select(SynItem.From).ToList()  the consumer's pattern: a row object, then an inline Select (two objects per row)
//   V7   Dapper 2.1.89  connection.Query<SynRow>(sql).AsList()
//   V7a  the same call through Dapper.AOT 1.1.0's interceptor (RealProviderDapperAot.cs, the only class with [DapperAot])
//   V9c  the typed-getter ceiling: a hand loop with typed getters on the CONCRETE reader (SqlDataReader or SqliteDataReader), constant ordinals,
//        IsDBNull only on the columns that can be NULL, SequentialAccess, the later result sets read through as the library does. Written once per
//        provider and width (5 and 20) because BS-C's V9c is written against the synthetic reader only.
//   V9c-wrapped  (control, not part of the matrix) the same loop on the timing wrapper's DbDataReader: what the wrapper's forwarding costs.
//   V_best  the slot for the best BS-C candidate. EMPTY until the planning session names it: nothing is wired here and nothing is guessed.
//
// How ReaderOpen is taken (the same interval as the synthetic recorder: from the entry of ExecuteReader to the close of the reader):
//   V1, V2, V3, V7 reach the provider through ODX's or Dapper's own command API, so the connection they get is a TimedConnection (In6Timing.cs): its
//   commands and readers forward every call and stamp the clock. V9c owns its reader, so it takes the two timestamps itself around ExecuteReader and
//   the close, on the REAL reader (a wrapped reader would not be a SqlDataReader any more, and the concrete type is the point of V9c).
//   The wrapper's cost is therefore in V1, V2, V3 and V7 and not in V9c; --verify-rp prints it (V9c against V9c-wrapped).

/// <summary>The variants, over any open connection (the library variants and Dapper) or over the provider's own connection type (V9c).</summary>
public static class RealProviderVariants
{
	public const string Table = "rp", Sql = "select * from rp";

	static readonly Func<SynRow, SynItem> ToItem = static row => SynItem.From(row);

	public static List<SynRow> V1(DbConnection connection) => connection.Command(Sql).Map<SynRow>().ToList();

	public static List<SynRow> V2(DbConnection connection, Func<IDataRecord, SynRow> selector) => connection.Command(Sql).Map(selector).ToList();

	public static List<SynItem> V3(DbConnection connection) => connection.Command(Sql).Map<SynRow>().Select(ToItem).ToList();

	public static List<SynRow> V7(DbConnection connection) => connection.Query<SynRow>(Sql).AsList();

	/// <summary>
	/// Dapper reads a TEXT column into a Guid member only through a type handler (SQLite stores a Guid as text and Dapper does not convert it); this is the standard
	/// recipe for SQLite. <see cref="PrepareDapper"/> registers it for SQLite cases and clears every handler for SQL Server cases, so a SQL Server run never pays for it.
	/// </summary>
	sealed class SqliteGuidHandler : SqlMapper.TypeHandler<Guid>
	{
		public static readonly SqliteGuidHandler Instance = new();

		public override Guid Parse(object value) => value is Guid guid ? guid : Guid.Parse((string)value);

		public override void SetValue(IDbDataParameter parameter, Guid value) => parameter.Value = value.ToString();
	}

	/// <summary>Sets Dapper up for the provider of the case about to run (see <see cref="SqliteGuidHandler"/>).</summary>
	public static void PrepareDapper(string provider)
	{
		SqlMapper.ResetTypeHandlers();
		if (provider == RealProviderParams.Sqlite) SqlMapper.AddTypeHandler(SqliteGuidHandler.Instance);
	}

	// ---- V9c on the concrete reader ----------------------------------------------------------------
	public static List<SynRow> V9cSql(SqlConnection connection, SyntheticRecorder recorder, Func<SqlDataReader, SynRow> read)
	{
		using var command = connection.CreateCommand();
		command.CommandText = Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			if (reader.Read())
			{
				do list.Add(read(reader));
				while (reader.Read());
			}

			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	public static List<SynRow> V9cLite(SqliteConnection connection, SyntheticRecorder recorder, Func<SqliteDataReader, SynRow> read)
	{
		using var command = connection.CreateCommand();
		command.CommandText = Sql;
		var list = new List<SynRow>();
		long start = Stopwatch.GetTimestamp();
		using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
		{
			if (reader.Read())
			{
				do list.Add(read(reader));
				while (reader.Read());
			}

			while (reader.NextResult()) { }
		}

		recorder.ReaderClosed(Stopwatch.GetTimestamp() - start, 0, 0, 0, 0, 0, 0, 0, 0);
		return list;
	}

	/// <summary>The control: the same loop with the DbDataReader getters of the synthetic V9c-Db, through the timing wrapper (the clock is the wrapper's).</summary>
	public static List<SynRow> V9cWrapped(DbConnection timed, Func<DbDataReader, SynRow> read)
		=> timed.Command(Sql).ExecuteReader(read, static (reader, map) =>
		{
			var db = (DbDataReader)reader;
			var list = new List<SynRow>();
			if (db.Read())
			{
				do list.Add(map(db));
				while (db.Read());
			}

			while (db.NextResult()) { }
			return list;
		}, CommandBehavior.SequentialAccess);

	public static Func<SqlDataReader, SynRow> SqlReader(int width) => width switch
	{
		5 => ReadSql5,
		20 => ReadSql20,
		_ => throw new ArgumentOutOfRangeException(nameof(width), "The real-provider hand loops exist for widths 5 and 20."),
	};

	public static Func<SqliteDataReader, SynRow> LiteReader(int width) => width switch
	{
		5 => ReadLite5,
		20 => ReadLite20,
		_ => throw new ArgumentOutOfRangeException(nameof(width), "The real-provider hand loops exist for widths 5 and 20."),
	};

	// SqlDataReader and SqliteDataReader are sealed, so every call below is direct.
	internal static SynRow ReadSql5(SqlDataReader r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.GetString(2),
		Price = r.GetDecimal(3),
		Qty = r.IsDBNull(4) ? null : r.GetInt32(4),
	};

	internal static SynRow ReadSql20(SqlDataReader r) => new()
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

	internal static SynRow ReadLite5(SqliteDataReader r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.GetString(2),
		Price = r.GetDecimal(3),
		Qty = r.IsDBNull(4) ? null : r.GetInt32(4),
	};

	internal static SynRow ReadLite20(SqliteDataReader r) => new()
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
