using System;
using System.Data;
using System.Globalization;
using System.Linq;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// AddParam("@id", id, SqlDbType.Int): a value and a database type. Every call in this file must COMPILE at the language version of its target (7.3, 9, 12),
	// so the build of this project is the guard: a call that becomes ambiguous (CS0121) or stops binding fails the build. The asserts say what the command holds afterwards.
	// SQL Server commands use SqlDbType as the database type; ExpressiveCommand uses DbType. The overloads are the same, so both are written out.
	public static class ParameterTypeTests
	{
		// A command typed by SqlDbType, as the SQL Server packages are, without needing a connection.
		sealed class SqlTyped : ExpressiveCommandBase<IDbConnection, IDbCommand, IDataReader, SqlDbType, SqlTyped>
		{
			public SqlTyped() : base(NoConnection, CommandType.Text, "select 1", null) { }

			static IDbConnection NoConnection() => throw new NotSupportedException();

			protected override void AddParams(IDbCommand command) { }
		}

		sealed class NoConnections : IDbConnectionFactory
		{
			public IDbConnection Create() => throw new NotSupportedException();
		}

		static SqlTyped New() => new SqlTyped();

		static ExpressiveCommand NewDb() => new ExpressiveCommand(new NoConnections(), CommandType.Text, "select 1");

		static string Show(object value)
			=> value == null ? "null" : value is DBNull ? "DBNull" : Convert.ToString(value, CultureInfo.InvariantCulture);

		// What the command holds: name=value databaseType direction, "-" when no database type was given.
		static string Sent(SqlTyped command)
			=> string.Join("; ", command.Params.Select(p => p.Name + "=" + Show(p.Value) + " " + (p.Type.HasValue ? p.Type.Value.ToString() : "-") + " " + p.Direction));

		static string SentDb(ExpressiveCommand command)
			=> string.Join("; ", command.Params.Select(p => p.Name + "=" + Show(p.Value) + " " + (p.Type.HasValue ? p.Type.Value.ToString() : "-") + " " + p.Direction));

		// ---- a value and a database type: the forms that were ambiguous ----

		[Fact]
		public static void AddParam_StringAndType()
		{
			string s = "a";
			string n = null;

			Assert.Equal("x=a NVarChar Input", Sent(New().AddParam("x", s, SqlDbType.NVarChar)));
			Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParam("x", n, SqlDbType.NVarChar)));
			Assert.Equal("x=a NVarChar Input", Sent(New().AddParam("x", "a", SqlDbType.NVarChar)));
			Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParam("x", null, SqlDbType.NVarChar)));
			Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParam("x", (string)null, SqlDbType.NVarChar)));
			Assert.Equal("x=a NVarChar Output", Sent(New().AddParam("x", s, SqlDbType.NVarChar, ParameterDirection.Output)));
			Assert.Equal("x=a NVarChar Input", Sent(New().AddParam(name: "x", value: s, type: SqlDbType.NVarChar)));
		}

		[Fact]
		public static void AddParam_IntegersAndType()
		{
			int i = 5;
			long l = 6;
			short sh = 7;
			byte b = 8;

			Assert.Equal("x=5 Int Input", Sent(New().AddParam("x", i, SqlDbType.Int)));
			Assert.Equal("x=6 BigInt Input", Sent(New().AddParam("x", l, SqlDbType.BigInt)));
			Assert.Equal("x=7 SmallInt Input", Sent(New().AddParam("x", sh, SqlDbType.SmallInt)));
			Assert.Equal("x=8 TinyInt Input", Sent(New().AddParam("x", b, SqlDbType.TinyInt)));
			Assert.Equal("x=5 Int Input", Sent(New().AddParam("x", 5, SqlDbType.Int)));
			Assert.Equal("x=0 Int Input", Sent(New().AddParam("x", 0, SqlDbType.Int)));
			Assert.Equal("x=6 BigInt Input", Sent(New().AddParam("x", 6L, SqlDbType.BigInt)));
			Assert.Equal("x=5 Int Output", Sent(New().AddParam("x", i, SqlDbType.Int, ParameterDirection.Output)));
			Assert.Equal("x=5 Int Input", Sent(New().AddParam(name: "x", value: i, type: SqlDbType.Int)));
			Assert.Equal("x=5 Int Input", Sent(New().AddParam<int>("x", 5, SqlDbType.Int)));
		}

		[Fact]
		public static void AddParam_NarrowAndUnsignedIntegersAndType_SendTheNumber()
		{
			sbyte sb = 3;
			ushort us = 4;
			uint ui = 5;
			ulong ul = 6;

			Assert.Equal("x=3 SmallInt Input", Sent(New().AddParam("x", sb, SqlDbType.SmallInt)));
			Assert.Equal("x=4 Int Input", Sent(New().AddParam("x", us, SqlDbType.Int)));
			Assert.Equal("x=5 BigInt Input", Sent(New().AddParam("x", ui, SqlDbType.BigInt)));
			Assert.Equal("x=6 Decimal Input", Sent(New().AddParam("x", ul, SqlDbType.Decimal)));
		}

		// A char is sent as the character, not as its code.
		[Fact]
		public static void AddParam_CharAndType_SendsTheCharacter()
		{
			char c = 'c';

			Assert.Equal("x=c NChar Input", Sent(New().AddParam("x", c, SqlDbType.NChar)));
			Assert.Equal("x=c NChar Input", Sent(New().AddParam("x", 'c', SqlDbType.NChar)));
			Assert.Equal("x=c NChar Output", Sent(New().AddParam("x", c, SqlDbType.NChar, ParameterDirection.Output)));
			Assert.Equal("x=c NChar Input", Sent(New().AddParamIf(true, "x", c, SqlDbType.NChar)));
		}

		// A char converts implicitly to int, but it is a character: AddParam("x", 'c') sends 'c', not 99, as it did before there was an int overload. An int is still an int.
		[Fact]
		public static void AddParam_CharWithoutType_SendsTheCharacter_AndAnIntStaysAnInt()
		{
			char c = 'c';
			SqlDbType? type = SqlDbType.NChar;
			int i = 5;
			short sh = 7;

			Assert.Equal('c', Assert.IsType<char>(New().AddParam("x", c).Params[0].Value));
			Assert.Equal('c', Assert.IsType<char>(New().AddParam("x", 'c').Params[0].Value));
			Assert.Equal('c', Assert.IsType<char>(New().AddParam("x", c, null).Params[0].Value));
			Assert.Equal('c', Assert.IsType<char>(New().AddParam("x", c, type).Params[0].Value));
			Assert.Equal('c', Assert.IsType<char>(New().AddParamIf(true, "x", c).Params[0].Value));
			Assert.Equal('c', Assert.IsType<char>(New().AddParamIf(true, "x", 'c').Params[0].Value));
			Assert.Equal('c', Assert.IsType<char>(New().AddParamIf(true, "x", c, null).Params[0].Value));
			Assert.Equal("x=c - Input", Sent(New().AddParam("x", c)));
			Assert.Equal("x=c NChar Input", Sent(New().AddParam("x", c, type)));
			Assert.Equal("x=c NChar Input", Sent(New().AddParam("x", c, SqlDbType.NChar)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", c)));

			Assert.Equal(5, Assert.IsType<int>(New().AddParam("x", 5).Params[0].Value));
			Assert.Equal(5, Assert.IsType<int>(New().AddParam("x", i).Params[0].Value));
			Assert.Equal(7, Assert.IsType<int>(New().AddParam("x", sh).Params[0].Value));
			Assert.Equal(5, Assert.IsType<int>(New().AddParamIf(true, "x", i).Params[0].Value));
			Assert.Equal(6L, Assert.IsType<long>(New().AddParam("x", 6L).Params[0].Value));
		}

		[Fact]
		public static void AddParam_NullableValueAndType()
		{
			int? ni = 9;
			int? nn = null;
			long? ln = 10;
			short? sh = 11;
			byte? by = 12;
			decimal? d = 1.5m;
			DateTime? dt = new DateTime(2020, 1, 2, 3, 4, 5);
			Guid? g = new Guid("11111111-2222-3333-4444-555555555555");
			bool? b = true;
			double? db = 2.5;

			Assert.Equal("x=9 Int Input", Sent(New().AddParam("x", ni, SqlDbType.Int)));
			Assert.Equal("x=DBNull Int Input", Sent(New().AddParam("x", nn, SqlDbType.Int)));
			Assert.Equal("x=DBNull Int Input", Sent(New().AddParam("x", (int?)null, SqlDbType.Int)));
			Assert.Equal("x=10 BigInt Input", Sent(New().AddParam("x", ln, SqlDbType.BigInt)));
			Assert.Equal("x=11 SmallInt Input", Sent(New().AddParam("x", sh, SqlDbType.SmallInt)));
			Assert.Equal("x=12 TinyInt Input", Sent(New().AddParam("x", by, SqlDbType.TinyInt)));
			Assert.Equal("x=1.5 Decimal Input", Sent(New().AddParam("x", d, SqlDbType.Decimal)));
			Assert.Equal("x=01/02/2020 03:04:05 DateTime2 Input", Sent(New().AddParam("x", dt, SqlDbType.DateTime2)));
			Assert.Equal("x=11111111-2222-3333-4444-555555555555 UniqueIdentifier Input", Sent(New().AddParam("x", g, SqlDbType.UniqueIdentifier)));
			Assert.Equal("x=True Bit Input", Sent(New().AddParam("x", b, SqlDbType.Bit)));
			Assert.Equal("x=2.5 Float Input", Sent(New().AddParam("x", db, SqlDbType.Float)));
			Assert.Equal("x=9 Int Output", Sent(New().AddParam("x", ni, SqlDbType.Int, ParameterDirection.Output)));
		}

		// ---- a value and a database type: the forms that already compiled keep their meaning ----

		[Fact]
		public static void AddParam_OtherValuesAndType_AreUnchanged()
		{
			object o = "a";

			Assert.Equal("x=1.5 Decimal Input", Sent(New().AddParam("x", 1.5m, SqlDbType.Decimal)));
			Assert.Equal("x=01/02/2020 03:04:05 DateTime2 Input", Sent(New().AddParam("x", new DateTime(2020, 1, 2, 3, 4, 5), SqlDbType.DateTime2)));
			Assert.Equal("x=11111111-2222-3333-4444-555555555555 UniqueIdentifier Input", Sent(New().AddParam("x", new Guid("11111111-2222-3333-4444-555555555555"), SqlDbType.UniqueIdentifier)));
			Assert.Equal("x=True Bit Input", Sent(New().AddParam("x", true, SqlDbType.Bit)));
			Assert.Equal("x=System.Byte[] VarBinary Input", Sent(New().AddParam("x", new byte[] { 1 }, SqlDbType.VarBinary)));
			Assert.Equal("x=2.5 Float Input", Sent(New().AddParam("x", 2.5, SqlDbType.Float)));
			Assert.Equal("x=3.5 Real Input", Sent(New().AddParam("x", 3.5f, SqlDbType.Real)));
			Assert.Equal("x=a NVarChar Input", Sent(New().AddParam("x", o, SqlDbType.NVarChar)));
			Assert.Equal("x=5 Int Input", Sent(New().AddParam("x", (object)5, SqlDbType.Int)));
			Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParam("x", DBNull.Value, SqlDbType.NVarChar)));
			Assert.Equal("x=Monday Int Input", Sent(New().AddParam("x", DayOfWeek.Monday, SqlDbType.Int)));
		}

		[Fact]
		public static void AddParam_OptionalTypeForms_AreUnchanged()
		{
			int i = 5;
			string s = "a";
			int? ni = 9;
			SqlDbType? type = SqlDbType.Int;

			Assert.Equal("x=5 - Input", Sent(New().AddParam("x", i)));
			Assert.Equal("x=a - Input", Sent(New().AddParam("x", s)));
			Assert.Equal("x=9 - Input", Sent(New().AddParam("x", ni)));
			Assert.Equal("x=5 - Input", Sent(New().AddParam("x", i, null)));
			Assert.Equal("x=a - Input", Sent(New().AddParam("x", s, null)));
			Assert.Equal("x=5 - Input", Sent(New().AddParam<int>("x", 5, null)));
			Assert.Equal("x=5 Int Input", Sent(New().AddParam("x", i, type)));
			Assert.Equal("x=a Int Input", Sent(New().AddParam("x", s, type)));
			Assert.Equal("x=9 Int Input", Sent(New().AddParam("x", ni, type)));
			Assert.Equal("x=DBNull Int Input", Sent(New().AddParam("x", SqlDbType.Int)));
			Assert.Equal("x=5 - Output", Sent(New().AddParam("x", i, ParameterDirection.Output)));
		}

		// ---- the conditional forms ----

		[Fact]
		public static void AddParamIf_ValueAndType_AddsWhenTrue()
		{
			string s = "a";
			string n = null;
			int i = 5;
			long l = 6;
			short sh = 7;
			byte b = 8;
			int? ni = 9;
			long? ln = 10;
			decimal d = 1.5m;
			decimal? dn = 2.5m;
			DateTime dt = new DateTime(2020, 1, 2, 3, 4, 5);

			Assert.Equal("x=a NVarChar Input", Sent(New().AddParamIf(true, "x", s, SqlDbType.NVarChar)));
			Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParamIf(true, "x", n, SqlDbType.NVarChar)));
			Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParamIf(true, "x", null, SqlDbType.NVarChar)));
			Assert.Equal("x=5 Int Input", Sent(New().AddParamIf(true, "x", i, SqlDbType.Int)));
			Assert.Equal("x=6 BigInt Input", Sent(New().AddParamIf(true, "x", l, SqlDbType.BigInt)));
			Assert.Equal("x=7 SmallInt Input", Sent(New().AddParamIf(true, "x", sh, SqlDbType.SmallInt)));
			Assert.Equal("x=8 TinyInt Input", Sent(New().AddParamIf(true, "x", b, SqlDbType.TinyInt)));
			Assert.Equal("x=9 Int Input", Sent(New().AddParamIf(true, "x", ni, SqlDbType.Int)));
			Assert.Equal("x=10 BigInt Input", Sent(New().AddParamIf(true, "x", ln, SqlDbType.BigInt)));
			Assert.Equal("x=1.5 Decimal Input", Sent(New().AddParamIf(true, "x", d, SqlDbType.Decimal)));
			Assert.Equal("x=2.5 Decimal Input", Sent(New().AddParamIf(true, "x", dn, SqlDbType.Decimal)));
			Assert.Equal("x=01/02/2020 03:04:05 DateTime2 Input", Sent(New().AddParamIf(true, "x", dt, SqlDbType.DateTime2)));
			Assert.Equal("x=5 Int Output", Sent(New().AddParamIf(true, "x", i, SqlDbType.Int, ParameterDirection.Output)));
			Assert.Equal("x=3 Int Input", Sent(New().AddParamIf<int>(true, "x", 3, SqlDbType.Int)));
		}

		[Fact]
		public static void AddParamIf_ValueAndType_AddsNothingWhenFalse()
		{
			string s = "a";
			int i = 5;
			long l = 6;
			short sh = 7;
			int? ni = 9;
			decimal d = 1.5m;

			Assert.Equal("", Sent(New().AddParamIf(false, "x", s, SqlDbType.NVarChar)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", null, SqlDbType.NVarChar)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", i, SqlDbType.Int)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", l, SqlDbType.BigInt)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", sh, SqlDbType.SmallInt)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", ni, SqlDbType.Int)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", d, SqlDbType.Decimal)));
			Assert.Equal("", Sent(New().AddParamIf(false, "x", 'c', SqlDbType.NChar)));
		}

		[Fact]
		public static void AddParamIf_OptionalTypeForms_AreUnchanged()
		{
			string s = "a";
			int i = 5;
			SqlDbType? type = SqlDbType.Int;

			Assert.Equal("x=a - Input", Sent(New().AddParamIf(true, "x", s)));
			Assert.Equal("x=5 - Input", Sent(New().AddParamIf(true, "x", i)));
			Assert.Equal("x=5 - Input", Sent(New().AddParamIf(true, "x", i, null)));
			Assert.Equal("x=5 Int Input", Sent(New().AddParamIf(true, "x", i, type)));
		}

		// ---- the same with DbType, the database type of ExpressiveCommand ----

		[Fact]
		public static void ExpressiveCommand_ValueAndDbType()
		{
			string s = "a";
			string n = null;
			int i = 5;
			long l = 6;
			short sh = 7;
			byte b = 8;
			char c = 'c';
			int? ni = 9;
			decimal d = 1.5m;

			Assert.Equal("x=a String Input", SentDb(NewDb().AddParam("x", s, DbType.String)));
			Assert.Equal("x=DBNull String Input", SentDb(NewDb().AddParam("x", n, DbType.String)));
			Assert.Equal("x=DBNull String Input", SentDb(NewDb().AddParam("x", null, DbType.String)));
			Assert.Equal("x=5 Int32 Input", SentDb(NewDb().AddParam("x", i, DbType.Int32)));
			Assert.Equal("x=6 Int64 Input", SentDb(NewDb().AddParam("x", l, DbType.Int64)));
			Assert.Equal("x=7 Int16 Input", SentDb(NewDb().AddParam("x", sh, DbType.Int16)));
			Assert.Equal("x=8 Byte Input", SentDb(NewDb().AddParam("x", b, DbType.Byte)));
			Assert.Equal("x=c StringFixedLength Input", SentDb(NewDb().AddParam("x", c, DbType.StringFixedLength)));
			Assert.Equal("x=9 Int32 Input", SentDb(NewDb().AddParam("x", ni, DbType.Int32)));
			Assert.Equal("x=1.5 Decimal Input", SentDb(NewDb().AddParam("x", d, DbType.Decimal)));
			Assert.Equal("x=5 Int32 Input", SentDb(NewDb().AddParam("x", 5, DbType.Int32)));
			Assert.Equal("x=0 Int32 Input", SentDb(NewDb().AddParam("x", 0, DbType.Int32)));
			Assert.Equal("x=5 Int32 Output", SentDb(NewDb().AddParam("x", i, DbType.Int32, ParameterDirection.Output)));
			Assert.Equal("x=5 Int32 Input", SentDb(NewDb().AddParamIf(true, "x", i, DbType.Int32)));
			Assert.Equal("x=a String Input", SentDb(NewDb().AddParamIf(true, "x", s, DbType.String)));
			Assert.Equal("x=6 Int64 Input", SentDb(NewDb().AddParamIf(true, "x", l, DbType.Int64)));
			Assert.Equal("x=9 Int32 Input", SentDb(NewDb().AddParamIf(true, "x", ni, DbType.Int32)));
			Assert.Equal("", SentDb(NewDb().AddParamIf(false, "x", i, DbType.Int32)));
		}
	}
}
