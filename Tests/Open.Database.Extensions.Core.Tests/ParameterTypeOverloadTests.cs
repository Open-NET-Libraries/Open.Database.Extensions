using System.Globalization;

namespace Open.Database.Extensions.Tests;

#nullable enable

// AddParam("@id", id, SqlDbType.Int): a value and a database type. Every call below must compile at the latest language version, so the build of this project is the guard:
// a call that becomes ambiguous (CS0121) or stops binding fails the build. The asserts say what the command holds, and what reaches the provider's parameter, afterwards.
// SQL Server commands use SqlDbType as the database type; ExpressiveCommand uses DbType. The overloads are the same.
[ExcludeFromCodeCoverage]
public static class ParameterTypeOverloadTests
{
	// A command typed by SqlDbType, as the SQL Server packages are, without needing a connection.
	sealed class SqlTyped() : ExpressiveCommandBase<IDbConnection, IDbCommand, IDataReader, SqlDbType, SqlTyped>(NoConnection, CommandType.Text, "select 1", null)
	{
		static IDbConnection NoConnection() => throw new NotSupportedException();

		protected override void AddParams(IDbCommand command) { }
	}

	static SqlTyped New() => new();

	static string Show(object? value)
		=> value is null ? "null" : value is DBNull ? "DBNull" : Convert.ToString(value, CultureInfo.InvariantCulture)!;

	// What the command holds: name=value databaseType direction, "-" when no database type was given.
	static string Sent(SqlTyped command)
		=> string.Join("; ", command.Params.Select(p => $"{p.Name}={Show(p.Value)} {(p.Type.HasValue ? p.Type.Value.ToString() : "-")} {p.Direction}"));

	static string Only(bool condition, string expected) => condition ? expected : "";

	static readonly DateTime Stamp = new(2020, 1, 2, 3, 4, 5);
	static readonly Guid Key = new("11111111-2222-3333-4444-555555555555");

	// ---- a value and a database type: the forms that were ambiguous ----

	[Fact]
	public static void AddParam_StringAndType()
	{
		string s = "a";
		string? n = null;

		Assert.Equal("x=a NVarChar Input", Sent(New().AddParam("x", s, SqlDbType.NVarChar)));
		Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParam("x", n, SqlDbType.NVarChar)));
		Assert.Equal("x=a NVarChar Input", Sent(New().AddParam("x", "a", SqlDbType.NVarChar)));
		Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParam("x", null, SqlDbType.NVarChar)));
		Assert.Equal("x=DBNull NVarChar Input", Sent(New().AddParam("x", (string?)null, SqlDbType.NVarChar)));
		Assert.Equal("x=a NVarChar Output", Sent(New().AddParam("x", s, SqlDbType.NVarChar, ParameterDirection.Output)));
		Assert.Equal("x=a NVarChar InputOutput", Sent(New().AddParam(name: "x", value: s, type: SqlDbType.NVarChar, direction: ParameterDirection.InputOutput)));
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
		Assert.IsType<char>(New().AddParam("x", c, SqlDbType.NChar).Params[0].Value);
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
		DateTime? dt = Stamp;
		Guid? g = Key;
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
		Assert.Equal("x=01/02/2020 03:04:05 DateTime2 Input", Sent(New().AddParam("x", Stamp, SqlDbType.DateTime2)));
		Assert.Equal("x=11111111-2222-3333-4444-555555555555 UniqueIdentifier Input", Sent(New().AddParam("x", Key, SqlDbType.UniqueIdentifier)));
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

	// ---- the conditional forms, with the condition true and false ----

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static void AddParamIf_ValueAndType_AddsOnlyWhenTrue(bool condition)
	{
		string s = "a";
		string? n = null;
		int i = 5;
		long l = 6;
		short sh = 7;
		byte b = 8;
		int? ni = 9;
		long? ln = 10;
		decimal d = 1.5m;
		decimal? dn = 2.5m;
		char c = 'c';

		Assert.Equal(Only(condition, "x=a NVarChar Input"), Sent(New().AddParamIf(condition, "x", s, SqlDbType.NVarChar)));
		Assert.Equal(Only(condition, "x=DBNull NVarChar Input"), Sent(New().AddParamIf(condition, "x", n, SqlDbType.NVarChar)));
		Assert.Equal(Only(condition, "x=DBNull NVarChar Input"), Sent(New().AddParamIf(condition, "x", null, SqlDbType.NVarChar)));
		Assert.Equal(Only(condition, "x=5 Int Input"), Sent(New().AddParamIf(condition, "x", i, SqlDbType.Int)));
		Assert.Equal(Only(condition, "x=6 BigInt Input"), Sent(New().AddParamIf(condition, "x", l, SqlDbType.BigInt)));
		Assert.Equal(Only(condition, "x=7 SmallInt Input"), Sent(New().AddParamIf(condition, "x", sh, SqlDbType.SmallInt)));
		Assert.Equal(Only(condition, "x=8 TinyInt Input"), Sent(New().AddParamIf(condition, "x", b, SqlDbType.TinyInt)));
		Assert.Equal(Only(condition, "x=9 Int Input"), Sent(New().AddParamIf(condition, "x", ni, SqlDbType.Int)));
		Assert.Equal(Only(condition, "x=10 BigInt Input"), Sent(New().AddParamIf(condition, "x", ln, SqlDbType.BigInt)));
		Assert.Equal(Only(condition, "x=1.5 Decimal Input"), Sent(New().AddParamIf(condition, "x", d, SqlDbType.Decimal)));
		Assert.Equal(Only(condition, "x=2.5 Decimal Input"), Sent(New().AddParamIf(condition, "x", dn, SqlDbType.Decimal)));
		Assert.Equal(Only(condition, "x=01/02/2020 03:04:05 DateTime2 Input"), Sent(New().AddParamIf(condition, "x", Stamp, SqlDbType.DateTime2)));
		Assert.Equal(Only(condition, "x=c NChar Input"), Sent(New().AddParamIf(condition, "x", c, SqlDbType.NChar)));
		Assert.Equal(Only(condition, "x=5 Int Output"), Sent(New().AddParamIf(condition, "x", i, SqlDbType.Int, ParameterDirection.Output)));
		Assert.Equal(Only(condition, "x=3 Int Input"), Sent(New().AddParamIf<int>(condition, "x", 3, SqlDbType.Int)));
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static void AddParamIf_OptionalTypeForms_AreUnchanged(bool condition)
	{
		string s = "a";
		int i = 5;
		SqlDbType? type = SqlDbType.Int;

		Assert.Equal(Only(condition, "x=a - Input"), Sent(New().AddParamIf(condition, "x", s)));
		Assert.Equal(Only(condition, "x=5 - Input"), Sent(New().AddParamIf(condition, "x", i)));
		Assert.Equal(Only(condition, "x=5 - Input"), Sent(New().AddParamIf(condition, "x", i, null)));
		Assert.Equal(Only(condition, "x=5 Int Input"), Sent(New().AddParamIf(condition, "x", i, type)));
	}

	// ---- what reaches the provider's parameter (DbType, as ExpressiveCommand uses) ----
	// A short, byte or char binds to the int or char overload; the parameter must still carry the value and the given database type.

	[Fact]
	public static void ValueAndDbType_ReachTheProviderParameter()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		string s = "a";
		string? n = null;
		int i = 5;
		long l = 6;
		short sh = 7;
		byte b = 8;
		char c = 'c';
		int? ni = 9;
		int? nn = null;
		decimal d = 1.5m;

		new ExpressiveCommand(probe.Connection, CommandType.Text, "select 1")
			.AddParam("s", s, DbType.String)
			.AddParam("n", n, DbType.String)
			.AddParam("i", i, DbType.Int32)
			.AddParam("l", l, DbType.Int64)
			.AddParam("sh", sh, DbType.Int16)
			.AddParam("b", b, DbType.Byte)
			.AddParam("c", c, DbType.StringFixedLength)
			.AddParam("ni", ni, DbType.Int32)
			.AddParam("nn", nn, DbType.Int32)
			.AddParam("d", d, DbType.Decimal)
			.AddParamIf(true, "if", i, DbType.Int32)
			.AddParamIf(false, "skipped", i, DbType.Int32)
			.ExecuteNonQuery();

		Assert.Equal(
			[
				"s=a String", "n=DBNull String", "i=5 Int32", "l=6 Int64", "sh=7 Int16", "b=8 Byte", "c=c StringFixedLength",
				"ni=9 Int32", "nn=DBNull Int32", "d=1.5 Decimal", "if=5 Int32",
			],
			probe.Parameters.Select(p => $"{p.ParameterName}={Show(p.Value)} {p.DbType}"));
	}
}
