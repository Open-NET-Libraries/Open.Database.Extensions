extern alias mssql;

namespace Open.Database.Extensions.Tests;

#nullable enable

// AddParam("@o", DbType.Int32, ParameterDirection.Output) on the two SQL Server commands. Their database type is SqlDbType, so a DbType used to bind to AddParam(string, object?, ParameterDirection)
// and the enum itself became the VALUE of an output parameter. The overloads below take the DbType, as the generic commands do, and build a parameter of that type with no value.
// Each case is written for both packages (System.Data.SqlClient and Microsoft.Data.SqlClient): the second is reached through an extern alias, as both define ExpressiveSqlCommand.
// A command that declares an AddParam of its own hides the inherited ones that apply to the same call, so the last tests pin the calls that must keep their meaning, a literal 0 above all.
[ExcludeFromCodeCoverage]
public static class SqlServerParameterTypeTests
{
#pragma warning disable CS0618 // System.Data.SqlClient is obsolete; it is one of the two packages under test
	static global::Open.Database.Extensions.ExpressiveSqlCommand Legacy()
		=> new(new System.Data.SqlClient.SqlConnection(), CommandType.StoredProcedure, "dbo.p");
#pragma warning restore CS0618

	static mssql::Open.Database.Extensions.ExpressiveSqlCommand Microsoft()
		=> new(new Microsoft.Data.SqlClient.SqlConnection(), CommandType.StoredProcedure, "dbo.p");

	static string Show(object? value)
		=> value is null ? "null" : value is DBNull ? "DBNull" : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!;

	// What the command holds: name=value databaseType direction, "-" when no database type was given.
#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
	static string Sent(global::Open.Database.Extensions.ExpressiveSqlCommand command)
		=> string.Join("; ", command.Params.Select(p => $"{p.Name}={Show(p.Value)} {(p.Type.HasValue ? p.Type.Value.ToString() : "-")} {p.Direction}"));
#pragma warning restore CS0618

	static string Sent(mssql::Open.Database.Extensions.ExpressiveSqlCommand command)
		=> string.Join("; ", command.Params.Select(p => $"{p.Name}={Show(p.Value)} {(p.Type.HasValue ? p.Type.Value.ToString() : "-")} {p.Direction}"));

	static string Only(bool condition, string expected) => condition ? expected : "";

	// ---- a DbType and a direction: a parameter of that type with no value ----

	[Fact]
	public static void ADbTypeAndADirection_AreATypedParameterWithNoValue()
	{
		Assert.Equal("@o=null Int Output", Sent(Legacy().AddParam("@o", DbType.Int32, ParameterDirection.Output)));
		Assert.Equal("@o=null Int Output", Sent(Microsoft().AddParam("@o", DbType.Int32, ParameterDirection.Output)));
		Assert.Equal("@o=null BigInt Output", Sent(Legacy().AddParam("@o", DbType.Int64, ParameterDirection.Output)));
		Assert.Equal("@o=null NVarChar Output", Sent(Microsoft().AddParam("@o", DbType.String, ParameterDirection.Output)));
		Assert.Equal("@o=null VarChar Output", Sent(Legacy().AddParam("@o", DbType.AnsiString, ParameterDirection.Output)));
		Assert.Equal("@o=null DateTime2 Output", Sent(Microsoft().AddParam("@o", DbType.DateTime2, ParameterDirection.Output)));
		Assert.Equal("@o=null Bit Output", Sent(Legacy().AddParam("@o", DbType.Boolean, ParameterDirection.Output)));
	}

	[Fact]
	public static void TheOtherDirections_KeepTheirMeaning()
	{
		Assert.Equal("@r=null Int ReturnValue", Sent(Legacy().AddParam("@r", DbType.Int32, ParameterDirection.ReturnValue)));
		Assert.Equal("@r=null Int ReturnValue", Sent(Microsoft().AddParam("@r", DbType.Int32, ParameterDirection.ReturnValue)));

		// An input or input-output parameter with no value is a typed NULL, as for a SqlDbType.
		Assert.Equal("@i=DBNull Int Input", Sent(Legacy().AddParam("@i", DbType.Int32, ParameterDirection.Input)));
		Assert.Equal("@io=DBNull Int InputOutput", Sent(Microsoft().AddParam("@io", DbType.Int32, ParameterDirection.InputOutput)));
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static void AddParamIf_AddsOnlyWhenTrue(bool condition)
	{
		Assert.Equal(Only(condition, "@o=null Int Output"), Sent(Legacy().AddParamIf(condition, "@o", DbType.Int32, ParameterDirection.Output)));
		Assert.Equal(Only(condition, "@o=null Int Output"), Sent(Microsoft().AddParamIf(condition, "@o", DbType.Int32, ParameterDirection.Output)));
	}

	[Fact]
	public static void ADbTypeSqlServerHasNoTypeFor_Throws_AsTheProviderDoes()
	{
		Assert.Throws<ArgumentException>(() => Legacy().AddParam("@o", DbType.UInt16, ParameterDirection.Output));
		Assert.Throws<ArgumentException>(() => Microsoft().AddParam("@o", DbType.UInt16, ParameterDirection.Output));
	}

	// ---- the calls that must keep their meaning: the new overload is declared on the command, so it hides the inherited overloads of any call it applies to ----
	// A literal 0 converts implicitly to any enum, so a DbType parameter with a default direction would take AddParam("@count", 0) and add a typed NULL where an int 0 was meant.

	[Fact]
	[SuppressMessage("Roslynator", "RCS1118:Mark local variable as const", Justification = "The local is a variable on purpose: a literal 0 converts implicitly to any enum, a variable does not, and the test pins both.")]
	public static void AValueIsStillAValue_AboveAllALiteralZero()
	{
		int zero = 0;

		Assert.Equal("@x=0 - Input", Sent(Legacy().AddParam("@x", 0)));
		Assert.Equal("@x=0 - Input", Sent(Microsoft().AddParam("@x", 0)));
		Assert.Equal("@x=0 - Input", Sent(Legacy().AddParam("@x", zero)));
		Assert.Equal("@x=0 - Input", Sent(Microsoft().AddParam("@x", zero)));
		Assert.Equal("@x=0 - Input", Sent(Legacy().AddParam("@x", 0L)));
		Assert.Equal("@x=0 Int Input", Sent(Microsoft().AddParam("@x", 0, SqlDbType.Int)));
		Assert.Equal("@x=0 Int Input", Sent(Legacy().AddParamIf(true, "@x", 0, SqlDbType.Int)));
		Assert.Equal("@x=0 - Input", Sent(Microsoft().AddParamIf(true, "@x", 0)));
		Assert.Equal("@x=c - Input", Sent(Legacy().AddParam("@x", 'c')));
		Assert.Equal("@x=c - Input", Sent(Microsoft().AddParam("@x", 'c')));
		Assert.Equal("@x=5 - Output", Sent(Legacy().AddParam("@x", 5, ParameterDirection.Output)));
		Assert.Equal("@x=a - Input", Sent(Microsoft().AddParam("@x", "a")));
	}

	[Fact]
	public static void ASqlDbTypeIsStillASqlDbType()
	{
		Assert.Equal("@x=DBNull Int Input", Sent(Legacy().AddParam("@x", SqlDbType.Int)));
		Assert.Equal("@x=null Int Output", Sent(Microsoft().AddParam("@x", SqlDbType.Int, ParameterDirection.Output)));
		Assert.Equal("@x=5 Int Input", Sent(Legacy().AddParam("@x", 5, SqlDbType.Int)));
		Assert.Equal("@x=null Int ReturnValue", Sent(Microsoft().AddReturnParam("@x", SqlDbType.Int)));
	}
}
