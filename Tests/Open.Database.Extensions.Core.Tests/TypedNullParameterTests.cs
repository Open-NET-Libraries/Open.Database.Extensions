#nullable enable

namespace Open.Database.Extensions.Tests;

// A NULL passed to a typed AddParam overload is sent with the database type
// of the overload's static type, the way AddParams types a NULL member.
// The object overload has no type to use, so it stays untyped; a database
// type the caller passes always wins; a value is left to the provider.
[ExcludeFromCodeCoverage]
public static class TypedNullParameterTests
{
	static ExpressiveCommand Command(
		ConnectionProbe probe,
		string sql = "SELECT @a, @b, @c")
		=> probe.Connection.Command(sql);

	// "Untyped" means the library never set a type: reading DbType back from
	// a substitute gives 0 (AnsiString) whether or not it was set to that.
	static void AssertUntyped(IDbDataParameter parameter)
		=> parameter.DidNotReceive().DbType = Arg.Any<DbType>();

	static void AssertAllNull(ConnectionProbe probe)
		=> Assert.All(
			probe.Parameters,
			p => Assert.Equal(DBNull.Value, p.Value));

	[Fact]
	public static void ANullToATypedOverload_IsSentWithTheTypeOfTheOverload()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (long?)null)
			.AddParam("@b", default(int?))
			.AddParam("@c", (string?)null)
			.ExecuteNonQuery();

		Assert.Equal(
			[DbType.Int64, DbType.Int32, DbType.String],
			probe.Parameters.Select(p => p.DbType));
		AssertAllNull(probe);
	}

	[Fact]
	public static void ABareNullLiteral_BindsTheStringOverload_AndIsSentAsString()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", null)
			.ExecuteNonQuery();

		Assert.Equal(DbType.String, probe.Parameters[0].DbType);
		AssertAllNull(probe);
	}

	[Fact]
	public static void ANullToTheOtherValueTypes_FollowsTheMappingAddParamsUses()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (DateTime?)null)
			.AddParam("@b", (Guid?)null)
			.AddParam("@c", (decimal?)null)
			.AddParam("@d", (bool?)null)
			.AddParam("@e", (char?)null)
			.AddParam("@f", (DayOfWeek?)null) // an enum: its underlying type
			.ExecuteNonQuery();

		Assert.Equal(
			[
				DbType.DateTime,
				DbType.Guid,
				DbType.Decimal,
				DbType.Boolean,
				DbType.StringFixedLength,
				DbType.Int32,
			],
			probe.Parameters.Select(p => p.DbType));
		AssertAllNull(probe);
	}

	[Fact]
	public static void ANullToTheRemainingValueTypes_FollowsTheSameMapping()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (short?)null)
			.AddParam("@b", (byte?)null)
			.AddParam("@c", (double?)null)
			.AddParam("@d", (float?)null)
			.AddParam("@e", (TimeSpan?)null)
			.AddParam("@f", (DateTimeOffset?)null)
			.ExecuteNonQuery();

		Assert.Equal(
			[
				DbType.Int16,
				DbType.Byte,
				DbType.Double,
				DbType.Single,
				DbType.Time,
				DbType.DateTimeOffset,
			],
			probe.Parameters.Select(p => p.DbType));
		AssertAllNull(probe);
	}

	[Fact]
	public static void AnExplicitType_AlwaysWins_WhetherTheValueIsNullOrNot()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (long?)null, DbType.Int32)
			.AddParam("@b", (string?)null, DbType.AnsiStringFixedLength)
			.AddParam("@c", (long?)5, DbType.Int16)
			.AddParam("@d", (object?)null, DbType.Int32)
			.ExecuteNonQuery();

		Assert.Equal(
			[
				DbType.Int32,
				DbType.AnsiStringFixedLength,
				DbType.Int16,
				DbType.Int32,
			],
			probe.Parameters.Select(p => p.DbType));
	}

	[Fact]
	public static void AValue_IsLeftToTheProvider()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (long?)5)
			.AddParam("@b", "text")
			.AddParam("@c", 7)
			.ExecuteNonQuery();

		Assert.All(probe.Parameters, AssertUntyped);
		Assert.Equal(
			[5L, "text", 7],
			probe.Parameters.Select(p => p.Value));
	}

	[Fact]
	public static void TheObjectOverload_WithANull_StaysUntyped()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (object?)null)
			.ExecuteNonQuery();

		AssertUntyped(probe.Parameters[0]);
		AssertAllNull(probe);
	}

	[Fact]
	public static void AnOutputParameter_KeepsItsNullAndStaysUntyped()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (int?)null, direction: ParameterDirection.Output)
			.ExecuteNonQuery();

		AssertUntyped(probe.Parameters[0]);
		Assert.Null(probe.Parameters[0].Value);
	}

	[Fact]
	public static void AnInputOutputParameter_WithANull_IsTyped()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (int?)null, direction: ParameterDirection.InputOutput)
			.ExecuteNonQuery();

		Assert.Equal(DbType.Int32, probe.Parameters[0].DbType);
		AssertAllNull(probe);
	}

	// Known limit: a positional ParameterDirection is not a database type, so
	// the call binds the object overload, which has no type to use. Name the
	// direction (direction: ...) or pass a database type.
	[Fact]
	public static void APositionalDirection_BindsTheObjectOverload_AndStaysUntyped()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParam("@a", (long?)null, ParameterDirection.Input)
			.ExecuteNonQuery();

		AssertUntyped(probe.Parameters[0]);
		AssertAllNull(probe);
	}

	[Fact]
	public static void AddParamIf_WithANull_IsTypedTheSameWay()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe)
			.AddParamIf(true, "@a", (long?)null)
			.AddParamIf(true, "@b", (string?)null)
			.AddParamIf(false, "@c", (int?)null)
			.ExecuteNonQuery();

		Assert.Equal(
			[DbType.Int64, DbType.String],
			probe.Parameters.Select(p => p.DbType));
		AssertAllNull(probe);
	}

	[Fact]
	public static void AddParamIf_WithATypeOrAnObject_FollowsTheSameRules()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe, "SELECT @a, @b, @c, @d, @e")
			.AddParamIf(true, "@a", (long?)null, DbType.Int32)
			.AddParamIf(true, "@b", (string?)null, DbType.AnsiStringFixedLength)
			.AddParamIf(true, "@c", (long?)null, (DbType?)null)
			.AddParamIf(true, "@d", (object?)null, DbType.Int16)
			.AddParamIf(true, "@e", (object?)null)
			.ExecuteNonQuery();

		Assert.Equal(
			[DbType.Int32, DbType.AnsiStringFixedLength, DbType.Int64, DbType.Int16],
			probe.Parameters.Take(4).Select(p => p.DbType));
		AssertUntyped(probe.Parameters[4]);
		AssertAllNull(probe);
	}

	[Fact]
	public static void ANullArgument_NamedFromItsExpression_IsTyped()
	{
		long? n = null;
		var probe = new ConnectionProbe(ConnectionState.Open);

		Command(probe, "SELECT @n")
			.AddParam(n)
			.ExecuteNonQuery();

		Assert.Equal(DbType.Int64, probe.Parameters[0].DbType);
		AssertAllNull(probe);
	}
}
