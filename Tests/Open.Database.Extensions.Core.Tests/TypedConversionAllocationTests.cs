using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// SQLite stores a date or a decimal as TEXT and a bool as INTEGER, so the column type the provider reports differs from the type of the member.
/// The map reads each of those cells through a typed conversion, and a mapped read allocates what the hand-written read allocates:
/// the per-row bytes of the two are measured in the same run and compared (a box per cell is 24, 32 or 48 bytes, far above the slack).
/// The behaviour of the conversions is pinned too: what a NULL, a bad value and the culture do.
/// </summary>
[ExcludeFromCodeCoverage]
public static class TypedConversionAllocationTests
{
	public sealed class DateCell { public DateTime Value { get; set; } }
	public sealed class DecimalCell { public decimal Value { get; set; } }
	public sealed class BoolCell { public bool Value { get; set; } }
	public sealed class MaybeDateCell { public DateTime? Value { get; set; } }
	public sealed class MaybeDecimalCell { public decimal? Value { get; set; } }
	public sealed class MaybeBoolCell { public bool? Value { get; set; } }

	// What the mapped read may cost per row beyond the hand-written read: the two do the same work, so they differ by no more than a rounding of the measurement.
	const int SlackPerRow = 8;

	static SqliteConnection Open(string declaration, string expression, int rows)
	{
		SqliteConnection connection = new("Data Source=:memory:");
		connection.Open();
		using SqliteCommand command = connection.CreateCommand();
		command.CommandText
			= $"create table t(value {declaration});"
			+ $"with recursive n(i) as (select 1 union all select i + 1 from n where i < {rows})"
			+ $" insert into t select {expression} from n;";
		command.ExecuteNonQuery();
		return connection;
	}

	// Bytes per call: after a warm-up, the smallest of three rounds (the allocated-bytes counter of this thread is exact, so only a stray JIT or GC adds noise).
	static long BytesPerCall(Func<object> call)
	{
		for (int i = 0; i < 50; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 20; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 20);
		}

		return best;
	}

	static List<TModel> Hand<TModel>(SqliteConnection connection, Func<DbDataReader, TModel> read)
	{
		using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "select value from t";
		using DbDataReader reader = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
		List<TModel> list = [];
		while (reader.Read()) list.Add(read(reader));
		return list;
	}

	static List<TModel> Mapped<TModel>(SqliteConnection connection)
		where TModel : class
	{
		using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "select value from t";
		return command.Map<TModel>().ToList();
	}

	// (bytes at 1000 rows - bytes at 100 rows) / 900 per read: the cost of a call, the model object and the list growth cancel, so only the row remains.
	static void AssertSameBytesPerRow<TModel>(string declaration, string expression, Func<DbDataReader, TModel> read)
		where TModel : class
	{
		double[] mapped = new double[2];
		double[] hand = new double[2];
		int[] sizes = [100, 1000];
		for (int s = 0; s < 2; s++)
		{
			using SqliteConnection connection = Open(declaration, expression, sizes[s]);
			mapped[s] = BytesPerCall(() => Mapped<TModel>(connection));
			hand[s] = BytesPerCall(() => Hand(connection, read));
		}

		double mappedPerRow = (mapped[1] - mapped[0]) / 900;
		double handPerRow = (hand[1] - hand[0]) / 900;
		Assert.True(
			mappedPerRow - handPerRow <= SlackPerRow,
			$"{typeof(TModel).Name}: the mapped read allocates {mappedPerRow:F1} B per row, the hand-written read {handPerRow:F1} B.");
	}

	// ---- no allocation beyond the hand-written read ----

	[Fact]
	public static void DateTime_FromText_AllocatesAsTheHandWrittenReadDoes()
		=> AssertSameBytesPerRow(
			"text",
			"'2024-01-01T00:00:00'",
			r => new DateCell { Value = r.GetDateTime(0) });

	[Fact]
	public static void Decimal_FromText_AllocatesAsTheHandWrittenReadDoes()
		=> AssertSameBytesPerRow(
			"text",
			"(10 + i * 0.25) || ''",
			r => new DecimalCell { Value = r.GetDecimal(0) });

	[Fact]
	public static void Bool_FromInteger_AllocatesAsTheHandWrittenReadDoes()
		=> AssertSameBytesPerRow(
			"integer",
			"i % 2",
			r => new BoolCell { Value = r.GetBoolean(0) });

	[Fact]
	public static void NullableDateTime_FromText_AllocatesAsTheHandWrittenReadDoes()
		=> AssertSameBytesPerRow(
			"text",
			"case when i % 5 = 2 then null else '2024-01-01T00:00:00' end",
			r => new MaybeDateCell { Value = r.IsDBNull(0) ? null : r.GetDateTime(0) });

	[Fact]
	public static void NullableDecimal_FromText_AllocatesAsTheHandWrittenReadDoes()
		=> AssertSameBytesPerRow(
			"text",
			"case when i % 5 = 2 then null else (10 + i * 0.25) || '' end",
			r => new MaybeDecimalCell { Value = r.IsDBNull(0) ? null : r.GetDecimal(0) });

	[Fact]
	public static void NullableBool_FromInteger_AllocatesAsTheHandWrittenReadDoes()
		=> AssertSameBytesPerRow(
			"integer",
			"case when i % 5 = 2 then null else i % 2 end",
			r => new MaybeBoolCell { Value = r.IsDBNull(0) ? null : r.GetBoolean(0) });

	// ---- the values and the failures stay what they were ----

	static List<TModel> Read<TModel>(string declaration, string expression)
		where TModel : class
	{
		using SqliteConnection connection = Open(declaration, expression, 1);
		return Mapped<TModel>(connection);
	}

	[Theory]
	[InlineData("0", false)]
	[InlineData("1", true)]
	[InlineData("2", true)]
	[InlineData("-1", true)]
	[InlineData("9000000000", true)]
	public static void Bool_FromAnyInteger_IsNotZero(string sql, bool expected)
		=> Assert.Equal(expected, Read<BoolCell>("integer", sql).Single().Value);

	[Fact]
	public static void Values_AreParsedWithTheInvariantCulture()
	{
		CultureInfo before = CultureInfo.CurrentCulture;
		try
		{
			CultureInfo.CurrentCulture = new CultureInfo("de-DE");

			Assert.Equal(1234.5m, Read<DecimalCell>("text", "'1234.5'").Single().Value);
			Assert.Equal(new DateTime(2024, 5, 6, 7, 8, 9), Read<DateCell>("text", "'2024-05-06T07:08:09'").Single().Value);
			Assert.Equal(new DateTime(2024, 5, 6), Read<DateCell>("text", "'05/06/2024'").Single().Value);
		}
		finally
		{
			CultureInfo.CurrentCulture = before;
		}
	}

	[Fact]
	public static void Null_IsNullForTheNullableMembers()
	{
		Assert.Null(Read<MaybeDateCell>("text", "null").Single().Value);
		Assert.Null(Read<MaybeDecimalCell>("text", "null").Single().Value);
		Assert.Null(Read<MaybeBoolCell>("integer", "null").Single().Value);
	}

	[Fact]
	public static void Null_KeepsTheValueOfAMemberThatIsNotNullable()
	{
		Assert.Equal(default, Read<DateCell>("text", "null").Single().Value);
		Assert.Equal(0m, Read<DecimalCell>("text", "null").Single().Value);
		Assert.False(Read<BoolCell>("integer", "null").Single().Value);
	}

	[Fact]
	public static void ABadValue_IsAValueMappingException_WithTheCauseOfTheParse()
	{
		ValueMappingException date = Assert.Throws<ValueMappingException>(() => Read<DateCell>("text", "'not a date'"));
		Assert.IsType<FormatException>(date.InnerException);
		Assert.Equal("Value", date.PropertyName);

		ValueMappingException number = Assert.Throws<ValueMappingException>(() => Read<DecimalCell>("text", "'12,5x'"));
		Assert.IsType<FormatException>(number.InnerException);

		ValueMappingException big = Assert.Throws<ValueMappingException>(() => Read<DecimalCell>("text", "'99999999999999999999999999999999'"));
		Assert.IsType<OverflowException>(big.InnerException);
	}
}
