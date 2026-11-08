#nullable enable

namespace Open.Database.Extensions.Tests;

// The date and time types are not IConvertible. A model with a DateOnly, TimeOnly, DateTimeOffset or TimeSpan property reads what a provider returns for such a column:
// a DateTime for a date column, a TimeSpan for a time column, a DateTimeOffset for datetimeoffset, text from SQLite.
[ExcludeFromCodeCoverage]
public static class DateAndTimeMappingTests
{
	public sealed class Stamps
	{
		public DateOnly Day { get; set; }
		public TimeOnly Time { get; set; }
		public DateTimeOffset Offset { get; set; }
		public TimeSpan Span { get; set; }
	}

	public sealed class MaybeStamps
	{
		public DateOnly? Day { get; set; }
		public TimeOnly? Time { get; set; }
		public DateTimeOffset? Offset { get; set; }
		public TimeSpan? Span { get; set; }
	}

	static readonly DateTime Moment = new(2024, 5, 6, 7, 8, 9, DateTimeKind.Unspecified);
	static readonly DateTimeOffset Zoned = new(2024, 5, 6, 7, 8, 9, TimeSpan.FromHours(2));

	static FakeExecuteReader Cmd(ImmutableArray<ColumnDef> columns, params object?[][] rows)
		=> new(() => TypedReader.Create(columns, rows));

	[Fact]
	public static void TheTypesAProviderReturns_AreMapped()
	{
		// date -> DateTime, time -> TimeSpan, datetimeoffset -> DateTimeOffset, time -> TimeSpan.
		var cmd = Cmd(
			[("Day", typeof(DateTime)), ("Time", typeof(TimeSpan)), ("Offset", typeof(DateTimeOffset)), ("Span", typeof(TimeSpan))],
			[Moment.Date, new TimeSpan(7, 8, 9), Zoned, new TimeSpan(1, 2, 3, 4)]);

		Stamps item = Assert.Single(cmd.Map<Stamps>().ToList());

		Assert.Equal(new DateOnly(2024, 5, 6), item.Day);
		Assert.Equal(new TimeOnly(7, 8, 9), item.Time);
		Assert.Equal(Zoned, item.Offset);
		Assert.Equal(Zoned.Offset, item.Offset.Offset);
		Assert.Equal(new TimeSpan(1, 2, 3, 4), item.Span);
	}

	[Fact]
	public static void Text_IsMapped_AsSqliteReturnsIt()
	{
		var cmd = Cmd(
			[("Day", typeof(string)), ("Time", typeof(string)), ("Offset", typeof(string)), ("Span", typeof(string))],
			["2024-05-06", "07:08:09", "2024-05-06T07:08:09+02:00", "1.02:03:04"]);

		Stamps item = Assert.Single(cmd.Map<Stamps>().ToList());

		Assert.Equal(new DateOnly(2024, 5, 6), item.Day);
		Assert.Equal(new TimeOnly(7, 8, 9), item.Time);
		Assert.Equal(Zoned, item.Offset);
		Assert.Equal(Zoned.Offset, item.Offset.Offset);
		Assert.Equal(new TimeSpan(1, 2, 3, 4), item.Span);
	}

	[Fact]
	public static void ADateTime_FeedsATimeOnly_ADateOnly_AndADateTimeOffset()
	{
		var cmd = Cmd(
			[("Day", typeof(DateTime)), ("Time", typeof(DateTime)), ("Offset", typeof(DateTime)), ("Span", typeof(DateTime))],
			[Moment, Moment, Moment, Moment]);

		Stamps item = Assert.Single(cmd.Map<Stamps>().ToList());

		Assert.Equal(new DateOnly(2024, 5, 6), item.Day);
		Assert.Equal(new TimeOnly(7, 8, 9), item.Time);
		Assert.Equal(new DateTimeOffset(Moment), item.Offset);
		Assert.Equal(new TimeSpan(7, 8, 9), item.Span);
	}

	[Fact]
	public static void ADateTimeOffset_FeedsADateOnly_AndATimeOnly_ByItsOwnClock()
	{
		var cmd = Cmd([("Day", typeof(DateTimeOffset)), ("Time", typeof(DateTimeOffset))], [Zoned, Zoned]);

		Stamps item = Assert.Single(cmd.Map<Stamps>().ToList());

		Assert.Equal(new DateOnly(2024, 5, 6), item.Day);
		Assert.Equal(new TimeOnly(7, 8, 9), item.Time);
	}

	[Fact]
	public static void Nullable_Members_GetNull_ForNull_AndTheValue_Otherwise()
	{
		var cmd = Cmd(
			[("Day", typeof(DateTime)), ("Time", typeof(TimeSpan)), ("Offset", typeof(DateTimeOffset)), ("Span", typeof(TimeSpan))],
			[Moment.Date, new TimeSpan(7, 8, 9), Zoned, new TimeSpan(1, 2, 3, 4)],
			[null, null, null, null]);

		var items = cmd.Map<MaybeStamps>().ToList();

		Assert.Equal(new DateOnly(2024, 5, 6), items[0].Day);
		Assert.Equal(new TimeOnly(7, 8, 9), items[0].Time);
		Assert.Equal(Zoned, items[0].Offset);
		Assert.Equal(new TimeSpan(1, 2, 3, 4), items[0].Span);

		Assert.Null(items[1].Day);
		Assert.Null(items[1].Time);
		Assert.Null(items[1].Offset);
		Assert.Null(items[1].Span);
	}

	[Fact]
	public static void NullInANonNullableMember_LeavesTheDefault()
	{
		var cmd = Cmd([("Day", typeof(DateTime)), ("Time", typeof(TimeSpan))], new object?[] { null, null }); // one row of two NULLs, not two rows

		Stamps item = Assert.Single(cmd.Map<Stamps>().ToList());

		Assert.Equal(default, item.Day);
		Assert.Equal(default, item.Time);
	}

	[Fact]
	public static void Text_ThatIsNotADate_ThrowsValueMappingException_WithTheCause()
	{
		var cmd = Cmd([("Day", typeof(string))], ["not a date"]);

		var ex = Assert.Throws<ValueMappingException>(() => cmd.Map<Stamps>().ToList());

		Assert.Equal("Day", ex.PropertyName);
		Assert.Equal(typeof(DateOnly), ex.TargetType);
		Assert.IsType<FormatException>(ex.InnerException);
	}

	[Fact]
	public static void ANumber_IsNotADate_ThrowsValueMappingException()
	{
		var cmd = Cmd([("Day", typeof(int))], [5]);

		var ex = Assert.Throws<ValueMappingException>(() => cmd.Map<Stamps>().ToList());

		Assert.Equal("Day", ex.PropertyName);
		Assert.IsType<InvalidCastException>(ex.InnerException);
	}
}
