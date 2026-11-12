using Open.Database.Extensions.Core;
using System.Globalization;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A converter on a RowMap pair: a column that needs a trim, a parse or a format on its way to a member
// maps straight through the compiled row reader, and the rest of the type still maps by name.
[ExcludeFromCodeCoverage]
public static class RowMapConverterTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;
	static readonly DateTime Start = new(2024, 5, 6, 14, 5, 0, DateTimeKind.Utc);

	public sealed class Order
	{
		public int Id { get; set; }
		public string? OrderNum { get; set; }
		public string? FirstLoad { get; set; }
		public string Customer { get; set; } = "unknown";
		public int Quantity { get; set; } = 5;
		public int Count { get; set; } = 3;
		public string? Note { get; set; }

		// Computed from the others: never mapped.
		public string Summary => $"{OrderNum}@{FirstLoad}";
	}

	public sealed record Line(int Id, string Label)
	{
		public required string Code { get; init; }
		public string? Note { get; init; }
	}

	public sealed record Point(int? X, int Y = 4);

	public sealed class Measure
	{
		public int? Weight { get; set; }
		public object? Tag { get; set; }
		public IEnumerable<char>? Letters { get; set; }
	}

	static readonly ImmutableArray<ColumnDef> Columns =
	[
		("Id", typeof(int)),
		("order_num", typeof(long)),
		("first_load_datetime", typeof(DateTime)),
		("Customer", typeof(string)),
		("Quantity", typeof(string)),
		("Count", typeof(int)),
		("Note", typeof(string)),
	];

	const int OrderNumAt = 1, CustomerAt = 3, QuantityAt = 4;

	static object?[] Valid() => [7, 42L, Start, "  Ann  ", "12", 9, "n"];

	static object?[] With(int column, object? value)
	{
		object?[] row = Valid();
		row[column] = value;
		return row;
	}

	// The pair for the one column that cannot match by name.
	static readonly RowMap<Order> LoadPair = [new(nameof(Order.FirstLoad), "first_load_datetime")];

	// order_num becomes text, first_load_datetime becomes "HH:mm", the text is trimmed or parsed: no hand-written NULL check anywhere.
	static readonly RowMap<Order> Full = LoadPair
		.Convert<long, string>(nameof(Order.OrderNum), n => n.ToString("D6", Invariant))
		.Convert<DateTime, string>(nameof(Order.FirstLoad), t => t.ToString("HH:mm", Invariant))
		.Convert<string, string>(nameof(Order.Customer), s => s.Trim())
		.Convert<string, int>(nameof(Order.Quantity), s => int.Parse(s, Invariant))
		.Convert<int?, int>(nameof(Order.Count), c => c ?? -1);

	static FakeExecuteReader Cmd(ImmutableArray<ColumnDef> columns, params object?[][] rows)
		=> new(() => TypedReader.Create(columns, rows));

	static T One<T>(RowMap<T> map, ImmutableArray<ColumnDef> columns, object?[] row)
		=> Assert.Single(Cmd(columns, row).Map(map).ToList());

	static void AssertFull(Order order, int count = 9)
	{
		Assert.Equal(7, order.Id);
		Assert.Equal("000042", order.OrderNum);
		Assert.Equal("14:05", order.FirstLoad);
		Assert.Equal("Ann", order.Customer);
		Assert.Equal(12, order.Quantity);
		Assert.Equal(count, order.Count);
		Assert.Equal("n", order.Note);
		Assert.Equal("000042@14:05", order.Summary);
	}

	// ---- the ask: a pair names a column, a member and a conversion; the rest of the type maps by name ----

	[Fact]
	public static void TrimParseAndFormat_MapTheRowStraightThrough()
		=> AssertFull(One(Full, Columns, Valid()));

	[Fact]
	public static void ANamedPair_ConvertsThatColumn_AndTheRestMapsByName()
	{
		RowMap<Order> map = LoadPair.Convert<DateTime, string>(
			nameof(Order.FirstLoad),
			t => t.ToString("HH:mm", Invariant));

		Order order = One(map, Columns, Valid());

		Assert.Equal("14:05", order.FirstLoad);
		Assert.Equal(7, order.Id);
		Assert.Equal("42", order.OrderNum);         // order_num by name, not converted
		Assert.Equal("  Ann  ", order.Customer);    // not converted
		Assert.Equal(12, order.Quantity);
		Assert.Equal("n", order.Note);
	}

	[Theory]
	[InlineData("OrderNum")]
	[InlineData("ordernum")]
	[InlineData("order_num")]
	[InlineData("ORDER_NUM")]
	public static void ByNameOnly_NeedsNoPair(string column)
	{
		RowMap<Order> map = RowMap<Order>.Default.Convert<long, string>(
			nameof(Order.OrderNum),
			n => n.ToString("D6", Invariant));
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), (column, typeof(long))];

		Order order = One(map, columns, [7, 42L]);

		Assert.Equal(7, order.Id);
		Assert.Equal("000042", order.OrderNum);
		Assert.Null(order.FirstLoad);
	}

	[Fact]
	public static void TheColumnForm_IsAPairWithAConverter()
	{
		RowMap<Order> map = RowMap<Order>.Default.Convert<DateTime, string>(
			nameof(Order.FirstLoad),
			"FIRST_LOAD_DATETIME",
			t => t.ToString("HH:mm", Invariant));

		Order order = One(map, Columns, Valid());

		Assert.Equal("14:05", order.FirstLoad);
		Assert.Equal("42", order.OrderNum);

		KeyValuePair<string, string?> pair = Assert.Single(map);
		Assert.Equal(nameof(Order.FirstLoad), pair.Key);
		Assert.Equal("FIRST_LOAD_DATETIME", pair.Value);
	}

	[Fact]
	public static void TheNameForm_DoesNotNameTheMemberInThePairs()
		=> Assert.Empty(RowMap<Order>.Default.Convert<long, string>(
			nameof(Order.OrderNum),
			n => n.ToString(Invariant)));

	[Fact]
	public static void AFormatThatPads_AndATrim_AndAParse()
	{
		Order order = One(Full, Columns, With(OrderNumAt, 7L));
		Assert.Equal("000007", order.OrderNum);

		Assert.Equal("Bo", One(Full, Columns, With(CustomerAt, "\t Bo \r\n")).Customer);
		Assert.Equal(-3, One(Full, Columns, With(QuantityAt, "-3")).Quantity);
	}

	[Fact]
	public static void AValueTheConverterCannotParse_NamesThePropertyAndColumn()
	{
		var ex = Assert.Throws<ValueMappingException>(
			() => One(Full, Columns, With(QuantityAt, "twelve")));

		Assert.Equal(nameof(Order.Quantity), ex.PropertyName);
		Assert.Equal("Quantity", ex.ColumnName);
		Assert.Equal(typeof(int), ex.TargetType);
		Assert.IsType<FormatException>(ex.InnerException);
	}

	// ---- NULL ----

	[Fact]
	public static void ANullCell_DoesNotCallTheConverter_AndFollowsTheNullRules()
	{
		int calls = 0;
		RowMap<Order> map = LoadPair
			.Convert<long, string>(nameof(Order.OrderNum), _ => { calls++; return "x"; })
			.Convert<DateTime, string>(nameof(Order.FirstLoad), _ => { calls++; return "x"; })
			.Convert<string, string>(nameof(Order.Customer), _ => { calls++; return "x"; })
			.Convert<string, int>(nameof(Order.Quantity), _ => { calls++; return 1; });

		Order order = One(map, Columns, [7, null, null, null, null, 9, null]);

		Assert.Equal(0, calls);
		Assert.Null(order.OrderNum);            // a nullable member: null
		Assert.Null(order.FirstLoad);
		Assert.Equal("unknown", order.Customer); // a non-nullable member keeps its initializer
		Assert.Equal(5, order.Quantity);
		Assert.Equal(9, order.Count);
	}

	[Fact]
	public static void ANullableTColumn_ReceivesNull_AndWhatItReturnsIsTheValue()
	{
		RowMap<Order> map = LoadPair
			.Convert<long?, string>(nameof(Order.OrderNum), n => n is null ? "none" : "n" + n)
			.Convert<DateTime?, string>(nameof(Order.FirstLoad), t => t is null ? "-" : t.Value.ToString("HH:mm", Invariant))
			.Convert<int?, int>(nameof(Order.Count), c => c ?? -1);

		Order nulls = One(map, Columns, [7, null, null, "Ann", "12", null, null]);
		Assert.Equal("none", nulls.OrderNum);
		Assert.Equal("-", nulls.FirstLoad);
		Assert.Equal(-1, nulls.Count); // a non-nullable member, set by the converter from NULL

		Order values = One(map, Columns, Valid());
		Assert.Equal("n42", values.OrderNum);
		Assert.Equal("14:05", values.FirstLoad);
		Assert.Equal(9, values.Count);
	}

	[Fact]
	public static void AReferenceTColumn_IsNeverGivenNull_WhateverItsAnnotation()
	{
		int calls = 0;
		RowMap<Order> map = RowMap<Order>.Default
			.Convert<string?, string>(nameof(Order.Customer), s => { calls++; return s ?? "null"; })
			.Convert<string?, string?>(nameof(Order.Note), s => { calls++; return s ?? "null"; });

		Order order = One(map, Columns, [7, 42L, Start, null, "12", 9, null]);

		Assert.Equal(0, calls);
		Assert.Equal("unknown", order.Customer);
		Assert.Null(order.Note);
	}

	[Fact]
	public static void ANullableTColumn_OverAColumnOfAnotherType_ConvertsAndReceivesNull()
	{
		RowMap<Order> map = RowMap<Order>.Default.Convert<int?, int>(nameof(Order.Count), c => c ?? -1);
		ImmutableArray<ColumnDef> columns = [("Count", typeof(long))];

		Assert.Equal(-1, One(map, columns, [null]).Count);
		Assert.Equal(5, One(map, columns, [5L]).Count);
		Assert.Throws<ValueMappingException>(() => One(map, columns, [long.MaxValue]));
	}

	// ---- a column of another type than TColumn ----

	[Fact]
	public static void ABigintColumn_AnIntConverter_ConvertsAndAnOverflowThrows()
	{
		RowMap<Order> map = RowMap<Order>.Default.Convert<int, string>(
			nameof(Order.OrderNum),
			n => "#" + n.ToString(Invariant));

		Assert.Equal("#42", One(map, Columns, Valid()).OrderNum);

		var ex = Assert.Throws<ValueMappingException>(
			() => One(map, Columns, With(OrderNumAt, long.MaxValue)));
		Assert.IsType<OverflowException>(ex.InnerException);
		Assert.Equal(nameof(Order.OrderNum), ex.PropertyName);
		Assert.Equal("order_num", ex.ColumnName);
	}

	[Fact]
	public static void ATextColumn_IsReadAsTColumn_ByTheGeneralConversion()
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("order_num", typeof(string))];
		RowMap<Order> map = RowMap<Order>.Default.Convert<long, string>(
			nameof(Order.OrderNum),
			n => n.ToString("D6", Invariant));

		Assert.Equal("000042", One(map, columns, [7, "42"]).OrderNum);
		Assert.Null(One(map, columns, [7, null]).OrderNum);
		Assert.Throws<ValueMappingException>(() => One(map, columns, [7, "abc"]));
	}

	[Fact]
	public static void ADecimalColumn_IsWidenedToADoubleTColumn()
	{
		RowMap<Order> map = RowMap<Order>.Default.Convert<double, string>(
			nameof(Order.OrderNum),
			d => d.ToString("0.0", Invariant));
		ImmutableArray<ColumnDef> columns = [("order_num", typeof(decimal))];

		Assert.Equal("2.5", One(map, columns, [2.5m]).OrderNum);
	}

	sealed class CountingReader(ImmutableArray<ColumnDef> columns, object?[][] rows) : TypedReader(columns, rows)
	{
		public int GetValueCalls { get; private set; }

		public override object GetValue(int ordinal)
		{
			GetValueCalls++;
			return base.GetValue(ordinal);
		}
	}

	[Fact]
	public static void AGeneralPathCell_IsReadOnce_ForTheNullTestAndTheConversion()
	{
		ImmutableArray<ColumnDef> columns = [("order_num", typeof(string))];
		RowMap<Order> map = RowMap<Order>.Default.Convert<long, string>(
			nameof(Order.OrderNum),
			n => n.ToString("D6", Invariant));

		CountingReader? reader = null;
		var command = new FakeExecuteReader(() => reader = new CountingReader(columns, [["1"], ["2"], [null]]));

		List<Order> orders = command.Map(map).ToList();

		Assert.Equal(new string?[] { "000001", "000002", null }, orders.Select(o => o.OrderNum));
		Assert.Equal(3, reader!.GetValueCalls);
	}

	// ---- constructor parameters, required and init members ----

	static readonly ImmutableArray<ColumnDef> LineColumns =
	[
		("Id", typeof(int)),
		("Label", typeof(string)),
		("Code", typeof(string)),
		("Note", typeof(string)),
	];

	static readonly RowMap<Line> LineMap = RowMap<Line>.Default
		.Convert<string, string>("label", s => s.Trim().ToUpperInvariant()) // the constructor parameter, by the name it declares
		.Convert<string, string>(nameof(Line.Code), s => s.Trim());         // a required init property

	[Fact]
	public static void AConstructorParameter_AndARequiredInitMember_AreConverted()
	{
		Line line = One(LineMap, LineColumns, [1, " ab ", " 7 ", "n"]);

		Assert.Equal(1, line.Id);
		Assert.Equal("AB", line.Label);
		Assert.Equal("7", line.Code);
		Assert.Equal("n", line.Note);
	}

	[Fact]
	public static void ANullIntoAConstructorParameter_GetsItsDefault_NotTheConverter()
	{
		int calls = 0;
		RowMap<Line> map = RowMap<Line>.Default
			.Convert<string, string>(nameof(Line.Label), _ => { calls++; return "x"; })
			.Convert<string, string>(nameof(Line.Code), _ => { calls++; return "x"; });

		Line line = One(map, LineColumns, [1, null, null, null]);

		Assert.Equal(0, calls);
		Assert.Null(line.Label);
		Assert.Null(line.Code);
	}

	[Fact]
	public static void ARequiredMemberWithAConverter_StillNeedsItsColumn()
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Label", typeof(string))];

		var ex = Assert.Throws<ValueMappingException>(() => One(LineMap, columns, [1, "a"]));

		Assert.Equal(nameof(Line.Code), ex.PropertyName);
	}

	[Fact]
	public static void AConstructorParameter_TakesATMemberAssignableToIt()
	{
		RowMap<Point> map = RowMap<Point>.Default
			.Convert<string, int>(nameof(Point.X), s => int.Parse(s, Invariant)) // int into int?
			.Convert<string, int>(nameof(Point.Y), s => int.Parse(s, Invariant));
		ImmutableArray<ColumnDef> columns = [("X", typeof(string)), ("Y", typeof(string))];

		Point point = One(map, columns, ["12", "3"]);
		Assert.Equal(12, point.X);
		Assert.Equal(3, point.Y);

		// NULL: a nullable parameter gets null, a non-nullable one its declared default; the converter is not called.
		Point nulls = One(map, columns, [null, null]);
		Assert.Null(nulls.X);
		Assert.Equal(4, nulls.Y);
	}

	[Fact]
	public static void ATMemberAssignableToTheMember_IsAccepted()
	{
		ImmutableArray<ColumnDef> columns = [("Weight", typeof(string)), ("Tag", typeof(string)), ("Letters", typeof(string))];
		RowMap<Measure> map = RowMap<Measure>.Default
			.Convert<string, int>(nameof(Measure.Weight), s => int.Parse(s, Invariant)) // int into int?
			.Convert<string, string>(nameof(Measure.Tag), s => s)                       // string into object?
			.Convert<string, string>(nameof(Measure.Letters), s => s);                  // string into IEnumerable<char>?

		Measure measure = One(map, columns, ["12", "t", "abc"]);
		Assert.Equal(12, measure.Weight);
		Assert.Equal("t", measure.Tag);
		Assert.Equal("abc", string.Concat(measure.Letters!));

		Measure nulls = One(map, columns, [null, null, null]);
		Assert.Null(nulls.Weight);
		Assert.Null(nulls.Tag);
		Assert.Null(nulls.Letters);
	}

	[Fact]
	public static void ADelegateConvertedByVariance_IsCalledAsDeclared()
	{
		ImmutableArray<ColumnDef> columns = [("Tag", typeof(string))];
		static string Describe(object o) => "<" + o + ">";
		// Takes any object, returns a string: usable as a Func<string, string> by variance.
		Func<object, string> describe = Describe;
		static string Same(string s) => s + "!";
		Func<string, string> same = Same;

		RowMap<Measure> map = RowMap<Measure>.Default
			.Convert<string, string>(nameof(Measure.Tag), describe);
		Assert.Equal("<t>", One(map, columns, ["t"]).Tag);

		map = RowMap<Measure>.Default.Convert<string, object>(nameof(Measure.Tag), same);
		Assert.Equal("t!", One(map, columns, ["t"]).Tag);
	}

	// ---- validation, at Convert ----

	static ArgumentException Rejects(Func<RowMap<Order>> convert)
		=> Assert.Throws<ArgumentException>(() => convert());

	static readonly Func<string, string> Same = s => s;

	[Fact]
	public static void AnUnknownMember_Throws()
	{
		ArgumentException ex = Rejects(() => RowMap<Order>.Default.Convert("Nope", Same));

		Assert.Contains("'Nope'", ex.Message);
		Assert.Equal("member", ex.ParamName);
	}

	[Fact]
	public static void AnUnsettableMember_Throws()
	{
		ArgumentException ex = Rejects(() => RowMap<Order>.Default.Convert(nameof(Order.Summary), Same));

		Assert.Contains("'Summary'", ex.Message);
	}

	[Fact]
	public static void AMemberWithAConverterAlready_Throws()
	{
		RowMap<Order> once = RowMap<Order>.Default.Convert(nameof(Order.Customer), Same);

		ArgumentException ex = Rejects(() => once.Convert<string, string>(nameof(Order.Customer), s => s.Trim()));
		Assert.Contains("'Customer'", ex.Message);
		Rejects(() => once.Convert<string, string>(nameof(Order.Customer), "Customer", s => s.Trim()));

		// the same member by another name (a constructor parameter, ignoring case)
		Assert.Throws<ArgumentException>(() => LineMap.Convert("LABEL", Same));
	}

	[Fact]
	public static void ATMemberThatCannotBeAssigned_Throws()
	{
		ArgumentException ex = Rejects(() => RowMap<Order>.Default.Convert<string, string>(nameof(Order.Id), Same));

		Assert.Contains("String", ex.Message);
		Assert.Contains("Int32", ex.Message);
		Assert.Contains("'Id'", ex.Message);

		// int? into int, and int into string, are no better
		Rejects(() => RowMap<Order>.Default.Convert<int, int?>(nameof(Order.Count), n => n));
		Rejects(() => RowMap<Order>.Default.Convert<int, int>(nameof(Order.Customer), n => n));
	}

	[Fact]
	public static void NullArguments_Throw()
	{
		RowMap<Order> map = RowMap<Order>.Default;

		Assert.Equal("convert", Assert.Throws<ArgumentNullException>(() => map.Convert<string, string>(nameof(Order.Customer), null!)).ParamName);
		Assert.Equal("member", Assert.Throws<ArgumentNullException>(() => map.Convert<string, string>(null!, Same)).ParamName);
		Assert.Equal("convert", Assert.Throws<ArgumentNullException>(() => map.Convert<string, string>(nameof(Order.Customer), "Customer", null!)).ParamName);
		Assert.Equal("member", Assert.Throws<ArgumentNullException>(() => map.Convert<string, string>(null!, "Customer", Same)).ParamName);
		Assert.Equal("column", Assert.Throws<ArgumentNullException>(() => map.Convert<string, string>(nameof(Order.Customer), null!, Same)).ParamName);
	}

	[Fact]
	public static void AnEmptyColumnName_Throws()
	{
		ArgumentException ex = Rejects(() => RowMap<Order>.Default.Convert(nameof(Order.Customer), "", Same));

		Assert.Equal("column", ex.ParamName);
		Assert.Contains("empty", ex.Message);
	}

	[Fact]
	public static void AColumnAnotherMemberReads_Throws()
	{
		// Id reads the column "Id" by name; a pair gives the column to another member.
		ArgumentException byName = Rejects(() => RowMap<Order>.Default.Convert<int, string>(nameof(Order.OrderNum), "Id", n => n.ToString(Invariant)));
		Assert.Contains("'Id'", byName.Message);

		RowMap<Order> pair = [new(nameof(Order.Id), "order_id")];
		Rejects(() => pair.Convert<int, string>(nameof(Order.OrderNum), "order_id", n => n.ToString(Invariant)));
	}

	[Fact]
	public static void AMemberAPairNamesAlready_IsConvertedByNameOnly()
	{
		ArgumentException ex = Rejects(() => LoadPair.Convert<DateTime, string>(nameof(Order.FirstLoad), "first_load_datetime", t => t.ToString("HH:mm", Invariant)));
		Assert.Contains("'FirstLoad'", ex.Message);

		// the first form takes the column from the pair
		Assert.Equal("14:05", One(
			LoadPair.Convert<DateTime, string>(nameof(Order.FirstLoad), t => t.ToString("HH:mm", Invariant)),
			Columns,
			Valid()).FirstLoad);
	}

	[Fact]
	public static void AMemberMappedToNull_ReadsNoColumnToConvert()
	{
		RowMap<Order> ignored = [new(nameof(Order.Note), null)];

		ArgumentException ex = Rejects(() => ignored.Convert(nameof(Order.Note), Same));

		Assert.Contains("'Note'", ex.Message);
	}

	[Fact]
	public static void AFailedConvert_LeavesTheMapUsable()
	{
		RowMap<Order> map = LoadPair;
		Rejects(() => map.Convert("Nope", Same));
		Rejects(() => map.Convert<string, string>(nameof(Order.Id), Same));

		Order order = One(map, Columns, Valid());
		Assert.Equal(7, order.Id);
		Assert.Equal("42", order.OrderNum);

		// and the same member can still be converted
		Assert.Equal("Ann", One(map.Convert<string, string>(nameof(Order.Customer), s => s.Trim()), Columns, Valid()).Customer);
	}

	// ---- immutability ----

	[Fact]
	public static void Convert_ReturnsANewMap_AndTheOriginalIsUnchanged()
	{
		RowMap<Order> original = LoadPair;
		RowMap<Order> converted = original.Convert<long, string>(nameof(Order.OrderNum), n => n.ToString("D6", Invariant));
		RowMap<Order> more = converted.Convert<string, string>(nameof(Order.Customer), s => s.Trim());

		Assert.NotSame(original, converted);
		Assert.NotSame(converted, more);

		Order a = One(original, Columns, Valid());
		Order b = One(converted, Columns, Valid());
		Order c = One(more, Columns, Valid());
		Assert.Equal(("42", "  Ann  "), (a.OrderNum, a.Customer));
		Assert.Equal(("000042", "  Ann  "), (b.OrderNum, b.Customer));
		Assert.Equal(("000042", "Ann"), (c.OrderNum, c.Customer));

		// The original can be converted again, differently.
		RowMap<Order> other = original.Convert<long, string>(nameof(Order.OrderNum), n => "#" + n);
		Assert.Equal("#42", One(other, Columns, Valid()).OrderNum);
		Assert.Equal("000042", One(converted, Columns, Valid()).OrderNum);

		// Neither is the shared default ever changed.
		Assert.Equal("42", One(RowMap<Order>.Default, Columns, Valid()).OrderNum);
		Assert.Empty(RowMap<Order>.Default);
		Assert.Single(original);
	}

	[Fact]
	public static void EveryWayOfDeclaringAMap_TakesAConverter()
	{
		static string Pad(long n) => n.ToString("D6", Invariant);
		Func<long, string> pad = Pad;
		KeyValuePair<string, string?> note = new(nameof(Order.Note), "Note");
		RowMap<Order> declared = [note];
		RowMap<Order>[] maps =
		[
			RowMap<Order>.Default.Convert(nameof(Order.OrderNum), pad),
			new RowMap<Order>().Convert(nameof(Order.OrderNum), pad),
			declared.Convert(nameof(Order.OrderNum), pad),
			new RowMap<Order>(note).Convert(nameof(Order.OrderNum), pad),
			new RowMap<Order>(ImmutableArray.Create(note)).Convert(nameof(Order.OrderNum), pad),
			new RowMap<Order>(new List<KeyValuePair<string, string?>> { note }).Convert(nameof(Order.OrderNum), pad),
			RowMap<Order>.From([]).Convert(nameof(Order.OrderNum), pad),
			RowMap<Order>.From([note]).Convert(nameof(Order.OrderNum), pad),
		];

		foreach (RowMap<Order> map in maps)
			Assert.Equal("000042", One(map, Columns, Valid()).OrderNum);
	}

	// ---- layouts and threads ----

	[Fact]
	public static void OneMap_ManyResultSetLayouts()
	{
		ImmutableArray<ColumnDef> reordered =
		[
			("Quantity", typeof(string)),
			("order_num", typeof(string)),
			("Id", typeof(long)),
		];
		ImmutableArray<ColumnDef> few = [("order_num", typeof(long))];

		for (int round = 0; round < 4; round++)
		{
			AssertFull(One(Full, Columns, Valid()));

			Order b = One(Full, reordered, ["12", "42", 7L]);
			Assert.Equal(("000042", 12, 7), (b.OrderNum, b.Quantity, b.Id));
			Assert.Equal("unknown", b.Customer);
			Assert.Null(b.FirstLoad);
			Assert.Equal(3, b.Count);

			Order c = One(Full, few, [5L]);
			Assert.Equal("000005", c.OrderNum);
			Assert.Equal(("unknown", 5, 3), (c.Customer, c.Quantity, c.Count));
		}
	}

	[Fact]
	public static void OneMap_IsSafeOnManyThreads()
	{
		ImmutableArray<ColumnDef> few = [("order_num", typeof(long))];

		Parallel.For(0, 600, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i =>
		{
			if (i % 2 == 0)
			{
				AssertFull(One(Full, Columns, Valid()));
			}
			else
			{
				Assert.Equal("000005", One(Full, few, [5L]).OrderNum);
			}
		});
	}

	// ---- the routes a map takes: a reader, a command, a table, buffered rows, a plain data reader ----

	static DataTable Table(params object?[][] rows)
	{
		DataTable table = new();
		foreach (ColumnDef column in Columns) table.Columns.Add(column.Name, column.Type);
		foreach (object?[] row in rows) table.Rows.Add(row!);
		return table;
	}

	[Fact]
	public static async Task AReaderACommandAndATable_GiveTheSameRows()
	{
		await using (var reader = TypedReader.Create(Columns, Valid()))
			AssertFull(Assert.Single(reader.Map(Full).ToList()));

		AssertFull(Assert.Single(Cmd(Columns, Valid()).Map(Full).ToList()));
		AssertFull(Assert.Single(await Cmd(Columns, Valid()).Map(Full).ToListAsync(Ct)));
		AssertFull(Cmd(Columns, Valid()).Map(Full).ToArray().Single());
		AssertFull(Cmd(Columns, Valid()).Map(Full).FirstOrDefault()!);

		using DataTable table = Table(Valid());
		AssertFull(Assert.Single(table.Map(Full).ToList()));
		AssertFull(Assert.Single(await table.Map(Full).ToListAsync(Ct)));
		AssertFull(Assert.Single(table.Map(Full).Skip(0).Take(1).ToList()));
	}

	[Fact]
	public static void ANullRowThroughATable_FollowsTheNullRules()
	{
		using DataTable table = Table([7, null, null, null, null, null, null]);

		Order order = Assert.Single(table.Map(Full).ToList());

		Assert.Null(order.OrderNum);
		Assert.Null(order.FirstLoad);
		Assert.Equal("unknown", order.Customer);
		Assert.Equal(5, order.Quantity);
		Assert.Equal(-1, order.Count); // the nullable TColumn was called with null
	}

	static List<Order> Buffered(RowMap<Order> map, object?[] row)
	{
		string[] names = [.. Columns.Select(c => c.Name)];
		QueryResult<Queue<object[]>> result = new([.. Enumerable.Range(0, names.Length)], [.. names], new Queue<object[]>([row!]));
#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		return result.DequeueAs(map).ToList();
#pragma warning restore CS0618
	}

	[Fact]
	public static void BufferedRows_ApplyTheConverters_AndTheNullRules()
	{
		AssertFull(Assert.Single(Buffered(Full, Valid())));

		int calls = 0;
		RowMap<Order> map = LoadPair
			.Convert<long, string>(nameof(Order.OrderNum), _ => { calls++; return "x"; })
			.Convert<string, string>(nameof(Order.Customer), _ => { calls++; return "x"; })
			.Convert<int?, int>(nameof(Order.Count), c => c ?? -1);

		Order order = Assert.Single(Buffered(map, [7, null, null, null, "12", null, null]));

		Assert.Equal(0, calls);
		Assert.Null(order.OrderNum);
		Assert.Equal("unknown", order.Customer);
		Assert.Equal(-1, order.Count);

		var ex = Assert.Throws<ValueMappingException>(
			() => Buffered(RowMap<Order>.Default.Convert<int, string>(nameof(Order.OrderNum), n => n.ToString(Invariant)), With(OrderNumAt, long.MaxValue)));
		Assert.Equal(nameof(Order.OrderNum), ex.PropertyName);
	}

	[Fact]
	public static void APlainDataReader_TakesTheSameRoute()
	{
		string[] names = [.. Columns.Select(c => c.Name)];
		using IDataReader reader = FakeReader.Create(names, Valid());

		AssertFull(Assert.Single(reader.Map(Full).ToList()));
	}

	// ---- exceptions and cancellation ----

	[Fact]
	public static void AConverterThatThrows_IsReportedWithThePropertyAndColumn()
	{
		RowMap<Order> map = RowMap<Order>.Default.Convert<string, string>(
			nameof(Order.Customer),
			_ => throw new InvalidOperationException("boom"));

		var ex = Assert.Throws<ValueMappingException>(() => One(map, Columns, Valid()));

		Assert.Equal(nameof(Order.Customer), ex.PropertyName);
		Assert.Equal("Customer", ex.ColumnName);
		Assert.Equal("boom", ex.InnerException!.Message);
	}

	[Fact]
	public static void ACancellationInAConverter_PropagatesUnwrapped()
	{
		using CancellationTokenSource cts = new();
		RowMap<Order> map = RowMap<Order>.Default.Convert<string, string>(
			nameof(Order.Customer),
			s =>
			{
				cts.Cancel();
				cts.Token.ThrowIfCancellationRequested();
				return s;
			});

		Assert.ThrowsAny<OperationCanceledException>(() => Cmd(Columns, Valid()).Map(map).ToList());
	}

	[Fact]
	public static async Task ACancelledToken_StopsTheQuery_WithAConverterMap()
	{
		using CancellationTokenSource cts = new();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Cmd(Columns, Valid()).Map(Full).ToListAsync(cts.Token));
	}

	[Fact]
	public static async Task NoRows_NeverCallTheConverter()
	{
		int calls = 0;
		RowMap<Order> map = RowMap<Order>.Default
			.Convert<string, string>(nameof(Order.Customer), s => { calls++; return s; });

		Assert.Empty(Cmd(Columns).Map(map).ToList());
		Assert.Empty(await Cmd(Columns).Map(map).ToListAsync(Ct));
		Assert.Null(Cmd(Columns).Map(map).FirstOrDefault());
		Assert.Equal(0, calls);
	}

	[Fact]
	public static void EveryRow_CallsTheConverterOnce()
	{
		int calls = 0;
		RowMap<Order> map = RowMap<Order>.Default
			.Convert<string, string>(nameof(Order.Customer), s => { calls++; return s; });

		Assert.Equal(3, Cmd(Columns, Valid(), Valid(), Valid()).Map(map).ToList().Count);
		Assert.Equal(3, calls);
	}

	// ---- cost: a converter adds nothing per row ----

	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 100; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 1000; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 1000);
		}

		return best;
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void AConverter_AllocatesNothingBeyondItself_PerRow(int rows)
	{
		// A DataTableReader is a DbDataReader with typed getters: the typed path runs.
		using DataTable table = new();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Customer", typeof(string));
		table.Columns.Add("Count", typeof(long));
		for (int i = 0; i < rows; i++) table.Rows.Add(i, "x", (long)i);
		var command = new PlainReaderCommand(() => new DataTableReader(table));

		RowMap<Order> plain = RowMap<Order>.Default;
		RowMap<Order> converted = RowMap<Order>.Default
			.Convert<int, int>(nameof(Order.Id), static n => n)
			.Convert<string, string>(nameof(Order.Customer), static s => s)
			.Convert<int, int>(nameof(Order.Count), static n => n); // a bigint column: the checked conversion, then the delegate

		long mine = BytesPerCall(() => command.Map(converted).ToList());
		long theirs = BytesPerCall(() => command.Map(plain).ToList());

		Assert.True(
			mine <= theirs + 16,
			$"{rows} rows: {mine} B per call with converters, {theirs} B without (allowed 16 more).");
	}
}
