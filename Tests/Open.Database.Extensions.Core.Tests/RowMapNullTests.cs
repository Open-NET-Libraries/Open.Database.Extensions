using Open.Database.Extensions.Core;
using System.Reflection;

#nullable enable

namespace Open.Database.Extensions.Tests;

// NULL in a column that feeds a non-nullable value-type property: a strict map (the default) throws a ValueMappingException, naming the property and column;
// a RowMap<T>.NullLeavesValue map skips the assignment, so the property keeps what new T() gave it. Nullable and reference properties get null either way.
[ExcludeFromCodeCoverage]
public static class RowMapNullTests
{
	public enum Shade { None, Red, Green }

	static readonly DateTime Stamp = new(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
	static readonly DateTime Stamp2 = new(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
	static readonly Guid Initial = Guid.NewGuid(), KeyA = Guid.NewGuid(), KeyB = Guid.NewGuid();

	// Every kind of non-nullable value cell, each property with an initializer that is neither its default nor the value the rows carry.
	public sealed class Solid
	{
		public int Count { get; set; } = 7;
		public long Big { get; set; } = 8;
		public bool Flag { get; set; } = true;
		public decimal Price { get; set; } = 9.5m;
		public DateTime Created { get; set; } = Stamp;
		public Guid Key { get; set; } = Initial;
		public Shade Shade { get; set; } = Shade.Green; // read from a text column: the general path
		public Guid TextKey { get; set; } = Initial;    // read from a text column: the general path
	}

	public sealed class Optional
	{
		public int? Maybe { get; set; } = 5;
		public string? Text { get; set; } = "init";
		public Shade? Tint { get; set; } = Shade.Green;
	}

	static readonly ImmutableArray<ColumnDef> SolidColumns =
	[
		("Count", typeof(int)), ("Big", typeof(long)), ("Flag", typeof(bool)), ("Price", typeof(decimal)),
		("Created", typeof(DateTime)), ("Key", typeof(Guid)), ("Shade", typeof(string)), ("TextKey", typeof(string)),
	];

	static readonly string[] SolidNames = [.. SolidColumns.Select(c => c.Name)];

	public static TheoryData<string> NonNullable => [.. SolidNames];

	static readonly RowMap<Solid> Lenient = RowMap<Solid>.NullLeavesValue();

	// The default map, built both ways it can be built.
	static readonly RowMap<Solid>[] StrictMaps = [RowMap<Solid>.Default, new()];

	// A row of valid cells, except that the cell of `nullColumn` is NULL.
	static object?[] SolidRow(string? nullColumn = null)
		=> [.. SolidNames.Select(n => n == nullColumn ? null : n switch
		{
			"Count" => (object)1,
			"Big" => 2L,
			"Flag" => false,
			"Price" => 3.5m,
			"Created" => Stamp2,
			"Key" => KeyA,
			"Shade" => "Red",
			_ => KeyB.ToString(),
		})];

	static Solid Valid()
		=> new() { Count = 1, Big = 2, Flag = false, Price = 3.5m, Created = Stamp2, Key = KeyA, Shade = Shade.Red, TextKey = KeyB };

	static FakeExecuteReader Cmd(ImmutableArray<ColumnDef> columns, params object?[][] rows)
		=> new(() => TypedReader.Create(columns, rows));

	// The two routes a row takes to a map: a live reader (typed getters, or the boxed value for the general path) and rows already buffered as object[].
	static List<Solid> ViaReader(RowMap<Solid> map, string? nullColumn)
		=> Cmd(SolidColumns, SolidRow(nullColumn)).Map(map).ToList();

	static List<Solid> ViaArray(RowMap<Solid> map, string? nullColumn)
	{
		QueryResult<Queue<object?[]>> result = new([.. Enumerable.Range(0, SolidNames.Length)], [.. SolidNames], new Queue<object?[]>([SolidRow(nullColumn)]));
		return result.DequeueAs(map).ToList();
	}

	static readonly Func<RowMap<Solid>, string?, List<Solid>>[] Routes = [ViaReader, ViaArray];

	static void AssertSame(Solid expected, Solid actual)
	{
		foreach (PropertyInfo p in typeof(Solid).GetProperties())
		{
			object? e = p.GetValue(expected), a = p.GetValue(actual);
			Assert.True(Equals(e, a), $"{p.Name}: expected {e}, was {a}");
		}
	}

	// ---- strict (the default): NULL into a non-nullable value type throws, naming the property and the column ----

	[Theory]
	[MemberData(nameof(NonNullable))]
	public static void Strict_NullInANonNullableValueColumn_Throws_NamingThePropertyAndColumn(string column)
	{
		foreach (RowMap<Solid> map in StrictMaps)
		{
			foreach (Func<RowMap<Solid>, string?, List<Solid>> route in Routes)
			{
				var ex = Assert.Throws<ValueMappingException>(() => route(map, column));
				Assert.Contains($"Unable to set value of property '{column}' from column '{column}'.", ex.Message);
				Assert.Equal(column, ex.PropertyName);
				Assert.Equal(column, ex.ColumnName);
				Assert.Equal(typeof(Solid).GetProperty(column)!.PropertyType, ex.TargetType);
				Assert.NotNull(ex.InnerException);
			}
		}

		// Map<T>() with no map is the same default.
		Assert.Throws<ValueMappingException>(() => Cmd(SolidColumns, SolidRow(column)).Map<Solid>().ToList());
	}

	// ---- lenient (NullLeavesValue): the property keeps what new T() gave it, every other cell is read ----

	[Theory]
	[MemberData(nameof(NonNullable))]
	public static void Lenient_NullInANonNullableValueColumn_KeepsWhatNewTGave(string column)
	{
		PropertyInfo property = typeof(Solid).GetProperty(column)!;

		foreach (Func<RowMap<Solid>, string?, List<Solid>> route in Routes)
		{
			Solid expected = Valid();
			property.SetValue(expected, property.GetValue(new Solid())); // the initializer

			AssertSame(expected, Assert.Single(route(Lenient, column)));
		}
	}

	[Fact]
	public static void BothMaps_ReadEveryCellWhenNothingIsNull()
	{
		foreach (Func<RowMap<Solid>, string?, List<Solid>> route in Routes)
		{
			AssertSame(Valid(), Assert.Single(route(Lenient, null)));
			foreach (RowMap<Solid> map in StrictMaps) AssertSame(Valid(), Assert.Single(route(map, null)));
		}
	}

	[Fact]
	public static void Lenient_AFailureThatIsNotNull_StillNamesThePropertyAndColumn()
	{
		object?[] row = SolidRow();
		row[Array.IndexOf(SolidNames, "Shade")] = "Purple"; // not a member of Shade

		var ex = Assert.Throws<ValueMappingException>(() => Cmd(SolidColumns, row).Map(Lenient).ToList());
		Assert.Contains("property 'Shade' from column 'Shade'", ex.Message);
		Assert.Equal("Shade", ex.PropertyName);
		Assert.Equal("Shade", ex.ColumnName);
		Assert.Equal(typeof(Shade), ex.TargetType);
		Assert.NotNull(ex.InnerException);
	}

	// ---- nullable and reference properties get null, whichever map ----

	static void AssertNullableAndReferenceGetNull(RowMap<Optional> map)
	{
		ImmutableArray<ColumnDef> columns = [("Maybe", typeof(int)), ("Text", typeof(string)), ("Tint", typeof(string))];

		Optional viaReader = ReadOptional(columns, new object?[] { null, null, null }, map);
		Assert.Null(viaReader.Maybe);
		Assert.Null(viaReader.Text);
		Assert.Null(viaReader.Tint);

		// Buffered rows: a C# null and a DBNull both count as NULL.
		QueryResult<Queue<object?[]>> result = new([0, 1, 2], ["Maybe", "Text", "Tint"], new Queue<object?[]>([[null, DBNull.Value, DBNull.Value]]));
		Optional viaArray = Assert.Single(result.DequeueAs(map).ToList());
		Assert.Null(viaArray.Maybe);
		Assert.Null(viaArray.Text);
		Assert.Null(viaArray.Tint);
	}

	static Optional ReadOptional(ImmutableArray<ColumnDef> columns, object?[] row, RowMap<Optional> map)
		=> Assert.Single(Cmd(columns, row).Map(map).ToList());

	[Fact]
	public static void StrictMap_NullableAndReferenceProperties_GetNull()
		=> AssertNullableAndReferenceGetNull(RowMap<Optional>.Default);

	[Fact]
	public static void LenientMap_NullableAndReferenceProperties_GetNull()
		=> AssertNullableAndReferenceGetNull(RowMap<Optional>.NullLeavesValue());

	// ---- the factory ----

	[Fact]
	public static void NullLeavesValue_WithNoPairs_IsAMapOfItsOwn_NotTheDefault()
	{
		RowMap<Solid> lenient = RowMap<Solid>.NullLeavesValue();

		Assert.NotSame(RowMap<Solid>.Default, lenient);
		Assert.NotSame(lenient, RowMap<Solid>.NullLeavesValue());

		// The strict default and the lenient map do not share compiled readers: same T, same result-set layout, opposite outcomes.
		Assert.Throws<ValueMappingException>(() => ViaReader(RowMap<Solid>.Default, "Count"));
		Assert.Equal(7, Assert.Single(ViaReader(lenient, "Count")).Count);
		Assert.Throws<ValueMappingException>(() => ViaReader(RowMap<Solid>.Default, "Count"));
	}

	[Fact]
	public static void NullLeavesValue_WithARename_ReadsTheRenamedColumn()
	{
		ImmutableArray<ColumnDef> columns = [("n_count", typeof(int))];
		KeyValuePair<string, string?> rename = new(nameof(Solid.Count), "n_count");
		RowMap<Solid> lenient = RowMap<Solid>.NullLeavesValue(rename);
		var strict = new RowMap<Solid>(rename);

		Assert.Equal(5, Assert.Single(Cmd(columns, new object?[] { 5 }).Map(lenient).ToList()).Count);
		Assert.Equal(7, Assert.Single(Cmd(columns, new object?[] { null }).Map(lenient).ToList()).Count);

		var ex = Assert.Throws<ValueMappingException>(() => Cmd(columns, new object?[] { null }).Map(strict).ToList());
		Assert.Contains("property 'Count' from column 'n_count'", ex.Message);
	}

	[Fact]
	public static void NullLeavesValue_ANullColumn_StillMeansDoNotMap()
	{
		ImmutableArray<ColumnDef> columns = [("Count", typeof(int))];
		RowMap<Solid> lenient = RowMap<Solid>.NullLeavesValue([new(nameof(Solid.Count), null)]);

		Assert.Equal(7, Assert.Single(Cmd(columns, new object?[] { 99 }).Map(lenient).ToList()).Count);
	}

	[Fact]
	public static void NullLeavesValue_ValidatesLikeTheConstructor()
	{
		Assert.Throws<ArgumentException>(() => RowMap<Solid>.NullLeavesValue([new("Nope", "x")]));
		Assert.Throws<ArgumentException>(() => RowMap<Solid>.NullLeavesValue(new(nameof(Solid.Count), "x"), new(nameof(Solid.Count), "y")));
	}

	// ---- cost: strict maps read nothing extra; lenient maps pay one IsDBNull per typed non-nullable cell and read a general cell once ----

	sealed class CountingReader(ImmutableArray<ColumnDef> columns, object?[][] rows) : TypedReader(columns, rows)
	{
		public int IsDBNullCalls { get; private set; }
		public int GetValueCalls { get; private set; }

		public override bool IsDBNull(int ordinal)
		{
			IsDBNullCalls++;
			return base.IsDBNull(ordinal);
		}

		public override object GetValue(int ordinal)
		{
			GetValueCalls++;
			return base.GetValue(ordinal);
		}
	}

	static CountingReader ReadValidRows(RowMap<Solid> map, int rows)
	{
		CountingReader? reader = null;
		var command = new FakeExecuteReader(() => reader = new CountingReader(SolidColumns, [.. Enumerable.Range(0, rows).Select(_ => SolidRow())]));
		Assert.Equal(rows, command.Map(map).ToList().Count);
		return reader!;
	}

	// Pins the zero cost of the default: the compiled readers of a strict map make no IsDBNull call for a non-nullable cell.
	[Fact]
	public static void StrictMap_NeverCallsIsDBNull_ForNonNullableCells()
	{
		foreach (RowMap<Solid> map in StrictMaps)
		{
			CountingReader reader = ReadValidRows(map, 3);
			Assert.Equal(0, reader.IsDBNullCalls);
			Assert.Equal(3 * 2, reader.GetValueCalls); // the two text columns, once per row
		}
	}

	[Fact]
	public static void LenientMap_CostsOneIsDBNullPerTypedCell_AndReadsAGeneralCellOnce()
	{
		CountingReader reader = ReadValidRows(Lenient, 3);
		Assert.Equal(3 * 6, reader.IsDBNullCalls); // six typed columns per row
		Assert.Equal(3 * 2, reader.GetValueCalls); // the two general-path columns, read once per row, not once to test and once to convert
	}
}
