using Open.Database.Extensions.Core;
using System.Reflection;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The declared type of the property decides what a NULL does: a nullable or reference property gets null; a non-nullable value-type property keeps what new T() gave it
// (its initializer, else its default). Every map does this, RowMap<T>.Default included; only a value that cannot be mapped throws a ValueMappingException.
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

	// No initializers: a NULL leaves the default.
	public sealed class Plain
	{
		public int Count { get; set; }
		public DateTime Created { get; set; }
		public Shade Shade { get; set; }
	}

	static readonly ImmutableArray<ColumnDef> SolidColumns =
	[
		("Count", typeof(int)), ("Big", typeof(long)), ("Flag", typeof(bool)), ("Price", typeof(decimal)),
		("Created", typeof(DateTime)), ("Key", typeof(Guid)), ("Shade", typeof(string)), ("TextKey", typeof(string)),
	];

	static readonly string[] SolidNames = [.. SolidColumns.Select(c => c.Name)];

	public static TheoryData<string> NonNullable => [.. SolidNames];

	// The default map, built both ways it can be built.
	static readonly RowMap<Solid>[] Maps = [RowMap<Solid>.Default, new()];

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

	// ---- a non-nullable value-type property keeps what new T() gave it; every other cell is read ----

	[Theory]
	[MemberData(nameof(NonNullable))]
	public static void NullInANonNullableValueColumn_KeepsWhatNewTGave(string column)
	{
		PropertyInfo property = typeof(Solid).GetProperty(column)!;

		foreach (RowMap<Solid> map in Maps)
		{
			foreach (Func<RowMap<Solid>, string?, List<Solid>> route in Routes)
			{
				Solid expected = Valid();
				property.SetValue(expected, property.GetValue(new Solid())); // the initializer

				AssertSame(expected, Assert.Single(route(map, column)));
			}
		}

		// Map<T>() with no map is the same default.
		Solid viaNoMap = Assert.Single(Cmd(SolidColumns, SolidRow(column)).Map<Solid>().ToList());
		Assert.Equal(property.GetValue(new Solid()), property.GetValue(viaNoMap));
	}

	[Fact]
	public static void NullInEveryColumn_KeepsEveryInitializer()
	{
		object?[] allNull = new object?[SolidNames.Length];

		foreach (RowMap<Solid> map in Maps)
		{
			AssertSame(new Solid(), Assert.Single(Cmd(SolidColumns, allNull).Map(map).ToList()));

			QueryResult<Queue<object?[]>> result = new([.. Enumerable.Range(0, SolidNames.Length)], [.. SolidNames], new Queue<object?[]>([allNull]));
			AssertSame(new Solid(), Assert.Single(result.DequeueAs(map).ToList()));
		}
	}

	[Fact]
	public static void NullInAPropertyWithoutAnInitializer_LeavesTheDefault()
	{
		ImmutableArray<ColumnDef> columns = [("Count", typeof(int)), ("Created", typeof(DateTime)), ("Shade", typeof(string))];

		Plain viaReader = Assert.Single(Cmd(columns, new object?[] { null, null, null }).Map<Plain>().ToList());
		Assert.Equal(0, viaReader.Count);
		Assert.Equal(default, viaReader.Created);
		Assert.Equal(Shade.None, viaReader.Shade);

		QueryResult<Queue<object?[]>> result = new([0, 1, 2], ["Count", "Created", "Shade"], new Queue<object?[]>([[null, DBNull.Value, DBNull.Value]]));
		Plain viaArray = Assert.Single(result.DequeueAs(RowMap<Plain>.Default).ToList());
		Assert.Equal(0, viaArray.Count);
		Assert.Equal(default, viaArray.Created);
		Assert.Equal(Shade.None, viaArray.Shade);
	}

	[Fact]
	public static void ARealValue_StillMaps()
	{
		foreach (RowMap<Solid> map in Maps)
		{
			foreach (Func<RowMap<Solid>, string?, List<Solid>> route in Routes)
				AssertSame(Valid(), Assert.Single(route(map, null)));
		}
	}

	[Fact]
	public static void AValueThatCannotBeMapped_StillNamesThePropertyAndColumn()
	{
		object?[] row = SolidRow();
		row[Array.IndexOf(SolidNames, "Shade")] = "Purple"; // not a member of Shade

		var ex = Assert.Throws<ValueMappingException>(() => Cmd(SolidColumns, row).Map(RowMap<Solid>.Default).ToList());
		Assert.Contains("property 'Shade' (Shade) from column 'Shade'", ex.Message);
		Assert.Equal("Shade", ex.PropertyName);
		Assert.Equal("Shade", ex.ColumnName);
		Assert.Equal(typeof(Shade), ex.TargetType);
		Assert.NotNull(ex.InnerException);
	}

	// ---- nullable and reference properties get null ----

	[Fact]
	public static void NullableAndReferenceProperties_GetNull()
	{
		ImmutableArray<ColumnDef> columns = [("Maybe", typeof(int)), ("Text", typeof(string)), ("Tint", typeof(string))];

		Optional viaReader = Assert.Single(Cmd(columns, new object?[] { null, null, null }).Map(RowMap<Optional>.Default).ToList());
		Assert.Null(viaReader.Maybe);
		Assert.Null(viaReader.Text);
		Assert.Null(viaReader.Tint);

		// Buffered rows: a C# null and a DBNull both count as NULL.
		QueryResult<Queue<object?[]>> result = new([0, 1, 2], ["Maybe", "Text", "Tint"], new Queue<object?[]>([[null, DBNull.Value, DBNull.Value]]));
		Optional viaArray = Assert.Single(result.DequeueAs(RowMap<Optional>.Default).ToList());
		Assert.Null(viaArray.Maybe);
		Assert.Null(viaArray.Text);
		Assert.Null(viaArray.Tint);
	}

	// ---- cost: one IsDBNull per typed non-nullable cell, and a general cell is read once ----

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

	[Fact]
	public static void DefaultMap_CostsOneIsDBNullPerTypedCell_AndReadsAGeneralCellOnce()
	{
		foreach (RowMap<Solid> map in Maps)
		{
			CountingReader reader = ReadValidRows(map, 3);
			Assert.Equal(3 * 6, reader.IsDBNullCalls); // six typed columns per row
			Assert.Equal(3 * 2, reader.GetValueCalls); // the two general-path columns, read once per row, not once to test and once to convert
		}
	}
}
