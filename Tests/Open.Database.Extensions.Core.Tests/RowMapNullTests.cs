using Open.Database.Extensions.Core;
using System.Reflection;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The declared type of the property decides what a NULL does: a nullable or oblivious reference property gets null; a non-nullable value-type property rejects it
// (the row throws ValueMappingException naming the model and the row), its initializer notwithstanding. Every map does this, RowMap<T>.Default included.
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

	static readonly Func<RowMap<Solid>, string?, List<Solid>>[] Routes = [ViaReader];

	static void AssertSame(Solid expected, Solid actual)
	{
		foreach (PropertyInfo p in typeof(Solid).GetProperties())
		{
			object? e = p.GetValue(expected), a = p.GetValue(actual);
			Assert.True(Equals(e, a), $"{p.Name}: expected {e}, was {a}");
		}
	}

	// ---- a non-nullable value-type property rejects NULL, on the typed path and the general path alike ----

	[Theory]
	[MemberData(nameof(NonNullable))]
	public static void NullInANonNullableValueColumn_IsAnError(string column)
	{
		foreach (RowMap<Solid> map in Maps)
		{
			foreach (Func<RowMap<Solid>, string?, List<Solid>> route in Routes)
				NullAssert.Model<Solid>(0, () => route(map, column));
		}

		// Map<T>() with no map is the same.
		NullAssert.Model<Solid>(0, () => Cmd(SolidColumns, SolidRow(column)).Map<Solid>().ToList());
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
	public static void AValueThatCannotBeMapped_NamesTheModelAndRow()
	{
		object?[] row = SolidRow();
		row[Array.IndexOf(SolidNames, "Shade")] = "Purple"; // not a member of Shade

		var ex = Assert.Throws<ValueMappingException>(() => Cmd(SolidColumns, row).Map(RowMap<Solid>.Default).ToList());
		NullAssert.Check<Solid, ArgumentException>("Unable to map row 0 to 'Solid'.", ex);
	}

	// ---- cost: no IsDBNull for a non-nullable cell, and a general cell is read once ----

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
	public static void DefaultMap_AsksNoIsDBNullForANonNullableCell_AndReadsAGeneralCellOnce()
	{
		foreach (RowMap<Solid> map in Maps)
		{
			CountingReader reader = ReadValidRows(map, 3);
			Assert.Equal(0, reader.IsDBNullCalls); // a NULL makes the typed getter or the conversion throw: nothing to ask first
			Assert.Equal(3 * 2, reader.GetValueCalls); // the two general-path columns, read once per row, not once to test and once to convert
		}
	}
}
