#nullable enable

namespace Open.Database.Extensions.Tests;

// The mapped queries open their readers with CommandBehavior.SequentialAccess, where a provider (SqlClient, for one) only lets a row's cells be read in ascending ordinal order.
// The row readers the mapping core compiles must therefore read the cells of a row in column order, whatever order the model declares its properties in, and whatever path
// (typed getter, or the boxed general conversion) each cell takes. Constructor binding builds the object after reading its arguments, and depends on this being
// the existing behavior; these tests pin it, for a model built as ever and for a model built through its constructor.
[ExcludeFromCodeCoverage]
public static class SequentialAccessOrderTests
{
	// A reader configured for SequentialAccess: like SqlClient, it throws when a cell before the one last read is read, and it records every ordinal touched, row by row.
	sealed class SequentialReader(ImmutableArray<ColumnDef> columns, object?[][] rows) : TypedReader(columns, rows)
	{
		int _last = -1;

		public List<List<int>> TouchedPerRow { get; } = [];

		void Touch(int ordinal)
		{
			if (ordinal < _last)
				throw new InvalidOperationException($"Invalid attempt to read from column ordinal '{ordinal}'. With CommandBehavior.SequentialAccess, you may only read from column ordinal '{_last}' or greater.");

			_last = ordinal;
			TouchedPerRow[^1].Add(ordinal);
		}

		public override bool Read()
		{
			_last = -1;
			bool more = base.Read();
			if (more) TouchedPerRow.Add([]);
			return more;
		}

		public override bool IsDBNull(int ordinal) { Touch(ordinal); return base.IsDBNull(ordinal); }
		public override object GetValue(int ordinal) { Touch(ordinal); return base.GetValue(ordinal); }
		public override int GetInt32(int ordinal) { Touch(ordinal); return base.GetInt32(ordinal); }
		public override long GetInt64(int ordinal) { Touch(ordinal); return base.GetInt64(ordinal); }
		public override decimal GetDecimal(int ordinal) { Touch(ordinal); return base.GetDecimal(ordinal); }
		public override string GetString(int ordinal) { Touch(ordinal); return base.GetString(ordinal); }
		public override DateTime GetDateTime(int ordinal) { Touch(ordinal); return base.GetDateTime(ordinal); }
		public override bool GetBoolean(int ordinal) { Touch(ordinal); return base.GetBoolean(ordinal); }
	}

	public enum Shade { None, Red, Green }

	// Declared in the opposite order to the columns of the result sets below.
	public sealed class Declared
	{
		public int Alpha { get; set; } = 100;          // typed, non-nullable (IsDBNull, then the typed getter)
		public string? Beta { get; set; }              // typed, reference
		public decimal Gamma { get; set; }             // typed, non-nullable
		public int? Delta { get; set; }                // typed, Nullable<T>
		public Shade Epsilon { get; set; }             // from a text column: the general path
		public DateTime Zeta { get; set; }             // typed, non-nullable
		public long Eta { get; set; }                  // from an int column: widened on the typed path
	}

	static readonly DateTime Stamp = new(2021, 2, 3, 4, 5, 6, DateTimeKind.Utc);

	// Reverse of the declaration order, with unmapped columns in the gaps.
	static readonly ImmutableArray<ColumnDef> Columns =
	[
		("Eta", typeof(int)), ("unmapped_1", typeof(string)), ("Zeta", typeof(DateTime)), ("Epsilon", typeof(string)), ("Delta", typeof(int)),
		("unmapped_2", typeof(string)), ("Gamma", typeof(decimal)), ("Beta", typeof(string)), ("Alpha", typeof(int)),
	];

	static object?[] Row(int n, bool withNulls = false)
		=> [
			n, "u1", Stamp, "Green", withNulls ? null : n + 1, "u2", n * 1.5m, withNulls ? null : "beta" + n, withNulls ? null : n + 2,
		];

	static readonly int[] MappedOrdinals = [0, 2, 3, 4, 6, 7, 8];

	static SequentialReader NewReader(int rows, bool withNulls = false)
		=> new(Columns, [.. Enumerable.Range(1, rows).Select(n => Row(n, withNulls))]);

	static void AssertAscendingAndComplete(SequentialReader reader, int rows)
	{
		Assert.Equal(rows, reader.TouchedPerRow.Count);
		foreach (List<int> touched in reader.TouchedPerRow)
		{
			Assert.Equal(touched.Order(), touched); // never backwards (a repeat of the same cell, an IsDBNull before its getter, is fine)
			Assert.Equal(MappedOrdinals, touched.Distinct()); // every mapped cell, nothing else: the unmapped columns are never touched
		}
	}

	[Fact]
	public static void MapOfT_ReadsEachRowInColumnOrder_WhateverTheDeclarationOrder()
	{
		SequentialReader? reader = null;
		var command = new FakeExecuteReader(() => reader = NewReader(3));

		List<Declared> items = command.Map<Declared>().ToList();

		Assert.Equal(3, items.Count);
		for (int n = 1; n <= 3; n++)
		{
			Declared item = items[n - 1];
			Assert.Equal((n + 2, "beta" + n, n * 1.5m, n + 1, Shade.Green, Stamp, (long)n), (item.Alpha, item.Beta, item.Gamma, item.Delta, item.Epsilon, item.Zeta, item.Eta));
		}

		AssertAscendingAndComplete(reader!, 3);
	}

	[Fact]
	public static void NullCells_AreStillReadInColumnOrder()
	{
		SequentialReader? reader = null;
		var command = new FakeExecuteReader(() => reader = NewReader(2, withNulls: true));

		List<Declared> items = command.Map<Declared>().ToList();

		Assert.All(items, item =>
		{
			Assert.Equal(100, item.Alpha); // NULL into a non-nullable value type keeps the initializer
			Assert.Null(item.Beta);
			Assert.Null(item.Delta);
		});
		AssertAscendingAndComplete(reader!, 2);
	}

	[Fact]
	public static void ADeclaredMapWithRenamedColumns_ReadsInColumnOrderToo()
	{
		ImmutableArray<ColumnDef> renamed =
		[
			("c_eta", typeof(int)), ("c_zeta", typeof(DateTime)), ("c_epsilon", typeof(string)), ("c_delta", typeof(int)),
			("c_gamma", typeof(decimal)), ("c_beta", typeof(string)), ("c_alpha", typeof(int)),
		];
		var map = new RowMap<Declared>([
			new(nameof(Declared.Alpha), "c_alpha"), new(nameof(Declared.Beta), "c_beta"), new(nameof(Declared.Gamma), "c_gamma"), new(nameof(Declared.Delta), "c_delta"),
			new(nameof(Declared.Epsilon), "c_epsilon"), new(nameof(Declared.Zeta), "c_zeta"), new(nameof(Declared.Eta), "c_eta"),
		]);

		SequentialReader? reader = null;
		object?[][] data = [[1, Stamp, "Red", 2, 3.5m, "b", 4]];
		var command = new FakeExecuteReader(() => reader = new SequentialReader(renamed, data));

		Declared item = command.Map(map).ToList().Single();

		Assert.Equal((4, "b", 3.5m, 2, Shade.Red, Stamp, 1L), (item.Alpha, item.Beta, item.Gamma, item.Delta, item.Epsilon, item.Zeta, item.Eta));
		Assert.Equal([0, 1, 2, 3, 4, 5, 6], reader!.TouchedPerRow.Single().Distinct());
		Assert.Equal(reader.TouchedPerRow.Single().Order(), reader.TouchedPerRow.Single());
	}

	[Fact]
	public static async Task TheAsyncRead_ReadsInColumnOrderToo()
	{
		SequentialReader? reader = null;
		var command = new FakeExecuteReader(() => reader = NewReader(2));

		List<Declared> items = await command.Map<Declared>().ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(2, items.Count);
		AssertAscendingAndComplete(reader!, 2);
	}

	// ---- constructor binding (T7): the constructor needs every argument before any property can be set, yet the cells must still be read once, in column order ----

	// Declared in the opposite order to the columns, and with the cells that feed the constructor spread among the cells that are only set: Zeta and Eta (columns 2 and 0) are set after
	// Alpha (column 8) has been read; Epsilon is a parameter fed from a text column (the general conversion path).
	public sealed record DeclaredRecord(int Alpha, string? Beta, decimal Gamma, int? Delta, Shade Epsilon)
	{
		public DateTime Zeta { get; set; }
		public long Eta { get; set; }
	}

	// The opposite: the parameters read the first columns, and the properties that are set come after the constructor's cells.
	public sealed record LeadingParameters(long Eta, Shade Epsilon)
	{
		public DateTime Zeta { get; set; }
		public int? Delta { get; set; }
		public decimal Gamma { get; set; }
		public string? Beta { get; set; }
		public int Alpha { get; set; } = 100;
	}

	// The general-path cell (Epsilon, text to enum, column 3) is a non-nullable value-type PROPERTY here: it is read into a local, NULL-tested, and set after the constructor.
	public sealed record GeneralPathProperty(long Eta, int? Delta)
	{
		public DateTime Zeta { get; set; }
		public Shade Epsilon { get; set; } = Shade.Red;
		public decimal Gamma { get; set; }
		public string? Beta { get; set; }
		public int Alpha { get; set; } = 100;
	}

	// Each cell is touched in ascending order and the general-path cell (Epsilon, a text column, ordinal 3) is read once: not by a NULL test and then again by the conversion.
	static void AssertEachCellOnceInOrder(SequentialReader reader, int rows)
	{
		AssertAscendingAndComplete(reader, rows);
		Assert.All(reader.TouchedPerRow, touched => Assert.Equal(1, touched.Count(o => o == 3)));
		Assert.All(reader.TouchedPerRow, touched => Assert.All(touched.GroupBy(o => o), g => Assert.InRange(g.Count(), 1, 2))); // at most IsDBNull and then the getter
	}

	[Fact]
	public static void APositionalRecord_IsReadInColumnOrder_WhateverTheParameterOrder()
	{
		SequentialReader? reader = null;
		var command = new FakeExecuteReader(() => reader = NewReader(3));

		List<DeclaredRecord> items = command.Map<DeclaredRecord>().ToList();

		Assert.Equal(3, items.Count);
		for (int n = 1; n <= 3; n++)
		{
			DeclaredRecord item = items[n - 1];
			Assert.Equal((n + 2, "beta" + n, n * 1.5m, n + 1, Shade.Green, Stamp, (long)n), (item.Alpha, item.Beta, item.Gamma, item.Delta, item.Epsilon, item.Zeta, item.Eta));
		}

		AssertEachCellOnceInOrder(reader!, 3);
	}

	[Fact]
	public static void ParametersThatReadTheFirstColumns_AreReadInColumnOrderToo()
	{
		SequentialReader? reader = null;
		var command = new FakeExecuteReader(() => reader = NewReader(2));

		List<LeadingParameters> items = command.Map<LeadingParameters>().ToList();

		Assert.Equal(2, items.Count);
		for (int n = 1; n <= 2; n++)
		{
			LeadingParameters item = items[n - 1];
			Assert.Equal(((long)n, Shade.Green, Stamp, n + 1, n * 1.5m, "beta" + n, n + 2), (item.Eta, item.Epsilon, item.Zeta, item.Delta, item.Gamma, item.Beta, item.Alpha));
		}

		AssertEachCellOnceInOrder(reader!, 2);
	}

	[Fact]
	public static void AGeneralPathProperty_OfAConstructorModel_IsReadOnce_AndSetAfterTheConstructor()
	{
		SequentialReader? reader = null;
		List<GeneralPathProperty> items = new FakeExecuteReader(() => reader = NewReader(2)).Map<GeneralPathProperty>().ToList();

		Assert.Equal(2, items.Count);
		Assert.All(items, item => Assert.Equal(Shade.Green, item.Epsilon));
		AssertEachCellOnceInOrder(reader!, 2);

		// A NULL text cell keeps the property's initializer, and still reads the cell once.
		ImmutableArray<ColumnDef> columns = [("Eta", typeof(int)), ("Epsilon", typeof(string)), ("Delta", typeof(int))];
		var nulls = new SequentialReader(columns, [[1, null, null]]);
		GeneralPathProperty item = new FakeExecuteReader(() => nulls).Map<GeneralPathProperty>().ToList().Single();
		Assert.Equal((1L, Shade.Red, (int?)null), (item.Eta, item.Epsilon, item.Delta));
		Assert.Equal(1, nulls.TouchedPerRow.Single().Count(o => o == 1));
	}

	[Fact]
	public static void NullCells_OfAConstructorModel_AreStillReadInColumnOrder_AndGiveTheFallbacks()
	{
		SequentialReader? reader = null;
		var command = new FakeExecuteReader(() => reader = NewReader(2, withNulls: true));

		List<DeclaredRecord> records = command.Map<DeclaredRecord>().ToList();
		Assert.All(records, item =>
		{
			Assert.Equal(0, item.Alpha); // NULL into a non-nullable value-type parameter: its fallback, default(int)
			Assert.Null(item.Beta);
			Assert.Null(item.Delta);
		});
		AssertEachCellOnceInOrder(reader!, 2);

		List<LeadingParameters> leading = new FakeExecuteReader(() => reader = NewReader(2, withNulls: true)).Map<LeadingParameters>().ToList();
		Assert.All(leading, item =>
		{
			Assert.Equal(100, item.Alpha); // NULL into a non-nullable value-type property after the constructor: skipped, so the initializer stays
			Assert.Null(item.Beta);
			Assert.Null(item.Delta);
		});
		AssertEachCellOnceInOrder(reader!, 2);
	}

	[Fact]
	public static void ADeclaredMapOverAConstructorModel_ReadsInColumnOrderToo()
	{
		ImmutableArray<ColumnDef> renamed =
		[
			("c_eta", typeof(int)), ("c_zeta", typeof(DateTime)), ("c_epsilon", typeof(string)), ("c_delta", typeof(int)),
			("c_gamma", typeof(decimal)), ("c_beta", typeof(string)), ("c_alpha", typeof(int)),
		];

		// A pair names a parameter by its property's name (nameof), and the parameter's own name, ignoring case, would do as well.
		var map = new RowMap<DeclaredRecord>([
			new(nameof(DeclaredRecord.Alpha), "c_alpha"), new("beta", "c_beta"), new(nameof(DeclaredRecord.Gamma), "c_gamma"), new(nameof(DeclaredRecord.Delta), "c_delta"),
			new(nameof(DeclaredRecord.Epsilon), "c_epsilon"), new(nameof(DeclaredRecord.Zeta), "c_zeta"), new(nameof(DeclaredRecord.Eta), "c_eta"),
		]);

		SequentialReader? reader = null;
		object?[][] data = [[1, Stamp, "Red", 2, 3.5m, "b", 4]];
		var command = new FakeExecuteReader(() => reader = new SequentialReader(renamed, data));

		DeclaredRecord item = command.Map(map).ToList().Single();

		Assert.Equal((4, "b", 3.5m, 2, Shade.Red, Stamp, 1L), (item.Alpha, item.Beta, item.Gamma, item.Delta, item.Epsilon, item.Zeta, item.Eta));
		Assert.Equal([0, 1, 2, 3, 4, 5, 6], reader!.TouchedPerRow.Single().Distinct());
		Assert.Equal(reader.TouchedPerRow.Single().Order(), reader.TouchedPerRow.Single());
	}

	[Fact]
	public static async Task TheAsyncRead_OfAConstructorModel_ReadsInColumnOrderToo()
	{
		SequentialReader? reader = null;
		var command = new FakeExecuteReader(() => reader = NewReader(2));

		List<DeclaredRecord> items = await command.Map<DeclaredRecord>().ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(2, items.Count);
		AssertEachCellOnceInOrder(reader!, 2);
	}

	// The fake itself: a reader that goes back must throw, or the tests above prove nothing.
	[Fact]
	public static void TheFakeReader_RefusesToGoBackToAnEarlierCell()
	{
		SequentialReader reader = NewReader(1);
		Assert.True(reader.Read());
		Assert.Equal(3, reader.GetInt32(8)); // Alpha = n + 2
		var ex = Assert.Throws<InvalidOperationException>(() => reader.GetInt32(0));
		Assert.Contains("SequentialAccess", ex.Message);
	}
}
