#nullable enable

namespace Open.Database.Extensions.Tests;

// The mapped queries open their readers with CommandBehavior.SequentialAccess, where a provider (SqlClient, for one) only lets a row's cells be read in ascending ordinal order.
// The row readers the mapping core compiles must therefore read the cells of a row in column order, whatever order the model declares its properties in, and whatever path
// (typed getter, or the boxed general conversion) each cell takes. Constructor binding (a later phase) builds the object after reading its arguments, and depends on this being
// the existing behavior; these tests pin it.
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

	static object?[] Row(int n, bool withNulls = false) =>
	[
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
