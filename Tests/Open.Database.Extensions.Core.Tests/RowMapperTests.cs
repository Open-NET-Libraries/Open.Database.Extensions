#nullable enable

namespace Open.Database.Extensions.Tests;

// The generated row mappers: typed getters, conversions, NULL handling, layout caching, overrides, and the
// read loops (skip/take/first/single). These run over TypedReader, which reports real column types, so the
// mappers take their typed (unboxed) path rather than the boxed fallback the NSubstitute fakes exercise.
[ExcludeFromCodeCoverage]
public static class RowMapperTests
{
	public sealed class Wide
	{
		public int Id { get; set; }
		public long Big { get; set; }
		public short Small { get; set; }
		public bool Flag { get; set; }
		public double Score { get; set; }
		public decimal Price { get; set; }
		public DateTime Created { get; set; }
		public Guid Key { get; set; }
		public string Title { get; set; } = "";
		public string? Body { get; set; }
		public int? Views { get; set; }
		public DateTime? Seen { get; set; }
	}

	public enum Color { None, Red, Green }

	public sealed class Converted
	{
		public long FromInt { get; set; }
		public int FromLong { get; set; }
		public double FromDecimal { get; set; }
		public Color FromInt32Enum { get; set; }
		public Color FromString { get; set; }
		public Guid FromGuidString { get; set; }
		public string? FromInt32String { get; set; }
		public int? NullableFromLong { get; set; }
	}

	public sealed class Simple
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	static readonly (string, Type)[] WideColumns =
	[
		("Id", typeof(int)), ("Big", typeof(long)), ("Small", typeof(short)), ("Flag", typeof(bool)),
		("Score", typeof(double)), ("Price", typeof(decimal)), ("Created", typeof(DateTime)), ("Key", typeof(Guid)),
		("Title", typeof(string)), ("Body", typeof(string)), ("Views", typeof(int)), ("Seen", typeof(DateTime)),
	];

	static readonly DateTime When = new(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
	static readonly Guid Id1 = Guid.NewGuid(), Id2 = Guid.NewGuid();

	static object?[][] WideRows() =>
	[
		[1, 10_000_000_000L, (short)7, true, 1.5, 12.25m, When, Id1, "first", "has body", 5, When],
		[2, -1L, (short)-3, false, -2.5, 0m, When.AddDays(1), Id2, "second", null, null, null],
	];

	static FakeExecuteReader Cmd((string, Type)[] columns, params object?[][] rows)
		=> new(() => TypedReader.Create(columns, rows));

	static FakeExecuteReader Cmd(out Func<TypedReader?> reader, (string, Type)[] columns, params object?[][] rows)
	{
		TypedReader? last = null;
		reader = () => last;
		return new(() => last = TypedReader.Create(columns, rows));
	}

	static FakeExecuteReader SimpleRows(int count)
		=> Cmd([("Id", typeof(int)), ("Name", typeof(string))],
			Enumerable.Range(1, count).Select(i => new object?[] { i, "n" + i }).ToArray());

	// ---- typed mapping ----

	[Fact]
	public static void Typed_MapsEveryType_IncludingNulls()
	{
		var items = Cmd(WideColumns, WideRows()).Map<Wide>().ToList();

		Assert.Equal(2, items.Count);
		Wide a = items[0], b = items[1];
		Assert.Equal(1, a.Id);
		Assert.Equal(10_000_000_000L, a.Big);
		Assert.Equal((short)7, a.Small);
		Assert.True(a.Flag);
		Assert.Equal(1.5, a.Score);
		Assert.Equal(12.25m, a.Price);
		Assert.Equal(When, a.Created);
		Assert.Equal(Id1, a.Key);
		Assert.Equal("first", a.Title);
		Assert.Equal("has body", a.Body);
		Assert.Equal(5, a.Views);
		Assert.Equal(When, a.Seen);

		Assert.Equal(2, b.Id);
		Assert.False(b.Flag);
		Assert.Equal(Id2, b.Key);
		Assert.Null(b.Body);
		Assert.Null(b.Views);
		Assert.Null(b.Seen);
	}

	[Fact]
	public static void Typed_NullInNonNullableValueProperty_ThrowsNamingTheColumn()
	{
		var cmd = Cmd([("Id", typeof(int)), ("Name", typeof(string))], [null, "x"]);

		var ex = Assert.Throws<InvalidOperationException>(() => cmd.Map<Simple>().ToList());
		Assert.Contains("Id", ex.Message);
		Assert.NotNull(ex.InnerException);
	}

	[Fact]
	public static void Typed_ConversionsBetweenColumnAndPropertyTypes()
	{
		var cmd = Cmd(
			[
				("FromInt", typeof(int)), ("FromLong", typeof(long)), ("FromDecimal", typeof(decimal)),
				("FromInt32Enum", typeof(int)), ("FromString", typeof(string)), ("FromGuidString", typeof(string)),
				("FromInt32String", typeof(int)), ("NullableFromLong", typeof(long)),
			],
			[7, 8L, 9.5m, 2, "Green", Id1.ToString(), 42, 11L],
			[8, 9L, 1m, 1, "Red", Id2.ToString(), 43, null]);

		var items = cmd.Map<Converted>().ToList();

		Assert.Equal(7L, items[0].FromInt);
		Assert.Equal(8, items[0].FromLong);
		Assert.Equal(9.5, items[0].FromDecimal);
		Assert.Equal(Color.Green, items[0].FromInt32Enum);
		Assert.Equal(Color.Green, items[0].FromString);
		Assert.Equal(Id1, items[0].FromGuidString);
		Assert.Equal("42", items[0].FromInt32String);
		Assert.Equal(11, items[0].NullableFromLong);

		Assert.Equal(Color.Red, items[1].FromInt32Enum);
		Assert.Equal(Color.Red, items[1].FromString);
		Assert.Equal(Id2, items[1].FromGuidString);
		Assert.Null(items[1].NullableFromLong);
	}

	[Fact]
	public static void Columns_AreMatchedIgnoringCase_AndUnmappedOnesIgnored()
	{
		var cmd = Cmd([("ID", typeof(int)), ("nAmE", typeof(string)), ("Extra", typeof(string))], [5, "five", "ignored"]);

		Simple item = cmd.Map<Simple>().ToList().Single();
		Assert.Equal(5, item.Id);
		Assert.Equal("five", item.Name);
	}

	[Fact]
	public static void MissingColumns_LeaveDefaults()
	{
		Simple item = Cmd([("Id", typeof(int))], [9]).Map<Simple>().ToList().Single();
		Assert.Equal(9, item.Id);
		Assert.Null(item.Name);
	}

	[Fact]
	public static void Overrides_RenameAndIgnoreFields()
	{
		var cmd = Cmd([("Key", typeof(int)), ("Name", typeof(string)), ("Id", typeof(int))], [3, "renamed", 99]);

		Simple item = cmd.Map<Simple>(("Id", "Key"), ("Name", null)).ToList().Single();
		Assert.Equal(3, item.Id);
		Assert.Null(item.Name); // Ignored.
	}

	[Fact]
	public static void Overrides_UnknownField_Throws()
	{
		var cmd = Cmd([("Id", typeof(int))], [1]);
		Assert.Throws<ArgumentException>(() => cmd.Map<Simple>(("Nope", "Id")));
	}

	[Fact]
	public static void DuplicateColumnNames_FirstWins()
	{
		var cmd = Cmd([("Id", typeof(int)), ("Id", typeof(int))], [1, 2]);
		Assert.Equal(1, cmd.Map<Simple>().ToList().Single().Id);
	}

	// More distinct overrides (and layouts) than the cache bounds: results stay correct when entries are evicted.
	[Fact]
	public static void Caches_StayCorrectPastTheirBounds()
	{
		for (int i = 0; i < 300; i++)
		{
			string column = "Key" + i;
			Simple item = Cmd([(column, typeof(int)), ("Name", typeof(string))], [i, "n"]).Map<Simple>(("Id", column)).ToList().Single();
			Assert.Equal(i, item.Id);
		}
	}

	[Fact]
	public static void Plans_AreCachedPerLayout_AndSwitchBetweenLayouts()
	{
		(string, Type)[] a = [("Id", typeof(int)), ("Name", typeof(string))];
		(string, Type)[] b = [("Name", typeof(string)), ("Id", typeof(long))]; // reordered + different type.

		for (int i = 0; i < 3; i++)
		{
			Simple x = Cmd(a, [1, "one"]).Map<Simple>().ToList().Single();
			Simple y = Cmd(b, ["two", 2L]).Map<Simple>().ToList().Single();
			Assert.Equal((1, "one"), (x.Id, x.Name));
			Assert.Equal((2, "two"), (y.Id, y.Name));
		}
	}

	// ---- loops ----

	[Fact]
	public static void Take_StopsReadingOnceSatisfied()
	{
		var cmd = Cmd(out var reader, [("Id", typeof(int))], [1], [2], [3], [4]);

		Assert.Equal([1, 2], cmd.Map<Simple>().Take(2).ToList().Select(s => s.Id));
		Assert.Equal(2, reader()!.RowsRead);
	}

	[Fact]
	public static void SkipThenTake_OnArray()
	{
		var items = SimpleRows(10).Map<Simple>().Skip(3).Take(4).ToArray();
		Assert.Equal([4, 5, 6, 7], items.Select(s => s.Id));
	}

	[Fact]
	public static void Take_Zero_ReadsNothing()
	{
		var cmd = Cmd(out var reader, [("Id", typeof(int))], [1], [2]);

		Assert.Empty(cmd.Map<Simple>().Take(0).ToList());
		Assert.Empty(cmd.Map<Simple>().Take(0).ToArray());
		Assert.Equal(0, reader()!.RowsRead);
	}

	[Fact]
	public static void Skip_PastTheEnd_IsEmpty()
	{
		Assert.Empty(SimpleRows(3).Map<Simple>().Skip(10).ToList());
		Assert.Empty(SimpleRows(3).Map<Simple>().Skip(10).ToArray());
		Assert.Null(SimpleRows(3).Map<Simple>().Skip(10).FirstOrDefault());
	}

	[Fact]
	public static void FirstOrDefault_ReadsOnlyOneRow()
	{
		var cmd = Cmd(out var reader, [("Id", typeof(int))], [1], [2], [3]);

		Assert.Equal(1, cmd.Map<Simple>().FirstOrDefault()!.Id);
		Assert.Equal(1, reader()!.RowsRead);
	}

	[Fact]
	public static void FirstOrDefault_AfterSkip()
	{
		Assert.Equal(3, SimpleRows(5).Map<Simple>().Skip(2).FirstOrDefault()!.Id);
	}

	[Fact]
	public static void SingleOrDefault_ThrowsOnSecondRow_UnlessTakeLimitsIt()
	{
		Assert.Throws<InvalidOperationException>(() => SimpleRows(2).Map<Simple>().SingleOrDefault());
		Assert.Equal(1, SimpleRows(2).Map<Simple>().Take(1).SingleOrDefault()!.Id);
		Assert.Null(SimpleRows(0).Map<Simple>().SingleOrDefault());
	}

	public struct Point
	{
		public int X { get; set; }
		public int Y { get; set; }
	}

	[Fact]
	public static void ValueTypeModels_AreMapped()
	{
		var points = Cmd([("X", typeof(int)), ("Y", typeof(int))], [1, 2], [3, 4]).Map<Point>().ToArray();
		Assert.Equal([(1, 2), (3, 4)], points.Select(p => (p.X, p.Y)));
	}

	public sealed class Guarded
	{
		public int Id { get; set; }
		public int ReadOnly { get; } = 5;
		public int PrivateSet { get; private set; } = 6;
	}

	[Fact]
	public static void OnlyPublicSettersAreMapped()
	{
		var item = Cmd([("Id", typeof(int)), ("ReadOnly", typeof(int)), ("PrivateSet", typeof(int))], [1, 99, 99]).Map<Guarded>().ToList().Single();
		Assert.Equal((1, 5, 6), (item.Id, item.ReadOnly, item.PrivateSet));
	}

	[Fact]
	public static void Selector_RunsOverTypedReader()
	{
		var items = SimpleRows(3).Map(r => r.GetInt32(0) * 10).ToList();
		Assert.Equal([10, 20, 30], items);
	}

	[Fact]
	public static void Results_OverPlainDataReader_UsesTheRecordPath()
	{
		var reader = FakeReader.Create(["Id", "Name"], [1, "a"], [2, "b"]);
		Assert.Equal([1, 2], reader.Results<Simple>().Select(s => s.Id));
	}

	[Fact]
	public static void DictionaryHashSetAndImmutableArray_BuildDirectly()
	{
		var dictionary = SimpleRows(4).Map<Simple>().Skip(1).ToDictionary(s => s.Id);
		Assert.Equal([2, 3, 4], dictionary.Keys.OrderBy(k => k));
		Assert.Equal("n3", dictionary[3].Name);

		var set = SimpleRows(4).Map<Simple>().Take(3).ToHashSet(new IdComparer());
		Assert.Equal(3, set.Count);

		var immutable = SimpleRows(3).Map<Simple>().ToImmutableArray();
		Assert.Equal([1, 2, 3], immutable.Select(s => s.Id));
		Assert.True(SimpleRows(0).Map<Simple>().ToImmutableArray().IsEmpty);
	}

	[Fact]
	public static void Dictionary_DuplicateKey_Throws()
		=> Assert.Throws<ArgumentException>(() => SimpleRows(3).Map<Simple>().ToDictionary(_ => 1));

	sealed class IdComparer : IEqualityComparer<Simple>
	{
		public bool Equals(Simple? x, Simple? y) => x?.Id == y?.Id;
		public int GetHashCode(Simple obj) => obj.Id;
	}

	// ---- async ----

	[Fact]
	public static async Task Async_DictionaryHashSetAndImmutableArray()
	{
		var ct = TestContext.Current.CancellationToken;

		var dictionary = await SimpleRows(4).Map<Simple>().Take(2).ToDictionaryAsync(s => s.Name!, cancellationToken: ct);
		Assert.Equal(["n1", "n2"], dictionary.Keys.OrderBy(k => k));

		var set = await SimpleRows(5).Map<Simple>().Skip(1).ToHashSetAsync(new IdComparer(), ct);
		Assert.Equal(4, set.Count);

		var immutable = await SimpleRows(3).Map<Simple>().ToImmutableArrayAsync(ct);
		Assert.Equal([1, 2, 3], immutable.Select(s => s.Id));
	}

	[Fact]
	public static async Task Async_ListArrayFirstSingle()
	{
		var ct = TestContext.Current.CancellationToken;

		var list = await SimpleRows(4).Map<Simple>().ToListAsync(ct);
		Assert.Equal([1, 2, 3, 4], list.Select(s => s.Id));

		var array = await SimpleRows(4).Map<Simple>().Skip(1).Take(2).ToArrayAsync(ct);
		Assert.Equal([2, 3], array.Select(s => s.Id));

		var first = await SimpleRows(4).Map<Simple>().Skip(2).FirstOrDefaultAsync(ct);
		Assert.Equal(3, first!.Id);

		Assert.Null(await SimpleRows(0).Map<Simple>().FirstOrDefaultAsync(ct));
		Assert.Equal(1, (await SimpleRows(1).Map<Simple>().SingleOrDefaultAsync(ct))!.Id);
		await Assert.ThrowsAsync<InvalidOperationException>(async () => await SimpleRows(2).Map<Simple>().SingleOrDefaultAsync(ct));
	}

	[Fact]
	public static async Task Async_Stream_HonorsSkipAndTake()
	{
		var ct = TestContext.Current.CancellationToken;
		var ids = new List<int>();

		await SimpleRows(10).Map<Simple>().Skip(2).Take(3).ReadAsync(async (items, token) =>
		{
			await foreach (Simple s in items.WithCancellation(token)) ids.Add(s.Id);
		}, ct);

		Assert.Equal([3, 4, 5], ids);
	}

	[Fact]
	public static async Task Async_Cancelled_Throws()
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await SimpleRows(3).Map<Simple>().ToListAsync(cts.Token));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_WithAndWithoutReadAsync()
	{
		var ct = TestContext.Current.CancellationToken;
		foreach (bool useReadAsync in new[] { true, false })
		{
			using var reader = TypedReader.Create([("Id", typeof(int)), ("Name", typeof(string))], [1, "a"], [2, "b"]);
			var items = (await reader.ResultsBufferedAsync<Simple>(null, useReadAsync, ct)).ToList();
			Assert.Equal([1, 2], items.Select(s => s.Id));
		}
	}

	[Fact]
	public static async Task ResultsAsync_StreamsOverTypedReader()
	{
		var ct = TestContext.Current.CancellationToken;
		using var reader = TypedReader.Create([("Id", typeof(int)), ("Name", typeof(string))], [1, "a"], [2, "b"]);

		var ids = new List<int>();
		await foreach (Simple s in reader.ResultsAsync<Simple>(cancellationToken: ct)) ids.Add(s.Id);
		Assert.Equal([1, 2], ids);
	}
}
