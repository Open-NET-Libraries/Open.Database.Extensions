using Open.Database.Extensions.Core;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// RowMap<T>: a predefined, validated, read-only mapping that every override parameter accepts.
[ExcludeFromCodeCoverage]
public static class RowMapTests
{
	internal sealed class Item
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public decimal Total { get; set; }
		public string? Note { get; set; }
		public string Computed => "computed";   // read-only: cannot be mapped
		public string? Locked { get; private set; } // no public setter: cannot be mapped
	}

	// The same properties under another type, and a model with fewer of them.
	internal sealed class Customer
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	internal static readonly string[] Renamed = ["item_id", "item_name", "item_total", "item_note"];

	// Every property under a different column name; defined once and shared by every test, like an application would.
	internal static readonly RowMap<Item> Plain = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];

	internal static object?[] Row(string[] columns, int i)
		=> [.. columns.Select(c => c switch
		{
			"item_id" or "id" => (object?)i,
			"item_name" or "NAME" => "name" + i,
			"item_total" or "total" => i * 1.5m,
			"item_note" => i % 2 == 0 ? null : "note" + i,
			_ => "extra",
		})];

	internal static FakeExecuteReader Cmd(string[] columns, int rows)
		=> new(columns, [.. Enumerable.Range(1, rows).Select(i => Row(columns, i))]);

	internal static void AssertItems(IReadOnlyList<Item> items, int rows, bool hasNote = true)
	{
		Assert.Equal(rows, items.Count);
		for (int n = 0; n < rows; n++)
		{
			int i = n + 1;
			Assert.Equal(i, items[n].Id);
			Assert.Equal("name" + i, items[n].Name);
			Assert.Equal(i * 1.5m, items[n].Total);
			Assert.Equal(hasNote && i % 2 != 0 ? "note" + i : null, items[n].Note);
		}
	}

	// ---- one declaration serves every query that reads the model ----

	[Fact]
	public static void Map_MapsRenamedColumns()
		=> AssertItems(Cmd(Renamed, 3).Map<Item>(Plain).ToList(), 3);

	[Fact]
	public static void OneMap_ServesEveryQueryThatReadsTheModel()
	{
		AssertItems(Cmd(Renamed, 0).Map<Item>(Plain).ToList(), 0);
		AssertItems(Cmd(Renamed, 1).Map<Item>(Plain).ToList(), 1);
		AssertItems([Cmd(Renamed, 1).Map<Item>(Plain).FirstOrDefault()!], 1);
		AssertItems([Cmd(Renamed, 1).Map<Item>(Plain).SingleOrDefault()!], 1);
		AssertItems(Cmd(Renamed, 50).Map<Item>(Plain).ToList(), 50);

		List<Item> page = Cmd(Renamed, 50).Map<Item>(Plain).Skip(10).Take(5).ToList();
		Assert.Equal([11, 12, 13, 14, 15], page.Select(x => x.Id));
	}

	[Fact]
	public static async Task Map_Async_MapsRenamedColumns()
	{
		AssertItems(await Cmd(Renamed, 4).Map<Item>(Plain).ToListAsync(TestContext.Current.CancellationToken), 4);
		Item? first = await Cmd(Renamed, 4).Map<Item>(Plain).FirstOrDefaultAsync(TestContext.Current.CancellationToken);
		AssertItems([first!], 1);
	}

	[Fact]
	public static void PassingAMap_AllocatesNothing()
	{
		FakeExecuteReader cmd = Cmd(Renamed, 1);
		_ = cmd.Map<Item>(Plain); // warm up
		long before = GC.GetAllocatedBytesForCurrentThread();
		MappedQuery<Item> query = cmd.Map<Item>(Plain);
		long after = GC.GetAllocatedBytesForCurrentThread();
		GC.KeepAlive(query);
		Assert.Equal(0, after - before);
	}

	// ---- validated where it is created ----

	[Fact]
	public static void UnknownProperty_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<Item>([new("Nope", "x")])); // "Nope" is deliberately not a property of Item
		Assert.Contains("Nope", ex.Message);
	}

	[Fact]
	public static void UnknownProperty_EvenWithANullColumn_Throws()
		=> Assert.Throws<ArgumentException>(() => RowMap.Create<Item>([new("Nope", null)])); // "Nope" is deliberately not a property of Item

	[Theory]
	[InlineData(nameof(Item.Computed))]
	[InlineData(nameof(Item.Locked))]
	public static void PropertyThatCannotBeSet_Throws(string property)
	{
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<Item>([new(property, "x")]));
		Assert.Contains(property, ex.Message);
	}

	[Fact]
	public static void NullPropertyName_Throws()
		=> Assert.Throws<ArgumentException>(() => RowMap.Create<Item>([new(null!, "x")]));

	[Fact]
	public static void PropertyNamedTwice_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<Item>(new(nameof(Item.Name), "a"), new(nameof(Item.Name), "b")));
		Assert.Contains("Name", ex.Message);
	}

	[Fact]
	public static void TwoPropertiesOnOneColumn_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<Item>(new(nameof(Item.Name), "x"), new(nameof(Item.Note), "X"))); // columns match ignoring case
		Assert.Contains("Name", ex.Message);
		Assert.Contains("Note", ex.Message);
	}

	[Fact]
	public static void ColumnTakenByAnUnmentionedProperty_Throws()
	{
		// Id is not mentioned, so it still reads column "Id"; so would Name.
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<Item>([new(nameof(Item.Name), "Id")]));
		Assert.Contains("Id", ex.Message);
		Assert.Contains("Name", ex.Message);
	}

	[Fact]
	public static void SwappingTwoColumns_IsFine()
	{
		var swap = RowMap.Create<Item>(new(nameof(Item.Name), "Note"), new(nameof(Item.Note), "Name"));
		Item item = new FakeExecuteReader(["Name", "Note"], ["n", "o"]).Map<Item>(swap).ToList().Single();
		Assert.Equal("o", item.Name);
		Assert.Equal("n", item.Note);
	}

	// ---- what the map does not mention still maps by name ----

	[Fact]
	public static void UnmentionedProperties_MapByName_IgnoringCase()
	{
		var onlyTotal = RowMap.Create<Item>([new(nameof(Item.Total), "item_total")]);
		Item item = new FakeExecuteReader(["id", "NAME", "item_total"], [7, "seven", 10.5m]).Map<Item>(onlyTotal).ToList().Single();
		Assert.Equal(7, item.Id);
		Assert.Equal("seven", item.Name);
		Assert.Equal(10.5m, item.Total);
	}

	[Fact]
	public static void EmptyMap_BehavesLikeMatchingNames()
	{
		Item item = new FakeExecuteReader(["Id", "Name"], [3, "three"]).Map<Item>(RowMap.Create<Item>()).ToList().Single();
		Assert.Equal(3, item.Id);
		Assert.Equal("three", item.Name);
	}

	[Fact]
	public static void NullColumn_IgnoresTheProperty()
	{
		var ignoreNote = RowMap.Create<Item>(new(nameof(Item.Name), "item_name"), new(nameof(Item.Note), null));
		Item item = new FakeExecuteReader(["item_name", "Note"], ["a", "should not land"]).Map<Item>(ignoreNote).ToList().Single();
		Assert.Equal("a", item.Name);
		Assert.Null(item.Note);
	}

	// ---- only public instance properties are mapped ----

	internal sealed class WithStatic
	{
		public int Id { get; set; }
		public static string? Shared { get; set; }
	}

	internal sealed class WithInit
	{
		public int Id { get; init; }
	}

	[Fact]
	public static void StaticProperty_NamedByAMap_FailsLikeAnUnknownProperty()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<WithStatic>([new(nameof(WithStatic.Shared), "x")]));
		Assert.Contains("Shared", ex.Message);
		Assert.Contains("no settable public property", ex.Message);
	}

	[Fact]
	public static void StaticProperty_IsIgnoredByTheDefaultMapping_EvenWhenAColumnHasItsName()
	{
		WithStatic.Shared = null;
		WithStatic row = new FakeExecuteReader(["Id", "Shared"], [1, "from the column"]).Map<WithStatic>().ToList().Single();
		Assert.Equal(1, row.Id);
		Assert.Null(WithStatic.Shared);
	}

	[Fact]
	public static void InitOnlySetter_IsAccepted()
		=> Assert.Equal(5, new FakeExecuteReader(["x"], [5]).Map<WithInit>(RowMap.Create<WithInit>([new(nameof(WithInit.Id), "x")])).ToList().Single().Id);

	// ---- validate only what the map mentions; an empty column name is a mistake ----

	internal sealed class CaseTwins
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public string? NAME { get; set; }
	}

	[Fact]
	public static void ModelWithPropertiesDifferingOnlyByCase_IsAcceptedWhenTheMapDoesNotTouchThem()
	{
		// Map<T>() accepts the model (the first customer reads the column), so a map that does not mention the twins must too.
		Assert.Equal(1, new FakeExecuteReader(["Id"], [1]).Map<CaseTwins>().ToList().Single().Id);
		_ = RowMap.Create<CaseTwins>();
		var map = RowMap.Create<CaseTwins>([new(nameof(CaseTwins.Id), "c_id")]);
		Assert.Equal(1, new FakeExecuteReader(["c_id"], [1]).Map<CaseTwins>(map).ToList().Single().Id);
	}

	[Fact]
	public static void MentionedProperty_CollidingWithACaseTwin_StillThrows()
		=> Assert.Throws<ArgumentException>(() => RowMap.Create<CaseTwins>([new(nameof(CaseTwins.Id), "name")]));

	[Fact]
	public static void EmptyColumnName_Throws_WhileNullStillIgnores()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<Item>([new(nameof(Item.Name), "")]));
		Assert.Contains("Name", ex.Message);
		_ = RowMap.Create<Item>([new(nameof(Item.Name), null)]);
	}

	// ---- a RowMap read allocates what Map<T>() allocates on matching names ----

	// Identical result shapes (same types, same rows) and a provider that serves stored name strings, so the only
	// difference between the two readers is the column names, and they cost the provider nothing to report. (The
	// real-provider caveat is in Benchmarks/RESULTS.md: SqlClient allocates longer column names on every query.)
	static readonly ImmutableArray<ColumnDef> RenamedColumns = [("item_id", typeof(int)), ("item_name", typeof(string)), ("item_total", typeof(decimal)), ("item_note", typeof(string))];
	static readonly ImmutableArray<ColumnDef> MatchingColumns = [("Id", typeof(int)), ("Name", typeof(string)), ("Total", typeof(decimal)), ("Note", typeof(string))];

	// The smallest allocation per call over several rounds, after warm-up (the layout is compiled and cached by then).
	static long BytesPerRead(Func<List<Item>> read)
	{
		for (int i = 0; i < 5; i++) read();
		long best = long.MaxValue;
		for (int round = 0; round < 5; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 20; i++) read();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 20);
		}

		return best;
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void RowMapRead_AllocatesWhatMapOfTAllocatesOnMatchingNames(int rows)
	{
		object?[][] data = [.. Enumerable.Range(1, rows).Select(i => (object?[])[i, "name" + i, i * 1.5m, i % 2 == 0 ? null : "note" + i])];
		var renamed = new FakeExecuteReader(() => TypedReader.Create(RenamedColumns, data));
		var matching = new FakeExecuteReader(() => TypedReader.Create(MatchingColumns, data));

		long viaRowMap = BytesPerRead(() => renamed.Map<Item>(Plain).ToList());
		long byName = BytesPerRead(() => matching.Map<Item>().ToList());
		long perCall = BytesPerRead(() => renamed.Map<Item>((nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")).ToList());

		Assert.InRange(viaRowMap - byName, -16, 16);
		Assert.True(perCall - byName > 400, $"Per-call overrides were expected to cost visibly more ({perCall} vs {byName} B); otherwise this test cannot tell the difference."); // the control
	}

	// ---- spellings: collection expression, factory, implicit conversion ----

	internal static readonly ImmutableArray<KeyValuePair<string, string?>> ItemPairs =
	[
		new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"),
	];

	[Fact]
	public static void CollectionExpression_Factory_AndConversion_BuildTheSameMap()
	{
		RowMap<Item> viaExpression = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];
		RowMap<Item> viaFactory = RowMap.Create<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"));
		RowMap<Item> viaConversion = ItemPairs; // the implicit conversion from an ImmutableArray, at a declaration

		foreach (RowMap<Item> map in new[] { viaExpression, viaFactory, viaConversion })
		{
			AssertItems(Cmd(Renamed, 3).Map<Item>(map).ToList(), 3);
			Assert.Equal(ItemPairs, [.. (IEnumerable<KeyValuePair<string, string?>>)map]);
		}

		// Each spelling validates where it is declared ("Nope" is deliberately not a property).
		Assert.Throws<ArgumentException>(() => { RowMap<Item> bad = [new("Nope", "x")]; return bad; });
		Assert.Throws<ArgumentException>(() => { ImmutableArray<KeyValuePair<string, string?>> pairs = [new("Nope", "x")]; RowMap<Item> bad = pairs; return bad; });
	}

	[Fact]
	public static void DefaultImmutableArray_Throws()
		=> Assert.Throws<ArgumentException>(() => { RowMap<Item> map = default(ImmutableArray<KeyValuePair<string, string?>>); return map; });

	// ---- a null column means "do not map this property" ----

	[Fact]
	public static void NullColumn_MeansDoNotMapTheProperty_NotEvenByName()
	{
		// The result set has a Note column, but it holds bytes that a string property cannot take.
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Note", typeof(byte[]))];
		object?[][] data = [[1, new byte[] { 1, 2, 3 }]];
		var command = new FakeExecuteReader(() => TypedReader.Create(columns, data));

		// By name, Note would be read from that column, and the read fails.
		Assert.ThrowsAny<Exception>(() => command.Map<Item>().ToList());

		// With a null column Note is not read at all (what is read: Id; what stays default: Note).
		RowMap<Item> ignoreNote = [new(nameof(Item.Note), null)];
		Item item = command.Map<Item>(ignoreNote).ToList().Single();
		Assert.Equal(1, item.Id);
		Assert.Null(item.Note);
	}

	// ---- a map written at the call site, or tuples, build a new map on every call; a declared map does not ----

	// The one-off forms (a collection expression or an ImmutableArray written at the call site, or the tuple overloads) build a new
	// map and compile the row reader on every call, and cache nothing. A declared map costs none of that.
	[Fact]
	public static void CallSiteMapsAndTuples_BuildANewMapOnEveryCall_AndCacheNothing()
	{
		object?[][] data = [[1, "name1", 1.5m, "note1"]];
		var renamed = new FakeExecuteReader(() => TypedReader.Create(RenamedColumns, data));

		long declared = BytesPerRead(() => renamed.Map<Item>(Plain).ToList());
		long collectionExpression = BytesPerRead(() => renamed.Map<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")]).ToList());
		long tuples = BytesPerRead(() => renamed.Map<Item>((nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")).ToList());

		// If either form cached its map, it would cost about what the declared map costs.
		Assert.True(collectionExpression - declared > 5_000, $"A collection expression at the call site was expected to build a map and compile a reader on every call ({collectionExpression} B against {declared} B through a declared map).");
		Assert.True(tuples - declared > 5_000, $"The tuple overload was expected to build a map and compile a reader on every call ({tuples} B against {declared} B through a declared map).");
	}

	// ---- every way of passing a map binds where it should ----

	[Fact]
	public static void MapBindings()
	{
		// No argument and null: by column name.
		Assert.Equal(1, new FakeExecuteReader(["Id", "Name"], [1, "x"]).Map<Item>().ToList().Single().Id);
		Assert.Equal(1, new FakeExecuteReader(["Id", "Name"], [1, "x"]).Map<Item>(null).ToList().Single().Id);

		// A declared map.
		AssertItems(Cmd(Renamed, 2).Map<Item>(Plain).ToList(), 2);

		// A collection expression written at the call site builds a new, validated map for that call.
		AssertItems(Cmd(Renamed, 2).Map<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")]).ToList(), 2);
		var duplicate = Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Item>([new(nameof(Item.Name), "a"), new(nameof(Item.Name), "b")]));
		Assert.Equal("mappings", duplicate.ParamName);

		// An ImmutableArray converts at the call site the same way.
		AssertItems(Cmd(Renamed, 2).Map<Item>(ItemPairs).ToList(), 2);

		// Tuples are the convenience overload: they build a strict map too, and the exception names the tuple parameter.
		AssertItems(Cmd(Renamed, 2).Map<Item>((nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")).ToList(), 2);
		var tupleDuplicate = Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Item>((nameof(Item.Name), "a"), (nameof(Item.Name), "b")));
		Assert.Equal("fieldMappingOverrides", tupleDuplicate.ParamName);
	}

	[Fact]
	public static void TupleOverloads_AreStrict()
	{
		// An unknown property is an error even with a null column ("Nope" is deliberately not a property), and so are a repeated property,
		// an empty column name and two properties on one column.
		Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Item>(("Nope", null)));
		Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Item>((nameof(Item.Name), "a"), (nameof(Item.Name), "b")));
		Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Item>((nameof(Item.Name), ""), (nameof(Item.Id), "x")));
		Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Item>((nameof(Item.Name), "x"), (nameof(Item.Note), "X")));
	}

	// The property side of a pair is matched exactly (ordinal); column names are matched ignoring case.
	[Fact]
	public static void PropertySideIsExact_ColumnSideIgnoresCase()
	{
		// "id" is not the property Id.
		var ex = Assert.Throws<ArgumentException>(() => RowMap.Create<Item>([new("id", "item_id")]));
		Assert.Contains("'id'", ex.Message);

		// The column "ITEM_ID" matches the result column "item_id".
		RowMap<Item> upper = [new(nameof(Item.Id), "ITEM_ID")];
		Assert.Equal(7, new FakeExecuteReader(["item_id"], [7]).Map<Item>(upper).ToList().Single().Id);

		// Two columns that differ only by case collide.
		Assert.Throws<ArgumentException>(() => RowMap.Create<Item>(new(nameof(Item.Name), "x"), new(nameof(Item.Note), "X")));
	}

	// The exceptions of a map built from pairs name the parameter the pairs came in through.
	[Fact]
	public static void ParamNamesOfTheStrictThrows_ArePinned()
	{
		Assert.Equal("mappings", Assert.Throws<ArgumentException>(() => new RowMap<Item>([new("Nope", "x")])).ParamName);               // "Nope" is deliberately not a property
		Assert.Equal("mappings", Assert.Throws<ArgumentException>(() => RowMap.Create<Item>(new(nameof(Item.Name), ""), new(nameof(Item.Id), "x"))).ParamName);
		Assert.Equal("mappings", Assert.Throws<ArgumentException>(() => { RowMap<Item> map = default(ImmutableArray<KeyValuePair<string, string?>>); return map; }).ParamName);
		Assert.Equal("fieldMappingOverrides", Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Item>((nameof(Item.Name), "x"), (nameof(Item.Note), "x"))).ParamName);
	}

	// A map enumerates the mapping it holds: every settable property, in declaration order, with the column it reads (null: not mapped).
	[Fact]
	public static void EnumeratingAMap_YieldsTheEffectiveMappingInDeclarationOrder()
	{
		// Declared out of order, with Note ignored and Name left to match by name.
		RowMap<Item> map = [new(nameof(Item.Note), null), new(nameof(Item.Id), "item_id")];
		Assert.Equal(
			[new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), nameof(Item.Name)), new(nameof(Item.Total), nameof(Item.Total)), new(nameof(Item.Note), null)],
			[.. (IEnumerable<KeyValuePair<string, string?>>)map]);
	}

	// ---- the override parameter is `map` everywhere ----

	[Fact]
	public static void EveryOverrideParameterIsAMap_AndTheTupleOverloadsAreWrappers()
	{
		Assembly[] assemblies =
		[
			typeof(RowMap<>).Assembly, typeof(ChannelDbExtensions).Assembly, typeof(DataflowExtensions).Assembly,
			Assembly.Load("Open.Database.Extensions"), Assembly.Load("Open.Database.Extensions.SqlClient"), Assembly.Load("Open.Database.Extensions.MSSqlClient"),
		];
		BindingFlags all = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;
		MethodBase[] methods = [.. assemblies.SelectMany(a => a.GetExportedTypes()).SelectMany(t => t.GetMethods(all).Cast<MethodBase>().Concat(t.GetConstructors(all)))];

		static bool IsPairs(Type t) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>) && t.GetGenericArguments()[0] == typeof(KeyValuePair<string, string>);
		static bool IsTuples(Type t) => (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>) && t.GetGenericArguments()[0] == typeof((string, string)))
			|| t == typeof((string, string)[]);
		static bool IsMap(Type t) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(RowMap<>);

		// No public method takes the pairs as a sequence.
		Assert.Empty(methods.Where(m => m.GetParameters().Any(p => IsPairs(p.ParameterType))).Select(m => m.DeclaringType + "." + m.Name));

		// A parameter of type RowMap<T> is named map.
		Assert.Empty(methods.SelectMany(m => m.GetParameters().Where(p => IsMap(p.ParameterType) && p.Name != "map").Select(p => m.DeclaringType + "." + m.Name + "(" + p.Name + ")")));

		// Every tuple overload has a core customer: the same method on the same type that takes a RowMap<T>.
		var orphans = methods.Where(m => m.GetParameters().Any(p => IsTuples(p.ParameterType)))
			.Where(m => !methods.Any(o => o.DeclaringType == m.DeclaringType && o.Name == m.Name && o.GetParameters().Any(p => IsMap(p.ParameterType))))
			.Select(m => m.DeclaringType + "." + m.Name)
			.ToArray();
		Assert.Empty(orphans);
	}

	// ---- column order and result-set shape ----

	[Fact]
	public static void DifferentColumnOrder_MapsTheSameWay()
	{
		string[] reversed = [.. Renamed.Reverse()];
		string[] withExtra = ["extra", "item_total", "item_id", "item_name"]; // no note column at all
		for (int round = 0; round < 3; round++) // interleaved, so each shape is met again after the others
		{
			AssertItems(Cmd(Renamed, 3).Map<Item>(Plain).ToList(), 3);
			AssertItems(Cmd(reversed, 3).Map<Item>(Plain).ToList(), 3);
			AssertItems(Cmd(withExtra, 3).Map<Item>(Plain).ToList(), 3, hasNote: false);
		}
	}

	// ---- several maps for one model; equal maps behave identically ----

	[Fact]
	public static void SeveralMapsForOneModel_EachAliasesItsOwnColumns()
	{
		var swapped = RowMap.Create<Item>(new(nameof(Item.Name), "b"), new(nameof(Item.Note), "a"));
		var straight = RowMap.Create<Item>(new(nameof(Item.Name), "a"), new(nameof(Item.Note), "b"));
		Item s = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(swapped).ToList().Single();
		Item t = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(straight).ToList().Single();
		Assert.Equal(("second", "first"), (s.Name, s.Note));
		Assert.Equal(("first", "second"), (t.Name, t.Note));
	}

	[Fact]
	public static void SeparatelyBuiltEqualMaps_GiveIdenticalResults()
	{
		static RowMap<Item> Build() => [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];
		RowMap<Item> a = Build(), b = Build();
		Assert.NotSame(a, b);
		string[][] shapes = [Renamed, [.. Renamed.Reverse()], ["extra", "item_total", "item_id", "item_name"]];
		foreach (string[] shape in shapes)
		{
			var fromA = Cmd(shape, 4).Map<Item>(a).ToList();
			var fromB = Cmd(shape, 4).Map<Item>(b).ToList();
			Assert.Equal(fromA.Select(x => (x.Id, x.Name, x.Total, x.Note)), fromB.Select(x => (x.Id, x.Name, x.Total, x.Note)));
		}
	}

	// The sample from the README and the XML docs: one static class, one lazy `field ??=` property per mapping.
	static class Maps
	{
		public static RowMap<Item> ItemMap => field ??= [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), null)];
		public static RowMap<Customer> CustomerMap => field ??= [new(nameof(Customer.Id), "twin_id"), new(nameof(Customer.Name), "full_name")];
	}

	[Fact]
	public static void ReadmeSample_Runs()
	{
		List<Item> items = Cmd(Renamed, 2).Map<Item>(Maps.ItemMap).ToList();
		Assert.Equal([1, 2], items.Select(i => i.Id));
		Assert.All(items, i => Assert.Null(i.Note)); // ("Note", null) ignores the column
		Customer customer = new FakeExecuteReader(["twin_id", "full_name"], [9, "Nine"]).Map<Customer>(Maps.CustomerMap).ToList().Single();
		Assert.Equal((9, "Nine"), (customer.Id, customer.Name));
	}

	// ---- concurrency ----

	static void Race(int threads, Action<int> body)
	{
		using var barrier = new Barrier(threads);
		var errors = new ConcurrentQueue<Exception>();
		Task[] tasks = [.. Enumerable.Range(0, threads).Select(t => Task.Factory.StartNew(() =>
		{
			try
			{
				barrier.SignalAndWait();
				body(t);
			}
			catch (Exception ex)
			{
				errors.Enqueue(ex);
			}
		}, TaskCreationOptions.LongRunning))];

		Task.WaitAll(tasks, TestContext.Current.CancellationToken);
		Assert.Empty(errors);
	}

	// The lazy forms from the docs: no Lazy<T>, and a first-use race may build a mapping more than once.
	//
	// What these tests prove: every racer gets a working, equivalent mapping and every read returns correct rows.
	// What they cannot prove: x86/x64 never reorders the stores that a missing release barrier would expose, so a
	// memory-model mistake could not show up here. Safe publication rests on the runtime's documented guarantee
	// (reference assignment is a release), not on these tests.
	static RowMap<Item> BuildMap(ref int builds)
	{
		Interlocked.Increment(ref builds);
		return [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];
	}

	// Leads in the docs: a plain null check on the compiler-generated backing field.
	static class CoalescingMaps
	{
		public static int Builds;
		public static RowMap<Item> Map => field ??= BuildMap(ref Builds);
	}

	// Exactly one instance is published (by Interlocked.CompareExchange); the factory may still run more than once.
	static class EnsuredMaps
	{
		public static int Builds;
		public static RowMap<Item> Map => LazyInitializer.EnsureInitialized(ref field, static () => BuildMap(ref Builds));
	}

	// Every thread is released from a barrier immediately before its first call, so first use is contended.
	// Returns the instance each racer got from that first call.
	static RowMap<Item>[] RaceOnFirstUseThenSustained(Func<RowMap<Item>> map)
	{
		string[][] shapes = [Renamed, [.. Renamed.Reverse()], ["extra", "item_total", "item_id", "item_name"]];
		var firsts = new RowMap<Item>[16];
		Race(16, t =>
		{
			firsts[t] = map(); // the contended first call
			string[] shape = shapes[t % shapes.Length];
			for (int n = 0; n < 150; n++) // sustained use after the race
				AssertItems(Cmd(shape, 5).Map<Item>(map()).ToList(), 5, hasNote: shape.Contains("item_note"));
		});

		return firsts;
	}

	static void AssertEquivalent(RowMap<Item>[] maps)
	{
		List<KeyValuePair<string, string?>> expected = [.. (IEnumerable<KeyValuePair<string, string?>>)maps[0]];
		Assert.All(maps, m =>
		{
			Assert.NotNull(m);
			Assert.Equal(expected, [.. (IEnumerable<KeyValuePair<string, string?>>)m]);
		});
	}

	[Fact]
	public static void FieldCoalescingMap_RacedOnFirstUse_GivesEquivalentMapsAndIdenticalRows()
	{
		RowMap<Item>[] got = RaceOnFirstUseThenSustained(() => CoalescingMaps.Map);
		AssertEquivalent(got); // possibly several instances, all equivalent
		Assert.InRange(CoalescingMaps.Builds, 1, 16);
	}

	[Fact]
	public static void EnsureInitializedMap_RacedOnFirstUse_PublishesOneInstanceToEveryRacer()
	{
		RowMap<Item>[] got = RaceOnFirstUseThenSustained(() => EnsuredMaps.Map);
		AssertEquivalent(got);
		Assert.All(got, m => Assert.Same(got[0], m)); // exactly one published instance, though the factory may have run more than once
		Assert.InRange(EnsuredMaps.Builds, 1, 16);
	}

	// The layout cache holds 128 layouts and clears itself when full. Readers keep using one layout while other
	// threads push 240 distinct layouts through the same map, so the cache fills and clears under them.
	[Fact]
	public static void ReadingWhileTheLayoutCacheOverflowsAndClears_GivesCorrectRows()
	{
		var shared = RowMap.Create<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"));
		using var stop = new CancellationTokenSource();
		var errors = new ConcurrentQueue<Exception>();
		Thread[] readers = [.. Enumerable.Range(0, 4).Select(_ => new Thread(() =>
		{
			try
			{
				while (!stop.IsCancellationRequested) AssertItems(Cmd(Renamed, 3).Map<Item>(shared).ToList(), 3);
			}
			catch (Exception ex)
			{
				errors.Enqueue(ex);
			}
		}))];

		foreach (Thread reader in readers) reader.Start();
		try
		{
			Race(4, t =>
			{
				for (int n = 0; n < 60; n++) // 4 x 60 = 240 layouts that no other thread shares
					AssertItems(Cmd([.. Renamed, $"extra_{t}_{n}"], 3).Map<Item>(shared).ToList(), 3);
			});
		}
		finally
		{
			stop.Cancel();
			foreach (Thread reader in readers) reader.Join();
		}

		Assert.Empty(errors);
	}

	[Fact]
	public static void ManyMapsBuiltAtOnce_EachKeepsItsOwnMapping()
		=> Race(16, t =>
		{
			string a = "a" + t, b = "b" + t;
			var own = RowMap.Create<Item>(new(nameof(Item.Name), a), new(nameof(Item.Note), b));
			for (int n = 0; n < 50; n++)
			{
				Item item = new FakeExecuteReader([a, b], ["name" + t, "note" + t]).Map<Item>(own).ToList().Single();
				Assert.Equal(("name" + t, "note" + t), (item.Name, item.Note));
			}
		});
}
