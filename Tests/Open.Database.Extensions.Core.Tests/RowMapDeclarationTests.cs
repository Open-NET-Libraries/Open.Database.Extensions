using Item = Open.Database.Extensions.Tests.RowMapTests.Item;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Declaring a RowMap<T> is seamless: one pair or many, by constructor or by collection syntax; a map enumerates what it was declared with.
[ExcludeFromCodeCoverage]
public static class RowMapDeclarationTests
{
	// Declared the way an application would: one static class, one lazy `field ??=` property per map.
	static class Maps
	{
		public static RowMap<Item> One => field ??= [new(nameof(Item.Id), "item_id")];
		public static RowMap<Item> Many => field ??= [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];
		public static RowMap<Item> None => field ??= [];
	}

	sealed class Probe
	{
		public int? Count { get; set; }
		public string? Text { get; set; }
	}

	// One row: item_id = 7, Name = "seven".
	static FakeExecuteReader OneRow() => new(["item_id", "Name"], [7, "seven"]);

	static void AssertSeven(IReadOnlyList<Item> items)
	{
		Item item = Assert.Single(items);
		Assert.Equal(7, item.Id);
		Assert.Equal("seven", item.Name);
	}

	static List<(string, string?)> Pairs(RowMap<Item> map) => [.. map.Select(p => (p.Key, p.Value))];

	// ---- the constructor: one pair or several ----

	[Fact]
	public static void Constructor_OnePair_CompilesAndMaps()
	{
		var map = new RowMap<Item>(new(nameof(Item.Id), "item_id"));

		AssertSeven(OneRow().Map(map).ToList());
	}

	[Fact]
	public static void Constructor_OnePair_AVariable_Maps()
	{
		KeyValuePair<string, string?> pair = new(nameof(Item.Id), "item_id");

		AssertSeven(OneRow().Map(new RowMap<Item>(pair)).ToList());
	}

	[Fact]
	public static void Constructor_OnePair_ANullColumn_IgnoresTheProperty()
	{
		var map = new RowMap<Item>(new(nameof(Item.Name), null));

		Item item = Assert.Single(new FakeExecuteReader(["Id", "Name"], [3, "three"]).Map(map).ToList());
		Assert.Equal(3, item.Id);
		Assert.Null(item.Name);
	}

	[Fact]
	public static void Constructor_OnePair_IsValidatedLikeAnyOther()
		=> Assert.Throws<ArgumentException>(() => new RowMap<Item>(new("Nope", "x")));

	[Fact]
	public static void Constructor_SeveralPairs_Map()
	{
		var map = new RowMap<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"));

		var items = new FakeExecuteReader(["item_id", "item_name"], [7, "seven"]).Map(map).ToList();

		AssertSeven(items);
	}

	[Fact]
	public static void Constructor_ACollectionExpression_Maps()
	{
		var one = new RowMap<Item>([new(nameof(Item.Id), "item_id")]);
		var many = new RowMap<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")]);
		var none = new RowMap<Item>([]);

		AssertSeven(OneRow().Map(one).ToList());
		RowMapTests.AssertItems(RowMapTests.Cmd(RowMapTests.Renamed, 3).Map(many).ToList(), 3);
		Assert.Empty(none);
		Assert.Equal(7, Assert.Single(new FakeExecuteReader(["Id"], [7]).Map(none).ToList()).Id);
	}

	[Fact]
	public static void Constructor_NoArguments_MapsByName()
	{
		var map = new RowMap<Item>();

		Assert.Equal(7, Assert.Single(new FakeExecuteReader(["Id"], [7]).Map(map).ToList()).Id);
		Assert.Empty(map);
	}

	// ---- the collection syntax ----

	[Fact]
	public static void CollectionExpression_OnePair_Maps()
	{
		RowMap<Item> map = [new(nameof(Item.Id), "item_id")];

		AssertSeven(OneRow().Map(map).ToList());
		Assert.Equal([(nameof(Item.Id), "item_id")], Pairs(map));
	}

	[Fact]
	public static void CollectionExpression_ManyPairs_Map()
	{
		RowMap<Item> map = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];

		RowMapTests.AssertItems(RowMapTests.Cmd(RowMapTests.Renamed, 3).Map(map).ToList(), 3);
	}

	[Fact]
	public static void CollectionExpression_Empty_MapsByName_LikeTheDefault()
	{
		RowMap<Item> map = [];
		string[] columns = ["Id", "NAME", "total"];
		FakeExecuteReader Command() => new(columns, [4, "four", 2.5m]);

		Assert.Empty(map);
		var mapped = Assert.Single(Command().Map(map).ToList());
		var byDefault = Assert.Single(Command().Map(RowMap<Item>.Default).ToList());
		Assert.Equal((byDefault.Id, byDefault.Name, byDefault.Total), (mapped.Id, mapped.Name, mapped.Total));
		Assert.Equal((4, "four", 2.5m), (mapped.Id, mapped.Name, mapped.Total));
	}

	[Fact]
	public static void CollectionExpression_AssignedToFieldStyleProperties_Works()
	{
		AssertSeven(OneRow().Map(Maps.One).ToList());
		RowMapTests.AssertItems(RowMapTests.Cmd(RowMapTests.Renamed, 2).Map(Maps.Many).ToList(), 2);
		Assert.Equal(1, Assert.Single(new FakeExecuteReader(["Id"], [1]).Map(Maps.None).ToList()).Id);

		Assert.Same(Maps.One, Maps.One); // declared once: `field ??=` keeps the map
		Assert.Same(Maps.Many, Maps.Many);
	}

	[Fact]
	public static void CollectionExpression_ASpread_Maps()
	{
		KeyValuePair<string, string?>[] pairs = [new(nameof(Item.Id), "item_id")];
		RowMap<Item> map = [.. pairs];

		AssertSeven(OneRow().Map(map).ToList());
	}

	[Fact]
	public static void CollectionExpression_IsValidatedLikeTheConstructor()
	{
		Assert.Throws<ArgumentException>(() => { RowMap<Item> map = [new("Nope", "x")]; GC.KeepAlive(map); });
		Assert.Throws<ArgumentException>(() => { RowMap<Item> map = [new(nameof(Item.Id), "a"), new(nameof(Item.Id), "b")]; GC.KeepAlive(map); });
		Assert.Throws<ArgumentException>(() => { RowMap<Item> map = [new(nameof(Item.Id), "")]; GC.KeepAlive(map); });
	}

	// ---- enumeration: exactly the pairs the map was declared with ----

	[Fact]
	public static void Enumeration_YieldsTheDeclaredPairs_InPropertyOrder_KeepingNull_AndLeavingOutTheUndeclared()
	{
		// Declared out of order, one with a null column, one whose column is the property's own name; Name is not declared at all.
		RowMap<Item> map = [new(nameof(Item.Note), null), new(nameof(Item.Total), "t"), new(nameof(Item.Id), nameof(Item.Id))];

		Assert.Equal([(nameof(Item.Id), nameof(Item.Id)), (nameof(Item.Total), "t"), (nameof(Item.Note), null)], Pairs(map));
	}

	[Fact]
	public static void Enumeration_OfTheDefaultMap_IsEmpty()
		=> Assert.Empty(RowMap<Item>.Default);

	[Fact]
	public static void Enumeration_CanBeRepeated_AndAsTheNonGenericInterface()
	{
		RowMap<Item> map = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Note), null)];

		Assert.Equal(Pairs(map), Pairs(map));
		int count = 0;
		foreach (object? pair in (System.Collections.IEnumerable)map)
		{
			Assert.IsType<KeyValuePair<string, string?>>(pair);
			count++;
		}

		Assert.Equal(2, count);
	}

	[Fact]
	public static void RoundTrip_ThroughTheIEnumerableConstructor_BuildsAnEquivalentMap()
	{
		RowMap<Item> original = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), null)];

		var copy = new RowMap<Item>(original);

		Assert.NotSame(original, copy);
		Assert.Equal(Pairs(original), Pairs(copy));
		var fromOriginal = RowMapTests.Cmd(RowMapTests.Renamed, 3).Map(original).ToList();
		var fromCopy = RowMapTests.Cmd(RowMapTests.Renamed, 3).Map(copy).ToList();
		Assert.Equal(fromOriginal.Select(i => (i.Id, i.Name, i.Total, i.Note)), fromCopy.Select(i => (i.Id, i.Name, i.Total, i.Note)));
		Assert.All(fromCopy, i => Assert.Null(i.Note)); // the null column survived the trip
	}

	[Fact]
	public static void RoundTrip_OfAnEmptyMap_IsAnEmptyMap()
	{
		RowMap<Item> original = [];

		Assert.Empty(new RowMap<Item>(original));
	}

	// ---- the ValueMappingException message names the property, its type and the column ----

	[Fact]
	public static void ValueMappingException_Message_NamesThePropertyItsTypeAndTheColumn()
	{
		RowMap<Item> map = [new(nameof(Item.Id), "item_id")];

		var ex = Assert.Throws<ValueMappingException>(() => new FakeExecuteReader(["item_id"], ["abc"]).Map(map).ToList());

		Assert.Equal("Unable to set value of property 'Id' (Int32) from column 'item_id'.", ex.Message);
		Assert.Equal("Id", ex.PropertyName);
		Assert.Equal("item_id", ex.ColumnName);
		Assert.Equal(typeof(int), ex.TargetType);
	}

	[Fact]
	public static void ValueMappingException_Message_NamesANullableTypeWithAQuestionMark()
	{
		var ex = Assert.Throws<ValueMappingException>(() => new FakeExecuteReader(["Count"], ["abc"]).Map<Probe>().ToList());

		Assert.Equal("Unable to set value of property 'Count' (Int32?) from column 'Count'.", ex.Message);
		Assert.Equal(typeof(int?), ex.TargetType);
	}

	[Fact]
	public static void ValueMappingException_Message_NamesAReferenceType()
	{
		var ex = Assert.Throws<ValueMappingException>(() => new FakeExecuteReader(["Text"], [new byte[] { 1 }]).Map<Probe>().ToList());

		Assert.Equal("Unable to set value of property 'Text' (String) from column 'Text'.", ex.Message);
	}

	// ---- overload resolution: nothing is ambiguous, every spelling reaches the map ----

	static IDataReader Reader() => FakeReader.Create(RowMapTests.Renamed, [RowMapTests.Row(RowMapTests.Renamed, 1), RowMapTests.Row(RowMapTests.Renamed, 2)]);

	static void AssertRenamed(IEnumerable<Item> items) => RowMapTests.AssertItems([.. items], 2);

	[Fact]
	public static void Probe_Map_AcceptsEverySpellingOfAMap()
	{
		static FakeExecuteReader Command() => RowMapTests.Cmd(RowMapTests.Renamed, 2);
		RowMap<Item> declared = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];

		AssertRenamed(Command().Map(declared).ToList());                                       // a RowMap variable
		AssertRenamed(Command().Map<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")]).ToList());
		AssertRenamed(Command().Map<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")).ToList());
		AssertRenamed(Command().Map(new RowMap<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")])).ToList());
		AssertRenamed(Command().Map(RowMapTests.Plain).ToList());

		AssertSeven(OneRow().Map<Item>([new(nameof(Item.Id), "item_id")]).ToList());           // one pair, collection expression
		AssertSeven(OneRow().Map<Item>(new(nameof(Item.Id), "item_id")).ToList());             // one pair, bare
		AssertSeven(OneRow().Map(new RowMap<Item>([new(nameof(Item.Id), "item_id")])).ToList());
		AssertSeven(OneRow().Map(new RowMap<Item>(new(nameof(Item.Id), "item_id"))).ToList());

		Assert.Equal(7, Assert.Single(new FakeExecuteReader(["Id"], [7]).Map<Item>([]).ToList()).Id); // none, collection expression
		Assert.Equal(7, Assert.Single(new FakeExecuteReader(["Id"], [7]).Map<Item>().ToList()).Id);   // none
	}

	[Fact]
	public static void Probe_Results_AcceptsACollectionExpression_AndAVariable()
	{
		RowMap<Item> declared = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];

		AssertRenamed(Reader().Results(declared).ToList());
		AssertRenamed(Reader().Results<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")]).ToList());
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		AssertRenamed(RowMapTests.Cmd(RowMapTests.Renamed, 2).Results<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")]).ToList());
		#pragma warning restore CS0618
		AssertRenamed(Reader().Results(new RowMap<Item>([new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")])).ToList());
		AssertRenamed(Reader().Results(new RowMap<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"))).ToList());
		Assert.Equal(7, Assert.Single(FakeReader.Create(["Id"], [[7]]).Results<Item>([]).ToList()).Id);
	}

	[Fact]
	public static async Task Probe_ToChannel_AcceptsACollectionExpression_AndAVariable()
	{
		static async Task<List<Item>> Drain(ChannelReader<Item> reader)
		{
			var items = new List<Item>();
			await foreach (Item item in reader.ReadAllAsync()) items.Add(item);
			return items;
		}

		RowMap<Item> declared = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];

		AssertRenamed(await Drain(Reader().ToChannel(true, declared, TestContext.Current.CancellationToken)));
		AssertRenamed(await Drain(Reader().ToChannel<Item>(true, [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")], TestContext.Current.CancellationToken)));
		AssertRenamed(
			await Drain(
				RowMapTests
					.Cmd(RowMapTests.Renamed, 2)
					.ToChannel<Item>(
						true,
						[
							new(nameof(Item.Id), "item_id"),
							new(nameof(Item.Name), "item_name"),
							new(nameof(Item.Total), "item_total"),
							new(nameof(Item.Note), "item_note")
						],
						cancellationToken: TestContext.Current.CancellationToken)));
	}

	// ---- the builder ----

	[Fact]
	public static void RowMapCreate_BuildsTheSameMapAsTheConstructor()
	{
		var map = RowMap.Create<Item>([new(nameof(Item.Id), "item_id")]);

		AssertSeven(OneRow().Map(map).ToList());
		Assert.Equal([(nameof(Item.Id), "item_id")], Pairs(map));
		Assert.Empty(RowMap.Create<Item>([]));
	}
}
