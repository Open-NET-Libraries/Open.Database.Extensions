using Open.Database.Extensions.Core;
using System.Collections.Concurrent;
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
	internal sealed class Twin
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public decimal Total { get; set; }
		public string? Note { get; set; }
	}

	internal sealed class Slim
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	static readonly string[] Renamed = ["item_id", "item_name", "item_total", "item_note"];

	// Every property under a different column name; defined once and shared by every test, like an application would.
	static readonly RowMap<Item> Plain = new(("Id", "item_id"), ("Name", "item_name"), ("Total", "item_total"), ("Note", "item_note"));

	static object?[] Row(string[] columns, int i)
		=> [.. columns.Select(c => c switch
		{
			"item_id" or "id" => (object?)i,
			"item_name" or "NAME" => "name" + i,
			"item_total" or "total" => i * 1.5m,
			"item_note" => i % 2 == 0 ? null : "note" + i,
			_ => "extra",
		})];

	static FakeExecuteReader Cmd(string[] columns, int rows)
		=> new(columns, [.. Enumerable.Range(1, rows).Select(i => Row(columns, i))]);

	static void AssertItems(IReadOnlyList<Item> items, int rows, bool hasNote = true)
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

	// ---- criterion 1: declared once, reused by every query ----

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

	// ---- every existing override parameter accepts it (and nothing is ambiguous) ----

	[Fact]
	public static void Results_AcceptsAMap()
	{
		AssertItems([.. FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).Results<Item>(Plain)], 2);
		AssertItems([.. Cmd(Renamed, 2).Results<Item>(Plain)], 2);
		AssertItems([.. FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).ResultsBuffered<Item>(Plain)], 2);
	}

	[Fact]
	public static void DataTable_To_AcceptsAMap()
	{
		using var table = new DataTable();
		table.Columns.Add("item_id", typeof(int));
		table.Columns.Add("item_name", typeof(string));
		table.Columns.Add("item_total", typeof(decimal));
		table.Columns.Add("item_note", typeof(string));
		for (int i = 1; i <= 3; i++)
			table.Rows.Add(i, "name" + i, i * 1.5m, i % 2 == 0 ? DBNull.Value : "note" + i);

		AssertItems([.. table.To<Item>(Plain)], 3);
	}

	[Fact]
	public static void QueryResult_DequeueAs_AcceptsAMap()
	{
		var queue = new Queue<object?[]>([Row(Renamed, 1), Row(Renamed, 2)]);
		var result = new QueryResult<Queue<object?[]>>([0, 1, 2, 3], [.. Renamed], queue);
		AssertItems([.. result.DequeueAs<Item>(Plain)], 2);
	}

	[Fact]
	public static async Task ToChannel_AcceptsAMap()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<Item>();
		long count = await FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).ToChannel(channel.Writer, true, Plain, TestContext.Current.CancellationToken);
		Assert.Equal(2, count);
		var items = new List<Item>();
		await foreach (Item item in channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken)) items.Add(item);
		AssertItems(items, 2);
	}

	[Fact]
	public static async Task ToTargetBlock_AcceptsAMap()
	{
		var block = new BufferBlock<Item>();
		long count = FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).ToTargetBlock(block, true, Plain);
		Assert.Equal(2, count);
		// The mapped overloads deliver from a second stage, so wait for the rows; a BufferBlock completes once it is drained.
		var items = new List<Item>();
		for (int i = 0; i < 2; i++) items.Add(await block.ReceiveAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
		AssertItems(items, 2);
		await block.Completion.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
	}

	// Compiles only if the override parameters of the command families bind unambiguously to a RowMap. Never executed.
	public static object[] OverloadResolution(IDbCommand dbCommand, ExpressiveDbCommand command)
		=> [dbCommand.Results<Item>(Plain), command.ResultsAsync<Item>(Plain)];

	// ---- criterion 3: validated where it is built ----

	[Fact]
	public static void UnknownProperty_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Item>(("Nope", "x")));
		Assert.Contains("Nope", ex.Message);
	}

	[Fact]
	public static void UnknownProperty_EvenWithANullColumn_Throws()
		=> Assert.Throws<ArgumentException>(() => new RowMap<Item>(("Nope", null)));

	[Theory]
	[InlineData("Computed")]
	[InlineData("Locked")]
	public static void PropertyThatCannotBeSet_Throws(string property)
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Item>((property, "x")));
		Assert.Contains(property, ex.Message);
	}

	[Fact]
	public static void NullPropertyName_Throws()
		=> Assert.Throws<ArgumentException>(() => new RowMap<Item>((null!, "x")));

	[Fact]
	public static void PropertyNamedTwice_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Item>(("Name", "a"), ("Name", "b")));
		Assert.Contains("Name", ex.Message);
	}

	[Fact]
	public static void TwoPropertiesOnOneColumn_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Item>(("Name", "x"), ("Note", "X"))); // columns match ignoring case
		Assert.Contains("Name", ex.Message);
		Assert.Contains("Note", ex.Message);
	}

	[Fact]
	public static void ColumnTakenByAnUnmentionedProperty_Throws()
	{
		// Id is not mentioned, so it still reads column "Id"; so would Name.
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Item>(("Name", "Id")));
		Assert.Contains("Id", ex.Message);
		Assert.Contains("Name", ex.Message);
	}

	[Fact]
	public static void SwappingTwoColumns_IsFine()
	{
		var swap = new RowMap<Item>(("Name", "Note"), ("Note", "Name"));
		Item item = new FakeExecuteReader(["Name", "Note"], ["n", "o"]).Map<Item>(swap).ToList().Single();
		Assert.Equal("o", item.Name);
		Assert.Equal("n", item.Note);
	}

	[Fact]
	public static void NullMappings_Throws()
		=> Assert.Throws<ArgumentNullException>(() => new RowMap<Item>(null!));

	// ---- criterion 4: what the map does not mention still maps by name ----

	[Fact]
	public static void UnmentionedProperties_MapByName_IgnoringCase()
	{
		var onlyTotal = new RowMap<Item>(("Total", "item_total"));
		Item item = new FakeExecuteReader(["id", "NAME", "item_total"], [7, "seven", 10.5m]).Map<Item>(onlyTotal).ToList().Single();
		Assert.Equal(7, item.Id);
		Assert.Equal("seven", item.Name);
		Assert.Equal(10.5m, item.Total);
	}

	[Fact]
	public static void EmptyMap_BehavesLikeMatchingNames()
	{
		Item item = new FakeExecuteReader(["Id", "Name"], [3, "three"]).Map<Item>(new RowMap<Item>()).ToList().Single();
		Assert.Equal(3, item.Id);
		Assert.Equal("three", item.Name);
	}

	[Fact]
	public static void NullColumn_IgnoresTheProperty()
	{
		var ignoreNote = new RowMap<Item>(("Name", "item_name"), ("Note", null));
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
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<WithStatic>(("Shared", "x")));
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
		=> Assert.Equal(5, new FakeExecuteReader(["x"], [5]).Map<WithInit>(new RowMap<WithInit>(("Id", "x"))).ToList().Single().Id);

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
		var swapped = new RowMap<Item>(("Name", "b"), ("Note", "a"));
		var straight = new RowMap<Item>(("Name", "a"), ("Note", "b"));
		Item s = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(swapped).ToList().Single();
		Item t = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(straight).ToList().Single();
		Assert.Equal(("second", "first"), (s.Name, s.Note));
		Assert.Equal(("first", "second"), (t.Name, t.Note));
	}

	[Fact]
	public static void SeparatelyBuiltEqualMaps_GiveIdenticalResults()
	{
		static RowMap<Item> Build() => new(("Id", "item_id"), ("Name", "item_name"), ("Total", "item_total"), ("Note", "item_note"));
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

	[Fact]
	public static void EnumeratingAMap_YieldsTheGivenPairsInOrder()
	{
		var map = new RowMap<Item>(("Id", "item_id"), ("Note", null));
		Assert.Equal([new("Id", "item_id"), new("Note", null)], ((IEnumerable<KeyValuePair<string, string?>>)map).ToList());
		Assert.Equal([("Id", "item_id"), ("Note", null)], ((IEnumerable<(string Field, string? Column)>)map).ToList());
	}

	// ---- the other override shapes are untouched ----

	[Fact]
	public static void PerCallOverrides_KeepTheirLenientBehavior()
	{
		// An unknown field with a null column is harmless; a repeated field takes its last column.
		Item item = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(("Nope", null), ("Name", "a"), ("Name", "b")).ToList().Single();
		Assert.Equal("second", item.Name);
		Assert.Throws<ArgumentException>(() => new FakeExecuteReader(["a"], ["x"]).Map<Item>(("Nope", "a")));
	}

	[Fact]
	public static void MapWithSelector_IsUnaffected()
		=> Assert.Equal("x", new FakeExecuteReader(["a"], ["x"]).Map(r => (string)r.GetValue(0)).ToList().Single());

	// ---- a map used with a different model ----

	[Fact]
	public static void MapForAnotherModel_WorksWhenThatModelHasTheProperties()
	{
		var twins = Cmd(Renamed, 3).Map<Twin>(Plain).ToList();
		Assert.Equal([1, 2, 3], twins.Select(t => t.Id));
		Assert.Equal(["name1", "name2", "name3"], twins.Select(t => t.Name));
	}

	[Fact]
	public static void MapForAnotherModel_ThrowsClearlyWhenThatModelLacksTheProperties()
	{
		var ex = Assert.Throws<ArgumentException>(() => Cmd(Renamed, 1).Map<Slim>(Plain));
		Assert.Contains("Slim", ex.Message);
		Assert.Contains("Total", ex.Message);
	}

	// The sample from the README and the XML docs: one static class, one lazy `field ??=` property per mapping.
	static class Maps
	{
		public static RowMap<Item> Item => field ??= new(("Id", "item_id"), ("Name", "item_name"), ("Total", "item_total"), ("Note", null));
		public static RowMap<Twin> Twin => field ??= new(("Id", "twin_id"), ("Name", "full_name"));
	}

	[Fact]
	public static void ReadmeSample_Runs()
	{
		List<Item> items = Cmd(Renamed, 2).Map<Item>(Maps.Item).ToList();
		Assert.Equal([1, 2], items.Select(i => i.Id));
		Assert.All(items, i => Assert.Null(i.Note)); // ("Note", null) ignores the column
		Twin twin = new FakeExecuteReader(["twin_id", "full_name"], [9, "Nine"]).Map<Twin>(Maps.Twin).ToList().Single();
		Assert.Equal((9, "Nine"), (twin.Id, twin.Name));
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
	static RowMap<Item> BuildMap(ref int builds)
	{
		Interlocked.Increment(ref builds);
		return new(("Id", "item_id"), ("Name", "item_name"), ("Total", "item_total"), ("Note", "item_note"));
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

	static void RaceOnFirstUseThenSustained(Func<RowMap<Item>> map)
	{
		string[][] shapes = [Renamed, [.. Renamed.Reverse()], ["extra", "item_total", "item_id", "item_name"]];
		Race(16, t =>
		{
			string[] shape = shapes[t % shapes.Length];
			for (int n = 0; n < 150; n++) // the first pass races on first use; the rest is sustained use
				AssertItems(Cmd(shape, 5).Map<Item>(map()).ToList(), 5, hasNote: shape.Contains("item_note"));
		});
	}

	[Fact]
	public static void FieldCoalescingMap_RacedOnFirstUseAndThenSustained_GivesIdenticalRows()
	{
		RaceOnFirstUseThenSustained(() => CoalescingMaps.Map);
		Assert.InRange(CoalescingMaps.Builds, 1, 16);
	}

	[Fact]
	public static void EnsureInitializedMap_RacedOnFirstUseAndThenSustained_GivesIdenticalRows()
	{
		RaceOnFirstUseThenSustained(() => EnsuredMaps.Map);
		Assert.InRange(EnsuredMaps.Builds, 1, 16);
		Assert.Same(EnsuredMaps.Map, EnsuredMaps.Map); // exactly one instance was published
	}

	[Fact]
	public static void OneSharedMap_UsedByManyThreadsAndShapes_GivesIdenticalRows()
	{
		var shared = new RowMap<Item>(("Id", "item_id"), ("Name", "item_name"), ("Total", "item_total"), ("Note", "item_note"));
		string[][] shapes = [Renamed, [.. Renamed.Reverse()], ["extra", "item_total", "item_id", "item_name"], ["item_name", "item_id", "item_total", "x", "item_note"]];
		Race(16, t =>
		{
			for (int n = 0; n < 150; n++)
			{
				string[] shape = shapes[(t + n) % shapes.Length]; // every thread meets every shape
				AssertItems(Cmd(shape, 3).Map<Item>(shared).ToList(), 3, hasNote: shape.Contains("item_note"));
			}
		});
	}

	[Fact]
	public static void ManyMapsBuiltAtOnce_EachKeepsItsOwnMapping()
		=> Race(16, t =>
		{
			string a = "a" + t, b = "b" + t;
			var own = new RowMap<Item>(("Name", a), ("Note", b));
			for (int n = 0; n < 50; n++)
			{
				Item item = new FakeExecuteReader([a, b], ["name" + t, "note" + t]).Map<Item>(own).ToList().Single();
				Assert.Equal(("name" + t, "note" + t), (item.Name, item.Note));
			}
		});
}
