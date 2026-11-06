using Open.Database.Extensions.Core;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// RowPlan<T>: a predefined, validated, read-only mapping that every override parameter accepts.
[ExcludeFromCodeCoverage]
public static class RowPlanTests
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
	static readonly RowPlan<Item> Plain = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];

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

	// Compiles only if the override parameters of the command families bind unambiguously to a RowPlan. Never executed.
	public static object[] OverloadResolution(IDbCommand dbCommand, ExpressiveDbCommand command)
		=> [dbCommand.Results<Item>(Plain), command.ResultsAsync<Item>(Plain)];

	// ---- criterion 3: validated where it is built ----

	[Fact]
	public static void UnknownProperty_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>([new("Nope", "x")])); // "Nope" is deliberately not a property of Item
		Assert.Contains("Nope", ex.Message);
	}

	[Fact]
	public static void UnknownProperty_EvenWithANullColumn_Throws()
		=> Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>([new("Nope", null)])); // "Nope" is deliberately not a property of Item

	[Theory]
	[InlineData(nameof(Item.Computed))]
	[InlineData(nameof(Item.Locked))]
	public static void PropertyThatCannotBeSet_Throws(string property)
	{
		var ex = Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>([new(property, "x")]));
		Assert.Contains(property, ex.Message);
	}

	[Fact]
	public static void NullPropertyName_Throws()
		=> Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>([new(null!, "x")]));

	[Fact]
	public static void PropertyNamedTwice_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>(new(nameof(Item.Name), "a"), new(nameof(Item.Name), "b")));
		Assert.Contains("Name", ex.Message);
	}

	[Fact]
	public static void TwoPropertiesOnOneColumn_Throws()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>(new(nameof(Item.Name), "x"), new(nameof(Item.Note), "X"))); // columns match ignoring case
		Assert.Contains("Name", ex.Message);
		Assert.Contains("Note", ex.Message);
	}

	[Fact]
	public static void ColumnTakenByAnUnmentionedProperty_Throws()
	{
		// Id is not mentioned, so it still reads column "Id"; so would Name.
		var ex = Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>([new(nameof(Item.Name), "Id")]));
		Assert.Contains("Id", ex.Message);
		Assert.Contains("Name", ex.Message);
	}

	[Fact]
	public static void SwappingTwoColumns_IsFine()
	{
		var swap = RowPlan.Create<Item>(new(nameof(Item.Name), "Note"), new(nameof(Item.Note), "Name"));
		Item item = new FakeExecuteReader(["Name", "Note"], ["n", "o"]).Map<Item>(swap).ToList().Single();
		Assert.Equal("o", item.Name);
		Assert.Equal("n", item.Note);
	}

	// ---- criterion 4: what the map does not mention still maps by name ----

	[Fact]
	public static void UnmentionedProperties_MapByName_IgnoringCase()
	{
		var onlyTotal = RowPlan.Create<Item>([new(nameof(Item.Total), "item_total")]);
		Item item = new FakeExecuteReader(["id", "NAME", "item_total"], [7, "seven", 10.5m]).Map<Item>(onlyTotal).ToList().Single();
		Assert.Equal(7, item.Id);
		Assert.Equal("seven", item.Name);
		Assert.Equal(10.5m, item.Total);
	}

	[Fact]
	public static void EmptyMap_BehavesLikeMatchingNames()
	{
		Item item = new FakeExecuteReader(["Id", "Name"], [3, "three"]).Map<Item>(RowPlan.Create<Item>()).ToList().Single();
		Assert.Equal(3, item.Id);
		Assert.Equal("three", item.Name);
	}

	[Fact]
	public static void NullColumn_IgnoresTheProperty()
	{
		var ignoreNote = RowPlan.Create<Item>(new(nameof(Item.Name), "item_name"), new(nameof(Item.Note), null));
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
		var ex = Assert.Throws<ArgumentException>(() => RowPlan.Create<WithStatic>([new(nameof(WithStatic.Shared), "x")]));
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
		=> Assert.Equal(5, new FakeExecuteReader(["x"], [5]).Map<WithInit>(RowPlan.Create<WithInit>([new(nameof(WithInit.Id), "x")])).ToList().Single().Id);

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
		// Map<T>() accepts the model (the first twin reads the column), so a map that does not mention the twins must too.
		Assert.Equal(1, new FakeExecuteReader(["Id"], [1]).Map<CaseTwins>().ToList().Single().Id);
		_ = RowPlan.Create<CaseTwins>();
		var map = RowPlan.Create<CaseTwins>([new(nameof(CaseTwins.Id), "c_id")]);
		Assert.Equal(1, new FakeExecuteReader(["c_id"], [1]).Map<CaseTwins>(map).ToList().Single().Id);
	}

	[Fact]
	public static void MentionedProperty_CollidingWithACaseTwin_StillThrows()
		=> Assert.Throws<ArgumentException>(() => RowPlan.Create<CaseTwins>([new(nameof(CaseTwins.Id), "name")]));

	[Fact]
	public static void EmptyColumnName_Throws_WhileNullStillIgnores()
	{
		var ex = Assert.Throws<ArgumentException>(() => RowPlan.Create<Item>([new(nameof(Item.Name), "")]));
		Assert.Contains("Name", ex.Message);
		_ = RowPlan.Create<Item>([new(nameof(Item.Name), null)]);
	}

	// ---- a RowPlan read allocates what Map<T>() allocates on matching names ----

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
	public static void RowPlanRead_AllocatesWhatMapOfTAllocatesOnMatchingNames(int rows)
	{
		object?[][] data = [.. Enumerable.Range(1, rows).Select(i => (object?[])[i, "name" + i, i * 1.5m, i % 2 == 0 ? null : "note" + i])];
		var renamed = new FakeExecuteReader(() => TypedReader.Create(RenamedColumns, data));
		var matching = new FakeExecuteReader(() => TypedReader.Create(MatchingColumns, data));

		long viaRowPlan = BytesPerRead(() => renamed.Map<Item>(Plain).ToList());
		long byName = BytesPerRead(() => matching.Map<Item>().ToList());
		long perCall = BytesPerRead(() => renamed.Map<Item>((nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")).ToList());

		Assert.InRange(viaRowPlan - byName, -16, 16);
		Assert.True(perCall - byName > 400, $"Per-call overrides were expected to cost visibly more ({perCall} vs {byName} B); otherwise this test cannot tell the difference."); // the control
	}

	// ---- spellings: collection expression, factory, implicit conversion, inline pairs, a plain dictionary ----

	static readonly ImmutableArray<KeyValuePair<string, string?>> ItemPairs =
	[
		new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"),
	];

	[Fact]
	public static void CollectionExpression_Factory_AndConversion_BuildTheSamePlan()
	{
		RowPlan<Item> viaExpression = [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];
		RowPlan<Item> viaFactory = RowPlan.Create<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"));
		RowPlan<Item> viaConversion = ItemPairs; // the implicit conversion from an ImmutableArray, at a declaration

		foreach (RowPlan<Item> plan in new[] { viaExpression, viaFactory, viaConversion })
		{
			AssertItems(Cmd(Renamed, 3).Map<Item>(plan).ToList(), 3);
			Assert.Equal(ItemPairs, [.. (IEnumerable<KeyValuePair<string, string?>>)plan]);
		}

		// Each spelling validates where it is declared ("Nope" is deliberately not a property).
		Assert.Throws<ArgumentException>(() => { RowPlan<Item> bad = [new("Nope", "x")]; return bad; });
		Assert.Throws<ArgumentException>(() => { ImmutableArray<KeyValuePair<string, string?>> pairs = [new("Nope", "x")]; RowPlan<Item> bad = pairs; return bad; });
	}

	[Fact]
	public static void DefaultImmutableArray_Throws()
		=> Assert.Throws<ArgumentException>(() => { RowPlan<Item> plan = default(ImmutableArray<KeyValuePair<string, string?>>); return plan; });

	// Every way of calling Map binds where it should, and nothing is ambiguous.
	[Fact]
	public static void MapBindings()
	{
		// No arguments: the no-argument overload (by name).
		Assert.Equal(1, new FakeExecuteReader(["Id", "Name"], [1, "x"]).Map<Item>().ToList().Single().Id);

		// Inline pairs, target-typed new for the elements: the params ImmutableArray overload.
		AssertItems(Cmd(Renamed, 2).Map<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")).ToList(), 2);

		// An ImmutableArray argument: the same overload.
		AssertItems(Cmd(Renamed, 2).Map<Item>(ItemPairs).ToList(), 2);

		// A prebuilt plan: the IEnumerable<KeyValuePair> overload, handing over the shared plan.
		AssertItems(Cmd(Renamed, 2).Map<Item>(Plain).ToList(), 2);

		// Tuples: the convenience overload.
		AssertItems(Cmd(Renamed, 2).Map<Item>((nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")).ToList(), 2);
	}

	// The implicit conversion from an ImmutableArray builds (and validates) a plan, so it must only ever run at a declaration.
	// It can run in a call only where a parameter is typed RowPlan<T>; none of the public methods has one.
	[Fact]
	public static void NoPublicMethodTakesAPlan_SoTheConversionCannotRunInACall()
	{
		Assembly[] assemblies = [typeof(RowPlan<>).Assembly, typeof(ChannelDbExtensions).Assembly, typeof(DataflowExtensions).Assembly];
		BindingFlags all = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;
		string[] offenders = [.. assemblies.SelectMany(a => a.GetExportedTypes())
			.SelectMany(t => t.GetMethods(all).Cast<MethodBase>().Concat(t.GetConstructors(all)))
			.Where(m => m.IsPublic || m.IsFamily || m.IsFamilyOrAssembly)
			.Where(m => m.GetParameters().Any(p => IsPlan(p.ParameterType)))
			.Select(m => m.DeclaringType + "." + m.Name)];
		Assert.Empty(offenders);

		static bool IsPlan(Type type)
		{
			if (type.HasElementType) type = type.GetElementType()!;
			return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(RowPlan<>);
		}
	}

	// A plain Dictionary is a sequence of pairs, so it works wherever the pair-typed override parameter does.
	[Fact]
	public static void DictionaryOfPairs_WorksAsOverrides()
	{
		var overrides = new Dictionary<string, string?> { [nameof(Item.Id)] = "item_id", [nameof(Item.Name)] = "item_name", [nameof(Item.Total)] = "item_total", [nameof(Item.Note)] = "item_note" };
		AssertItems(Cmd(Renamed, 2).Map<Item>(overrides).ToList(), 2);
	}

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
		RowPlan<Item> ignoreNote = [new(nameof(Item.Note), null)];
		Item item = command.Map<Item>(ignoreNote).ToList().Single();
		Assert.Equal(1, item.Id);
		Assert.Null(item.Note);
	}

	// ---- the pair overloads of ToChannel and ToTargetBlock; the tuple overloads are a convenience that converts once ----

	[Fact]
	public static async Task ToChannel_PairOverload_AndTupleConvenienceOverload_MapTheSame()
	{
		var viaPairs = System.Threading.Channels.Channel.CreateUnbounded<Item>();
		var viaTuples = System.Threading.Channels.Channel.CreateUnbounded<Item>();
		KeyValuePair<string, string?>[] pairs = [.. ItemPairs];
		(string, string?)[] tuples = [(nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")];

		Assert.Equal(2, await FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).ToChannel(viaPairs.Writer, true, pairs, TestContext.Current.CancellationToken));
		Assert.Equal(2, await FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).ToChannel(viaTuples.Writer, true, tuples, TestContext.Current.CancellationToken));
		foreach (var channel in new[] { viaPairs, viaTuples })
		{
			var items = new List<Item>();
			await foreach (Item item in channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken)) items.Add(item);
			AssertItems(items, 2);
		}
	}

	[Fact]
	public static async Task ToTargetBlock_PairOverload_AndTupleConvenienceOverload_MapTheSame()
	{
		var viaPairs = new BufferBlock<Item>();
		var viaTuples = new BufferBlock<Item>();
		KeyValuePair<string, string?>[] pairs = [.. ItemPairs];
		(string, string?)[] tuples = [(nameof(Item.Id), "item_id"), (nameof(Item.Name), "item_name"), (nameof(Item.Total), "item_total"), (nameof(Item.Note), "item_note")];

		Assert.Equal(2, FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).ToTargetBlock(viaPairs, true, pairs));
		Assert.Equal(2, FakeReader.Create(Renamed, Row(Renamed, 1), Row(Renamed, 2)).ToTargetBlock(viaTuples, true, tuples));
		foreach (BufferBlock<Item> block in new[] { viaPairs, viaTuples })
		{
			var items = new List<Item>();
			for (int i = 0; i < 2; i++) items.Add(await block.ReceiveAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
			AssertItems(items, 2);
		}
	}

	// The inline overload is for one-offs: it builds a new plan and compiles the row reader on every call, and caches nothing.
	[Fact]
	public static void InlinePairs_BuildANewPlanOnEveryCall_AndCacheNothing()
	{
		object?[][] data = [[1, "name1", 1.5m, "note1"]];
		var renamed = new FakeExecuteReader(() => TypedReader.Create(RenamedColumns, data));

		long viaPlan = BytesPerRead(() => renamed.Map<Item>(Plain).ToList());
		long inline = BytesPerRead(() => renamed.Map<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")).ToList());

		// If the plan were cached, the inline call would cost about what the plan costs.
		Assert.True(inline - viaPlan > 5_000, $"The inline path was expected to build a plan and compile a reader on every call ({inline} B against {viaPlan} B through a declared plan).");
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
		var swapped = RowPlan.Create<Item>(new(nameof(Item.Name), "b"), new(nameof(Item.Note), "a"));
		var straight = RowPlan.Create<Item>(new(nameof(Item.Name), "a"), new(nameof(Item.Note), "b"));
		Item s = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(swapped).ToList().Single();
		Item t = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(straight).ToList().Single();
		Assert.Equal(("second", "first"), (s.Name, s.Note));
		Assert.Equal(("first", "second"), (t.Name, t.Note));
	}

	[Fact]
	public static void SeparatelyBuiltEqualMaps_GiveIdenticalResults()
	{
		static RowPlan<Item> Build() => [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];
		RowPlan<Item> a = Build(), b = Build();
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
		var map = RowPlan.Create<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Note), null));
		Assert.Equal([new(nameof(Item.Id), "item_id"), new(nameof(Item.Note), null)], ((IEnumerable<KeyValuePair<string, string?>>)map).ToList());
	}

	// ---- the other override shapes are untouched ----

	[Fact]
	public static void PerCallOverrides_KeepTheirLenientBehavior()
	{
		// An unknown field with a null column is harmless; a repeated field takes its last column.
		Item item = new FakeExecuteReader(["a", "b"], ["first", "second"]).Map<Item>(("Nope", null), (nameof(Item.Name), "a"), (nameof(Item.Name), "b")).ToList().Single(); // "Nope" is deliberately not a property of Item
		Assert.Equal("second", item.Name);
		Assert.Throws<ArgumentException>(() => new FakeExecuteReader(["a"], ["x"]).Map<Item>(("Nope", "a"))); // "Nope" is deliberately not a property of Item
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
		public static RowPlan<Item> ItemPlan => field ??= [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), null)];
		public static RowPlan<Twin> TwinPlan => field ??= [new(nameof(Twin.Id), "twin_id"), new(nameof(Twin.Name), "full_name")];
	}

	[Fact]
	public static void ReadmeSample_Runs()
	{
		List<Item> items = Cmd(Renamed, 2).Map<Item>(Maps.ItemPlan).ToList();
		Assert.Equal([1, 2], items.Select(i => i.Id));
		Assert.All(items, i => Assert.Null(i.Note)); // ("Note", null) ignores the column
		Twin twin = new FakeExecuteReader(["twin_id", "full_name"], [9, "Nine"]).Map<Twin>(Maps.TwinPlan).ToList().Single();
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
	//
	// What these tests prove: every racer gets a working, equivalent mapping and every read returns correct rows.
	// What they cannot prove: x86/x64 never reorders the stores that a missing release barrier would expose, so a
	// memory-model mistake could not show up here. Safe publication rests on the runtime's documented guarantee
	// (reference assignment is a release), not on these tests.
	static RowPlan<Item> BuildMap(ref int builds)
	{
		Interlocked.Increment(ref builds);
		return [new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note")];
	}

	// Leads in the docs: a plain null check on the compiler-generated backing field.
	static class CoalescingMaps
	{
		public static int Builds;
		public static RowPlan<Item> Map => field ??= BuildMap(ref Builds);
	}

	// Exactly one instance is published (by Interlocked.CompareExchange); the factory may still run more than once.
	static class EnsuredMaps
	{
		public static int Builds;
		public static RowPlan<Item> Map => LazyInitializer.EnsureInitialized(ref field, static () => BuildMap(ref Builds));
	}

	// Every thread is released from a barrier immediately before its first call, so first use is contended.
	// Returns the instance each racer got from that first call.
	static RowPlan<Item>[] RaceOnFirstUseThenSustained(Func<RowPlan<Item>> map)
	{
		string[][] shapes = [Renamed, [.. Renamed.Reverse()], ["extra", "item_total", "item_id", "item_name"]];
		var firsts = new RowPlan<Item>[16];
		Race(16, t =>
		{
			firsts[t] = map(); // the contended first call
			string[] shape = shapes[t % shapes.Length];
			for (int n = 0; n < 150; n++) // sustained use after the race
				AssertItems(Cmd(shape, 5).Map<Item>(map()).ToList(), 5, hasNote: shape.Contains("item_note"));
		});

		return firsts;
	}

	static void AssertEquivalent(RowPlan<Item>[] maps)
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
		RowPlan<Item>[] got = RaceOnFirstUseThenSustained(() => CoalescingMaps.Map);
		AssertEquivalent(got); // possibly several instances, all equivalent
		Assert.InRange(CoalescingMaps.Builds, 1, 16);
	}

	[Fact]
	public static void EnsureInitializedMap_RacedOnFirstUse_PublishesOneInstanceToEveryRacer()
	{
		RowPlan<Item>[] got = RaceOnFirstUseThenSustained(() => EnsuredMaps.Map);
		AssertEquivalent(got);
		Assert.All(got, m => Assert.Same(got[0], m)); // exactly one published instance, though the factory may have run more than once
		Assert.InRange(EnsuredMaps.Builds, 1, 16);
	}

	// The layout cache holds 128 layouts and clears itself when full. Readers keep using one layout while other
	// threads push 240 distinct layouts through the same map, so the cache fills and clears under them.
	[Fact]
	public static void ReadingWhileTheLayoutCacheOverflowsAndClears_GivesCorrectRows()
	{
		var shared = RowPlan.Create<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"));
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
	public static void OneSharedMap_UsedByManyThreadsAndShapes_GivesIdenticalRows()
	{
		var shared = RowPlan.Create<Item>(new(nameof(Item.Id), "item_id"), new(nameof(Item.Name), "item_name"), new(nameof(Item.Total), "item_total"), new(nameof(Item.Note), "item_note"));
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
			var own = RowPlan.Create<Item>(new(nameof(Item.Name), a), new(nameof(Item.Note), b));
			for (int n = 0; n < 50; n++)
			{
				Item item = new FakeExecuteReader([a, b], ["name" + t, "note" + t]).Map<Item>(own).ToList().Single();
				Assert.Equal(("name" + t, "note" + t), (item.Name, item.Note));
			}
		});
}
