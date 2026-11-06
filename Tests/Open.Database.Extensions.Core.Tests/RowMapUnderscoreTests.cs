using Open.Database.Extensions.Core;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A column feeds a property of the same name ignoring case and ignoring every underscore (q_type feeds QType), with no RowMap<T>.
// The tiers run over the whole result set: exact, then ignoring case, then ignoring case and underscores (tier 3, only for a property no pair named).
[ExcludeFromCodeCoverage]
public static class RowMapUnderscoreTests
{
	public sealed class Counts
	{
		public string? QType { get; set; }
		public DateTime MinDate { get; set; } = new(2001, 2, 3);
		public int TotalDays { get; set; }
		public long? MaxDays { get; set; }
		public string? UserID { get; set; }
		public string? HTTPRequest { get; set; }
		public string? Address2 { get; set; }
	}

	public sealed class QOnly
	{
		public string? QType { get; set; }
	}

	// Two properties whose names differ only by case.
	public sealed class Twins
	{
		public string? Id { get; set; }
		public string? ID { get; set; }
	}

	public sealed class Small
	{
		public string? QType { get; set; }
		public DateTime MinDate { get; set; }
		public int TotalDays { get; set; }
	}

	static readonly DateTime Stamp = new(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);

	static FakeExecuteReader Cmd(ImmutableArray<ColumnDef> columns, params object?[][] rows)
		=> new(() => TypedReader.Create(columns, rows));

	// A one-row result of text columns, each holding its own name, so a property's value says which column fed it.
	static T ReadOne<T>(RowMap<T>? map, params string[] names)
		where T : new()
	{
		ImmutableArray<ColumnDef> columns = [.. names.Select(n => new ColumnDef(n, typeof(string)))];
		object?[] row = [.. names];
		FakeExecuteReader cmd = Cmd(columns, row);
		return (map is null ? cmd.Map<T>() : cmd.Map(map)).ToList().Single();
	}

	static readonly string[] CountsNames = ["q_type", "min_date", "total_days", "max_days", "user_id", "http_request", "address_2"];

	static readonly ImmutableArray<ColumnDef> CountsColumns =
	[
		("q_type", typeof(string)), ("min_date", typeof(DateTime)), ("total_days", typeof(int)), ("max_days", typeof(long)),
		("user_id", typeof(string)), ("http_request", typeof(string)), ("address_2", typeof(string)),
	];

	static object?[] CountsRow(bool nullDate = false)
		=> ["T1", nullDate ? null : Stamp, 12, 99L, "u7", "GET /", "1 Main St"];

	static void AssertCounts(Counts c, bool nullDate = false)
	{
		Assert.Equal("T1", c.QType);
		Assert.Equal(nullDate ? new DateTime(2001, 2, 3) : Stamp, c.MinDate);
		Assert.Equal(12, c.TotalDays);
		Assert.Equal(99L, c.MaxDays);
		Assert.Equal("u7", c.UserID);
		Assert.Equal("GET /", c.HTTPRequest);
		Assert.Equal("1 Main St", c.Address2);
	}

	// ---- (a) no map ----

	[Fact]
	public static void UnderscoredColumns_FeedPascalCaseProperties_WithNoMap()
		=> AssertCounts(Cmd(CountsColumns, CountsRow()).Map<Counts>().ToList().Single());

	// ---- (b) an exact match beats a looser one, in either column order ----

	[Theory]
	[InlineData("q_type", "QType", "QType")]
	[InlineData("QType", "q_type", "QType")]
	[InlineData("qtype", "QType", "QType")] // tier 1 beats tier 2
	[InlineData("QType", "qtype", "QType")]
	[InlineData("q_type", "qtype", "qtype")] // tier 2 beats tier 3
	[InlineData("qtype", "q_type", "qtype")]
	[InlineData("q_type", "q__type", "q_type")] // within a tier the first column wins
	public static void ABetterMatchWins_WhateverTheColumnOrder(string first, string second, string winner)
		=> Assert.Equal(winner, ReadOne<QOnly>(null, first, second).QType);

	// ---- (c) tier 2 is unchanged ----

	[Theory]
	[InlineData("QTYPE")]
	[InlineData("qtype")]
	[InlineData("qType")]
	public static void IgnoringCase_StillFeedsTheProperty(string column)
		=> Assert.Equal(column, ReadOne<QOnly>(null, column).QType);

	// ---- (d) explicit pairs stay exact; a null column still means do not map ----

	[Fact]
	public static void APropertyWithAPair_ReadsOnlyItsColumn()
	{
		RowMap<QOnly> toX = new([new(nameof(QOnly.QType), "x")]);
		Assert.Equal("x", ReadOne(toX, "q_type", "x", "QType").QType);
		Assert.Null(ReadOne(toX, "q_type", "QType").QType);

		// A pair column with an underscore is not matched to a column without it, but is still matched ignoring case.
		RowMap<QOnly> toUnderscored = new([new(nameof(QOnly.QType), "q_type")]);
		Assert.Null(ReadOne(toUnderscored, "QType").QType);
		Assert.Null(ReadOne(toUnderscored, "qtype").QType);
		Assert.Equal("Q_TYPE", ReadOne(toUnderscored, "Q_TYPE").QType);
	}

	[Fact]
	public static void APairIsExactAndFinal_EvenWhenItsColumnIsThePropertysOwnName()
	{
		// The pair names QType, so the underscore tier never looks at it, though the column equals the property's own name.
		RowMap<QOnly> sameName = new([new(nameof(QOnly.QType), nameof(QOnly.QType))]);
		Assert.Null(ReadOne(sameName, "q_type").QType);
		Assert.Equal("QType", ReadOne(sameName, "QType").QType);
		Assert.Equal("qtype", ReadOne(sameName, "qtype").QType);

		// The same property written with the underscored column does read it.
		RowMap<QOnly> underscored = new([new(nameof(QOnly.QType), "q_type")]);
		Assert.Equal("q_type", ReadOne(underscored, "q_type").QType);
	}

	[Fact]
	public static void ANullColumn_StillMeansDoNotMap_EvenWhenAnUnderscoredColumnExists()
	{
		RowMap<QOnly> ignored = new([new(nameof(QOnly.QType), null)]);
		Assert.Null(ReadOne(ignored, "q_type").QType);
		Assert.Null(ReadOne(ignored, "QType", "q_type").QType);
	}

	// ---- (e) one column feeds one property, and a looser match never takes a property a better one filled ----

	[Theory]
	[InlineData(new[] { "i_d" }, "i_d", null)]                 // one column, one property (the first)
	[InlineData(new[] { "Id", "i_d" }, "Id", "i_d")]           // the exact column keeps Id; the other column feeds ID
	[InlineData(new[] { "i_d", "Id" }, "Id", "i_d")]           // whatever the order
	[InlineData(new[] { "ID", "i_d" }, "i_d", "ID")]
	[InlineData(new[] { "i_d", "ID" }, "i_d", "ID")]
	public static void AColumnFeedsOneProperty_AndNeverTakesAFilledOne(string[] columns, string? id, string? upperId)
	{
		Twins twins = ReadOne<Twins>(null, columns);
		Assert.Equal(id, twins.Id);
		Assert.Equal(upperId, twins.ID);
	}

	// ---- (f) the buffered object[] route ----

	[Fact]
	public static void BufferedRows_MapUnderscoredColumns()
	{
		QueryResult<Queue<object?[]>> result = new([.. Enumerable.Range(0, CountsNames.Length)], [.. CountsNames], new Queue<object?[]>([CountsRow()]));
		AssertCounts(result.DequeueAs<Counts>().Single());
	}

	// ---- (g) a NULL through an underscored column keeps the initializer ----

	[Fact]
	public static void ANullThroughAnUnderscoredColumn_KeepsTheInitializer()
	{
		AssertCounts(Cmd(CountsColumns, CountsRow(nullDate: true)).Map<Counts>().ToList().Single(), nullDate: true);

		QueryResult<Queue<object?[]>> result = new([.. Enumerable.Range(0, CountsNames.Length)], [.. CountsNames], new Queue<object?[]>([CountsRow(nullDate: true)]));
		AssertCounts(result.DequeueAs<Counts>().Single(), nullDate: true);
	}

	// ---- a wide result set ----

	[Fact]
	public static void AWideResult_MapsTheThreeUnderscoredColumns_AndIgnoresTheRest()
	{
		const int width = 1000;
		var defs = new List<ColumnDef>(width);
		var cells = new List<object?>(width);
		for (int i = 0; i < width; i++)
		{
			switch (i)
			{
				case 3: defs.Add(new("q_type", typeof(string))); cells.Add("wide"); break;
				case 500: defs.Add(new("min_date", typeof(DateTime))); cells.Add(Stamp); break;
				case 997: defs.Add(new("total_days", typeof(int))); cells.Add(42); break;
				default: defs.Add(new($"filler_col_{i}", typeof(int))); cells.Add(i); break;
			}
		}

		object?[] cellRow = [.. cells];
		Small row = Cmd([.. defs], cellRow).Map<Small>().ToList().Single();

		Assert.Equal("wide", row.QType);
		Assert.Equal(Stamp, row.MinDate);
		Assert.Equal(42, row.TotalDays);
	}

	// ---- the matcher against a plain O(columns x properties) reference ----

	public sealed class Wide
	{
		public int Id { get; set; }
		public int ID { get; set; }
		public string? QType { get; set; }
		public string? UserID { get; set; }
		public string? HTTPRequest { get; set; }
		public string? Address2 { get; set; }
		public string? Name { get; set; }
		public string? Email { get; set; }
		public string? FirstName { get; set; }
		public string? LastName { get; set; }
	}

	static readonly PropertyInfo[] WideProperties = [.. typeof(Wide).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetSetMethod() is not null)];

	static readonly MethodInfo MatchMethod = typeof(RowMap<Wide>).GetMethod("Match", BindingFlags.NonPublic | BindingFlags.Instance)!;

	// The definition, written the slow way: for each column in order, the first property that qualifies (tier 3: among the properties no pair named and unfilled when it began).
	static int?[] Reference(string?[] columnOf, bool[] named, string[] names)
	{
		int?[] feeds = new int?[names.Length];
		bool[] filled = new bool[columnOf.Length];

		void Tier(Func<int, string, bool> same, Func<int, bool> eligible)
		{
			bool[] candidate = [.. Enumerable.Range(0, columnOf.Length).Select(eligible)];
			for (int i = 0; i < names.Length; i++)
			{
				if (feeds[i] is not null) continue;

				for (int p = 0; p < columnOf.Length; p++)
				{
					if (!candidate[p] || !same(p, names[i])) continue;

					if (!filled[p]) { filled[p] = true; feeds[i] = p; }

					break;
				}
			}
		}

		static string Strip(string s) => s.Replace("_", "");

		Tier((p, n) => string.Equals(columnOf[p], n, StringComparison.Ordinal), _ => true);
		Tier((p, n) => string.Equals(columnOf[p], n, StringComparison.OrdinalIgnoreCase), _ => true);
		Tier((p, n) => string.Equals(Strip(WideProperties[p].Name), Strip(n), StringComparison.OrdinalIgnoreCase),
			p => !filled[p] && !named[p]);
		return feeds;
	}

	static readonly string[] Tokens = ["id", "ID", "Id", "q", "type", "QType", "user", "http", "request", "address", "2", "name", "email", "e", "mail", "first", "last", "full", "x", "_", "__"];

	static string RandomName(Random random, string?[] columnOf)
	{
		string name;
		switch (random.Next(4))
		{
			case 0: // a declared name or a property name, as is
				name = random.Next(2) == 0 ? WideProperties[random.Next(WideProperties.Length)].Name : columnOf[random.Next(columnOf.Length)] ?? "none";
				break;
			case 1: // pieces glued together
				name = string.Concat(Enumerable.Range(0, random.Next(1, 4)).Select(_ => Tokens[random.Next(Tokens.Length)]));
				break;
			default: // a declared name with underscores added or removed
				string source = (random.Next(2) == 0 ? WideProperties[random.Next(WideProperties.Length)].Name : columnOf[random.Next(columnOf.Length)]) ?? "none";
				name = string.Concat(source.Select(c => random.Next(5) == 0 ? "_" + c : c.ToString()));
				if (random.Next(3) == 0) name = name.Replace("_", "");
				break;
		}

		return random.Next(3) switch
		{
			0 => name.ToUpperInvariant(),
			1 => name.ToLowerInvariant(),
			_ => name,
		};
	}

	[Fact]
	public static void TheMatcher_AgreesWithTheReference_OnGeneratedLayouts()
	{
		RowMap<Wide>[] maps =
		[
			RowMap<Wide>.Default,
			new([new(nameof(Wide.Name), "full_name"), new(nameof(Wide.Email), null), new(nameof(Wide.FirstName), "First_Name"), new(nameof(Wide.QType), "QType")]),
		];

		var random = new Random(20260611);
		int matched = 0;
		foreach (RowMap<Wide> map in maps)
		{
			// The column each property reads: its own name unless a pair says otherwise.
			string?[] columnOf = [.. WideProperties.Select(p => p.Name)];
			bool[] named = new bool[WideProperties.Length]; // The properties a pair named, whatever its column.
			if (map != RowMap<Wide>.Default)
			{
				void Pair(string property, string? column)
				{
					int index = Array.FindIndex(WideProperties, p => p.Name == property);
					columnOf[index] = column;
					named[index] = true;
				}

				Pair(nameof(Wide.Name), "full_name");
				Pair(nameof(Wide.Email), null);
				Pair(nameof(Wide.FirstName), "First_Name");
				Pair(nameof(Wide.QType), nameof(Wide.QType)); // A pair whose column is the property's own name is still a pair.
			}

			for (int trial = 0; trial < 400; trial++)
			{
				string[] names = [.. Enumerable.Range(0, random.Next(0, 15)).Select(_ => RandomName(random, columnOf))];

				var feeds = (PropertyInfo?[])MatchMethod.Invoke(map, [names])!;
				int?[] expected = Reference(columnOf, named, names);

				Assert.Equal(names.Length, feeds.Length);
				for (int i = 0; i < names.Length; i++)
				{
					Assert.True(expected[i] is { } p ? feeds[i]?.Name == WideProperties[p].Name : feeds[i] is null,
						$"[{string.Join(", ", names)}] column {i} '{names[i]}': expected {(expected[i] is { } e ? WideProperties[e].Name : "nothing")}, was {feeds[i]?.Name ?? "nothing"}");
					if (feeds[i] is not null) matched++;
				}
			}
		}

		Assert.True(matched > 500, "The generated layouts should match often enough to mean something.");
	}

	// ---- the pipelines (ToChannel, ToTargetBlock, AsSourceBlock) choose the reader's columns with the same match, with no explicit map ----

	static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

	static KeyValuePair<string, object> Cell(string name, object value) => new(name, value);

	// A one-row reader whose columns are the cells, typed by their values.
	static TypedReader PipelineReader(params KeyValuePair<string, object>[] cells)
	{
		ImmutableArray<ColumnDef> columns = [.. cells.Select(c => new ColumnDef(c.Key, c.Value.GetType()))];
		object?[] row = [.. cells.Select(c => (object?)c.Value)];
		return TypedReader.Create(columns, row);
	}

	static async Task<Small> ViaToChannel(TypedReader reader)
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<Small>();
		await reader.ToChannel(channel.Writer, true);
		using var cts = new CancellationTokenSource(Wait);
		var items = new List<Small>();
		await foreach (Small item in channel.Reader.ReadAllAsync(cts.Token)) items.Add(item);
		return items.Single();
	}

	static async Task<Small> ViaToTargetBlock(TypedReader reader)
	{
		var block = new BufferBlock<Small>();
		Assert.Equal(1, reader.ToTargetBlock(block, true));
		return await block.ReceiveAsync(Wait);
	}

	static Task<Small> ViaAsSourceBlock(TypedReader reader)
		=> reader.AsSourceBlock<Small>().ReceiveAsync(Wait);

	public static TheoryData<string> Pipelines => ["ToChannel", "ToTargetBlock", "AsSourceBlock"];

	static Task<Small> Pipe(string pipeline, TypedReader reader) => pipeline switch
	{
		"ToChannel" => ViaToChannel(reader),
		"ToTargetBlock" => ViaToTargetBlock(reader),
		_ => ViaAsSourceBlock(reader),
	};

	[Theory]
	[MemberData(nameof(Pipelines))]
	public static async Task Pipelines_MapUnderscoredColumns_WithNoExplicitMap(string pipeline)
	{
		Small row = await Pipe(pipeline, PipelineReader(Cell("q_type", "T1"), Cell("min_date", Stamp)));

		Assert.Equal("T1", row.QType);
		Assert.Equal(Stamp, row.MinDate);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task Pipelines_ExactBeatsFuzzy_InEitherColumnOrder(bool exactFirst)
	{
		KeyValuePair<string, object> fuzzy = Cell("q_type", "fuzzy"), exact = Cell("QType", "exact");
		TypedReader reader = exactFirst
			? PipelineReader(exact, fuzzy, Cell("min_date", Stamp))
			: PipelineReader(fuzzy, exact, Cell("min_date", Stamp));

		Small row = await ViaToChannel(reader);

		Assert.Equal("exact", row.QType);
		Assert.Equal(Stamp, row.MinDate);
	}

	[Fact]
	public static async Task Pipelines_StillLeaveOutAColumnThatMapsToNothing()
	{
		// The unrelated columns sit before, between and after the mapped ones: the rows only line up if they are left out.
		Small row = await ViaToChannel(PipelineReader(Cell("note", "ignored"), Cell("q_type", "T1"), Cell("other_thing", 5), Cell("min_date", Stamp), Cell("extra", 1.5)));

		Assert.Equal("T1", row.QType);
		Assert.Equal(Stamp, row.MinDate);
		Assert.Equal(0, row.TotalDays);
	}

	// ---- the comparer tier 3 uses ----

	static IEqualityComparer<string> Comparer()
	{
		Type type = typeof(RowMap<Wide>).GetNestedType("IgnoreCaseAndUnderscores", BindingFlags.NonPublic)!;
		if (type.IsGenericTypeDefinition) type = type.MakeGenericType(typeof(Wide));
		return (IEqualityComparer<string>)Activator.CreateInstance(type)!;
	}

	[Theory]
	[InlineData("q_type", "QType", true)]
	[InlineData("__a__b__", "AB", true)]
	[InlineData("address_2", "Address2", true)]
	[InlineData("http_request", "HTTPRequest", true)]
	[InlineData("", "", true)]
	[InlineData("", "___", true)]
	[InlineData("_", "", true)]
	[InlineData("a", "b", false)]
	[InlineData("ab", "abc", false)]
	[InlineData("abc", "ab_", false)]
	[InlineData("a_b", "a b", false)]
	public static void TheComparer_IgnoresCaseAndUnderscores(string x, string y, bool equal)
	{
		IEqualityComparer<string> comparer = Comparer();

		Assert.Equal(equal, comparer.Equals(x, y));
		Assert.Equal(equal, comparer.Equals(y, x));
		if (equal) Assert.Equal(comparer.GetHashCode(x), comparer.GetHashCode(y));
	}

	[Fact]
	public static void TheComparer_HandlesNullAndTheSameInstance()
	{
		IEqualityComparer<string> comparer = Comparer();

		Assert.True(comparer.Equals(null!, null!));
		Assert.False(comparer.Equals("a", null!));
		Assert.False(comparer.Equals(null!, "a"));
		string same = "a_b";
		Assert.True(comparer.Equals(same, same));
	}
}
