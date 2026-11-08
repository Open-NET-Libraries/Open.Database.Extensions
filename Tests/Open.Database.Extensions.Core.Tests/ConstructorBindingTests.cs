using Open.Database.Extensions.Core;
using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Constructor binding (phase C): a model with no public parameterless constructor is built through its one public constructor, and each parameter is a slot beside the settable properties.
// The cases follow the design note's test plan: T1 positional records on every route, T2 constructor plus setters (an init setter is never called twice), T4 parameter defaults,
// T5 structs, T8 pairs and validation for parameters, T9 the matching tiers, T13 errors. (T3 is in UnmappableTypeTests, T7 in SequentialAccessOrderTests, T10 in ConstructorBindingPipelineTests,
// T11 and T12 in MappingCostAndRaceCharacterizationTests.)
[ExcludeFromCodeCoverage]
public static class ConstructorBindingTests
{
	// The three ways a row reaches a map: a typed reader (the provider's typed getters, nothing boxed), a reader whose columns report object (the general conversion),
	// and rows already buffered as object[] (the Retrieve/QueryResult route).
	public enum Route { Typed, Boxed, Buffered }

	public static TheoryData<Route> Routes => [Route.Typed, Route.Boxed, Route.Buffered];

	static List<T> Read<T>(Route route, ImmutableArray<ColumnDef> columns, object?[] row, RowMap<T>? map = null)
		=> ReadRows(route, columns, [row], map);

	static List<T> ReadRows<T>(Route route, ImmutableArray<ColumnDef> columns, object?[][] rows, RowMap<T>? map = null)
	{
		if (route == Route.Buffered)
		{
			var result = new QueryResult<Queue<object?[]>>([.. Enumerable.Range(0, columns.Length)], [.. columns.Select(c => c.Name)], new Queue<object?[]>(rows));
			return result.DequeueAs(map).ToList();
		}

		ImmutableArray<ColumnDef> shown = route == Route.Typed ? columns : [.. columns.Select(c => new ColumnDef(c.Name, typeof(object)))];
		var command = new FakeExecuteReader(() => TypedReader.Create(shown, rows));
		return (map is null ? command.Map<T>() : command.Map(map)).ToList();
	}

	// ---- the models ----

	public sealed record Post(int Id, string Title);

	// A positional record with an extra settable property, and an init property with an initializer.
	public sealed record PostExtra(int Id, string Title)
	{
		public int Views { get; set; }
		public string Label { get; init; } = "none";
	}

	// The positional property is declared by hand, so its init setter can count how often it is called: the constructor assigns it, and the mapper must not call it again.
	public sealed record Counted(int Id, string Title)
	{
		public int TitleSets { get; private set; }

		public string Title
		{
			get => field;
			init
			{
				TitleSets++;
				field = value;
			}
		} = Title;
	}

	public enum Tone { None, Green, Blue }

	// A class with one public constructor: parameters with defaults, a get-only property for each (matched ignoring case: `count` feeds Count).
	public sealed class WithDefaults(int id, int views = 3, decimal price = 9.99m, Tone tone = Tone.Green, DateTime at = default, int? count = 4, string? note = "none", string? bare = null)
	{
		public int Id { get; } = id;
		public int Views { get; } = views;
		public decimal Price { get; } = price;
		public Tone Tone { get; } = tone;
		public DateTime At { get; } = at;
		public int? Count { get; } = count;
		public string? Note { get; } = note;
		public string? Bare { get; } = bare;
	}

	static readonly DateTime Stamp = new(2022, 3, 4, 5, 6, 7, DateTimeKind.Utc);

	// ---- T1: a positional record through every terminal and every route ----

	static readonly ImmutableArray<ColumnDef> PostColumns = [("Id", typeof(int)), ("Title", typeof(string))];

	[Theory]
	[MemberData(nameof(Routes))]
	public static void APositionalRecord_IsBuilt_OnEveryRoute(Route route)
		=> Assert.Equal<Post>([new(1, "one"), new(2, "two")], ReadRows<Post>(route, PostColumns, [[1, "one"], [2, "two"]]));

	[Fact]
	public static async Task EveryMapTerminal_BuildsThePositionalRecord()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var command = new FakeExecuteReader(() => TypedReader.Create(PostColumns, [1, "one"], [2, "two"], [3, "three"]));
		Post[] all = [new(1, "one"), new(2, "two"), new(3, "three")];

		Assert.Equal<Post>(all, command.Map<Post>().ToList());
		Assert.Equal<Post>(all, command.Map<Post>().ToArray());
		Assert.Equal(all, command.Map<Post>().ToImmutableArray().ToArray());
		Assert.Equal(all.ToHashSet(), command.Map<Post>().ToHashSet());
		Assert.Equal(all[0], command.Map<Post>().FirstOrDefault());
		Assert.Equal<Post>(all[1..], command.Map<Post>().Skip(1).ToList());
		Assert.Equal<Post>(all[..2], command.Map<Post>().Take(2).ToArray());
		Assert.Equal<Post>(all[1..2], command.Map<Post>().Skip(1).Take(1).ToList());

		Assert.Equal<Post>(all, await command.Map<Post>().ToListAsync(ct));
		Post[] asArray = await command.Map<Post>().ToArrayAsync(ct);
		Assert.Equal(all, asArray);
		Assert.Equal(all[0], await command.Map<Post>().FirstOrDefaultAsync(ct));
		Assert.Equal(all.ToDictionary(p => p.Id), await command.Map<Post>().ToDictionaryAsync(p => p.Id, null, ct));

		var one = new FakeExecuteReader(() => TypedReader.Create(PostColumns, [9, "nine"]));
		Assert.Equal(new Post(9, "nine"), one.Map<Post>().SingleOrDefault());
		Assert.Equal(new Post(9, "nine"), await one.Map<Post>().SingleOrDefaultAsync(ct));
	}

	[Fact]
	public static async Task Results_ResultsBuffered_AndTheAsyncTwins_BuildThePositionalRecord()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		object?[][] rows = [[1, "one"], [2, "two"]];
		Post[] expected = [new(1, "one"), new(2, "two")];

		// The substitute readers report object columns (the general path); the typed reader reports the real types.
		Assert.Equal<Post>(expected, FakeReader.Create(["Id", "Title"], rows).Results<Post>());
		Assert.Equal<Post>(expected, FakeReader.Create(["Id", "Title"], rows).ResultsBuffered<Post>());
		Assert.Equal<Post>(expected, TypedReader.Create(PostColumns, rows).Results<Post>());
		Assert.Equal<Post>(expected, TypedReader.Create(PostColumns, rows).ResultsBuffered<Post>());
		Assert.Equal<Post>(expected, new FakeExecuteReader(() => TypedReader.Create(PostColumns, rows)).Results<Post>());
		Assert.Equal<Post>(expected, await TypedReader.Create(PostColumns, rows).ResultsBufferedAsync<Post>(ct));

		var streamed = new List<Post>();
		await foreach (Post post in TypedReader.Create(PostColumns, rows).ResultsAsync<Post>(cancellationToken: ct)) streamed.Add(post);
		Assert.Equal<Post>(expected, streamed);
	}

	[Fact]
	public static void ADataTable_IsMappedToPositionalRecords()
	{
		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Title", typeof(string));
		table.Rows.Add(1, "one");
		table.Rows.Add(2, DBNull.Value);

		Assert.Equal<Post>([new(1, "one"), new(2, null!)], table.To<Post>()); // a NULL into a non-nullable reference parameter is null (phase E changes that)
	}

	[Fact]
	public static void AResultWithNoColumnTheRecordCanUse_BuildsItFromItsFallbacks()
	{
		Assert.Equal(new Post(0, null!), Assert.Single(Read<Post>(Route.Buffered, [("other", typeof(int))], [5])));
		Assert.Equal(new Post(0, null!), Assert.Single(Read<Post>(Route.Typed, [("other", typeof(int))], [5])));
		Assert.Equal(new Post(0, null!), Assert.Single(Read<Post>(Route.Buffered, [], [])));
	}

	// ---- T2: the constructor, then the setters, in one plan ----

	static readonly ImmutableArray<ColumnDef> ExtraColumns = [("Label", typeof(string)), ("Id", typeof(int)), ("Views", typeof(int)), ("Title", typeof(string))];

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ARecordWithSettableAndInitProperties_GetsThemAfterTheConstructor(Route route)
	{
		PostExtra post = Assert.Single(Read<PostExtra>(route, ExtraColumns, ["from the column", 7, 42, "t"]));
		Assert.Equal((7, "t", 42, "from the column"), (post.Id, post.Title, post.Views, post.Label));
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AnInitPropertyWithNoColumn_KeepsItsInitializer_AndANullIntoAValueTypePropertyKeepsItsValue(Route route)
	{
		PostExtra post = Assert.Single(Read<PostExtra>(route, [("Id", typeof(int)), ("Title", typeof(string)), ("Views", typeof(int))], [1, "t", null]));
		Assert.Equal((1, "t", 0, "none"), (post.Id, post.Title, post.Views, post.Label));
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ThePositionalInitSetter_IsNeverCalled_TheConstructorFeedsIt(Route route)
	{
		Counted counted = Assert.Single(Read<Counted>(route, PostColumns, [3, "three"]));
		Assert.Equal((3, "three"), (counted.Id, counted.Title));
		Assert.Equal(0, counted.TitleSets); // the initializer assigns the backing field; the mapper must not call the setter on top of the constructor
	}

	// ---- T4: a parameter no column feeds takes its declared default; a NULL is a value, except into a non-nullable value type ----

	static readonly ImmutableArray<ColumnDef> DefaultsColumns =
	[
		("Id", typeof(int)), ("Views", typeof(int)), ("Price", typeof(decimal)), ("Tone", typeof(int)), ("At", typeof(DateTime)), ("Count", typeof(int)), ("Note", typeof(string)), ("Bare", typeof(string)),
	];

	static void AssertDeclaredDefaults(WithDefaults item, int id)
		=> Assert.Equal((id, 3, 9.99m, Tone.Green, default(DateTime), 4, "none", null), (item.Id, item.Views, item.Price, item.Tone, item.At, item.Count, item.Note, item.Bare));

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AParameterWithNoColumn_GetsItsDeclaredDefault(Route route)
		=> AssertDeclaredDefaults(Assert.Single(Read<WithDefaults>(route, [("Id", typeof(int))], [1])), 1);

	[Theory]
	[MemberData(nameof(Routes))]
	public static void NoColumnAtAll_GivesTheDefaultsAndDefaultOfTheRest(Route route)
	{
		WithDefaults item = Assert.Single(Read<WithDefaults>(route, [("unrelated", typeof(int))], [1]));
		AssertDeclaredDefaults(item, 0); // `id` has no default: default(int)
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ARealValue_InEveryColumn_IsUsed(Route route)
	{
		WithDefaults item = Assert.Single(Read<WithDefaults>(route, DefaultsColumns, [2, 5, 1.5m, (int)Tone.Blue, Stamp, 6, "n", "b"]));
		Assert.Equal((2, 5, 1.5m, Tone.Blue, Stamp, 6, "n", "b"), (item.Id, item.Views, item.Price, item.Tone, item.At, item.Count, item.Note, item.Bare));
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ANull_IntoANonNullableValueType_GivesTheParametersFallback_AndIntoANullableOrReference_GivesNull(Route route)
	{
		WithDefaults item = Assert.Single(Read<WithDefaults>(route, DefaultsColumns, [null, null, null, null, null, null, null, null]));

		// Id has no declared default: its fallback is default(int). Views, Price, Tone and At fall back to their declared default (At is `default`).
		Assert.Equal((0, 3, 9.99m, Tone.Green, default(DateTime)), (item.Id, item.Views, item.Price, item.Tone, item.At));

		// A NULL into a nullable value type or a reference is null, not the declared default: NULL is a value.
		Assert.Null(item.Count);
		Assert.Null(item.Note);
		Assert.Null(item.Bare);
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ANull_InOneColumn_OnlyAffectsThatParameter(Route route)
	{
		for (int column = 1; column < DefaultsColumns.Length; column++)
		{
			object?[] row = [2, 5, 1.5m, (int)Tone.Blue, Stamp, 6, "n", "b"];
			row[column] = null;
			WithDefaults item = Assert.Single(Read<WithDefaults>(route, DefaultsColumns, row));

			object?[] expected = [2, 5, 1.5m, Tone.Blue, Stamp, 6, "n", "b"];
			expected[column] = column switch
			{
				1 => 3,
				2 => 9.99m,
				3 => Tone.Green,
				4 => default(DateTime),
				_ => null, // a nullable value type and a reference receive null: not the declared 4, not "none"
			};

			Assert.Equal<object?>(expected, [item.Id, item.Views, item.Price, item.Tone, item.At, item.Count, item.Note, item.Bare]);
		}
	}

	// A nullable enum with a declared default (ParameterInfo reports the underlying integer for it) and an enum with one.
	public sealed class WithEnumDefaults(int id, Tone? maybe = Tone.Blue, Tone plain = Tone.Blue)
	{
		public int Id { get; } = id;
		public Tone? Maybe { get; } = maybe;
		public Tone Plain { get; } = plain;
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AnEnumDefault_NullableOrNot_IsTheDeclaredOne(Route route)
	{
		WithEnumDefaults missing = Assert.Single(Read<WithEnumDefaults>(route, [("Id", typeof(int))], [1]));
		Assert.Equal((Tone.Blue, Tone.Blue), (missing.Maybe, missing.Plain));

		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Maybe", typeof(int)), ("Plain", typeof(int))];
		WithEnumDefaults nulls = Assert.Single(Read<WithEnumDefaults>(route, columns, [1, null, null]));
		Assert.Equal((null, Tone.Blue), (nulls.Maybe, nulls.Plain)); // NULL into a nullable is null, not the default; into the non-nullable enum it is the default

		WithEnumDefaults given = Assert.Single(Read<WithEnumDefaults>(route, columns, [1, (int)Tone.None, (int)Tone.Green]));
		Assert.Equal((Tone.None, Tone.Green), (given.Maybe, given.Plain));
	}

	// ---- T5: structs ----

	public record struct RecordStruct(int Id, string Title);

	public readonly record struct ReadonlyRecordStruct(int Id, string Title);

	// A readonly struct with get-only properties: until constructor binding it mapped to all defaults, silently (there is no setter).
	public readonly struct ReadonlyStruct(int id, string? title)
	{
		public int Id { get; } = id;
		public string? Title { get; } = title;
	}

	// What a record struct was mapped as before: its default value, then its settable properties.
	public sealed class RecordStructBefore
	{
		public int Id { get; set; }
		public string? Title { get; set; }
	}

	static readonly ImmutableArray<ColumnDef> ReversedPostColumns = [("junk", typeof(string)), ("Title", typeof(string)), ("Id", typeof(int))];

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ARecordStruct_MapsToTheSameResultsAsDefaultPlusSetters(Route route)
	{
		(ImmutableArray<ColumnDef> Columns, object?[] Row)[] shapes =
		[
			(PostColumns, [1, "one"]),                         // everything there
			(PostColumns, [null, null]),                       // NULL into a non-nullable value type, and into a reference
			([("Id", typeof(int))], [4]),                      // no column for Title
			([("Title", typeof(string))], ["t"]),              // no column for Id
			(ReversedPostColumns, ["x", "r", 5]),              // reversed, with an extra column
			([("other", typeof(string))], ["x"]),              // no column of the struct at all
		];

		foreach ((ImmutableArray<ColumnDef> columns, object?[] row) in shapes)
		{
			RecordStructBefore before = Assert.Single(Read<RecordStructBefore>(route, columns, row));
			RecordStruct now = Assert.Single(Read<RecordStruct>(route, columns, row));
			ReadonlyRecordStruct readonlyNow = Assert.Single(Read<ReadonlyRecordStruct>(route, columns, row));

			Assert.Equal((before.Id, before.Title), (now.Id, now.Title));
			Assert.Equal((before.Id, before.Title), (readonlyNow.Id, readonlyNow.Title));
			Assert.Equal(new RecordStruct(before.Id, before.Title!), now);
		}
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AReadonlyStructWithGetOnlyProperties_IsBuiltThroughItsConstructor(Route route)
	{
		ReadonlyStruct item = Assert.Single(Read<ReadonlyStruct>(route, PostColumns, [8, "eight"]));
		Assert.Equal((8, "eight"), (item.Id, item.Title));

		ReadonlyStruct partial = Assert.Single(Read<ReadonlyStruct>(route, [("Title", typeof(string))], ["only"]));
		Assert.Equal((0, "only"), (partial.Id, partial.Title));
	}

	[Fact]
	public static void AStructBuiltThroughItsConstructor_IsAValueResult_WithTheDefaultForAnEmptyResult()
	{
		var none = new FakeExecuteReader(() => TypedReader.Create(PostColumns));
		Assert.Empty(none.Map<RecordStruct>().ToList());
		Assert.Equal(default(RecordStruct), none.Map<RecordStruct>().FirstOrDefault());
	}

	// A struct built through its constructor that also has properties the constructor does not cover: they are set on the struct after it is built.
	public record struct RecordStructExtra(int Id)
	{
		public string? Note { get; set; }
		public int Views { get; init; }
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AStructBuiltThroughItsConstructor_GetsItsOtherPropertiesSetAfterwards(Route route)
	{
		RecordStructExtra item = Assert.Single(Read<RecordStructExtra>(route, [("Note", typeof(string)), ("Id", typeof(int)), ("Views", typeof(int))], ["n", 4, 9]));
		Assert.Equal((4, "n", 9), (item.Id, item.Note, item.Views));

		RecordStructExtra nulls = Assert.Single(Read<RecordStructExtra>(route, [("Note", typeof(string)), ("Id", typeof(int)), ("Views", typeof(int))], [null, null, null]));
		Assert.Equal((0, null, 0), (nulls.Id, nulls.Note, nulls.Views));
	}

	// ---- T8: pairs and validation, for parameters ----

	// Each parameter has a get-only property of its name ignoring case, which is not settable but is no other slot: the parameter and the property are one slot, named `A` and `B`.
	public sealed class Plain(int a, string? b)
	{
		public int A => a;
		public string? B => b;
		public string Ro { get; } = "read-only";
	}

	// Parameters no property carries: each slot is named after its parameter.
	public sealed class OnlyParameters(int a, string? b)
	{
		public string Both => a + b;
	}

	static readonly RowMap<Post> TitleMap = new([new(nameof(Post.Title), "post_title")]);

	[Theory]
	[MemberData(nameof(Routes))]
	public static void APair_IsAppliedToThePositionalParameter(Route route)
	{
		Post post = Assert.Single(Read(route, [("Id", typeof(int)), ("post_title", typeof(string))], [1, "t"], TitleMap));
		Assert.Equal(new Post(1, "t"), post);

		// The parameter reads only its column now: a column called Title feeds nothing.
		Assert.Equal(new Post(1, null!), Assert.Single(Read(route, PostColumns, [1, "ignored"], TitleMap)));
	}

	[Fact]
	public static void APairNamesAParameter_IgnoringCase_AsNameofCannotReachAParameter()
	{
		foreach (string key in (string[])["Title", "title", "TITLE"])
		{
			var map = new RowMap<Post>([new(key, "post_title")]);
			Assert.Equal(new Post(2, "t"), Assert.Single(Read(Route.Typed, [("Id", typeof(int)), ("post_title", typeof(string))], [2, "t"], map)));
		}

		// A parameter of Plain has no property: its slot is named after the parameter, and a pair names it by that name.
		var plain = new RowMap<Plain>([new("a", "col_a"), new("B", "col_b")]);
		Plain item = Assert.Single(Read(Route.Typed, [("col_a", typeof(int)), ("col_b", typeof(string))], [3, "bee"], plain));
		Assert.Equal((3, "bee"), (item.A, item.B));
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ANullColumn_OnAParameter_MeansDoNotMap_SoItGetsItsFallback(Route route)
	{
		var ignoreTitle = new RowMap<Post>([new(nameof(Post.Title), null)]);
		Assert.Equal(new Post(1, null!), Assert.Single(Read(route, PostColumns, [1, "from the column"], ignoreTitle)));

		var ignoreViews = new RowMap<WithDefaults>([new(nameof(WithDefaults.Views), null), new(nameof(WithDefaults.Note), null)]);
		WithDefaults item = Assert.Single(Read(route, DefaultsColumns, [2, 5, 1.5m, (int)Tone.Blue, Stamp, 6, "n", "b"], ignoreViews));
		Assert.Equal((2, 3, 1.5m, "none"), (item.Id, item.Views, item.Price, item.Note)); // the declared defaults, though both columns are there
	}

	[Fact]
	public static void AnUnknownName_ThrowsArgumentException_NamingPropertiesAndConstructorParameters()
	{
		foreach (string? column in (string?[])["x", null])
		{
			var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new("Nope", column)]));
			Assert.Equal("mappings", ex.ParamName);
			Assert.StartsWith($"'{typeof(Post)}' has no settable public property or constructor parameter named 'Nope'.", ex.Message);
		}

		// A get-only property no parameter covers cannot be set, so it is not a name either.
		var readOnly = Assert.Throws<ArgumentException>(() => new RowMap<Plain>([new(nameof(Plain.Ro), "x")]));
		Assert.StartsWith($"'{typeof(Plain)}' has no settable public property or constructor parameter named 'Ro'.", readOnly.Message);
	}

	[Fact]
	public static void AParameterNamedTwice_Throws_WhateverTheCaseOfTheNames()
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Title), "a"), new("title", "b")]));
		Assert.Equal("mappings", ex.ParamName);
		Assert.StartsWith("The property 'Title' is mapped more than once.", ex.Message);

		// Plain.A is a property too: the parameter `a` and the property `A` are one slot, however each is spelled.
		var one = Assert.Throws<ArgumentException>(() => new RowMap<Plain>([new("a", "x"), new("A", "y")]));
		Assert.StartsWith("The property 'A' is mapped more than once.", one.Message);
	}

	[Fact]
	public static void ASlotNamedByNoPropertyAtAll_IsAConstructorParameterInTheMessages()
	{
		var twice = Assert.Throws<ArgumentException>(() => new RowMap<OnlyParameters>([new("a", "x"), new("b", "y"), new("a", "z")]));
		Assert.StartsWith("The constructor parameter 'a' is mapped more than once.", twice.Message);

		var shared = Assert.Throws<ArgumentException>(() => new RowMap<OnlyParameters>([new("a", "X"), new("b", "x")]));
		Assert.StartsWith("The constructor parameter 'a' and the constructor parameter 'b' both read column 'x'. Map one of them to another column, or to null to ignore it.", shared.Message);

		var unknown = Assert.Throws<ArgumentException>(() => new RowMap<OnlyParameters>([new("Both", "x")]));
		Assert.StartsWith($"'{typeof(OnlyParameters)}' has no settable public property or constructor parameter named 'Both'.", unknown.Message);

		OnlyParameters item = Assert.Single(Read(Route.Typed, [("x", typeof(int)), ("y", typeof(string))], [1, "z"], new RowMap<OnlyParameters>([new("A", "x"), new("b", "y")])));
		Assert.Equal("1z", item.Both);
	}

	[Fact]
	public static void TwoParametersOnOneColumn_Throw_NamingBothAndTheColumn()
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Id), "X"), new(nameof(Post.Title), "x")]));
		Assert.Equal("mappings", ex.ParamName);
		Assert.StartsWith("The properties 'Id' and 'Title' both read column 'x'. Map one of them to another column, or to null to ignore it.", ex.Message);

		// A pair onto the column of an unmentioned parameter: Id still reads "Id".
		var onto = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Title), "Id")]));
		Assert.StartsWith("The properties 'Id' and 'Title' both read column 'Id'.", onto.Message);

		// Mapping the other one away (to null, or to its own other column) makes room.
		_ = new RowMap<Post>([new(nameof(Post.Title), "Id"), new(nameof(Post.Id), null)]);
		_ = new RowMap<Post>([new(nameof(Post.Title), "Id"), new(nameof(Post.Id), "post_id")]);
	}

	[Fact]
	public static void AnEmptyColumnName_AndANullName_StillThrow_ForAParameter()
	{
		var empty = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Title), "")]));
		Assert.StartsWith("The property 'Title' is mapped to an empty column name. Use null to ignore it.", empty.Message);
		Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(null!, "x")]));
	}

	[Fact]
	public static void AMapOverAConstructorType_EnumeratesItsPairs_InSlotOrder()
	{
		IEnumerable<KeyValuePair<string, string?>> pairs = new RowMap<PostExtra>([new(nameof(PostExtra.Views), null), new("title", "post_title")]);
		Assert.Equal<KeyValuePair<string, string?>>([new(nameof(PostExtra.Title), "post_title"), new(nameof(PostExtra.Views), null)], pairs); // the parameters first, then the other properties
		Assert.Empty((IEnumerable<KeyValuePair<string, string?>>)RowMap<Post>.Default);
	}

	// The by-name behavior of a type with no parameter is untouched: the message for a class built as ever still says "settable public property".
	[Fact]
	public static void AnUnknownName_OnATypeBuiltWithNoConstructorParameters_KeepsItsMessage()
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<RecordStructBefore>([new("Nope", "x")]));
		Assert.StartsWith($"'{typeof(RecordStructBefore)}' has no settable public property named 'Nope'.", ex.Message);
	}

	// Two parameters whose names differ only in case, and one property: the exact name wins, so `Id` feeds the property and `id` is a slot of its own.
	public sealed class Competing
	{
		public Competing(int id, int Id)
		{
			Lower = id;
			this.Id = Id;
		}

		public int Id { get; }
		public int Lower { get; }
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void WhenTwoParametersCompeteForAProperty_TheExactNameWins_AndEachPropertyServesOne(Route route)
	{
		// The column Id feeds the slot Id (the parameter `Id` and the property); the column id feeds the slot `id` (the other parameter, with no property).
		Competing item = Assert.Single(Read<Competing>(route, [("id", typeof(int)), ("Id", typeof(int))], [20, 10]));
		Assert.Equal((10, 20), (item.Id, item.Lower));

		Competing swapped = Assert.Single(Read<Competing>(route, [("Id", typeof(int)), ("id", typeof(int))], [10, 20]));
		Assert.Equal((10, 20), (swapped.Id, swapped.Lower));

		Assert.Equal<string>(["id", "Id"], new RowMap<Competing>([new("Id", "x"), new("id", "y")]).Select(p => p.Key)); // slot order: the parameters, in order
	}

	// ---- T9: the matching tiers, against parameters ----

	public sealed record Tiered(int PostId, string? PostTitle);

	static ImmutableArray<ColumnDef> Cols(params string[] names) => [.. names.Select(n => new ColumnDef(n, typeof(object)))];

	// Columns of type int, named as given: each one competes to feed the int parameter PostId.
	static ImmutableArray<ColumnDef> IdColumns(params string[] names) => [.. names.Select(n => new ColumnDef(n, typeof(int)))];

	[Theory]
	[InlineData(Route.Typed, "PostId", "PostTitle")]
	[InlineData(Route.Typed, "postid", "POSTTITLE")]
	[InlineData(Route.Typed, "post_id", "Post_Title")]
	[InlineData(Route.Typed, "_post_id_", "p_o_s_t_t_i_t_l_e")]
	[InlineData(Route.Buffered, "PostId", "PostTitle")]
	[InlineData(Route.Buffered, "postid", "POSTTITLE")]
	[InlineData(Route.Buffered, "post_id", "Post_Title")]
	[InlineData(Route.Buffered, "_post_id_", "p_o_s_t_t_i_t_l_e")]
	public static void EachTierFeedsTheParameter(Route route, string idColumn, string titleColumn)
		=> Assert.Equal(new Tiered(5, "t"), Assert.Single(Read<Tiered>(route, [(idColumn, typeof(int)), (titleColumn, typeof(string))], [5, "t"])));

	// The winner is the column of that name; the loser feeds nothing. Each cell holds 10 x (its position + 1), so the value says which column fed.
	[Theory]
	[InlineData("postid", "PostId", "PostId")]       // exact beats ignoring case
	[InlineData("PostId", "postid", "PostId")]
	[InlineData("post_id", "PostId", "PostId")]      // exact beats underscores
	[InlineData("PostId", "post_id", "PostId")]
	[InlineData("post_id", "postid", "postid")]      // ignoring case beats underscores
	[InlineData("postid", "post_id", "postid")]
	[InlineData("post_id", "POST_ID", "post_id")]    // the same tier: the first column wins
	[InlineData("POST_ID", "post_id", "POST_ID")]
	public static void ABetterTierWins_WhateverTheColumnOrder(string first, string second, string winner)
	{
		foreach (Route route in (Route[])[Route.Typed, Route.Buffered])
		{
			Tiered item = Assert.Single(Read<Tiered>(route, IdColumns(first, second), [10, 20]));
			Assert.Equal(winner == first ? 10 : 20, item.PostId);
			Assert.Null(item.PostTitle);
		}
	}

	[Fact]
	public static void AColumnFeedsOneParameter_AndAFilledParameterIsNeverTakenByALowerTier()
	{
		// PostId is filled by the exact column; "post_id" would be its underscore match but the parameter is taken.
		Tiered item = Assert.Single(Read<Tiered>(Route.Typed, [("post_id", typeof(int)), ("PostId", typeof(int)), ("PostTitle", typeof(string))], [10, 20, "t"]));
		Assert.Equal(new Tiered(20, "t"), item);
	}

	[Fact]
	public static void AParameterWithAPair_NeverUsesTheUnderscoreTier_EvenForItsOwnName()
	{
		Assert.Equal(5, Assert.Single(Read<Tiered>(Route.Typed, IdColumns("post_id"), [5])).PostId); // no pair: the underscore tier feeds it

		var pinned = new RowMap<Tiered>([new(nameof(Tiered.PostId), "PostId")]); // a pair, though to its own name: exact and final
		Assert.Equal(0, Assert.Single(Read(Route.Typed, IdColumns("post_id"), [5], pinned)).PostId);
		Assert.Equal(6, Assert.Single(Read(Route.Typed, IdColumns("postid"), [6], pinned)).PostId); // ignoring case still applies to a pair
	}

	[Fact]
	public static void TheMatchingStaysTheSame_WhenTheShapeIsMetAgain()
	{
		// Interleaved shapes through one map: each comes back from the layout cache, and must feed the same columns.
		var map = new RowMap<Tiered>([new(nameof(Tiered.PostTitle), "title_col")]);
		for (int round = 0; round < 3; round++)
		{
			Tiered a = Assert.Single(Read(Route.Typed, [("post_id", typeof(int)), ("title_col", typeof(string))], [1, "a"], map));
			Tiered b = Assert.Single(Read(Route.Typed, [("title_col", typeof(string)), ("PostId", typeof(int)), ("post_id", typeof(int))], ["b", 2, 3], map));
			Tiered c = Assert.Single(Read(Route.Buffered, [("title_col", typeof(string)), ("PostId", typeof(int))], ["c", 4], map));
			Assert.Equal((1, "a", 2, "b", 4, "c"), (a.PostId, a.PostTitle, b.PostId, b.PostTitle, c.PostId, c.PostTitle));
		}
	}

	// ---- T13: errors ----

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AnUnconvertibleValue_IntoAParameter_ThrowsValueMappingException_NamingTheParameterColumnAndType(Route route)
	{
		var map = new RowMap<Post>([new(nameof(Post.Id), "post_id")]);
		var ex = Assert.Throws<ValueMappingException>(() => Read(route, [("post_id", typeof(object)), ("Title", typeof(string))], ["abc", "t"], map));

		Assert.Equal("Id", ex.PropertyName); // the slot's name: the property of the parameter's name
		Assert.Equal("post_id", ex.ColumnName);
		Assert.Equal(typeof(int), ex.TargetType);
		Assert.Equal("Unable to pass value of column 'post_id' to constructor parameter 'Id' (Int32) of 'Post'.", ex.Message);
		Assert.IsType<FormatException>(ex.InnerException);
	}

	[Fact]
	public static void AnUnconvertibleValue_IntoANullableParameter_ReportsTheNullableType_AndAParameterOfADifferentNameThanItsProperty_NamesTheParameter()
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read<WithDefaults>(Route.Boxed, Cols("Id", "Count"), [1, "abc"]));
		Assert.Equal(("Count", "Count", typeof(int?)), (ex.PropertyName, ex.ColumnName, ex.TargetType));
		Assert.Equal("Unable to pass value of column 'Count' to constructor parameter 'count' (Int32?) of 'WithDefaults'.", ex.Message);
	}

	[Fact]
	public static void AnOverflowOnTheTypedPath_IntoAParameter_IsReportedTheSameWay()
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read<Post>(Route.Typed, [("Id", typeof(long)), ("Title", typeof(string))], [4_294_967_301L, "x"]));
		Assert.Equal(("Id", "Id", typeof(int)), (ex.PropertyName, ex.ColumnName, ex.TargetType));
		Assert.IsType<OverflowException>(ex.InnerException);
	}

	[Fact]
	public static void AnUnconvertibleValue_IntoAPropertyOfAConstructorModel_StillSaysProperty()
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read<PostExtra>(Route.Boxed, Cols("Id", "Title", "Views"), [1, "t", "abc"]));
		Assert.Equal(("Views", "Views", typeof(int)), (ex.PropertyName, ex.ColumnName, ex.TargetType));
		Assert.Equal("Unable to set value of property 'Views' (Int32) from column 'Views'.", ex.Message);
	}

	public sealed class ThrowingSetter(int id)
	{
		public int Id { get; } = id;
		public string? Name { get => null; set => throw new NotSupportedException("no name for you"); }
	}

	[Fact]
	public static void APropertySetterThatThrows_AfterTheConstructor_IsReportedAsAValueMappingException()
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read<ThrowingSetter>(Route.Typed, [("Id", typeof(int)), ("Name", typeof(string))], [1, "x"]));
		Assert.Equal(("Name", "Name", typeof(string)), (ex.PropertyName, ex.ColumnName, ex.TargetType));
		Assert.IsType<NotSupportedException>(ex.InnerException);
	}

	sealed class ProviderFailure : DbException
	{
		public ProviderFailure() : base("the provider failed") { }
	}

	// The constructor is the last thing to run after every column was read, so a failure in it must not be blamed on the last column.
	public sealed class ThrowingConstructor
	{
		public static Exception? Failure;

		public ThrowingConstructor(int id, string? name)
		{
			Id = id;
			Name = name;
			throw Failure!;
		}

		public int Id { get; }
		public string? Name { get; }
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AConstructorThatThrows_IsWrapped_AsInvalidOperationException_WithTheOriginalInside(Route route)
	{
		var original = new ArgumentException("constructor said no");
		ThrowingConstructor.Failure = original;

		var ex = Assert.Throws<InvalidOperationException>(() => Read<ThrowingConstructor>(route, [("Id", typeof(int)), ("Name", typeof(string))], [1, "x"]));
		Assert.IsNotType<ValueMappingException>(ex);
		Assert.Equal($"Unable to create '{typeof(ThrowingConstructor)}' from the current row.", ex.Message);
		Assert.Same(original, ex.InnerException);
	}

	[Fact]
	public static void AnInvalidOperationExceptionFromTheConstructor_PassesThroughUnwrapped_AsDoProviderAndCancellationExceptions()
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Name", typeof(string))];

		var refused = new InvalidOperationException("constructor refused");
		ThrowingConstructor.Failure = refused;
		Assert.Same(refused, Assert.Throws<InvalidOperationException>(() => Read<ThrowingConstructor>(Route.Typed, columns, [1, "x"])));

		var provider = new ProviderFailure();
		ThrowingConstructor.Failure = provider;
		Assert.Same(provider, Assert.Throws<ProviderFailure>(() => Read<ThrowingConstructor>(Route.Typed, columns, [1, "x"])));

		var canceled = new OperationCanceledException();
		ThrowingConstructor.Failure = canceled;
		Assert.Same(canceled, Assert.Throws<OperationCanceledException>(() => Read<ThrowingConstructor>(Route.Typed, columns, [1, "x"])));
	}

	[Fact]
	public static void AFailureOnOneRow_DoesNotPoisonTheNextRead()
	{
		var map = RowMap<Post>.Default;
		Assert.Throws<ValueMappingException>(() => Read(Route.Boxed, [("Id", typeof(object)), ("Title", typeof(string))], ["abc", "t"], map));
		Assert.Equal(new Post(3, "t"), Assert.Single(Read(Route.Boxed, [("Id", typeof(object)), ("Title", typeof(string))], [3, "t"], map)));
	}
}
