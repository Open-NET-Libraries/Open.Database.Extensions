using static Open.Database.Extensions.Tests.ConstructorBindingTests;

#nullable enable

namespace Open.Database.Extensions.Tests;

// RowMap<T>.From(factory, pairs): a function builds each T, its parameters are the slots, and they follow the constructor rules exactly.
// The shared rule is one parameterized test over a constructor route and a factory route (the same type, built by its own constructor and by a factory that calls it),
// so the two cannot drift; what only a factory has (a return type, 17 parameters, a type with several constructors, no member set afterwards) is tested on its own.
[ExcludeFromCodeCoverage]
public static class FactoryMapTests
{
	static readonly DateTime Stamp = new(2022, 3, 4, 5, 6, 7, DateTimeKind.Utc);

	public static TheoryData<Route> Routes => ConstructorBindingTests.Routes;

	// ---- the two routes to the same type ----

	static readonly RowMap<WithDefaults> ConstructorRoute = RowMap<WithDefaults>.Default;

	// The declared defaults are the same as the constructor's, so what a missing column or a NULL gives can be compared.
	static readonly Delegate DefaultingFactory =
		static (int id, int views = 3, decimal price = 9.99m, Tone tone = Tone.Green, DateTime at = default, int? count = 4, string? note = "none", string? bare = null)
			=> new WithDefaults(id, views, price, tone, at, count, note, bare);

	static readonly RowMap<WithDefaults> FactoryRoute = RowMap<WithDefaults>.From(DefaultingFactory);

	static readonly ImmutableArray<ColumnDef> DefaultsColumns =
	[
		("Id", typeof(int)), ("Views", typeof(int)), ("Price", typeof(decimal)), ("Tone", typeof(int)), ("At", typeof(DateTime)), ("Count", typeof(int)), ("Note", typeof(string)), ("Bare", typeof(string)),
	];

	static readonly object?[] RealRow = [2, 5, 1.5m, (int)Tone.Blue, Stamp, 6, "n", "b"];

	sealed record Case(string Name, ImmutableArray<ColumnDef> Columns, object?[] Row);

	// Every shape the shared rule covers: real values, every cell NULL, a NULL in each column alone, a result with only the id, and one no parameter reads.
	static readonly Case[] Cases =
	[
		new("every column real", DefaultsColumns, RealRow),
		new("every cell NULL", DefaultsColumns, [null, null, null, null, null, null, null, null]),
		new("only the id column", [("Id", typeof(int))], [1]),
		new("no column a parameter reads", [("unrelated", typeof(int))], [1]),
		.. Enumerable.Range(1, DefaultsColumns.Length - 1).Select(column =>
		{
			object?[] row = [.. RealRow];
			row[column] = null;
			return new Case($"NULL in {DefaultsColumns[column].Name}", DefaultsColumns, row);
		}),
	];

	public static TheoryData<Route, int> RoutesAndCases
	{
		get
		{
			var data = new TheoryData<Route, int>();
			foreach (Route route in (Route[])[Route.Typed, Route.Boxed, Route.Buffered])
			{
				for (int i = 0; i < Cases.Length; i++)
					data.Add(route, i);
			}

			return data;
		}
	}

	static (int, int, decimal, Tone, DateTime, int?, string?, string?) Project(WithDefaults w)
		=> (w.Id, w.Views, w.Price, w.Tone, w.At, w.Count, w.Note, w.Bare);

	[Theory]
	[MemberData(nameof(RoutesAndCases))]
	public static void TheConstructorAndTheFactoryRoutes_GiveTheSameRow_ForNullAndMissingColumns(Route route, int caseIndex)
	{
		Case c = Cases[caseIndex];
		WithDefaults viaConstructor = Assert.Single(Read(route, c.Columns, c.Row, ConstructorRoute));
		WithDefaults viaFactory = Assert.Single(Read(route, c.Columns, c.Row, FactoryRoute));

		Assert.Equal(Project(viaConstructor), Project(viaFactory));
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void TheFactoryRoute_FollowsTheDocumentedRule(Route route)
	{
		// A NULL into a non-nullable value parameter: its declared default, else default; into a nullable one: null, the declared default (4) not used.
		WithDefaults nulls = Assert.Single(Read(route, DefaultsColumns, [null, null, null, null, null, null, null, null], FactoryRoute));
		Assert.Equal((0, 3, 9.99m, Tone.Green, default, null, null, null), (nulls.Id, nulls.Views, nulls.Price, nulls.Tone, nulls.At, nulls.Count, nulls.Note, nulls.Bare));

		// A parameter no column feeds: its declared default, else default; nothing throws.
		WithDefaults idOnly = Assert.Single(Read(route, [("Id", typeof(int))], [1], FactoryRoute));
		Assert.Equal((1, 3, 9.99m, Tone.Green, default, 4, "none", null), (idOnly.Id, idOnly.Views, idOnly.Price, idOnly.Tone, idOnly.At, idOnly.Count, idOnly.Note, idOnly.Bare));

		WithDefaults real = Assert.Single(Read(route, DefaultsColumns, RealRow, FactoryRoute));
		Assert.Equal((2, 5, 1.5m, Tone.Blue, Stamp, 6, "n", "b"), (real.Id, real.Views, real.Price, real.Tone, real.At, real.Count, real.Note, real.Bare));
	}

	// ---- pairs, on both routes: a constructor pair names the property, a factory pair names the parameter, ignoring case ----

	[Theory]
	[MemberData(nameof(Routes))]
	public static void APairRenamesAParameter_OnBothRoutes(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("v", typeof(int))];

		var viaConstructor = new RowMap<WithDefaults>([new(nameof(WithDefaults.Views), "v")]);
		var viaFactory = RowMap<WithDefaults>.From(DefaultingFactory, new KeyValuePair<string, string?>("views", "v"));
		var ignoringCase = RowMap<WithDefaults>.From(DefaultingFactory, new KeyValuePair<string, string?>("VIEWS", "v"));

		Assert.Equal(9, Assert.Single(Read(route, columns, [1, 9], viaConstructor)).Views);
		Assert.Equal(9, Assert.Single(Read(route, columns, [1, 9], viaFactory)).Views);
		Assert.Equal(9, Assert.Single(Read(route, columns, [1, 9], ignoringCase)).Views);

		// Without the pair the column 'v' feeds nothing.
		Assert.Equal(3, Assert.Single(Read(route, columns, [1, 9], FactoryRoute)).Views);
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ANullColumn_OnAParameter_MeansDoNotMap_OnBothRoutes(Route route)
	{
		var viaConstructor = new RowMap<WithDefaults>([new(nameof(WithDefaults.Views), null), new(nameof(WithDefaults.Note), null)]);
		var viaFactory = RowMap<WithDefaults>.From(DefaultingFactory, new("views", null), new("note", null));

		foreach (RowMap<WithDefaults> map in (RowMap<WithDefaults>[])[viaConstructor, viaFactory])
		{
			WithDefaults item = Assert.Single(Read(route, DefaultsColumns, RealRow, map));
			Assert.Equal((2, 3, 1.5m, "none"), (item.Id, item.Views, item.Price, item.Note)); // the declared defaults, though both columns are there
		}
	}

	[Fact]
	public static void APairWhoseKeyIsNoParameter_FailsAtBuild_OnBothRoutes()
	{
		var viaConstructor = Assert.Throws<ArgumentException>(() => new RowMap<WithDefaults>([new("Nope", "x")]));
		Assert.Equal("mappings", viaConstructor.ParamName);

		var viaFactory = Assert.Throws<ArgumentException>(() => RowMap<WithDefaults>.From(DefaultingFactory, new KeyValuePair<string, string?>("Nope", "x")));
		Assert.Equal("pairs", viaFactory.ParamName);
		Assert.StartsWith($"The factory of '{typeof(WithDefaults)}' has no parameter named 'Nope'.", viaFactory.Message);

		// A parameter named twice, whatever the case, and two parameters reading one column.
		var twice = Assert.Throws<ArgumentException>(() => RowMap<WithDefaults>.From(DefaultingFactory, new("views", "a"), new("VIEWS", "b")));
		Assert.StartsWith("The factory parameter 'views' is mapped more than once.", twice.Message);
		var shared = Assert.Throws<ArgumentException>(() => RowMap<WithDefaults>.From(DefaultingFactory, new("views", "a"), new("price", "A")));
		Assert.Contains("both read column", shared.Message);
	}

	// ---- only a factory has these ----

	public sealed class WeightEntry
	{
		public DateTime WeighedAt { get; set; }
		public decimal? TareWeight { get; set; }
		public decimal? GrossWeight { get; set; }
	}

	// The rows' own columns by name, three tiers: weighed_at feeds weighedAt ignoring case and underscores.
	static RowMap<WeightEntry> FromHistory
		=> field ??= RowMap<WeightEntry>.From(
			static (
				DateTime weighedAt,
				string? vehicleWeightTypeId,
				decimal? weightValue)
				=> new WeightEntry
				{
					WeighedAt = weighedAt,
					TareWeight = vehicleWeightTypeId == "T" ? weightValue : null,
					GrossWeight = vehicleWeightTypeId == "G" ? weightValue : null,
				});

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AFactoryComputesMembersFromColumns_MatchedByTheThreeTiers(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("weighed_at", typeof(DateTime)), ("vehicle_weight_type_id", typeof(string)), ("weight_value", typeof(decimal))];
		List<WeightEntry> rows = ReadRows(route, columns, [[Stamp, "T", 10m], [Stamp, "G", 20m], [Stamp, "X", 30m]], FromHistory);

		Assert.Equal<(DateTime, decimal?, decimal?)>(
			new (DateTime, decimal?, decimal?)[] { (Stamp, 10m, null), (Stamp, null, 20m), (Stamp, null, null) },
			rows.Select(r => (r.WeighedAt, r.TareWeight, r.GrossWeight)));

		// A NULL into a nullable parameter is null: the function can tell it from a value.
		WeightEntry nulls = Assert.Single(Read(route, columns, [Stamp, null, null], FromHistory));
		Assert.Equal((Stamp, null, null), (nulls.WeighedAt, nulls.TareWeight, nulls.GrossWeight));
	}

	[Fact]
	public static void AFactoryMapsOnlyItsParameters_NoMemberIsSetByNameAfterwards()
	{
		var map = RowMap<WeightEntry>.From(static (DateTime weighedAt) => new WeightEntry { WeighedAt = weighedAt });
		ImmutableArray<ColumnDef> columns = [("weighedAt", typeof(DateTime)), ("TareWeight", typeof(decimal))];

		WeightEntry item = Assert.Single(Read(Route.Typed, columns, [Stamp, 5m], map));
		Assert.Equal(Stamp, item.WeighedAt);
		Assert.Null(item.TareWeight); // a column named like a settable property feeds nothing: the function built the row.
	}

	[Fact]
	public static void ARebuiltFactoryMap_Throws_AndItsPairsSpreadIntoAMemberMap()
	{
		var factoryMap = RowMap<Post>.From(
			static (int id) => new Post(id + 100, "from the function"),
			new KeyValuePair<string, string?>("id", "N"));
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("N", typeof(int))];

		// Its pairs do not hold its function: rebuilding a map from them would silently be a map of the members.
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>(factoryMap));
		Assert.Equal("mappings", ex.ParamName);
		Assert.Contains("factory map", ex.Message);

		// The map itself is the factory: parameter `id` reads column N, and the function adds 100.
		Assert.Equal(107, Assert.Single(Read(Route.Typed, columns, [1, 7], factoryMap)).Id);

		// Spreading the pairs gives a member map (documented on From): the pair names the constructor parameter, the function is gone.
		RowMap<Post> spread = [.. factoryMap];
		Post member = Assert.Single(Read(Route.Typed, columns, [1, 7], spread));
		Assert.Equal((7, null), (member.Id, member.Title));

		// A map of the model's own members still rebuilds equivalent.
		var members = new RowMap<Post>([new("Id", "N")]);
		Assert.Equal(7, Assert.Single(Read(Route.Typed, columns, [1, 7], new RowMap<Post>(members))).Id);
	}

	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 100; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 1000; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 1000);
		}

		return best;
	}

	// Cost: calling the function allocates nothing. A typed reader (a DataTableReader has typed getters) read through the factory map costs what the same type costs through its constructor map:
	// the row object and the list are the same, so nothing is allocated per row for the function, its arguments or the delegate call.
	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void TheFactoryRoute_AllocatesNothingPerRow_BeyondTheConstructorRoute(int rows)
	{
		using DataTable table = new();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Title", typeof(string));
		for (int i = 0; i < rows; i++) table.Rows.Add(i, "x");
		var command = new PlainReaderCommand(() => new DataTableReader(table));

		RowMap<Post> viaConstructor = RowMap<Post>.Default;
		var viaFactory = RowMap<Post>.From(static (int id, string title) => new Post(id, title));

		long theirs = BytesPerCall(() => command.Map(viaConstructor).ToList());
		long mine = BytesPerCall(() => command.Map(viaFactory).ToList());

		Assert.True(
			mine <= theirs + 16,
			$"{rows} rows: {mine} B per call through the factory, {theirs} B through the constructor (allowed 16 more).");
	}

	[Fact]
	public static void AMapEnumeratesThePairsItWasDeclaredWith()
	{
		var map = RowMap<WithDefaults>.From(DefaultingFactory, new KeyValuePair<string, string?>("views", "v"));
		Assert.Equal<KeyValuePair<string, string?>>([new("views", "v")], map);
	}

	[Fact]
	public static void AWrongReturnType_FailsAtMapBuild()
	{
		var wrong = Assert.Throws<ArgumentException>(() => RowMap<WeightEntry>.From(static (int a) => new Post(a, "x")));
		Assert.Equal("factory", wrong.ParamName);
		Assert.Contains("cannot be assigned", wrong.Message);

		var none = Assert.Throws<ArgumentException>(() => RowMap<WeightEntry>.From(static (int _) => { }));
		Assert.Equal("factory", none.ParamName);

		// A type the function returns that is assignable to T is fine.
		var derived = RowMap<object>.From(static (int a) => new Post(a, "x"));
		Assert.Equal(new Post(4, "x"), Assert.Single(Read(Route.Typed, [("a", typeof(int))], [4], derived)));
	}

	[Fact]
	public static void ASingleValue_IsNoRowModel_AsItIsNotForAConstructorMap()
	{
		var expected = Assert.Throws<InvalidOperationException>(() => RowMap<int>.Default);
		var factory = Assert.Throws<InvalidOperationException>(() => RowMap<int>.From(static (int a) => a));
		Assert.Equal(expected.Message, factory.Message);

		Assert.Throws<InvalidOperationException>(() => RowMap<string>.From(static (string? a) => a ?? ""));
	}

	[Fact]
	public static void ANullFactory_Throws_AndAPairsOnlyCallIsNotAFactoryCall()
	{
		var ex = Assert.Throws<ArgumentNullException>(() => RowMap<Post>.From((Delegate)null!));
		Assert.Equal("factory", ex.ParamName);

		// A bare null, or default, is the pairs overload: the default map, as before.
		Assert.Same(RowMap<Post>.Default, RowMap<Post>.From(null));
		Assert.Same(RowMap<Post>.Default, RowMap<Post>.From(default));
	}

	[Fact]
	public static void SeventeenParameters_AreMapped()
	{
		var map = RowMap<Post>.From(
			static (int a1, int a2, int a3, int a4, int a5, int a6, int a7, int a8, int a9, int a10, int a11, int a12, int a13, int a14, int a15, int a16, int a17)
				=> new Post(a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15 + a16 + a17, "seventeen"));

		ImmutableArray<ColumnDef> columns = [.. Enumerable.Range(1, 17).Select(i => new ColumnDef("a" + i, typeof(int)))];
		object?[] row = [.. Enumerable.Range(1, 17).Select(i => (object?)i)];

		Assert.Equal(new Post(153, "seventeen"), Assert.Single(Read(Route.Typed, columns, row, map)));
	}

	// The reason the factory exists (R9): a type with several constructors, which no constructor map accepts.
	public sealed class TwoConstructors
	{
		public TwoConstructors(int id) => Id = id;

		public TwoConstructors(string name) => Name = name;

		public int Id { get; }
		public string? Name { get; }
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void ATypeWithSeveralConstructors_IsBuiltByAFactoryThatCallsTheOneItWants(Route route)
	{
		Assert.Throws<InvalidOperationException>(() => RowMap<TwoConstructors>.Default);
		Assert.Throws<InvalidOperationException>(() => new RowMap<TwoConstructors>([]));

		var map = RowMap<TwoConstructors>.From(static (int id) => new TwoConstructors(id));
		TwoConstructors item = Assert.Single(Read(route, [("Id", typeof(int)), ("Name", typeof(string))], [7, "ignored"], map));
		Assert.Equal((7, null), (item.Id, item.Name));
	}

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AMethodGroupAndAClosure_AreFactories(Route route)
	{
		var group = RowMap<Post>.From(MakePost);
		Assert.Equal(new Post(1, "one"), Assert.Single(Read(route, [("Id", typeof(int)), ("Title", typeof(string))], [1, "one"], group)));

		const int offset = 100;
		var closure = RowMap<Post>.From((int id, string title) => new Post(id + offset, title));
		Assert.Equal(new Post(101, "one"), Assert.Single(Read(route, [("Id", typeof(int)), ("Title", typeof(string))], [1, "one"], closure)));
	}

	static Post MakePost(int id, string title) => new(id, title);

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AConverter_AndATypeChange_WorkOnAFactoryParameter(Route route)
	{
		var map = RowMap<Post>
			.From(static (long id, string? title) => new Post((int)id, title ?? ""))
			.Convert<string, string?>("title", static s => s.Trim());

		// The int column is read as the long parameter; the converter is not called for NULL, and the parameter is nullable so the function sees it.
		Assert.Equal(new Post(5, "padded"), Assert.Single(Read(route, [("id", typeof(int)), ("title", typeof(string))], [5, "  padded "], map)));
		Assert.Equal(new Post(5, ""), Assert.Single(Read(route, [("id", typeof(int)), ("title", typeof(string))], [5, null], map)));

		var ex = Assert.Throws<ValueMappingException>(() => Read(Route.Boxed, [("id", typeof(object)), ("title", typeof(string))], ["abc", "t"], map));
		Assert.Equal("id", ex.ColumnName);
	}

	[Fact]
	public static void AMapOfAFactory_IsReadAgainOnALaterQuery()
	{
		ImmutableArray<ColumnDef> columns = [("weighed_at", typeof(DateTime)), ("vehicle_weight_type_id", typeof(string)), ("weight_value", typeof(decimal))];
		for (int i = 0; i < 3; i++)
			Assert.Equal(10m, Assert.Single(Read(Route.Typed, columns, [Stamp, "T", 10m], FromHistory)).TareWeight);
	}

	// A reference parameter with a declared default shows whether the compiler's nullable annotation reached the lambda's parameter (U4):
	// non-nullable gets the declared default for a NULL, nullable and unannotated get null.
	public sealed record Labeled(int Id, string Code, string? Other);

	[Theory]
	[MemberData(nameof(Routes))]
	public static void AReferenceParameterWithADefault_FollowsItsNullableAnnotation(Route route)
	{
		var map = RowMap<Labeled>.From(static (int id, string code = "d1", string? other = "d2") => new Labeled(id, code, other));

		Labeled item = Assert.Single(Read(route, [("id", typeof(int)), ("code", typeof(string)), ("other", typeof(string))], [1, null, null], map));
		Assert.Equal(("d1", null), (item.Code, item.Other));
	}
}
