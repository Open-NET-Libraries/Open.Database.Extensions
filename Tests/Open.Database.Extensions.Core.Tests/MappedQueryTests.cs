using Person = Open.Database.Extensions.Tests.TransformerResultsTests.Person;

#nullable enable

namespace Open.Database.Extensions.Tests;

// End-to-end coverage for the .Map<T>() deferred query surface: the producers on IExecuteReader,
// the staged Skip/Take config structs, and every IMappedQuery<T> terminal (sync + async). The async
// tests run over a real DbDataReader (FakeExecuteReader), so they exercise the true streaming branch.
[ExcludeFromCodeCoverage]
public static class MappedQueryTests
{
	static readonly string[] Columns = ["FirstName", "LastName", "Age"];

	static FakeExecuteReader Cmd(params object?[][] rows) => new(Columns, rows);

	static FakeExecuteReader ThreePeople() => Cmd(
		["Ada", "Lovelace", 36],
		["Alan", "Turing", 41],
		["Grace", "Hopper", 85]);

	static string Names(IEnumerable<Person> people) => string.Join(",", people.Select(p => p.FirstName));

	// ---- producers ----

	[Fact]
	public static void Map_Poco_MapsEveryRowAndColumn()
	{
		var people = ThreePeople().Map<Person>().ToList();
		Assert.Equal(3, people.Count);
		Assert.Equal("Ada", people[0].FirstName);
		Assert.Equal("Lovelace", people[0].LastName);
		Assert.Equal(36, people[0].Age);
		Assert.Equal("Hopper", people[2].LastName);
	}

	[Fact]
	public static void Map_TupleOverride_MapsRenamedColumns()
	{
		var cmd = new FakeExecuteReader(["first", "last"], ["Ada", "Lovelace"]);
		Person p = cmd.Map<Person>(("FirstName", "first"), ("LastName", "last")).ToList().Single();
		Assert.Equal("Ada", p.FirstName);
		Assert.Equal("Lovelace", p.LastName);
	}

	[Fact]
	public static void Map_KeyValuePairOverride_MapsRenamedColumns()
	{
		var cmd = new FakeExecuteReader(["first", "last"], ["Ada", "Lovelace"]);
		KeyValuePair<string, string?>[] overrides =
		[
			new("FirstName", "first"),
			new("LastName", "last"),
		];
		Person p = cmd.Map<Person>(overrides).ToList().Single();
		Assert.Equal("Ada", p.FirstName);
		Assert.Equal("Lovelace", p.LastName);
	}

	[Fact]
	public static void Map_Selector_ProjectsRows()
	{
		var names = ThreePeople()
			.Map(r => $"{r["FirstName"]} {r["LastName"]}")
			.ToList();
		Assert.Equal("Ada Lovelace,Alan Turing,Grace Hopper", string.Join(",", names));
	}

	[Fact]
	public static void Map_IsDeferred_AndExecutesOncePerTerminal()
	{
		FakeExecuteReader cmd = ThreePeople();
		MappedQuery<Person> query = cmd.Map<Person>();
		Assert.Equal(0, cmd.ExecuteCount);      // configuring does nothing

		_ = query.ToList();
		Assert.Equal(1, cmd.ExecuteCount);      // one terminal → one execution

		_ = query.ToList();
		Assert.Equal(2, cmd.ExecuteCount);      // re-usable: each terminal re-executes
	}

	// ---- staged skip / take ----

	[Fact]
	public static void Skip_SkipsLeadingRows()
		=> Assert.Equal("Alan,Grace", Names(ThreePeople().Map<Person>().Skip(1).ToList()));

	[Fact]
	public static void Take_LimitsRows()
		=> Assert.Equal("Ada,Alan", Names(ThreePeople().Map<Person>().Take(2).ToList()));

	[Fact]
	public static void SkipThenTake_Slices()
	{
		Person only = Assert.Single(ThreePeople().Map<Person>().Skip(1).Take(1).ToList());
		Assert.Equal("Alan", only.FirstName);
	}

	[Fact]
	public static void Take_MoreThanAvailable_ReturnsAll()
		=> Assert.Equal(3, ThreePeople().Map<Person>().Take(10).ToList().Count);

	// ---- sync terminals ----

	[Fact]
	public static void ToArray_MapsRows()
		=> Assert.Equal("Ada,Alan,Grace", Names(ThreePeople().Map<Person>().ToArray()));

	[Fact]
	public static void ToImmutableArray_MapsRows()
	{
		var people = ThreePeople().Map<Person>().ToImmutableArray();
		Assert.Equal("Ada,Alan,Grace", Names(people));
	}

	[Fact]
	public static void ToDictionary_KeyedBySelector()
	{
		var byLast = ThreePeople().Map<Person>().ToDictionary(p => p.LastName!);
		Assert.Equal(3, byLast.Count);
		Assert.Equal("Ada", byLast["Lovelace"].FirstName);
	}

	[Fact]
	public static void ToHashSet_ContainsMappedValues()
	{
		var names = ThreePeople().Map(r => (string?)r["FirstName"]).ToHashSet();
		Assert.Equal(3, names.Count);
		Assert.Contains("Ada", names);
	}

	[Fact]
	public static void FirstOrDefault_ReturnsFirstRow()
	{
		Person? p = ThreePeople().Map<Person>().FirstOrDefault();
		Assert.Equal("Ada", p!.FirstName);
	}

	[Fact]
	public static void FirstOrDefault_Empty_ReturnsNull()
		=> Assert.Null(Cmd().Map<Person>().FirstOrDefault());

	[Fact]
	public static void SingleOrDefault_OneRow_ReturnsIt()
	{
		var cmd = new FakeExecuteReader(Columns, ["Ada", "Lovelace", 36]);
		Assert.Equal("Ada", cmd.Map<Person>().SingleOrDefault()!.FirstName);
	}

	[Fact]
	public static void SingleOrDefault_Empty_ReturnsNull()
		=> Assert.Null(Cmd().Map<Person>().SingleOrDefault());

	[Fact]
	public static void SingleOrDefault_MultipleRows_Throws()
		=> Assert.Throws<InvalidOperationException>(() => ThreePeople().Map<Person>().SingleOrDefault());

	// ---- async terminals (real DbDataReader streaming) ----

	[Fact]
	public static async Task ToListAsync_MapsAllRows()
		=> Assert.Equal("Ada,Alan,Grace", Names(await ThreePeople().Map<Person>().ToListAsync()));

	[Fact]
	public static async Task FirstOrDefaultAsync_ReturnsFirstRow()
	{
		Person? p = await ThreePeople().Map<Person>().FirstOrDefaultAsync();
		Assert.Equal("Ada", p!.FirstName);
	}

	[Fact]
	public static async Task SkipTake_Async_Slices()
	{
		List<Person> people = await ThreePeople().Map<Person>().Skip(1).Take(1).ToListAsync();
		Assert.Equal("Alan", Assert.Single(people).FirstName);
	}

	[Fact]
	public static async Task Selector_Async_Projects()
	{
		List<string?> last = await ThreePeople().Map(r => (string?)r["LastName"]).ToListAsync();
		Assert.Equal("Lovelace,Turing,Hopper", string.Join(",", last));
	}

	// ---- cancellation-token precedence (choose the cancelable one; don't combine) ----

	[Fact]
	public static async Task ReadAsync_FallsBackToCommandToken_WhenCallTokenNotCancelable()
	{
		using var commandCts = new CancellationTokenSource();
		var cmd = new FakeExecuteReader(Columns, ["Ada", "Lovelace", 36]) { CancellationToken = commandCts.Token };

		CancellationToken observed = default;
		await cmd.Map<Person>().ReadAsync((_, token) => { observed = token; return default; }, CancellationToken.None);

		Assert.Equal(commandCts.Token, observed);
	}

	[Fact]
	public static async Task ReadAsync_PrefersCallToken_OverCommandToken()
	{
		using var commandCts = new CancellationTokenSource();
		using var callCts = new CancellationTokenSource();
		var cmd = new FakeExecuteReader(Columns, ["Ada", "Lovelace", 36]) { CancellationToken = commandCts.Token };

		CancellationToken observed = default;
		await cmd.Map<Person>().ReadAsync((_, token) => { observed = token; return default; }, callCts.Token);

		Assert.Equal(callCts.Token, observed);
	}

	// ---- contracts ----

	[Fact]
	public static void Map_NullCommand_Throws()
	{
		IExecuteReader command = null!;
		Assert.Throws<ArgumentNullException>(() => command.Map<Person>());
	}

	[Fact]
	public static void Map_NullSelector_Throws()
		=> Assert.Throws<ArgumentNullException>(() => ThreePeople().Map((Func<IDataRecord, string>)null!));
}
