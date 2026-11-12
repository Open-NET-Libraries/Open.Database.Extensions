using System.Data;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ToDictionary(keySelector, valueSelector, comparer) builds the dictionary in
// one pass over the rows, keeping the value the selector returns and not the
// row, with the same duplicate-key behavior as the key-only ToDictionary.
[ExcludeFromCodeCoverage]
public static class MappedQueryDictionaryValuesTests
{
	sealed record Person
	{
		public required int Id { get; init; }
		public string? Name { get; init; }
	}

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	const int LaterSets = 2; // a full read-through is three calls of NextResult

	static FakeExecuteReader Command(int rows)
		=> new(() => Reader(Rows(rows)));

	static FakeExecuteReader People()
		=> new(() => Reader(TwoRows)); // 1/"a", 2/null

	static DataTable Table(int count)
	{
		DataTable table = new();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		for (int i = 1; i <= count; i++)
		{
			table.Rows.Add(i, "n" + i);
		}

		return table;
	}

	// ---- every level: expressive command, reader, command, table ----

	public static TheoryData<string> Levels
		=> ["command", "reader", "commandView", "table"];

	static Dictionary<int, string?> Sync(
		string level,
		int rows,
		Func<Person, int> key,
		Func<Person, string?> value,
		IEqualityComparer<int>? comparer = null)
	{
		switch (level)
		{
			case "command":
				return Command(rows).Map<Person>()
					.ToDictionary(key, value, comparer);
			case "reader":
				return Reader(Rows(rows)).Map<Person>()
					.ToDictionary(key, value, comparer);
			case "commandView":
				return new FakeDbCommand(
					new FakeDbConnection(),
					() => Reader(Rows(rows))).Map<Person>()
					.ToDictionary(key, value, comparer);
			case "table":
				return Table(rows).Map<Person>()
					.ToDictionary(key, value, comparer);
			default:
				throw new ArgumentOutOfRangeException(nameof(level));
		}
	}

	static async Task<Dictionary<int, string?>> Async(
		string level,
		int rows,
		Func<Person, int> key,
		Func<Person, string?> value,
		IEqualityComparer<int>? comparer = null)
	{
		switch (level)
		{
			case "command":
				return await Command(rows).Map<Person>()
					.ToDictionaryAsync(key, value, comparer, Ct);
			case "reader":
				return await Reader(Rows(rows)).Map<Person>()
					.ToDictionaryAsync(key, value, comparer, Ct);
			case "commandView":
				return await new FakeDbCommand(
					new FakeDbConnection(),
					() => Reader(Rows(rows))).Map<Person>()
					.ToDictionaryAsync(key, value, comparer, Ct);
			case "table":
				return await Table(rows).Map<Person>()
					.ToDictionaryAsync(key, value, comparer, Ct);
			default:
				throw new ArgumentOutOfRangeException(nameof(level));
		}
	}

	static void AssertThreeRows(Dictionary<int, string?> actual)
		=> Assert.Equal(
			new Dictionary<int, string?> { [1] = "n1", [2] = "n2", [3] = "n3" },
			actual);

	[Theory, MemberData(nameof(Levels))]
	public static async Task KeySelectorAndValueSelector_BuildTheDictionary(
		string level)
	{
		AssertThreeRows(Sync(level, 3, p => p.Id, p => p.Name));
		AssertThreeRows(await Async(level, 3, p => p.Id, p => p.Name));
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task NoRows_GiveAnEmptyDictionary(string level)
	{
		Assert.Empty(Sync(level, 0, p => p.Id, p => p.Name));
		Assert.Empty(await Async(level, 0, p => p.Id, p => p.Name));
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task ADuplicateKey_Throws_AsItDoesWithoutAValueSelector(
		string level)
	{
		Assert.Throws<ArgumentException>(
			() => Sync(level, 3, _ => 1, p => p.Name));
		await Assert.ThrowsAsync<ArgumentException>(
			async () => await Async(level, 3, _ => 1, p => p.Name));

		// The key-only form it mirrors.
		Assert.Throws<ArgumentException>(
			() => Command(3).Map<Person>().ToDictionary(_ => 1));
		await Assert.ThrowsAsync<ArgumentException>(
			async () => await Command(3).Map<Person>()
				.ToDictionaryAsync(_ => 1, null, Ct));
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task ANullSelector_IsRefused_BeforeAnythingRuns(
		string level)
	{
		Assert.Equal(
			"keySelector",
			Assert.Throws<ArgumentNullException>(
				() => Sync(level, 1, null!, p => p.Name)).ParamName);
		Assert.Equal(
			"valueSelector",
			Assert.Throws<ArgumentNullException>(
				() => Sync(level, 1, p => p.Id, null!)).ParamName);
		Assert.Equal(
			"keySelector",
			(await Assert.ThrowsAsync<ArgumentNullException>(
				async () => await Async(level, 1, null!, p => p.Name))).ParamName);
		Assert.Equal(
			"valueSelector",
			(await Assert.ThrowsAsync<ArgumentNullException>(
				async () => await Async(level, 1, p => p.Id, null!))).ParamName);
	}

	[Fact]
	public static async Task ANullSelector_NeverExecutesTheCommand()
	{
		FakeExecuteReader command = Command(1);

		Assert.Throws<ArgumentNullException>(
			() => command.Map<Person>().ToDictionary<int, string>(null!, p => p.Name!));
		Assert.Throws<ArgumentNullException>(
			() => command.Map<Person>().ToDictionary<int, string>(p => p.Id, null!));
		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await command.Map<Person>()
				.ToDictionaryAsync<int, string>(null!, p => p.Name!, null, Ct));
		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await command.Map<Person>()
				.ToDictionaryAsync<int, string>(p => p.Id, null!, null, Ct));

		Assert.Equal(0, command.ExecuteCount);
	}

	// ---- what the selectors see ----

	[Fact]
	public static async Task EachSelector_SeesEachRowOnce_KeyFirst()
	{
		List<string> log = [];
		int Key(Person p)
		{
			log.Add("k" + p.Id);
			return p.Id;
		}

		string? Value(Person p)
		{
			log.Add("v" + p.Id);
			return p.Name;
		}

		AssertThreeRows(
			Command(3).Map<Person>().ToDictionary(Key, Value));
		Assert.Equal(["k1", "v1", "k2", "v2", "k3", "v3"], log);

		log.Clear();
		AssertThreeRows(
			await Command(3).Map<Person>().ToDictionaryAsync(Key, Value, null, Ct));
		Assert.Equal(["k1", "v1", "k2", "v2", "k3", "v3"], log);
	}

	[Fact]
	public static async Task SkippedRows_AreNeverSeenByTheSelectors()
	{
		List<int> keys = [];
		List<int> values = [];
		int Key(Person p)
		{
			keys.Add(p.Id);
			return p.Id;
		}

		int Value(Person p)
		{
			values.Add(p.Id);
			return p.Id * 10;
		}

		Assert.Equal(
			new Dictionary<int, int> { [2] = 20, [3] = 30 },
			Command(5).Map<Person>().Skip(1).Take(2).ToDictionary(Key, Value));
		Assert.Equal([2, 3], keys);
		Assert.Equal([2, 3], values);

		keys.Clear();
		values.Clear();
		Assert.Equal(
			new Dictionary<int, int> { [4] = 40, [5] = 50 },
			await Command(5).Map<Person>().Skip(3).ToDictionaryAsync(Key, Value, null, Ct));
		Assert.Equal([4, 5], keys);
		Assert.Equal([4, 5], values);

		Assert.Equal(
			new Dictionary<int, int> { [1] = 10 },
			await Command(5).Map<Person>().Take(1).ToDictionaryAsync(Key, Value, null, Ct));
	}

	[Fact]
	public static async Task ANullValue_IsKept()
	{
		Dictionary<int, string?> sync
			= People().Map<Person>().ToDictionary(p => p.Id, p => p.Name);
		Assert.Equal("a", sync[1]);
		Assert.True(sync.ContainsKey(2));
		Assert.Null(sync[2]);

		Dictionary<int, string> coalesced = await People().Map<Person>()
			.ToDictionaryAsync(p => p.Id, p => p.Name ?? "", null, Ct);
		Assert.Equal(new Dictionary<int, string> { [1] = "a", [2] = "" }, coalesced);
	}

	[Fact]
	public static async Task ASingleValue_CanBeKeyedAndValued()
	{
		Assert.Equal(
			new Dictionary<int, long> { [1] = 10, [2] = 20 },
			Command(2).Map<int>().ToDictionary(i => i, i => i * 10L));
		Assert.Equal(
			new Dictionary<int, long> { [1] = 10, [2] = 20 },
			await Command(2).Map<int>()
				.ToDictionaryAsync(i => i, i => i * 10L, null, Ct));
	}

	// ---- the comparer ----

	[Fact]
	public static async Task TheComparer_IsUsedForTheKeys()
	{
		FakeExecuteReader command = new(() => Reader([[1, "a", null], [2, "b", null]]));

		Dictionary<string, int> sync = command.Map<Person>().ToDictionary(
			p => p.Name!, p => p.Id, StringComparer.OrdinalIgnoreCase);
		Assert.Equal(1, sync["A"]);
		Assert.Equal(2, sync["B"]);

		Dictionary<string, int> viaAsync = await command.Map<Person>()
			.ToDictionaryAsync(
				p => p.Name!, p => p.Id, StringComparer.OrdinalIgnoreCase, Ct);
		Assert.Equal(1, viaAsync["A"]);

		// Without it the lookup is case-sensitive.
		Assert.False(
			command.Map<Person>().ToDictionary(p => p.Name!, p => p.Id)
				.ContainsKey("A"));
	}

	[Fact]
	public static async Task ADuplicateUnderTheComparer_Throws()
	{
		FakeExecuteReader command = new(() => Reader([[1, "a", null], [2, "A", null]]));

		// Distinct keys by default, one key under the comparer.
		Assert.Equal(
			2,
			command.Map<Person>().ToDictionary(p => p.Name!, p => p.Id).Count);
		Assert.Throws<ArgumentException>(
			() => command.Map<Person>().ToDictionary(
				p => p.Name!, p => p.Id, StringComparer.OrdinalIgnoreCase));
		await Assert.ThrowsAsync<ArgumentException>(
			async () => await command.Map<Person>().ToDictionaryAsync(
				p => p.Name!, p => p.Id, StringComparer.OrdinalIgnoreCase, Ct));
	}

	// ---- later result sets, failures and cancellation ----

	static FakeExecuteReader Sets(
		int rows,
		out Func<MultiSetReader> reader,
		int? failAt = null)
	{
		MultiSetReader? last = null;
		reader = () => last ?? throw new InvalidOperationException("never executed");
		return new FakeExecuteReader(
			() => last = new MultiSetReader(Rows(rows), LaterSets, failAt));
	}

	[Fact]
	public static async Task TheLaterResultSets_AreReadThrough_AndALaterFailureIsThrown()
	{
		FakeExecuteReader ok = Sets(2, out Func<MultiSetReader> okReader);
		Assert.Equal(
			2,
			ok.Map<Person>().ToDictionary(p => p.Id, p => p.Name).Count);
		Assert.Equal(LaterSets + 1, okReader().NextResultCalls);
		Assert.Equal(0, okReader().ReadsAfterNextResult);

		FakeExecuteReader failing = Sets(
			2, out Func<MultiSetReader> failingReader, failAt: 2);
		Assert.Throws<LaterSetFailure>(
			() => failing.Map<Person>().ToDictionary(p => p.Id, p => p.Name));
		Assert.Equal(2, failingReader().NextResultCalls);

		FakeExecuteReader asyncOk = Sets(2, out Func<MultiSetReader> asyncOkReader);
		Assert.Equal(
			2,
			(await asyncOk.Map<Person>()
				.ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct)).Count);
		Assert.Equal(LaterSets + 1, asyncOkReader().NextResultCalls);

		FakeExecuteReader asyncFailing = Sets(
			2, out Func<MultiSetReader> asyncFailingReader, failAt: 1);
		await Assert.ThrowsAsync<LaterSetFailure>(
			async () => await asyncFailing.Map<Person>()
				.ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct));
		Assert.Equal(1, asyncFailingReader().NextResultCalls);
	}

	[Fact]
	public static async Task SingleResult_ReadsNothingThrough()
	{
		FakeExecuteReader command = Sets(
			2, out Func<MultiSetReader> reader, failAt: 1);

		Assert.Equal(
			2,
			command.Map<Person>().SingleResult()
				.ToDictionary(p => p.Id, p => p.Name).Count);
		Assert.Equal(
			2,
			(await command.Map<Person>().SingleResult()
				.ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct)).Count);

		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static async Task ASelectorThatThrows_IsNotReplaced_AndNothingIsReadThrough()
	{
		FakeExecuteReader command = Sets(
			3, out Func<MultiSetReader> reader, failAt: 1);

		Assert.Equal(
			"mine",
			Assert.Throws<FormatException>(
				() => command.Map<Person>().ToDictionary<int, string>(
					p => p.Id, _ => throw new FormatException("mine"))).Message);
		Assert.Equal(
			"mine",
			(await Assert.ThrowsAsync<FormatException>(
				async () => await command.Map<Person>()
					.ToDictionaryAsync<int, string>(
						p => p.Id,
						_ => throw new FormatException("mine"),
						null,
						Ct))).Message);

		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static async Task ACanceledToken_Cancels_AndTheCallsTokenReachesTheReader()
	{
		using var canceled = new CancellationTokenSource();
		await canceled.CancelAsync();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Command(1).Map<Person>()
				.ToDictionaryAsync(p => p.Id, p => p.Name, null, canceled.Token));

		FakeExecuteReader onCommand = Command(1);
		onCommand.CancellationToken = canceled.Token;
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await onCommand.Map<Person>()
				.ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct));

		using var live = new CancellationTokenSource();
		FakeExecuteReader command = Command(1);
		await command.Map<Person>()
			.ToDictionaryAsync(p => p.Id, p => p.Name, null, live.Token);
		Assert.Equal([live.Token], command.ReaderTokens);
	}

	// ---- a reader that is not a DbDataReader ----

	[Fact]
	public static async Task APlainIDataReader_IsReadTheSameWay()
	{
		static IDataReader Plain()
		{
			IDataReader reader = Substitute.For<IDataReader>();
			reader.Read().Returns(true, true, false);
			reader.GetInt32(0).Returns(1, 2);
			return reader;
		}

		var command = new PlainReaderCommand(Plain);
		var expected = new Dictionary<int, string> { [1] = "v1", [2] = "v2" };

		Assert.Equal(
			expected,
			command.Map(r => r.GetInt32(0)).ToDictionary(i => i, i => "v" + i));
		Assert.Equal(
			expected,
			await command.Map(r => r.GetInt32(0))
				.ToDictionaryAsync(i => i, i => "v" + i, null, Ct));
	}

	// ---- through IMappedQuery<T> (the extensions, for generic code) ----

	[Fact]
	public static async Task ThroughTheInterface_TheExtensionsDoTheSame()
	{
		static IMappedQuery<Person> Q(int rows) => Command(rows).Map<Person>();

		AssertThreeRows(Q(3).ToDictionary(p => p.Id, p => p.Name));
		AssertThreeRows(
			await Q(3).ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct));
		Assert.Empty(Q(0).ToDictionary(p => p.Id, p => p.Name));
		Assert.Empty(
			await Q(0).ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct));

		Assert.Throws<ArgumentException>(
			() => Q(3).ToDictionary(_ => 1, p => p.Name));
		await Assert.ThrowsAsync<ArgumentException>(
			async () => await Q(3).ToDictionaryAsync(_ => 1, p => p.Name, null, Ct));
	}

	[Fact]
	public static async Task ThroughTheInterface_ANullArgumentIsRefused()
	{
		IMappedQuery<Person> query = null!;
		IMappedQuery<Person> real = Command(1).Map<Person>();

		Assert.Equal(
			"query",
			Assert.Throws<ArgumentNullException>(
				() => query.ToDictionary(p => p.Id, p => p.Name)).ParamName);
		Assert.Equal(
			"keySelector",
			Assert.Throws<ArgumentNullException>(
				() => real.ToDictionary<Person, int, string>(null!, p => p.Name!)).ParamName);
		Assert.Equal(
			"valueSelector",
			Assert.Throws<ArgumentNullException>(
				() => real.ToDictionary<Person, int, string>(p => p.Id, null!)).ParamName);
		Assert.Equal(
			"query",
			(await Assert.ThrowsAsync<ArgumentNullException>(
				async () => await query.ToDictionaryAsync(
					p => p.Id, p => p.Name, null, Ct))).ParamName);
		Assert.Equal(
			"valueSelector",
			(await Assert.ThrowsAsync<ArgumentNullException>(
				async () => await real.ToDictionaryAsync<Person, int, string>(
					p => p.Id, null!, null, Ct))).ParamName);
	}

	// ---- overload resolution (compile-only) ----

	// The key-only forms, with and without a comparer, still bind to
	// themselves, and the value forms are found for every shape of call.
#pragma warning disable xUnit1051 // The point is the forms that pass no token.
	[Fact]
	public static async Task TheNewOverloads_DoNotMakeNeighbouringCallsAmbiguous()
	{
		MappedQuery<Person> q = People().Map<Person>();
		IEqualityComparer<int> comparer = EqualityComparer<int>.Default;

		Dictionary<int, Person> keyOnly = q.ToDictionary(p => p.Id);
		keyOnly = q.ToDictionary(p => p.Id, null);
		keyOnly = q.ToDictionary(p => p.Id, comparer);
		keyOnly = await q.ToDictionaryAsync(p => p.Id, null, Ct);
		keyOnly = await q.ToDictionaryAsync(p => p.Id, comparer, Ct);
		keyOnly = await q.ToDictionaryAsync(p => p.Id, cancellationToken: Ct);
		Assert.Equal(2, keyOnly.Count);

		Dictionary<int, string?> values = q.ToDictionary(p => p.Id, p => p.Name);
		values = q.ToDictionary(p => p.Id, p => p.Name, null);
		values = q.ToDictionary(p => p.Id, p => p.Name, comparer);
		values = q.ToDictionary(p => p.Id, p => p.Name, comparer: comparer);
		values = await q.ToDictionaryAsync(p => p.Id, p => p.Name);
		values = await q.ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct);
		values = await q.ToDictionaryAsync(p => p.Id, p => p.Name, comparer, Ct);
		values = await q.ToDictionaryAsync(
			p => p.Id, p => p.Name, cancellationToken: Ct);
		values = await q.Skip(0).ToDictionaryAsync(p => p.Id, p => p.Name);
		values = await q.Skip(0).Take(2).ToDictionaryAsync(
			p => p.Id, p => p.Name, cancellationToken: Ct);
		Assert.Equal(2, values.Count);

		IMappedQuery<Person> boxed = q;
		keyOnly = boxed.ToDictionary(p => p.Id);
		keyOnly = boxed.ToDictionary(p => p.Id, null);
		values = boxed.ToDictionary(p => p.Id, p => p.Name);
		values = await boxed.ToDictionaryAsync(p => p.Id, p => p.Name, null, Ct);
		Assert.Equal(2, values.Count);
	}
#pragma warning restore xUnit1051
}
