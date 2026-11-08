using Open.Database.Extensions.Core;
using System.Buffers;
using System.Reflection;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// Three small allocation fixes in the library's internals, each proved by a measurement and each with its contract pinned:
/// <c>ToDictionary</c> presizes its dictionary, <c>ParameterNames.Match</c> removes the <c>@</c> of the real names once, and a pooled row array is cleared
/// only where it was written. Each measurement compares the call with the cheapest hand-written
/// equivalent measured in the same run, so the budget follows the runtime and the configuration instead of a number copied from one machine.
/// </summary>
[ExcludeFromCodeCoverage]
public static class AllocationQuickWinTests
{
	// ---- measuring ----

	// Bytes per call: after a warm-up, the smallest of three rounds of 1,000 calls (the allocated-bytes counter of this thread is exact, so only a stray JIT or GC adds noise).
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

	// What the hand-written version may cost beyond the measured ideal: the two differ by no more than a rounding of the measurement.
	const int Slack = 16;

	// ---- DataRecord.ToDictionary: the dictionary has its final size from the start ----

	// Each overload of ToDictionary that reads a known number of columns.
	static readonly string[] Overloads =
	[
		"KeyValuePairs", "TuplesEnumerable", "TuplesList", "TuplesSpan", "NamesEnumerable", "NamesList", "NamesSpan",
	];

	// The overloads whose argument is an IEnumerable: they are read with an enumerator, which the hand-written ideal has to pay for too.
	static bool Enumerates(string overload) => overload is "KeyValuePairs" or "TuplesEnumerable" or "NamesEnumerable";

	// Calls one overload with the columns `names` of the record, passing them in the shape that overload takes.
	static Func<Dictionary<string, object?>> Prepare(string overload, DataTableReader record, string[] names)
	{
		KeyValuePair<int, string>[] pairs = [.. names.Select(n => new KeyValuePair<int, string>(record.GetOrdinal(n), n))];
		(string Name, int Ordinal)[] tuples = [.. names.Select(n => (n, record.GetOrdinal(n)))];
		return overload switch
		{
			"KeyValuePairs" => () => record.ToDictionary((IEnumerable<KeyValuePair<int, string>>)pairs),
			"TuplesEnumerable" => () => record.ToDictionary((IEnumerable<(string Name, int Ordinal)>)tuples),
			"TuplesList" => () => record.ToDictionary((IReadOnlyList<(string Name, int Ordinal)>)tuples),
			"TuplesSpan" => () => record.ToDictionary(new ReadOnlySpan<(string Name, int Ordinal)>(tuples)),
			"NamesEnumerable" => () => record.ToDictionary((IEnumerable<string>)names),
			"NamesList" => () => record.ToDictionary((IReadOnlyList<string>)names),
			"NamesSpan" => () => record.ToDictionary(new ReadOnlySpan<string>(names)),
			_ => throw new ArgumentOutOfRangeException(nameof(overload)),
		};
	}

	static object? Read(IDataRecord record, string name)
	{
		object value = record.GetValue(record.GetOrdinal(name));
		return value is DBNull ? null : value;
	}

	// The cheapest way to do the same by hand: a dictionary of the final size and a plain loop.
	static Dictionary<string, object?> ByHand(IDataRecord record, string[] names, bool enumerate)
	{
		var e = new Dictionary<string, object?>(names.Length);
		if (enumerate)
		{
			foreach (string name in (IEnumerable<string>)names)
				e.Add(name, Read(record, name));
		}
		else
		{
			for (int i = 0; i < names.Length; i++)
				e.Add(names[i], Read(record, names[i]));
		}

		return e;
	}

	static string[] ColumnNames(int count) => [.. Enumerable.Range(0, count).Select(i => "Column" + i)];

	// A one-row table of mixed columns (int and decimal cells are boxed on every read), read by a data table reader positioned on its row.
	static DataTableReader PositionedRow(int columns)
	{
		var table = new DataTable();
		var row = new object[columns];
		for (int i = 0; i < columns; i++)
		{
			(Type type, object value) = (i % 3) switch
			{
				0 => (typeof(int), i),
				1 => (typeof(string), "v" + i),
				_ => (typeof(decimal), (object)(i * 1.5m)),
			};
			table.Columns.Add("Column" + i, type);
			row[i] = value;
		}

		table.Rows.Add(row);
		DataTableReader reader = table.CreateDataReader();
		Assert.True(reader.Read());
		return reader;
	}

	public static TheoryData<string, int> ToDictionaryCases { get; } = BuildToDictionaryCases();

	static TheoryData<string, int> BuildToDictionaryCases()
	{
		var data = new TheoryData<string, int>();
		foreach (string overload in Overloads)
		{
			data.Add(overload, 4);
			data.Add(overload, 12);
		}

		return data;
	}

	[Theory]
	[MemberData(nameof(ToDictionaryCases))]
	public static void ToDictionary_AllocatesOnlyADictionaryOfItsFinalSize(string overload, int columns)
	{
		DataTableReader record = PositionedRow(columns);
		string[] names = ColumnNames(columns);
		Func<Dictionary<string, object?>> call = Prepare(overload, record, names);

		Assert.Equal(columns, call().Count);

		long actual = BytesPerCall(() => call());
		long ideal = BytesPerCall(() => ByHand(record, names, Enumerates(overload)));

		Assert.True(actual <= ideal + Slack, $"{overload}, {columns} columns: {actual} B per call, the hand-written ideal is {ideal} B.");
	}

	static DataTableReader PositionedSample()
	{
		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		table.Columns.Add("Value", typeof(string));
		table.Rows.Add(1, DBNull.Value, "v");
		DataTableReader reader = table.CreateDataReader();
		Assert.True(reader.Read());
		return reader;
	}

	[Theory]
	[InlineData("KeyValuePairs")]
	[InlineData("TuplesEnumerable")]
	[InlineData("TuplesList")]
	[InlineData("TuplesSpan")]
	[InlineData("NamesEnumerable")]
	[InlineData("NamesList")]
	[InlineData("NamesSpan")]
	public static void ToDictionary_KeepsTheOrderOfTheMappingAndConvertsDBNullToNull(string overload)
	{
		DataTableReader record = PositionedSample();

		Dictionary<string, object?> result = Prepare(overload, record, ["Value", "Id", "Name"])();

		Assert.Equal(["Value", "Id", "Name"], result.Keys);
		Assert.Equal("v", result["Value"]);
		Assert.Equal(1, result["Id"]);
		Assert.Null(result["Name"]);
	}

	[Theory]
	[InlineData("KeyValuePairs")]
	[InlineData("TuplesEnumerable")]
	[InlineData("TuplesList")]
	[InlineData("TuplesSpan")]
	[InlineData("NamesEnumerable")]
	[InlineData("NamesList")]
	[InlineData("NamesSpan")]
	public static void ToDictionary_ARepeatedKey_Throws(string overload)
	{
		DataTableReader record = PositionedSample();

		Func<Dictionary<string, object?>> call = Prepare(overload, record, ["Id", "Name", "Id"]);

		Assert.Throws<ArgumentException>(() => call());
	}

	[Theory]
	[InlineData("KeyValuePairs")]
	[InlineData("TuplesEnumerable")]
	[InlineData("TuplesList")]
	[InlineData("TuplesSpan")]
	[InlineData("NamesEnumerable")]
	[InlineData("NamesList")]
	[InlineData("NamesSpan")]
	public static void ToDictionary_NothingToRead_IsAnEmptyDictionary(string overload)
		=> Assert.Empty(Prepare(overload, PositionedSample(), [])());

	[Fact]
	public static void ToDictionary_AnEnumerableThatIsNotACollection_IsReadOnce_InOrder()
	{
		DataTableReader record = PositionedSample();
		int enumerations = 0;
		IEnumerable<string> Names()
		{
			enumerations++;
			yield return "Name";
			yield return "Id";
		}

		Dictionary<string, object?> byName = record.ToDictionary(Names());
		IEnumerable<KeyValuePair<int, string>> Pairs()
		{
			yield return new(2, "v");
			yield return new(0, "i");
		}

		Dictionary<string, object?> byPairs = record.ToDictionary(Pairs());
		IEnumerable<(string Name, int Ordinal)> Tuples()
		{
			yield return ("a", 1);
			yield return ("b", 0);
		}

		Dictionary<string, object?> byTuples = record.ToDictionary(Tuples());

		Assert.Equal(1, enumerations);
		Assert.Equal(["Name", "Id"], byName.Keys);
		Assert.Equal(["v", "i"], byPairs.Keys);
		Assert.Equal("v", byPairs["v"]);
		Assert.Equal(["a", "b"], byTuples.Keys);
		Assert.Null(byTuples["a"]);
	}

	[Fact]
	public static void ToDictionary_ANullMapping_ThrowsArgumentNull()
	{
		DataTableReader record = PositionedSample();

		Assert.Throws<ArgumentNullException>(() => record.ToDictionary((IEnumerable<KeyValuePair<int, string>>)null!));
		Assert.Throws<ArgumentNullException>(() => record.ToDictionary((IEnumerable<(string Name, int Ordinal)>)null!));
	}

	// ---- ParameterNames.Match: the @ of the real names is removed once per call, not once per name, candidate and tier ----

	static readonly Func<ImmutableArray<string>, ImmutableArray<string>, string?> Match = typeof(DataRecordExtensions).Assembly
		.GetType("Open.Database.Extensions.ParameterNames", throwOnError: true)!
		.GetMethod("Match", BindingFlags.Static | BindingFlags.NonPublic)!
		.CreateDelegate<Func<ImmutableArray<string>, ImmutableArray<string>, string?>>();

	static readonly ImmutableArray<string> RealNames =
	[
		"@customer_id", "@order_date", "@ship_to", "@amount", "@currency", "@notes", "@is_active", "@created_by", "@region_code", "@page_size",
	];

	// Nine candidates that match nothing (each runs all four tiers over every real name) and then one that matches in the third tier.
	static readonly ImmutableArray<string> Candidates =
	[
		"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "CustomerId",
	];

	[Fact]
	public static void Match_ReturnsTheRealNameAsReported_WithItsAt()
	{
		Assert.Equal("@customer_id", Match(Candidates, RealNames));
		Assert.Equal("@page_size", Match(["PAGESIZE"], RealNames));
		Assert.Equal("@ship_to", Match(["p_shipTo", "ship_to"], RealNames));
		Assert.Null(Match(["nothing"], RealNames));
	}

	[Fact]
	public static void Match_AllocatesOneSetOfBareNames_NotOnePerCandidateAndTier()
	{
		long actual = BytesPerCall(() => Match(Candidates, RealNames));

		// The cheapest possible: the bare names of all ten real names, once.
		string[] real = [.. RealNames];
		long ideal = BytesPerCall(() =>
		{
			var bare = new string[real.Length];
			for (int i = 0; i < real.Length; i++)
				bare[i] = real[i][1..];
		});

		Assert.True(actual <= ideal + Slack, $"{actual} B per Match, the cheapest possible is {ideal} B.");
	}

	[Fact]
	public static void Match_NamesWithoutAnAt_AllocateNothing()
	{
		ImmutableArray<string> plain = [.. RealNames.Select(n => n[1..])];

		Assert.Equal("customer_id", Match(Candidates, plain));
		Assert.Equal(0, BytesPerCall(() => Match(Candidates, plain)));
	}

	// ---- pooled arrays: only the slots that were written are cleared ----

	public sealed class Probe
	{
		public int Id { get; set; }
	}

	static readonly ImmutableArray<ColumnDef> ProbeColumns = [("Id", typeof(int))];

	static Probe[] ReadProbes(int rows)
	{
		object?[][] data = [.. Enumerable.Range(1, rows).Select(i => new object?[] { i })];
		return new FakeExecuteReader(() => TypedReader.Create(ProbeColumns, data)).Map<Probe>().ToArray();
	}

	// Puts an array of `length` slots in this thread's cache of the shared pool (where the next Rent of that size finds it), with `marker` in its last slot.
	static Probe[] Seed(int length, Probe marker)
	{
		Probe[] array = ArrayPool<Probe>.Shared.Rent(length);
		Assert.Equal(length, array.Length);
		Array.Clear(array);
		array[^1] = marker;
		ArrayPool<Probe>.Shared.Return(array, clearArray: false);
		return array;
	}

	[Fact]
	public static void ArraySink_Dispose_ClearsTheRowsItHeld_AndNothingElse()
	{
		var marker = new Probe();
		Probe[] seeded = Seed(16, marker);

		Probe[] rows = ReadProbes(5);

		Assert.Equal([1, 2, 3, 4, 5], rows.Select(p => p.Id));
		Probe[] back = ArrayPool<Probe>.Shared.Rent(16);
		Assert.Same(seeded, back); // The sink used the seeded array, so this is the array it returned.
		Assert.All(back.Take(5), p => Assert.Null(p)); // The five rows are no longer referenced by the pool.
		Assert.Same(marker, back[15]); // A slot the sink never wrote was not touched.
	}

	[Fact]
	public static void ArraySink_Grow_ClearsTheFullArrayItOutgrows_AndDisposeTheUsedPartOfTheLast()
	{
		var marker = new Probe();
		Probe[] a16 = Seed(16, marker);
		Probe[] a32 = Seed(32, marker);
		Probe[] a64 = Seed(64, marker);

		Probe[] rows = ReadProbes(40); // Fills 16, grows to 32, grows to 64 and uses 40 of its slots.

		Assert.Equal(Enumerable.Range(1, 40), rows.Select(p => p.Id));
		Assert.Same(a64, ArrayPool<Probe>.Shared.Rent(64));
		Assert.Same(a32, ArrayPool<Probe>.Shared.Rent(32));
		Assert.Same(a16, ArrayPool<Probe>.Shared.Rent(16));
		Assert.All(a16, p => Assert.Null(p)); // Full when it was outgrown, and cleared whole.
		Assert.All(a32, p => Assert.Null(p));
		Assert.All(a64.Take(40), p => Assert.Null(p)); // The last is cleared where it was written...
		Assert.Same(marker, a64[63]); // ...and only there.
	}

	// A pool that makes its arrays and does what a pool does with the clear flag, recording what came back.
	sealed class RecordingPool : ArrayPool<object>
	{
		public List<object[]> Returned { get; } = [];

		public override object[] Rent(int minimumLength) => new object[minimumLength];

		public override void Return(object[] array, bool clearArray = false)
		{
			if (clearArray) Array.Clear(array);
			Returned.Add(array);
		}
	}

	sealed class Person
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	static QueryResult<Queue<object[]>> Buffered(params object[][] rows)
		=> new(
			[.. Enumerable.Range(0, 2)],
			ImmutableArray.Create("Id", "Name"),
			new Queue<object[]>(rows));

	[Fact]
	public static void Transformer_ClearArrays_ClearsTheColumnsOfEachRow_AndNoMore()
	{
		var pool = new RecordingPool();
		object[] first = [1, "a", "unused"]; // Two columns, and a third slot as a rented array has beyond what was written.
		object[] second = [2, "b", "unused"];

		List<Person> people = [.. Transformer<Person>.Create().AsDequeueingEnumerable(Buffered(first, second), pool, clearArrays: true)];

		Assert.Equal([1, 2], people.Select(p => p.Id));
		Assert.Equal(["a", "b"], people.Select(p => p.Name));
		Assert.Collection(pool.Returned, r => Assert.Same(first, r), r => Assert.Same(second, r));
		foreach (object[] row in new[] { first, second })
		{
			Assert.Null(row[0]);
			Assert.Null(row[1]);
			Assert.Equal("unused", row[2]);
		}
	}

	[Fact]
	public static void Transformer_NoClear_LeavesTheRowsAsTheyWere()
	{
		var pool = new RecordingPool();
		object[] row = [1, "a", "unused"];

		List<Person> people = [.. Transformer<Person>.Create().AsDequeueingEnumerable(Buffered(row), pool)];

		Assert.Equal(1, people.Single().Id);
		Assert.Same(row, Assert.Single(pool.Returned));
		Assert.Equal([1, "a", "unused"], row);
	}

	[Fact]
	public static void Transformer_ClearArrays_AFailingRowIsStillClearedAndReturned()
	{
		var pool = new RecordingPool();
		object[] bad = ["not an int", "a", "unused"];

		IEnumerable<Person> people = Transformer<Person>.Create().AsDequeueingEnumerable(Buffered(bad), pool, clearArrays: true);

		Assert.Throws<ValueMappingException>(() => people.ToList());
		Assert.Same(bad, Assert.Single(pool.Returned));
		Assert.Null(bad[0]);
		Assert.Null(bad[1]);
	}

	[Fact]
	public static void Transformer_ClearArrays_ARowShorterThanTheColumns_FailsWithTheMappingError_NotWithTheClear()
	{
		var pool = new RecordingPool();
		object[] shortRow = [1]; // Two columns are declared, one slot exists.

		IEnumerable<Person> people = Transformer<Person>.Create().AsDequeueingEnumerable(Buffered(shortRow), pool, clearArrays: true);

		Exception ex = Assert.ThrowsAny<Exception>(() => people.ToList());
		Assert.IsNotType<ArgumentException>(ex); // Array.Clear on a longer range than the array would hide the real failure.
		Assert.Same(shortRow, Assert.Single(pool.Returned));
	}
}
