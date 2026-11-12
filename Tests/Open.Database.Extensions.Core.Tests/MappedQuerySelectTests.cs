using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// query.Select(selector) is a lazy projection of an IMappedQuery<T>:
// building it runs nothing, each terminal on it runs the command once,
// and the selector runs once per row, in row order, only while the rows are read.
// Every level that has a Map<T>() (expressive command, raw command,
// reader view, command view, table view) gets it.
[ExcludeFromCodeCoverage]
public static class MappedQuerySelectTests
{
	sealed record Person
	{
		public required int Id { get; init; }
		public string? Name { get; init; }
	}

	sealed record Label(int Id, string Text);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	#region Rigs: one way to reach a mapped query per level

	// A mapped query, and a count of the work done against it:
	// readers created for a command, rows read for a reader view.
	// A table has nothing to count.
	sealed class Rig : IDisposable
	{
		public required IMappedQuery<Person> Query { get; init; }
		public Func<int>? Work { get; init; }
		public DataTable? Table { get; init; }

		public void Dispose() => Table?.Dispose();
	}

	public static TheoryData<string> Levels
		=> ["expressive", "execute", "reader", "command", "table"];

	// The levels whose query can be read more than once.
	public static TheoryData<string> Repeatable
		=> ["expressive", "execute", "command", "table"];

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

	static DbTally Tracked(int rows)
		=> new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, rows).Select(i => (object?[])[i, "n" + i])],
		});

	static Rig Make(string level, int rows = 3)
	{
		switch (level)
		{
			case "expressive":
				DbTally tally = Tracked(rows);
				return new()
				{
					Query = tally.Factory.Command("select").Map<Person>(),
					Work = () => tally.Readers.Count,
				};

			case "execute":
				FakeExecuteReader execute = new(() => Reader(Rows(rows)));
				return new()
				{
					Query = execute.Map<Person>(),
					Work = () => execute.ExecuteCount,
				};

			case "reader":
				TrackedReader reader = TrackedRows(rows);
				return new()
				{
					Query = reader.Map<Person>(),
					Work = () => reader.Reads,
				};

			case "command":
				FakeDbCommand command = new(
					new FakeDbConnection(),
					() => Reader(Rows(rows)));
				return new()
				{
					Query = command.Map<Person>(),
					Work = () => command.Behaviors.Count,
				};

			case "table":
				DataTable table = Table(rows);
				return new() { Query = table.Map<Person>(), Table = table };

			default:
				throw new ArgumentOutOfRangeException(nameof(level));
		}
	}

	// The selector the tests use: it notes which row it was given.
	static Func<Person, string> Project(List<int>? seen = null)
		=> p =>
		{
			seen?.Add(p.Id);
			return $"{p.Id}:{p.Name}";
		};

	const string All = "1:n1,2:n2,3:n3";

	static string Flat(object? value)
		=> value switch
		{
			null => "null",
			string s => s,
			IEnumerable<string> items => string.Join(",", items),
			_ => throw new ArgumentOutOfRangeException(nameof(value)),
		};

	#endregion

	#region Nothing runs until a terminal runs

	[Theory, MemberData(nameof(Levels))]
	public static void BuildingAProjection_RunsNothing(string level)
	{
		using Rig rig = Make(level);
		List<int> seen = [];

		IMappedQuery<string> query = rig.Query.Select(Project(seen));
		IMappedQuery<string> chained = query.Select(s => s + "!");

		Assert.Empty(seen);
		if (rig.Work is { } work) Assert.Equal(0, work());

		Assert.Equal("1:n1!,2:n2!,3:n3!", Flat(chained.ToList()));
		Assert.Equal([1, 2, 3], seen);
		if (rig.Work is { } done) Assert.True(done() > 0);
	}

	#endregion

	#region Every terminal

	static readonly string[] Terminals =
	[
		"ToList", "ToArray", "ToImmutableArray", "ToHashSet", "ToDictionary",
		"FirstOrDefault", "Read", "ReadAction",
	];

	static readonly string[] AsyncTerminals =
	[
		"ToListAsync", "ToArrayAsync", "ToImmutableArrayAsync",
		"ToHashSetAsync", "ToDictionaryAsync", "FirstOrDefaultAsync",
		"ReadAsync", "ReadAsyncAction",
	];

	static TheoryData<string, string> Cases(string[] terminals)
	{
		TheoryData<string, string> data = [];
		foreach (string level in new[] { "expressive", "execute", "reader", "command", "table" })
		{
			foreach (string terminal in terminals)
			{
				data.Add(level, terminal);
			}
		}

		return data;
	}

	public static TheoryData<string, string> SyncCases => Cases(Terminals);

	public static TheoryData<string, string> AsyncCases
		=> Cases(AsyncTerminals);

	static string Expected(string terminal)
		=> terminal.StartsWith("FirstOrDefault", StringComparison.Ordinal)
			? "1:n1"
			: All;

	static object? Run(IMappedQuery<string> query, string terminal)
		=> terminal switch
		{
			"ToList" => query.ToList(),
			"ToArray" => query.ToArray(),
			"ToImmutableArray" => query.ToImmutableArray(),
			"ToHashSet" => query.ToHashSet(),
			"ToDictionary" => query.ToDictionary(s => s).Keys,
			"FirstOrDefault" => query.FirstOrDefault(),
			"Read" => query.Read(items => items.ToList()),
			"ReadAction" => ReadInto(query),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		};

	static List<string> ReadInto(IMappedQuery<string> query)
	{
		List<string> got = [];
		query.Read(items => got.AddRange(items));
		return got;
	}

	static async ValueTask<object?> RunAsync(
		IMappedQuery<string> query,
		string terminal,
		CancellationToken token)
	{
		switch (terminal)
		{
			case "ToListAsync":
				return await query.ToListAsync(token);
			case "ToArrayAsync":
				return await query.ToArrayAsync(token);
			case "ToImmutableArrayAsync":
				return await query.ToImmutableArrayAsync(token);
			case "ToHashSetAsync":
				return await query.ToHashSetAsync(null, token);
			case "ToDictionaryAsync":
				return (await query.ToDictionaryAsync(s => s, null, token)).Keys;
			case "FirstOrDefaultAsync":
				return await query.FirstOrDefaultAsync(token);
			case "ReadAsync":
				return await query.ReadAsync(
					async (items, t) => await items.ToListAsync(t),
					token);
			case "ReadAsyncAction":
				List<string> got = [];
				await query.ReadAsync(
					async (items, t) =>
					{
						await foreach (string s in items.WithCancellation(t))
						{
							got.Add(s);
						}
					},
					token);
				return got;
			default:
				throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	[Theory, MemberData(nameof(SyncCases))]
	public static void EverySyncTerminal_ReturnsTheProjectedRows(
		string level,
		string terminal)
	{
		using Rig rig = Make(level);
		List<int> seen = [];

		object? result = Run(rig.Query.Select(Project(seen)), terminal);

		Assert.Equal(Expected(terminal), Flat(result));
	}

	[Theory, MemberData(nameof(AsyncCases))]
	public static async Task EveryAsyncTerminal_ReturnsTheProjectedRows(
		string level,
		string terminal)
	{
		using Rig rig = Make(level);
		List<int> seen = [];

		object? result = await RunAsync(
			rig.Query.Select(Project(seen)),
			terminal,
			Ct);

		Assert.Equal(Expected(terminal), Flat(result));
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task SingleOrDefault_ReadsTheOnlyRow_AndThrowsForMore(
		string level)
	{
		using (Rig one = Make(level, 1))
		{
			Assert.Equal("1:n1", one.Query.Select(Project()).SingleOrDefault());
		}

		using (Rig one = Make(level, 1))
		{
			Assert.Equal(
				"1:n1",
				await one.Query.Select(Project()).SingleOrDefaultAsync(Ct));
		}

		using (Rig many = Make(level))
		{
			Assert.Throws<InvalidOperationException>(
				() => many.Query.Select(Project()).SingleOrDefault());
		}

		using (Rig many = Make(level))
		{
			await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await many.Query.Select(Project()).SingleOrDefaultAsync(Ct));
		}
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task NoRows_GiveNothing(string level)
	{
		using (Rig none = Make(level, 0))
		{
			IMappedQuery<string> query = none.Query.Select(Project());
			Assert.Empty(query.ToList());
		}

		using (Rig none = Make(level, 0))
		{
			IMappedQuery<string> query = none.Query.Select(Project());
			Assert.Null(query.FirstOrDefault());
		}

		using (Rig none = Make(level, 0))
		{
			IMappedQuery<string> query = none.Query.Select(Project());
			Assert.Empty(await query.ToArrayAsync(Ct));
		}

		using (Rig none = Make(level, 0))
		{
			IMappedQuery<string> query = none.Query.Select(Project());
			Assert.Null(await query.FirstOrDefaultAsync(Ct));
		}
	}

	#endregion

	#region The selector: once per row, in row order, only while reading

	[Theory, MemberData(nameof(Levels))]
	public static async Task TheSelector_RunsOncePerRow_InRowOrder(string level)
	{
		using (Rig rig = Make(level, 4))
		{
			List<int> seen = [];
			rig.Query.Select(Project(seen)).ToArray();
			Assert.Equal([1, 2, 3, 4], seen);
		}

		using (Rig rig = Make(level, 4))
		{
			List<int> seen = [];
			await rig.Query.Select(Project(seen)).ToListAsync(Ct);
			Assert.Equal([1, 2, 3, 4], seen);
		}
	}

	// The projection streams: the selector for a row runs when the handler
	// asks for it, not before the handler starts.
	[Theory, MemberData(nameof(Levels))]
	public static async Task TheSelector_RunsAsTheHandlerEnumerates(
		string level)
	{
		using (Rig rig = Make(level))
		{
			List<string> log = [];
			rig.Query
				.Select(p =>
				{
					log.Add("select" + p.Id);
					return p.Id;
				})
				.Read(items =>
				{
					log.Add("start");
					foreach (int id in items)
					{
						log.Add("got" + id);
					}
				});

			Assert.Equal(
				"start,select1,got1,select2,got2,select3,got3",
				string.Join(",", log));
		}

		using (Rig rig = Make(level))
		{
			List<string> log = [];
			await rig.Query
				.Select(p =>
				{
					log.Add("select" + p.Id);
					return p.Id;
				})
				.ReadAsync(
					async (items, token) =>
					{
						log.Add("start");
						await foreach (int id in items.WithCancellation(token))
						{
							log.Add("got" + id);
						}
					},
					Ct);

			Assert.Equal(
				"start,select1,got1,select2,got2,select3,got3",
				string.Join(",", log));
		}
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task AnEarlyExit_RunsTheSelectorOnlyForTheRowsRead(
		string level)
	{
		using (Rig rig = Make(level))
		{
			List<int> seen = [];
			Assert.Equal(
				"1:n1",
				rig.Query.Select(Project(seen)).FirstOrDefault());
			Assert.Equal([1], seen);
		}

		using (Rig rig = Make(level))
		{
			List<int> seen = [];
			Assert.Equal(
				"1:n1",
				await rig.Query.Select(Project(seen)).FirstOrDefaultAsync(Ct));
			Assert.Equal([1], seen);
		}

		using (Rig rig = Make(level, 5))
		{
			List<int> seen = [];
			string taken = rig.Query
				.Select(Project(seen))
				.Read(items => string.Join(",", items.Take(2)));
			Assert.Equal("1:n1,2:n2", taken);
			Assert.Equal([1, 2], seen);
		}

		using (Rig rig = Make(level, 5))
		{
			List<int> seen = [];
			string taken = await rig.Query
				.Select(Project(seen))
				.ReadAsync(
					async (items, token) =>
					{
						List<string> got = [];
						await foreach (string s in items.WithCancellation(token))
						{
							got.Add(s);
							if (got.Count == 2) break;
						}

						return string.Join(",", got);
					},
					Ct);
			Assert.Equal("1:n1,2:n2", taken);
			Assert.Equal([1, 2], seen);
		}
	}

	#endregion

	#region Skip, Take and SingleResult belong to the source

	[Fact]
	public static async Task SkipAndTake_ApplyToTheSourceRows_BeforeTheSelector()
	{
		List<int> seen = [];
		FakeExecuteReader command = new(() => Reader(Rows(5)));

		Assert.Equal(
			"2:n2,3:n3",
			Flat(command.Map<Person>().Skip(1).Take(2).Select(Project(seen)).ToList()));
		Assert.Equal([2, 3], seen);

		seen.Clear();
		Assert.Equal(
			"3:n3,4:n4,5:n5",
			Flat(command.Map<Person>().Skip(2).Select(Project(seen)).ToList()));
		Assert.Equal([3, 4, 5], seen);

		seen.Clear();
		Assert.Equal(
			"1:n1,2:n2",
			Flat(await command.Map<Person>().Take(2).Select(Project(seen)).ToListAsync(Ct)));
		Assert.Equal([1, 2], seen);

		seen.Clear();
		using DataTable table = Table(5);
		Assert.Equal(
			"4:n4",
			Flat(table.Map<Person>().Skip(3).Take(1).Select(Project(seen)).ToList()));
		Assert.Equal([4], seen);

		seen.Clear();
		Assert.Empty(command.Map<Person>().Take(0).Select(Project(seen)).ToList());
		Assert.Empty(seen);
	}

	[Fact]
	public static void SingleResult_IsStillTheSourceQuerysChoice()
	{
		FakeExecuteReader command = new(() => Reader(Rows(2)));

		command.Map<Person>().Select(Project()).ToList();
		command.Map<Person>().SingleResult().Select(Project()).ToList();

		Assert.Equal(2, command.Behaviors.Count);
		Assert.False(command.Behaviors[0].HasFlag(CommandBehavior.SingleResult));
		Assert.True(command.Behaviors[1].HasFlag(CommandBehavior.SingleResult));
	}

	const int LaterSets = 2;

	static FakeExecuteReader MultiSet(
		int rows,
		out Func<MultiSetReader> reader,
		int? failAt = null)
	{
		MultiSetReader? last = null;
		reader = () => last ?? throw new InvalidOperationException("never executed");
		return new FakeExecuteReader(
			() => last = new MultiSetReader(Rows(rows), LaterSets, failAt));
	}

	// The projection sits on Read: reading to the end reads through the later
	// result sets, as the source query does, and a failure there is thrown.
	[Fact]
	public static async Task AFailureInALaterResultSet_IsStillThrown_AfterTheLastRow()
	{
		FakeExecuteReader command = MultiSet(2, out Func<MultiSetReader> reader);
		Assert.Equal(2, command.Map<Person>().Select(Project()).ToList().Count);
		Assert.Equal(LaterSets + 1, reader().NextResultCalls);
		Assert.Equal(0, reader().ReadsAfterNextResult);

		command = MultiSet(2, out reader);
		Assert.Equal(
			2,
			(await command.Map<Person>().Select(Project()).ToListAsync(Ct)).Count);
		Assert.Equal(LaterSets + 1, reader().NextResultCalls);

		command = MultiSet(2, out _, failAt: 1);
		Assert.Throws<LaterSetFailure>(
			() => command.Map<Person>().Select(Project()).ToList());

		command = MultiSet(2, out _, failAt: 1);
		await Assert.ThrowsAsync<LaterSetFailure>(
			async () => await command.Map<Person>().Select(Project()).ToListAsync(Ct));
	}

	[Fact]
	public static async Task NoReadThrough_AfterSingleResult_OrAnEarlyExit()
	{
		FakeExecuteReader command = MultiSet(2, out Func<MultiSetReader> reader, failAt: 1);
		Assert.Equal(
			2,
			command.Map<Person>().SingleResult().Select(Project()).ToList().Count);
		Assert.Equal(0, reader().NextResultCalls);

		command = MultiSet(2, out reader, failAt: 1);
		Assert.Equal(
			"1:n1",
			command.Map<Person>().Select(Project()).FirstOrDefault());
		Assert.Equal(0, reader().NextResultCalls);

		command = MultiSet(2, out reader, failAt: 1);
		Assert.Equal(
			"1:n1",
			await command.Map<Person>().Select(Project()).FirstOrDefaultAsync(Ct));
		Assert.Equal(0, reader().NextResultCalls);
	}

	#endregion

	#region Failure and release

	// The reader, the command and the connection are released exactly as
	// they are when the caller's own handler throws or stops early.
	static void AssertReleased(DbTally tally)
	{
		LeakAssert.CommandsAndReadersDisposed(tally);
		LeakAssert.ConnectionsReleased(tally, LeakSource.Factory, tally.Pool, 1);
	}

	static IMappedQuery<Person> Expressive(DbTally tally)
		=> tally.Factory.Command("select").Map<Person>();

	static string Boom(Person p)
		=> p.Id == 2
			? throw new InvalidOperationException("boom")
			: p.Name!;

	[Theory, MemberData(nameof(Levels))]
	public static async Task ASelectorThatThrows_ThrowsFromTheTerminal(string level)
	{
		using (Rig rig = Make(level))
		{
			InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
				() => rig.Query.Select(Boom).ToList());
			Assert.Equal("boom", ex.Message);
		}

		using (Rig rig = Make(level))
		{
			InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
				async () => await rig.Query.Select(Boom).ToListAsync(Ct));
			Assert.Equal("boom", ex.Message);
		}

		using (Rig rig = Make(level))
		{
			// Not the second row: the selector was never asked for it.
			Assert.Equal("n1", rig.Query.Select(Boom).FirstOrDefault());
		}
	}

	[Fact]
	public static async Task ASelectorThatThrows_ReleasesEverything()
	{
		DbTally syncTally = Tracked(3);
		Assert.Throws<InvalidOperationException>(
			() => Expressive(syncTally).Select(Boom).ToList());
		Assert.Equal(2, Assert.Single(syncTally.Readers).Reads);
		AssertReleased(syncTally);

		DbTally asyncTally = Tracked(3);
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await Expressive(asyncTally).Select(Boom).ToListAsync(Ct));
		AssertReleased(asyncTally);
	}

	[Fact]
	public static async Task AnEarlyExit_ReleasesEverything()
	{
		DbTally first = Tracked(3);
		Assert.Equal("1:n1", Expressive(first).Select(Project()).FirstOrDefault());
		Assert.Equal(1, Assert.Single(first.Readers).Reads);
		AssertReleased(first);

		DbTally firstAsync = Tracked(3);
		Assert.Equal(
			"1:n1",
			await Expressive(firstAsync).Select(Project()).FirstOrDefaultAsync(Ct));
		Assert.Equal(1, Assert.Single(firstAsync.Readers).Reads);
		AssertReleased(firstAsync);

		DbTally taken = Tracked(5);
		Assert.Equal(
			"1:n1,2:n2",
			Expressive(taken)
				.Select(Project())
				.Read(items => string.Join(",", items.Take(2))));
		Assert.Equal(2, Assert.Single(taken.Readers).Reads);
		AssertReleased(taken);
	}

	[Fact]
	public static async Task ATerminalThatCompletes_ReleasesEverything()
	{
		DbTally syncTally = Tracked(3);
		Expressive(syncTally).Select(Project()).ToArray();
		AssertReleased(syncTally);

		DbTally asyncTally = Tracked(3);
		await Expressive(asyncTally).Select(Project()).ToArrayAsync(Ct);
		AssertReleased(asyncTally);
	}

	[Fact]
	public static async Task AHandlerThatThrows_ReleasesEverything()
	{
		DbTally syncTally = Tracked(3);
		Assert.Throws<FormatException>(
			() => Expressive(syncTally)
				.Select(Project())
				.Read(items =>
				{
					_ = items.First();
					throw new FormatException();
				}));
		AssertReleased(syncTally);

		DbTally asyncTally = Tracked(3);
		await Assert.ThrowsAsync<FormatException>(
			async () => await Expressive(asyncTally)
				.Select(Project())
				.ReadAsync(
					async (items, token) =>
					{
						_ = await items.FirstAsync(token);
						throw new FormatException();
					},
					Ct));
		AssertReleased(asyncTally);
	}

	#endregion

	#region Cancellation

	[Theory, MemberData(nameof(Levels))]
	public static async Task ACanceledToken_CancelsTheCall_BeforeTheSelectorRuns(
		string level)
	{
		using Rig rig = Make(level);
		List<int> seen = [];
		using CancellationTokenSource cts = new();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await rig.Query.Select(Project(seen)).ToListAsync(cts.Token));
		Assert.Empty(seen);
	}

	[Theory, MemberData(nameof(Levels))]
	public static async Task CancelingWhileRowsAreRead_StopsTheStream(
		string level)
	{
		using Rig rig = Make(level, 5);
		using CancellationTokenSource cts = new();
		List<int> seen = [];

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await rig.Query
				.Select(p =>
				{
					seen.Add(p.Id);
					if (p.Id == 2) cts.Cancel();
					return p.Id;
				})
				.ToListAsync(cts.Token));

		Assert.Equal([1, 2], seen);
	}

	[Fact]
	public static async Task CancelingWhileRowsAreRead_ReleasesEverything()
	{
		DbTally tally = Tracked(5);
		using CancellationTokenSource cts = new();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Expressive(tally)
				.Select(p =>
				{
					if (p.Id == 2) cts.Cancel();
					return p.Id;
				})
				.ToListAsync(cts.Token));

		AssertReleased(tally);
	}

	// The handler is given the token in effect, as the source query gives it.
	[Fact]
	public static async Task TheHandler_IsGivenTheToken_AndCanPassItOn()
	{
		FakeExecuteReader command = new(() => Reader(Rows(3)));
		using CancellationTokenSource cts = new();
		CancellationToken given = default;

		string joined = await command
			.Map<Person>()
			.Select(Project())
			.ReadAsync(
				async (items, token) =>
				{
					given = token;
					List<string> got = [];
					await foreach (string s in items.WithCancellation(token))
					{
						got.Add(s);
					}

					return string.Join(",", got);
				},
				cts.Token);

		Assert.Equal(All, joined);
		Assert.Equal(cts.Token, given);
	}

	// The token of the command and the one of the call both reach the rows.
	[Fact]
	public static async Task TheCommandsOwnToken_AlsoCancelsTheStream()
	{
		using CancellationTokenSource cts = new();
		FakeExecuteReader command = new(() => Reader(Rows(5)))
		{
			CancellationToken = cts.Token,
		};
		List<int> seen = [];

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await command
				.Map<Person>()
				.Select(p =>
				{
					seen.Add(p.Id);
					if (p.Id == 3) cts.Cancel();
					return p.Id;
				})
				.ToListAsync(Ct));

		Assert.Equal([1, 2, 3], seen);
	}

	#endregion

	#region Composing

	[Theory, MemberData(nameof(Levels))]
	public static async Task Select_Chains_AndStillReadsThroughOneReader(
		string level)
	{
		using (Rig rig = Make(level))
		{
			List<string> log = [];
			IMappedQuery<string> query = rig.Query
				.Select(p =>
				{
					log.Add("a" + p.Id);
					return p.Id;
				})
				.Select(id =>
				{
					log.Add("b" + id);
					return id * 10;
				})
				.Select(n => $"#{n}");

			Assert.Equal("#10,#20,#30", Flat(query.ToList()));
			Assert.Equal("a1,b1,a2,b2,a3,b3", string.Join(",", log));
			if (rig.Work is { } work && level != "reader") Assert.Equal(1, work());
		}

		using (Rig rig = Make(level))
		{
			IMappedQuery<string> query = rig.Query
				.Select(p => p.Id)
				.Select(id => id * 10)
				.Select(n => $"#{n}");

			Assert.Equal("#10,#20,#30", Flat(await query.ToListAsync(Ct)));
		}
	}

	[Theory, MemberData(nameof(Repeatable))]
	public static async Task TheSameProjection_RunsTheCommandAgainForEachTerminal(
		string level)
	{
		using Rig rig = Make(level);
		List<int> seen = [];
		IMappedQuery<string> query = rig.Query.Select(Project(seen));

		Assert.Equal(All, Flat(query.ToList()));
		Assert.Equal(All, Flat(await query.ToListAsync(Ct)));
		Assert.Equal("1:n1", query.FirstOrDefault());

		Assert.Equal([1, 2, 3, 1, 2, 3, 1], seen);
		if (rig.Work is { } work) Assert.Equal(3, work());
	}

	[Fact]
	public static void ASelectedQuery_OfADefaultStruct_FailsAtTheTerminal_NotAtSelect()
	{
		IMappedQuery<int> query = default(MappedQuery<int>);

		IMappedQuery<int> projected = query.Select(i => i + 1);

		Assert.Throws<InvalidOperationException>(() => projected.ToList());
	}

	#endregion

	#region Arguments

	[Fact]
	public static void ANullQueryOrSelector_ThrowsAtTheSelectCall()
	{
		IMappedQuery<Person>? none = null;
		using Rig rig = Make("execute");

		Assert.Equal(
			"query",
			Assert.Throws<ArgumentNullException>(
				() => none!.Select(p => p.Id)).ParamName);
		Assert.Equal(
			"selector",
			Assert.Throws<ArgumentNullException>(
				() => rig.Query.Select((Func<Person, int>)null!)).ParamName);
		Assert.Equal(0, rig.Work!());
	}

	// The projected query guards its own entry points once,
	// before anything is executed.
	[Fact]
	public static async Task ANullHandler_ThrowsAtTheCall_AndRunsNothing()
	{
		using Rig rig = Make("execute");
		IMappedQuery<int> query = rig.Query.Select(p => p.Id);

		Assert.Equal(
			"handler",
			Assert.Throws<ArgumentNullException>(
				() => query.Read((Action<IEnumerable<int>>)null!)).ParamName);
		Assert.Equal(
			"handler",
			Assert.Throws<ArgumentNullException>(
				() => query.Read((Func<IEnumerable<int>, int>)null!)).ParamName);
		ArgumentNullException noHandler = await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await query.ReadAsync(
				(Func<IAsyncEnumerable<int>, CancellationToken, ValueTask>)null!,
				Ct));
		Assert.Equal("handler", noHandler.ParamName);
		noHandler = await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await query.ReadAsync(
				(Func<IAsyncEnumerable<int>, CancellationToken, ValueTask<int>>)null!,
				Ct));
		Assert.Equal("handler", noHandler.ParamName);
		Assert.Equal(0, rig.Work!());
	}

	#endregion

	#region A query factory

	// A data class returns the query; each caller picks the terminal.
	static IMappedQuery<Label> Labels(IExecuteReader command)
		=> command
			.Map<Person>()
			.Select(p => new Label(p.Id, p.Name ?? ""));

	[Fact]
	public static async Task AQueryFactory_LetsEachCallerPickTheTerminal()
	{
		FakeExecuteReader command = new(() => Reader(Rows(3)));

		IMappedQuery<Label> query = Labels(command);
		Assert.Equal(0, command.ExecuteCount);

		List<Label> all = await Labels(command).ToListAsync(Ct);
		Label? first = await Labels(command).FirstOrDefaultAsync(Ct);
		Label[] array = Labels(command).ToArray();

		Assert.Equal(
			new Label[] { new(1, "n1"), new(2, "n2"), new(3, "n3") },
			all);
		Assert.Equal(new Label(1, "n1"), first);
		Assert.Equal(3, array.Length);
		Assert.Equal(3, command.ExecuteCount);

		// The one value can be read again, and runs the command again.
		Assert.Equal(new Label(1, "n1"), query.FirstOrDefault());
		Assert.Equal(4, command.ExecuteCount);
	}

	#endregion

	#region Cost

	static PlainReaderCommand Ints(int count)
	{
		DataTable table = new();
		table.Columns.Add("v", typeof(int));
		for (int i = 0; i < count; i++) table.Rows.Add(i);
		return new PlainReaderCommand(() => new DataTableReader(table));
	}

	// Bytes per call: after a warm-up,
	// the smallest of three rounds of 1,000 calls
	// (the allocated-bytes counter of this thread is exact,
	// so only a stray JIT or GC adds noise).
	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 100; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 1000; i++) call();
			best = Math.Min(
				best,
				(GC.GetAllocatedBytesForCurrentThread() - before) / 1000);
		}

		return best;
	}

	// Every call here completes synchronously, so the thread never changes.
	static async Task<long> BytesPerCallAsync(Func<ValueTask> call)
	{
		for (int i = 0; i < 100; i++) await call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 1000; i++) await call();
			best = Math.Min(
				best,
				(GC.GetAllocatedBytesForCurrentThread() - before) / 1000);
		}

		return best;
	}

	// What a projection costs beyond the same terminal without it is a few
	// objects per terminal call: the same bytes for 100 rows as for 1,000,
	// so nothing is allocated per row.
	const int Slack = 16;
	const int FixedCostCeiling = 512;

	[Fact]
	public static void TheSyncProjection_AllocatesNothingPerRow()
	{
		long Extra(int rows)
		{
			PlainReaderCommand cmd = Ints(rows);
			long plain = BytesPerCall(
				() => ((IMappedQuery<int>)cmd.Map<int>()).ToList());
			long projected = BytesPerCall(
				() => cmd.Map<int>().Select(static i => i + 1).ToList());
			return projected - plain;
		}

		long few = Extra(100);
		long many = Extra(1000);

		Assert.True(
			Math.Abs(many - few) <= Slack,
			$"A projection costs {few} B per call over 100 rows and {many} B over 1000: it allocates per row.");
		Assert.True(
			few <= FixedCostCeiling,
			$"A projection costs {few} B per terminal call, more than the {FixedCostCeiling} B expected.");
	}

	[Fact]
	public static async Task TheAsyncProjection_AllocatesNothingPerRow()
	{
		async Task<long> Extra(int rows)
		{
			PlainReaderCommand cmd = Ints(rows);
			long plain = await BytesPerCallAsync(
				async () => await ((IMappedQuery<int>)cmd.Map<int>()).ToListAsync(Ct));
			long projected = await BytesPerCallAsync(
				async () => await cmd.Map<int>().Select(static i => i + 1).ToListAsync(Ct));
			return projected - plain;
		}

		long few = await Extra(100);
		long many = await Extra(1000);

		Assert.True(
			Math.Abs(many - few) <= Slack,
			$"A projection costs {few} B per call over 100 rows and {many} B over 1000: it allocates per row.");
		Assert.True(
			few <= FixedCostCeiling,
			$"A projection costs {few} B per terminal call, more than the {FixedCostCeiling} B expected.");
	}

	#endregion

	#region ConfigureAwait(false)

	sealed class CountingContext : SynchronizationContext
	{
		readonly ConcurrentQueue<string> _posts = new();

		public int Posts => _posts.Count;

		// What was posted: the awaiter's continuation names its state machine.
		public string What => string.Join(" ; ", _posts);

		public override void Post(SendOrPostCallback d, object? state)
		{
			_posts.Enqueue(Describe(state));
			ThreadPool.QueueUserWorkItem(_ => d(state));
		}

		public override void Send(SendOrPostCallback d, object? state)
		{
			_posts.Enqueue(Describe(state));
			d(state);
		}

		static string Describe(object? state)
			=> state switch
			{
				null => "null",
				Delegate del => $"{del.Target?.GetType()} {del.Method.Name}",
				ITuple tuple => string.Join(
					" | ",
					Enumerable.Range(0, tuple.Length).Select(i => Describe(tuple[i]))),
				_ => state.GetType().ToString(),
			};
	}

	// A source of two rows whose one awaited step
	// (the first row, or the disposal) completes only when the gate opens.
	sealed class GatedQuery(TaskCompletionSource gate, string gated)
		: IMappedQuery<int>
	{
		sealed class Rows(TaskCompletionSource gate, string gated)
			: IAsyncEnumerable<int>, IAsyncEnumerator<int>
		{
			int _row;

			public int Current => _row;

			public IAsyncEnumerator<int> GetAsyncEnumerator(
				CancellationToken cancellationToken = default)
				=> this;

			public ValueTask<bool> MoveNextAsync()
			{
				_row++;
				if (_row > 2) return new(false);
				return _row == 1 && gated == "move"
					? new(Opened(gate.Task))
					: new(true);
			}

			static async Task<bool> Opened(Task gate)
			{
				await gate.ConfigureAwait(false);
				return true;
			}

			public ValueTask DisposeAsync()
				=> gated == "dispose" ? new(gate.Task) : default;
		}

		public void Read(Action<IEnumerable<int>> handler)
			=> handler([1, 2]);

		public TResult Read<TResult>(Func<IEnumerable<int>, TResult> handler)
			=> handler([1, 2]);

		public ValueTask ReadAsync(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask> handler,
			CancellationToken cancellationToken)
			=> handler(new Rows(gate, gated), cancellationToken);

		public ValueTask<TResult> ReadAsync<TResult>(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask<TResult>> handler,
			CancellationToken cancellationToken)
			=> handler(new Rows(gate, gated), cancellationToken);
	}

	// Runs the call under a context that counts posts; the gate is opened
	// after the call has returned an incomplete task.
	static async Task<CountingContext> PostsDuring(
		string gated,
		bool project,
		Func<IMappedQuery<int>, Task> terminal)
	{
		TaskCompletionSource gate = new(
			TaskCreationOptions.RunContinuationsAsynchronously);
		CountingContext context = new();
		SynchronizationContext? prior = SynchronizationContext.Current;
		IMappedQuery<int> query = new GatedQuery(gate, gated);
		if (project) query = query.Select(i => i * 10);

		Task task;
		SynchronizationContext.SetSynchronizationContext(context);
		try
		{
			task = terminal(query);
		}
		finally
		{
			SynchronizationContext.SetSynchronizationContext(prior);
		}

		Assert.False(task.IsCompleted, "the gated step must be genuinely asynchronous");
		gate.TrySetResult();
		await task;
		return context;
	}

	[Theory]
	[InlineData("move", "ToListAsync")]
	[InlineData("dispose", "ToListAsync")]
	[InlineData("move", "FirstOrDefaultAsync")]
	[InlineData("dispose", "FirstOrDefaultAsync")]
	public static async Task TheProjection_NeverResumesOnTheCallersContext(
		string gated,
		string terminal)
	{
		Task Run(IMappedQuery<int> q)
			=> terminal == "ToListAsync"
				? q.ToListAsync(Ct).AsTask()
				: q.FirstOrDefaultAsync(Ct).AsTask();

		// The same call without a projection is the baseline: the projection
		// must add no post to it.
		CountingContext plain = await PostsDuring(gated, false, Run);
		CountingContext projected = await PostsDuring(gated, true, Run);

		TestContext.Current.TestOutputHelper?.WriteLine(
			$"{gated}/{terminal}: plain {plain.Posts} [{plain.What}]; projected {projected.Posts} [{projected.What}]");
		Assert.True(
			projected.Posts <= plain.Posts,
			$"The projection posted {projected.Posts} time(s) to the caller's context, the same call without it {plain.Posts}: {projected.What}");
	}

	#endregion
}
