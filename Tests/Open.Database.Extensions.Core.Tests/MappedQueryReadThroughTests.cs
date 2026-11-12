using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A Map terminal returns the rows of the first result set and then reads through the later ones (NextResult, no Read), so a failure in a later statement is thrown, unwrapped.
// Not when the caller stopped reading early or something of the caller's threw (the original exception is never replaced), and not after .SingleResult().
[ExcludeFromCodeCoverage]
public static class MappedQueryReadThroughTests
{
	sealed record IdRow(int Id);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	const int LaterSets = 2; // so a full read-through is three calls of NextResult: true, true, false

	static FakeExecuteReader Command(int rows, out Func<MultiSetReader> reader, int? failAt = null)
	{
		MultiSetReader? last = null;
		reader = () => last ?? throw new InvalidOperationException("never executed");
		return new FakeExecuteReader(() => last = new MultiSetReader(Rows(rows), LaterSets, failAt));
	}

	static MappedQuery<int> Query(FakeExecuteReader command) => command.Map(r => r.GetInt32(0));

	// ---- every terminal ----

	public static TheoryData<string> Terminals
		=> ["ToList", "ToArray", "ToImmutableArray", "FirstOrDefault", "SingleOrDefault", "First", "Single", "ToDictionary", "ToDictionaryValues", "ToHashSet", "Read", "ReadResult"];

	static void RunSync(MappedQuery<int> query, string terminal)
	{
		switch (terminal)
		{
			case "ToList": query.ToList(); break;
			case "ToArray": query.ToArray(); break;
			case "ToImmutableArray": query.ToImmutableArray(); break;
			case "FirstOrDefault": query.FirstOrDefault(); break;
			case "SingleOrDefault": query.SingleOrDefault(); break;
			case "First": query.First(); break;
			case "Single": query.Single(); break;
			case "ToDictionary": query.ToDictionary(i => i); break;
			case "ToDictionaryValues": query.ToDictionary(i => i, i => i * 2); break;
			case "ToHashSet": query.ToHashSet(); break;
			case "Read": query.Read(items => { foreach (int _ in items) { } }); break;
			case "ReadResult": query.Read(items => items.Count()); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	static async Task RunAsync(MappedQuery<int> query, string terminal, CancellationToken token)
	{
		switch (terminal)
		{
			case "ToList": await query.ToListAsync(token); break;
			case "ToArray": await query.ToArrayAsync(token); break;
			case "ToImmutableArray": await query.ToImmutableArrayAsync(token); break;
			case "FirstOrDefault": await query.FirstOrDefaultAsync(token); break;
			case "SingleOrDefault": await query.SingleOrDefaultAsync(token); break;
			case "First": await query.FirstAsync(token); break;
			case "Single": await query.SingleAsync(token); break;
			case "ToDictionary": await query.ToDictionaryAsync(i => i, null, token); break;
			case "ToDictionaryValues": await query.ToDictionaryAsync(i => i, i => i * 2, null, token); break;
			case "ToHashSet": await query.ToHashSetAsync(null, token); break;
			case "Read": await query.ReadAsync(async (items, t) => { await foreach (int _ in items.WithCancellation(t)) { } }, token); break;
			case "ReadResult": await query.ReadAsync(async (items, t) => { int n = 0; await foreach (int _ in items.WithCancellation(t)) n++; return n; }, token); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	[Theory, MemberData(nameof(Terminals))]
	public static void EveryTerminal_ReadsThroughTheLaterSets_AndNeverReadsTheirRows(string terminal)
	{
		FakeExecuteReader command = Command(1, out Func<MultiSetReader> reader);

		RunSync(Query(command), terminal);

		Assert.Equal(LaterSets + 1, reader().NextResultCalls);
		Assert.Equal(0, reader().ReadsAfterNextResult);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static void EveryTerminal_ThrowsALaterFailureUnwrapped(string terminal)
	{
		FakeExecuteReader command = Command(1, out Func<MultiSetReader> reader, failAt: 2);

		Assert.Throws<LaterSetFailure>(() => RunSync(Query(command), terminal));

		Assert.Equal(2, reader().NextResultCalls);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task EveryTerminalAsync_ReadsThroughWithTheCallersToken_AndNeverReadsTheirRows(string terminal)
	{
		using var cts = new CancellationTokenSource();
		FakeExecuteReader command = Command(1, out Func<MultiSetReader> reader);

		await RunAsync(Query(command), terminal, cts.Token);

		Assert.Equal(LaterSets + 1, reader().NextResultCalls);
		Assert.Equal(LaterSets + 1, reader().AsyncCalls);
		Assert.Equal(cts.Token, reader().LastToken);
		Assert.Equal(0, reader().ReadsAfterNextResult);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task EveryTerminalAsync_ThrowsALaterFailureUnwrapped(string terminal)
	{
		FakeExecuteReader command = Command(1, out Func<MultiSetReader> reader, failAt: 1);

		await Assert.ThrowsAsync<LaterSetFailure>(() => RunAsync(Query(command), terminal, CancellationToken.None));

		Assert.Equal(1, reader().NextResultCalls);
	}

	// ---- shapes of the first set ----

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	public static void FirstOrDefault_AndSingleOrDefault_WithNoRowOrOne_ReadThrough(int rows)
	{
		int expected = rows == 0 ? 0 : 1;

		FakeExecuteReader first = Command(rows, out Func<MultiSetReader> firstReader);
		Assert.Equal(expected, Query(first).FirstOrDefault());
		Assert.Equal(LaterSets + 1, firstReader().NextResultCalls);

		FakeExecuteReader single = Command(rows, out Func<MultiSetReader> singleReader);
		Assert.Equal(expected, Query(single).SingleOrDefault());
		Assert.Equal(LaterSets + 1, singleReader().NextResultCalls);
	}

	[Fact]
	public static void FirstOrDefault_WithTwoRows_ReturnsTheFirst_AndReadsThrough()
	{
		FakeExecuteReader command = Command(2, out Func<MultiSetReader> reader);

		Assert.Equal(1, Query(command).FirstOrDefault());

		Assert.Equal(LaterSets + 1, reader().NextResultCalls);
	}

	[Fact]
	public static void SingleOrDefault_WithTwoRows_Throws_AndDoesNotReadThrough()
	{
		FakeExecuteReader command = Command(2, out Func<MultiSetReader> reader, failAt: 1);

		Assert.Throws<InvalidOperationException>(() => Query(command).SingleOrDefault());

		Assert.Equal(0, reader().NextResultCalls); // so the later failure did not replace it
	}

	[Fact]
	public static async Task SingleOrDefaultAsync_WithTwoRows_Throws_AndDoesNotReadThrough()
	{
		FakeExecuteReader command = Command(2, out Func<MultiSetReader> reader, failAt: 1);

		await Assert.ThrowsAsync<InvalidOperationException>(async () => await Query(command).SingleOrDefaultAsync(Ct));

		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static async Task SkipAndTake_ReadThrough_AndOnlyTheFirstSetsRowsAreReturned()
	{
		FakeExecuteReader skipTake = Command(5, out Func<MultiSetReader> skipTakeReader);
		Assert.Equal([2, 3], Query(skipTake).Skip(1).Take(2).ToList());
		Assert.Equal(LaterSets + 1, skipTakeReader().NextResultCalls);

		FakeExecuteReader skip = Command(5, out Func<MultiSetReader> skipReader);
		Assert.Equal([4, 5], await Query(skip).Skip(3).ToListAsync(Ct));
		Assert.Equal(LaterSets + 1, skipReader().NextResultCalls);

		FakeExecuteReader none = Command(5, out Func<MultiSetReader> noneReader);
		Assert.Empty(Query(none).Take(0).ToList()); // a take of 0 reads no row, but the later sets are still read through
		Assert.Equal(LaterSets + 1, noneReader().NextResultCalls);
		Assert.Equal(0, noneReader().ReadsAfterNextResult);
	}

	[Fact]
	public static void AGeneratedMapping_ReadsThroughToo()
	{
		FakeExecuteReader command = Command(2, out Func<MultiSetReader> reader);

		Assert.Equal([new IdRow(1), new IdRow(2)], command.Map<IdRow>().ToList());

		Assert.Equal(LaterSets + 1, reader().NextResultCalls);
	}

	[Fact]
	public static async Task APlainIDataReader_ReadsThroughWithTheSyncLoop_SyncAndAsync()
	{
		static IDataReader NewReader()
		{
			IDataReader reader = Substitute.For<IDataReader>();
			reader.Read().Returns(true, false);
			reader.NextResult().Returns(true, true, false);
			return reader;
		}

		IDataReader sync = NewReader();
		Assert.Equal([7], new PlainReaderCommand(() => sync).Map(_ => 7).ToList());
		sync.Received(3).NextResult();

		IDataReader asyncReader = NewReader();
		Assert.Equal([7], await new PlainReaderCommand(() => asyncReader).Map(_ => 7).ToListAsync(Ct));
		asyncReader.Received(3).NextResult();
	}

	// ---- the caller stopped, or something of the caller's threw: no read-through, and its exception is never replaced ----

	[Fact]
	public static void ABreakOutOfTheStream_DoesNotReadThrough()
	{
		FakeExecuteReader command = Command(3, out Func<MultiSetReader> reader, failAt: 1);

		Assert.Equal(1, Query(command).Read(items => items.First()));
		Query(command).Read(items => { foreach (int _ in items) break; });

		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static async Task ABreakOutOfTheAsyncStream_DoesNotReadThrough()
	{
		FakeExecuteReader command = Command(3, out Func<MultiSetReader> reader, failAt: 1);

		await Query(command).ReadAsync(async (items, t) => { await foreach (int _ in items.WithCancellation(t)) break; }, Ct);

		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static void AStreamRead_ToTheTakeLimit_ReadsThrough_AndSoDoesOneToTheLastRow()
	{
		FakeExecuteReader limited = Command(5, out Func<MultiSetReader> limitedReader);
		Assert.Equal(2, Query(limited).Take(2).Read(items => items.Count()));
		Assert.Equal(LaterSets + 1, limitedReader().NextResultCalls);

		FakeExecuteReader all = Command(5, out Func<MultiSetReader> allReader);
		Assert.Equal(5, Query(all).Read(items => items.Count()));
		Assert.Equal(LaterSets + 1, allReader().NextResultCalls);
	}

	[Fact]
	public static async Task AnAsyncStreamRead_ToTheTakeLimit_ReadsThrough()
	{
		FakeExecuteReader command = Command(5, out Func<MultiSetReader> reader);

		int count = await Query(command).Take(2).ReadAsync(async (items, t) => { int n = 0; await foreach (int _ in items.WithCancellation(t)) n++; return n; }, Ct);

		Assert.Equal(2, count);
		Assert.Equal(LaterSets + 1, reader().NextResultCalls);
	}

	[Fact]
	public static void AHandlerThatThrows_IsNotReplaced_AndNothingIsReadThrough()
	{
		FakeExecuteReader command = Command(3, out Func<MultiSetReader> reader, failAt: 1);

		FormatException ex = Assert.Throws<FormatException>(() => Query(command).Read(items =>
		{
			foreach (int _ in items) throw new FormatException("mine");
		}));

		Assert.Equal("mine", ex.Message);
		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static async Task AnAsyncHandlerThatThrows_IsNotReplaced_AndNothingIsReadThrough()
	{
		FakeExecuteReader command = Command(3, out Func<MultiSetReader> reader, failAt: 1);

		FormatException ex = await Assert.ThrowsAsync<FormatException>(async () => await Query(command).ReadAsync(async (items, t) =>
		{
			await foreach (int _ in items.WithCancellation(t)) throw new FormatException("mine");
		}, Ct));

		Assert.Equal("mine", ex.Message);
		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static async Task ASelectorThatThrows_IsNotReplaced_AndNothingIsReadThrough()
	{
		FakeExecuteReader command = Command(3, out Func<MultiSetReader> reader, failAt: 1);
		MappedQuery<int> query = command.Map<int>(_ => throw new FormatException("mine"));

		Assert.Equal("mine", Assert.Throws<FormatException>(() => query.ToList()).Message);
		Assert.Equal("mine", (await Assert.ThrowsAsync<FormatException>(async () => await query.ToListAsync(Ct))).Message);
		Assert.Equal(0, reader().NextResultCalls);
	}

	// ---- .SingleResult(): today's flags exactly, and the later sets are never visited ----

	[Fact]
	public static void WithoutSingleResult_TheFlagsDropSingleResultAndSingleRow()
	{
		FakeExecuteReader command = Command(2, out _);

		Query(command).ToList();
		Query(command).Take(1).FirstOrDefault();
		command.Map<IdRow>().ToList();

		Assert.Equal([CommandBehavior.Default, CommandBehavior.Default, CommandBehavior.SequentialAccess], command.Behaviors);
	}

	[Fact]
	public static void SingleResult_OnEachType_AndAnywhereInTheChain_RestoresTheFlags_AndNeverCallsNextResult()
	{
		FakeExecuteReader command = Command(3, out Func<MultiSetReader> reader, failAt: 1);
		const CommandBehavior Result = CommandBehavior.SingleResult;
		const CommandBehavior ResultAndRow = CommandBehavior.SingleResult | CommandBehavior.SingleRow;

		Assert.Equal([1, 2, 3], Query(command).SingleResult().ToList()); // on MappedQuery
		Assert.Equal([2, 3], Query(command).SingleResult().Skip(1).ToList()); // before the skip
		Assert.Equal([2, 3], Query(command).Skip(1).SingleResult().ToList()); // on MappedQueryWithSkip
		Assert.Equal([2], Query(command).Skip(1).Take(1).SingleResult().ToList()); // on MappedQueryWithTake
		Assert.Equal([1], Query(command).SingleResult().Take(1).ToList());
		Assert.Equal(1, Query(command).SingleResult().FirstOrDefault()); // nothing skipped: SingleRow
		Assert.Equal(2, Query(command).Skip(1).SingleResult().FirstOrDefault()); // a row is skipped first: no SingleRow
		Assert.Equal(1, Query(command).Take(2).SingleResult().FirstOrDefault());
		Assert.Equal(1, Query(command).SingleResult().Read(items => items.First()));

		Assert.Equal([Result, Result, Result, Result, Result, ResultAndRow, Result, ResultAndRow, Result], command.Behaviors);
		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static async Task SingleResult_AsyncTerminals_NeverCallNextResult_AndAGeneratedMappingKeepsSequentialAccess()
	{
		FakeExecuteReader command = Command(2, out Func<MultiSetReader> reader, failAt: 1);

		Assert.Equal([1, 2], await Query(command).SingleResult().ToListAsync(Ct));
		Assert.Equal([new IdRow(1), new IdRow(2)], await command.Map<IdRow>().SingleResult().ToListAsync(Ct));
		Assert.Equal(1, await Query(command).SingleResult().FirstOrDefaultAsync(Ct));
		await Query(command).SingleResult().ReadAsync(async (items, t) => { await foreach (int _ in items.WithCancellation(t)) { } }, Ct);

		Assert.Equal(
			[CommandBehavior.SingleResult, CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, CommandBehavior.SingleResult | CommandBehavior.SingleRow, CommandBehavior.SingleResult],
			command.Behaviors);
		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static void SingleResult_OnAPlainIDataReader_NeverCallsNextResult()
	{
		IDataReader reader = Substitute.For<IDataReader>();
		reader.Read().Returns(true, false);

		Assert.Equal([7], new PlainReaderCommand(() => reader).Map(_ => 7).SingleResult().ToList());

		reader.DidNotReceive().NextResult();
	}
}
