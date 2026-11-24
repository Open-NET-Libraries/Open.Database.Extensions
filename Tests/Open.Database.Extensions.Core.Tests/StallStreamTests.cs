using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Tests;

// Stream(stallTimeout, ct): the rows one at a time off the open reader,
// stopped when the consumer goes the timeout without a pull.
[ExcludeFromCodeCoverage]
public static class StallStreamTests
{
	sealed record Row(int Id, string Name);

	static readonly TimeSpan Long = TimeSpan.FromSeconds(30);
	static readonly TimeSpan Short = TimeSpan.FromMilliseconds(100);

	// A result larger than the stream's read-ahead: only then does the producer wait for room, so only then can a consumer stall it.
	const int Over = StallStream.DefaultBufferSize + 50;

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static DbTally Tally(int rows, int failOnRead = 0)
		=> new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, rows).Select(i => new object?[] { i, "n" + i })],
			FailOnRead = failOnRead,
		});

	// A query over a factory connection: the library opens it and closes it.
	static MappedQuery<Row> Query(DbTally tally)
		=> tally.Factory.Command("select").Map<Row>();

	static async Task<List<int>> Collect(IAsyncEnumerable<Row> rows)
	{
		List<int> ids = [];
		await foreach (Row row in rows)
			ids.Add(row.Id);

		return ids;
	}

	static async Task WaitUntil(Func<bool> condition)
	{
		for (int i = 0; i < 500 && !condition(); i++)
			await Task.Delay(10, TestContext.Current.CancellationToken);

		Assert.True(condition(), "The condition did not become true in time.");
	}

	public static TheoryData<bool> FiniteOrNot => [true, false];

	[Theory]
	[MemberData(nameof(FiniteOrNot))]
	public static async Task Stream_DeliversEveryRow_InOrder_ThenEnds(bool finite)
	{
		DbTally tally = Tally(25);

		List<int> ids = await Collect(Query(tally).Stream(finite ? Long : Timeout.InfiniteTimeSpan, Ct));

		Assert.Equal(Enumerable.Range(1, 25), ids);
		Assert.Single(tally.Readers);
		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task Stream_OfAnEmptyResult_Ends()
	{
		DbTally tally = Tally(0);

		Assert.Empty(await Collect(Query(tally).Stream(Long, Ct)));
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task Stream_KeepsSkipAndTake()
	{
		DbTally tally = Tally(10);

		List<int> ids = await Collect(
			Query(tally).Skip(2).Take(3).Stream(Long, Ct));

		Assert.Equal([3, 4, 5], ids);
	}

	[Fact]
	public static async Task Stream_IsReachableThroughTheInterface()
	{
		DbTally tally = Tally(4);
		IMappedQuery<Row> query = Query(tally);

		Assert.Equal([1, 2, 3, 4], await Collect(query.Stream(Long, Ct)));
	}

	[Fact]
	public static async Task Stream_IsLazy_AndTakenOnce()
	{
		DbTally tally = Tally(3);

		IAsyncEnumerable<Row> rows = Query(tally).Stream(Long, Ct);

		// Nothing was created: no connection, no command.
		Assert.Empty(tally.Connections);
		Assert.Empty(tally.Commands);

		await using IAsyncEnumerator<Row> first = rows.GetAsyncEnumerator(Ct);
		Assert.Throws<InvalidOperationException>(() => rows.GetAsyncEnumerator(Ct));
		Assert.Empty(tally.Connections);

		Assert.True(await first.MoveNextAsync());
		Assert.Single(tally.Connections);
	}

	[Theory]
	[InlineData(0)]
	[InlineData(-1000)]
	[InlineData(-2)]
	public static void ANonPositiveTimeout_IsRefusedAtTheCall(int milliseconds)
	{
		DbTally tally = Tally(1);
		MappedQuery<Row> query = Query(tally);

		Assert.Throws<ArgumentOutOfRangeException>(
			() => query.Stream(TimeSpan.FromMilliseconds(milliseconds), Ct));
	}

	[Fact]
	public static void ADefaultView_IsRefusedAtTheCall()
		=> Assert.Throws<InvalidOperationException>(
			() => default(MappedQuery<Row>).Stream(Long, Ct));

	[Fact]
	public static async Task ACanceledToken_StartsNothing()
	{
		DbTally tally = Tally(3);
		using CancellationTokenSource cts = new();
		cts.Cancel();

		IAsyncEnumerable<Row> rows = Query(tally).Stream(Long, cts.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Collect(rows));
		Assert.Empty(tally.Connections);
	}

	[Fact]
	public static async Task AStalledConsumer_StopsTheRead_AndTheNextPullThrowsTimeout()
	{
		DbTally tally = Tally(50);
		await using IAsyncEnumerator<Row> rows
			= Query(tally).Stream(Short, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		Assert.Equal(1, rows.Current.Id);

		// No pull for much longer than the timeout.
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);

		TimeoutException ex = await Assert.ThrowsAsync<TimeoutException>(
			async () => await rows.MoveNextAsync());
		Assert.Contains(Short.ToString(), ex.Message);

		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(1, tally.Connections[0].Closes);
	}

	[Fact]
	public static async Task AMovingConsumer_DoesNotTripIt()
	{
		DbTally tally = Tally(12);
		List<int> ids = [];

		// Each pull is well inside the timeout; the whole read is several times longer than it.
		await foreach (Row row in Query(tally).Stream(TimeSpan.FromMilliseconds(1000), Ct))
		{
			ids.Add(row.Id);
			await Task.Delay(150, TestContext.Current.CancellationToken);
		}

		Assert.Equal(Enumerable.Range(1, 12), ids);
	}

	[Fact]
	public static async Task AnAbandonedEnumeration_ClosesItselfAfterTheTimeout()
	{
		DbTally tally = Tally(50);
		IAsyncEnumerator<Row> rows
			= Query(tally).Stream(Short, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());

		// Never pulled again, never disposed.
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);
		Assert.Equal(1, tally.Readers[0].Disposes);

		await rows.DisposeAsync();
	}

	[Fact]
	public static async Task WithAnInfiniteTimeout_AnAbandonedEnumeration_StaysOpenUntilItIsDisposed()
	{
		DbTally tally = Tally(50);
		IAsyncEnumerator<Row> rows
			= Query(tally).Stream(Timeout.InfiniteTimeSpan, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		await Task.Delay(300, TestContext.Current.CancellationToken);

		Assert.Equal(ConnectionState.Open, tally.Connections[0].State);
		Assert.Equal(0, tally.Readers[0].Disposes);

		await rows.DisposeAsync();

		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
		Assert.Equal(1, tally.Readers[0].Disposes);
	}

	[Theory]
	[MemberData(nameof(FiniteOrNot))]
	public static async Task BreakingOutOfTheLoop_DisposesTheEnumeration_AndClosesTheConnection(bool finite)
	{
		DbTally tally = Tally(100);

		await foreach (Row row in Query(tally).Stream(finite ? Long : Timeout.InfiniteTimeSpan, Ct))
		{
			if (row.Id == 2) break;
		}

		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
		Assert.Equal(1, tally.Connections[0].Closes);
	}

	[Fact]
	public static async Task ACallerConnection_StaysOpen_WhenTheStreamStops()
	{
		DbTally tally = Tally(100);
		TrackedConnection connection = tally.NewOpenConnection();

		await foreach (Row row in connection.Command("select").Map<Row>().Stream(Long, Ct))
		{
			if (row.Id == 2) break;
		}

		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(ConnectionState.Open, connection.State);
		Assert.Equal(0, connection.Closes);
	}

	[Fact]
	public static async Task ACallerCancel_DuringTheWait_IsACancel_NotATimeout()
	{
		DbTally tally = Tally(50);
		using CancellationTokenSource cts = new();
		await using IAsyncEnumerator<Row> rows = Query(tally)
			.Stream(TimeSpan.FromSeconds(30), cts.Token)
			.GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		await cts.CancelAsync();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				// The consumer was not pulling: the next pull reports the cancel.
				while (await rows.MoveNextAsync()) { }
			});
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);
	}

	[Theory]
	[MemberData(nameof(FiniteOrNot))]
	public static async Task AFailureWhileReading_ReachesThePull_AfterTheRowsAlreadyDelivered(bool finite)
	{
		DbTally tally = Tally(10, failOnRead: 3);
		List<int> ids = [];

		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
			async () =>
			{
				await foreach (Row row in Query(tally).Stream(finite ? Long : Timeout.InfiniteTimeSpan, Ct))
					ids.Add(row.Id);
			});

		Assert.Equal("The provider failed while reading.", ex.Message);
		Assert.Equal([1, 2], ids);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task AnExceptionInTheConsumersLoop_ClosesTheConnection()
	{
		DbTally tally = Tally(100);

		await Assert.ThrowsAsync<ArgumentException>(
			async () =>
			{
				await foreach (Row row in Query(tally).Stream(Long, Ct))
					throw new ArgumentException("consumer");
			});

		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task ExpiryRacingDisposeAndCancel_CleansUpExactlyOnce()
	{
		for (int i = 0; i < 200; i++)
		{
			DbTally tally = Tally(20);
			using CancellationTokenSource cts = new();
			IAsyncEnumerator<Row> rows = Query(tally)
				.Stream(TimeSpan.FromMilliseconds(1), cts.Token)
				.GetAsyncEnumerator(Ct);

			try
			{
				Assert.True(await rows.MoveNextAsync());
				if (i % 2 == 0) cts.Cancel();
				await Task.Delay(i % 3, Ct);
				while (await rows.MoveNextAsync()) { }
			}
			catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
			{
			}
			finally
			{
				await rows.DisposeAsync();
			}

			Assert.Equal(1, tally.Readers[0].Disposes);
			Assert.Equal(1, tally.Connections[0].Closes);
		}
	}

	[Fact]
	public static async Task TheStallSignal_IsCanceledAtExpiry_AndOnlyThen()
	{
		DbTally tally = Tally(50);
		IAsyncEnumerable<Row> stream = Query(tally).Stream(Short, Ct);
		IStallSignal signal = Assert.IsType<IStallSignal>(stream, exactMatch: false);

		await using IAsyncEnumerator<Row> rows = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);
		Assert.True(await rows.MoveNextAsync());
		Assert.False(signal.Expired.IsCancellationRequested);

		await WaitUntil(() => signal.Expired.IsCancellationRequested);

		TimeoutException thrown = await Assert.ThrowsAsync<TimeoutException>(
			async () => await rows.MoveNextAsync());
		Assert.Equal(signal.ToException().Message, thrown.Message);
	}

	[Fact]
	public static async Task TheStallSignal_NeverFires_WhenTheConsumerKeepsUp()
	{
		DbTally tally = Tally(5);
		IStallSignal signal = Assert.IsType<IStallSignal>(Query(tally).Stream(Long, Ct), exactMatch: false);
		var stream = (IAsyncEnumerable<Row>)signal;

		Assert.Equal(5, (await Collect(stream)).Count);
		Assert.False(signal.Expired.IsCancellationRequested);
	}

	[Theory]
	[InlineData(-1)]
	[InlineData(3650)]
	public static void AnInfiniteOrVeryLongTimeout_HasNoSignalToWaitFor(int days)
	{
		DbTally tally = Tally(1);
		TimeSpan timeout = days < 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromDays(days);

		IStallSignal signal = Assert.IsType<IStallSignal>(Query(tally).Stream(timeout, Ct), exactMatch: false);

		Assert.False(signal.Expired.CanBeCanceled);
	}

	// ---- the read-ahead: up to the buffer size in rows, then a wait ----

	[Fact]
	public static void TheDefaultBufferSize_Is1024Rows() => Assert.Equal(1024, StallStream.DefaultBufferSize);

	[Fact]
	public static async Task AResultThatFits_IsReadWhole_AndTheConnectionIsReleased_BeforeTheConsumerPullsTheRest()
	{
		DbTally tally = Tally(25);
		await using IAsyncEnumerator<Row> rows
			= Query(tally).StreamBuffered(Long, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());

		// One pull; the producer reads the other rows ahead, finishes, and closes the connection.
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);

		// The rows already read are still delivered, in order.
		List<int> ids = [rows.Current.Id];
		while (await rows.MoveNextAsync()) ids.Add(rows.Current.Id);

		Assert.Equal(Enumerable.Range(1, 25), ids);
	}

	[Fact]
	public static async Task AResultLargerThanTheBuffer_IsReadAhead_ByTheBufferSize_ThenTheProducerWaits()
	{
		int reads = 0;
		DbTally tally = new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, Over).Select(i => new object?[] { i, "n" + i })],
			OnRead = _ => Interlocked.Increment(ref reads),
		});

		await using IAsyncEnumerator<Row> rows
			= Query(tally).StreamBuffered(Long, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());

		// The producer fills the buffer and stops: no more than the buffer size ahead of the one pulled (one more is read and held waiting for room).
		await WaitUntil(() => Volatile.Read(ref reads) >= StallStream.DefaultBufferSize);
		await Task.Delay(200, TestContext.Current.CancellationToken);
		int ahead = Volatile.Read(ref reads);

		// One row pulled, StallStream.DefaultBufferSize waiting in the channel, one more read and held waiting for room (a freed slot lets one more through): N + 1 or N + 2.
		Assert.InRange(ahead, StallStream.DefaultBufferSize + 1, StallStream.DefaultBufferSize + 2);
		Assert.Equal(ConnectionState.Open, tally.Connections[0].State);
	}

	// ---- the buffer size is the caller's to set ----

	[Theory]
	[InlineData(-1)]
	[InlineData(int.MinValue)]
	public static void ANegativeBufferSize_IsRefusedAtTheCall(int bufferSize)
	{
		DbTally tally = Tally(3);

		ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
			() => Query(tally).StreamBuffered(Long, bufferSize, Ct));

		Assert.Equal("bufferSize", ex.ParamName);
	}

	[Fact]
	public static async Task ABufferSizeOfZero_IsStream_OneRowAtATime_AndAStalledConsumerStopsTheRead()
	{
		// 0 asks for no buffer: it routes to Stream, so the read stays at most a couple of rows ahead (one pulled, one waiting in the channel, one held for room).
		int reads = 0;
		DbTally tally = new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, 50).Select(i => new object?[] { i, "n" + i })],
			OnRead = _ => Interlocked.Increment(ref reads),
		});

		await using IAsyncEnumerator<Row> rows
			= Query(tally).StreamBuffered(Short, 0, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);

		Assert.InRange(Volatile.Read(ref reads), 2, 3);
		await Assert.ThrowsAsync<TimeoutException>(async () => await rows.MoveNextAsync());
	}

	[Theory]
	[InlineData(0)]
	[InlineData(-1000)]
	public static void StreamBuffered_ANonPositiveStallTimeout_IsRefusedAtTheCall(int milliseconds)
	{
		DbTally tally = Tally(1);
		MappedQuery<Row> query = Query(tally);

		Assert.Throws<ArgumentOutOfRangeException>(() => query.StreamBuffered(TimeSpan.FromMilliseconds(milliseconds), Ct));
		Assert.Throws<ArgumentOutOfRangeException>(() => query.StreamBuffered(TimeSpan.FromMilliseconds(milliseconds), 8, Ct));
	}

	[Fact]
	public static async Task StreamBuffered_OnASkippedAndTakenQuery_DeliversThoseRowsInOrder()
	{
		Assert.Equal([3, 4, 5], await Collect(Query(Tally(10)).Skip(2).Take(3).StreamBuffered(Long, Ct)));
		Assert.Equal([1, 2], await Collect(Query(Tally(10)).Take(2).StreamBuffered(Long, 4, Ct)));
		Assert.Equal([4, 5, 6, 7, 8, 9, 10], await Collect(Query(Tally(10)).Skip(3).StreamBuffered(Long, Ct)));
	}

	[Fact]
	public static async Task StreamBuffered_AnExceptionInTheConsumersLoop_ClosesTheConnection()
	{
		DbTally tally = Tally(Over);

		await Assert.ThrowsAsync<FormatException>(async () =>
		{
			await foreach (Row _ in Query(tally).StreamBuffered(Long, Ct)) throw new FormatException("mine");
		});

		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(1, tally.Connections[0].Closes);
	}

	[Fact]
	public static async Task ASmallerBufferSize_StopsTheReadAheadThere_AndAStalledConsumerStopsThatRead()
	{
		const int size = 16;
		int reads = 0;
		DbTally tally = new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, 200).Select(i => new object?[] { i, "n" + i })],
			OnRead = _ => Interlocked.Increment(ref reads),
		});

		await using IAsyncEnumerator<Row> rows
			= Query(tally).StreamBuffered(Short, size, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());

		// The buffer fills at the chosen size, and the producer waits there; with no pull the wait ends the read.
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);
		Assert.InRange(Volatile.Read(ref reads), size, size + 3);

		await Assert.ThrowsAsync<TimeoutException>(async () => await rows.MoveNextAsync());
	}

	[Fact]
	public static async Task ALargerBufferSize_ReadsALargeResultWhole_AndReleasesTheConnectionEarly()
	{
		DbTally tally = Tally(3000);
		await using IAsyncEnumerator<Row> rows
			= Query(tally).StreamBuffered(Long, 4000, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);

		List<int> ids = [rows.Current.Id];
		while (await rows.MoveNextAsync()) ids.Add(rows.Current.Id);

		Assert.Equal(Enumerable.Range(1, 3000), ids);
	}

	[Fact]
	public static async Task AStalledConsumer_OfAResultLargerThanTheDefaultBuffer_StopsTheRead_AndTheNextPullThrowsTimeout()
	{
		DbTally tally = Tally(Over);
		await using IAsyncEnumerator<Row> rows
			= Query(tally).StreamBuffered(Short, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());

		// The buffer fills and the producer waits; no pull for much longer than the timeout.
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);

		TimeoutException ex = await Assert.ThrowsAsync<TimeoutException>(
			async () => await rows.MoveNextAsync());
		Assert.Contains(Short.ToString(), ex.Message);
	}

	// ---- every streaming terminal is lazy: calling it does nothing until the first MoveNextAsync ----

	public static TheoryData<string> StreamingTerminals
		=> ["Stream", "StreamBuffered", "StreamBufferedSized", "ToPooledBufferStream", "ToPooledStream"];

	[Theory]
	[MemberData(nameof(StreamingTerminals))]
	public static async Task EveryStreamingTerminal_IsLazy_NothingStartsBeforeTheFirstMoveNext(string terminal)
	{
		DbTally tally = Tally(3);
		MappedQuery<Row> query = Query(tally);

		switch (terminal)
		{
			case "Stream": await AssertLazy(tally, query.Stream(Long, Ct)); break;
			case "StreamBuffered": await AssertLazy(tally, query.StreamBuffered(Long, Ct)); break;
			case "StreamBufferedSized": await AssertLazy(tally, query.StreamBuffered(Long, 8, Ct)); break;
			case "ToPooledBufferStream": await AssertLazy(tally, query.ToPooledBufferStream(Ct)); break;
			case "ToPooledStream": await AssertLazy(tally, query.ToPooledStream(Ct)); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	static async Task AssertLazy<T>(DbTally tally, IAsyncEnumerable<T> sequence)
	{
		// Called, not iterated: no connection opened, no command created or executed, no producer started.
		Assert.Empty(tally.Connections);
		Assert.Empty(tally.Commands);

		await using IAsyncEnumerator<T> enumerator = sequence.GetAsyncEnumerator(Ct);

		// Asking for the enumerator starts nothing either.
		Assert.Empty(tally.Connections);
		Assert.Empty(tally.Commands);

		// The first pull starts the read.
		Assert.True(await enumerator.MoveNextAsync());
		Assert.Single(tally.Connections);
		Assert.Single(tally.Commands);
	}
}
