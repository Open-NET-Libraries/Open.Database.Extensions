using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Tests;

// Stream(stallTimeout, ct) and StreamBuffered(stallTimeout, 0, ct): no channel and no producer task.
// The rows are read on the consumer's thread as it pulls, and not one row ahead of it.
[ExcludeFromCodeCoverage]
public static class DirectStreamTests
{
	sealed record Row(int Id, string Name);

	static readonly TimeSpan Long = TimeSpan.FromSeconds(30);
	static readonly TimeSpan Short = TimeSpan.FromMilliseconds(100);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static DbTally Tally(int rows, int failOnRead = 0, Action<int>? onRead = null)
		=> new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, rows).Select(i => new object?[] { i, "n" + i })],
			FailOnRead = failOnRead,
			OnRead = onRead,
		});

	static MappedQuery<Row> Query(DbTally tally)
		=> tally.Factory.Command("select").Map<Row>();

	static async Task WaitUntil(Func<bool> condition)
	{
		for (int i = 0; i < 500 && !condition(); i++)
			await Task.Delay(10, Ct);

		Assert.True(condition(), "The condition did not become true in time.");
	}

	// Stream, and StreamBuffered with no buffer: the same read.
	public static TheoryData<bool> Forms => [false, true];

	static IAsyncEnumerable<Row> Open(MappedQuery<Row> query, bool viaBuffered, TimeSpan stall, CancellationToken ct = default)
		=> viaBuffered ? query.StreamBuffered(stall, 0, ct) : query.Stream(stall, ct);

	[Theory]
	[MemberData(nameof(Forms))]
	public static async Task NoRowIsReadAhead_OfTheConsumer(bool viaBuffered)
	{
		DbTally tally = Tally(50);
		await using IAsyncEnumerator<Row> rows
			= Open(Query(tally), viaBuffered, Long).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		Assert.Equal(1, rows.Current.Id);

		// Nothing runs ahead: the read waits for the next pull, however long that takes.
		await Task.Delay(300, Ct);
		Assert.Equal(1, tally.Readers[0].Reads);

		Assert.True(await rows.MoveNextAsync());
		Assert.Equal(2, rows.Current.Id);
		Assert.Equal(2, tally.Readers[0].Reads);
	}

	[Fact]
	public static async Task AChannelOfOne_ReadsAhead_ButNoBufferDoesNot()
	{
		DbTally channelled = Tally(50);
		await using IAsyncEnumerator<Row> ahead
			= Query(channelled).StreamBuffered(Long, 1, Ct).GetAsyncEnumerator(Ct);
		Assert.True(await ahead.MoveNextAsync());
		await WaitUntil(() => channelled.Readers[0].Reads >= 2);

		DbTally direct = Tally(50);
		await using IAsyncEnumerator<Row> none
			= Query(direct).StreamBuffered(Long, 0, Ct).GetAsyncEnumerator(Ct);
		Assert.True(await none.MoveNextAsync());
		await Task.Delay(200, Ct);

		Assert.Equal(1, direct.Readers[0].Reads);
	}

	[Fact]
	public static async Task TheRows_AreReadOnTheConsumersThread()
	{
		const int count = 40;
		int[] readOn = new int[count + 2];
		DbTally tally = Tally(count, onRead: read => readOn[read] = Environment.CurrentManagedThreadId);

		int[] pulledOn = new int[count + 2];
		int index = 0;
		await foreach (Row row in Query(tally).Stream(Long, Ct))
		{
			index = row.Id;

			// The loop body makes no await: the next pull comes from this thread.
			pulledOn[index] = Environment.CurrentManagedThreadId;
		}

		Assert.Equal(count, index);

		// The read of row n + 1 ran where the consumer was after row n.
		for (int n = 1; n < count; n++)
			Assert.Equal(pulledOn[n], readOn[n + 1]);
	}

	[Fact]
	public static async Task AnAwaitingConsumer_IsNotBlockedByTheStreamsOwnState()
	{
		// A consumer that awaits something that hops threads every row still gets every row, in order.
		DbTally tally = Tally(200);
		List<int> ids = [];

		await foreach (Row row in Query(tally).Stream(Long, Ct))
		{
			ids.Add(row.Id);
			await Task.Yield();
		}

		Assert.Equal(Enumerable.Range(1, 200), ids);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
		Assert.Equal(1, tally.Connections[0].Closes);
	}

	[Fact]
	public static async Task AStalledConsumer_ExpiresTheRead_AndTheNextPullThrowsTimeout_NeverAnEnd()
	{
		DbTally tally = Tally(50);
		await using IAsyncEnumerator<Row> rows = Query(tally).Stream(Short, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);

		// The read stopped on its own: a reader that is closed is not read again.
		TimeoutException first = await Assert.ThrowsAsync<TimeoutException>(async () => await rows.MoveNextAsync());
		Assert.Contains(Short.ToString(), first.Message);

		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(1, tally.Connections[0].Closes);
	}

	[Fact]
	public static async Task TheWatchdog_StartsAfterTheFirstRow_AndStopsWhenTheStreamEnds()
	{
		DbTally tally = Tally(3);
		IAsyncEnumerable<Row> stream = Query(tally).Stream(Long, Ct);
		DirectStream<Row> direct = Assert.IsType<DirectStream<Row>>(stream);

		Assert.False(direct.HasTimer);

		await using (IAsyncEnumerator<Row> rows = stream.GetAsyncEnumerator(Ct))
		{
			Assert.False(direct.HasTimer);
			Assert.True(await rows.MoveNextAsync());
			Assert.True(direct.HasTimer);
			while (await rows.MoveNextAsync()) { }
			Assert.False(direct.HasTimer);
		}

		Assert.False(direct.HasTimer);
	}

	[Fact]
	public static async Task AnInfiniteTimeout_MakesNoTimer()
	{
		DbTally tally = Tally(3);
		IAsyncEnumerable<Row> stream = Query(tally).Stream(Timeout.InfiniteTimeSpan, Ct);
		DirectStream<Row> direct = Assert.IsType<DirectStream<Row>>(stream);

		await using IAsyncEnumerator<Row> rows = stream.GetAsyncEnumerator(Ct);
		Assert.True(await rows.MoveNextAsync());

		Assert.False(direct.HasTimer);
	}

	[Theory]
	[InlineData("break")]
	[InlineData("throw")]
	[InlineData("dispose")]
	[InlineData("expire")]
	public static async Task TheTimer_IsGone_OnEveryWayOut(string how)
	{
		DbTally tally = Tally(100);
		IAsyncEnumerable<Row> stream = Query(tally).Stream(how == "expire" ? Short : Long, Ct);
		DirectStream<Row> direct = Assert.IsType<DirectStream<Row>>(stream);

		switch (how)
		{
			case "break":
				await foreach (Row row in stream)
				{
					if (row.Id == 2) break;
				}

				break;

			case "throw":
				await Assert.ThrowsAsync<ArgumentException>(async () =>
				{
					await foreach (Row row in stream)
						throw new ArgumentException("consumer");
				});
				break;

			case "dispose":
			{
				IAsyncEnumerator<Row> rows = stream.GetAsyncEnumerator(Ct);
				Assert.True(await rows.MoveNextAsync());
				await rows.DisposeAsync();
				break;
			}

			default:
			{
				IAsyncEnumerator<Row> rows = stream.GetAsyncEnumerator(Ct);
				Assert.True(await rows.MoveNextAsync());
				await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);
				Assert.False(direct.HasTimer);
				await Assert.ThrowsAsync<TimeoutException>(async () => await rows.MoveNextAsync());
				await rows.DisposeAsync();
				break;
			}
		}

		Assert.False(direct.HasTimer);
		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	[Fact]
	public static async Task DisposeAsync_IsTolerant_OfAnAlreadyStoppedStream_AndOfASecondCall()
	{
		DbTally tally = Tally(50);
		IAsyncEnumerator<Row> rows = Query(tally).Stream(Short, Ct).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		await WaitUntil(() => tally.Connections[0].ClosedAfterOpen);

		await rows.DisposeAsync();
		await rows.DisposeAsync();

		Assert.Equal(1, tally.Readers[0].Disposes);
		Assert.Equal(1, tally.Connections[0].Closes);
	}

	// The consumer's pull and the watchdog's expiry decide on one word: whichever moves it first wins, the other does nothing.
	// Pulls timed around the timeout: each run ends in rows or in a timeout, and always in exactly one cleanup.
	[Fact]
	public static async Task ResumeRacingExpiry_IsOneOrTheOther_AndCleansUpExactlyOnce()
	{
		TimeSpan stall = TimeSpan.FromMilliseconds(30);
		int timeouts = 0;
		int completed = 0;

		for (int i = 0; i < 150; i++)
		{
			DbTally tally = Tally(4);
			IAsyncEnumerator<Row> rows = Query(tally).Stream(stall, Ct).GetAsyncEnumerator(Ct);

			try
			{
				while (await rows.MoveNextAsync())
				{
					// Around the timeout: sometimes just under it, sometimes just over.
					await Task.Delay(20 + (i % 4) * 7, Ct);
				}

				completed++;
			}
			catch (TimeoutException)
			{
				timeouts++;
			}
			finally
			{
				await rows.DisposeAsync();
			}

			Assert.Equal(1, tally.Readers[0].Disposes);
			Assert.Equal(1, tally.Connections[0].Closes);
		}

		// The runs split between the two; neither is ever lost or doubled.
		Assert.Equal(150, timeouts + completed);
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
	public static async Task ACancelBetweenPulls_WinsOverTheNextRow()
	{
		DbTally tally = Tally(50);
		using CancellationTokenSource cts = new();
		await using IAsyncEnumerator<Row> rows = Query(tally).Stream(Long, cts.Token).GetAsyncEnumerator(Ct);

		Assert.True(await rows.MoveNextAsync());
		await cts.CancelAsync();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await rows.MoveNextAsync());
	}

	[Fact]
	public static async Task WithAsyncReads_EveryRowIsStillDelivered_InOrder_AndTheConnectionCloses()
	{
		DbTally tally = Tally(300);
		List<int> ids = [];

		await foreach (Row row in tally.Factory.Command("select").Map<Row>().AsyncReads().Stream(Long, Ct))
		{
			ids.Add(row.Id);
			if (row.Id % 50 == 0) await Task.Yield();
		}

		Assert.Equal(Enumerable.Range(1, 300), ids);
		Assert.Equal(1, tally.Connections[0].Closes);
	}
	[Fact]
	public static async Task AFailureWhileReading_ReachesThePull_AfterTheRowsAlreadyDelivered()
	{
		DbTally tally = Tally(10, failOnRead: 3);
		List<int> ids = [];

		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
		{
			await foreach (Row row in Query(tally).Stream(Long, Ct))
				ids.Add(row.Id);
		});

		Assert.Equal("The provider failed while reading.", ex.Message);
		Assert.Equal([1, 2], ids);
		Assert.Equal(ConnectionState.Closed, tally.Connections[0].State);
	}

	// The stream adds nothing per row: each row costs what the map costs, so the cost of 900 more rows is a small multiple of 900 rows' own objects.
	[Fact]
	public static async Task AStream_AllocatesNothingPerRow_OfItsOwn()
	{
		static async Task<long> Measure(int rows)
		{
			DbTally tally = Tally(rows);
			MappedQuery<Row> query = Query(tally);

			long before = GC.GetAllocatedBytesForCurrentThread();
			await foreach (Row _ in query.Stream(Timeout.InfiniteTimeSpan, TestContext.Current.CancellationToken)) { }
			return GC.GetAllocatedBytesForCurrentThread() - before;
		}

		// Warm the type's caches first.
		await Measure(50);
		await Measure(50);

		long small = await Measure(100);
		long large = await Measure(1000);

		// A row costs its own object (a record of two fields, 32 bytes) and the fake's own work; a handoff object per row would add at least 56 bytes.
		double perRow = (large - small) / 900.0;
		Assert.True(perRow < 120, $"{perRow:F1} bytes per row.");
	}
}
