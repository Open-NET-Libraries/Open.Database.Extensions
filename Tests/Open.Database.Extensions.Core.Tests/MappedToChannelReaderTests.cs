using static Open.Database.Extensions.Tests.ChannelRig;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ToChannelReader on the four command-level typed views
// (and, where the behaviour is shared, ToChannel and
// ToChannel(out ...), as the "form" of each case).
// A feed makes its own channel, so it always completes it:
// the end, a failure and a cancel all reach the reader.
// Nothing here waits without a limit: every wait is bounded.
[ExcludeFromCodeCoverage]
public static class MappedToChannelReaderTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(300);

	public static TheoryData<string> Views => ViewData;
	public static TheoryData<string, string> ViewsAndForms => ViewAndFormData;

	// ---- every rung of every view -------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task EveryRung_DeliversTheRowsInOrder_AndCompletes(
		string view)
	{
		ChannelOptions bounded = new BoundedChannelOptions(2);
		var unbounded = new UnboundedChannelOptions();
		var rig = Create(view, () => new ChannelTestReader(4));
		var token = Ct;

		var readers = new ChannelReader<ChannelRow>[]
		{
			rig.ToReader(null, false, token),
			rig.ToReader(unbounded, false, token),
			rig.ToReader(bounded, false, token),
			rig.ToReader(null, true, token),
			rig.ToReader(bounded, true, token),
		};

		foreach (var reader in readers)
		{
			var (rows, error) = await Collect(reader);
			Assert.Null(error);
			Assert.Equal(Ids(4), rows);
			await reader.Completion.Within();
		}

		Assert.Equal(readers.Length, rig.Executes);
	}

	// ---- 1: a bounded, full channel waits for room --------------------

	[Theory, MemberData(nameof(ViewsAndForms))]
	public static async Task ABoundedFullChannel_WaitsForRoom_AndDropsNothing(
		string view, string form)
	{
		var rig = Create(view, () => new ChannelTestReader(6));
		var reader = rig.Open(form, new BoundedChannelOptions(1), true, Ct);

		// One row is queued and the second is in hand: the read waits.
		await Until(() => rig.Reader is { RowsRead: >= 2 }, "two rows read");
		await Task.Delay(Quiet, Ct);
		Assert.Equal(2, rig.Reader!.RowsRead);

		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Equal(Ids(6), rows);
	}

	// ---- 2: cancel ------------------------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task Cancel_StopsTheFeed_AndEndsTheChannel(
		string view)
	{
		using var cts = new CancellationTokenSource();
		var rig = Create(view, () => new ChannelTestReader(10));
		var reader = rig.ToReader(
			new BoundedChannelOptions(1),
			true,
			cts.Token);
		await Until(() => rig.Reader is { RowsRead: >= 2 }, "two rows read");

		cts.Cancel();
		var (rows, error) = await Collect(reader);

		Assert.IsAssignableFrom<OperationCanceledException>(error);
		Assert.True(rows.Count < 10);
		await Until(
			() => reader.Completion.IsCompleted,
			"the channel completes");
		Assert.True(reader.Completion.IsCanceled);
		Assert.Equal(1, rig.Reader!.Disposes);
		if (rig.Connection is { } connection)
			Assert.Equal(["open", "close"], connection.Log);
	}

	[Theory]
	[InlineData("Query", false)]
	[InlineData("Query", true)]
	[InlineData("Skip", false)]
	[InlineData("Take", true)]
	[InlineData("Command", false)]
	[InlineData("Command", true)]
	public static async Task PreCancelled_NoThrow_NeverExecutes(
		string view, bool startImmediately)
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		var rig = Create(view, () => new ChannelTestReader(3));

		var reader = rig.ToReader(null, startImmediately, cts.Token);
		var (rows, error) = await Collect(reader);

		Assert.IsAssignableFrom<OperationCanceledException>(error);
		Assert.Empty(rows);
		Assert.Equal(0, rig.Executes);
		Assert.True(rig.Connection is null || rig.Connection.OpenCalls == 0);
	}

	// ---- 3 and 6: a failure completes the channel with it -------------

	[Theory, MemberData(nameof(ViewsAndForms))]
	public static async Task LaterSetFailure_FaultsAfterTheRows(
		string view, string form)
	{
		var rig = Create(
			view,
			() => new ChannelTestReader(3, laterSets: 2, failAtNextResult: 1));
		var reader = rig.Open(form, null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Equal(Ids(3), rows);
		Assert.IsType<LaterSetFailure>(error);
		await Assert.ThrowsAsync<LaterSetFailure>(
			() => reader.Completion.Within());
		Assert.Equal(1, rig.Reader!.NextResultCalls);
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task RowFailure_FaultsAfterTheEarlierRows(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(5, failAtRow: 3));
		var reader = rig.ToReader(null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Equal([1, 2], rows);
		Assert.IsType<InvalidOperationException>(error);
		Assert.Equal(1, rig.Reader!.Disposes);
	}

	// ---- 4: deferred, never read, never executes ----------------------

	[Theory, MemberData(nameof(ViewsAndForms))]
	public static async Task DeferredNeverRead_NeverExecutes(
		string view, string form)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var reader = rig.Open(form, null, false, Ct);

		await Task.Delay(Quiet, Ct);

		Assert.Equal(0, rig.Executes);
		Assert.True(rig.Connection is null || rig.Connection.OpenCalls == 0);
		Assert.False(reader.Completion.IsCompleted);
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task CompletionAndCount_DoNotStartTheFeed(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var reader = rig.ToReader(null, false, Ct);

		_ = reader.Completion;
		_ = reader.CanCount;
		_ = reader.CanPeek;
		if (reader.CanCount) Assert.Equal(0, reader.Count);

		await Task.Delay(Quiet, Ct);
		Assert.Equal(0, rig.Executes);
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task EveryKindOfFirstRead_StartsTheFeed(string view)
	{
		string[] kinds =
			["TryRead", "TryPeek", "WaitToRead", "Read", "ReadAll"];
		foreach (string kind in kinds)
		{
			var rig = Create(view, () => new ChannelTestReader(2));
			var reader = rig.ToReader(null, false, Ct);

			switch (kind)
			{
				case "TryRead": reader.TryRead(out _); break;
				case "TryPeek": reader.TryPeek(out _); break;
				case "WaitToRead":
					await reader.WaitToReadAsync(Ct).AsTask().Within();
					break;
				case "Read":
					_ = await reader.ReadAsync(Ct).AsTask().Within();
					break;
				default: await Collect(reader); break;
			}

			await Until(() => rig.Executes == 1, kind + " starts the feed");
		}
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task ProducerStartsOnce_UnderConcurrentReads(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(50));
		var reader = rig.ToReader(null, false, Ct);

		var readers = Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
		{
			if (i % 2 == 0) reader.TryRead(out _);
			else await reader.WaitToReadAsync(Ct);
		}, Ct));
		await Task.WhenAll(readers).Within();
		await Collect(reader);

		Assert.Equal(1, rig.Executes);
	}

	// ---- 5: the connection --------------------------------------------

	[Theory]
	[InlineData("Reader")]
	[InlineData("Channel")]
	[InlineData("Counted")]
	public static async Task AConnectionTheFeedOpened_IsClosed_WhenItEnds(
		string form)
	{
		var rig = Create("Command", () => new ChannelTestReader(3));
		var reader = rig.Open(form, null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Equal(Ids(3), rows);
		Assert.Equal(["open", "close"], rig.Connection!.Log);
		Assert.Equal(ConnectionState.Closed, rig.Connection.State);
		Assert.Equal(1, rig.Reader!.Disposes);
	}

	[Theory]
	[InlineData("Reader")]
	[InlineData("Channel")]
	[InlineData("Counted")]
	public static async Task AnOpenConnection_IsLeftOpen_ByTheFeed(string form)
	{
		var rig = Create(
			"Command",
			() => new ChannelTestReader(3),
			state: ConnectionState.Open);
		var reader = rig.Open(form, null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Equal(Ids(3), rows);
		Assert.Empty(rig.Connection!.Log);
		Assert.Equal(ConnectionState.Open, rig.Connection.State);
	}

	[Fact]
	public static async Task OpenCallerConnection_FeedRunsAtFirstRead()
	{
		var rig = Create(
			"Command",
			() => new ChannelTestReader(3),
			state: ConnectionState.Open);
		var reader = rig.ToReader(null, false, Ct);

		await Task.Delay(Quiet, Ct);
		Assert.Equal(0, rig.Executes);

		var (rows, _) = await Collect(reader);
		Assert.Equal(Ids(3), rows);
		Assert.Equal(1, rig.Executes);
	}

	// ---- 6: read through ----------------------------------------------

	[Theory, MemberData(nameof(ViewsAndForms))]
	public static async Task ReadsThroughTheLaterSets(
		string view, string form)
	{
		var rig = Create(view, () => new ChannelTestReader(2, laterSets: 2));
		var reader = rig.Open(form, null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Equal(Ids(2), rows);
		Assert.Equal(3, rig.Reader!.NextResultCalls);
		Assert.Equal(3, rig.Reader.AsyncNextResults);
	}

	// ---- 7: Read by default, ReadAsync with AsyncReads() --------------

	[Theory, MemberData(nameof(ViewsAndForms))]
	public static async Task Rows_AreReadWithRead_ByDefault(
		string view, string form)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var reader = rig.Open(form, null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Equal(Ids(3), rows);
		Assert.Equal(4, rig.Reader!.SyncReads);
		Assert.Equal(0, rig.Reader.AsyncReads);
	}

	[Theory, MemberData(nameof(ViewsAndForms))]
	public static async Task Rows_AreReadWithReadAsync_WithAsyncReads(
		string view, string form)
	{
		var rig = Create(
			view,
			() => new ChannelTestReader(3),
			asyncReads: true);
		var reader = rig.Open(form, null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Equal(Ids(3), rows);
		Assert.Equal(4, rig.Reader!.AsyncReads);
		Assert.Equal(0, rig.Reader.SyncReads);
	}

	// ---- 8: always completes ------------------------------------------

	[Theory, MemberData(nameof(ViewsAndForms))]
	public static async Task AFeedAlwaysCompletes_EvenWithNoRows(
		string view, string form)
	{
		var rig = Create(view, () => new ChannelTestReader(0, laterSets: 1));
		var reader = rig.Open(form, null, false, Ct);

		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Empty(rows);
		await reader.Completion.Within();
	}

	[Fact]
	public static async Task SkipAndTake_ApplyToTheFeed_AndItCompletes()
	{
		static FakeExecuteReader Command(int rows)
			=> new(() => new ChannelTestReader(rows));

		var skipped = Command(5).Map<ChannelRow>().Skip(3).ToChannelReader(Ct);
		Assert.Equal([4, 5], (await Collect(skipped)).Rows);

		var taken = Command(5).Map<ChannelRow>().Take(2).ToChannelReader(Ct);
		Assert.Equal([1, 2], (await Collect(taken)).Rows);

		var both = Command(5)
			.Map<ChannelRow>()
			.Skip(1)
			.Take(3)
			.ToChannelReader(Ct);
		Assert.Equal([2, 3, 4], (await Collect(both)).Rows);

		var past = Command(2).Map<ChannelRow>().Skip(10).ToChannelReader(Ct);
		var (rows, error) = await Collect(past);
		Assert.Null(error);
		Assert.Empty(rows);

		var none = Command(2).Map<ChannelRow>().Take(0).ToChannelReader(Ct);
		(rows, error) = await Collect(none);
		Assert.Null(error);
		Assert.Empty(rows);
	}

	// ---- 10: a Drop* FullMode is accepted and drops as it defines -----

	[Theory]
	[InlineData(BoundedChannelFullMode.DropWrite, new[] { 1, 2 })]
	[InlineData(BoundedChannelFullMode.DropNewest, new[] { 1, 10 })]
	[InlineData(BoundedChannelFullMode.DropOldest, new[] { 9, 10 })]
	public static async Task DropMode_Drops_AndStillCompletes(
		BoundedChannelFullMode mode, int[] expected)
	{
		var rig = Create("Query", () => new ChannelTestReader(10));
		var options = new BoundedChannelOptions(2) { FullMode = mode };
		var reader = rig.ToReader(options, true, Ct);

		// The feed never waits in a Drop mode: it ends before anything reads.
		await Until(() => rig.Reader is { Disposes: 1 }, "the feed ends");
		var (rows, error) = await Collect(reader);

		Assert.Null(error);
		Assert.Equal(expected, rows);
	}

	// ---- options ------------------------------------------------------

	sealed class OtherOptions : ChannelOptions;

	[Theory, MemberData(nameof(Views))]
	public static void AnUnknownKindOfOptions_ThrowsAtTheCall_NotAtTheFirstRead(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(1));

		Assert.Throws<ArgumentException>(
			"options", () => rig.ToReader(new OtherOptions(), false, Ct));
		Assert.Throws<ArgumentException>(
			"options", () => rig.ToReader(new OtherOptions(), true, Ct));
		Assert.Throws<ArgumentException>(
			"options", () => rig.ToChannel(new OtherOptions(), false, Ct));
		Assert.Throws<ArgumentException>(
			"options",
			() => rig.ToCounted(out _, new OtherOptions(), false, Ct));
		Assert.Equal(0, rig.Executes);
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task StandardOptions_PassThrough(string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var options = new BoundedChannelOptions(8)
		{
			SingleReader = true,
			SingleWriter = true,
			AllowSynchronousContinuations = true,
		};

		var (rows, error) = await Collect(rig.ToReader(options, false, Ct));

		Assert.Null(error);
		Assert.Equal(Ids(3), rows);
	}

	// ---- a default struct ---------------------------------------------

	[Fact]
	public static void ADefaultView_SaysItWasNotCreatedByMap_AtTheCall()
	{
		static void Says(Action call)
			=> Assert.Contains(
				"not created by Map<T>()",
				Assert.Throws<InvalidOperationException>(call).Message);

		Says(() => default(MappedQuery<ChannelRow>).ToChannelReader());
		Says(() => default(MappedQueryWithSkip<ChannelRow>).ToChannelReader());
		Says(() => default(MappedQueryWithTake<ChannelRow>).ToChannelReader());
		Says(() => default(MappedCommand<ChannelRow>).ToChannelReader());
		Says(() => default(MappedQuery<ChannelRow>).ToChannel());
		Says(() => default(MappedQuery<ChannelRow>).ToChannel(out _));
	}

	// ---- the thread: neither start runs the query on the caller ----------

	[Theory, MemberData(nameof(Views))]
	public static async Task StartImmediately_ReturnsBeforeAllRowsRead(
		string view)
	{
		var gated = new ChannelTestReader(5);
		gated.Hold(2);
		var rig = Create(view, () => gated);
		try
		{
			var call = Task.Run(() => rig.ToReader(null, true, Ct), Ct);

			// A start on the caller's thread would block here,
			// in the gated read.
			Assert.Same(
				call,
				await Task.WhenAny(
					call,
					Task.Delay(TimeSpan.FromSeconds(10), Ct)));
			Assert.True(gated.RowsRead < 5);

			gated.Release();
			var (rows, error) = await Collect(await call);
			Assert.Null(error);
			Assert.Equal(Ids(5), rows);
		}
		finally
		{
			gated.Release();
		}
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task TheFirstRead_ReturnsAtOnce_AndDoesNotRunTheQuery(
		string view)
	{
		var gated = new ChannelTestReader(5);
		gated.Hold(2);
		var rig = Create(view, () => gated);
		try
		{
			var reader = rig.ToReader(null, false, Ct);
			var read = Task.Run(() => reader.TryPeek(out _), Ct);

			Assert.Same(
				read,
				await Task.WhenAny(
					read,
					Task.Delay(TimeSpan.FromSeconds(10), Ct)));

			gated.Release();
			var (rows, error) = await Collect(reader);
			Assert.Null(error);
			Assert.Equal(Ids(5), rows);
		}
		finally
		{
			gated.Release();
		}
	}

	// ---- 9: the documented samples, as the same call texts ------------

	static FakeExecuteReader Posts(int rows = 5)
		=> new(() => new ChannelTestReader(rows));

	[Fact]
	public static async Task Sample_TheDefault_UnboundedAndDeferred()
	{
		var received = new List<int>();
		var query = Posts();

		ChannelReader<ChannelRow> reader = query
			.Map<ChannelRow>()
			.ToChannelReader(Ct);

		await foreach (ChannelRow row in reader.ReadAllAsync(Ct))
			received.Add(row.Id);

		Assert.Equal(Ids(5), received);
		Assert.True(reader.Completion.IsCompletedSuccessfully);
	}

#pragma warning disable xUnit1051 // the documented sample text has no token argument
	[Fact]
	public static async Task Sample_TheChain_MapToChannelReaderToReadAllAsync()
	{
		var received = new List<int>();
		var query = Posts();

		await foreach (var row in query
			.Map<ChannelRow>()
			.ToChannelReader()
			.ReadAllAsync(Ct))
		{
			received.Add(row.Id);
		}

		Assert.Equal(Ids(5), received);
	}
#pragma warning restore xUnit1051

	[Fact]
	public static async Task Sample_TheDirectIteration_ReturnsTheSameRows()
	{
		var query = Posts();

		List<int> received = await query.Map<ChannelRow>().ReadAsync(
			static async (rows, token) =>
			{
				var list = new List<int>();
				await foreach (ChannelRow row in rows.WithCancellation(token))
					list.Add(row.Id);

				return list;
			},
			Ct);

		Assert.Equal(Ids(5), received);
	}

	[Fact]
	public static async Task Sample_ABoundAndStartImmediately()
	{
		var received = new List<int>();

		ChannelReader<ChannelRow> reader = Posts()
			.Map<ChannelRow>()
			.ToChannelReader(
				new BoundedChannelOptions(2) { SingleReader = true },
				startImmediately: true,
				Ct);

		await foreach (ChannelRow row in reader.ReadAllAsync(Ct))
			received.Add(row.Id);

		Assert.Equal(Ids(5), received);
	}

	[Fact]
	public static async Task Sample_TheCount()
	{
		var received = new List<int>();

		var channel = Posts()
			.Map<ChannelRow>()
			.ToChannel(out var rows, Ct);

		await foreach (ChannelRow row in channel.Reader.ReadAllAsync(Ct))
			received.Add(row.Id);

		long count = await rows;

		Assert.Equal(Ids(5), received);
		Assert.Equal(5, count);
	}
}
