using static Open.Database.Extensions.Tests.ChannelRig;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ToChannel returning the Channel<T>: the caller gets both ends of a
// channel ODX made, and ODX still completes it.
// The shared behaviours (a bound, the connection, read-through,
// the read mode) run against this form in MappedToChannelReaderTests.
[ExcludeFromCodeCoverage]
public static class MappedToChannelTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(300);

	public static TheoryData<string> Views => ViewData;

	// ---- 19: it always completes ----------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task ToChannel_AlwaysCompletes_AfterTheLastRow(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3, laterSets: 1));
		var channel = rig.ToChannel(null, false, Ct);

		var (rows, error) = await Collect(channel.Reader);

		Assert.Null(error);
		Assert.Equal(Ids(3), rows);
		await channel.Reader.Completion.Within();
		Assert.False(channel.Writer.TryWrite(new ChannelRow(99)));
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task EveryRung_ReturnsAChannelWithBothEnds(string view)
	{
		var rig = Create(view, () => new ChannelTestReader(2));

		foreach (bool start in new[] { false, true })
		{
			var channel = rig.ToChannel(
				new BoundedChannelOptions(4),
				start,
				Ct);

			Assert.NotNull(channel.Reader);
			Assert.NotNull(channel.Writer);
			Assert.Equal(Ids(2), (await Collect(channel.Reader)).Rows);
		}
	}

	// ---- 20: a failure goes through TryComplete(ex) ----------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task Failure_EndsTheChannelWithIt(
		string view)
	{
		var rig = Create(
			view,
			() => new ChannelTestReader(2, laterSets: 2, failAtNextResult: 2));
		var channel = rig.ToChannel(null, false, Ct);

		var (rows, error) = await Collect(channel.Reader);

		Assert.Equal(Ids(2), rows);
		Assert.IsType<LaterSetFailure>(error);
		await Assert.ThrowsAsync<LaterSetFailure>(
			() => channel.Reader.Completion.Within());
	}

	// ---- 21: the start ----------------------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task DeferredNeverRead_AWriteDoesNotStartIt(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var channel = rig.ToChannel(null, false, Ct);

		Assert.True(channel.Writer.TryWrite(new ChannelRow(100)));
		await channel.Writer.WriteAsync(new ChannelRow(101), Ct);
		await Task.Delay(Quiet, Ct);

		Assert.Equal(0, rig.Executes);
		Assert.True(rig.Connection is null || rig.Connection.OpenCalls == 0);
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task StartImmediately_StartsTheFeedAtTheCall(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3));

		var channel = rig.ToChannel(null, true, Ct);

		await Until(() => rig.Executes == 1, "the feed starts without a read");
		await Until(() => rig.Reader is { Disposes: 1 }, "the feed ends");
		Assert.Equal(Ids(3), (await Collect(channel.Reader)).Rows);
	}

	// ---- the writer -------------------------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task CallersWrites_Interleave_ThenFail(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var channel = rig.ToChannel(null, false, Ct);
		Assert.True(channel.Writer.TryWrite(new ChannelRow(100)));

		var (rows, error) = await Collect(channel.Reader);

		Assert.Null(error);
		Assert.Equal([100, 1, 2, 3], rows);
		Assert.False(channel.Writer.TryWrite(new ChannelRow(101)));
		await Assert.ThrowsAsync<ChannelClosedException>(
			async () => await channel.Writer.WriteAsync(
				new ChannelRow(101),
				Ct));
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task CallerCloses_TheReadStops(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(10));
		var channel = rig.ToChannel(new BoundedChannelOptions(1), true, Ct);
		await Until(
			() => rig.Reader is { RowsRead: >= 2 },
			"the feed waits for room");

		Assert.True(channel.Writer.TryComplete());
		await Until(() => rig.Reader!.Disposes == 1, "the feed stops");

		var (rows, error) = await Collect(channel.Reader);
		Assert.Null(error);
		Assert.True(rows.Count < 10);
		if (rig.Connection is { } connection)
			Assert.Equal(["open", "close"], connection.Log);
	}

	// ---- 22: the options are honoured -------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task ABoundedCapacity_WaitsForRoom(string view)
	{
		var rig = Create(view, () => new ChannelTestReader(6));
		var channel = rig.ToChannel(new BoundedChannelOptions(2), true, Ct);

		await Until(() => rig.Reader is { RowsRead: >= 3 }, "three rows read");
		await Task.Delay(Quiet, Ct);
		Assert.Equal(3, rig.Reader!.RowsRead);

		Assert.Equal(Ids(6), (await Collect(channel.Reader)).Rows);
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task ADropMode_DropsRows_AndTheChannelStillCompletes(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(10));
		var options = new BoundedChannelOptions(3)
		{
			FullMode = BoundedChannelFullMode.DropOldest,
		};
		var channel = rig.ToChannel(options, true, Ct);

		await Until(() => rig.Reader is { Disposes: 1 }, "the feed ends");
		var (rows, error) = await Collect(channel.Reader);

		Assert.Null(error);
		Assert.Equal([8, 9, 10], rows);
	}

	[Fact]
	public static void ToChannel_TakesNoEndOption_TheOptionsAreTheBCLsOnly()
	{
		var rig = Create("Query", () => new ChannelTestReader(1));

		var channel = rig.ToChannel(
			new UnboundedChannelOptions { SingleReader = true }, false, Ct);

		Assert.NotNull(channel.Reader);
	}
}
