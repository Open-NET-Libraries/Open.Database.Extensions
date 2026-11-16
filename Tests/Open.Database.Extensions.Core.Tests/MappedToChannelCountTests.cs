using static Open.Database.Extensions.Tests.ChannelRig;
using static Open.Database.Extensions.Tests.ChannelWait;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ToChannel(out Task<long> readCount, ...): the channel behaves as
// ToChannel does, and the task is the count of the rows written,
// completed after the read-through, faulting or cancelled the way
// the channel ends.
[ExcludeFromCodeCoverage]
public static class MappedToChannelCountTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(300);

	public static TheoryData<string> Views => ViewData;

	// ---- 23: the documented usage -----------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task DocumentedUsage_CountEqualsTheRows(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(5, laterSets: 1));
		var received = new List<int>();

		var channel = rig.ToCounted(out var rows, null, false, Ct);
		await foreach (ChannelRow row in channel.Reader.ReadAllAsync(Ct))
			received.Add(row.Id);

		long n = await rows.Within();

		Assert.Equal(Ids(5), received);
		Assert.Equal(5, n);
		Assert.Equal(2, rig.Reader!.NextResultCalls);
	}

	// ---- 24: a failure faults the count with the channel's exception ------

	[Theory, MemberData(nameof(Views))]
	public static async Task Failure_FaultsTheCountAndTheChannel(
		string view)
	{
		var rig = Create(
			view,
			() => new ChannelTestReader(2, laterSets: 2, failAtNextResult: 1));
		var channel = rig.ToCounted(out var rows, null, false, Ct);

		var (received, error) = await Collect(channel.Reader);
		var countError = await Assert.ThrowsAsync<LaterSetFailure>(
			() => rows.Within());

		Assert.Equal(Ids(2), received);
		Assert.Same(error, countError);
	}

	// ---- 25: a cancel ------------------------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task Cancel_CancelsTheCountAndTheChannel(
		string view)
	{
		using var cts = new CancellationTokenSource();
		var rig = Create(view, () => new ChannelTestReader(10));
		var channel = rig.ToCounted(
			out var rows,
			new BoundedChannelOptions(1),
			true,
			cts.Token);
		await Until(() => rig.Reader is { RowsRead: >= 2 }, "two rows read");

		cts.Cancel();
		var (_, error) = await Collect(channel.Reader);

		Assert.IsAssignableFrom<OperationCanceledException>(error);
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => rows.Within());
		Assert.True(rows.IsCanceled);
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task PreCancelled_CancelsTheCount(
		string view)
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		var rig = Create(view, () => new ChannelTestReader(3));

		var channel = rig.ToCounted(out var rows, null, false, cts.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => rows.Within());
		Assert.True(rows.IsCanceled);
		var (_, error) = await Collect(channel.Reader);
		Assert.IsAssignableFrom<OperationCanceledException>(error);
		Assert.Equal(0, rig.Executes);
	}

	// ---- 26: the start ------------------------------------------------------

	[Theory, MemberData(nameof(Views))]
	public static async Task StartImmediately_CountNeedsNoRead(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(4));

		_ = rig.ToCounted(out var rows, null, true, Ct);

		Assert.Equal(4, await rows.Within());
	}

	[Theory, MemberData(nameof(Views))]
	public static async Task ADeferredCount_IsIncompleteUntilTheFirstRead(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(4));
		var channel = rig.ToCounted(out var rows, null, false, Ct);

		var first = await Task.WhenAny(rows, Task.Delay(Quiet, Ct));
		Assert.NotSame(rows, first);
		Assert.False(rows.IsCompleted);

		channel.Reader.TryRead(out _);

		Assert.Equal(4, await rows.Within());
	}

	// ---- 27: awaiting twice ----

	[Theory, MemberData(nameof(Views))]
	public static async Task AwaitingTheCountTwice_ReturnsTheSameCount(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var channel = rig.ToCounted(out var rows, null, false, Ct);
		await Collect(channel.Reader);

		long first = await rows;
		long second = await rows;

		Assert.Equal(3, first);
		Assert.Equal(first, second);
	}

	// ---- the count when the consumer closes the channel early ----

	[Theory, MemberData(nameof(Views))]
	public static async Task ClosedEarly_CountIsTheRowsWritten(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(10));
		var channel = rig.ToCounted(
			out var rows,
			new BoundedChannelOptions(1),
			true,
			Ct);
		await Until(
			() => rig.Reader is { RowsRead: >= 2 },
			"the feed waits for room");

		channel.Reader.TryRead(out _);
		Assert.True(channel.Writer.TryComplete());

		long n = await rows.Within();

		Assert.InRange(n, 1, 3);
		Assert.Equal(1, rig.Reader!.Disposes);
	}

	// ---- a channel the caller completed with an error ----

	[Theory, MemberData(nameof(Views))]
	public static async Task CallersError_IsPropagated(
		string view)
	{
		var rig = Create(view, () => new ChannelTestReader(3));
		var channel = rig.ToCounted(out var rows, null, false, Ct);
		var owner = new InvalidOperationException("the owner's error");
		Assert.True(channel.Writer.TryComplete(owner));

		// The first read starts the feed, and the feed's first write meets
		// the closed channel: the owner's error, not a silent stop.
		var (_, readError) = await Collect(channel.Reader);
		var countError = await Assert.ThrowsAsync<InvalidOperationException>(
			() => rows.Within());

		Assert.Same(owner, readError);
		Assert.Same(owner, countError);
	}
}
