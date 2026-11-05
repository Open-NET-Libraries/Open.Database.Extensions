using System.Threading.Channels;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The channel producers: a channel is the queue, so the command dumps straight into it; the producing work
// must never run on the caller's thread; and no row may be lost or executed twice.
[ExcludeFromCodeCoverage]
public static class ChannelProducerTests
{
	sealed class Item { public int Id { get; set; } }

	static FakeExecuteReader Rows(int count)
		=> new(() => TypedReader.Create([("Id", typeof(int))], Enumerable.Range(1, count).Select(i => new object?[] { i }).ToArray()));

	[Fact]
	public static async Task BoundedTarget_SlowConsumer_LosesNothing()
	{
		const int count = 5_000;
		var ct = TestContext.Current.CancellationToken;
		var channel = Channel.CreateBounded<Item>(new BoundedChannelOptions(10) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });

		Task<long> producer = Rows(count).ToChannel<Item>(channel.Writer, true).AsTask();
		int received = 0;
		await foreach (Item _ in channel.Reader.ReadAllAsync(ct))
		{
			received++;
			if (received % 7 == 0) await Task.Yield(); // a consumer slower than the reader
		}

		Assert.Equal(count, received);
		Assert.Equal(count, await producer);
	}

	[Fact]
	public static async Task CompleteFalse_ExecutesOnce_AndLeavesTheWriterOpen()
	{
		var ct = TestContext.Current.CancellationToken;
		var channel = Channel.CreateUnbounded<Item>();
		FakeExecuteReader cmd = Rows(3);

		long n = await cmd.ToChannel<Item>(channel.Writer, false);

		Assert.Equal(3, n);
		Assert.Equal(1, cmd.ExecuteCount); // It used to run the command twice when asked not to complete.
		Assert.True(channel.Writer.TryWrite(new Item()), "The writer must still be open.");
		Assert.False(channel.Reader.Completion.IsCompleted);

		channel.Writer.Complete();
		int total = 0;
		await foreach (Item _ in channel.Reader.ReadAllAsync(ct)) total++;
		Assert.Equal(4, total);
	}

	[Fact]
	public static async Task CompleteTrue_CompletesTheWriter()
	{
		var ct = TestContext.Current.CancellationToken;
		var channel = Channel.CreateUnbounded<Item>();
		FakeExecuteReader cmd = Rows(3);

		await cmd.ToChannel<Item>(channel.Writer, true);

		Assert.Equal(1, cmd.ExecuteCount);
		int total = 0;
		await foreach (Item _ in channel.Reader.ReadAllAsync(ct)) total++;
		Assert.Equal(3, total);
	}

	[Fact]
	public static void ChannelReaderOverload_ProducesOffTheCallersThread()
	{
		const int count = 200;
		int caller = Environment.CurrentManagedThreadId;
		int onCaller = 0, seen = 0;

		_ = Rows(count).ToChannel(true, r =>
		{
			if (Environment.CurrentManagedThreadId == caller) Interlocked.Increment(ref onCaller);
			Interlocked.Increment(ref seen);
			return new Item { Id = r.GetInt32(0) };
		});

		// Keep this thread occupied (no await) until the rows have been produced: if the caller were the
		// producer, everything would already have run here before ToChannel returned.
		Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref seen) == count, TimeSpan.FromSeconds(10)));
		Assert.Equal(0, onCaller);
	}

	[Fact]
	public static async Task ChannelReaderOverload_Mapped_DeliversEveryRow()
	{
		var ct = TestContext.Current.CancellationToken;
		int total = 0;
		await foreach (Item _ in Rows(1_000).ToChannel<Item>(true).ReadAllAsync(ct)) total++;
		Assert.Equal(1_000, total);
	}

	[Fact]
	public static async Task StartFailure_CompletesTheChannelWithTheError()
	{
		var ct = TestContext.Current.CancellationToken;
		var cmd = new FakeExecuteReader(() => throw new InvalidOperationException("boom"));

		ChannelReader<Item> reader = cmd.ToChannel<Item>(true);

		var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
		{
			await foreach (Item _ in reader.ReadAllAsync(ct)) { }
		});
		Assert.Contains("boom", ex.ToString());
	}
}
