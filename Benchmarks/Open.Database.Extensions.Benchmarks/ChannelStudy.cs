using System.Diagnostics;
using System.Threading.Channels;
using SysChannel = System.Threading.Channels.Channel;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// Targeted study of the producer/consumer question: what does a second stage between the database reader and
/// the target channel buy, and what does it cost? Three designs are compared for each scenario:
///   one-stage  - map on the reading thread, write straight to the target (back-pressure blocks the reader)
///   two-stage  - ODE's ToChannel as shipped: raw rows into a bounded 1024-row buffer, a second task maps them
///   spill      - write straight to the target while it accepts items; only when it is full, spill into a bounded
///                1024-row buffer drained by a pump task (so the reader is freed early without a second hop)
/// "reader-held" is how long the database reader/connection is busy; "total" is until the consumer has everything.
/// Rows that never arrive are reported as LOST. Run: dotnet Open.Database.Extensions.Benchmarks.dll --channel-study
/// </summary>
public static class ChannelStudy
{
	const string Sql = "select * from posts";
	const int Warmup = 3, Runs = 9;

	delegate Task Producer(BenchConnection conn, ChannelWriter<Post> target, Action readerDone);

	public static async Task RunAsync()
	{
		Console.WriteLine("rows  target-cap  consumer-us/item  design        reader-held-ms  total-ms   (median of {0}; LOST = rows never delivered)", Runs);
		foreach (int rows in new[] { 1_000, 10_000 })
		{
			foreach (int capacity in new[] { 0, 100 }) // 0 = unbounded
			{
				foreach (int delayUs in new[] { 0, 2, 20 })
				{
					var conn = new BenchConnection(BenchTable.Posts(rows));
					foreach ((string name, Producer produce) in new (string, Producer)[]
					{
						("one-stage", OneStage),
						("two-stage", TwoStage),
						("spill", Spill),
					})
					{
						var held = new List<double>();
						var total = new List<double>();
						var delivered = new List<double>();
						for (int i = 0; i < Warmup + Runs; i++)
						{
							(double h, double t, long n) = await MeasureAsync(conn, capacity, delayUs, produce);
							if (i >= Warmup) { held.Add(h); total.Add(t); delivered.Add(n); }
						}

						double lost = rows - Median(delivered);
						Console.WriteLine("{0,-5} {1,-11} {2,-17} {3,-13} {4,-15:F2} {5,-9:F2} {6}",
							rows, capacity == 0 ? "unbounded" : capacity, delayUs, name, Median(held), Median(total),
							lost == 0 ? "" : $"LOST {lost:F0} of {rows} rows");
					}
				}
			}
		}
	}

	static double Median(List<double> v)
	{
		v.Sort();
		return v[v.Count / 2];
	}

	static async Task<(double HeldMs, double TotalMs, long Count)> MeasureAsync(
		BenchConnection conn, int capacity, int delayUs, Producer produce)
	{
		Channel<Post> channel = capacity == 0
			? SysChannel.CreateUnbounded<Post>(new UnboundedChannelOptions { SingleReader = true })
			: SysChannel.CreateBounded<Post>(new BoundedChannelOptions(capacity) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });

		long start = Stopwatch.GetTimestamp();
		long heldAt = 0;
		long n = 0;
		var consumer = Task.Run(async () =>
		{
			await foreach (Post _ in channel.Reader.ReadAllAsync())
			{
				Spin(delayUs);
				n++;
			}
		});

		await produce(conn, channel.Writer, () => heldAt = Stopwatch.GetTimestamp());
		if (heldAt == 0) heldAt = Stopwatch.GetTimestamp();
		await Task.WhenAny(consumer, Task.Delay(30_000)); // a lossy design may leave rows undelivered but the channel still completes
		double total = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
		double held = (heldAt - start) * 1000.0 / Stopwatch.Frequency;
		return (held, total, Interlocked.Read(ref n));
	}

	static void Spin(int microseconds)
	{
		if (microseconds == 0) return;
		long end = Stopwatch.GetTimestamp() + microseconds * Stopwatch.Frequency / 1_000_000;
		while (Stopwatch.GetTimestamp() < end) Thread.SpinWait(4);
	}

	// Maps on the reading thread and writes straight to the target (back-pressure blocks the reader).
	static async Task OneStage(BenchConnection conn, ChannelWriter<Post> target, Action readerDone)
	{
		await conn.Command(Sql).Map<Post>().ReadAsync(async (items, ct) =>
		{
			await foreach (Post p in items.WithCancellation(ct)) await target.WriteAsync(p, ct);
		}, default);
		readerDone();
		target.Complete();
	}

	// ODE's actual ToChannel: raw rows into a bounded buffer, second task maps into the target.
	static async Task TwoStage(BenchConnection conn, ChannelWriter<Post> target, Action readerDone)
	{
		await conn.Command(Sql).ToChannel<Post>(target, true);
		readerDone();
	}

	// Direct while the target accepts; spill to a bounded buffer + pump only when it is full. The target is
	// completed only after everything has been delivered.
	static async Task Spill(BenchConnection conn, ChannelWriter<Post> target, Action readerDone)
	{
		Channel<Post>? overflow = null;
		Task? pump = null;
		await conn.Command(Sql).Map<Post>().ReadAsync(async (items, ct) =>
		{
			await foreach (Post p in items.WithCancellation(ct))
			{
				if (overflow is null)
				{
					if (target.TryWrite(p)) continue;

					overflow = SysChannel.CreateBounded<Post>(new BoundedChannelOptions(1024) { SingleReader = true, SingleWriter = true });
					Channel<Post> buffer = overflow;
					pump = Task.Run(async () =>
					{
						await foreach (Post queued in buffer.Reader.ReadAllAsync()) await target.WriteAsync(queued);
					}, ct);
				}

				await overflow.Writer.WriteAsync(p, ct);
			}
		}, default);

		readerDone();
		if (overflow is not null)
		{
			overflow.Writer.Complete();
			await pump!;
		}

		target.Complete();
	}
}
