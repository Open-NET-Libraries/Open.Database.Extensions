using System.Collections.Concurrent;
using System.Diagnostics;

namespace Open.Database.Extensions.Benchmarks;

// BS-SA: the three pieces behind the buffer-boundary study.
//  - RefillRequest / RefillTimer: how an incomplete ReadAsync of the synthetic provider is finished (a dedicated timer thread).
//  - ThreadPoolPressure: busy work items that keep the thread pool occupied, so a continuation has to wait for a thread.
//  - SpinCalibration: how many Spin iterations make one microsecond on this machine (to turn a measured time into RowCostIterations).

/// <summary>One refill handed to the timer thread: finished after its deadline, on the timer thread.</summary>
internal abstract class RefillRequest(long deadline)
{
	/// <summary>The stopwatch timestamp at which the refill ends.</summary>
	internal long Deadline { get; } = deadline;

	/// <summary>Called on the timer thread once the deadline has passed. A request canceled meanwhile is left alone.</summary>
	internal abstract void Complete();

	internal abstract void Fail(Exception exception);
}

/// <summary>One buffer refill that <see cref="SyntheticReader.ReadAsync"/> hands to the timer thread. Its Task completes after the deadline.</summary>
internal sealed class ReadRefillRequest : RefillRequest
{
	// The continuation of the awaiting code is queued to the thread pool (never run inline on the timer thread), so the reader stays open
	// until the pool gives that continuation a thread: the situation the study asks about.
	readonly TaskCompletionSource<bool> _source = new(TaskCreationOptions.RunContinuationsAsynchronously);
	readonly SyntheticReader _reader;
	readonly CancellationTokenRegistration _registration;

	internal ReadRefillRequest(SyntheticReader reader, long deadline, CancellationToken cancellationToken)
		: base(deadline)
	{
		_reader = reader;
		if (cancellationToken.CanBeCanceled)
#pragma warning disable IDE0370 // the analyzer reports the `!` as unnecessary; without it the compiler warns CS8600
			_registration = cancellationToken.UnsafeRegister(static (state, token) => ((ReadRefillRequest)state!)._source.TrySetCanceled(token), this);
#pragma warning restore IDE0370
	}

	/// <summary>The task ReadAsync returns: incomplete until the refill ends or the token is canceled.</summary>
	internal Task<bool> Result => _source.Task;

	internal override void Complete()
	{
		if (_source.Task.IsCompleted) return;
		_reader.AdvanceAfterRefill();
		_registration.Dispose();
		_source.TrySetResult(true);
	}

	internal override void Fail(Exception exception) => _source.TrySetException(exception);
}

/// <summary>One refill paid by an async column read (BoundaryInGetter): the task completes with the value read once the refill has ended.</summary>
internal sealed class GetterRefillRequest<T> : RefillRequest
{
	readonly TaskCompletionSource<T> _source = new(TaskCreationOptions.RunContinuationsAsynchronously);
	readonly SyntheticReader _reader;
	readonly int _ordinal;
	readonly Func<SyntheticReader, int, T> _read;
	readonly CancellationTokenRegistration _registration;

	internal GetterRefillRequest(SyntheticReader reader, int ordinal, Func<SyntheticReader, int, T> read, long deadline, CancellationToken cancellationToken)
		: base(deadline)
	{
		_reader = reader;
		_ordinal = ordinal;
		_read = read;
		if (cancellationToken.CanBeCanceled)
#pragma warning disable IDE0370 // the analyzer reports the `!` as unnecessary; without it the compiler warns CS8600
			_registration = cancellationToken.UnsafeRegister(static (state, token) => ((GetterRefillRequest<T>)state!)._source.TrySetCanceled(token), this);
#pragma warning restore IDE0370
	}

	internal Task<T> Result => _source.Task;

	internal override void Complete()
	{
		if (_source.Task.IsCompleted) return;
		_registration.Dispose();
		try { _source.TrySetResult(_read(_reader, _ordinal)); }
		catch (Exception ex) { _source.TrySetException(ex); }
	}

	internal override void Fail(Exception exception) => _source.TrySetException(exception);
}

/// <summary>
/// The thread that finishes incomplete reads: it spins on the stopwatch until a request's deadline (never Task.Delay or a timer: the Windows timer
/// granularity is about 15 ms) and completes it. One thread for the process, started on the first request, polling for a short while after each
/// request (so back-to-back boundaries do not pay a wake-up), blocked after that, and gone after <see cref="IdleExitMs"/> of idleness (so nothing is left behind).
/// </summary>
internal static class RefillTimer
{
	public const int IdleExitMs = 500;

	static readonly ConcurrentQueue<RefillRequest> Queue = new();
	static readonly SemaphoreSlim Signal = new(0); // One count per queued request, taken before the request is dequeued.
	static readonly Lock Gate = new();
	static readonly long HotTicks = Stopwatch.Frequency / 5000; // 200 us of polling after a request.
	static bool _running;
	static int _threadStarts;

	/// <summary>True while the timer thread exists.</summary>
	public static bool IsRunning
	{
		get
		{
			lock (Gate) return _running;
		}
	}

	/// <summary>How many times the timer thread was started in this process.</summary>
	public static int ThreadStarts => Volatile.Read(ref _threadStarts);

	public static void Schedule(RefillRequest request)
	{
		Queue.Enqueue(request);
		Signal.Release();
		lock (Gate)
		{
			if (_running) return;
			_running = true;
			Interlocked.Increment(ref _threadStarts);
			new Thread(Loop) { IsBackground = true, Name = "synthetic-refill" }.Start();
		}
	}

	static void Loop()
	{
		while (true)
		{
			if (!Signal.Wait(0) && !HotWait() && !Signal.Wait(IdleExitMs))
			{
				lock (Gate)
				{
					// A request enqueued after this check starts a new thread (Schedule sees _running false under the same lock).
					if (Signal.CurrentCount == 0 && Queue.IsEmpty)
					{
						_running = false;
						return;
					}
				}

				continue;
			}

			if (!Queue.TryDequeue(out var request)) continue;
			try
			{
				while (Stopwatch.GetTimestamp() < request.Deadline) Thread.SpinWait(4);
				request.Complete();
			}
			catch (Exception ex)
			{
				request.Fail(ex);
			}
		}
	}

	static bool HotWait()
	{
		long until = Stopwatch.GetTimestamp() + HotTicks;
		while (Stopwatch.GetTimestamp() < until)
		{
			if (Signal.CurrentCount > 0 && Signal.Wait(0)) return true;
			Thread.SpinWait(8);
		}

		return false;
	}
}

/// <summary>
/// K busy work items on the thread pool, kept busy until <see cref="Stop"/>. Each item spins for a fixed slice of <see cref="SliceMicroseconds"/>
/// (not until the stop flag: a slice that ended is a chance for the pool to run something else), then queues itself again to the GLOBAL queue
/// (the same queue a continuation goes to), so the K items are always either running or ahead of a new continuation. An item that sees the stop flag
/// ends. With K at or above the processor count the pool has no idle worker, and a continuation waits for the next slice to end; the pool may add
/// threads while it runs (it is not capped), which is part of what is measured. The calling thread, the timer thread and the OS scheduler compete
/// with the busy items for the cores too, so sync paths feel pressure as well (read the sync numbers with that in mind).
/// </summary>
public static class ThreadPoolPressure
{
	public const int SliceMicroseconds = 1000;

	static readonly Lock Gate = new();
	static volatile bool _stop = true;
	static int _items; // Items queued or running; 0 again once every one has seen the stop flag.
	static long _slices;
	static int _count;

	/// <summary>Busy items still queued or running. 0 before Start and after Stop.</summary>
	public static int Items => Volatile.Read(ref _items);

	/// <summary>The K of the current (or last) run.</summary>
	public static int Count => Volatile.Read(ref _count);

	/// <summary>Slices spun since Start.</summary>
	public static long Slices => Interlocked.Read(ref _slices);

	/// <summary>Resolves a setting: an integer, or "auto" meaning twice the processor count. Null or empty = 0 (off).</summary>
	public static int Parse(string? text)
	{
		if (string.IsNullOrWhiteSpace(text)) return 0;
		text = text.Trim();
		return text.Equals("auto", StringComparison.OrdinalIgnoreCase)
			? Environment.ProcessorCount * 2
			: int.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
	}

	/// <summary>Starts k busy items and returns once min(k, processor count) slices have run, so the pool is busy before the caller measures. k &lt;= 0 does nothing.</summary>
	public static void Start(int k)
	{
		if (k <= 0) return;
		lock (Gate)
		{
			if (!_stop) throw new InvalidOperationException("Thread-pool pressure is already running.");
			_stop = false;
			Volatile.Write(ref _count, k);
			Interlocked.Exchange(ref _slices, 0);
			for (int i = 0; i < k; i++)
			{
				Interlocked.Increment(ref _items);
				Queue();
			}
		}

		long needed = Math.Min(k, Environment.ProcessorCount);
		var wait = Stopwatch.StartNew();
		while (Slices < needed)
		{
			if (wait.ElapsedMilliseconds > 10_000) throw new InvalidOperationException($"Thread-pool pressure did not start: {Slices} of {needed} slices ran in 10 s.");
			Thread.Sleep(1);
		}
	}

	/// <summary>Stops the items and waits until the last one has ended. Does nothing when not running. Throws if an item is still alive after 10 s.</summary>
	public static void Stop()
	{
		lock (Gate)
		{
			if (_stop) return;
			_stop = true;
		}

		var wait = Stopwatch.StartNew();
		while (Items > 0)
		{
			if (wait.ElapsedMilliseconds > 10_000) throw new InvalidOperationException($"Thread-pool pressure did not stop: {Items} busy items are still alive.");
			Thread.Sleep(1);
		}
	}

	static void Queue() => ThreadPool.UnsafeQueueUserWorkItem(static _ => Slice(), 0, preferLocal: false);

	static void Slice()
	{
		if (_stop)
		{
			Interlocked.Decrement(ref _items);
			return;
		}

		Spin.Microseconds(SliceMicroseconds);
		Interlocked.Increment(ref _slices);
		Thread.Yield();
		if (_stop)
		{
			Interlocked.Decrement(ref _items);
			return;
		}

		Queue();
	}
}

/// <summary>Spin iterations per microsecond on this machine (<c>--calibrate-spin</c>). Run it under the same affinity and priority as the benchmark.</summary>
public static class SpinCalibration
{
	/// <summary>Times Thread.SpinWait over several samples; returns the median, the minimum and the maximum, in iterations per microsecond.</summary>
	public static (double Median, double Min, double Max) Measure(int samples = 21, int iterationsPerSample = 200_000)
	{
		for (int i = 0; i < 50; i++) Spin.Iterations(iterationsPerSample / 10); // Warm up, so the first samples are not slower.
		var rates = new double[samples];
		for (int s = 0; s < samples; s++)
		{
			long t0 = Stopwatch.GetTimestamp();
			Spin.Iterations(iterationsPerSample);
			double us = (Stopwatch.GetTimestamp() - t0) * 1_000_000d / Stopwatch.Frequency;
			rates[s] = iterationsPerSample / us;
		}

		Array.Sort(rates);
		return (rates[samples / 2], rates[0], rates[^1]);
	}

	public static int Run()
	{
		var (median, min, max) = Measure();
		Console.WriteLine("runtime: " + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription + ", processors: " + Environment.ProcessorCount);
		Console.WriteLine(FormattableString.Invariant($"Thread.SpinWait iterations per microsecond: median {median:F2}, min {min:F2}, max {max:F2} (21 samples of 200000 iterations)"));
		Console.WriteLine(FormattableString.Invariant($"one iteration is about {1000 / median:F1} ns"));
		foreach (int us in new[] { 1, 5, 10, 20, 50, 100 })
			Console.WriteLine(FormattableString.Invariant($"  {us,4} us = {(int)Math.Round(us * median),7} iterations (RowCostIterations / NextResultCostIterations for a cost of {us} us)"));

		Console.WriteLine("CALIBRATE-SPIN DONE (the median is the number to use; a wide min-max range means the machine was busy: run it again)");
		return 0;
	}
}
