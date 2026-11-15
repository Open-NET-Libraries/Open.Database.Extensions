using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;

namespace Open.Database.Extensions.Benchmarks;

// BS-SA: --selftest-refill. The buffer-boundary model of the synthetic provider and the thread-pool pressure setting do what they say.
//   (a) with the refill off nothing is counted (the full "nothing changed" proof is --verify and --selftest-alloc, run beside this one)
//   (b) with it on, the sync path and the async path both pay about the refill time per boundary
//   (c) the async path really goes async: one incomplete Task per boundary, at the right calls, resumed on a pool thread other than the caller's
//   (d) what a canceled token does to a read that waits for a refill (the model; R15)
//   (e) pressure starts and stops cleanly, and it makes the async path's reader-open larger
public static partial class SyntheticChecks
{
	const int RefillRows = 1000, RefillBuffer = 100, RefillUs = 500, RefillReps = 15, RefillWidth = 5;

	sealed class AsyncDrive
	{
		public int Rows, Calls, Incomplete, OnOtherThread, OnPoolThread;
		public readonly List<int> IncompleteAt = [];
	}

	static SyntheticConnection RefillConnection(SyntheticRecorder recorder, int rows = RefillRows, int bufferRows = 0, int refillUs = 0, int width = RefillWidth)
		=> new(SyntheticTable.Create(width, rows), new SyntheticOptions { BufferRows = bufferRows, RefillCostUs = refillUs, Recorder = recorder });

	static DbDataReader OpenReader(SyntheticConnection connection, CommandBehavior behavior = CommandBehavior.SequentialAccess)
	{
		using var command = connection.CreateCommand();
		return command.ExecuteReader(behavior);
	}

	static int ReadSync(DbDataReader reader)
	{
		int rows = 0;
		while (reader.Read()) rows++;
		return rows;
	}

	// Drives ReadAsync by hand on the CALLING thread (the main thread, which is not a pool thread): an incomplete task gets a continuation, the caller waits for it.
	static AsyncDrive DriveAsync(DbDataReader reader)
	{
		var drive = new AsyncDrive();
		int caller = Environment.CurrentManagedThreadId;
		using var gate = new SemaphoreSlim(0);
		while (true)
		{
			var task = reader.ReadAsync();
			bool more;
			if (task.IsCompleted)
			{
				more = task.Result;
			}
			else
			{
				drive.Incomplete++;
				drive.IncompleteAt.Add(drive.Calls);
				int resumedOn = 0;
				bool pool = false;
				task.GetAwaiter().UnsafeOnCompleted(() =>
				{
					resumedOn = Environment.CurrentManagedThreadId;
					pool = Thread.CurrentThread.IsThreadPoolThread;
					gate.Release();
				});
				gate.Wait();
				if (resumedOn != caller) drive.OnOtherThread++;
				if (pool) drive.OnPoolThread++;
				more = task.Result;
			}

			drive.Calls++;
			if (!more) return drive;
			drive.Rows++;
		}
	}

	static double MedianUs(Action operation, int reps)
	{
		operation();
		operation();
		var times = new double[reps];
		for (int i = 0; i < reps; i++)
		{
			long t0 = Stopwatch.GetTimestamp();
			operation();
			times[i] = (Stopwatch.GetTimestamp() - t0) * 1_000_000d / Stopwatch.Frequency;
		}

		Array.Sort(times);
		return times[reps / 2];
	}

	static string Us(double us) => us.ToString("F0", CultureInfo.InvariantCulture);

	public static int SelfTestRefill()
	{
		var report = new Report();
		Console.WriteLine($"processors {Environment.ProcessorCount}; {RefillRows} rows, width {RefillWidth}, refill every {RefillBuffer} rows for {RefillUs} us = {(RefillRows - 1) / RefillBuffer} boundaries per read, {(RefillRows - 1) / RefillBuffer * RefillUs} us expected extra; median of {RefillReps} runs");
		RefillOffChecks(report);
		RefillCostChecks(report);
		RefillThreadChecks(report);
		RefillCancelChecks(report);
		PressureChecks(report);
		Console.WriteLine(report.Failures == 0 ? $"SELFTEST REFILL PASS ({report.Checks} checks)" : $"SELFTEST REFILL FAIL ({report.Failures} of {report.Checks})");
		return report.Failures == 0 ? 0 : 1;
	}

	// ---- (a) off ----------------------------------------------------------------
	static void RefillOffChecks(Report report)
	{
		Console.WriteLine("== (a) refill off: no boundary is counted on any path ==");
		var rec = new SyntheticRecorder();
		var conn = RefillConnection(rec);
		using (var r = OpenReader(conn)) report.Check(ReadSync(r) == RefillRows, "sync read, refill off: row count");
		AsyncDrive drive;
		using (var r = OpenReader(conn)) drive = DriveAsync(r);
		report.Check(drive.Rows == RefillRows && drive.Incomplete == 0, $"async read, refill off: {drive.Rows} rows, {drive.Incomplete} incomplete tasks, expected {RefillRows} and 0");
		var rows = SyntheticVariants.V1(conn);
		var rowsAsyncRead = Wait(SyntheticVariants.V1AsyncRead(conn));
		report.Check(rows.Count == RefillRows && rowsAsyncRead.Count == RefillRows, "V1 and V1 with ReadAsync, refill off: row counts");
		report.Check(rec.RefillsBlocked == 0 && rec.RefillsAsync == 0, $"refill off: {rec.RefillsBlocked} blocked and {rec.RefillsAsync} async refills counted, expected 0 and 0");
		Console.WriteLine($"  ok   off: blocked {rec.RefillsBlocked}, async {rec.RefillsAsync}, incomplete tasks {drive.Incomplete} (the bytes-per-row proof is --selftest-alloc, the equal-objects proof is --verify)");
	}

	// ---- (b) cost ----------------------------------------------------------------
	static void RefillCostChecks(Report report)
	{
		Console.WriteLine("== (b) both paths pay about the refill time per boundary ==");
		int boundaries = (RefillRows - 1) / RefillBuffer;
		double expected = boundaries * (double)RefillUs;
		double handOff = 400; // Per boundary, the most the async hand-off (timer thread wake-up, pool wake-up) may add to the refill time before the check fails.

		var offRec = new SyntheticRecorder();
		var onRec = new SyntheticRecorder();
		var off = RefillConnection(offRec);
		var on = RefillConnection(onRec, bufferRows: RefillBuffer, refillUs: RefillUs);

		double syncOff = MedianUs(() => { using var r = OpenReader(off); ReadSync(r); }, RefillReps);
		double syncOn = MedianUs(() => { using var r = OpenReader(on); ReadSync(r); }, RefillReps);
		double asyncOff = MedianUs(() => { using var r = OpenReader(off); DriveAsync(r); }, RefillReps);
		double asyncOn = MedianUs(() => { using var r = OpenReader(on); DriveAsync(r); }, RefillReps);
		double syncExtra = syncOn - syncOff, asyncExtra = asyncOn - asyncOff;
		Console.WriteLine($"  raw loops, total us: sync {Us(syncOff)} -> {Us(syncOn)} (+{Us(syncExtra)}), async {Us(asyncOff)} -> {Us(asyncOn)} (+{Us(asyncExtra)}); expected +{Us(expected)}; async minus sync per boundary {Us((asyncExtra - syncExtra) / boundaries)} us");
		report.Check(syncExtra >= 0.95 * expected && syncExtra <= (1.10 * expected) + 200, $"sync Read: refill added {Us(syncExtra)} us, expected {Us(expected)} (tolerance -5% to +10% and 200 us)");
		report.Check(asyncExtra >= 0.95 * expected && asyncExtra <= (1.10 * expected) + (boundaries * handOff), $"ReadAsync: refill added {Us(asyncExtra)} us, expected {Us(expected)} (tolerance -5% to +10% and {Us(boundaries * handOff)} us of hand-off)");

		// The library's three paths, reader-open as the recorder sees it (from ExecuteReader to the close).
		var variants = new (string Name, Func<SyntheticConnection, List<SynRow>> Run, bool PaysAsync)[]
		{
			("V1 (sync execute, Read)", SyntheticVariants.V1, false),
			("V1_Async (async execute, Read)", static c => Wait(SyntheticVariants.V1Async(c)), false),
			("V1_AsyncRead (EnableAsyncRead)", static c => Wait(SyntheticVariants.V1AsyncRead(c)), true),
		};

		var table = SyntheticTable.Create(RefillWidth, RefillRows);
		foreach (var (name, run, paysAsync) in variants)
		{
			var recOff = new SyntheticRecorder();
			var recOn = new SyntheticRecorder();
			var connOff = RefillConnection(recOff);
			var connOn = RefillConnection(recOn, bufferRows: RefillBuffer, refillUs: RefillUs);
			double totalOff = MedianUs(() => run(connOff), RefillReps), totalOn = MedianUs(() => run(connOn), RefillReps);
			double openOff = recOff.Summary().MedianOpenUs, openOn = recOn.Summary().MedianOpenUs;
			double extra = openOn - openOff;
			double upper = (1.10 * expected) + 200 + (paysAsync ? boundaries * handOff : 0);
			Console.WriteLine($"  {name,-32} reader-open {Us(openOff)} -> {Us(openOn)} (+{Us(extra)}), total {Us(totalOff)} -> {Us(totalOn)}; blocked {recOn.RefillsBlocked}, async {recOn.RefillsAsync} over {recOn.Readers} readers");
			report.Check(extra >= 0.95 * expected && extra <= upper, $"{name}: reader-open grew by {Us(extra)} us, expected about {Us(expected)}");
			long readers = recOn.Readers;
			report.Check(paysAsync
				? recOn.RefillsAsync == readers * boundaries && recOn.RefillsBlocked == 0
				: recOn.RefillsBlocked == readers * boundaries && recOn.RefillsAsync == 0,
				$"{name}: {recOn.RefillsBlocked} blocked and {recOn.RefillsAsync} async refills over {readers} readers, expected {(paysAsync ? 0 : readers * boundaries)} and {(paysAsync ? readers * boundaries : 0)}");

			var result = run(connOn);
			bool equal = result.Count == RefillRows;
			for (int i = 0; equal && i < result.Count; i++) equal = SynRow.Diff(SynRow.FromTable(table, i), result[i]).Count == 0;
			report.Check(equal, $"{name}: the rows read with the refill on equal the table");
		}
	}

	// ---- (c) the async path really goes async ----------------------------------------------------------------
	static void RefillThreadChecks(Report report)
	{
		Console.WriteLine("== (c) ReadAsync returns an incomplete Task at each boundary and only there; its continuation runs on a pool thread, not the caller's ==");
		foreach (int rows in new[] { 1, 100, 101, 1000, 1001 })
		{
			var rec = new SyntheticRecorder();
			var conn = RefillConnection(rec, rows, RefillBuffer, RefillUs);
			AsyncDrive d;
			using (var r = OpenReader(conn)) d = DriveAsync(r);
			int boundaries = (rows - 1) / RefillBuffer;
			var expectedAt = Enumerable.Range(1, boundaries).Select(static i => i * RefillBuffer).ToList();
			bool at = d.IncompleteAt.SequenceEqual(expectedAt);
			Console.WriteLine($"  rows {rows,4}: {d.Calls} ReadAsync calls, {d.Incomplete} incomplete (expected {boundaries}), continuations on another thread {d.OnOtherThread}, on a pool thread {d.OnPoolThread}, synchronous {d.Calls - d.Incomplete}");
			report.Check(d.Rows == rows && d.Calls == rows + 1, $"rows {rows}: {d.Rows} rows in {d.Calls} calls, expected {rows} in {rows + 1}");
			report.Check(d.Incomplete == boundaries && at, $"rows {rows}: incomplete tasks at calls [{string.Join(",", d.IncompleteAt)}], expected exactly the {boundaries} boundaries [{string.Join(",", expectedAt)}] (the first call and the calls between stay synchronous)");
			report.Check(d.OnOtherThread == boundaries && d.OnPoolThread == boundaries, $"rows {rows}: {d.OnOtherThread} continuations on another thread and {d.OnPoolThread} on a pool thread, expected {boundaries} of each");
			report.Check(rec.RefillsAsync == boundaries && rec.RefillsBlocked == 0, $"rows {rows}: the reader counted {rec.RefillsAsync} async refills, expected {boundaries}");
		}

		// A zero-cost refill still hands off on the async path and costs the sync path nothing: the boundary is the thread switch itself.
		var zero = RefillConnection(new SyntheticRecorder(), RefillRows, RefillBuffer, 0);
		AsyncDrive z;
		using (var r = OpenReader(zero)) z = DriveAsync(r);
		report.Check(z.Incomplete == (RefillRows - 1) / RefillBuffer, $"refill cost 0: {z.Incomplete} incomplete tasks, expected {(RefillRows - 1) / RefillBuffer}");
		Console.WriteLine($"  refill cost 0 us: {z.Incomplete} incomplete tasks (a pure thread hand-off)");

		// Where there is no boundary there is none: the column reads and NextResult never wait.
		var rec2 = new SyntheticRecorder();
		var conn2 = new SyntheticConnection(SyntheticTable.Create(3, 150), new SyntheticOptions { BufferRows = 50, RefillCostUs = 1000, ExtraResultSets = 2, Recorder = rec2 });
		using (var r = OpenReader(conn2, CommandBehavior.Default))
		{
			while (r.Read()) { }
			long t0 = Stopwatch.GetTimestamp();
			bool more = r.NextResultAsync().IsCompleted && r.NextResult();
			var field = r.GetFieldValueAsync<int>(0);
			var isNull = r.IsDBNullAsync(0);
			report.Check(more && field.IsCompleted && isNull.IsCompleted, "NextResultAsync, GetFieldValueAsync and IsDBNullAsync complete synchronously (no boundary)");
			report.Check((Stopwatch.GetTimestamp() - t0) * 1_000_000d / Stopwatch.Frequency < 900, "NextResult and the async column reads did not wait for a refill");
		}
	}

	// ---- (d) cancellation (R15) ----------------------------------------------------------------
	static void RefillCancelChecks(Report report)
	{
		Console.WriteLine("== (d) a token canceled while a read waits for its refill (the synthetic model only; not SqlClient) ==");
		const int RefillMs = 300, CancelMs = 30;
		var asyncConn = RefillConnection(new SyntheticRecorder(), 6, 2, RefillMs * 1000);
		using (var r = OpenReader(asyncConn))
		{
			r.ReadAsync().Wait();
			r.ReadAsync().Wait();
			using var cts = new CancellationTokenSource();
			long t0 = Stopwatch.GetTimestamp();
			var task = r.ReadAsync(cts.Token);
			cts.CancelAfter(CancelMs);
			bool canceled;
			try { task.Wait(); canceled = false; }
			catch (AggregateException ex) { canceled = ex.InnerException is OperationCanceledException; }
			double ms = (Stopwatch.GetTimestamp() - t0) * 1000d / Stopwatch.Frequency;
			Console.WriteLine($"  ReadAsync(token): token canceled after {CancelMs} ms, the task ended canceled={canceled} after {ms:F0} ms (the refill takes {RefillMs} ms)");
			report.Check(canceled && ms < RefillMs * 0.6, $"ReadAsync(token) stopped {ms:F0} ms in (canceled={canceled}), expected a cancellation well before the {RefillMs} ms refill");
		}

		var syncConn = RefillConnection(new SyntheticRecorder(), 6, 2, RefillMs * 1000);
		using (var r = OpenReader(syncConn))
		{
			r.Read();
			r.Read();
			using var cts = new CancellationTokenSource();
			cts.CancelAfter(CancelMs);
			long t0 = Stopwatch.GetTimestamp();
			bool got = r.Read(); // Read takes no token: the cancellation has nothing to act on.
			double ms = (Stopwatch.GetTimestamp() - t0) * 1000d / Stopwatch.Frequency;
			Console.WriteLine($"  Read(): the same token was canceled after {CancelMs} ms, Read() returned {got} after {ms:F0} ms (the whole refill)");
			report.Check(got && cts.IsCancellationRequested && ms >= RefillMs * 0.95, $"sync Read() returned after {ms:F0} ms, expected the whole {RefillMs} ms refill (a blocked read cannot be canceled)");
		}
	}

	// ---- (e) pressure ----------------------------------------------------------------
	static void PressureChecks(Report report)
	{
		Console.WriteLine("== (e) thread-pool pressure: starts and stops cleanly, and makes the async path's reader-open larger ==");
		int k = ThreadPoolPressure.Parse("auto");
		report.Check(k == Environment.ProcessorCount * 2 && ThreadPoolPressure.Parse("3") == 3 && ThreadPoolPressure.Parse("") == 0, $"Parse: auto = {k} (processor count x 2 = {Environment.ProcessorCount * 2}), 3 = 3, empty = 0");

		const int Rows = 1000, Buffer = 100, PressureRefillUs = 100;
		int boundaries = (Rows - 1) / Buffer;
		var variants = new (string Name, Func<SyntheticConnection, List<SynRow>> Run)[]
		{
			("V1 (sync Read)", SyntheticVariants.V1),
			("V1_AsyncRead", static c => Wait(SyntheticVariants.V1AsyncRead(c))),
		};

		var open = new double[2, 2]; // [variant, pressure off/on]
		for (int pressure = 0; pressure < 2; pressure++)
		{
			if (pressure == 1)
			{
				int threadsBefore = Process.GetCurrentProcess().Threads.Count;
				ThreadPoolPressure.Start(k);
				report.Check(ThreadPoolPressure.Items == k && ThreadPoolPressure.Slices > 0, $"pressure started: {ThreadPoolPressure.Items} busy items (K = {k}), {ThreadPoolPressure.Slices} slices run");
				Console.WriteLine($"  pressure on: K = {k}, pool threads {ThreadPool.ThreadCount}, pending items {ThreadPool.PendingWorkItemCount}, process threads {threadsBefore} before");
				try
				{
					report.Check(Throws(() => ThreadPoolPressure.Start(k)), "a second Start while running is refused");
				}
				catch
				{
					ThreadPoolPressure.Stop();
					throw;
				}
			}

			for (int v = 0; v < variants.Length; v++)
			{
				var rec = new SyntheticRecorder();
				var conn = RefillConnection(rec, Rows, Buffer, PressureRefillUs);
				MedianUs(() => variants[v].Run(conn), 3);
				rec.Reset();
				MedianUs(() => variants[v].Run(conn), RefillReps);
				open[v, pressure] = rec.Summary().MedianOpenUs;
			}

			if (pressure == 1) ThreadPoolPressure.Stop();
		}

		double expectedFloor = 0.1 * boundaries * ThreadPoolPressure.SliceMicroseconds;
		for (int v = 0; v < variants.Length; v++)
			Console.WriteLine($"  {variants[v].Name,-16} reader-open (median, us): pressure off {Us(open[v, 0])}, on {Us(open[v, 1])} ({(open[v, 1] >= open[v, 0] ? "+" : "")}{Us(open[v, 1] - open[v, 0])}); {boundaries} boundaries of {PressureRefillUs} us per read, busy slice {ThreadPoolPressure.SliceMicroseconds} us");

		// Expected direction: the async path waits for a pool thread at every boundary, so reader-open is larger with pressure. The sync path holds its thread and does not wait for the pool
		// (the OS scheduler may still slow it); it is printed, not asserted.
		report.Check(open[1, 1] >= open[1, 0] + expectedFloor, $"V1_AsyncRead reader-open: {Us(open[1, 0])} us without pressure, {Us(open[1, 1])} us with, expected at least {Us(expectedFloor)} us more");

		// Stopped cleanly.
		report.Check(ThreadPoolPressure.Items == 0, $"after Stop: {ThreadPoolPressure.Items} busy items alive, expected 0");
		var wait = Stopwatch.StartNew();
		while (ThreadPool.PendingWorkItemCount > 0 && wait.ElapsedMilliseconds < 500) Thread.Sleep(5);
		report.Check(ThreadPool.PendingWorkItemCount == 0, $"after Stop: {ThreadPool.PendingWorkItemCount} work items still queued, expected 0");
		var process = Process.GetCurrentProcess();
		process.Refresh();
		var cpu0 = process.TotalProcessorTime;
		Thread.Sleep(400);
		process.Refresh();
		double idleCpuMs = (process.TotalProcessorTime - cpu0).TotalMilliseconds;
		report.Check(idleCpuMs < 100, $"after Stop: the process used {idleCpuMs:F0} ms of CPU in 400 ms of idle, expected under 100 (no thread left spinning; with K = {k} busy items it would be about {k * 400} ms or the number of cores x 400)");
		ThreadPoolPressure.Start(2);
		ThreadPoolPressure.Stop();
		report.Check(ThreadPoolPressure.Items == 0, "Start and Stop again: no items left");
		Console.WriteLine($"  after Stop: busy items {ThreadPoolPressure.Items}, queued items {ThreadPool.PendingWorkItemCount}, CPU in 400 ms idle {idleCpuMs:F0} ms, pool threads {ThreadPool.ThreadCount} (idle pool threads end by themselves after about 20 s), process threads {Process.GetCurrentProcess().Threads.Count}");

		// The refill timer thread ends by itself when idle.
		var recT = new SyntheticRecorder();
		using (var r = OpenReader(RefillConnection(recT, 3, 1, 10))) DriveAsync(r);
		bool started = RefillTimer.IsRunning;
		Thread.Sleep(RefillTimer.IdleExitMs + 400);
		report.Check(started && !RefillTimer.IsRunning, $"the refill timer thread was running after a boundary ({started}) and is gone after {RefillTimer.IdleExitMs + 400} ms idle ({!RefillTimer.IsRunning}); started {RefillTimer.ThreadStarts} time(s) in this process");
	}

	static bool Throws(Action action)
	{
		try { action(); }
		catch (InvalidOperationException) { return true; }
		return false;
	}
}
