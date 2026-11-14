using System.Diagnostics;
using System.Diagnostics.Tracing;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Reports;

namespace Open.Database.Extensions.Benchmarks;

// BS-RO: the ReaderOpen and Drain columns describe the iterations BenchmarkDotNet's Mean is made of, and no others.
//
// BenchmarkDotNet runs a case in stages: jitting, pilot, warm-up, then the WorkloadActual iterations (about 16,000 operations each).
// Its Mean is the mean of the WorkloadResult runs: the WorkloadActual iterations minus the outliers it removed, each less the overhead.
// How this reaches the same iterations, with no benchmark hook that changes the measurement:
//  1. BenchmarkDotNet's engine writes an EventSource event (BenchmarkDotNet.EngineEventSource) at the start and at the stop of every iteration,
//     outside the clock. The listener below turns the WorkloadActual pair into recorder.BeginMeasuredIteration / EndMeasuredIteration.
//     Readers that close between them are summed into that iteration (a flag read and two adds in ReaderClosed; nothing allocated).
//     Warm-up, pilot, jitting, the overhead iterations and the extra GC pass BenchmarkDotNet runs afterwards (it raises no event) never count.
//  2. At the end of the run the column gets BenchmarkDotNet's own report. The recorded iterations must be the report's WorkloadActual
//     measurements one for one (same count, same operations), otherwise the cell says so instead of a number.
//  3. The report's WorkloadResult runs are the actual iterations that survived outlier removal, each lowered by one constant (the overhead).
//     Each result is matched to its actual iteration (monotone, least total difference), and the match is refused unless the differences
//     are all the same constant. The cell is the mean, over those iterations, of reader-open time per operation, exactly as Mean is.

/// <summary>One WorkloadActual iteration as the recorder saw it: the operations it ran, and the reader-open and drain ticks of the readers closed inside it.</summary>
public readonly record struct MeasuredIteration(long Operations, long OpenTicks, long DrainTicks);

/// <summary>Reader-open and drain time per operation, microseconds, as the mean over the iterations of BenchmarkDotNet's Mean.</summary>
public readonly record struct MeasuredOpenSummary(int Iterations, double OpenUs, double DrainUs);

public static class MeasuredOpenTime
{
	// The match is refused when a result is not its actual iteration less the same constant, within this many nanoseconds per operation.
	const double SameOverheadNs = 1e-3;

	static double ToUsPerOperation(long ticks, long operations) => ticks * 1_000_000d / Stopwatch.Frequency / operations;

	/// <summary>Mean over all the recorded iterations (outliers not removed). For the log line, where there is no BenchmarkDotNet report yet.</summary>
	public static MeasuredOpenSummary MeanOfAll(IReadOnlyList<MeasuredIteration> iterations)
	{
		if (iterations.Count == 0) return default;
		double open = 0, drain = 0;
		foreach (var it in iterations)
		{
			open += ToUsPerOperation(it.OpenTicks, it.Operations);
			drain += ToUsPerOperation(it.DrainTicks, it.Operations);
		}

		return new MeasuredOpenSummary(iterations.Count, open / iterations.Count, drain / iterations.Count);
	}

	/// <summary>
	/// The mean of the recorded iterations that BenchmarkDotNet's Mean is made of. False (with the reason) when the recorded iterations are not
	/// the report's WorkloadActual measurements, or the results cannot be matched to them.
	/// </summary>
	public static bool TrySummarize(IReadOnlyList<MeasuredIteration> recorded, IReadOnlyList<Measurement> actual, IReadOnlyList<Measurement> results, out MeasuredOpenSummary summary, out string reason)
	{
		summary = default;
		if (recorded.Count == 0)
		{
			reason = "no WorkloadActual events seen";
			return false;
		}

		if (recorded.Count != actual.Count)
		{
			reason = $"{recorded.Count} iterations recorded, {actual.Count} reported";
			return false;
		}

		if (results.Count == 0 || results.Count > actual.Count)
		{
			reason = $"{results.Count} result runs for {actual.Count} iterations";
			return false;
		}

		for (int i = 0; i < actual.Count; i++)
		{
			if (recorded[i].Operations != actual[i].Operations)
			{
				reason = $"iteration {i + 1} ran {recorded[i].Operations} operations, reported {actual[i].Operations}";
				return false;
			}
		}

		if (!TryMatch(actual, results, out int[] picked))
		{
			reason = "results do not match the iterations";
			return false;
		}

		double open = 0, drain = 0;
		foreach (int i in picked)
		{
			open += ToUsPerOperation(recorded[i].OpenTicks, recorded[i].Operations);
			drain += ToUsPerOperation(recorded[i].DrainTicks, recorded[i].Operations);
		}

		summary = new MeasuredOpenSummary(picked.Length, open / picked.Length, drain / picked.Length);
		reason = "";
		return true;
	}

	/// <summary>For each result (in order), the index of the actual iteration it was made from.</summary>
	public static bool TryMatch(IReadOnlyList<Measurement> actual, IReadOnlyList<Measurement> results, out int[] picked)
	{
		int n = actual.Count, m = results.Count;
		picked = new int[m];
		var cost = new double[m + 1, n + 1];
		for (int k = 1; k <= m; k++) cost[k, 0] = double.PositiveInfinity;

		for (int k = 1; k <= m; k++)
		{
			for (int j = 1; j <= n; j++)
			{
				double take = actual[j - 1].Operations == results[k - 1].Operations
					? cost[k - 1, j - 1] + Math.Abs((actual[j - 1].Nanoseconds - results[k - 1].Nanoseconds) / actual[j - 1].Operations)
					: double.PositiveInfinity;
				cost[k, j] = Math.Min(cost[k, j - 1], take);
			}
		}

		if (double.IsInfinity(cost[m, n])) return false;
		for (int k = m, j = n; k > 0; j--)
		{
			if (cost[k, j] == cost[k, j - 1]) continue; // skipped: an outlier BenchmarkDotNet removed
			picked[--k] = j - 1;
		}

		double first = Difference(actual[picked[0]], results[0]);
		for (int k = 1; k < m; k++)
		{
			if (Math.Abs(Difference(actual[picked[k]], results[k]) - first) > SameOverheadNs) return false;
		}

		return true;
	}

	static double Difference(Measurement actual, Measurement result) => (actual.Nanoseconds - result.Nanoseconds) / actual.Operations;
}

/// <summary>
/// Listens for BenchmarkDotNet's per-iteration events (in this process, so for an in-process run) and tells the attached recorder
/// where a WorkloadActual iteration begins and ends. The events are written by the engine outside its clock.
/// </summary>
public sealed class SyntheticIterationListener : EventListener
{
	static readonly Lock Gate = new();
	static SyntheticIterationListener? _instance;
	static volatile SyntheticRecorder? _active;

	/// <summary>Makes the recorder the one the iteration events are written to; starts the listener on first use.</summary>
	public static void Attach(SyntheticRecorder recorder)
	{
		lock (Gate) _instance ??= new SyntheticIterationListener();
		_active = recorder;
	}

	public static void Detach(SyntheticRecorder recorder)
	{
		if (ReferenceEquals(_active, recorder)) _active = null;
	}

	protected override void OnEventSourceCreated(EventSource eventSource)
	{
		if (eventSource.Name == EngineEventSource.SourceName) EnableEvents(eventSource, EventLevel.Informational, EventKeywords.All);
	}

	protected override void OnEventWritten(EventWrittenEventArgs eventData)
	{
		if (eventData.EventSource.Name != EngineEventSource.SourceName) return;
		long operations = eventData.Payload is { Count: > 0 } payload && payload[0] is { } first ? Convert.ToInt64(first) : 0;
		Handle(eventData.EventId, operations);
	}

	static void Handle(int eventId, long operations)
	{
		var recorder = _active;
		if (recorder is null) return;
		if (eventId == EngineEventSource.WorkloadActualStartEventId) recorder.BeginMeasuredIteration();
		else if (eventId == EngineEventSource.WorkloadActualStopEventId) recorder.EndMeasuredIteration(operations);
	}
}
