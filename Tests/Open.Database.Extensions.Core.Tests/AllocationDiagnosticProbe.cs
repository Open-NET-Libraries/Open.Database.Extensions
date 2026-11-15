using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Globalization;
using System.Runtime;
using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Tests;

#nullable enable

#pragma warning disable xUnit1051 // The probe measures calls with no token and with one.

// DIAGNOSTIC, not a test of anything: it prints, and always passes.
// Question: is the per-call allocation excess seen on CI present on every
// call, or does it disappear once the process has settled?
// Every line it prints starts with "DIAG> ".
// It runs the same way the token allocation tests measure (four variants of a
// call that take turns, the bytes of each call counted on this thread), on
// three channel shapes, in two blocks: right after a short warm-up ("cold"),
// and after a settle phase ("settled").
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public sealed class AllocationDiagnosticProbe(ITestOutputHelper output)
{
	const int Rounds = 15;
	const int CallsPerRound = 1000;
	const int TickCalls = 20000;

	static readonly string[] Shapes = ["Transform", "AsyncPool", "Plain"];
	static readonly string[] Variants = ["none", "call", "cmd", "both"];

	static readonly Func<IDataRecord, TokenItem> ToItem = static r
		=> new TokenItem { Id = r.GetInt32(0) };

	void P(string line)
	{
		string text = "DIAG> " + line;
		output.WriteLine(text);
		try { TestContext.Current.SendDiagnosticMessage(text); }
		catch (Exception) { /* the diagnostic message is an extra. */ }
	}

	static string F(double value)
		=> value.ToString("0.##", CultureInfo.InvariantCulture);

	static Task<long> CallChannel(
		string shape,
		IExecuteReaderAsync c,
		System.Threading.Channels.Channel<object[]> objects,
		System.Threading.Channels.Channel<TokenItem> items,
		CancellationToken token)
		=> shape switch
		{
			"Plain" => c.ToChannel(objects.Writer, false, token).AsTask(),
			"Transform" => c
				.ToChannel(items.Writer, false, ToItem, token)
				.AsTask(),
			"AsyncPool" => c
				.ToChannelAsync(
					objects.Writer,
					false,
					ArrayPool<object>.Shared,
					token)
				.AsTask(),
			_ => throw new ArgumentOutOfRangeException(nameof(shape)),
		};

	// The variants, in this order: no token, a call token only,
	// a command token only (the reference), both.
	static Action[] MakeVariants(string shape, CancellationToken live)
	{
		Action Make(CancellationToken commandToken, CancellationToken callToken)
		{
			ExpressiveDbCommand cmd = new(
				new TokenConnection(
					new TokenRig { Recording = false, Rows = 0 },
					ConnectionState.Open),
				CommandType.Text,
				"SELECT Id");
			cmd.UseCancellationToken(commandToken);
			var objects = System.Threading.Channels.Channel
				.CreateUnbounded<object[]>();
			var items = System.Threading.Channels.Channel
				.CreateUnbounded<TokenItem>();
			return () => CallChannel(shape, cmd, objects, items, callToken)
				.GetAwaiter()
				.GetResult();
		}

		return
		[
			Make(default, default),
			Make(default, live),
			Make(live, default),
			Make(live, live),
		];
	}

	static void MeasureRound(Action[] calls, long[] bytes, int round)
	{
		Array.Clear(bytes);
		for (int i = 0; i < CallsPerRound; i++)
		{
			for (int j = 0; j < calls.Length; j++)
			{
				int c = (i + j + round) % calls.Length;
				long before = GC.GetAllocatedBytesForCurrentThread();
				calls[c]();
				bytes[c] += GC.GetAllocatedBytesForCurrentThread() - before;
			}
		}
	}

	static double Median(List<double> values)
	{
		double[] sorted = [.. values];
		Array.Sort(sorted);
		return sorted[sorted.Length / 2];
	}

	// One block: Rounds rounds, one line each, in order.
	void Block(string label, string shape, Action[] calls)
	{
		long[] bytes = new long[calls.Length];
		List<double> none = [], call = [], both = [];
		List<double>[] levels = [.. calls.Select(_ => new List<double>())];
		for (int round = 0; round < Rounds; round++)
		{
			int gc0 = GC.CollectionCount(0);
			long jit0 = JitInfo.GetCompiledMethodCount();
			long start = Stopwatch.GetTimestamp();
			MeasureRound(calls, bytes, round);
			double ms = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
			double[] v = [.. bytes.Select(b => b / (double)CallsPerRound)];
			for (int c = 0; c < v.Length; c++)
				levels[c].Add(v[c]);
			none.Add(v[0] - v[2]);
			call.Add(v[1] - v[2]);
			both.Add(v[3] - v[2]);
			P($"{label} shape={shape} round={round + 1:00} bytesPerCall none={F(v[0])} call={F(v[1])} cmd={F(v[2])} both={F(v[3])} diffVsCmd none={F(none[^1])} call={F(call[^1])} both={F(both[^1])} ms={F(ms)} gc0={GC.CollectionCount(0) - gc0} jitMethods={JitInfo.GetCompiledMethodCount() - jit0}");
		}

		for (int c = 0; c < levels.Length; c++)
			P($"{label} shape={shape} variant={Variants[c]} bytesPerCall over {Rounds} rounds: min={F(levels[c].Min())} median={F(Median(levels[c]))} max={F(levels[c].Max())}");

		P($"{label} shape={shape} medianOfDiffs none={F(Median(none))} call={F(Median(call))} both={F(Median(both))} (the test fails above 8)");
	}

	// Rounds until two successive rounds agree on the cmd variant's level
	// (within one byte per call), or a cap.
	void Settle(string shape, Action[] calls)
	{
		long[] bytes = new long[calls.Length];
		double previous = double.NaN;
		for (int round = 0; round < 40; round++)
		{
			MeasureRound(calls, bytes, round);
			double level = bytes[2] / (double)CallsPerRound;
			bool agreed = Math.Abs(level - previous) <= 1;
			P($"settle shape={shape} round={round + 1:00} cmd={F(level)} agreedWithPrevious={agreed}");
			if (agreed)
				return;
			previous = level;
		}

		P($"settle shape={shape} did not agree in 40 rounds");
	}

	sealed class TickListener : EventListener
	{
		public readonly Dictionary<string, long> Ticks = [];

		protected override void OnEventSourceCreated(EventSource source)
		{
			if (source.Name == "Microsoft-Windows-DotNETRuntime")
				EnableEvents(source, EventLevel.Verbose, (EventKeywords)0x1);
		}

		protected override void OnEventWritten(EventWrittenEventArgs e)
		{
			if (e.EventName is null
				|| !e.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal)
				|| e.PayloadNames is null
				|| e.Payload is null)
			{
				return;
			}

			int i = e.PayloadNames.IndexOf("TypeName");
			string type = i < 0 ? "?" : e.Payload[i]?.ToString() ?? "?";
			int generic = type.IndexOf('[');
			if (generic > 0)
				type = type[..generic];

			lock (Ticks)
				Ticks[type] =Ticks.GetValueOrDefault(type) + 1;
		}
	}

	// GCAllocationTick events (about one per 100 KB allocated) name the type
	// the allocation that crossed the line was of: counted per variant, one
	// variant at a time, after the settle phase.
	void Ticks(string shape, Action[] calls)
	{
		try
		{
			using var listener = new TickListener();
			Thread.Sleep(300);
			for (int c = 0; c < calls.Length; c++)
			{
				lock (listener.Ticks)
					listener.Ticks.Clear();
				long before = GC.GetAllocatedBytesForCurrentThread();
				for (int i = 0; i < TickCalls; i++)
					calls[c]();
				long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
				Thread.Sleep(300);
				string top;
				lock (listener.Ticks)
				{
					top = string.Join(
						" ",
						listener.Ticks
							.OrderByDescending(kv => kv.Value)
							.Take(12)
							.Select(kv => $"{kv.Key}={kv.Value}"));
				}
				P($"ticks shape={shape} variant={Variants[c]} bytesPerCall={F(bytes / (double)TickCalls)} types: {(top.Length == 0 ? "(no events)" : top)}");
			}
		}
		catch (Exception ex)
		{
			P($"GCAllocationTick skipped: {ex.GetType().Name}: {ex.Message}");
		}
	}

	void Header()
	{
		P($"runtime={RuntimeInformation.FrameworkDescription} version={Environment.Version} os={RuntimeInformation.OSDescription} arch={RuntimeInformation.ProcessArchitecture}");
		P($"processorCount={Environment.ProcessorCount} serverGC={GCSettings.IsServerGC} gcLatencyMode={GCSettings.LatencyMode} threadPoolThreads={ThreadPool.ThreadCount}");
#if DEBUG
		const string config = "Debug";
#else
		const string config = "Release";
#endif
		P($"testAssemblyConfiguration={config}");
		string[] switches =
		[
			"System.Runtime.TieredCompilation",
			"System.Runtime.TieredPGO",
			"System.Runtime.TieredCompilation.QuickJit",
			"System.Runtime.TieredCompilation.QuickJitForLoops",
			"System.Runtime.TieredCompilation.BackgroundWorkerTimeoutMs",
			"System.GC.Server",
			"System.GC.Concurrent",
		];
		P("appContext: " + string.Join(
			" ",
			switches.Select(s => $"{s}={AppContext.GetData(s) ?? "(unset)"}")));
		var env = Environment.GetEnvironmentVariables()
			.Cast<System.Collections.DictionaryEntry>()
			.Select(e => (Name: e.Key.ToString() ?? "", Value: e.Value?.ToString() ?? ""))
			.Where(e => e.Name.StartsWith("DOTNET_", StringComparison.OrdinalIgnoreCase)
				|| e.Name.StartsWith("COMPlus_", StringComparison.OrdinalIgnoreCase))
			.OrderBy(e => e.Name, StringComparer.OrdinalIgnoreCase)
			.Select(e => $"{e.Name}={e.Value}")
			.ToArray();
		P("env (DOTNET_*, COMPlus_*): " + (env.Length == 0 ? "(none set)" : string.Join(" ", env)));
	}

	[Fact]
	public void Probe()
	{
		Header();
#if DEBUG && NET11_0_OR_GREATER
		using var cts = new CancellationTokenSource();
		var rigs = Shapes.ToDictionary(s => s, s => MakeVariants(s, cts.Token));

		P("BLOCK COLD begins: after a 300-call warm-up per variant, the first rounds of each shape");
		foreach (string shape in Shapes)
		{
			foreach (Action call in rigs[shape])
			{
				for (int i = 0; i < 300; i++)
					call();
			}

			Block("cold", shape, rigs[shape]);
		}

		P("SETTLE begins: a 2 s warm loop over every shape and variant, then rounds until two successive agree");
		long start = Stopwatch.GetTimestamp();
		int turn = 0;
		while (Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(2))
		{
			foreach (string shape in Shapes)
			{
				foreach (Action call in rigs[shape])
				{
					for (int i = 0; i < 200; i++)
						call();
				}
			}

			turn++;
		}

		P($"SETTLE warm loop done: {turn} turns, jitMethodsSoFar={JitInfo.GetCompiledMethodCount()}");
		foreach (string shape in Shapes)
			Settle(shape, rigs[shape]);

		P("BLOCK SETTLED begins: the same rounds, after the settle phase");
		foreach (string shape in Shapes)
			Block("settled", shape, rigs[shape]);

		P("TICKS begin: per variant, after the settle phase");
		foreach (string shape in Shapes)
			Ticks(shape, rigs[shape]);

		P("END");
#else
		P("inert on this leg: the probe runs on Debug net11.0 only");
#endif
	}
}
