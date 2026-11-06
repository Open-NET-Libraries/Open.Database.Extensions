using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;

namespace Open.Database.Extensions.Benchmarks;

// What it costs to hold a RowPlan<T>: the four ways to hold one, each read 1000 times warm (bytes per read, time per read),
// and what building one costs (the dominant part of the first call, and what a program that built all of its
// mappings up front would pay for each one).
[MemoryDiagnoser]
public class LazyMapBenchmarks
{
	const int Reads = 1000;

	static class Hot
	{
		public static readonly RowPlan<RenamedPost> Map = (RowPlan<RenamedPost>)RenamedColumns.Pairs;
	}

	static class Coalescing
	{
		public static RowPlan<RenamedPost> Map => field ??= (RowPlan<RenamedPost>)RenamedColumns.Pairs;
	}

	static class Ensured
	{
		public static RowPlan<RenamedPost> Map => LazyInitializer.EnsureInitialized(ref field, static () => (RowPlan<RenamedPost>)RenamedColumns.Pairs);
	}

	static readonly Lazy<RowPlan<RenamedPost>> Wrapped = new(static () => (RowPlan<RenamedPost>)RenamedColumns.Pairs);

	// A call the JIT cannot fold away, so every variant pays the same for consuming the mapping and the differences are the reads.
	[MethodImpl(MethodImplOptions.NoInlining)]
	static RowPlan<RenamedPost> Consume(RowPlan<RenamedPost> map) => map;

	[GlobalSetup]
	public void Warm()
	{
		_ = Hot.Map;
		_ = Coalescing.Map;
		_ = Ensured.Map;
		_ = Wrapped.Value;
	}

	[Benchmark(Baseline = true, OperationsPerInvoke = Reads)]
	public RowPlan<RenamedPost> StaticReadonly()
	{
		RowPlan<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Hot.Map);
		return last;
	}

	[Benchmark(OperationsPerInvoke = Reads)]
	public RowPlan<RenamedPost> FieldCoalesce()
	{
		RowPlan<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Coalescing.Map);
		return last;
	}

	[Benchmark(OperationsPerInvoke = Reads)]
	public RowPlan<RenamedPost> EnsureInitialized()
	{
		RowPlan<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Ensured.Map);
		return last;
	}

	[Benchmark(OperationsPerInvoke = Reads)]
	public RowPlan<RenamedPost> LazyValue()
	{
		RowPlan<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Wrapped.Value);
		return last;
	}

	/// <summary>Building one 12-field mapping: the cost of a first call, whichever way it is held.</summary>
	[Benchmark]
	public RowPlan<RenamedPost> BuildOneMapping() => (RowPlan<RenamedPost>)RenamedColumns.Pairs;

	/// <summary>The extra objects a <see cref="Lazy{T}"/> adds per mapping (the <c>Lazy</c> and its internal helper; the factory delegate is a cached static lambda); the other ways add none.</summary>
	[Benchmark]
	public object CreateLazyWrapper() => new Lazy<RowPlan<RenamedPost>>(static () => (RowPlan<RenamedPost>)RenamedColumns.Pairs);
}
