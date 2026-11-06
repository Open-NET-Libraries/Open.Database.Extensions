using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;

namespace Open.Database.Extensions.Benchmarks;

// What it costs to hold a RowMap<T>: the four ways to hold one, each read 1000 times warm (bytes per read, time per read),
// and what building one costs (the dominant part of the first call, and what a program that built all of its
// mappings up front would pay for each one).
[MemoryDiagnoser]
public class LazyMapBenchmarks
{
	const int Reads = 1000;

	static class Hot
	{
		public static readonly RowMap<RenamedPost> Map = new RowMap<RenamedPost>(RenamedColumns.Pairs);
	}

	static class Coalescing
	{
		public static RowMap<RenamedPost> Map => field ??= new RowMap<RenamedPost>(RenamedColumns.Pairs);
	}

	static class Ensured
	{
		public static RowMap<RenamedPost> Map => LazyInitializer.EnsureInitialized(ref field, static () => new RowMap<RenamedPost>(RenamedColumns.Pairs));
	}

	static readonly Lazy<RowMap<RenamedPost>> Wrapped = new(static () => new RowMap<RenamedPost>(RenamedColumns.Pairs));

	// A call the JIT cannot fold away, so every variant pays the same for consuming the mapping and the differences are the reads.
	[MethodImpl(MethodImplOptions.NoInlining)]
	static RowMap<RenamedPost> Consume(RowMap<RenamedPost> map) => map;

	[GlobalSetup]
	public void Warm()
	{
		_ = Hot.Map;
		_ = Coalescing.Map;
		_ = Ensured.Map;
		_ = Wrapped.Value;
	}

	[Benchmark(Baseline = true, OperationsPerInvoke = Reads)]
	public RowMap<RenamedPost> StaticReadonly()
	{
		RowMap<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Hot.Map);
		return last;
	}

	[Benchmark(OperationsPerInvoke = Reads)]
	public RowMap<RenamedPost> FieldCoalesce()
	{
		RowMap<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Coalescing.Map);
		return last;
	}

	[Benchmark(OperationsPerInvoke = Reads)]
	public RowMap<RenamedPost> EnsureInitialized()
	{
		RowMap<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Ensured.Map);
		return last;
	}

	[Benchmark(OperationsPerInvoke = Reads)]
	public RowMap<RenamedPost> LazyValue()
	{
		RowMap<RenamedPost> last = null!;
		for (int i = 0; i < Reads; i++) last = Consume(Wrapped.Value);
		return last;
	}

	/// <summary>Building one 12-field mapping: the cost of a first call, whichever way it is held.</summary>
	[Benchmark]
	public RowMap<RenamedPost> BuildOneMapping() => new RowMap<RenamedPost>(RenamedColumns.Pairs);

	/// <summary>The extra objects a <see cref="Lazy{T}"/> adds per mapping (the <c>Lazy</c> and its internal helper; the factory delegate is a cached static lambda); the other ways add none.</summary>
	[Benchmark]
	public object CreateLazyWrapper() => new Lazy<RowMap<RenamedPost>>(static () => new RowMap<RenamedPost>(RenamedColumns.Pairs));
}
