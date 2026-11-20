namespace Open.Database.Extensions.Benchmarks;

// What the linked synthetic provider records per measured iteration; the benchmark project defines it beside the benchmarks, which are not linked.
public readonly record struct MeasuredIteration(long Operations, long OpenTicks, long DrainTicks);
