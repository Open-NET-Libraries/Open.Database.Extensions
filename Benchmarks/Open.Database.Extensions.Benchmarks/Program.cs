using BenchmarkDotNet.Running;

// dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*" [--job short]
if (args.Contains("--channel-study"))
{
	await Open.Database.Extensions.Benchmarks.ChannelStudy.RunAsync();
	return;
}

if (args.Contains("--map-memory"))
{
	Open.Database.Extensions.Benchmarks.MapMemoryStudy.Run();
	return;
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
