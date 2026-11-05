using BenchmarkDotNet.Running;

// dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*" [--job short]
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
