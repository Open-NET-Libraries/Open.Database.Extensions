using BenchmarkDotNet.Running;
using Open.Database.Extensions.Benchmarks;

// dotnet run -c Release --project Benchmarks/Open.Database.Extensions.Benchmarks -- --filter "*" [--job short]
//
// The synthetic suite (SyntheticBenchmarks) also takes --widths 1,5,20 --rows 1,1000 --row-cost 0 --extra-sets 0 --next-result-cost 0
// to narrow its parameters, and runs in process (--inprocess) to fill its ReaderOpen and Drain columns.
// Checks that are not benchmarks:
//   -- --verify                 equal final objects from every variant, the provider's switches and read paths
//   -- --selftest-open-time     a delay inside an open reader shows in reader-open time; after the close it does not
//   -- --selftest-pace          consumer pace (--pace N) grows reader-open for the inline variants, not for V5 and V6d
//   -- --selftest-alloc         the typed-getter loop allocates 0 B per row
// The checks say which runtime and target framework they ran on (the build is multi-targeted).
if (args.Any(static a => a is "--verify" or "--selftest-open-time" or "--selftest-alloc" or "--selftest-pace"))
{
	Console.WriteLine("runtime: " + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription
		+ ", built for: " + System.Reflection.Assembly.GetExecutingAssembly().GetCustomAttributes(typeof(System.Runtime.Versioning.TargetFrameworkAttribute), false)
			.Cast<System.Runtime.Versioning.TargetFrameworkAttribute>().First().FrameworkName);
}

if (args.Contains("--verify"))
{
	Environment.ExitCode = SyntheticChecks.Verify();
	return;
}

if (args.Contains("--selftest-open-time"))
{
	Environment.ExitCode = SyntheticChecks.SelfTestOpenTime();
	return;
}

if (args.Contains("--selftest-pace"))
{
	Environment.ExitCode = SyntheticChecks.SelfTestPace();
	return;
}

if (args.Contains("--selftest-alloc"))
{
	Environment.ExitCode = SyntheticChecks.SelfTestAlloc();
	return;
}

if (args.Contains("--channel-study"))
{
	await ChannelStudy.RunAsync();
	return;
}

if (args.Contains("--map-memory"))
{
	MapMemoryStudy.Run();
	return;
}

// Our parameter options go to the parameter sources through environment variables; BenchmarkDotNet does not know them.
var rest = new List<string>();
for (int i = 0; i < args.Length; i++)
{
	if (SyntheticParams.TryGetVariable(args[i], out string variable) && i + 1 < args.Length)
		Environment.SetEnvironmentVariable(variable, args[++i]);
	else
		rest.Add(args[i]);
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run([.. rest]);
