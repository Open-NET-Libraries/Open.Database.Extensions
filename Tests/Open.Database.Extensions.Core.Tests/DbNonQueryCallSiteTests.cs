using System.Reflection;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The call sites of Open.Database.Extensions.WriteScalarCompat (Program.cs): that project building is the compile check,
// and its entry point returns the number of checks that failed on SQLite in memory.
[ExcludeFromCodeCoverage]
public static class DbNonQueryCallSiteTests
{
	[Fact]
	public static void TheFrozenCallSites_CompileAndPass()
	{
		MethodInfo main = Assembly
			.Load("Open.Database.Extensions.WriteScalarCompat")
			.EntryPoint!;

		Assert.Equal(0, main.Invoke(null, null));
	}
}
