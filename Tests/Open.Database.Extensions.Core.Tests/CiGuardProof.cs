namespace Open.Database.Extensions.Tests;

// Temporary: proves that CI runs the tests. Removed again by the next commit.
public class CiGuardProof
{
	[Fact]
	public void ThisTestFailsOnPurpose() => Assert.Fail("deliberate: proves CI runs the tests");
}
