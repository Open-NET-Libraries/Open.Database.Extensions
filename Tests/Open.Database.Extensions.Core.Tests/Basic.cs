namespace Open.Database.Extensions.Core.Tests;

public class Basic
{
	[Fact]
	public void ExpressiveCommandValidation()
	{
		var factory = DbConnectionFactory.Create(() => Substitute.For<IDbConnection>());

		Assert.Throws<ArgumentNullException>(() => factory.Command(null));
		Assert.Throws<ArgumentException>(() => factory.Command(string.Empty));
	}

	[SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Compile test.")]
	static async Task AmbiguityValidation(IDbCommand command)
		=> await command.Connection.EnsureOpenAsync();
}
