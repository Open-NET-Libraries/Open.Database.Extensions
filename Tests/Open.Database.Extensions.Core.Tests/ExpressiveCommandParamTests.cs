namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class ExpressiveCommandParamTests
{
	static ExpressiveCommand NewCommand()
		=> DbConnectionFactory
			.Create(() => Substitute.For<IDbConnection>())
			.Command("SELECT 1");

	[Fact]
	public static void Command_SetsTypeAndText()
	{
		var factory = DbConnectionFactory.Create(() => Substitute.For<IDbConnection>());

		ExpressiveCommand text = factory.Command("SELECT 1");
		Assert.Equal(CommandType.Text, text.Type);
		Assert.Equal("SELECT 1", text.Command);

		ExpressiveCommand proc = factory.StoredProcedure("do_thing");
		Assert.Equal(CommandType.StoredProcedure, proc.Type);
		Assert.Equal("do_thing", proc.Command);
	}

	[Fact]
	public static void AddParam_AppendsInOrder_WithValues()
	{
		ExpressiveCommand cmd = NewCommand()
			.AddParam("a", 1)
			.AddParam("b", "x");

		Assert.Equal(2, cmd.Params.Count);

		Assert.Equal("a", cmd.Params[0].Name);
		Assert.Equal(1, cmd.Params[0].Value);

		Assert.Equal("b", cmd.Params[1].Name);
		Assert.Equal("x", cmd.Params[1].Value);
	}

	[Fact]
	public static void AddParam_NullInputValue_BecomesDBNull()
	{
		ExpressiveCommand cmd = NewCommand().AddParam("a", (object?)null);

		Assert.Single(cmd.Params);
		Assert.Equal(DBNull.Value, cmd.Params[0].Value);
	}

	[Fact]
	public static void AddReturnParam_SetsReturnDirection()
	{
		ExpressiveCommand cmd = NewCommand().AddReturnParam("ret");

		Assert.Single(cmd.Params);
		Assert.Equal(ParameterDirection.ReturnValue, cmd.Params[0].Direction);
	}

	[Fact]
	public static void AddParamIf_OnlyAddsWhenConditionTrue()
	{
		ExpressiveCommand cmd = NewCommand()
			.AddParamIf(false, "skip", 1)
			.AddParamIf(true, "keep", 2);

		Assert.Single(cmd.Params);
		Assert.Equal("keep", cmd.Params[0].Name);
	}

	[Fact]
	public static void SetTimeout_UpdatesTimeout()
	{
		ExpressiveCommand cmd = NewCommand().SetTimeout(45);
		Assert.Equal(45, cmd.Timeout);
	}
}
