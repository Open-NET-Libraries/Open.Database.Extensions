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

	// A literal 0 converts to any enum, so it used to bind to the type-only overload and add a parameter with no value.
	[Fact]
	public static void AddParam_LiteralZero_IsTheValue()
	{
		ExpressiveCommand cmd = NewCommand()
			.AddParam("i", 0)
			.AddParam("l", 0L)
			.AddParamIf(true, "j", 0)
			.AddParamIf(true, "k", 0L);

		Assert.Equal([0, 0L, 0, 0L], cmd.Params.Select(p => p.Value));
		Assert.All(cmd.Params, p => Assert.False(p.Type.HasValue));
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

	// AddParam("name", someString) uses the name written. Compilers older than C# 13 ignore [OverloadResolutionPriority], so they bound the deriving overload
	// AddParam<T>(T value, string? expression) with value = "name" and expression = the string, and sent the pair swapped (ann = author). These tests run at the
	// latest language version and guard the overloads themselves; Tests/Open.Database.Extensions.LegacyCompiler.Tests compiles the same calls with older language versions.
	static string Sent(ExpressiveCommand cmd)
		=> string.Join(", ", cmd.Params.Select(p => p.Name + "=" + (p.Value is DBNull ? "<null>" : p.Value)));

	[Fact]
	public static void AddParam_StringValue_UsesTheNameGiven_NeverTheValue()
	{
		const string s = "ann";
		const string? maybe = null;
		const string name = "author";

		Assert.Equal("author=ann", Sent(NewCommand().AddParam("author", "ann")));
		Assert.Equal("author=ann", Sent(NewCommand().AddParam("author", s)));
		Assert.Equal("author=<null>", Sent(NewCommand().AddParam("author", maybe)));
		Assert.Equal("author=<null>", Sent(NewCommand().AddParam("author", null)));
		Assert.Equal("author=ann", Sent(NewCommand().AddParam(name, s)));
		Assert.Equal("author=ann", Sent(NewCommand().AddParam(name: "author", value: s)));
	}

	[Fact]
	public static void AddParamIf_StringValue_UsesTheNameGiven_NeverTheValue()
	{
		const string s = "ann";
		const string? maybe = null;

		Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(true, "author", "ann")));
		Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(true, "author", s)));
		Assert.Equal("author=<null>", Sent(NewCommand().AddParamIf(true, "author", maybe)));
		Assert.Equal("author=<null>", Sent(NewCommand().AddParamIf(true, "author", null)));
		Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(condition: true, name: "author", value: s)));
		Assert.Equal("", Sent(NewCommand().AddParamIf(false, "author", "ann")));
		Assert.Equal("", Sent(NewCommand().AddParamIf(false, "author", s)));
	}

	[Fact]
	public static void AddParam_OtherValues_UseTheNameGiven()
	{
		object o = "ann";

		Assert.Equal("author=5", Sent(NewCommand().AddParam("author", 5)));
		Assert.Equal("author=5", Sent(NewCommand().AddParam("author", 5L)));
		Assert.Equal("author=5", Sent(NewCommand().AddParam("author", (int?)5)));
		Assert.Equal("author=ann", Sent(NewCommand().AddParam("author", o)));
		Assert.Equal("author=5", Sent(NewCommand().AddParamIf(true, "author", 5)));
		Assert.Equal("author=ann", Sent(NewCommand().AddParamIf(true, "author", o)));
	}
}
