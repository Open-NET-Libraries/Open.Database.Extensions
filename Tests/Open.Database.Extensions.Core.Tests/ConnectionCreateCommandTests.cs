namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class ConnectionCreateCommandTests
{
	[Fact]
	public static void CreateCommand_ConfiguresTypeTextAndTimeout()
	{
		var probe = new ConnectionProbe();

		IDbCommand command = probe.Connection.CreateCommand(CommandType.TableDirect, "my_table", 17);

		Assert.Same(probe.Command, command);
		Assert.Equal(CommandType.TableDirect, command.CommandType);
		Assert.Equal("my_table", command.CommandText);
		Assert.Equal(17, command.CommandTimeout);
	}

	[Fact]
	public static void CreateTextCommand_And_CreateStoredProcedureCommand_SelectTheMatchingType()
	{
		var text = new ConnectionProbe();
		var proc = new ConnectionProbe();

		Assert.Equal(CommandType.Text, text.Connection.CreateTextCommand("SELECT 1").CommandType);
		Assert.Equal(CommandType.StoredProcedure, proc.Connection.CreateStoredProcedureCommand("do_thing").CommandType);
		Assert.Equal("do_thing", proc.Command.CommandText);
	}

	[Fact]
	public static void CreateCommand_OpensNothing()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);

		probe.Connection.CreateTextCommand("SELECT 1");

		// Only the command is created; opening is the caller's (or the execute helpers') decision.
		Assert.Equal("CreateCommand", probe.Log);
	}

	[Theory]
	[InlineData("")]
	[InlineData("   ")]
	public static void CreateCommand_BlankText_ThrowsArgumentException_BeforeCreatingACommand(string text)
	{
		var probe = new ConnectionProbe();

		ArgumentException ex = Assert.Throws<ArgumentException>(
			() => probe.Connection.CreateCommand(CommandType.Text, text));

		Assert.Equal("commandText", ex.ParamName);
		Assert.Equal(string.Empty, probe.Log);
	}

	[Fact]
	public static void CreateCommand_NullArguments_ThrowArgumentNull()
	{
		var probe = new ConnectionProbe();

		Assert.Equal("connection", Assert.Throws<ArgumentNullException>(
			() => ((IDbConnection)null!).CreateCommand(CommandType.Text, "SELECT 1")).ParamName);
		Assert.Equal("commandText", Assert.Throws<ArgumentNullException>(
			() => probe.Connection.CreateCommand(CommandType.Text, null!)).ParamName);
	}
}
