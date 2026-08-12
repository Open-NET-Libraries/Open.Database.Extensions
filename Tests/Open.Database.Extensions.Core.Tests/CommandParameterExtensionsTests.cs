namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class CommandParameterExtensionsTests
{
	static (IDbCommand Command, IDbDataParameter Param, IDataParameterCollection Parameters) NewCommand()
	{
		var command = Substitute.For<IDbCommand>();
		var param = Substitute.For<IDbDataParameter>();
		var parameters = Substitute.For<IDataParameterCollection>();
		command.CreateParameter().Returns(param);
		command.Parameters.Returns(parameters);
		return (command, param, parameters);
	}

	[Fact]
	public static void AddParameter_SetsNameValueTypeAndAddsToCollection()
	{
		(IDbCommand command, IDbDataParameter param, IDataParameterCollection parameters) = NewCommand();

		IDbDataParameter result = command.AddParameter("@id", 5, DbType.Int32);

		Assert.Same(param, result);
		Assert.Equal("@id", param.ParameterName);
		Assert.Equal(5, param.Value);
		Assert.Equal(DbType.Int32, param.DbType);
		parameters.Received(1).Add(param);
	}

	[Fact]
	public static void AddParameterType_SetsTypeAndDirection()
	{
		(IDbCommand command, IDbDataParameter param, _) = NewCommand();

		command.AddParameterType("@when", DbType.DateTime, ParameterDirection.Input);

		Assert.Equal("@when", param.ParameterName);
		Assert.Equal(DbType.DateTime, param.DbType);
		Assert.Equal(ParameterDirection.Input, param.Direction);
	}

	[Fact]
	public static void AddReturnParameter_SetsReturnDirection()
	{
		(IDbCommand command, IDbDataParameter param, _) = NewCommand();

		command.AddReturnParameter(DbType.Int32, "ret");

		Assert.Equal(ParameterDirection.ReturnValue, param.Direction);
	}

	[Fact]
	public static void AddParameter_BlankName_Throws()
	{
		(IDbCommand command, _, _) = NewCommand();

		Assert.Throws<ArgumentException>(() => command.AddParameter(" ", 1));
	}
}
