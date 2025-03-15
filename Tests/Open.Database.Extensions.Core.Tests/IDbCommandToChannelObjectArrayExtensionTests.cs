namespace Open.Database.Extensions.Tests;

[ExcludeFromCodeCoverage]
public static class IDbCommandToChannelObjectArrayExtensionTests
{
	[Fact]
	public static async Task WithValidParameters_NoRecords()
	{
		// Arrange
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		var reader = Substitute.For<IDataReader>();
		var c = Channel.CreateUnbounded<object[]>();
		var cWriter = c.Writer;
		var cReader = c.Reader;
		var cancellationToken = new CancellationToken();

		command.Connection.Returns(connection);
		connection.State.Returns(ConnectionState.Open);
		command.ExecuteReader(Arg.Any<CommandBehavior>())
			.Returns(reader);

		reader.Read().Returns(false); // Simulate no records

		// Act
		long result = await command.ToChannel(cWriter, true, cancellationToken);

		// Assert
		Assert.Equal(0, result); // Assuming the reader has no records

		await Task.Yield();
		Assert.True(cReader.Completion.IsCompleted);
	}

	[Fact]
	public static async Task WithValidParameters_WithRecords()
	{
		// Arrange
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		var reader = Substitute.For<IDataReader>();

		command.Connection.Returns(connection);
		connection.State.Returns(ConnectionState.Open);
		command.ExecuteReader(Arg.Any<CommandBehavior>())
			.Returns(reader);

		reader.FieldCount.Returns(4); // Simulate valid FieldCount
		reader.Read().Returns(true, true, false); // Simulate 2 records then no more records

		reader
			.When(x => x.GetValues(Arg.Any<object[]>()))
			.Do(callInfo =>
			{
				object[] values = callInfo.Arg<object[]>();
				values[0] = "Value1";
				values[1] = "Value2";
				values[2] = 0;
				values[3] = DBNull.Value;
			});

		int count = 0;
		await foreach (object[] record in command.ToChannel(true).ReadAllAsync())
		{
			count++;
			Assert.Equal("Value1", record[0]);
			Assert.Equal("Value2", record[1]);
			Assert.Equal(0, record[2]);
			Assert.Equal(DBNull.Value, record[3]);
		}

		Assert.Equal(2, count);
	}

	[Fact]
	public static async Task WithNullCommand_ThrowsArgumentNullException()
	{
		// Arrange
		IDbCommand command = null;
		var writer = Channel.CreateUnbounded<object[]>().Writer;

		// Act & Assert
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await command.ToChannel(writer, true));
	}

	[Fact]
	public static async Task WithNullWriter_ThrowsArgumentNullException()
	{
		// Arrange
		var command = Substitute.For<IDbCommand>();

		// Act & Assert
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await command.ToChannel(null, true));
	}
}