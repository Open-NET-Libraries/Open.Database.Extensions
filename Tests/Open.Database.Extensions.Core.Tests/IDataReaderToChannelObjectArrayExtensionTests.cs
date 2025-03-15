namespace Open.Database.Extensions.Tests;

[ExcludeFromCodeCoverage]
public static class IDataReaderToChannelObjectArrayExtensionTests
{
	[Fact]
	public static async Task WithValidParameters_WritesDataToChannel()
	{
		// Arrange
		IDataReader mockReader = Substitute.For<IDataReader>();

		mockReader.Read().Returns(true, false);
		mockReader.FieldCount.Returns(1);
		mockReader.GetValue(Arg.Any<int>()).Returns("test");

		// Act
		long result = await mockReader.ToChannel(true).ReadAll(static _ => { });

		// Assert
		Assert.Equal(1, result);
	}

	[Fact]
	public static async Task WithValidParametersAndTransform_WritesTransformedDataToChannel()
	{
		// Arrange
		IDataReader mockReader = Substitute.For<IDataReader>();

		mockReader.Read().Returns(true, false);
		mockReader.FieldCount.Returns(1);
		mockReader.GetValue(Arg.Any<int>()).Returns("test");

		static string transform(IDataRecord record) => record.GetValue(0).ToString();

		// Act
		long result = await mockReader.ToChannel(true, transform).ReadAll(static _ => { });

		// Assert
		Assert.Equal(1, result);
	}

	[Fact]
	public static async Task WithValidParameters_NoRecords()
	{
		// Arrange
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		var reader = Substitute.For<IDataReader>();

		command.Connection.Returns(connection);
		connection.State.Returns(ConnectionState.Open);
		command.ExecuteReader(Arg.Any<CommandBehavior>())
			.Returns(reader);

		reader.Read().Returns(false); // Simulate no records

		// Act
		long result = await command.ToChannel(true).ReadAll(static _ => { });

		// Assert
		Assert.Equal(0, result); // Assuming the reader has no records
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
}