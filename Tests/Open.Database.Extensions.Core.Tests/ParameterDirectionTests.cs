namespace Open.Database.Extensions.Tests;

#nullable enable

// The direction of a parameter reaches the provider's command on every command type. The SQL Server command always did; the generic commands (ExpressiveCommand over an IDbConnection,
// ExpressiveDbCommand over a DbConnection) sent every parameter as an input, so a stored procedure with an output parameter failed with "expects parameter '@o', which was not supplied".
// ExpressiveCommand is read through the connection probe (a substitute remembers what is set on it), ExpressiveDbCommand through a command that records real DbParameter objects.
[ExcludeFromCodeCoverage]
public static class ParameterDirectionTests
{
	public static TheoryData<ParameterDirection> Directions
		=> [
			ParameterDirection.Input,
			ParameterDirection.Output,
			ParameterDirection.InputOutput,
			ParameterDirection.ReturnValue,
		];

	static ExpressiveDbCommand DbCommand(DerivedParameterNameTests.RecordingDbCommand recorder)
		=> new(new DerivedParameterNameTests.OpenDbConnection(recorder), CommandType.StoredProcedure, "dbo.p");

	// ---- every direction reaches the provider's parameter ----

	[Theory]
	[MemberData(nameof(Directions))]
	public static async Task ExpressiveDbCommand_SendsTheDirectionOfTheParameter_Async(ParameterDirection direction)
	{
		var recorder = new DerivedParameterNameTests.RecordingDbCommand();

		await DbCommand(recorder)
			.AddParam("@p", 5, DbType.Int32, direction)
			.ExecuteNonQueryAsync(
				cancellationToken: TestContext.Current.CancellationToken);

		Assert.Equal(direction, Assert.Single(recorder.Sent).Direction);
	}

	// ---- the usual procedure call: inputs, an output, an input-output and a return value, together ----

	[Fact]
	public static void AProcedureCallWithEveryKindOfParameter_SendsEachOneWithItsDirection()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var recorder = new DerivedParameterNameTests.RecordingDbCommand();

		new ExpressiveCommand(probe.Connection, CommandType.StoredProcedure, "dbo.p")
			.AddParam("@i", 1)
			.AddParam("@o", DbType.Int32, ParameterDirection.Output)
			.AddParam("@io", 3, DbType.Int32, ParameterDirection.InputOutput)
			.AddReturnParam("@r", DbType.Int32)
			.ExecuteNonQuery();

		DbCommand(recorder)
			.AddParam("@i", 1)
			.AddParam("@o", DbType.Int32, ParameterDirection.Output)
			.AddParam("@io", 3, DbType.Int32, ParameterDirection.InputOutput)
			.AddReturnParam("@r", DbType.Int32)
			.ExecuteNonQuery();

		ParameterDirection[] expected = [ParameterDirection.Input, ParameterDirection.Output, ParameterDirection.InputOutput, ParameterDirection.ReturnValue];
		Assert.Equal(["@i", "@o", "@io", "@r"], probe.Parameters.Select(p => p.ParameterName));
		Assert.Equal(expected, probe.Parameters.Select(p => p.Direction));
		Assert.Equal(["@i", "@o", "@io", "@r"], recorder.Sent.Select(p => p.ParameterName));
		Assert.Equal(expected, recorder.Sent.Select(p => p.Direction));
	}

	// ---- a NULL-valued output parameter still reaches the command ----

	[Fact]
	public static void ANullValuedOutputParameter_ReachesTheCommand_WithItsDirection()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var recorder = new DerivedParameterNameTests.RecordingDbCommand();

		new ExpressiveCommand(probe.Connection, CommandType.StoredProcedure, "dbo.p")
			.AddParam("@o", ParameterDirection.Output)
			.AddParam("@io", null, ParameterDirection.InputOutput)
			.AddReturnParam("@r")
			.ExecuteNonQuery();

		DbCommand(recorder)
			.AddParam("@o", ParameterDirection.Output)
			.AddParam("@io", null, ParameterDirection.InputOutput)
			.AddReturnParam("@r")
			.ExecuteNonQuery();

		Assert.Equal(["@o", "@io", "@r"], probe.Parameters.Select(p => p.ParameterName));
		Assert.Equal([ParameterDirection.Output, ParameterDirection.InputOutput, ParameterDirection.ReturnValue], probe.Parameters.Select(p => p.Direction));
		Assert.Null(probe.Parameters[0].Value); // an output parameter is sent as given; an input-output one with no value is a database null
		Assert.Equal(DBNull.Value, probe.Parameters[1].Value);

		Assert.Equal(["@o", "@io", "@r"], recorder.Sent.Select(p => p.ParameterName));
		Assert.Equal([ParameterDirection.Output, ParameterDirection.InputOutput, ParameterDirection.ReturnValue], recorder.Sent.Select(p => p.Direction));
		Assert.Null(recorder.Sent[0].Value);
		Assert.Equal(DBNull.Value, recorder.Sent[1].Value);
	}

	// A name that is derived from the argument is still an input, and stays one once its real name is known.
	[Fact]
	public static void AnArgumentNamedByItsExpression_IsSentAsAnInput()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		const int orderId = 5;

		new ExpressiveCommand(probe.Connection, CommandType.StoredProcedure, "dbo.p")
			.AddParam(orderId)
			.AddParams(new { Name = "ann" })
			.ExecuteNonQuery();

		Assert.Equal(["orderId", "Name"], probe.Parameters.Select(p => p.ParameterName));
		Assert.All(probe.Parameters, p => Assert.Equal(ParameterDirection.Input, p.Direction));
	}
}
