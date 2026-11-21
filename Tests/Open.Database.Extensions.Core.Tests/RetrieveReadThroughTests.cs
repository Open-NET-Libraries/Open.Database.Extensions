using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The command-level Retrieve and FirstOrdinalResults (the extensions on a command, the Expressive commands' own members)
// return the rows of the first result set and then read through the later sets (NextResult, no Read),
// so a failure in a later statement is thrown, as it is for Map. SingleResult is the one opt-out: in the behavior passed,
// or on the view command.SingleResult() returns. A reader the caller supplies is never advanced.
[ExcludeFromCodeCoverage]
public static class RetrieveReadThroughTests
{
	const int LaterSets = 2; // so a full read-through is three calls of NextResult: true, true, false

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	/// <summary>A connection whose commands hand back a reader with later result sets; it keeps the last command and reader.</summary>
	sealed class SetsConnection(int? failAt = null) : DbConnection
	{
		ConnectionState _state = ConnectionState.Closed;

		public MultiSetReader? Reader { get; private set; }

		public FakeDbCommand? Command { get; private set; }

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "sets";
		public override string DataSource => "sets";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => _state;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() => _state = ConnectionState.Open;
		public override void Close() => _state = ConnectionState.Closed;

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

		protected override DbCommand CreateDbCommand()
			=> Command = new FakeDbCommand(this, () => Reader = new MultiSetReader(ReaderFixtures.Rows(1), LaterSets, failAt)) { CommandText = "select 1" };
	}

	// Every command-level call that reads through. The connection is given so the test can look at what was read.
	static readonly Dictionary<string, Func<SetsConnection, Task>> Calls = new()
	{
		["IDbCommand.Retrieve()"] = c => Task.FromResult(c.CreateCommand().Retrieve()),
		["IDbCommand.Retrieve(ordinals)"] = c => Task.FromResult(c.CreateCommand().Retrieve([0])),
		["IDbCommand.Retrieve(n, others)"] = c => Task.FromResult(c.CreateCommand().Retrieve(0, 1)),
		["IDbCommand.Retrieve(names)"] = c => Task.FromResult(c.CreateCommand().Retrieve(["Id"])),
		["IDbCommand.Retrieve(c, others)"] = c => Task.FromResult(c.CreateCommand().Retrieve("Id", "Name")),
		["IDbCommand.FirstOrdinalResults()"] = c => Task.FromResult(((IDbCommand)c.CreateCommand()).FirstOrdinalResults().ToList()),
		["DbCommand.FirstOrdinalResults<T>()"] = c => Task.FromResult(c.CreateCommand().FirstOrdinalResults<int>().ToList()),
		["DbCommand.RetrieveAsync(token)"] = async c => await c.CreateCommand().RetrieveAsync(Ct),
		["DbCommand.RetrieveAsync(ordinals)"] = async c => await c.CreateCommand().RetrieveAsync([0], Ct),
		["DbCommand.RetrieveAsync(names)"] = async c => await c.CreateCommand().RetrieveAsync(["Id"], false, Ct),
		["DbCommand.FirstOrdinalResultsAsync()"] = async c => await c.CreateCommand().FirstOrdinalResultsAsync(cancellationToken: Ct),
		["DbCommand.FirstOrdinalResultsAsync<T>()"] = async c => await c.CreateCommand().FirstOrdinalResultsAsync<int>(cancellationToken: Ct),
		["IExecuteReader.Retrieve()"] = c => Task.FromResult(new ExpressiveDbCommand(c, CommandType.Text, "select 1").Retrieve()),
		["IExecuteReader.Retrieve(ordinals)"] = c => Task.FromResult(new ExpressiveDbCommand(c, CommandType.Text, "select 1").Retrieve([0])),
		["IExecuteReader.Retrieve(n, others)"] = c => Task.FromResult(new ExpressiveDbCommand(c, CommandType.Text, "select 1").Retrieve(0, 1)),
		["IExecuteReader.Retrieve(names)"] = c => Task.FromResult(new ExpressiveDbCommand(c, CommandType.Text, "select 1").Retrieve(["Id"])),
		["IExecuteReader.Retrieve(c, others)"] = c => Task.FromResult(new ExpressiveDbCommand(c, CommandType.Text, "select 1").Retrieve("Id", "Name")),
		["Expressive.RetrieveAsync(token)"] = async c => await new ExpressiveDbCommand(c, CommandType.Text, "select 1").RetrieveAsync(Ct),
		["Expressive.RetrieveAsync(ordinals)"] = async c => await new ExpressiveDbCommand(c, CommandType.Text, "select 1").RetrieveAsync([0], Ct),
		["Expressive.RetrieveAsync(names)"] = async c => await new ExpressiveDbCommand(c, CommandType.Text, "select 1").RetrieveAsync(["Id"], false, Ct),
		["Expressive.FirstOrdinalResultsAsync()"] = async c => await new ExpressiveDbCommand(c, CommandType.Text, "select 1").FirstOrdinalResultsAsync(),
		["Expressive.FirstOrdinalResultsAsync<T>()"] = async c => await new ExpressiveDbCommand(c, CommandType.Text, "select 1").FirstOrdinalResultsAsync<int>(),
	};

	public static TheoryData<string> CallNames() => [.. Calls.Keys];

	[Theory, MemberData(nameof(CallNames))]
	public static async Task EveryCall_ReadsThroughTheLaterSets_AndNeverReadsTheirRows(string call)
	{
		using var connection = new SetsConnection();

		await Calls[call](connection);

		Assert.Equal(LaterSets + 1, connection.Reader!.NextResultCalls);
		Assert.Equal(0, connection.Reader.ReadsAfterNextResult);
	}

	[Theory, MemberData(nameof(CallNames))]
	public static async Task EveryCall_ALaterFailure_IsThrownAsTheProviderRaised(string call)
	{
		using var connection = new SetsConnection(failAt: 2);

		await Assert.ThrowsAsync<LaterSetFailure>(() => Calls[call](connection));
	}

	// ---- the opt-out: SingleResult ----

	// Each of these asks for the first set only, with SingleResult in the behavior or on the view.
	static readonly Dictionary<string, Func<SetsConnection, Task>> OptOuts = new()
	{
		["IDbCommand.Retrieve(SingleResult)"] = c => Task.FromResult(c.CreateCommand().Retrieve(CommandBehavior.SingleResult)),
		["command.SingleResult().Retrieve()"] = c => Task.FromResult(c.CreateCommand().SingleResult().Retrieve()),
		["command.SingleResult().Retrieve(names)"] = c => Task.FromResult(c.CreateCommand().SingleResult().Retrieve(["Id"])),
		["IDbCommand.FirstOrdinalResults(SingleResult)"] = c => Task.FromResult(((IDbCommand)c.CreateCommand()).FirstOrdinalResults(CommandBehavior.SingleResult).ToList()),
		["DbCommand.FirstOrdinalResults<T>(SingleResult)"] = c => Task.FromResult(c.CreateCommand().FirstOrdinalResults<int>(CommandBehavior.SingleResult).ToList()),
		["DbCommand.FirstOrdinalResultsAsync(SingleResult)"] = async c => await c.CreateCommand().FirstOrdinalResultsAsync(CommandBehavior.SingleResult, cancellationToken: Ct),
		["DbCommand.FirstOrdinalResultsAsync<T>(SingleResult)"] = async c => await c.CreateCommand().FirstOrdinalResultsAsync<int>(CommandBehavior.SingleResult, cancellationToken: Ct),
		["IExecuteReader.SingleResult().Retrieve()"] = c => Task.FromResult(new ExpressiveDbCommand(c, CommandType.Text, "select 1").SingleResult().Retrieve()),
		["IExecuteReader.SingleResult().Retrieve(ordinals)"] = c => Task.FromResult(new ExpressiveDbCommand(c, CommandType.Text, "select 1").SingleResult().Retrieve([0])),
	};

	public static TheoryData<string> OptOutNames() => [.. OptOuts.Keys];

	[Theory, MemberData(nameof(OptOutNames))]
	public static async Task SingleResult_ReadsOnlyTheFirstSet_AndALaterFailureIsNotSeen(string call)
	{
		using var connection = new SetsConnection(failAt: 2);

		await OptOuts[call](connection);

		Assert.Equal(0, connection.Reader!.NextResultCalls);
		Assert.NotEqual(0, (int)(connection.Command!.Behaviors[0] & CommandBehavior.SingleResult));
	}

	// Without the opt-out the provider is not told there is one result set.
	[Theory, MemberData(nameof(CallNames))]
	public static async Task WithoutTheOptOut_TheReaderIsNotAskedForSingleResult(string call)
	{
		using var connection = new SetsConnection();

		await Calls[call](connection);

		Assert.Equal(0, (int)(connection.Command!.Behaviors[0] & CommandBehavior.SingleResult));
	}

	// ---- nothing is replaced, nothing is advanced ----

	[Fact]
	public static void WhenTheReaderOfTheCommandThrows_NothingIsReadThrough_AndTheOriginalExceptionStands()
	{
		using var connection = new SetsConnection();
		IDbCommand command = connection.CreateCommand();

		Assert.Throws<InvalidOperationException>(() => command.ExecuteReaderThrough<int>(_ => throw new InvalidOperationException("the caller threw")));

		Assert.Equal(0, connection.Reader!.NextResultCalls);
	}

	[Fact]
	public static void ACallerSuppliedReader_IsNeverAdvanced()
	{
		var reader = new MultiSetReader(ReaderFixtures.Rows(1), LaterSets);

		reader.Retrieve();

		Assert.Equal(0, reader.NextResultCalls);
	}
}
