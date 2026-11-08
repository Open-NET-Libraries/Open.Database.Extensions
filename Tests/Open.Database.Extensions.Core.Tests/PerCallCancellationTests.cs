using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

/// <summary>The query types an async terminal can be called on.</summary>
public enum QueryShape { Plain, Skip, Take, Interface }

/// <summary>The async terminals (the <c>ReadAsync</c> forms are the primitives; the rest are built on them or beside them).</summary>
public enum QueryTerminal { ToList, ToArray, First, Single, ToImmutableArray, ToDictionary, ToHashSet, ReadHandler, ReadResult, ReadHandlerNoToken, ReadResultNoToken }

/// <summary>Which of the two tokens a call has.</summary>
public enum TokenSetup { CallOnly, CommandOnly, SameOnBoth, DifferentOnBoth }

/// <summary>Which token is canceled; the "WithLive" forms have the other one too, so the call runs on a linked token.</summary>
public enum CancelBy { CallOnly, CommandOnly, CallWithLiveCommand, CommandWithLiveCall }

/// <summary>How a call ends.</summary>
public enum CallOutcome { Completes, Fails, IsCanceled }

public sealed class TokenItem
{
	public int Id { get; set; }
}

// A token passed to an async terminal (ToListAsync(token)) is used together with the command's own (UseCancellationToken):
// one token governs the whole call, for the first check, the connection open, the name lookup of a stored procedure,
// the execute and the row reads, and either of the two cancels it.
[ExcludeFromCodeCoverage]
public static class PerCallCancellationTests
{
	static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
	static int _procedures;

	// ---- the calls under test ----

	static async ValueTask Drain(IAsyncEnumerable<TokenItem> items)
	{
		await foreach (TokenItem _ in items) { }
	}

	static async ValueTask<int> DrainAndCount(IAsyncEnumerable<TokenItem> items)
	{
		int n = 0;
		await foreach (TokenItem _ in items) n++;
		return n;
	}

	// The same calls on each type: the instance terminals of the three structs, and the extension methods on IMappedQuery<T>.
	static Task Call(MappedQuery<TokenItem> q, QueryTerminal terminal, CancellationToken token, TokenRig rig) => terminal switch
	{
		QueryTerminal.ToList => q.ToListAsync(token).AsTask(),
		QueryTerminal.ToArray => q.ToArrayAsync(token).AsTask(),
		QueryTerminal.First => q.FirstOrDefaultAsync(token).AsTask(),
		QueryTerminal.Single => q.SingleOrDefaultAsync(token).AsTask(),
		QueryTerminal.ToImmutableArray => q.ToImmutableArrayAsync(token).AsTask(),
		QueryTerminal.ToDictionary => q.ToDictionaryAsync(static i => i.Id, null, token).AsTask(),
		QueryTerminal.ToHashSet => q.ToHashSetAsync(null, token).AsTask(),
		QueryTerminal.ReadHandler => q.ReadAsync((items, t) => { rig.Handler.Add(t); return Drain(items); }, token).AsTask(),
		QueryTerminal.ReadResult => q.ReadAsync((items, t) => { rig.Handler.Add(t); return DrainAndCount(items); }, token).AsTask(),
		QueryTerminal.ReadHandlerNoToken => q.ReadAsync(Drain, token).AsTask(),
		QueryTerminal.ReadResultNoToken => q.ReadAsync(DrainAndCount, token).AsTask(),
		_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
	};

	static Task Call(MappedQueryWithSkip<TokenItem> q, QueryTerminal terminal, CancellationToken token, TokenRig rig) => terminal switch
	{
		QueryTerminal.ToList => q.ToListAsync(token).AsTask(),
		QueryTerminal.ToArray => q.ToArrayAsync(token).AsTask(),
		QueryTerminal.First => q.FirstOrDefaultAsync(token).AsTask(),
		QueryTerminal.Single => q.SingleOrDefaultAsync(token).AsTask(),
		QueryTerminal.ToImmutableArray => q.ToImmutableArrayAsync(token).AsTask(),
		QueryTerminal.ToDictionary => q.ToDictionaryAsync(static i => i.Id, null, token).AsTask(),
		QueryTerminal.ToHashSet => q.ToHashSetAsync(null, token).AsTask(),
		QueryTerminal.ReadHandler => q.ReadAsync((items, t) => { rig.Handler.Add(t); return Drain(items); }, token).AsTask(),
		QueryTerminal.ReadResult => q.ReadAsync((items, t) => { rig.Handler.Add(t); return DrainAndCount(items); }, token).AsTask(),
		QueryTerminal.ReadHandlerNoToken => q.ReadAsync(Drain, token).AsTask(),
		QueryTerminal.ReadResultNoToken => q.ReadAsync(DrainAndCount, token).AsTask(),
		_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
	};

	static Task Call(MappedQueryWithTake<TokenItem> q, QueryTerminal terminal, CancellationToken token, TokenRig rig) => terminal switch
	{
		QueryTerminal.ToList => q.ToListAsync(token).AsTask(),
		QueryTerminal.ToArray => q.ToArrayAsync(token).AsTask(),
		QueryTerminal.First => q.FirstOrDefaultAsync(token).AsTask(),
		QueryTerminal.Single => q.SingleOrDefaultAsync(token).AsTask(),
		QueryTerminal.ToImmutableArray => q.ToImmutableArrayAsync(token).AsTask(),
		QueryTerminal.ToDictionary => q.ToDictionaryAsync(static i => i.Id, null, token).AsTask(),
		QueryTerminal.ToHashSet => q.ToHashSetAsync(null, token).AsTask(),
		QueryTerminal.ReadHandler => q.ReadAsync((items, t) => { rig.Handler.Add(t); return Drain(items); }, token).AsTask(),
		QueryTerminal.ReadResult => q.ReadAsync((items, t) => { rig.Handler.Add(t); return DrainAndCount(items); }, token).AsTask(),
		QueryTerminal.ReadHandlerNoToken => q.ReadAsync(Drain, token).AsTask(),
		QueryTerminal.ReadResultNoToken => q.ReadAsync(DrainAndCount, token).AsTask(),
		_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
	};

	static Task Call(IMappedQuery<TokenItem> q, QueryTerminal terminal, CancellationToken token, TokenRig rig) => terminal switch
	{
		QueryTerminal.ToList => q.ToListAsync(token).AsTask(),
		QueryTerminal.ToArray => q.ToArrayAsync(token).AsTask(),
		QueryTerminal.First => q.FirstOrDefaultAsync(token).AsTask(),
		QueryTerminal.Single => q.SingleOrDefaultAsync(token).AsTask(),
		QueryTerminal.ToImmutableArray => q.ToImmutableArrayAsync(token).AsTask(),
		QueryTerminal.ToDictionary => q.ToDictionaryAsync(static i => i.Id, null, token).AsTask(),
		QueryTerminal.ToHashSet => q.ToHashSetAsync(null, token).AsTask(),
		QueryTerminal.ReadHandler => q.ReadAsync((items, t) => { rig.Handler.Add(t); return Drain(items); }, token).AsTask(),
		QueryTerminal.ReadResult => q.ReadAsync((items, t) => { rig.Handler.Add(t); return DrainAndCount(items); }, token).AsTask(),
		QueryTerminal.ReadHandlerNoToken => q.ReadAsync(Drain, token).AsTask(),
		QueryTerminal.ReadResultNoToken => q.ReadAsync(DrainAndCount, token).AsTask(),
		_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
	};

	static Task Run(ExpressiveDbCommand command, QueryShape shape, QueryTerminal terminal, CancellationToken token, TokenRig rig) => shape switch
	{
		QueryShape.Plain => Call(command.Map<TokenItem>(), terminal, token, rig),
		QueryShape.Skip => Call(command.Map<TokenItem>().Skip(0), terminal, token, rig),
		QueryShape.Take => Call(command.Map<TokenItem>().Take(100), terminal, token, rig),
		QueryShape.Interface => Call((IMappedQuery<TokenItem>)command.Map<TokenItem>(), terminal, token, rig),
		_ => throw new ArgumentOutOfRangeException(nameof(shape)),
	};

	// ---- the commands under test ----

	// A closed connection, a stored procedure with a derived parameter, and async reads: one call goes through every step that takes a token.
	static ExpressiveDbCommand Procedure(TokenRig rig)
	{
		const int id = 5;
		return new TokenProcCommand(rig, new TokenConnection(rig), "dbo.proc" + Interlocked.Increment(ref _procedures))
			.AddParam(id)
			.EnableAsyncRead();
	}

	static ExpressiveDbCommand Text(TokenRig rig, ConnectionState state = ConnectionState.Closed)
		=> new(new TokenConnection(rig, state), CommandType.Text, "SELECT Id");

	static List<CancellationToken> TokensOf(TokenRig rig, TokenStage stage) => stage switch
	{
		TokenStage.Open => rig.Open,
		TokenStage.Lookup => rig.Lookup,
		TokenStage.Execute => rig.Execute,
		_ => rig.Read,
	};

	// ---- theory data ----

	public static TheoryData<QueryShape, QueryTerminal> Terminals { get; } = BuildTerminals();

	static TheoryData<QueryShape, QueryTerminal> BuildTerminals()
	{
		var data = new TheoryData<QueryShape, QueryTerminal>();
		foreach (QueryShape shape in Enum.GetValues<QueryShape>())
			foreach (QueryTerminal terminal in Enum.GetValues<QueryTerminal>())
				data.Add(shape, terminal);

		return data;
	}

	public static TheoryData<QueryShape, QueryTerminal, TokenSetup> Setups { get; } = BuildSetups();

	static TheoryData<QueryShape, QueryTerminal, TokenSetup> BuildSetups()
	{
		var data = new TheoryData<QueryShape, QueryTerminal, TokenSetup>();
		foreach (QueryShape shape in Enum.GetValues<QueryShape>())
			foreach (QueryTerminal terminal in Enum.GetValues<QueryTerminal>())
				foreach (TokenSetup setup in Enum.GetValues<TokenSetup>())
					data.Add(shape, terminal, setup);

		return data;
	}

	public static TheoryData<TokenStage, CancelBy> Stalls { get; } = BuildStalls();

	static TheoryData<TokenStage, CancelBy> BuildStalls()
	{
		var data = new TheoryData<TokenStage, CancelBy>();
		foreach (TokenStage stage in Enum.GetValues<TokenStage>())
			foreach (CancelBy by in Enum.GetValues<CancelBy>())
				data.Add(stage, by);

		return data;
	}

	// ---- 1. a token that is already canceled stops the call before anything is opened or executed ----

	static async Task AssertStopsBeforeAnything(QueryShape shape, QueryTerminal terminal, bool commandHasLiveToken)
	{
		var rig = new TokenRig();
		ExpressiveDbCommand command = Procedure(rig);
		using var live = new CancellationTokenSource();
		if (commandHasLiveToken) command.UseCancellationToken(live.Token);
		using var canceled = new CancellationTokenSource();
		canceled.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run(command, shape, terminal, canceled.Token, rig));

		Assert.Empty(rig.Log); // not opened, not looked up, not executed
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static Task CanceledCallToken_ThrowsBeforeTheConnectionIsOpenedOrAnythingExecutes(QueryShape shape, QueryTerminal terminal)
		=> AssertStopsBeforeAnything(shape, terminal, commandHasLiveToken: false);

	[Theory]
	[MemberData(nameof(Terminals))]
	public static Task CanceledCallToken_WithALiveCommandToken_ThrowsBeforeTheConnectionIsOpenedOrAnythingExecutes(QueryShape shape, QueryTerminal terminal)
		=> AssertStopsBeforeAnything(shape, terminal, commandHasLiveToken: true);

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task CanceledCommandToken_ThrowsBeforeTheConnectionIsOpenedOrAnythingExecutes(QueryShape shape, QueryTerminal terminal)
	{
		var rig = new TokenRig();
		using var canceled = new CancellationTokenSource();
		canceled.Cancel();
		using var live = new CancellationTokenSource();
		ExpressiveDbCommand command = Procedure(rig).UseCancellationToken(canceled.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run(command, shape, terminal, live.Token, rig));

		Assert.Empty(rig.Log);
	}

	// ---- 2 and 3. the one token reaches the open, the name lookup, the execute and the row reads ----

	[Theory]
	[MemberData(nameof(Setups))]
	public static async Task OneTokenGoverns_TheOpen_TheNameLookup_TheExecute_AndTheRowReads(QueryShape shape, QueryTerminal terminal, TokenSetup setup)
	{
		var rig = new TokenRig();
		ExpressiveDbCommand command = Procedure(rig);
		using var callSource = new CancellationTokenSource();
		using var commandSource = new CancellationTokenSource();

		CancellationToken call = setup == TokenSetup.CommandOnly ? default : callSource.Token;
		switch (setup)
		{
			case TokenSetup.CommandOnly:
			case TokenSetup.DifferentOnBoth:
				command.UseCancellationToken(commandSource.Token);
				break;
			case TokenSetup.SameOnBoth:
				command.UseCancellationToken(callSource.Token);
				break;
		}

		await Run(command, shape, terminal, call, rig);

		CancellationToken seen = Assert.Single(rig.Open);
		Assert.Equal(seen, Assert.Single(rig.Lookup));
		Assert.Equal(seen, Assert.Single(rig.Execute));
		Assert.NotEmpty(rig.Read);
		Assert.All(rig.Read, t => Assert.Equal(seen, t));
		Assert.All(rig.Handler, t => Assert.Equal(seen, t));
		if (terminal is QueryTerminal.ReadHandler or QueryTerminal.ReadResult) Assert.NotEmpty(rig.Handler);

		switch (setup)
		{
			case TokenSetup.CallOnly:
			case TokenSetup.SameOnBoth:
				Assert.Equal(callSource.Token, seen); // used as it is: no source is made
				break;
			case TokenSetup.CommandOnly:
				Assert.Equal(commandSource.Token, seen);
				break;
			default:
				Assert.True(seen.CanBeCanceled);
				Assert.NotEqual(callSource.Token, seen);
				Assert.NotEqual(commandSource.Token, seen);
				Assert.Throws<ObjectDisposedException>(() => seen.WaitHandle); // the linked source was disposed when the call finished
				break;
		}
	}

	[Fact]
	public static async Task NoToken_LeavesEveryStepWithATokenThatCannotBeCanceled()
	{
		var rig = new TokenRig();

		await Procedure(rig).Map<TokenItem>().ToListAsync(CancellationToken.None);

		Assert.False(Assert.Single(rig.Open).CanBeCanceled);
		Assert.False(Assert.Single(rig.Lookup).CanBeCanceled);
		Assert.False(Assert.Single(rig.Execute).CanBeCanceled);
		Assert.All(rig.Read, t => Assert.False(t.CanBeCanceled));
	}

	// ---- 4. either token cancels a step that is waiting ----

	[Theory]
	[MemberData(nameof(Stalls))]
	public static async Task ASlowStep_IsCanceledByEitherToken(TokenStage stage, CancelBy by)
	{
		var rig = new TokenRig { Stall = stage };
		ExpressiveDbCommand command = Procedure(rig);
		using var callSource = new CancellationTokenSource();
		using var commandSource = new CancellationTokenSource();

		CancellationToken call = by == CancelBy.CommandOnly ? default : callSource.Token;
		if (by != CancelBy.CallOnly) command.UseCancellationToken(commandSource.Token);

		Task running = command.Map<TokenItem>().ToListAsync(call).AsTask();
		await rig.Entered.Task.WaitAsync(Patience, TestContext.Current.CancellationToken);
		Assert.False(running.IsCompleted); // it is waiting on the slow step, which only a token can end

		CancellationToken seen = Assert.Single(TokensOf(rig, stage));
		bool bothLive = by is CancelBy.CallWithLiveCommand or CancelBy.CommandWithLiveCall;
		if (bothLive)
		{
			Assert.NotEqual(callSource.Token, seen); // two tokens that can be canceled: the call runs on a linked one
			Assert.NotEqual(commandSource.Token, seen);
		}

		if (by is CancelBy.CallOnly or CancelBy.CallWithLiveCommand) callSource.Cancel();
		else commandSource.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(Patience, TestContext.Current.CancellationToken));
	}

	[Theory]
	[InlineData(CancelBy.CallOnly)]
	[InlineData(CancelBy.CommandOnly)]
	[InlineData(CancelBy.CallWithLiveCommand)]
	[InlineData(CancelBy.CommandWithLiveCall)]
	public static async Task RowReads_AreCanceledByEitherToken_WhenRowsArePulledWithRead(CancelBy by)
	{
		var rig = new TokenRig { Rows = 5 };
		ExpressiveDbCommand command = Text(rig, ConnectionState.Open); // UseAsyncRead is off: the rows come from Read(), and the token is checked between rows
		using var callSource = new CancellationTokenSource();
		using var commandSource = new CancellationTokenSource();

		CancellationToken call = by == CancelBy.CommandOnly ? default : callSource.Token;
		if (by != CancelBy.CallOnly) command.UseCancellationToken(commandSource.Token);

		CancellationTokenSource toCancel = by is CancelBy.CallOnly or CancelBy.CallWithLiveCommand ? callSource : commandSource;
		int reads = 0;
		rig.BeforeRead = n =>
		{
			reads = n;
			if (n == 2) toCancel.Cancel();
		};

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await command.Map<TokenItem>().ToListAsync(call));

		Assert.True(reads < 5); // it stopped; it did not read the rest
	}

	// ---- the linked source is disposed however the call ends ----

	[Theory]
	[InlineData(CallOutcome.Completes)]
	[InlineData(CallOutcome.Fails)]
	[InlineData(CallOutcome.IsCanceled)]
	public static async Task TheLinkedSource_IsDisposed_WhenTheCallEnds(CallOutcome outcome)
	{
		var rig = new TokenRig();
		ExpressiveDbCommand command = Text(rig);
		using var callSource = new CancellationTokenSource();
		using var commandSource = new CancellationTokenSource();
		command.UseCancellationToken(commandSource.Token);

		switch (outcome)
		{
			case CallOutcome.Fails:
				rig.ExecuteFailure = new InvalidOperationException("The server refused.");
				break;
			case CallOutcome.IsCanceled:
				rig.Stall = TokenStage.Execute;
				break;
		}

		Task running = command.Map<TokenItem>().ToListAsync(callSource.Token).AsTask();
		switch (outcome)
		{
			case CallOutcome.Completes:
				await running;
				break;
			case CallOutcome.Fails:
				await Assert.ThrowsAsync<InvalidOperationException>(() => running);
				break;
			default:
				await rig.Entered.Task.WaitAsync(Patience, TestContext.Current.CancellationToken);
				callSource.Cancel();
				await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(Patience, TestContext.Current.CancellationToken));
				break;
		}

		CancellationToken linked = Assert.Single(rig.Execute);
		Assert.NotEqual(callSource.Token, linked);
		Assert.NotEqual(commandSource.Token, linked);
		Assert.Throws<ObjectDisposedException>(() => linked.WaitHandle);
	}

	// ---- the pooled path (a command from a factory) and the second name lookup ----

	[Fact]
	public static async Task AFactoryCommand_RunsEveryStepOnTheCombinedToken()
	{
		var rig = new TokenRig();
		IDbConnectionFactory<DbConnection> factory = DbConnectionFactory.Create<DbConnection>(() => new TokenConnection(rig));
		const int id = 5;
		ExpressiveDbCommand command = new TokenProcCommand(rig, factory, "dbo.proc" + Interlocked.Increment(ref _procedures)).AddParam(id).EnableAsyncRead();
		using var callSource = new CancellationTokenSource();
		using var commandSource = new CancellationTokenSource();
		command.UseCancellationToken(commandSource.Token);

		await command.Map<TokenItem>().ToListAsync(callSource.Token);

		CancellationToken seen = Assert.Single(rig.Open);
		Assert.Equal(seen, Assert.Single(rig.Lookup));
		Assert.Equal(seen, Assert.Single(rig.Execute));
		Assert.NotEmpty(rig.Read);
		Assert.All(rig.Read, t => Assert.Equal(seen, t));
		Assert.NotEqual(callSource.Token, seen);
		Assert.NotEqual(commandSource.Token, seen);
	}

	// The names remembered from an earlier call do not fit this one, so they are read again; that lookup is on the call's token as well.
	[Fact]
	public static async Task TheSecondNameLookup_GetsTheCombinedToken()
	{
		var rig = new TokenRig();
		var connection = new TokenConnection(rig, ConnectionState.Open);
		string procedure = "dbo.proc" + Interlocked.Increment(ref _procedures);
		const int id = 5;
		const int other = 6;
		using var callSource = new CancellationTokenSource();
		using var commandSource = new CancellationTokenSource();

		await new TokenProcCommand(rig, connection, procedure).AddParam(id).UseCancellationToken(commandSource.Token).Map<TokenItem>().ToListAsync(callSource.Token); // remembers @id
		rig.Lookup.Clear();

		await Assert.ThrowsAsync<InvalidOperationException>(async () =>
			await new TokenProcCommand(rig, connection, procedure).AddParam(other).UseCancellationToken(commandSource.Token).Map<TokenItem>().ToListAsync(callSource.Token));

		CancellationToken seen = Assert.Single(rig.Lookup); // the remembered names were tried first; only the second reading goes to the provider
		Assert.True(seen.CanBeCanceled);
		Assert.NotEqual(callSource.Token, seen);
		Assert.NotEqual(commandSource.Token, seen);
	}

	// ---- the interface member: the token given is used instead of the command's ----

	[Fact]
	public static async Task TokenOverload_UsesTheGivenToken_InsteadOfTheCommandsOwn()
	{
		var rig = new TokenRig();
		using var canceled = new CancellationTokenSource();
		canceled.Cancel();
		using var given = new CancellationTokenSource();
		IExecuteReader command = Text(rig).UseCancellationToken(canceled.Token); // its own token is canceled, but this call is not governed by it

		int rows = await command.ExecuteReaderAsync(0, static (reader, _) => new ValueTask<int>(reader.Read() ? 1 : 0), CommandBehavior.Default, given.Token);

		Assert.Equal(1, rows);
		Assert.Equal(given.Token, Assert.Single(rig.Open));
		Assert.Equal(given.Token, Assert.Single(rig.Execute));
	}

	[Fact]
	public static async Task TokenOverload_ACanceledGivenToken_ThrowsEvenWhenTheCommandsOwnIsLive()
	{
		var rig = new TokenRig();
		using var live = new CancellationTokenSource();
		using var canceled = new CancellationTokenSource();
		canceled.Cancel();
		IExecuteReader command = Text(rig).UseCancellationToken(live.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			await command.ExecuteReaderAsync(0, static (reader, _) => new ValueTask<int>(reader.Read() ? 1 : 0), CommandBehavior.Default, canceled.Token));

		Assert.Empty(rig.Log);
	}

	[Fact]
	public static void TokenOverload_NullTransform_Throws()
	{
		IExecuteReader command = Text(new TokenRig());

		Assert.Throws<ArgumentNullException>(() => command.ExecuteReaderAsync<int, int>(0, null!, CommandBehavior.Default, CancellationToken.None));
	}
}

// The common paths allocate nothing for the tokens: no token, one token and the same token twice need no source. Two different tokens do (the control).
[ExcludeFromCodeCoverage]
public static class PerCallCancellationAllocationTests
{
	// A command on an open connection, a reader of one row: the call finishes without waiting, so the whole call runs on this thread and is counted.
	// The smallest allocation per call over several rounds, after warm-up (the row layout is compiled and cached by then).
	static long BytesPerCall(CancellationToken commandToken, CancellationToken callToken)
	{
		var rig = new TokenRig { Recording = false };
		ExpressiveDbCommand command = new ExpressiveDbCommand(new TokenConnection(rig, ConnectionState.Open), CommandType.Text, "SELECT Id").UseCancellationToken(commandToken);

		for (int i = 0; i < 20; i++) One(command, callToken);

		long best = long.MaxValue;
		for (int round = 0; round < 7; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 50; i++) One(command, callToken);
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 50);
		}

		return best;
	}

	static void One(ExpressiveDbCommand command, CancellationToken token)
	{
		ValueTask<List<TokenItem>> task = command.Map<TokenItem>().ToListAsync(token);
		if (!task.IsCompletedSuccessfully) throw new InvalidOperationException("The call did not finish without waiting.");
		_ = task.GetAwaiter().GetResult();
	}

	const long OneSmallObject = 32; // bytes; an object header, a few fields

#pragma warning disable xUnit1051 // The point is to call with no token, one token, or two.
	[Fact]
	public static void DefaultToken_SingleToken_AndTheSameTokenTwice_AddNothingOverTheBaseline()
	{
		using var source = new CancellationTokenSource();
		long baseline = BytesPerCall(default, default);
		long commandOnly = BytesPerCall(source.Token, default);
		long callOnly = BytesPerCall(default, source.Token);
		long same = BytesPerCall(source.Token, source.Token);

		TestContext.Current.SendDiagnosticMessage($"bytes per call: baseline {baseline}, command token only {commandOnly}, call token only {callOnly}, same token on both {same}");
		Assert.InRange(commandOnly, baseline - OneSmallObject, baseline + OneSmallObject);
		Assert.InRange(callOnly, baseline - OneSmallObject, baseline + OneSmallObject);
		Assert.InRange(same, baseline - OneSmallObject, baseline + OneSmallObject);
	}

	[Fact]
	public static void TwoDifferentTokens_CostALinkedSource_SoTheMeasureSeesTheDifference()
	{
		using var first = new CancellationTokenSource();
		using var second = new CancellationTokenSource();
		long baseline = BytesPerCall(default, default);
		long linked = BytesPerCall(first.Token, second.Token);

		TestContext.Current.SendDiagnosticMessage($"bytes per call: baseline {baseline}, two different tokens {linked}");
		Assert.True(linked >= baseline + 2 * OneSmallObject, $"baseline {baseline} B, linked {linked} B");
	}
#pragma warning restore xUnit1051
}
