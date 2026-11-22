using System.Buffers;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>The async terminals that take a token.</summary>
public enum TokenTerminal
{
	NonQuery,
	Scalar,
	ScalarFunc,
	ScalarFuncValueTask,
	ScalarT,
	Return,
	ReturnT,
	ReaderState,
	ReaderAction,
	ReaderFunc,
	ReaderFuncValueTask,
	ReaderFuncValueTaskT,
	ReaderActionToken,
	ReaderFuncToken,
	ReaderFuncValueTaskToken,
	ReaderFuncValueTaskTToken,
}

// Every kept async terminal takes a CancellationToken, last.
// It is used together with the command's own token: either cancels,
// one live token is used as it is,
// two different live tokens are linked (and the linked source is disposed
// when the call ends),
// and no token is no allocation.
#pragma warning disable xUnit1051 // These tests are about which token a call is given: no token, one, or two.
// The allocation tests count the bytes of the thread that calls, and what else
// the process does (the JIT, the GC, other tests on the thread pool) moves what a
// call costs: no other test class runs while they do (the collection is defined
// in LeakRetentionTests).
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class TokenTerminalTests
{
	static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
	static readonly string[] IdColumn = ["Id"];

	public static TheoryData<TokenTerminal> Terminals
		=> [.. Enum.GetValues<TokenTerminal>()];

	// ---- the command terminals ----

	static ExpressiveDbCommand Make(
		TokenRig rig,
		ConnectionState state = ConnectionState.Closed)
		=> new(new TokenConnection(rig, state), CommandType.Text, "SELECT Id");

	static async ValueTask Invoke(
		ExpressiveDbCommand cmd,
		TokenTerminal terminal,
		CancellationToken token)
	{
		switch (terminal)
		{
			case TokenTerminal.NonQuery:
				await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
				break;
			case TokenTerminal.Scalar:
				await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);
				break;
			case TokenTerminal.ScalarFunc:
				await cmd
					.ExecuteScalarAsync(static _ => 1, token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ScalarFuncValueTask:
				await cmd
					.ExecuteScalarAsync(
						static _ => new ValueTask<int>(1), token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ScalarT:
				await cmd.ExecuteScalarAsync<int>(token).ConfigureAwait(false);
				break;
			case TokenTerminal.Return:
				await cmd.ExecuteReturnAsync(token).ConfigureAwait(false);
				break;
			case TokenTerminal.ReturnT:
				await cmd.ExecuteReturnAsync<int>(token).ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderState:
				await cmd
					.ExecuteReaderAsync(
						1,
						static (_, s) => new ValueTask<int>(s),
						CommandBehavior.Default,
						token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderAction:
				await cmd
					.ExecuteReaderAsync(
						static _ => { },
						CommandBehavior.Default,
						token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderFunc:
				await cmd
					.ExecuteReaderAsync(
						static _ => 1,
						CommandBehavior.Default,
						token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderFuncValueTask:
				await cmd
					.ExecuteReaderAsync(
						static _ => new ValueTask(),
						CommandBehavior.Default,
						token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderFuncValueTaskT:
				await cmd
					.ExecuteReaderAsync(
						static _ => new ValueTask<int>(1),
						CommandBehavior.Default,
						token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderActionToken:
				await cmd
					.ExecuteReaderAsync(static _ => { }, token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderFuncToken:
				await cmd
					.ExecuteReaderAsync(static _ => 1, token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderFuncValueTaskToken:
				await cmd
					.ExecuteReaderAsync(static _ => new ValueTask(), token)
					.ConfigureAwait(false);
				break;
			case TokenTerminal.ReaderFuncValueTaskTToken:
				await cmd
					.ExecuteReaderAsync(
						static _ => new ValueTask<int>(1), token)
					.ConfigureAwait(false);
				break;
			default:
				throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	static Task Call(
		ExpressiveDbCommand cmd,
		TokenTerminal terminal,
		CancellationToken token)
		=> Invoke(cmd, terminal, token).AsTask();

	static bool IsReader(TokenTerminal t)
		=> t >= TokenTerminal.ReaderState;

	static async Task Cancels(Task task)
		=> await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => task.WaitAsync(Patience));

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task A_CancelledCallToken_ThrowsBeforeAnythingIsOpened(
		TokenTerminal terminal)
	{
		var rig = new TokenRig();
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		await Cancels(Call(Make(rig), terminal, cts.Token));
		Assert.Empty(rig.Log);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task A_CallTokenCancelledWhileStalledInTheOpen_Cancels(
		TokenTerminal terminal)
	{
		var rig = new TokenRig { Stall = TokenStage.Open };
		using var cts = new CancellationTokenSource();
		Task task = Call(Make(rig), terminal, cts.Token);
		await rig.Entered.Task.WaitAsync(Patience);
		cts.Cancel();
		await Cancels(task);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task B_CommandTokenStillCancels_WithALiveCallToken(
		TokenTerminal terminal)
	{
		var rig = new TokenRig { Stall = TokenStage.Open };
		using var commandSource = new CancellationTokenSource();
		using var callSource = new CancellationTokenSource();
		Task task = Call(
			Make(rig).UseCancellationToken(commandSource.Token),
			terminal,
			callSource.Token);
		await rig.Entered.Task.WaitAsync(Patience);
		commandSource.Cancel();
		await Cancels(task);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task C_BothLive_TheStepSeesTheLinkedToken_AndEitherCancels(
		TokenTerminal terminal)
	{
		foreach (bool cancelCall in new[] { true, false })
		{
			var rig = new TokenRig { Stall = TokenStage.Open };
			using var commandSource = new CancellationTokenSource();
			using var callSource = new CancellationTokenSource();
			Task task = Call(
				Make(rig).UseCancellationToken(commandSource.Token),
				terminal,
				callSource.Token);
			await rig.Entered.Task.WaitAsync(Patience);
			CancellationToken seen = Assert.Single(rig.Open);
			Assert.True(seen.CanBeCanceled);
			Assert.NotEqual(callSource.Token, seen);
			Assert.NotEqual(commandSource.Token, seen);
			(cancelCall ? callSource : commandSource).Cancel();
			await Cancels(task);
			Assert.Throws<ObjectDisposedException>(() => seen.WaitHandle); // the linked source was disposed when the call ended
		}
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task D_NoToken_EveryStepSeesATokenThatCannotBeCancelled(
		TokenTerminal terminal)
	{
		var rig = new TokenRig();
		await Call(Make(rig), terminal, default);
		Assert.NotEmpty(rig.Open);
		Assert.All(rig.Open, t => Assert.False(t.CanBeCanceled));
		Assert.All(rig.Execute, t => Assert.False(t.CanBeCanceled));
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task OneLiveToken_IsUsedAsItIs(TokenTerminal terminal)
	{
		var rig = new TokenRig();
		using var cts = new CancellationTokenSource();
		await Call(Make(rig), terminal, cts.Token);
		Assert.Equal(cts.Token, Assert.Single(rig.Open));
		if (IsReader(terminal))
			Assert.Equal(cts.Token, Assert.Single(rig.Execute));

		rig = new TokenRig();
		await Call(
			Make(rig).UseCancellationToken(cts.Token),
			terminal,
			cts.Token); // the same token twice
		Assert.Equal(cts.Token, Assert.Single(rig.Open));
	}

	[Theory]
	[InlineData(TokenTerminal.ReaderState)]
	[InlineData(TokenTerminal.ReaderAction)]
	[InlineData(TokenTerminal.ReaderFuncValueTaskTToken)]
	public static async Task Reader_CallTokenCancelledWhileStalledInTheExecute_Cancels(
		TokenTerminal terminal)
	{
		var rig = new TokenRig { Stall = TokenStage.Execute };
		using var cts = new CancellationTokenSource();
		Task task = Call(Make(rig, ConnectionState.Open), terminal, cts.Token);
		await rig.Entered.Task.WaitAsync(Patience);
		cts.Cancel();
		await Cancels(task);
	}

	[Fact]
	public static void ExecuteReturnAsync_ThrowsSynchronously_ForEitherCancelledToken()
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		var rig = new TokenRig();
#pragma warning disable CA2012 // The call throws before it returns a ValueTask.
		Assert.Throws<OperationCanceledException>(
			() => _ = Make(rig).ExecuteReturnAsync(cts.Token));
		Assert.Throws<OperationCanceledException>(
			() => _ = Make(rig)
				.UseCancellationToken(cts.Token)
				.ExecuteReturnAsync());
#pragma warning restore CA2012
		Assert.Empty(rig.Log);
	}

	// A call through the class combines the two tokens;
	// through IExecuteReader the token passed replaces the command's own.
	[Fact]
	public static async Task InterfaceRoute_ReplacesTheCommandsToken_WhileTheClassCombinesThem()
	{
		using var commandSource = new CancellationTokenSource();
		using var callSource = new CancellationTokenSource();

		var viaInterface = new TokenRig();
		IExecuteReader asInterface = Make(viaInterface)
			.UseCancellationToken(commandSource.Token);
		await asInterface.ExecuteReaderAsync(
			1,
			static (_, s) => new ValueTask<int>(s),
			CommandBehavior.Default,
			callSource.Token);
		Assert.Equal(callSource.Token, Assert.Single(viaInterface.Open));

		var viaClass = new TokenRig();
		await Make(viaClass)
			.UseCancellationToken(commandSource.Token)
			.ExecuteReaderAsync(
				1,
				static (_, s) => new ValueTask<int>(s),
				CommandBehavior.Default,
				callSource.Token);
		CancellationToken seen = Assert.Single(viaClass.Open);
		Assert.NotEqual(callSource.Token, seen);
		Assert.NotEqual(commandSource.Token, seen);

		// and the interface members without a token still use the command's own
		var plain = new TokenRig();
		await ((IExecuteReader)Make(plain)
			.UseCancellationToken(commandSource.Token))
			.ExecuteReaderAsync(1, static (_, s) => new ValueTask<int>(s));
		Assert.Equal(commandSource.Token, Assert.Single(plain.Open));
	}

	static RecordingAsyncExecuteReader Rows(
		int count = 3,
		bool asyncRead = false)
		=> new(
			IdColumn,
			[.. Enumerable.Range(1, count).Select(i => (object?[])[i])])
		{
			UseAsyncRead = asyncRead
		};

	static readonly Func<IDataRecord, TokenItem> ToItem = static r
		=> new TokenItem { Id = r.GetInt32(0) };

	// ---- allocation: a call with no token, or one token,
	// or the same token twice, costs what the call with no token costs ----

	const int Rounds = 41;
	const int CallsPerRound = 1000;

	// One round: CallsPerRound calls of each of the calls, bytes counted call
	// by call (the allocated-bytes counter of this thread is exact). The calls
	// take turns, one of each in every step; the first one of a step moves on
	// by one every step and by one more every round, so that no call is always
	// the one that follows the same other, or always the one that goes first.
	static void MeasureRound(Action[] calls, long[] bytes, int round)
	{
		Array.Clear(bytes);
		for (int i = 0; i < CallsPerRound; i++)
		{
			for (int j = 0; j < calls.Length; j++)
			{
				int c = (i + j + round) % calls.Length;
				long before = GC.GetAllocatedBytesForCurrentThread();
				calls[c]();
				bytes[c] += GC.GetAllocatedBytesForCurrentThread() - before;
			}
		}
	}

	// Bytes of each round, in total for CallsPerRound calls, of each call,
	// after a warm-up. What a call costs is not one figure of a process: some
	// calls and some rounds cost more (async state machine boxes that depend on
	// how the work was scheduled, a collection, a compile), and they do so
	// for all the calls together. So the rounds are not reduced to a figure of
	// their own for each call, which would compare each call at its own best
	// moment; they are kept, to be compared round by round, where the calls
	// that took turns meet the same state, whenever it changes.
	static long[][] BytesPerRound(Action[] calls)
	{
		foreach (Action call in calls)
		{
			for (int i = 0; i < 300; i++) call();
		}

		long[][] rounds = [.. calls.Select(_ => new long[Rounds])];
		long[] bytes = new long[calls.Length];
		for (int round = 0; round < Rounds; round++)
		{
			MeasureRound(calls, bytes, round);
			for (int c = 0; c < calls.Length; c++)
				rounds[c][round] = bytes[c];
		}

		return rounds;
	}

	// How much more a call allocated than the reference call, in every round,
	// in bytes per call.
	static double[] PairedDifferences(long[] call, long[] reference)
	{
		double[] differences = new double[call.Length];
		for (int round = 0; round < differences.Length; round++)
		{
			differences[round]
				= (call[round] - reference[round]) / (double)CallsPerRound;
		}

		return differences;
	}

	static double Median(double[] values)
	{
		double[] sorted = [.. values];
		Array.Sort(sorted);
		return sorted[sorted.Length / 2];
	}

	// Of the paired differences of a round (the same round, the same state of
	// the process, so what moves for all the calls cancels), the median is what
	// the call costs more than the reference: a round that met a pause or a
	// collection is one of the Rounds and does not decide, and an allocation
	// planted in every call raises every difference and shows.
	// Derivation: the paired differences of one round have a standard deviation
	// of 14.5 bytes per call on CI (artifact of CI run 43774021188); the median
	// of N = 41 of them has 1.2533 * 14.5 / sqrt(41) = 2.84 bytes. The slack is
	// 16 bytes, two thirds of 24 bytes, the smallest object a call can add (x64):
	// a false failure is 5.6 standard deviations away, and one extra object per
	// call (24 bytes) is 2.8 standard deviations above the slack.
	// The test detects one extra object per call (24 B on x64).
	const double Slack = 16;

	// Compares the variants of one call, in the same run,
	// so the budget follows the runtime and the configuration. (The calls
	// that hop threads, such as the mapped ones and Dataflow
	// with its Task.Yield, are missed by the per-thread counter and are
	// not measured.)
	static void AssertNoMoreThanWithoutAToken(
		string what,
		Func<CancellationToken, CancellationToken, Action> make)
	{
		using var cts = new CancellationTokenSource();
		// A call token alone must cost what the command's token alone always
		// cost (a live token costs a little inside the channel wait, whatever
		// passes it).
		// The four variants are measured together (see BytesPerRound).
		long[][] rounds = BytesPerRound(
		[
			make(default, default),
			make(default, cts.Token),
			make(cts.Token, default),
			make(cts.Token, cts.Token),
		]);
		AssertNoMoreThanTheCommandToken(what, "no token", rounds, 0);
		AssertNoMoreThanTheCommandToken(what, "one call token", rounds, 1);
		AssertNoMoreThanTheCommandToken(what, "the same token twice", rounds, 3);
	}

	// The variants are, in this order: no token, a call token only,
	// a command token only (the reference), both.
	static void AssertNoMoreThanTheCommandToken(
		string what,
		string name,
		long[][] rounds,
		int variant)
	{
		double[] differences = PairedDifferences(rounds[variant], rounds[2]);
		double median = Median(differences);
		if (median > Slack)
		{
			string levels = string.Join(
				", ",
				rounds.Select(r => Median([.. r.Select(b => b / (double)CallsPerRound)])));
			Assert.Fail(
				$"{what}: {name} allocates {median:0.##} bytes per call more than one command token (median of {Rounds} paired differences, slack {Slack}); per round: {string.Join(", ", differences.Select(d => d.ToString("0.##")))}; bytes per call (median of the rounds): no token, call token only, command token only, both: {levels}");
		}
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static void Allocation_Terminals(TokenTerminal terminal)
		=> AssertNoMoreThanWithoutAToken(
			terminal.ToString(),
			(commandToken, callToken) =>
			{
				var rig = new TokenRig { Recording = false, Rows = 0 };
				ExpressiveDbCommand cmd = Make(rig, ConnectionState.Open)
					.UseCancellationToken(commandToken);
				return () => Invoke(cmd, terminal, callToken)
					.AsTask()
					.GetAwaiter()
					.GetResult();
			});

	// ---- overload resolution: a later change breaks this build,
	// not a user ----

	[Fact]
	public static async Task OverloadResolution_Compiles_AndBindsTheSameShapes()
	{
		var rig = new TokenRig { Recording = false };
		ExpressiveDbCommand cmd = Make(rig, ConnectionState.Open);
		CancellationToken ct = new CancellationTokenSource().Token;
		const CommandBehavior behavior = CommandBehavior.Default;

		await cmd.ExecuteNonQueryAsync();
		await cmd.ExecuteNonQueryAsync(ct);
		await cmd.ExecuteScalarAsync();
		await cmd.ExecuteScalarAsync(ct);
		Assert.Equal(1, await cmd.ExecuteScalarAsync(_ => 1));
		Assert.Equal(
			1, await cmd.ExecuteScalarAsync(_ => new ValueTask<int>(1)));
		Assert.Equal(1, await cmd.ExecuteScalarAsync(_ => 1, ct));
		Assert.Equal(
			1, await cmd.ExecuteScalarAsync(_ => new ValueTask<int>(1), ct));
		Assert.Equal(0, await cmd.ExecuteScalarAsync<int>());
		Assert.Equal(0, await cmd.ExecuteScalarAsync<int>(ct));
		Assert.Equal(
			0, await cmd.ExecuteScalarAsync<int>(CancellationToken.None));
		await cmd.ExecuteReturnAsync();
		await cmd.ExecuteReturnAsync(ct);
		await cmd.ExecuteReturnAsync<int>(ct);

		// the reader terminals with 1, 2, 3 and 4 arguments,
		// lambdas and static lambdas
		await cmd.ExecuteReaderAsync(_ => { });
		await cmd.ExecuteReaderAsync(_ => { }, behavior);
		await cmd.ExecuteReaderAsync(_ => { }, ct);
		await cmd.ExecuteReaderAsync(_ => { }, behavior, ct);
		await cmd.ExecuteReaderAsync(_ => { }, default); // binds the token overload now: the same call, as default is an empty token and the default behavior
		await cmd.ExecuteReaderAsync(_ => { }, 0); // an int still binds the behavior
		Assert.Equal(1, await cmd.ExecuteReaderAsync(_ => 1));
		Assert.Equal(1, await cmd.ExecuteReaderAsync(_ => 1, ct));
		Assert.Equal(
			1, await cmd.ExecuteReaderAsync(_ => new ValueTask<int>(1), ct));
		await cmd.ExecuteReaderAsync(_ => new ValueTask(), ct);
		Assert.Equal(
			7,
			await cmd.ExecuteReaderAsync(
				7, static (_, s) => new ValueTask<int>(s)));
		Assert.Equal(
			7,
			await cmd.ExecuteReaderAsync(
				7,
				static (_, s) => new ValueTask<int>(s),
				behavior));
		Assert.Equal(
			7,
			await cmd.ExecuteReaderAsync(
				7,
				static (_, s) => new ValueTask<int>(s),
				behavior,
				ct));

		// the extensions: the token after the optional parameters
	}
}
#pragma warning restore xUnit1051
