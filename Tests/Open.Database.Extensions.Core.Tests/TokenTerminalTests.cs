using System.Buffers;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>The async terminals that take a token.</summary>
public enum TokenTerminal
{
	NonQuery, Scalar, ScalarFunc, ScalarFuncValueTask, ScalarT, Return, ReturnT,
	ReaderState, ReaderAction, ReaderFunc, ReaderFuncValueTask, ReaderFuncValueTaskT,
	ReaderActionToken, ReaderFuncToken, ReaderFuncValueTaskToken, ReaderFuncValueTaskTToken,
}

/// <summary>The extension terminals on <see cref="IExecuteReaderAsync"/> that iterate.</summary>
public enum IterateShape { Action, While, WhileAsync }

/// <summary>The Channel overloads on an <c>IExecuteReader</c> source: the first five are <c>ToChannel</c>, the others <c>ToChannelAsync</c>.</summary>
public enum ChannelShape { Plain, Pool, Transform, Mapped, MappedMap, AsyncPlain, AsyncPool, AsyncTransform, AsyncMapped, AsyncMappedMap }

/// <summary>The Dataflow overloads on an <c>IExecuteReader</c> source.</summary>
public enum DataflowShape { Plain, Pool, Transform, Mapped, MappedMap }

// Every kept async terminal takes a CancellationToken, last. It is used together with the command's own token: either cancels,
// one live token is used as it is, two different live tokens are linked (and the linked source is disposed when the call ends), and no token is no allocation.
#pragma warning disable xUnit1051 // These tests are about which token a call is given: no token, one, or two.
[ExcludeFromCodeCoverage]
public static class TokenTerminalTests
{
	static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
	static readonly string[] IdColumn = ["Id"];

	public static TheoryData<TokenTerminal> Terminals => [.. Enum.GetValues<TokenTerminal>()];
	public static TheoryData<IterateShape> Iterates => [.. Enum.GetValues<IterateShape>()];
	public static TheoryData<ChannelShape> Channels => [.. Enum.GetValues<ChannelShape>()];
	public static TheoryData<DataflowShape> Dataflows => [.. Enum.GetValues<DataflowShape>()];

	// ---- the command terminals ----

	static ExpressiveDbCommand Make(TokenRig rig, ConnectionState state = ConnectionState.Closed)
		=> new(new TokenConnection(rig, state), CommandType.Text, "SELECT Id");

	static async ValueTask Invoke(ExpressiveDbCommand cmd, TokenTerminal terminal, CancellationToken token)
	{
		switch (terminal)
		{
			case TokenTerminal.NonQuery: await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false); break;
			case TokenTerminal.Scalar: await cmd.ExecuteScalarAsync(token).ConfigureAwait(false); break;
			case TokenTerminal.ScalarFunc: await cmd.ExecuteScalarAsync(static _ => 1, token).ConfigureAwait(false); break;
			case TokenTerminal.ScalarFuncValueTask: await cmd.ExecuteScalarAsync(static _ => new ValueTask<int>(1), token).ConfigureAwait(false); break;
			case TokenTerminal.ScalarT: await cmd.ExecuteScalarAsync<int>(token).ConfigureAwait(false); break;
			case TokenTerminal.Return: await cmd.ExecuteReturnAsync(token).ConfigureAwait(false); break;
			case TokenTerminal.ReturnT: await cmd.ExecuteReturnAsync<int>(token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderState: await cmd.ExecuteReaderAsync(1, static (_, s) => new ValueTask<int>(s), CommandBehavior.Default, token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderAction: await cmd.ExecuteReaderAsync(static _ => { }, CommandBehavior.Default, token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderFunc: await cmd.ExecuteReaderAsync(static _ => 1, CommandBehavior.Default, token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderFuncValueTask: await cmd.ExecuteReaderAsync(static _ => new ValueTask(), CommandBehavior.Default, token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderFuncValueTaskT: await cmd.ExecuteReaderAsync(static _ => new ValueTask<int>(1), CommandBehavior.Default, token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderActionToken: await cmd.ExecuteReaderAsync(static _ => { }, token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderFuncToken: await cmd.ExecuteReaderAsync(static _ => 1, token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderFuncValueTaskToken: await cmd.ExecuteReaderAsync(static _ => new ValueTask(), token).ConfigureAwait(false); break;
			case TokenTerminal.ReaderFuncValueTaskTToken: await cmd.ExecuteReaderAsync(static _ => new ValueTask<int>(1), token).ConfigureAwait(false); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal));
		}
	}

	static Task Call(ExpressiveDbCommand cmd, TokenTerminal terminal, CancellationToken token) => Invoke(cmd, terminal, token).AsTask();

	static bool IsReader(TokenTerminal t) => t >= TokenTerminal.ReaderState;

	static async Task Cancels(Task task)
		=> await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task.WaitAsync(Patience));

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task A_CancelledCallToken_ThrowsBeforeAnythingIsOpened(TokenTerminal terminal)
	{
		var rig = new TokenRig();
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		await Cancels(Call(Make(rig), terminal, cts.Token));
		Assert.Empty(rig.Log);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task A_CallTokenCancelledWhileStalledInTheOpen_Cancels(TokenTerminal terminal)
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
	public static async Task B_CommandTokenStillCancels_WithALiveCallToken(TokenTerminal terminal)
	{
		var rig = new TokenRig { Stall = TokenStage.Open };
		using var commandSource = new CancellationTokenSource();
		using var callSource = new CancellationTokenSource();
		Task task = Call(Make(rig).UseCancellationToken(commandSource.Token), terminal, callSource.Token);
		await rig.Entered.Task.WaitAsync(Patience);
		commandSource.Cancel();
		await Cancels(task);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task C_BothLive_TheStepSeesTheLinkedToken_AndEitherCancels(TokenTerminal terminal)
	{
		foreach (bool cancelCall in new[] { true, false })
		{
			var rig = new TokenRig { Stall = TokenStage.Open };
			using var commandSource = new CancellationTokenSource();
			using var callSource = new CancellationTokenSource();
			Task task = Call(Make(rig).UseCancellationToken(commandSource.Token), terminal, callSource.Token);
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
	public static async Task D_NoToken_EveryStepSeesATokenThatCannotBeCancelled(TokenTerminal terminal)
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
		if (IsReader(terminal)) Assert.Equal(cts.Token, Assert.Single(rig.Execute));

		rig = new TokenRig();
		await Call(Make(rig).UseCancellationToken(cts.Token), terminal, cts.Token); // the same token twice
		Assert.Equal(cts.Token, Assert.Single(rig.Open));
	}

	[Theory]
	[InlineData(TokenTerminal.ReaderState)]
	[InlineData(TokenTerminal.ReaderAction)]
	[InlineData(TokenTerminal.ReaderFuncValueTaskTToken)]
	public static async Task Reader_CallTokenCancelledWhileStalledInTheExecute_Cancels(TokenTerminal terminal)
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
		Assert.Throws<OperationCanceledException>(() => _ = Make(rig).ExecuteReturnAsync(cts.Token));
		Assert.Throws<OperationCanceledException>(() => _ = Make(rig).UseCancellationToken(cts.Token).ExecuteReturnAsync());
#pragma warning restore CA2012
		Assert.Empty(rig.Log);
	}

	// A call through the class combines the two tokens; through IExecuteReader the token passed replaces the command's own.
	[Fact]
	public static async Task InterfaceRoute_ReplacesTheCommandsToken_WhileTheClassCombinesThem()
	{
		using var commandSource = new CancellationTokenSource();
		using var callSource = new CancellationTokenSource();

		var viaInterface = new TokenRig();
		IExecuteReader asInterface = Make(viaInterface).UseCancellationToken(commandSource.Token);
		await asInterface.ExecuteReaderAsync(1, static (_, s) => new ValueTask<int>(s), CommandBehavior.Default, callSource.Token);
		Assert.Equal(callSource.Token, Assert.Single(viaInterface.Open));

		var viaClass = new TokenRig();
		await Make(viaClass).UseCancellationToken(commandSource.Token).ExecuteReaderAsync(1, static (_, s) => new ValueTask<int>(s), CommandBehavior.Default, callSource.Token);
		CancellationToken seen = Assert.Single(viaClass.Open);
		Assert.NotEqual(callSource.Token, seen);
		Assert.NotEqual(commandSource.Token, seen);

		// and the interface members without a token still use the command's own
		var plain = new TokenRig();
		await ((IExecuteReader)Make(plain).UseCancellationToken(commandSource.Token)).ExecuteReaderAsync(1, static (_, s) => new ValueTask<int>(s));
		Assert.Equal(commandSource.Token, Assert.Single(plain.Open));
	}

	// ---- overload resolution: a later change breaks this build, not a user ----

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
		Assert.Equal(1, await cmd.ExecuteScalarAsync(_ => new ValueTask<int>(1)));
		Assert.Equal(1, await cmd.ExecuteScalarAsync(_ => 1, ct));
		Assert.Equal(1, await cmd.ExecuteScalarAsync(_ => new ValueTask<int>(1), ct));
		Assert.Equal(0, await cmd.ExecuteScalarAsync<int>());
		Assert.Equal(0, await cmd.ExecuteScalarAsync<int>(ct));
		Assert.Equal(0, await cmd.ExecuteScalarAsync<int>(CancellationToken.None));
		await cmd.ExecuteReturnAsync();
		await cmd.ExecuteReturnAsync(ct);
		await cmd.ExecuteReturnAsync<int>(ct);

		// the reader terminals with 1, 2, 3 and 4 arguments, lambdas and static lambdas
		await cmd.ExecuteReaderAsync(_ => { });
		await cmd.ExecuteReaderAsync(_ => { }, behavior);
		await cmd.ExecuteReaderAsync(_ => { }, ct);
		await cmd.ExecuteReaderAsync(_ => { }, behavior, ct);
		await cmd.ExecuteReaderAsync(_ => { }, default); // binds the token overload now: the same call, as default is an empty token and the default behavior
		await cmd.ExecuteReaderAsync(_ => { }, 0); // an int still binds the behavior
		Assert.Equal(1, await cmd.ExecuteReaderAsync(_ => 1));
		Assert.Equal(1, await cmd.ExecuteReaderAsync(_ => 1, ct));
		Assert.Equal(1, await cmd.ExecuteReaderAsync(_ => new ValueTask<int>(1), ct));
		await cmd.ExecuteReaderAsync(_ => new ValueTask(), ct);
		Assert.Equal(7, await cmd.ExecuteReaderAsync(7, static (_, s) => new ValueTask<int>(s)));
		Assert.Equal(7, await cmd.ExecuteReaderAsync(7, static (_, s) => new ValueTask<int>(s), behavior));
		Assert.Equal(7, await cmd.ExecuteReaderAsync(7, static (_, s) => new ValueTask<int>(s), behavior, ct));

		// the extensions: the token after the optional parameters
		await cmd.IterateReaderAsync(_ => { });
		await cmd.IterateReaderAsync(_ => { }, behavior, ct);
		await cmd.IterateReaderAsync(_ => { }, cancellationToken: ct);
		await cmd.IterateReaderWhileAsync(_ => true, cancellationToken: ct);
		await cmd.IterateReaderWhileAsync(_ => new ValueTask<bool>(true), cancellationToken: ct);

		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		await cmd.ToChannel(channel.Writer, false);
		await cmd.ToChannel(channel.Writer, false, ct);
		await cmd.ToChannel(channel.Writer, false, CancellationToken.None);
		await cmd.ToChannel(channel.Writer, false, ArrayPool<object>.Shared, ct);
		await cmd.ToChannelAsync(channel.Writer, false, ct);
		await cmd.ToChannelAsync(channel.Writer, false, CancellationToken.None);
		await cmd.ToChannelAsync(channel.Writer, false, (ArrayPool<object>?)null, ct);

		// A literal default: the token overload has no default to fill, so it wins. ToChannel(..., false, default) used to bind the non-null
		// ArrayPool overload and throw; it now binds the token overload and runs. ToChannelAsync(..., false, default) used to bind the nullable-pool
		// overload; it now binds the token one, the same call. A literal null as that argument is ambiguous with the transform overloads (it did not compile before either), so it is not tested.
		long plainCount = await cmd.ToChannel(channel.Writer, false);
		Assert.Equal(plainCount, await cmd.ToChannel(channel.Writer, false, default));
		Assert.Equal(plainCount, await cmd.ToChannelAsync(channel.Writer, false, default));

		var block = new BufferBlock<object[]>();
		await cmd.ToTargetBlockAsync(block, false);
		await cmd.ToTargetBlockAsync(block, false, ct);
		Assert.NotNull(cmd.AsSourceBlockAsync());
		Assert.NotNull(cmd.AsSourceBlockAsync(null, ct));
		Assert.NotNull(cmd.AsSourceBlockAsync(ArrayPool<object>.Shared, null, ct));
		Assert.NotNull(cmd.AsSourceBlockAsync(static _ => 1, null, ct));
		Assert.NotNull(cmd.AsSourceBlockAsync<int>(null, ct));
		Assert.NotNull(cmd.AsSourceBlockAsync((RowMap<TokenItem>?)null, null, ct));
	}

	// A null command is a precondition: the ValueTask<long> overloads throw at the call (not through the returned task). A null target is reported through the task.
	static void Drop(ValueTask<long> task) => GC.KeepAlive(task);

	[Fact]
	public static async Task Channel_NullCommand_ThrowsAtTheCall_NullTarget_ThroughTheTask()
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		var mapped = System.Threading.Channels.Channel.CreateUnbounded<TokenItem>();
		IExecuteReader? none = null;
		IExecuteReaderAsync? noneAsync = null;

		Assert.Throws<ArgumentNullException>(() => Drop(none!.ToChannel(channel.Writer, false)));
		Assert.Throws<ArgumentNullException>(() => Drop(none!.ToChannel(mapped.Writer, false)));
		Assert.Throws<ArgumentNullException>(() => Drop(noneAsync!.ToChannelAsync(channel.Writer, false)));
		Assert.Throws<ArgumentNullException>(() => Drop(noneAsync!.ToChannelAsync(mapped.Writer, false)));

		ExpressiveDbCommand cmd = Make(new TokenRig { Recording = false }, ConnectionState.Open);
		ValueTask<long> viaTask = cmd.ToChannel<TokenItem>(null!, false);
		await Assert.ThrowsAsync<ArgumentNullException>(async () => await viaTask);
	}

	// ---- the iterating extensions ----

	static RecordingAsyncExecuteReader Rows(int count = 3, bool asyncRead = false)
		=> new(IdColumn, [.. Enumerable.Range(1, count).Select(i => (object?[])[i])]) { UseAsyncRead = asyncRead };

	static Task Iterate(IterateShape shape, IExecuteReaderAsync cmd, Action? each, CancellationToken token) => shape switch
	{
		IterateShape.Action => cmd.IterateReaderAsync(_ => each?.Invoke(), CommandBehavior.Default, token).AsTask(),
		IterateShape.While => cmd.IterateReaderWhileAsync(_ => { each?.Invoke(); return true; }, CommandBehavior.Default, token).AsTask(),
		IterateShape.WhileAsync => cmd.IterateReaderWhileAsync(_ => { each?.Invoke(); return new ValueTask<bool>(true); }, CommandBehavior.Default, token).AsTask(),
		_ => throw new ArgumentOutOfRangeException(nameof(shape)),
	};

	[Theory]
	[MemberData(nameof(Iterates))]
	public static async Task Iterate_Cancellation(IterateShape shape)
	{
		using var callSource = new CancellationTokenSource();

		// cancelled up front: through either token
		RecordingAsyncExecuteReader cmd = Rows();
		callSource.Cancel();
		await Cancels(Iterate(shape, cmd, null, callSource.Token));
		Assert.True(Assert.Single(cmd.ReaderTokens).IsCancellationRequested);

		cmd = Rows();
		cmd.CancellationToken = callSource.Token;
		await Cancels(Iterate(shape, cmd, null, default));

		// no token
		cmd = Rows();
		await Iterate(shape, cmd, null, default);
		Assert.False(Assert.Single(cmd.ReaderTokens).CanBeCanceled);
	}

	// Both live: the execute and every read of the loop get the one linked token; cancelling either stops a read that is waiting.
	[Theory]
	[MemberData(nameof(Iterates))]
	public static async Task Iterate_BothLive_TheReadLoopUsesTheLinkedToken(IterateShape shape)
	{
		using var commandSource = new CancellationTokenSource();
		using var callSource = new CancellationTokenSource();
		var rig = new TokenRig { Rows = 2 };
		await Iterate(shape, Make(rig, ConnectionState.Open).EnableAsyncRead().UseCancellationToken(commandSource.Token), null, callSource.Token);
		CancellationToken seen = Assert.Single(rig.Execute);
		Assert.NotEqual(callSource.Token, seen);
		Assert.NotEqual(commandSource.Token, seen);
		Assert.NotEmpty(rig.Read);
		Assert.All(rig.Read, t => Assert.Equal(seen, t));

		foreach (bool cancelCall in new[] { true, false })
		{
			using var commandSource2 = new CancellationTokenSource();
			using var callSource2 = new CancellationTokenSource();
			var stalled = new TokenRig { Stall = TokenStage.Read };
			Task task = Iterate(shape, Make(stalled, ConnectionState.Open).EnableAsyncRead().UseCancellationToken(commandSource2.Token), null, callSource2.Token);
			await stalled.Entered.Task.WaitAsync(Patience);
			(cancelCall ? callSource2 : commandSource2).Cancel();
			await Cancels(task);
		}
	}

	// ---- Channel ----

	static readonly Func<IDataRecord, TokenItem> ToItem = static r => new TokenItem { Id = r.GetInt32(0) };

	// Runs a ValueTask<long> overload; the writer for the shape is created by the caller.
	static Task<long> CallChannel(ChannelShape shape, IExecuteReaderAsync c, Channel<object[]> objects, Channel<TokenItem> items, CancellationToken token) => shape switch
	{
		ChannelShape.Plain => c.ToChannel(objects.Writer, false, token).AsTask(),
		ChannelShape.Pool => c.ToChannel(objects.Writer, false, ArrayPool<object>.Shared, token).AsTask(),
		ChannelShape.Transform => c.ToChannel(items.Writer, false, ToItem, token).AsTask(),
		ChannelShape.Mapped => c.ToChannel(items.Writer, false, token).AsTask(),
		ChannelShape.MappedMap => c.ToChannel(items.Writer, false, (RowMap<TokenItem>?)null, token).AsTask(),
		ChannelShape.AsyncPlain => c.ToChannelAsync(objects.Writer, false, token).AsTask(),
		ChannelShape.AsyncPool => c.ToChannelAsync(objects.Writer, false, ArrayPool<object>.Shared, token).AsTask(),
		ChannelShape.AsyncTransform => c.ToChannelAsync(items.Writer, false, ToItem, token).AsTask(),
		ChannelShape.AsyncMapped => c.ToChannelAsync(items.Writer, false, token).AsTask(),
		ChannelShape.AsyncMappedMap => c.ToChannelAsync(items.Writer, false, (RowMap<TokenItem>?)null, token).AsTask(),
		_ => throw new ArgumentOutOfRangeException(nameof(shape)),
	};

	static bool IsMapped(ChannelShape s) => s is ChannelShape.Mapped or ChannelShape.MappedMap or ChannelShape.AsyncMapped or ChannelShape.AsyncMappedMap;

	[Theory]
	[MemberData(nameof(Channels))]
	public static async Task Channel_CancelledUpFront_ThroughEitherToken(ChannelShape shape)
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		RecordingAsyncExecuteReader c = Rows();
		await Cancels(CallChannel(shape, c, System.Threading.Channels.Channel.CreateUnbounded<object[]>(), System.Threading.Channels.Channel.CreateUnbounded<TokenItem>(), cts.Token));
		Assert.Empty(c.ReaderTokens);

		c = Rows();
		c.CancellationToken = cts.Token;
		await Cancels(CallChannel(shape, c, System.Threading.Channels.Channel.CreateUnbounded<object[]>(), System.Threading.Channels.Channel.CreateUnbounded<TokenItem>(), default));
		Assert.Empty(c.ReaderTokens);
	}

	[Theory]
	[MemberData(nameof(Channels))]
	public static async Task Channel_BothLive_TheReaderSeesALinkedToken_AndEitherCancels(ChannelShape shape)
	{
		foreach (bool cancelCall in new[] { true, false })
		{
			using var commandSource = new CancellationTokenSource();
			using var callSource = new CancellationTokenSource();
			RecordingAsyncExecuteReader c = Rows();
			c.CancellationToken = commandSource.Token;
			c.Stall = true;
			Task<long> task = CallChannel(shape, c, System.Threading.Channels.Channel.CreateUnbounded<object[]>(), System.Threading.Channels.Channel.CreateUnbounded<TokenItem>(), callSource.Token);
			await c.Entered.Task.WaitAsync(Patience);
			CancellationToken seen = Assert.Single(c.ReaderTokens);
			Assert.True(seen.CanBeCanceled);
			Assert.NotEqual(callSource.Token, seen);
			Assert.NotEqual(commandSource.Token, seen);
			(cancelCall ? callSource : commandSource).Cancel();
			await Cancels(task);
		}
	}

	[Theory]
	[MemberData(nameof(Channels))]
	public static async Task Channel_NoToken_RunsToTheEnd(ChannelShape shape)
	{
		RecordingAsyncExecuteReader c = Rows();
		var objects = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		var items = System.Threading.Channels.Channel.CreateUnbounded<TokenItem>();
		Assert.Equal(3, await CallChannel(shape, c, objects, items, default).WaitAsync(Patience));
		// the mapped overloads run a pipeline of their own, which has a token of its own
		if (!IsMapped(shape)) Assert.False(Assert.Single(c.ReaderTokens).CanBeCanceled);
	}

	[Theory]
	[MemberData(nameof(Channels))]
	public static async Task Channel_TheWaitForRoom_HonorsTheCallToken(ChannelShape shape)
	{
		var objects = System.Threading.Channels.Channel.CreateBounded<object[]>(1);
		var items = System.Threading.Channels.Channel.CreateBounded<TokenItem>(1);
		Assert.True(objects.Writer.TryWrite([0]));
		Assert.True(items.Writer.TryWrite(new TokenItem()));
		using var cts = new CancellationTokenSource();
		RecordingAsyncExecuteReader c = Rows();
		Task<long> task = CallChannel(shape, c, objects, items, cts.Token);
		await Task.Delay(100);
		Assert.False(task.IsCompleted);
		cts.Cancel();
		await Cancels(task);
		Assert.Empty(c.ReaderTokens);
	}

	static ChannelReader<object> ReaderOf(ChannelShape shape, RecordingAsyncExecuteReader c, CancellationToken token)
	{
		static ChannelReader<object> Box<T>(ChannelReader<T> r) where T : notnull => new BoxingReader<T>(r);
		return shape switch
		{
			ChannelShape.Plain => Box(c.ToChannel(true, token)),
			ChannelShape.Pool => Box(c.ToChannel(true, ArrayPool<object>.Shared, token)),
			ChannelShape.Transform => Box(c.ToChannel(true, ToItem, token)),
			ChannelShape.Mapped => Box(c.ToChannel<TokenItem>(true, token)),
			ChannelShape.MappedMap => Box(c.ToChannel(true, (RowMap<TokenItem>?)null, token)),
			ChannelShape.AsyncPlain => Box(c.ToChannelAsync(true, token)),
			ChannelShape.AsyncPool => Box(c.ToChannelAsync(true, ArrayPool<object>.Shared, token)),
			ChannelShape.AsyncTransform => Box(c.ToChannelAsync(true, ToItem, token)),
			ChannelShape.AsyncMapped => Box(c.ToChannelAsync<TokenItem>(true, token)),
			ChannelShape.AsyncMappedMap => Box(c.ToChannelAsync(true, (RowMap<TokenItem>?)null, token)),
			_ => throw new ArgumentOutOfRangeException(nameof(shape)),
		};
	}

	// Presents a ChannelReader<T> as a ChannelReader<object> to the test.
	sealed class BoxingReader<T>(ChannelReader<T> inner) : ChannelReader<object> where T : notnull
	{
		public override Task Completion => inner.Completion;
		public override bool TryRead([MaybeNullWhen(false)] out object item)
		{
			if (inner.TryRead(out T? value))
			{
				item = value;
				return true;
			}

			item = null;
			return false;
		}

		public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) => inner.WaitToReadAsync(cancellationToken);
	}

	// A token cancelled WHILE reading completes the channel the call created, with the cancellation.
	// (A token already cancelled before the read starts leaves such a channel open today: that is a separate change, so it is not tested here.)
	[Theory]
	[MemberData(nameof(Channels))]
	public static async Task ChannelReaderOverloads_CallTokenCancelledWhileReading_CompletesTheChannelAsCancelled(ChannelShape shape)
	{
		using var cts = new CancellationTokenSource();
		RecordingAsyncExecuteReader c = Rows();
		c.Stall = true;
		ChannelReader<object> reader = ReaderOf(shape, c, cts.Token);
		await c.Entered.Task.WaitAsync(Patience);
		cts.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
		{
			await foreach (object _ in reader.ReadAllAsync().WithCancellation(default)) { }
		}).WaitAsync(Patience);
		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.Completion.WaitAsync(Patience));
		Assert.True(reader.Completion.IsCanceled);
	}

	[Theory]
	[MemberData(nameof(Channels))]
	public static async Task ChannelReaderOverloads_CommandTokenCancelledWhileReading_CompletesTheChannelAsCancelled(ChannelShape shape)
	{
		using var commandSource = new CancellationTokenSource();
		RecordingAsyncExecuteReader c = Rows();
		c.CancellationToken = commandSource.Token;
		c.Stall = true;
		ChannelReader<object> reader = ReaderOf(shape, c, default);
		await c.Entered.Task.WaitAsync(Patience);
		commandSource.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.Completion.WaitAsync(Patience));
		Assert.True(reader.Completion.IsCanceled);
	}

	// ---- Dataflow ----

	static Task<long> CallDataflow(DataflowShape shape, RecordingAsyncExecuteReader c, CancellationToken token) => shape switch
	{
		DataflowShape.Plain => c.ToTargetBlockAsync(new BufferBlock<object[]>(), true, token).AsTask(),
		DataflowShape.Pool => c.ToTargetBlockAsync(new BufferBlock<object[]>(), true, ArrayPool<object>.Shared, token).AsTask(),
		DataflowShape.Transform => c.ToTargetBlockAsync(new BufferBlock<TokenItem>(), true, ToItem, token).AsTask(),
		DataflowShape.Mapped => c.ToTargetBlockAsync(new BufferBlock<TokenItem>(), true, token).AsTask(),
		DataflowShape.MappedMap => c.ToTargetBlockAsync(new BufferBlock<TokenItem>(), true, (RowMap<TokenItem>?)null, token).AsTask(),
		_ => throw new ArgumentOutOfRangeException(nameof(shape)),
	};

	[Theory]
	[MemberData(nameof(Dataflows))]
	public static async Task Dataflow_Cancellation(DataflowShape shape)
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		RecordingAsyncExecuteReader c = Rows();
		await Cancels(CallDataflow(shape, c, cts.Token));
		Assert.True(Assert.Single(c.ReaderTokens).IsCancellationRequested);

		c = Rows();
		c.CancellationToken = cts.Token;
		await Cancels(CallDataflow(shape, c, default));

		foreach (bool cancelCall in new[] { true, false })
		{
			using var commandSource = new CancellationTokenSource();
			using var callSource = new CancellationTokenSource();
			c = Rows();
			c.CancellationToken = commandSource.Token;
			c.Stall = true;
			Task<long> task = CallDataflow(shape, c, callSource.Token);
			await c.Entered.Task.WaitAsync(Patience);
			CancellationToken seen = Assert.Single(c.ReaderTokens);
			Assert.True(seen.CanBeCanceled);
			Assert.NotEqual(callSource.Token, seen);
			Assert.NotEqual(commandSource.Token, seen);
			(cancelCall ? callSource : commandSource).Cancel();
			await Cancels(task);
			Assert.Throws<ObjectDisposedException>(() => seen.WaitHandle);
		}

		c = Rows();
		Assert.Equal(3, await CallDataflow(shape, c, default).WaitAsync(Patience));
		Assert.False(Assert.Single(c.ReaderTokens).CanBeCanceled);
	}

	[Fact]
	public static async Task AsSourceBlockAsync_PassesTheCallToken_OnToTheRead()
	{
		using var cts = new CancellationTokenSource();
		RecordingAsyncExecuteReader c = Rows();
		c.Stall = true;
		IReceivableSourceBlock<object[]> block = c.AsSourceBlockAsync(null, cts.Token);
		await c.Entered.Task.WaitAsync(Patience);
		Assert.Equal(cts.Token, Assert.Single(c.ReaderTokens));
		cts.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => block.Completion.WaitAsync(Patience));
	}

	// ---- allocation: a call with no token, or one token, or the same token twice, costs what the call with no token costs ----

	// Bytes per call: after a warm-up, the smallest of three rounds of 1,000 calls (the allocated-bytes counter of this thread is exact).
	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 300; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 7; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 1000; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 1000);
		}

		return best;
	}

	const int Slack = 16;

	// Compares the variants of one call, in the same run, so the budget follows the runtime and the configuration.
	// (The calls that hop threads, such as the mapped ones and Dataflow with its Task.Yield, are missed by the per-thread counter and are not measured.)
	static void AssertNoMoreThanWithoutAToken(string what, Func<CancellationToken, CancellationToken, Action> make)
	{
		using var cts = new CancellationTokenSource();
		// A call token alone must cost what the command's token alone always cost (a live token costs a little inside the channel wait, whatever passes it).
		// Each variant is measured twice and the smaller kept, so that one disturbed run (a collection, a busy machine) does not decide the test.
		long Measure(CancellationToken commandToken, CancellationToken callToken) => Math.Min(BytesPerCall(make(commandToken, callToken)), BytesPerCall(make(commandToken, callToken)));
		long none = Measure(default, default);
		long callOnly = Measure(default, cts.Token);
		long commandOnly = Measure(cts.Token, default);
		long same = Measure(cts.Token, cts.Token);
		Assert.True(none <= commandOnly + Slack, $"{what}: no token allocates {none}, one command token {commandOnly}");
		Assert.True(callOnly <= commandOnly + Slack, $"{what}: one call token allocates {callOnly}, one command token {commandOnly}");
		Assert.True(same <= commandOnly + Slack, $"{what}: the same token twice allocates {same}, one command token {commandOnly}");
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static void Allocation_Terminals(TokenTerminal terminal)
		=> AssertNoMoreThanWithoutAToken(terminal.ToString(), (commandToken, callToken) =>
		{
			var rig = new TokenRig { Recording = false, Rows = 0 };
			ExpressiveDbCommand cmd = Make(rig, ConnectionState.Open).UseCancellationToken(commandToken);
			return () => Invoke(cmd, terminal, callToken).AsTask().GetAwaiter().GetResult();
		});

	[Theory]
	[MemberData(nameof(Iterates))]
	public static void Allocation_Iterate(IterateShape shape)
		=> AssertNoMoreThanWithoutAToken(shape.ToString(), (commandToken, callToken) =>
		{
			// The command of the token rig, not the big fake reader: its few bytes per call are what the counter can tell apart.
			ExpressiveDbCommand cmd = Make(new TokenRig { Recording = false, Rows = 0 }, ConnectionState.Open).UseCancellationToken(commandToken);
			return () => Iterate(shape, cmd, null, callToken).GetAwaiter().GetResult();
		});

	[Theory]
	[InlineData(ChannelShape.Plain)]
	[InlineData(ChannelShape.Pool)]
	[InlineData(ChannelShape.Transform)]
	[InlineData(ChannelShape.AsyncPlain)]
	[InlineData(ChannelShape.AsyncPool)]
	[InlineData(ChannelShape.AsyncTransform)]
	public static void Allocation_Channel(ChannelShape shape)
		=> AssertNoMoreThanWithoutAToken(shape.ToString(), (commandToken, callToken) =>
		{
			ExpressiveDbCommand cmd = Make(new TokenRig { Recording = false, Rows = 0 }, ConnectionState.Open).UseCancellationToken(commandToken);
			var objects = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
			var items = System.Threading.Channels.Channel.CreateUnbounded<TokenItem>();
			return () => CallChannel(shape, cmd, objects, items, callToken).GetAwaiter().GetResult();
		});
}
#pragma warning restore xUnit1051
