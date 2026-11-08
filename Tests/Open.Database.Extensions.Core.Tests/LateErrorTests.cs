using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A SQL error raised AFTER the rows ("SELECT 1; RAISERROR(...)") is only reported by the provider when the reader moves on (NextResult). A terminal that
// has read to the natural end of the rows must therefore call NextResult once, and one that stops early (the first row, a take, a handler that stops)
// must not: it peeks one more row, and only when there is none is that the end. The fake reader throws from NextResult, like a provider does for such an error.
[ExcludeFromCodeCoverage]
public static class LateErrorTests
{
	sealed class LateException() : Exception("late");

	sealed class Item
	{
		public int Id { get; set; }
	}

	/// <summary>A command whose every execution reads <c>rows</c> rows and then fails in NextResult (unless <c>error</c> is null).</summary>
	sealed class Run
	{
		readonly object?[][] _rows;
		readonly Exception? _error;

		public Run(int rows, Exception? error)
		{
			_rows = [.. Enumerable.Range(1, rows).Select(i => new object?[] { i })];
			_error = error;
			Command = new FakeExecuteReader(NewReader);
			Db = new FakeDbCommand(new FakeDbConnection(), NewReader);
		}

		public FakeExecuteReader Command { get; }
		public FakeDbCommand Db { get; }

		/// <summary>The reader the last execution used.</summary>
		public TypedReader Reader { get; private set; } = null!;

		public static Run Late(int rows) => new(rows, new LateException());
		public static Run Clean(int rows) => new(rows, null);

		public TypedReader NewReader()
		{
			Reader = TypedReader.Create([("Id", typeof(int))], _rows);
			Reader.NextResultError = _error;
			return Reader;
		}
	}

	// ---- every MappedQuery terminal that reads to the end of the rows (the rows asked for are all there is) ----

	public static TheoryData<string, int> SyncCases => new()
	{
		{ "ToList", 2 },
		{ "ToList", 0 },
		{ "ToArray", 2 },
		{ "ToArray", 0 },
		{ "ToImmutableArray", 2 },
		{ "ToDictionary", 2 },
		{ "ToHashSet", 2 },
		{ "FirstOrDefault", 1 },
		{ "FirstOrDefault", 0 },
		{ "SingleOrDefault", 1 },
		{ "SingleOrDefault", 0 },
		{ "Read", 2 },
		{ "ReadResult", 2 },
		{ "Skip(1).ToList", 2 },
		{ "Skip(5).ToList", 2 },
		{ "Skip(1).FirstOrDefault", 2 },
		{ "Take(2).ToList", 2 },
		{ "Take(3).ToList", 2 },
		{ "Take(2).ToArray", 2 },
		{ "Take(1).SingleOrDefault", 1 },
		{ "Skip(1).Take(1).ToList", 2 },
	};

	static void Execute(string terminal, MappedQuery<Item> q)
	{
		switch (terminal)
		{
			case "ToList": q.ToList(); break;
			case "ToArray": q.ToArray(); break;
			case "ToImmutableArray": q.ToImmutableArray(); break;
			case "ToDictionary": q.ToDictionary(i => i.Id); break;
			case "ToHashSet": q.ToHashSet(); break;
			case "FirstOrDefault": q.FirstOrDefault(); break;
			case "SingleOrDefault": q.SingleOrDefault(); break;
			case "Read": q.Read(rows => { foreach (Item _ in rows) { } }); break;
			case "ReadResult": _ = q.Read(rows => rows.Count()); break;
			case "Skip(1).ToList": q.Skip(1).ToList(); break;
			case "Skip(5).ToList": q.Skip(5).ToList(); break;
			case "Skip(1).FirstOrDefault": q.Skip(1).FirstOrDefault(); break;
			case "Take(2).ToList": q.Take(2).ToList(); break;
			case "Take(3).ToList": q.Take(3).ToList(); break;
			case "Take(2).ToArray": q.Take(2).ToArray(); break;
			case "Take(1).SingleOrDefault": q.Take(1).SingleOrDefault(); break;
			case "Skip(1).Take(1).ToList": q.Skip(1).Take(1).ToList(); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal), terminal, null);
		}
	}

	[Theory]
	[MemberData(nameof(SyncCases))]
	public static void Sync_LateError_ReachesTheCaller(string terminal, int rows)
	{
		var run = Run.Late(rows);
		Assert.Throws<LateException>(() => Execute(terminal, run.Command.Map<Item>()));
		Assert.Equal(1, run.Reader.NextResultCalls);
	}

	[Theory]
	[MemberData(nameof(SyncCases))]
	public static void Sync_NoLateError_ResultsAreUnchanged(string terminal, int rows)
	{
		var run = Run.Clean(rows);
		Execute(terminal, run.Command.Map<Item>());
		Assert.Equal(1, run.Reader.NextResultCalls);
	}

	public static TheoryData<string, int> AsyncCases => new()
	{
		{ "ToListAsync", 2 },
		{ "ToListAsync", 0 },
		{ "ToArrayAsync", 2 },
		{ "ToArrayAsync", 0 },
		{ "ToImmutableArrayAsync", 2 },
		{ "ToDictionaryAsync", 2 },
		{ "ToHashSetAsync", 2 },
		{ "FirstOrDefaultAsync", 1 },
		{ "FirstOrDefaultAsync", 0 },
		{ "SingleOrDefaultAsync", 1 },
		{ "SingleOrDefaultAsync", 0 },
		{ "ReadAsync", 2 },
		{ "ReadAsyncResult", 2 },
		{ "Skip(1).ToListAsync", 2 },
		{ "Skip(5).ToListAsync", 2 },
		{ "Skip(1).FirstOrDefaultAsync", 2 },
		{ "Take(2).ToListAsync", 2 },
		{ "Take(3).ToListAsync", 2 },
		{ "Take(2).ToArrayAsync", 2 },
		{ "Take(1).SingleOrDefaultAsync", 1 },
	};

	static async Task ExecuteAsync(string terminal, MappedQuery<Item> q)
	{
		switch (terminal)
		{
			case "ToListAsync": await q.ToListAsync(Ct); break;
			case "ToArrayAsync": await q.ToArrayAsync(Ct); break;
			case "ToImmutableArrayAsync": await q.ToImmutableArrayAsync(Ct); break;
			case "ToDictionaryAsync": await q.ToDictionaryAsync(i => i.Id, null, Ct); break;
			case "ToHashSetAsync": await q.ToHashSetAsync(null, Ct); break;
			case "FirstOrDefaultAsync": await q.FirstOrDefaultAsync(Ct); break;
			case "SingleOrDefaultAsync": await q.SingleOrDefaultAsync(Ct); break;
			case "ReadAsync":
				await q.ReadAsync(async (rows, ct) => { await foreach (Item _ in rows.WithCancellation(ct)) { } }, Ct);
				break;
			case "ReadAsyncResult":
				_ = await q.ReadAsync(async (rows, ct) => { int n = 0; await foreach (Item _ in rows.WithCancellation(ct)) n++; return n; }, Ct);
				break;
			case "Skip(1).ToListAsync": await q.Skip(1).ToListAsync(Ct); break;
			case "Skip(5).ToListAsync": await q.Skip(5).ToListAsync(Ct); break;
			case "Skip(1).FirstOrDefaultAsync": await q.Skip(1).FirstOrDefaultAsync(Ct); break;
			case "Take(2).ToListAsync": await q.Take(2).ToListAsync(Ct); break;
			case "Take(3).ToListAsync": await q.Take(3).ToListAsync(Ct); break;
			case "Take(2).ToArrayAsync": await q.Take(2).ToArrayAsync(Ct); break;
			case "Take(1).SingleOrDefaultAsync": await q.Take(1).SingleOrDefaultAsync(Ct); break;
			default: throw new ArgumentOutOfRangeException(nameof(terminal), terminal, null);
		}
	}

	[Theory]
	[MemberData(nameof(AsyncCases))]
	public static async Task Async_LateError_ReachesTheCaller(string terminal, int rows)
	{
		var run = Run.Late(rows);
		await Assert.ThrowsAsync<LateException>(() => ExecuteAsync(terminal, run.Command.Map<Item>()));
		Assert.Equal(1, run.Reader.NextResultCalls);
	}

	[Theory]
	[MemberData(nameof(AsyncCases))]
	public static async Task Async_NoLateError_ResultsAreUnchanged(string terminal, int rows)
	{
		var run = Run.Clean(rows);
		await ExecuteAsync(terminal, run.Command.Map<Item>());
		Assert.Equal(1, run.Reader.NextResultCalls);
	}

	// ---- a terminal that stops early does not read on and does not move the reader ----

	[Fact]
	public static void FirstOrDefault_PeeksOneMoreRow_AndStaysLazy()
	{
		var run = Run.Late(5);
		Assert.Equal(1, run.Command.Map<Item>().FirstOrDefault()!.Id);
		Assert.Equal(2, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	[Fact]
	public static async Task FirstOrDefaultAsync_PeeksOneMoreRow_AndStaysLazy()
	{
		var run = Run.Late(5);
		Assert.Equal(1, (await run.Command.Map<Item>().FirstOrDefaultAsync(Ct))!.Id);
		Assert.Equal(2, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls + run.Reader.NextResultAsyncCalls);
	}

	[Fact]
	public static void Skip_ThenFirst_PeeksOneMoreRow()
	{
		var run = Run.Late(5);
		Assert.Equal(3, run.Command.Map<Item>().Skip(2).FirstOrDefault()!.Id);
		Assert.Equal(4, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	[Fact]
	public static void Take_PeeksOneMoreRow_AndStaysLazy()
	{
		var run = Run.Late(5);
		Assert.Equal(2, run.Command.Map<Item>().Take(2).ToList().Count);
		Assert.Equal(3, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);

		run = Run.Late(5);
		Assert.Equal(2, run.Command.Map<Item>().Take(2).ToArray().Length);
		Assert.Equal(3, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	[Fact]
	public static async Task Take_Async_PeeksOneMoreRow_AndStaysLazy()
	{
		var run = Run.Late(5);
		Assert.Equal(2, (await run.Command.Map<Item>().Take(2).ToListAsync(Ct)).Count);
		Assert.Equal(3, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);

		run = Run.Late(5);
		Assert.Equal(2, (await run.Command.Map<Item>().Take(2).ToArrayAsync(Ct)).Length);
		Assert.Equal(3, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	[Fact]
	public static void SingleOrDefault_MoreThanOne_ThrowsThatNotTheLateError()
	{
		var run = Run.Late(3);
		Assert.Throws<InvalidOperationException>(() => run.Command.Map<Item>().SingleOrDefault());
		Assert.Equal(2, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	[Fact]
	public static void Handler_ThatStopsEarly_IsNotDrained()
	{
		var run = Run.Late(5);
		int first = run.Command.Map<Item>().Read(rows => rows.First().Id);
		Assert.Equal(1, first);
		Assert.Equal(1, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	[Fact]
	public static async Task HandlerAsync_ThatStopsEarly_IsNotDrained()
	{
		var run = Run.Late(5);
		await run.Command.Map<Item>().ReadAsync(async (rows, ct) =>
		{
			await foreach (Item _ in rows.WithCancellation(ct)) break;
		}, Ct);
		Assert.Equal(1, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	[Fact]
	public static void Take_WithAHandler_ThatEnumeratesAll_PeeksThenEnds()
	{
		var run = Run.Late(5);
		Assert.Equal(2, run.Command.Map<Item>().Take(2).Read(rows => rows.Count()));
		Assert.Equal(3, run.Reader.RowsRead);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	// ---- the command-level terminals of the older API own their reader too ----

	[Fact]
	public static void Results_OfACommand_ReachesTheCaller()
	{
		var run = Run.Late(2);
		Assert.Throws<LateException>(() => run.Db.Results<Item>());
		Assert.Throws<LateException>(() => run.Db.Results(RowMap<Item>.Default));
		Assert.Throws<LateException>(() => run.Command.Results<Item>());
		Assert.Throws<LateException>(() => run.Command.Results(RowMap<Item>.Default));
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ResultsAsync_OfACommand_ReachesTheCaller(bool useReadAsync)
	{
		var run = Run.Late(2);
		await Assert.ThrowsAsync<LateException>(async () => await run.Db.ResultsAsync<Item>(map: null, useReadAsync: useReadAsync, cancellationToken: Ct));
		Assert.Equal(1, run.Reader.NextResultCalls);
		Assert.Equal(useReadAsync ? 1 : 0, run.Reader.NextResultAsyncCalls);
	}

	[Fact]
	public static void Retrieve_OfACommand_ReachesTheCaller()
	{
		Assert.Throws<LateException>(() => Run.Late(2).Db.Retrieve());
		Assert.Throws<LateException>(() => Run.Late(2).Db.Retrieve([0]));
		Assert.Throws<LateException>(() => Run.Late(2).Db.Retrieve(["Id"]));
		Assert.Throws<LateException>(() => Run.Late(2).Command.Retrieve());
		Assert.Throws<LateException>(() => Run.Late(2).Command.Retrieve([0]));
		Assert.Throws<LateException>(() => Run.Late(2).Command.Retrieve(["Id"]));
		Assert.Equal(2, Run.Clean(2).Db.Retrieve().Result.Count);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task RetrieveAsync_OfACommand_ReachesTheCaller(bool useReadAsync)
	{
		await Assert.ThrowsAsync<LateException>(async () => await Run.Late(2).Db.RetrieveAsync(useReadAsync, Ct));
		await Assert.ThrowsAsync<LateException>(async () => await Run.Late(2).Db.RetrieveAsync([0], useReadAsync, Ct));
		await Assert.ThrowsAsync<LateException>(async () => await Run.Late(2).Db.RetrieveAsync(["Id"], false, useReadAsync, Ct));
		Assert.Equal(2, (await Run.Clean(2).Db.RetrieveAsync(useReadAsync, Ct)).Result.Count);
	}

	// ---- a reader the caller holds is left where it stopped: the caller may read the next result set ----

	[Fact]
	public static async Task ReaderLevelHelpers_DoNotMoveTheCallersReader()
	{
		var run = Run.Late(2);
		Assert.Equal(2, run.NewReader().ResultsBuffered<Item>().Count());
		Assert.Equal(0, run.Reader.NextResultCalls);

		Assert.Equal(2, run.NewReader().Results<Item>().Count());
		Assert.Equal(0, run.Reader.NextResultCalls);

		Assert.Equal(2, (await run.NewReader().ResultsBufferedAsync<Item>(Ct)).Count());
		Assert.Equal(0, run.Reader.NextResultCalls);

		int n = 0;
		await foreach (Item _ in run.NewReader().ResultsAsync<Item>(Ct)) n++;
		Assert.Equal(2, n);
		Assert.Equal(0, run.Reader.NextResultCalls);
	}

	// ---- the pipelines: the error faults the target when the call completes it ----

	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A channel completed with an error reports it to the reader once the rows that were written have been read.
	static async Task Drain<T>(System.Threading.Channels.ChannelReader<T> reader)
	{
		await foreach (T _ in reader.ReadAllAsync(Ct)) { }
	}

	[Fact]
	public static async Task Dataflow_ObjectRows_LateErrorFaultsTheTarget()
	{
		var run = Run.Late(2);
		var target = new BufferBlock<object[]>();
		Assert.Throws<LateException>(() => run.NewReader().ToTargetBlock(target, true));
		await Assert.ThrowsAsync<LateException>(() => target.Completion.WaitAsync(Timeout, Ct));

		target = new BufferBlock<object[]>();
		await Assert.ThrowsAsync<LateException>(async () => await run.NewReader().ToTargetBlockAsync(target, true, Ct));
		await Assert.ThrowsAsync<LateException>(() => target.Completion.WaitAsync(Timeout, Ct));

		// Not completing: the caller's target is left alone.
		target = new BufferBlock<object[]>();
		Assert.Throws<LateException>(() => run.NewReader().ToTargetBlock(target, false));
		Assert.False(target.Completion.IsCompleted);
	}

	[Fact]
	public static async Task Dataflow_Mapped_LateErrorFaultsTheTarget()
	{
		var run = Run.Late(2);
		var target = new BufferBlock<Item>();
		Assert.Throws<LateException>(() => run.NewReader().ToTargetBlock<Item>(target, true));
		await Assert.ThrowsAsync<LateException>(() => target.Completion.WaitAsync(Timeout, Ct));

		target = new BufferBlock<Item>();
		await Assert.ThrowsAsync<LateException>(async () => await run.NewReader().ToTargetBlockAsync<Item>(target, true, Ct));
		await Assert.ThrowsAsync<LateException>(() => target.Completion.WaitAsync(Timeout, Ct));

		target = new BufferBlock<Item>();
		Assert.Throws<LateException>(() => run.Command.ToTargetBlock<Item>(target, true));
		await Assert.ThrowsAsync<LateException>(() => target.Completion.WaitAsync(Timeout, Ct));
	}

	[Fact]
	public static async Task Channel_ObjectRows_LateErrorFaultsTheTarget()
	{
		var run = Run.Late(2);
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		await Assert.ThrowsAsync<LateException>(async () => await run.NewReader().ToChannel(channel.Writer, true, Ct));
		await Assert.ThrowsAsync<LateException>(() => Drain(channel.Reader).WaitAsync(Timeout, Ct));

		channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		await Assert.ThrowsAsync<LateException>(async () => await run.NewReader().ToChannelAsync(channel.Writer, true, Ct));
		await Assert.ThrowsAsync<LateException>(() => Drain(channel.Reader).WaitAsync(Timeout, Ct));
	}

	[Fact]
	public static async Task Channel_Mapped_LateErrorFaultsTheTarget()
	{
		var run = Run.Late(2);
		var channel = System.Threading.Channels.Channel.CreateUnbounded<Item>();
		await Assert.ThrowsAsync<LateException>(async () => await run.NewReader().ToChannel<Item>(channel.Writer, true, Ct));
		await Assert.ThrowsAsync<LateException>(() => Drain(channel.Reader).WaitAsync(Timeout, Ct));

		channel = System.Threading.Channels.Channel.CreateUnbounded<Item>();
		await Assert.ThrowsAsync<LateException>(async () => await run.NewReader().ToChannelAsync<Item>(channel.Writer, true, Ct));
		await Assert.ThrowsAsync<LateException>(() => Drain(channel.Reader).WaitAsync(Timeout, Ct));
	}

	[Fact]
	public static async Task Pipelines_WithoutALateError_Complete()
	{
		var run = Run.Clean(2);
		int received = 0;
		var block = new ActionBlock<Item>(_ => received++); // completes once it has handled what it was given
		Assert.Equal(2, run.NewReader().ToTargetBlock<Item>(block, true));
		await block.Completion.WaitAsync(Timeout, Ct);
		Assert.Equal(2, received);

		var channel = System.Threading.Channels.Channel.CreateUnbounded<Item>();
		Assert.Equal(2, await run.NewReader().ToChannelAsync<Item>(channel.Writer, true, Ct));
		await Drain(channel.Reader).WaitAsync(Timeout, Ct);
	}
}
