using System.Buffers;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Collections.Concurrent;
using System.Data.Common;
using static Open.Database.Extensions.Tests.ChannelWait;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The user's journeys through the buffer terminals and the terminals
// they are to replace, each against the real public members, the fake
// provider and a counting pool. Every journey asserts what it read,
// that every rent came back exactly once, that the connection was
// closed once, that no task was left unobserved, and it never waits
// without a limit.
[ExcludeFromCodeCoverage]
public static class PooledBufferJourneyTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// ---- the rig ---------------------------------------------------------

	// A reader of Id 1..N with simulated lag: every `batch` rows it
	// waits `lag` (awaitable, and cancelled by the token it was given).
	// `onRead` sees each row number as it is read (to cancel, to hold),
	// and `failAtRow` makes that read throw.
	sealed class LagReader(
		int rows,
		TimeSpan lag = default,
		int batch = 1,
		Action<int>? onRead = null,
		int failAtRow = 0)
		: TypedReader(Columns, Rows(rows))
	{
		int _reads;
		int _calls;
		int _disposes;
		volatile bool _cancelSeen;

		public int Reads => Volatile.Read(ref _reads);

		public bool CancelSeen => _cancelSeen;

		public int Disposes => Volatile.Read(ref _disposes);

		public override bool Read()
		{
			int n = Interlocked.Increment(ref _reads);
			onRead?.Invoke(n);
			if (n == failAtRow)
				throw new InvalidOperationException("journey fault");

			return base.Read();
		}

		public override async Task<bool> ReadAsync(
			CancellationToken cancellationToken)
		{
			int call = Interlocked.Increment(ref _calls);
			try
			{
				if (lag > TimeSpan.Zero && call > 1 && (call - 1) % batch == 0)
					await Task.Delay(lag, cancellationToken);
				else
					cancellationToken.ThrowIfCancellationRequested();
			}
			catch (OperationCanceledException)
			{
				_cancelSeen = true;
				throw;
			}

			return await base.ReadAsync(cancellationToken);
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing) Interlocked.Increment(ref _disposes);
			base.Dispose(disposing);
		}
	}

	sealed class Journey : IDisposable
	{
		readonly List<Exception> _unobserved = [];
		int _executes;

		public CountingPool<ChannelRow> Pool { get; } = new();

		public FakeDbConnection Connection { get; } = new();

		public MappedCommand<ChannelRow> View { get; }

		public DbDataReader? Reader { get; private set; }

		public int Executes => Volatile.Read(ref _executes);

		public bool Closed => Connection.Log.Contains("close");

		public Journey(Func<DbDataReader> make, bool asyncReads = true)
		{
			DbDataReader Make()
			{
				Interlocked.Increment(ref _executes);
				return Reader = make();
			}

			var view = new FakeDbCommand(Connection, Make)
				.Map<ChannelRow>();
			View = asyncReads ? view.AsyncReads() : view;
			TaskScheduler.UnobservedTaskException += OnUnobserved;
		}

		public static Journey Rows(int n)
			=> new(() => new LagReader(n));

		void OnUnobserved(
			object? sender,
			UnobservedTaskExceptionEventArgs e)
		{
			lock (_unobserved) _unobserved.Add(e.Exception);
		}

		public void Dispose()
			=> TaskScheduler.UnobservedTaskException -= OnUnobserved;

		// What every journey ends with: the connection closed once,
		// every rent back once, nothing unobserved.
		public async Task AssertClean(bool executed = true)
		{
			if (executed)
			{
				await Until(
					() => Closed && Pool.Outstanding == 0,
					"the connection closed and the arrays back");
			}

			Assert.Equal(
				executed ? 1 : 0,
				Connection.Log.Count(l => l == "close"));
			Assert.Equal(executed ? 1 : 0, Executes);
			Assert.Equal(Pool.Rents, Pool.Returns);
			Assert.Equal(0, Pool.Unmatched);
			Assert.Equal(0, Pool.Outstanding);

			AssertNoUnobserved();
		}

		public void AssertNoUnobserved()
		{
			for (int i = 0; i < 3; ++i)
			{
				GC.Collect();
				GC.WaitForPendingFinalizers();
			}

			lock (_unobserved)
			{
				Assert.DoesNotContain(
					_unobserved,
					e => e.ToString().Contains("journey fault"));
			}
		}
	}

	sealed class Outcome(List<int> ids, Exception? error)
	{
		public List<int> Ids { get; } = ids;

		public Exception? Error { get; } = error;
	}

	public static TheoryData<string> Terminals
		=> ["stream", "buffered", "buffer"];

	public static TheoryData<int> Sizes
		=> [0, 1, 16, 17, 100, 3500];

	public static TheoryData<string, int> TerminalsAndSizes
	{
		get
		{
			TheoryData<string, int> data = [];
			foreach (string t in new[] { "stream", "buffered", "buffer" })
			{
				foreach (int n in new[] { 0, 1, 16, 17, 100, 3500 })
					data.Add(t, n);
			}

			return data;
		}
	}

	// Reads a terminal to its end (or its break, or its failure).
	// The stream and the buffered stream may break after some rows;
	// a consumer lag slows the consumer down.
	static async Task<Outcome> Drive(
		string terminal,
		Journey j,
		CancellationToken? token = null,
		int breakAfter = 0,
		TimeSpan consumerLag = default)
	{
		List<int> ids = [];
		Exception? error = null;
		try
		{
			switch (terminal)
			{
				case "buffer":
					{
						await using var buffer = j.View
							.ToAsyncBuffer(j.Pool, token ?? Ct);
						if (consumerLag > TimeSpan.Zero)
							await Task.Delay(consumerLag, Ct);

						var items = await buffer.Items.Within();
						ids.AddRange(items.Select(r => r.Id));
						break;
					}

				case "stream":
					await foreach (var chunk in j.View
						.ToAsyncBufferStream(j.Pool, token ?? Ct))
					{
						foreach (ChannelRow row in chunk.ToArray())
							ids.Add(row.Id);

						if (breakAfter > 0 && ids.Count >= breakAfter)
							break;

						if (consumerLag > TimeSpan.Zero)
							await Task.Delay(consumerLag, Ct);
					}

					break;

				default:
					await foreach (ChannelRow row in j.View
						.ToBufferedAsyncStream(j.Pool, token ?? Ct))
					{
						ids.Add(row.Id);
						if (breakAfter > 0 && ids.Count >= breakAfter)
							break;

						if (consumerLag > TimeSpan.Zero && ids.Count % 50 == 0)
							await Task.Delay(consumerLag, Ct);
					}

					break;
			}
		}
		catch (Exception ex)
		{
			error = ex;
		}

		return new Outcome(ids, error);
	}

	static List<int> Ids(int count)
		=> [.. Enumerable.Range(1, count)];

	// The chunk sizes the stream makes: 16, 32 ... 4096, then 4096.
	static List<int> StreamSizes(int rows)
	{
		List<int> sizes = [];
		for (int k = 0; rows > 0; ++k)
		{
			int size = Math.Min(rows, 16 << Math.Min(k, 8));
			sizes.Add(size);
			rows -= size;
		}

		return sizes;
	}

	// ---- J1: the buffered stream, element by element --------------------

	[Theory]
	[MemberData(nameof(Sizes))]
	public static async Task J1_TheBufferedStream_YieldsEveryElementInOrder(
		int n)
	{
		using var j = Journey.Rows(n);

		Outcome o = await Drive("buffered", j).Within();

		Assert.Null(o.Error);
		Assert.Equal(Ids(n), o.Ids);
		await j.AssertClean();
	}

	[Fact]
	public static async Task J1_TheBufferedStream_IsLazy_AndSingleShot()
	{
		using var j = Journey.Rows(100);
		var stream = j.View.ToBufferedAsyncStream(j.Pool, Ct);

		await Task.Delay(150, Ct);
		Assert.Equal(0, j.Executes);

		await using var e = stream.GetAsyncEnumerator(Ct);
		Assert.Equal(0, j.Executes);

		Assert.True(await e.MoveNextAsync().AsTask().Within());
		Assert.Equal(1, j.Executes);
		Assert.Equal(1, e.Current.Id);

		// A second enumeration is refused at its first step.
		await using var second = stream.GetAsyncEnumerator(Ct);
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => second.MoveNextAsync().AsTask());

		while (await e.MoveNextAsync().AsTask().Within()) { }
		await using var third = stream.GetAsyncEnumerator(Ct);
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => third.MoveNextAsync().AsTask());
	}

	[Fact]
	public static async Task J1_AnEarlyBreak_CancelsTheReadAndReturnsEverything()
	{
		using var j = new Journey(
			() => new LagReader(500, TimeSpan.FromMilliseconds(5), 20));

		Outcome o = await Drive("buffered", j, breakAfter: 5).Within();

		Assert.Null(o.Error);
		Assert.Equal(Ids(5), o.Ids);
		await j.AssertClean();
		Assert.Equal(1, ((LagReader)j.Reader!).Disposes);
	}

	// ---- J2: the chunk stream ---------------------------------------------

	[Theory]
	[MemberData(nameof(Sizes))]
	public static async Task J2_TheStream_YieldsExactSlices(int n)
	{
		using var j = Journey.Rows(n);
		List<int> sizes = [];
		List<int> ids = [];

		await foreach (var chunk in j.View.ToAsyncBufferStream(j.Pool, Ct))
		{
			sizes.Add(chunk.Length);
			ids.AddRange(chunk.ToArray().Select(r => r.Id));
		}

		Assert.Equal(StreamSizes(n), sizes);
		Assert.Equal(n, sizes.Sum());
		Assert.Equal(Ids(n), ids);
		await j.AssertClean();
	}

	[Fact]
	public static async Task J2_AChunksArray_GoesBackAtTheNextStep()
	{
		using var j = Journey.Rows(100);
		await using var e = j.View
			.ToAsyncBufferStream(j.Pool, Ct)
			.GetAsyncEnumerator(Ct);

		Assert.True(await e.MoveNextAsync().AsTask().Within());
		Assert.Equal(0, j.Pool.Returns);

		Assert.True(await e.MoveNextAsync().AsTask().Within());
		Assert.Equal(1, j.Pool.Returns);

		Assert.True(await e.MoveNextAsync().AsTask().Within());
		Assert.Equal(2, j.Pool.Returns);

		Assert.False(await e.MoveNextAsync().AsTask().Within());
	}

	[Fact]
	public static async Task J2_TheStream_FillsFullChunksUpToItsCap()
	{
		using var j = Journey.Rows(9000);
		List<int> sizes = [];
		await foreach (var chunk in j.View.ToAsyncBufferStream(j.Pool, Ct))
			sizes.Add(chunk.Length);

		Assert.Equal(
			[16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 824],
			sizes);
		await j.AssertClean();
	}

	// ---- J3: the buffer and its complete view -----------------------------

	[Fact]
	public static async Task J3_TheItems_AreTheCompleteResult()
	{
		const int N = 3500;
		using var j = Journey.Rows(N);
		var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();

		Assert.Equal(N, items.Count);
		foreach (int i in new[] { 0, 15, 16, 47, 48, 2500, N - 1 })
			Assert.Equal(i + 1, items[i].Id);

		List<int> expected = Ids(N);
		ChannelRow[] array = items.ToArray();
		Assert.Equal(N, array.Length);
		Assert.Equal(expected, array.Select(r => r.Id).ToList());

		List<ChannelRow> list = items.ToList();
		Assert.Equal(expected, list.Select(r => r.Id).ToList());
		Assert.Equal(N, list.Capacity);

		List<ChannelRow> made = new(items);
		Assert.Equal(expected, made.Select(r => r.Id).ToList());
		Assert.Equal(N, made.Capacity);

		ChannelRow[] copy = new ChannelRow[N];
		items.CopyTo(copy);
		Assert.Equal(expected, copy.Select(r => r.Id).ToList());
		Assert.Equal(expected, items.Select(r => r.Id).ToList());

		// Disposing the view disposes the buffer.
		items.Dispose();
		Assert.Throws<ObjectDisposedException>(() => items.Count);
		Assert.Throws<ObjectDisposedException>(() => { _ = buffer.Items; });

		await j.AssertClean();
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(16)]
	[InlineData(17)]
	[InlineData(48)]
	[InlineData(112)]
	[InlineData(113)]
	public static async Task J3_TheItems_HoldEveryRow_AtSegmentMultiples(int n)
	{
		using var j = Journey.Rows(n);
		await using var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);

		PooledArray<ChannelRow> items = await buffer.Items.Within();

		Assert.Equal(Ids(n), items.Select(r => r.Id).ToList());
		if (n > 0) Assert.Equal(n, items[n - 1].Id);

		await buffer.DisposeAsync();
		await j.AssertClean();
	}

	// ---- J4: today's ToArrayAsync and ToListAsync, pinned ------------------

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static async Task J4_ToListAsyncAndToArrayAsync_ReadInOrder(int n)
	{
		using var j1 = Journey.Rows(n);
		List<ChannelRow> list = await j1.View.ToListAsync(Ct).AsTask().Within();
		Assert.Equal(Ids(n), list.Select(r => r.Id).ToList());
		Assert.Equal(1, j1.Executes);
		Assert.Equal(1, j1.Connection.Log.Count(l => l == "close"));
		Assert.Equal(1, ((LagReader)j1.Reader!).Disposes);

		using var j2 = Journey.Rows(n);
		ChannelRow[] array = await j2.View.ToArrayAsync(Ct).AsTask().Within();
		Assert.Equal(Ids(n), array.Select(r => r.Id).ToList());
		Assert.Equal(1, j2.Executes);
		Assert.Equal(1, j2.Connection.Log.Count(l => l == "close"));
		Assert.Equal(1, ((LagReader)j2.Reader!).Disposes);
	}

	[Theory]
	[InlineData(true, "list")]
	[InlineData(false, "list")]
	[InlineData(true, "array")]
	[InlineData(false, "array")]
	public static async Task J4_ACancelMidRead_Throws_AndClosesTheConnection(
		bool asyncReads,
		string terminal)
	{
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
		using var j = new Journey(
			() => new LagReader(
				100,
				onRead: n =>
				{
					if (n == 20) cts.Cancel();
				}),
			asyncReads);

		Func<Task> run = terminal == "list"
			? async () => await j.View.ToListAsync(cts.Token)
			: async () => await j.View.ToArrayAsync(cts.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => run().Within());

		Assert.Equal(1, j.Connection.Log.Count(l => l == "close"));
		Assert.Equal(1, ((LagReader)j.Reader!).Disposes);
	}

	[Theory]
	[InlineData("list")]
	[InlineData("array")]
	public static async Task J4_AReadFault_Throws_AndClosesTheConnection(
		string terminal)
	{
		using var j = new Journey(() => new LagReader(100, failAtRow: 30));

		Func<Task> run = terminal == "list"
			? async () => await j.View.ToListAsync(Ct)
			: async () => await j.View.ToArrayAsync(Ct);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(
			() => run().Within());

		Assert.Equal("journey fault", ex.Message);
		Assert.Equal(1, j.Connection.Log.Count(l => l == "close"));
		Assert.Equal(1, ((LagReader)j.Reader!).Disposes);
	}

	// ---- J5: lag ------------------------------------------------------------

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task J5_AProducerSlowerThanTheConsumer_Completes(
		string terminal)
	{
		using var j = new Journey(
			() => new LagReader(200, TimeSpan.FromMilliseconds(5), 25));

		Outcome o = await Drive(terminal, j).Within();

		Assert.Null(o.Error);
		Assert.Equal(Ids(200), o.Ids);
		await j.AssertClean();
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task J5_AConsumerSlowerThanTheProducer_Completes(
		string terminal)
	{
		using var j = Journey.Rows(500);

		Outcome o = await Drive(
			terminal,
			j,
			consumerLag: TimeSpan.FromMilliseconds(20)).Within();

		Assert.Null(o.Error);
		Assert.Equal(Ids(500), o.Ids);
		await j.AssertClean();
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task J5_ASlowConsumer_DoesNotHoldTheConnection(
		string terminal)
	{
		using var j = Journey.Rows(500);
		IAsyncEnumerator<object> e = terminal == "stream"
			? new BoxedChunks(j.View.ToAsyncBufferStream(j.Pool, Ct))
			: new BoxedRows(j.View.ToBufferedAsyncStream(j.Pool, Ct));

		Assert.True(await e.MoveNextAsync().AsTask().Within());

		// The consumer stops here; the producer runs to the end.
		await Until(() => j.Closed, "the connection closed");
		await e.DisposeAsync().AsTask().Within();
		await j.AssertClean();
	}

	sealed class BoxedChunks(
		IAsyncEnumerable<ReadOnlyMemory<ChannelRow>> source,
		CancellationToken token = default)
		: IAsyncEnumerator<object>
	{
		readonly IAsyncEnumerator<ReadOnlyMemory<ChannelRow>> _inner
			= source.GetAsyncEnumerator(
				token.CanBeCanceled ? token : Ct);

		public object Current => _inner.Current.ToArray();

		public ValueTask<bool> MoveNextAsync() => _inner.MoveNextAsync();

		public ValueTask DisposeAsync() => _inner.DisposeAsync();
	}

	sealed class BoxedRows(
		IAsyncEnumerable<ChannelRow> source,
		CancellationToken token = default)
		: IAsyncEnumerator<object>
	{
		readonly IAsyncEnumerator<ChannelRow> _inner
			= source.GetAsyncEnumerator(
				token.CanBeCanceled ? token : Ct);

		public object Current => _inner.Current;

		public ValueTask<bool> MoveNextAsync() => _inner.MoveNextAsync();

		public ValueTask DisposeAsync() => _inner.DisposeAsync();
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task J5_ACallerCancelMidCycle_ThrowsWithTheCallersToken(
		string terminal)
	{
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
		using var j = new Journey(
			() => new LagReader(
				500,
				TimeSpan.FromMilliseconds(5),
				20,
				onRead: n =>
				{
					if (n == 60) cts.Cancel();
				}));

		Outcome o = await Drive(terminal, j, cts.Token).Within();

		var cancel = Assert.IsAssignableFrom<OperationCanceledException>(
			o.Error);
		Assert.Equal(cts.Token, cancel.CancellationToken);

		// The rows read before the cancel come first, in order.
		Assert.Equal(Ids(o.Ids.Count), o.Ids);
		Assert.True(o.Ids.Count < 500);
		await j.AssertClean();

		// The loop saw the cancel before the next read: no more rows
		// were read than the one the cancel landed on.
		Assert.True(((LagReader)j.Reader!).Reads <= 61);
		Assert.Equal(1, ((LagReader)j.Reader!).Disposes);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task J5_AFaultMidCycle_ThrowsAfterTheDeliveredRows(
		string terminal)
	{
		using var j = new Journey(
			() => new LagReader(
				500, TimeSpan.FromMilliseconds(5), 20, failAtRow: 60));

		Outcome o = await Drive(terminal, j).Within();

		var ex = Assert.IsType<InvalidOperationException>(o.Error);
		Assert.Equal("journey fault", ex.Message);
		Assert.Equal(Ids(o.Ids.Count), o.Ids);

		// The stream and the buffered stream deliver the 59 rows read
		// before the fault; a buffer's Items faults without rows.
		Assert.Equal(terminal == "buffer" ? 0 : 59, o.Ids.Count);
		await j.AssertClean();
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task J5_ABreakMidStream_CancelsTheQuery(string terminal)
	{
		using var j = new Journey(
			() => new LagReader(500, TimeSpan.FromMilliseconds(5), 20));

		Outcome o = await Drive(terminal, j, breakAfter: 20).Within();

		Assert.Null(o.Error);
		Assert.True(o.Ids.Count >= 20);
		await j.AssertClean();
		Assert.True(((LagReader)j.Reader!).Reads < 500, "the read was cut short");
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task J5_ADisposeMidCycle_CancelsTheQuery(
		string terminal)
	{
		using var j = new Journey(
			() => new LagReader(500, TimeSpan.FromMilliseconds(5), 20));
		IAsyncEnumerator<object> e = terminal == "stream"
			? new BoxedChunks(j.View.ToAsyncBufferStream(j.Pool, Ct))
			: new BoxedRows(j.View.ToBufferedAsyncStream(j.Pool, Ct));
		Assert.True(await e.MoveNextAsync().AsTask().Within());
		await Until(
			() => j.Reader is LagReader { Reads: > 20 },
			"the read under way");

		await e.DisposeAsync().AsTask().Within();

		// Awaited: nothing is left running.
		Assert.True(j.Closed);
		Assert.Equal(0, j.Pool.Outstanding);
		await j.AssertClean();
		Assert.True(((LagReader)j.Reader!).Reads < 500, "the read was cut short");
		Assert.False(await e.MoveNextAsync().AsTask().Within());
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static async Task J5_TheBufferDisposedMidCycle_CancelsTheQuery(
		bool async)
	{
		using var j = new Journey(
			() => new LagReader(500, TimeSpan.FromMilliseconds(5), 20));
		var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);
		Task<PooledArray<ChannelRow>> items = buffer.Items;
		await Until(
			() => j.Reader is LagReader { Reads: > 20 },
			"the read under way");

		if (async)
		{
			await buffer.DisposeAsync().AsTask().Within();
			Assert.True(j.Closed);
		}
		else
		{
			buffer.Dispose();
		}

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => items.Within());
		await j.AssertClean();
		Assert.True(((LagReader)j.Reader!).Reads < 500, "the read was cut short");
		Assert.Throws<ObjectDisposedException>(() => { _ = buffer.Items; });
	}

	// ---- a cancelled enumeration token starts nothing ---------------------

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task ACancelledEnumerationToken_ExecutesNothing(
		string terminal)
	{
		using var j = Journey.Rows(100);
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		IAsyncEnumerator<object> e = terminal == "stream"
			? new BoxedChunks(
				j.View.ToAsyncBufferStream(j.Pool, Ct),
				cts.Token)
			: new BoxedRows(
				j.View.ToBufferedAsyncStream(j.Pool, Ct),
				cts.Token);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => e.MoveNextAsync().AsTask().Within());
		await e.DisposeAsync().AsTask().Within();

		// No command, no connection, no array rented.
		Assert.Empty(j.Connection.Log);
		Assert.Equal(0, j.Pool.Rents);
		await j.AssertClean(executed: false);
	}

	// ---- a provider that ignores the cancel token ---------------------------

	// Serves rows, but its read number `deafAt` takes `delay` and
	// ignores every token, as a provider that does not honour
	// cancellation would.
	sealed class DeafReader(int rows, int deafAt, TimeSpan delay)
		: TypedReader(Columns, Rows(rows))
	{
		int _calls;
		volatile bool _stalled;
		volatile bool _stallEnded;

		public bool Stalled => _stalled;

		public bool StallEnded => _stallEnded;

		public int Calls => Volatile.Read(ref _calls);

		public override async Task<bool> ReadAsync(
			CancellationToken cancellationToken)
		{
			if (Interlocked.Increment(ref _calls) == deafAt)
			{
				_stalled = true;
				await Task.Delay(delay, CancellationToken.None);
				_stallEnded = true;
			}

			return await base.ReadAsync(CancellationToken.None);
		}
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffer")]
	public static async Task ADisposeWaitsForAProviderThatIgnoresCancellation(
		string terminal)
	{
		using var j = new Journey(
			() => new DeafReader(100, 20, TimeSpan.FromMilliseconds(400)));
		Task dispose;
		if (terminal == "stream")
		{
			var e = j.View
				.ToAsyncBufferStream(j.Pool, Ct)
				.GetAsyncEnumerator(Ct);
			Assert.True(await e.MoveNextAsync().AsTask().Within());
			await Until(
				() => j.Reader is DeafReader { Stalled: true },
				"the stall");
			dispose = e.DisposeAsync().AsTask();
		}
		else
		{
			var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);
			await Until(
				() => j.Reader is DeafReader { Stalled: true },
				"the stall");
			dispose = buffer.DisposeAsync().AsTask();
		}

		// The cancel was sent, the provider ignores it: the dispose
		// is still waiting for the call in progress.
		await Task.Delay(100, Ct);
		DeafReader reader = (DeafReader)j.Reader!;
		Assert.False(dispose.IsCompleted);
		Assert.False(reader.StallEnded);

		await dispose.Within();

		// It completed only after that call returned, then the
		// producer stopped at the next row: no hang beyond the read.
		Assert.True(reader.StallEnded);
		// (the token is checked once per chunk now: the read stops at
		// the end of the chunk it was in, 48 rows in, not at the next row)
		Assert.True(reader.Calls <= 50);
		await j.AssertClean();
	}

	[Fact]
	public static async Task ACallerCancel_WithAProviderThatIgnoresIt_EndsAtTheNextChunk()
	{
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		using var j = new Journey(
			() => new DeafReader(100, 20, TimeSpan.FromMilliseconds(300)));
		await using var buffer = j.View.ToAsyncBuffer(
			j.Pool, clearOnDispose: false, cts.Token);
		await Until(
			() => j.Reader is DeafReader { Stalled: true },
			"the stall");

		cts.Cancel();

		var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => buffer.Items.Within());
		Assert.Equal(cts.Token, ex.CancellationToken);

		// The provider ignored the cancel; the producer ended the read
		// at the end of the chunk it was in (48 rows), not at the end of
		// the query.
		Assert.True(((DeafReader)j.Reader!).Calls <= 50);

		await buffer.DisposeAsync().AsTask().Within();
		await j.AssertClean();
	}

	// ---- the arbiter's missing tests that fit the journeys ----------------

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task APreCancelledToken_ExecutesNothing(string terminal)
	{
		using var j = Journey.Rows(100);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		Outcome o = await Drive(terminal, j, cts.Token).Within();

		Assert.IsAssignableFrom<OperationCanceledException>(o.Error);
		Assert.Empty(o.Ids);
		Assert.Empty(j.Connection.Log);
		await j.AssertClean(executed: false);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task SkipAndTake_ApplyToEveryTerminal(string terminal)
	{
		using var j = Journey.Rows(100);
		var view = j.View.Skip(3).Take(10);
		List<int> ids = [];

		switch (terminal)
		{
			case "buffer":
				{
					await using var buffer = view.ToAsyncBuffer(j.Pool, Ct);
					ids.AddRange(
						(await buffer.Items.Within()).Select(r => r.Id));
					break;
				}

			case "stream":
				await foreach (var chunk in view.ToAsyncBufferStream(j.Pool, Ct))
					ids.AddRange(chunk.ToArray().Select(r => r.Id));

				break;

			default:
				await foreach (ChannelRow row in view
					.ToBufferedAsyncStream(j.Pool, Ct))
				{
					ids.Add(row.Id);
				}

				break;
		}

		Assert.Equal(Enumerable.Range(4, 10).ToList(), ids);
		Assert.Equal(1, j.Pool.Rents);
		await j.AssertClean();
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task LaterResultSets_AreReadThrough(string terminal)
	{
		ChannelTestReader? reader = null;
		var connection = new FakeDbConnection();
		var view = new FakeDbCommand(
			connection,
			() => reader = new ChannelTestReader(100, laterSets: 3))
			.Map<ChannelRow>()
			.AsyncReads();
		CountingPool<ChannelRow> pool = new();
		List<int> ids = [];

		switch (terminal)
		{
			case "buffer":
				{
					await using var buffer = view.ToAsyncBuffer(pool, Ct);
					ids.AddRange(
						(await buffer.Items.Within()).Select(r => r.Id));
					break;
				}

			case "stream":
				await foreach (var chunk in view.ToAsyncBufferStream(pool, Ct))
					ids.AddRange(chunk.ToArray().Select(r => r.Id));

				break;

			default:
				await foreach (ChannelRow row in view
					.ToBufferedAsyncStream(pool, Ct))
				{
					ids.Add(row.Id);
				}

				break;
		}

		Assert.Equal(Ids(100), ids);
		Assert.True(reader!.NextResultCalls >= 3);
		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.Equal(pool.Rents, pool.Returns);

		ChannelTestReader? single = null;
		var view2 = new FakeDbCommand(
			new FakeDbConnection(),
			() => single = new ChannelTestReader(100, laterSets: 3))
			.Map<ChannelRow>()
			.SingleResult()
			.AsyncReads();
		await using var buffer2 = view2.ToAsyncBuffer(clearOnDispose: false, Ct);
		Assert.Equal(100, (await buffer2.Items.Within()).Count);
		Assert.Equal(0, single!.NextResultCalls);
	}

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task ALaterResultSetFailure_ArrivesAfterTheRows(
		string terminal)
	{
		using var j = new Journey(
			() => new ChannelTestReader(
				100, laterSets: 2, failAtNextResult: 1));

		Outcome o = await Drive(terminal, j).Within();

		Assert.IsType<LaterSetFailure>(o.Error);
		Assert.Equal(terminal == "buffer" ? 0 : 100, o.Ids.Count);
		await j.AssertClean();
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task AStreamDisposedBeforeTheFirstStep_ExecutesNothing(
		string terminal)
	{
		using var j = Journey.Rows(100);
		IAsyncEnumerator<object> e = terminal == "stream"
			? new BoxedChunks(j.View.ToAsyncBufferStream(j.Pool, Ct))
			: new BoxedRows(j.View.ToBufferedAsyncStream(j.Pool, Ct));

		await e.DisposeAsync().AsTask().Within();

		Assert.False(await e.MoveNextAsync().AsTask().Within());
		Assert.Empty(j.Connection.Log);
		await j.AssertClean(executed: false);
	}

	[Fact]
	public static async Task TheBuffer_AfterItsDispose_RefusesEverything()
	{
		using var j = Journey.Rows(100);
		var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		ICollection<ChannelRow> collection = items;

		await buffer.DisposeAsync().AsTask().Within();

		Assert.Throws<ObjectDisposedException>(() => items.Count);
		Assert.Throws<ObjectDisposedException>(() => items[0]);
		Assert.Throws<ObjectDisposedException>(
			() => items.CopyTo(new ChannelRow[100]));
		Assert.Throws<ObjectDisposedException>(() => items.GetEnumerator());
		Assert.Throws<ObjectDisposedException>(
			() => collection.Contains(new ChannelRow(1)));
		Assert.Throws<ObjectDisposedException>(() => { _ = buffer.Items; });
		await j.AssertClean();
	}

	// ---- J6: the controller flow -------------------------------------------

	static bool HasDisposed(Exception? ex)
	{
		for (; ex is not null; ex = ex.InnerException)
		{
			if (ex is ObjectDisposedException) return true;
		}

		return false;
	}

	static string Json(int n)
		=> JsonSerializer.Serialize(
			Enumerable.Range(1, n).Select(i => new ChannelRow(i)).ToList());

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(17)]
	[InlineData(3500)]
	public static async Task J6_AControllerHandsTheBufferToAnotherLayer(int n)
	{
		// A fill that is still running when the call returns.
		using var j = new Journey(
			() => new LagReader(
				n, TimeSpan.FromMilliseconds(15), Math.Max(1, n / 4)));
		string json;

		await using (var buffer = j.View.ToAsyncBuffer(j.Pool, Ct))
		{
			if (n >= 17) Assert.False(buffer.Items.IsCompleted);

			// The other layer awaits the view and serializes it.
			json = await Task.Run(
				async () => JsonSerializer.Serialize(await buffer.Items),
				Ct).Within();
		}

		Assert.Equal(Json(n), json);
		await j.AssertClean();
	}

	[Fact]
	public static async Task J6_ADisposeBeforeTheSerialization_ThrowsAndReadsNothing()
	{
		using var j = Journey.Rows(3500);
		var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		IEnumerator<ChannelRow> e = items.GetEnumerator();
		for (int i = 0; i < 100; ++i) Assert.True(e.MoveNext());

		// The owner disposes while the other layer is part-way.
		await buffer.DisposeAsync().AsTask().Within();

		Assert.Throws<ObjectDisposedException>(() => e.MoveNext());
		Assert.True(
			HasDisposed(
				Record.Exception(() => JsonSerializer.Serialize(items))));
		await j.AssertClean();
	}

	[Fact]
	public static async Task J6_TwoLayersAwaitingItems_ShareOneFill()
	{
		using var j = new Journey(
			() => new LagReader(100, TimeSpan.FromMilliseconds(15), 25));
		await using var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);

		Task<PooledArray<ChannelRow>> one = Task.Run(
			async () => await buffer.Items, Ct);
		Task<PooledArray<ChannelRow>> two = Task.Run(
			async () => await buffer.Items, Ct);
		PooledArray<ChannelRow>[] views = await Task
			.WhenAll(one, two)
			.Within();

		Assert.Same(views[0], views[1]);
		Assert.Equal(Ids(100), views[0].Select(r => r.Id).ToList());
		Assert.Equal(1, j.Executes);

		await buffer.DisposeAsync();
		await j.AssertClean();
	}

	// ---- J7: a forgotten dispose ----------------------------------------------

	[Fact]
	public static void J7_NoBufferTypeDeclaresAFinalizer()
	{
		Assembly core = typeof(PooledBuffer<>).Assembly;
		Type[] types =
		[
			typeof(PooledBuffer<>),
			typeof(PooledArray<>),
			core.GetType("Open.Database.Extensions.Core.ChunkProducer`1")!,
			core.GetType("Open.Database.Extensions.Core.ChunkStream`1")!,
			core.GetType("Open.Database.Extensions.Core.StreamTarget`1")!,
		];

		foreach (Type type in types)
		{
			Assert.NotNull(type);
			Assert.Null(type.GetMethod(
				"Finalize",
				BindingFlags.Instance
				| BindingFlags.NonPublic
				| BindingFlags.DeclaredOnly));
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static async Task ForgetABufferAsync(Journey j)
	{
		var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);
		var items = await buffer.Items.Within();
		Assert.Equal(100, items.Count);

		// No dispose: every reference is dropped on return.
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static async Task AbandonAStreamAsync(Journey j, string terminal)
	{
		if (terminal == "stream")
		{
			var e = j.View
				.ToAsyncBufferStream(j.Pool, Ct)
				.GetAsyncEnumerator(Ct);
			Assert.True(await e.MoveNextAsync().AsTask().Within());
		}
		else
		{
			var e = j.View
				.ToBufferedAsyncStream(j.Pool, Ct)
				.GetAsyncEnumerator(Ct);
			Assert.True(await e.MoveNextAsync().AsTask().Within());
		}
	}

	static void CollectTwice()
	{
		for (int i = 0; i < 2; ++i)
		{
			GC.Collect();
			GC.WaitForPendingFinalizers();
		}
	}

	[Fact]
	public static async Task J7_AForgottenBuffer_ThrowsNothing_AndALaterReadWorks()
	{
		using var j = Journey.Rows(100);

		await ForgetABufferAsync(j).Within();
		CollectTwice();

		// The forgotten arrays are never returned (the documented
		// consequence); the pool and the view still work.
		Outcome again = await Drive("buffer", j).Within();
		Assert.Null(again.Error);
		Assert.Equal(Ids(100), again.Ids);

		CollectTwice();
		j.AssertNoUnobserved();
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task J7_AnAbandonedEnumerator_ThrowsNothing_AndALaterReadWorks(
		string terminal)
	{
		using var j = Journey.Rows(500);

		await AbandonAStreamAsync(j, terminal).Within();
		CollectTwice();

		// The producer runs to its end on its own and closes the
		// connection; nothing is asserted about the arrays.
		await Until(() => j.Closed, "the abandoned read finished");

		Outcome again = await Drive(terminal, j).Within();
		Assert.Null(again.Error);
		Assert.Equal(Ids(500), again.Ids);

		CollectTwice();
		j.AssertNoUnobserved();
	}

	// ---- J8: Items awaited at once while the fill is delayed -----------------

	[Fact]
	public static async Task J8_Items_CompletesOnlyWhenTheFillIsDone()
	{
		using var j = new Journey(
			() => new LagReader(100, TimeSpan.FromMilliseconds(40), 25));
		await using var buffer = j.View.ToAsyncBuffer(j.Pool, Ct);

		Task<PooledArray<ChannelRow>> items = buffer.Items;
		Assert.False(items.IsCompleted);

		PooledArray<ChannelRow> view = await items.Within();

		Assert.True(((LagReader)j.Reader!).Reads >= 100, "the fill was done");
		Assert.Equal(100, view.Count);

		await buffer.DisposeAsync();
		await j.AssertClean();
	}

	[Fact]
	public static async Task J8_ACancelDuringTheWait_ReachesItems_AndTheBufferDisposesCleanly()
	{
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
		using var j = new Journey(
			() => new LagReader(500, TimeSpan.FromMilliseconds(40), 25));
		var buffer = j.View.ToAsyncBuffer(j.Pool, cts.Token);

		Task<PooledArray<ChannelRow>> items = buffer.Items;
		await Until(
			() => j.Reader is LagReader { Reads: > 1 },
			"the fill under way");
		Assert.False(items.IsCompleted);

		cts.Cancel();

		var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => items.Within());
		Assert.Equal(cts.Token, ex.CancellationToken);

		await buffer.DisposeAsync().AsTask().Within();
		await j.AssertClean();
	}
}
