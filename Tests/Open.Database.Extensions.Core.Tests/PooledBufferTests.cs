using System.Data.Common;
using static Open.Database.Extensions.Tests.ChannelWait;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The two buffer terminals of the typed views (ToAsyncBuffer and
// ToAsyncBufferStream) and the flattened stream over the second
// (ToBufferedAsyncStream), through the public surface only:
// the arrays' return to the pool cannot be observed from here.
// Every wait is bounded.
[ExcludeFromCodeCoverage]
public static class PooledBufferTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A reader that serves some rows and then stalls in ReadAsync
	// until the token it was given fires, and records that it did.
	sealed class StallingReader(int rows, int stallAfter)
		: TypedReader(Columns, Rows(rows))
	{
		int _reads;
		volatile bool _stalled;
		volatile bool _cancelSeen;
		int _disposes;

		public bool Stalled => _stalled;

		public bool CancelSeen => _cancelSeen;

		public int Disposes => Volatile.Read(ref _disposes);

		public override async Task<bool> ReadAsync(
			CancellationToken cancellationToken)
		{
			if (++_reads > stallAfter)
			{
				_stalled = true;
				try
				{
					await Task.Delay(Timeout.Infinite, cancellationToken);
				}
				catch (OperationCanceledException)
				{
					_cancelSeen = true;
					throw;
				}
			}

			return await base.ReadAsync(cancellationToken);
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing) Interlocked.Increment(ref _disposes);
			base.Dispose(disposing);
		}
	}

	sealed class Run
	{
		public FakeDbConnection Connection { get; } = new();

		public MappedCommand<ChannelRow> View { get; private set; }

		public int Executes => Volatile.Read(ref _executes);

		public DbDataReader? Reader { get; private set; }

		int _executes;

		public static Run Create(
			Func<DbDataReader> make,
			bool asyncReads = false)
		{
			Run run = new();
			DbDataReader Make()
			{
				Interlocked.Increment(ref run._executes);
				return run.Reader = make();
			}

			var view = new FakeDbCommand(run.Connection, Make)
				.Map<ChannelRow>();
			run.View = asyncReads ? view.AsyncReads() : view;
			return run;
		}

		public bool Closed
			=> Connection.Log.Contains("close");
	}

	static Run Rows100()
		=> Run.Create(() => new ChannelTestReader(100));

	static async Task<List<int[]>> Chunks(
		IAsyncEnumerable<ReadOnlyMemory<ChannelRow>> chunks)
	{
		List<int[]> result = [];
		await foreach (ReadOnlyMemory<ChannelRow> chunk in chunks)
			result.Add([.. chunk.ToArray().Select(r => r.Id)]);

		return result;
	}

	static void AssertContiguous(List<int[]> chunks, int rows)
	{
		Assert.Equal(Ids(rows), chunks.SelectMany(c => c).ToList());
		Assert.All(chunks, c => Assert.NotEmpty(c));
	}

	// ---- 1: Mode 1, the complete result ------------------------------

	[Fact]
	public static async Task Items_HoldsEveryRow_AndIndexesAcrossSegments()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(Ct);

		PooledArray<ChannelRow> items = await buffer.Items.Within();

		Assert.Equal(100, items.Count);

		// The first arrays hold 16, 32 and 64 rows:
		// the boundaries are 16, 48 and 112.
		foreach (int i in new[] { 0, 1, 15, 16, 17, 47, 48, 49, 99 })
			Assert.Equal(i + 1, items[i].Id);

		Assert.Equal(Ids(100), items.Select(r => r.Id).ToList());

		ChannelRow[] copy = new ChannelRow[100];
		items.CopyTo(copy);
		Assert.Equal(Ids(100), copy.Select(r => r.Id).ToList());

		Assert.Throws<ArgumentException>(
			() => items.CopyTo(new ChannelRow[99]));
		Assert.Throws<ArgumentOutOfRangeException>(() => items[100]);
		Assert.Throws<ArgumentOutOfRangeException>(() => items[-1]);
	}

	[Fact]
	public static async Task Items_OfAnEmptyResult_HasCountZero()
	{
		var run = Run.Create(() => new ChannelTestReader(0));
		await using var buffer = run.View.ToAsyncBuffer(Ct);

		PooledArray<ChannelRow> items = await buffer.Items.Within();
		Assert.True(items.Count is 0);
		Assert.Empty(items);
		items.CopyTo([]);
	}

	// ---- 2: Mode 1, lazy, one fill -------------------------------------

	[Fact]
	public static async Task ToAsyncBuffer_IsLazy_AndFillsOnce()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(Ct);

		await Task.Delay(200, Ct);
		Assert.Equal(0, run.Executes);

		Task<PooledArray<ChannelRow>> first = buffer.Items;
		Assert.Same(first, buffer.Items);
		await first.Within();
		await buffer.Items.Within();

		await foreach (var _ in buffer) { }
		await foreach (var _ in buffer) { }

		Assert.Equal(1, run.Executes);
	}

	[Fact]
	public static async Task Enumerating_StartsTheFill_WithoutItems()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(Ct);

		Assert.Equal(0, run.Executes);
		await foreach (var _ in buffer) { }

		Assert.Equal(1, run.Executes);
	}

	// ---- 3: Mode 1, chunk enumeration ----------------------------------

	[Fact]
	public static async Task Enumeration_YieldsExactSlices_ManyTimes()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(Ct);

		List<int[]> one = await Chunks(buffer).Within();
		List<int[]> two = await Chunks(buffer).Within();

		AssertContiguous(one, 100);
		Assert.Equal([16, 32, 52], one.Select(c => c.Length));
		Assert.Equal(
			one.SelectMany(c => c).ToList(),
			two.SelectMany(c => c).ToList());
	}

	// ---- 4: disposing the PooledArray disposes the buffer --------------

	[Fact]
	public static async Task DisposingTheItems_DisposesTheBuffer()
	{
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();

		items.Dispose();

		Assert.Throws<ObjectDisposedException>(() => items.Count);
		Assert.Throws<ObjectDisposedException>(() => items[0]);
		Assert.Throws<ObjectDisposedException>(() => { _ = buffer.Items; });
		await Assert.ThrowsAsync<ObjectDisposedException>(
			async () =>
			{
				await foreach (var _ in buffer) { }
			});

		// Idempotent, and the async form too.
		items.Dispose();
		await buffer.DisposeAsync();
	}

	// ---- 5: Mode 1, dispose while filling -------------------------------

	[Fact]
	public static async Task Dispose_WhileFilling_CancelsTheQuery()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);
		var buffer = run.View.ToAsyncBuffer(Ct);
		Task<PooledArray<ChannelRow>> items = buffer.Items;
		await Until(() => reader is { Stalled: true }, "the stall");

		buffer.Dispose();

		await Until(() => reader!.CancelSeen, "the cancel at the reader");
		await Until(() => run.Closed, "the connection closed");
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => items.Within());
		buffer.Dispose();
	}

	[Fact]
	public static async Task DisposeAsync_WhileFilling_AwaitsTheProducer()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);
		var buffer = run.View.ToAsyncBuffer(Ct);
		_ = buffer.Items;
		await Until(() => reader is { Stalled: true }, "the stall");

		await buffer.DisposeAsync().AsTask().Within();

		// No waiting: the producer was awaited.
		Assert.True(reader!.CancelSeen);
		Assert.True(run.Closed);
		Assert.Equal(1, reader.Disposes);
		await buffer.DisposeAsync();
	}

	// ---- 6: Mode 1, cancelling one enumeration -------------------------

	[Fact]
	public static async Task CancellingAnEnumeration_DoesNotCancelTheRead()
	{
		// The read holds at its 20th row: the first array (16 rows) is
		// out, the second is not full.
		ChannelTestReader? reader = null;
		var run = Run.Create(() =>
		{
			reader = new ChannelTestReader(50);
			reader.Hold(20);
			return reader;
		});
		await using var buffer = run.View.ToAsyncBuffer(Ct);
		var started = buffer.Items;

		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		await using var e = buffer.GetAsyncEnumerator(cts.Token);
		Assert.True(await e.MoveNextAsync().AsTask().Within());
		Assert.Equal(16, e.Current.Length);

		Task<bool> next = e.MoveNextAsync().AsTask();
		cts.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => next.Within());

		// The read did not notice: it finishes.
		reader!.Release();
		PooledArray<ChannelRow> items = await started.Within();
		Assert.Equal(50, items.Count);
	}

	// ---- 7: Mode 2 ------------------------------------------------------

	[Fact]
	public static async Task Stream_YieldsExactSlices_Lazy_AndSingleShot()
	{
		var run = Rows100();
		var stream = run.View.ToAsyncBufferStream(Ct);

		await Task.Delay(200, Ct);
		Assert.Equal(0, run.Executes);

		await using var e = stream.GetAsyncEnumerator(Ct);
		Assert.Equal(0, run.Executes);
		Assert.Throws<InvalidOperationException>(
			() => stream.GetAsyncEnumerator(Ct));

		List<int[]> chunks = [];
		while (await e.MoveNextAsync().AsTask().Within())
			chunks.Add([.. e.Current.ToArray().Select(r => r.Id)]);

		Assert.Equal(1, run.Executes);
		AssertContiguous(chunks, 100);
		Assert.Equal([16, 32, 52], chunks.Select(c => c.Length));
		Assert.Throws<InvalidOperationException>(
			() => stream.GetAsyncEnumerator(Ct));
	}

	// ---- 8: Mode 2, a slow consumer does not hold the connection ---------

	[Fact]
	public static async Task SlowConsumer_DoesNotHoldTheConnection()
	{
		var run = Rows100();
		await using var e = run.View
			.ToAsyncBufferStream(Ct)
			.GetAsyncEnumerator(Ct);

		Assert.True(await e.MoveNextAsync().AsTask().Within());

		// The consumer pauses here, holding the first chunk.
		await Until(() => run.Closed, "the connection closed");
		Assert.Equal(1, ((ChannelTestReader)run.Reader!).Disposes);

		int rows = e.Current.Length;
		while (await e.MoveNextAsync().AsTask().Within())
			rows += e.Current.Length;

		Assert.Equal(100, rows);
	}

	// ---- 9: Mode 2, break ---------------------------------------------

	[Fact]
	public static async Task Break_CancelsTheQuery_AndClosesTheConnection()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 40),
			asyncReads: true);

		await foreach (var chunk in run.View.ToAsyncBufferStream(Ct))
		{
			Assert.Equal(16, chunk.Length);
			await Until(() => reader is { Stalled: true }, "the stall");
			break;
		}

		// The break disposed the enumerator, which awaited the
		// producer: no waiting here.
		Assert.True(reader!.CancelSeen);
		Assert.True(run.Closed);
		Assert.Equal(1, reader.Disposes);
	}

	// ---- 10: the caller's token, and a fault ----------------------------

	[Fact]
	public static async Task CallerToken_MidRead_ReachesTheConsumer()
	{
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);

		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);
		var stream = run.View.ToAsyncBufferStream(cts.Token);
		await using var e = stream.GetAsyncEnumerator(Ct);
		Assert.True(await e.MoveNextAsync().AsTask().Within());

		await Until(() => reader is { Stalled: true }, "the stall");
		cts.Cancel();

		// The rows read so far (the 4 after the first chunk) come
		// first, then the cancel.
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				while (await e.MoveNextAsync().AsTask().Within()) { }
			});

		var run2 = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);
		using var cts2 = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		await using var buffer = run2.View.ToAsyncBuffer(cts2.Token);
		Task<PooledArray<ChannelRow>> items = buffer.Items;
		await Until(() => reader is { Stalled: true }, "the stall");
		cts2.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => items.Within());
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				await foreach (var _ in buffer) { }
			});
	}

	[Fact]
	public static async Task ReaderFault_IsThrown_AfterTheDeliveredChunks()
	{
		// The 30th read fails: 29 rows were read, in two chunks.
		Run Faulty() => Run.Create(
			() => new ChannelTestReader(40, failAtRow: 30));

		var stream = Faulty().View.ToAsyncBufferStream(Ct);
		List<int[]> chunks = [];
		await using (var e = stream.GetAsyncEnumerator(Ct))
		{
			await Assert.ThrowsAsync<InvalidOperationException>(
				async () =>
				{
					while (await e.MoveNextAsync().AsTask().Within())
						chunks.Add(
							[.. e.Current.ToArray().Select(r => r.Id)]);
				});
		}

		Assert.Equal(Ids(29), chunks.SelectMany(c => c).ToList());

		await using var buffer = Faulty().View.ToAsyncBuffer(Ct);
		List<int> seen = [];
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () =>
			{
				await foreach (var chunk in buffer)
					seen.AddRange(chunk.ToArray().Select(r => r.Id));
			});
		Assert.Equal(Ids(29), seen);

		await Assert.ThrowsAsync<InvalidOperationException>(
			() => buffer.Items.Within());
	}

	// ---- the producer does not capture the caller's context -------------

	sealed class UnpumpedContext : SynchronizationContext
	{
		int _posts;

		public int Posts => Volatile.Read(ref _posts);

		public override void Post(SendOrPostCallback d, object? state)
			=> Interlocked.Increment(ref _posts);

		public override void Send(SendOrPostCallback d, object? state)
			=> Interlocked.Increment(ref _posts);
	}

	[Fact]
	public static async Task TheProducer_DoesNotCaptureTheCallersContext()
	{
		var run = Rows100();
		var context = new UnpumpedContext();
		bool completed = false;

		// A thread with a context nobody pumps: a continuation that
		// was posted back to it would never run.
		Thread thread = new(() =>
		{
			SynchronizationContext.SetSynchronizationContext(context);
			var buffer = run.View.ToAsyncBuffer();
			completed = buffer.Items.Wait(TimeSpan.FromSeconds(20));

			var e = run.View.ToAsyncBufferStream().GetAsyncEnumerator();
			completed &= e.MoveNextAsync().AsTask()
				.Wait(TimeSpan.FromSeconds(20));
		});
		thread.Start();
		await Task.Run(() => thread.Join(TimeSpan.FromSeconds(60)), Ct);

		Assert.True(completed);
		Assert.Equal(0, context.Posts);
	}

	// ---- ToBufferedAsyncStream ------------------------------------------

	[Fact]
	public static async Task Buffered_YieldsEveryRowInOrder_Once()
	{
		var run = Rows100();
		var stream = run.View.ToBufferedAsyncStream(Ct);

		await Task.Delay(100, Ct);
		Assert.Equal(0, run.Executes);

		List<int> ids = [];
		await foreach (ChannelRow row in stream)
			ids.Add(row.Id);

		Assert.Equal(Ids(100), ids);
		Assert.Equal(1, run.Executes);
		Assert.Throws<InvalidOperationException>(
			() => stream.GetAsyncEnumerator(Ct));
		Assert.True(run.Closed);
	}

	[Fact]
	public static async Task Buffered_Break_CancelsTheQuery()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 40),
			asyncReads: true);

		await foreach (ChannelRow row in run.View.ToBufferedAsyncStream(Ct))
		{
			if (row.Id != 5) continue;

			await Until(() => reader is { Stalled: true }, "the stall");
			break;
		}

		Assert.True(reader!.CancelSeen);
		Assert.True(run.Closed);
		Assert.Equal(1, reader.Disposes);
	}
}
