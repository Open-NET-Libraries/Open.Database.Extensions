using System.Buffers;
using System.Reflection;
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
	sealed class StallingReader(
		int rows,
		int stallAfter,
		bool throwOnCancel = false)
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

				// A provider whose cancel callback fails.
				using var registration = throwOnCancel
					? cancellationToken.Register(
						static () => throw new InvalidOperationException(
							"provider cancel failed"))
					: default;
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
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);

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
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);

		PooledArray<ChannelRow> items = await buffer.Items.Within();
		Assert.True(items.Count is 0);
		Assert.Empty(items);
		items.CopyTo([]);
	}

	// ---- 2: Mode 1, lazy, one fill -------------------------------------

	[Fact]
	public static async Task ToAsyncBuffer_StartsAtTheCall_AndFillsOnce()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);

		// Nobody reads: the read runs, and the connection closes.
		await Until(() => run.Closed, "the connection closed");
		Assert.Equal(1, run.Executes);

		// Items attaches to that one fill, however often it is asked.
		Task<PooledArray<ChannelRow>> first = buffer.Items;
		Assert.Same(first, buffer.Items);
		await first.Within();
		Assert.Same(first, buffer.Items);

		Assert.Equal(1, run.Executes);
	}

	[Fact]
	public static async Task ToAsyncBuffer_OfADefaultView_ThrowsAtTheCall()
	{
		MappedCommand<ChannelRow> view = default;

		Assert.Throws<InvalidOperationException>(
			() => view.ToAsyncBuffer(clearOnDispose: false, Ct));
		await Task.CompletedTask;
	}

	// ---- 4: disposing the PooledArray disposes the buffer --------------

	[Fact]
	public static async Task DisposingTheItems_DisposesTheBuffer()
	{
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();

		items.Dispose();

		Assert.Throws<ObjectDisposedException>(() => items.Count);
		Assert.Throws<ObjectDisposedException>(() => items[0]);
		Assert.Throws<ObjectDisposedException>(() => { _ = buffer.Items; });

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
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
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
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
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

	// ---- 7: Mode 2 ------------------------------------------------------

	[Fact]
	public static async Task Stream_YieldsExactSlices_Lazy_AndSingleShot()
	{
		var run = Rows100();
		var stream = run.View.ToAsyncBufferStream(clearOnReturn: false, Ct);

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
			.ToAsyncBufferStream(clearOnReturn: false, Ct)
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

		await foreach (var chunk in run.View.ToAsyncBufferStream(clearOnReturn: false, Ct))
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
		var stream = run.View.ToAsyncBufferStream(clearOnReturn: false, cts.Token);
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
		await using var buffer = run2.View.ToAsyncBuffer(clearOnDispose: false, cts2.Token);
		Task<PooledArray<ChannelRow>> items = buffer.Items;
		await Until(() => reader is { Stalled: true }, "the stall");
		cts2.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => items.Within());
	}

	[Fact]
	public static async Task ReaderFault_IsThrown_AfterTheDeliveredChunks()
	{
		// The 30th read fails: 29 rows were read, in two chunks.
		Run Faulty() => Run.Create(
			() => new ChannelTestReader(40, failAtRow: 30));

		var stream = Faulty().View.ToAsyncBufferStream(clearOnReturn: false, Ct);
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

		// The buffer is not readable while it fills, and a faulted read
		// has no view: Items faults, every time it is asked.
		await using var buffer = Faulty().View.ToAsyncBuffer(clearOnDispose: false, Ct);
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => buffer.Items.Within());
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
			var buffer = run.View.ToAsyncBuffer(clearOnDispose: false);
			completed = buffer.Items.Wait(TimeSpan.FromSeconds(20));

			var e = run.View.ToAsyncBufferStream(clearOnReturn: false).GetAsyncEnumerator();
			completed &= e.MoveNextAsync().AsTask()
				.Wait(TimeSpan.FromSeconds(20));
		});
		thread.Start();
		await Task.Run(() => thread.Join(TimeSpan.FromSeconds(60)), Ct);

		Assert.True(completed);
		Assert.Equal(0, context.Posts);
	}

	// ---- swarm fixes ----------------------------------------------------

	[Fact]
	public static async Task AfterDispose_TheEnumeratorIndexerAndCopyTo_Throw()
	{
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();

		// Created, and moved once, before the dispose.
		IEnumerator<ChannelRow> e = items.GetEnumerator();
		Assert.True(e.MoveNext());
		ChannelRow[] copy = new ChannelRow[100];

		items.Dispose();

		Assert.Throws<ObjectDisposedException>(() => e.MoveNext());
		Assert.Throws<ObjectDisposedException>(() => items[0]);
		Assert.Throws<ObjectDisposedException>(
			() => items.CopyTo(copy));
		Assert.Throws<ObjectDisposedException>(
			() => items.GetEnumerator());
	}

	[Fact]
	public static async Task Indexer_IsRightAtEverySegmentBoundary()
	{
		// Arrays of 16, 32, 64 ... rows start at 0, 16, 48, 112, 240,
		// 496, 1008 (the last array is not full).
		var run = Run.Create(() => new ChannelTestReader(1100));
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();

		int[] at = [0, 15, 16, 47, 48, 111, 112, 239, 240, 495, 496,
			1007, 1008, 1099];
		foreach (int i in at)
			Assert.Equal(i + 1, items[i].Id);

		Assert.Equal(1100, items.Count);
	}

	[Fact]
	public static async Task AStreamEnumeration_WithACancelledToken_Throws()
	{
		var run = Rows100();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				await foreach (var _ in run.View
					.ToAsyncBufferStream(clearOnReturn: false, Ct)
					.WithCancellation(cts.Token)) { }
			});
	}

	// ---- the completed view is a read-only ICollection<T> ---------------

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(16)]
	[InlineData(17)]
	[InlineData(100)]
	public static async Task TheItems_AreAFastPathCollection_ForLinq(int n)
	{
		var run = Run.Create(() => new ChannelTestReader(n));
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();

		Assert.True(Enumerable.TryGetNonEnumeratedCount(items, out int c));
		Assert.Equal(n, c);

		List<int> expected = Ids(n);
		ChannelRow[] array = items.ToArray();
		Assert.Equal(n, array.Length);
		Assert.Equal(expected, array.Select(r => r.Id).ToList());

		List<ChannelRow> list = items.ToList();
		Assert.Equal(expected, list.Select(r => r.Id).ToList());
		Assert.Equal(n, list.Capacity);

		List<ChannelRow> made = new(items);
		Assert.Equal(expected, made.Select(r => r.Id).ToList());
		Assert.Equal(n, made.Capacity);
	}

	[Fact]
	public static async Task TheCollectionView_IsReadOnly_AndSearchable()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		ICollection<ChannelRow> collection = items;

		Assert.True(collection.IsReadOnly);
		Assert.Equal(100, collection.Count);
		Assert.Throws<NotSupportedException>(
			() => collection.Add(new ChannelRow(1)));
		Assert.Throws<NotSupportedException>(() => collection.Clear());
		Assert.Throws<NotSupportedException>(
			() => collection.Remove(items[0]));

		// Records compare by value, across segments.
		Assert.True(collection.Contains(new ChannelRow(1)));
		Assert.True(collection.Contains(new ChannelRow(17)));
		Assert.True(collection.Contains(new ChannelRow(100)));
		Assert.False(collection.Contains(new ChannelRow(101)));
		Assert.False(collection.Contains(new ChannelRow(0)));
	}

	[Fact]
	public static async Task TheCollectionCopyTo_ValidatesAndCopiesInOrder()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		ICollection<ChannelRow> collection = items;

		Assert.Throws<ArgumentNullException>(
			() => collection.CopyTo(null!, 0));
		Assert.Throws<ArgumentOutOfRangeException>(
			() => collection.CopyTo(new ChannelRow[100], -1));
		Assert.Throws<ArgumentException>(
			() => collection.CopyTo(new ChannelRow[99], 0));
		Assert.Throws<ArgumentException>(
			() => collection.CopyTo(new ChannelRow[100], 1));
		Assert.Throws<ArgumentException>(
			() => collection.CopyTo(new ChannelRow[100], 101));

		ChannelRow[] target = new ChannelRow[103];
		collection.CopyTo(target, 3);
		Assert.Equal(Ids(100), target[3..].Select(r => r.Id).ToList());
		Assert.All(target[..3], r => Assert.Null(r));
	}

	[Fact]
	public static async Task TheCollectionMembers_ThrowAfterDispose()
	{
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		ICollection<ChannelRow> collection = items;

		items.Dispose();

		Assert.Throws<ObjectDisposedException>(
			() => collection.IsReadOnly);
		Assert.Throws<ObjectDisposedException>(
			() => collection.Add(new ChannelRow(1)));
		Assert.Throws<ObjectDisposedException>(
			() => collection.Clear());
		Assert.Throws<ObjectDisposedException>(
			() => collection.Remove(new ChannelRow(1)));
		Assert.Throws<ObjectDisposedException>(() => collection.Count);
		Assert.Throws<ObjectDisposedException>(
			() => collection.Contains(new ChannelRow(1)));
		Assert.Throws<ObjectDisposedException>(
			() => collection.CopyTo(new ChannelRow[100], 0));
		Assert.Throws<ObjectDisposedException>(
			() => Enumerable.TryGetNonEnumeratedCount(items, out _));
	}

	// ---- a caller's pool: every rent is matched by one return ------------

	static void AssertBalanced(CountingPool<ChannelRow> pool)
	{
		Assert.True(pool.Rents > 0, "nothing was rented");
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
		Assert.Equal(0, pool.Outstanding);
	}

	public static TheoryData<string> Terminals
		=> ["buffer", "stream", "buffered"];

	static async Task<int> ReadAll(
		string terminal,
		MappedCommand<ChannelRow> view,
		MemoryPool<ChannelRow> pool)
	{
		int rows = 0;
		switch (terminal)
		{
			case "buffer":
				{
					await using var buffer = view.ToAsyncBuffer(pool, Ct);
					rows = (await buffer.Items.Within()).Count;
					break;
				}

			case "stream":
				await foreach (var chunk in view
					.ToAsyncBufferStream(pool, Ct))
				{
					rows += chunk.Length;
				}

				break;

			default:
				await foreach (var _ in view
					.ToBufferedAsyncStream(pool, Ct))
				{
					++rows;
				}

				break;
		}

		return rows;
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task APool_SeesEveryRentReturnedOnce_OnAFullRead(
		string terminal)
	{
		var pool = new CountingPool<ChannelRow>();
		var run = Rows100();

		Assert.Equal(100, await ReadAll(terminal, run.View, pool));

		AssertBalanced(pool);
		Assert.Equal(3, pool.Rents);
	}

	[Fact]
	public static void ANullPool_ThrowsAtTheCall_OnEveryOverload()
	{
		var run = Rows100();
		MemoryPool<ChannelRow> none = null!;

		foreach (Action call in new Action[]
		{
			() => run.View.ToAsyncBuffer(none, Ct),
			() => run.View.ToAsyncBuffer(none, clearOnDispose: true, Ct),
			() => run.View.ToAsyncBufferStream(none, Ct),
			() => run.View.ToAsyncBufferStream(none, clearOnReturn: true, Ct),
			() => run.View.ToBufferedAsyncStream(none, Ct),
			() => run.View.ToBufferedAsyncStream(none, clearOnReturn: true, Ct),
		})
		{
			var ex = Assert.Throws<ArgumentNullException>(call);
			Assert.Equal("pool", ex.ParamName);
		}

		// Nothing was started by any of them.
		Assert.Equal(0, run.Executes);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task TheSharedPoolForm_ReadsEveryRow(string terminal)
	{
		var run = Rows100();
		int rows = 0;

		switch (terminal)
		{
			case "buffer":
				{
					await using var buffer = run.View
						.ToAsyncBuffer(clearOnDispose: false, Ct);
					rows = (await buffer.Items.Within()).Count;
					break;
				}

			case "stream":
				await foreach (var chunk in run.View
					.ToAsyncBufferStream(clearOnReturn: false, Ct))
				{
					rows += chunk.Length;
				}

				break;

			default:
				await foreach (var _ in run.View
					.ToBufferedAsyncStream(clearOnReturn: false, Ct))
				{
					++rows;
				}

				break;
		}

		Assert.Equal(100, rows);
	}

	[Theory]
	[InlineData("stream")]
	[InlineData("buffered")]
	public static async Task APool_SeesEveryRentReturned_AfterABreak(
		string terminal)
	{
		var pool = new CountingPool<ChannelRow>();
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 40),
			asyncReads: true);

		if (terminal == "stream")
		{
			await foreach (var chunk in run.View
				.ToAsyncBufferStream(pool, Ct))
			{
				await Until(() => reader is { Stalled: true }, "the stall");
				break;
			}
		}
		else
		{
			await foreach (ChannelRow row in run.View
				.ToBufferedAsyncStream(pool, Ct))
			{
				if (row.Id != 5) continue;

				await Until(() => reader is { Stalled: true }, "the stall");
				break;
			}
		}

		// The break disposed the enumerator, which awaited the
		// producer and drained the channel: no waiting here.
		Assert.True(reader!.CancelSeen);
		AssertBalanced(pool);
	}

	[Fact]
	public static async Task APool_SeesEveryRentReturned_AfterADisposeWhileFilling()
	{
		var pool = new CountingPool<ChannelRow>();
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 40),
			asyncReads: true);
		var buffer = run.View.ToAsyncBuffer(pool, Ct);
		_ = buffer.Items;
		await Until(() => reader is { Stalled: true }, "the stall");

		await buffer.DisposeAsync().AsTask().Within();

		AssertBalanced(pool);

		// The sync form returns them when the producer exits.
		var pool2 = new CountingPool<ChannelRow>();
		StallingReader? reader2 = null;
		var run2 = Run.Create(
			() => reader2 = new StallingReader(100, 40),
			asyncReads: true);
		var buffer2 = run2.View.ToAsyncBuffer(pool2, Ct);
		_ = buffer2.Items;
		await Until(
			() => reader2 is { Stalled: true },
			"the second stall");

		buffer2.Dispose();

		await Until(() => run2.Closed, "the connection closed");
		await Until(() => pool2.Outstanding == 0, "the arrays back");
		AssertBalanced(pool2);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task APool_SeesEveryRentReturned_AfterAFault(
		string terminal)
	{
		var pool = new CountingPool<ChannelRow>();
		var run = Run.Create(
			() => new ChannelTestReader(40, failAtRow: 30));

		await Assert.ThrowsAsync<InvalidOperationException>(
			() => ReadAll(terminal, run.View, pool));

		AssertBalanced(pool);
	}

	[Theory]
	[InlineData(0)]
	[InlineData(3)]
	public static async Task ADrainedEnumerator_ThrowsAfterDispose(int n)
	{
		var run = Run.Create(() => new ChannelTestReader(n));
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();
		IEnumerator<ChannelRow> e = items.GetEnumerator();
		for (int i = 0; i < n; ++i) Assert.True(e.MoveNext());

		// Nothing is left, and the buffer is disposed: not a quiet
		// false, a throw.
		items.Dispose();

		Assert.Throws<ObjectDisposedException>(() => e.MoveNext());
	}

	// ---- the eager buffer ---------------------------------------------

	[Fact]
	public static async Task DisposeBeforeTheFillFinishes_CancelsTheQuery()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);

		// No access at all: the fill started at the call.
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		await Until(() => reader is { Stalled: true }, "the stall");

		await buffer.DisposeAsync().AsTask().Within();

		Assert.True(reader!.CancelSeen);
		Assert.True(run.Closed);
	}

	[Fact]
	public static async Task DisposeAfterTheFillFinished_OnlyReleasesTheArrays()
	{
		var pool = new CountingPool<ChannelRow>();
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(pool, Ct);

		Task<PooledArray<ChannelRow>> items = buffer.Items;
		await items.Within();
		Assert.True(items.IsCompletedSuccessfully);

		await buffer.DisposeAsync();

		// Nothing was cancelled: one execute, one close, one dispose
		// of the reader, every array back exactly once.
		Assert.Equal(1, run.Executes);
		Assert.Equal(1, run.Connection.Log.Count(l => l == "close"));
		Assert.Equal(1, ((ChannelTestReader)run.Reader!).Disposes);
		AssertBalanced(pool);
	}

	[Fact]
	public static async Task ACallerToken_CancelledMidFill_ReachesItems()
	{
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);

		// Nobody has touched the buffer when the token fires.
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, cts.Token);
		await Until(() => reader is { Stalled: true }, "the stall");
		cts.Cancel();
		await Until(() => run.Closed, "the connection closed");

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => buffer.Items.Within());
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => buffer.Items.Within());
	}

	[Fact]
	public static async Task AFault_NobodyReads_RaisesNoUnobservedTask()
	{
		List<Exception> unobserved = [];
		void OnUnobserved(object? s, UnobservedTaskExceptionEventArgs e)
		{
			lock (unobserved)
			{
				unobserved.Add(e.Exception);
			}
		}

		TaskScheduler.UnobservedTaskException += OnUnobserved;
		try
		{
			var run = Run.Create(
				() => new ChannelTestReader(40, failAtRow: 30));
			var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);

			// Nobody reads: the fill ends with the fault, stored.
			await Until(() => run.Closed, "the connection closed");
			await Task.Delay(100, Ct);

			// A later reader still gets it, after the chunks.
			await Assert.ThrowsAsync<InvalidOperationException>(
				() => buffer.Items.Within());
			await buffer.DisposeAsync();

			// And a buffer nobody ever touched.
			var run2 = Run.Create(
				() => new ChannelTestReader(40, failAtRow: 30));
			var untouched = run2.View.ToAsyncBuffer(clearOnDispose: false, Ct);
			await Until(() => run2.Closed, "the second close");
			await Task.Delay(100, Ct);
			await untouched.DisposeAsync();

			for (int i = 0; i < 3; ++i)
			{
				GC.Collect();
				GC.WaitForPendingFinalizers();
			}

			lock (unobserved)
			{
				Assert.DoesNotContain(
					unobserved,
					e => e.ToString().Contains("a row failed"));
			}
		}
		finally
		{
			TaskScheduler.UnobservedTaskException -= OnUnobserved;
		}
	}

	// ---- the proven repros of the whole-PR review ------------------------

	[Fact]
	public static async Task Dispose_NeverThrows_WhenTheProvidersCancelFails()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20, true),
			asyncReads: true);
		var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
		await Until(() => reader is { Stalled: true }, "the stall");

		Assert.Null(Record.Exception(() => buffer.Dispose()));
		Assert.Null(await Record.ExceptionAsync(
			() => buffer.DisposeAsync().AsTask()));
	}

	[Fact]
	public static async Task DisposeAsync_OfTheStream_ReturnsEveryArray_WhenTheProvidersCancelFails()
	{
		var pool = new CountingPool<ChannelRow>();
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 40, true),
			asyncReads: true);
		var e = run.View
			.ToAsyncBufferStream(pool, Ct)
			.GetAsyncEnumerator(Ct);
		Assert.True(await e.MoveNextAsync().AsTask().Within());
		await Until(() => reader is { Stalled: true }, "the stall");

		Exception? thrown = await Record.ExceptionAsync(
			() => e.DisposeAsync().AsTask().Within());

		Assert.Null(thrown);
		await Until(() => pool.Outstanding == 0, "the arrays back");
		AssertBalanced(pool);
	}

	[Fact]
	public static async Task ACallerCancel_OfTheStream_CarriesTheCallersToken()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		await using var e = run.View
			.ToAsyncBufferStream(clearOnReturn: false, cts.Token)
			.GetAsyncEnumerator(Ct);
		Assert.True(await e.MoveNextAsync().AsTask().Within());
		await Until(() => reader is { Stalled: true }, "the stall");
		cts.Cancel();

		var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				while (await e.MoveNextAsync().AsTask().Within()) { }
			});

		Assert.Equal(cts.Token, ex.CancellationToken);
	}

	[Fact]
	public static async Task ACallerCancel_OfItems_CarriesTheCallersToken()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		await using var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, cts.Token);
		await Until(() => reader is { Stalled: true }, "the stall");
		cts.Cancel();

		var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => buffer.Items.Within());

		Assert.Equal(cts.Token, ex.CancellationToken);
		Assert.True(buffer.Items.IsCanceled);
	}

	[Fact]
	public static async Task AnItems_TouchedButNeverAwaited_RaisesNoUnobservedTask()
	{
		List<Exception> unobserved = [];
		void OnUnobserved(object? s, UnobservedTaskExceptionEventArgs e)
		{
			lock (unobserved) unobserved.Add(e.Exception);
		}

		TaskScheduler.UnobservedTaskException += OnUnobserved;
		try
		{
			var run = Run.Create(
				() => new ChannelTestReader(40, failAtRow: 30));
			var buffer = run.View.ToAsyncBuffer(clearOnDispose: false, Ct);
			_ = buffer.Items;
			await Until(() => run.Closed, "the connection closed");
			await Task.Delay(100, Ct);
			await buffer.DisposeAsync();

			for (int i = 0; i < 3; ++i)
			{
				GC.Collect();
				GC.WaitForPendingFinalizers();
			}

			lock (unobserved)
			{
				Assert.DoesNotContain(
					unobserved,
					e => e.ToString().Contains("a row failed"));
			}
		}
		finally
		{
			TaskScheduler.UnobservedTaskException -= OnUnobserved;
		}
	}

	// ---- the memory pool and its owners -----------------------------------

	static readonly Type Extensions = typeof(PooledBuffer<>).Assembly
		.GetType("Open.Database.Extensions.Core.MemoryOwnerExtensions")!;

	static IMemoryOwner<T> Slice<T>(IMemoryOwner<T> owner, int count)
		=> (IMemoryOwner<T>)Extensions
			.GetMethod(
				"Slice",
				BindingFlags.Static | BindingFlags.NonPublic)!
			.MakeGenericMethod(typeof(T))
			.Invoke(null, [owner, count])!;

	static void Release<T>(IMemoryOwner<T> owner)
		=> Extensions
			.GetMethod(
				"Release",
				BindingFlags.Static | BindingFlags.NonPublic)!
			.MakeGenericMethod(typeof(T))
			.Invoke(null, [owner]);

	[Fact]
	public static void Slice_ReturnsTheSameOwner_WhenTheLengthMatches()
	{
		var pool = new CountingPool<int>();
		IMemoryOwner<int> owner = pool.Rent(16);

		Assert.Same(owner, Slice(owner, 16));
	}

	[Fact]
	public static void Slice_WrapsAnOwnerWhoseMemoryIsLonger()
	{
		var pool = new CountingPool<int>();
		IMemoryOwner<int> host = pool.Rent(16);

		IMemoryOwner<int> sliced = Slice(host, 10);

		Assert.NotSame(host, sliced);
		Assert.Equal(10, sliced.Memory.Length);

		// Disposing the wrapper disposes the host (the array goes back
		// once); a second disposal is safe, and after it the memory is
		// refused.
		sliced.Dispose();
		Assert.Equal(1, pool.Returns);
		Assert.Null(Record.Exception(() => sliced.Dispose()));
		Assert.Equal(1, pool.Returns);
		Assert.Throws<ObjectDisposedException>(() => sliced.Memory);
		Assert.Throws<ObjectDisposedException>(() => host.Memory);
	}

	[Fact]
	public static void Slice_OfTheSharedPoolsOwner_IsSafeToDisposeTwice()
	{
		IMemoryOwner<int> host = MemoryPool<int>.Shared.Rent(10);
		IMemoryOwner<int> sliced = Slice(host, 7);

		Assert.Equal(7, sliced.Memory.Length);
		sliced.Dispose();
		Assert.Null(Record.Exception(() => sliced.Dispose()));
		Assert.Throws<ObjectDisposedException>(() => sliced.Memory);
	}

	[Fact]
	public static async Task APoolThatRoundsUp_StillYieldsExactMemoryEverywhere()
	{
		var pool = new CountingPool<ChannelRow> { RoundUp = true };
		var run = Rows100();

		await using (var buffer = run.View.ToAsyncBuffer(pool, Ct))
		{
			PooledArray<ChannelRow> items = await buffer.Items.Within();
			Assert.Equal(100, items.Count);
			Assert.Equal(Ids(100), items.Select(r => r.Id).ToList());
			Assert.Equal(100, items[99].Id);
			Assert.Equal(48, items[47].Id);
			Assert.Equal(49, items[48].Id);

			ChannelRow[] copy = new ChannelRow[100];
			items.CopyTo(copy);
			Assert.Equal(Ids(100), copy.Select(r => r.Id).ToList());
			Assert.Equal(
				Ids(100),
				items.ToArray().Select(r => r.Id).ToList());
		}

		AssertBalanced(pool);

		var pool2 = new CountingPool<ChannelRow> { RoundUp = true };
		var run2 = Rows100();
		List<int> sizes = [];
		List<int> ids = [];
		await foreach (var chunk in run2.View.ToAsyncBufferStream(pool2, Ct))
		{
			sizes.Add(chunk.Length);
			ids.AddRange(chunk.ToArray().Select(r => r.Id));
		}

		Assert.Equal([16, 32, 52], sizes);
		Assert.Equal(Ids(100), ids);
		AssertBalanced(pool2);
	}

	[Fact]
	public static async Task Release_DoesNotClearByDefault()
	{
		var pool = new CountingPool<ChannelRow>();
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(pool, Ct);
		await buffer.Items.Within();
		await buffer.DisposeAsync();

		// The BCL default: the arrays went back with the rows in them.
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(1, pool.Captured[0][0].Id);
		Assert.All(pool.Captured, a => Assert.NotNull(a[0]));
	}

	// The recipe the pool parameter's docs give: a pool whose owners
	// clear on Dispose scrubs the rows.
	sealed class ClearingPool<T>(MemoryPool<T> inner) : MemoryPool<T>
	{
		public override int MaxBufferSize
			=> inner.MaxBufferSize;

		public override IMemoryOwner<T> Rent(int minBufferSize = -1)
			=> new ClearingOwner(inner.Rent(minBufferSize));

		protected override void Dispose(bool disposing) { }

		sealed class ClearingOwner(IMemoryOwner<T> host)
			: IMemoryOwner<T>
		{
			public Memory<T> Memory
				=> host.Memory;

			public void Dispose()
			{
				host.Memory.Span.Clear();
				host.Dispose();
			}
		}
	}

	[Fact]
	public static async Task AClearingPool_ScrubsTheRows_OnEveryPath()
	{
		var counting = new CountingPool<ChannelRow>();
		var pool = new ClearingPool<ChannelRow>(counting);
		var run = Rows100();

		var buffer = run.View.ToAsyncBuffer(pool, Ct);
		await buffer.Items.Within();
		Assert.All(counting.Captured, a => Assert.NotNull(a[0]));
		await buffer.DisposeAsync();
		Assert.All(
			counting.Captured,
			a => Assert.All(a, r => Assert.Null(r)));

		var counting2 = new CountingPool<ChannelRow>();
		var pool2 = new ClearingPool<ChannelRow>(counting2);
		await foreach (var _ in Rows100().View
			.ToAsyncBufferStream(pool2, Ct)) { }

		Assert.True(counting2.Rents > 0);
		Assert.Equal(counting2.Rents, counting2.Returns);
		Assert.All(
			counting2.Captured,
			a => Assert.All(a, r => Assert.Null(r)));
	}

	[Fact]
	public static async Task Release_DoesNotClear_AValueType()
	{
		var pool = new CountingPool<int>();
		var view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(100))
			.Map<int>()
			.AsyncReads();
		var buffer = view.ToAsyncBuffer(pool, Ct);
		PooledArray<int> items = await buffer.Items.Within();
		Assert.Equal(Enumerable.Range(1, 100), items);

		await buffer.DisposeAsync();

		// Nothing to keep alive: the rows are left as they were.
		Assert.Equal(1, pool.Captured[0][0]);
		Assert.Equal(pool.Rents, pool.Returns);
	}

	[Fact]
	public static void Release_JustDisposesTheOwner()
	{
		var pool = new CountingPool<string>();
		IMemoryOwner<string> owner = pool.Rent(4);
		owner.Memory.Span[0] = "a";

		Release(owner);

		Assert.Equal("a", pool.Captured[0][0]);
		Assert.Equal(1, pool.Returns);
	}

	[Fact]
	public static async Task APoolWhoseReturnThrows_PropagatesFromDisposeAsync()
	{
		var pool = new CountingPool<ChannelRow>();
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(pool, Ct);
		await buffer.Items.Within();
		pool.ThrowOnReturn = true;

		// The documented contract: the exception is the caller's pool's.
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => buffer.DisposeAsync().AsTask());
	}

	// ---- the lock-free Items: stress ---------------------------------------

	[Fact]
	public static async Task ManyThreadsTouchingItems_ShareOneTask_AndLoseNoCompletion()
	{
		const int Iterations = 10_000;
		const int Threads = 4;
		var view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(2))
			.Map<ChannelRow>();

		for (int i = 0; i < Iterations; ++i)
		{
			var buffer = view.ToAsyncBuffer(clearOnDispose: false, Ct);
			int round = i;

			// The first accesses land before, during and after the
			// read finishes, one way or another.
			Task<Task<PooledArray<ChannelRow>>>[] touches = [.. Enumerable
				.Range(0, Threads)
				.Select(k => Task.Factory.StartNew(
					() =>
					{
						Thread.SpinWait(((round * 31) + (k * 97)) % 2000);
						return buffer.Items;
					},
					Ct,
					TaskCreationOptions.None,
					TaskScheduler.Default))];
			Task<PooledArray<ChannelRow>>[] tasks
				= await Task.WhenAll(touches).Within();

			Assert.All(tasks, t => Assert.Same(tasks[0], t));
			PooledArray<ChannelRow> items = await tasks[0].Within();
			Assert.Equal(2, items.Count);
			Assert.All(
				tasks,
				t => Assert.Same(items, t.Result));

			await buffer.DisposeAsync();
		}
	}

	[Fact]
	public static async Task ADisposeRacingTheFinishAndItems_ReleasesEveryArrayOnce()
	{
		const int Iterations = 3_000;
		var pool = new CountingPool<ChannelRow>();
		var view = new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(40))
			.Map<ChannelRow>();

		for (int i = 0; i < Iterations; ++i)
		{
			var buffer = view.ToAsyncBuffer(pool, Ct);
			int round = i;
			Task touch = Task.Run(
				async () =>
				{
					Thread.SpinWait((round * 53) % 3000);
					try { await buffer.Items; }
					catch (ObjectDisposedException) { }
					catch (OperationCanceledException) { }
				},
				Ct);
			Task dispose = Task.Run(
				async () =>
				{
					Thread.SpinWait((round * 71) % 3000);
					if (round % 2 == 0) buffer.Dispose();
					else await buffer.DisposeAsync();
				},
				Ct);

			await Task.WhenAll(touch, dispose).Within();
			await buffer.DisposeAsync();
		}

		await Until(() => pool.Outstanding == 0, "the arrays back");
		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
	}

	// ---- ToBufferedAsyncStream ------------------------------------------

	[Fact]
	public static async Task Buffered_YieldsEveryRowInOrder_Once()
	{
		var run = Rows100();
		var stream = run.View.ToBufferedAsyncStream(clearOnReturn: false, Ct);

		await Task.Delay(100, Ct);
		Assert.Equal(0, run.Executes);

		List<int> ids = [];
		await foreach (ChannelRow row in stream)
			ids.Add(row.Id);

		Assert.Equal(Ids(100), ids);
		Assert.Equal(1, run.Executes);

		// A second enumeration is refused at its first step.
		await using var again = stream.GetAsyncEnumerator(Ct);
		await Assert.ThrowsAsync<InvalidOperationException>(
			() => again.MoveNextAsync().AsTask());
		Assert.True(run.Closed);
	}

	[Fact]
	public static async Task Buffered_Break_CancelsTheQuery()
	{
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 40),
			asyncReads: true);

		await foreach (ChannelRow row in run.View.ToBufferedAsyncStream(clearOnReturn: false, Ct))
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
