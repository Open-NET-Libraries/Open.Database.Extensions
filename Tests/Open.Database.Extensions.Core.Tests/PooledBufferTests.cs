using System.Buffers;
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
	public static async Task ToAsyncBuffer_StartsAtTheCall_AndFillsOnce()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(Ct);

		// Nobody reads: the read runs, and the connection closes.
		await Until(() => run.Closed, "the connection closed");
		Assert.Equal(1, run.Executes);

		// Items and every enumeration attach to that one fill.
		Task<PooledArray<ChannelRow>> first = buffer.Items;
		Assert.Same(first, buffer.Items);
		await first.Within();
		await foreach (var _ in buffer) { }
		await foreach (var _ in buffer) { }

		Assert.Equal(1, run.Executes);
	}

	[Fact]
	public static async Task ToAsyncBuffer_OfADefaultView_ThrowsAtTheCall()
	{
		MappedCommand<ChannelRow> view = default;

		Assert.Throws<InvalidOperationException>(
			() => view.ToAsyncBuffer(Ct));
		await Task.CompletedTask;
	}

	[Fact]
	public static async Task AnEnumeration_AttachesToTheRunningFill()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(Ct);

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

	// ---- swarm fixes ----------------------------------------------------

	[Fact]
	public static async Task AfterDispose_TheEnumeratorIndexerAndCopyTo_Throw()
	{
		var run = Rows100();
		var buffer = run.View.ToAsyncBuffer(Ct);
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
		await using var buffer = run.View.ToAsyncBuffer(Ct);
		PooledArray<ChannelRow> items = await buffer.Items.Within();

		int[] at = [0, 15, 16, 47, 48, 111, 112, 239, 240, 495, 496,
			1007, 1008, 1099];
		foreach (int i in at)
			Assert.Equal(i + 1, items[i].Id);

		Assert.Equal(1100, items.Count);
	}

	[Fact]
	public static async Task ACancelledEnumeration_Throws_TheBufferStaysWhole()
	{
		var run = Rows100();
		await using var buffer = run.View.ToAsyncBuffer(Ct);
		await buffer.Items.Within();

		using var cts = new CancellationTokenSource();
		cts.Cancel();

		// The arrays are all there, and the cancelled token still
		// stops the enumeration at the first step.
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				await foreach (var _ in buffer.WithCancellation(cts.Token)) { }
			});

		List<int[]> again = await Chunks(buffer).Within();
		AssertContiguous(again, 100);
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
					.ToAsyncBufferStream(Ct)
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
		await using var buffer = run.View.ToAsyncBuffer(Ct);
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
		await using var buffer = run.View.ToAsyncBuffer(Ct);
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
		await using var buffer = run.View.ToAsyncBuffer(Ct);
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
		var buffer = run.View.ToAsyncBuffer(Ct);
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

	// An ArrayPool that records what was rented and what came back,
	// and that a return of an array it did not hand out (or twice) is
	// counted as unmatched.
	sealed class CountingPool<T> : ArrayPool<T>
	{
		readonly ArrayPool<T> _inner = Create();

		readonly System.Collections.Concurrent.ConcurrentDictionary<T[], int>
			_out = new(ReferenceEqualityComparer.Instance);

		int _rents;
		int _returns;
		int _unmatched;

		public int Rents => Volatile.Read(ref _rents);

		public int Returns => Volatile.Read(ref _returns);

		public int Unmatched => Volatile.Read(ref _unmatched);

		public int Outstanding => _out.Count;

		public override T[] Rent(int minimumLength)
		{
			T[] array = _inner.Rent(minimumLength);
			Interlocked.Increment(ref _rents);
			_out[array] = 0;
			return array;
		}

		public override void Return(T[] array, bool clearArray = false)
		{
			Interlocked.Increment(ref _returns);
			if (!_out.TryRemove(array, out _))
				Interlocked.Increment(ref _unmatched);

			_inner.Return(array, clearArray);
		}
	}

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
		ArrayPool<ChannelRow>? pool)
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

	[Theory, MemberData(nameof(Terminals))]
	public static async Task ANullPool_UsesTheSharedPool(string terminal)
	{
		var run = Rows100();

		// The literal null binds to the pool overload only.
		int rows = terminal switch
		{
			"buffer" => await ReadBufferNull(run.View),
			"stream" => await ReadStreamNull(run.View),
			_ => await ReadBufferedNull(run.View),
		};

		Assert.Equal(100, rows);
	}

	static async Task<int> ReadBufferNull(MappedCommand<ChannelRow> view)
	{
		await using var buffer = view.ToAsyncBuffer(null);
		return (await buffer.Items.Within()).Count;
	}

	static async Task<int> ReadStreamNull(MappedCommand<ChannelRow> view)
	{
		int rows = 0;
		await foreach (var chunk in view.ToAsyncBufferStream(null))
			rows += chunk.Length;

		return rows;
	}

	static async Task<int> ReadBufferedNull(MappedCommand<ChannelRow> view)
	{
		int rows = 0;
		await foreach (var _ in view.ToBufferedAsyncStream(null))
			++rows;

		return rows;
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
		var buffer = run.View.ToAsyncBuffer(Ct);
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
		var buffer = run.View.ToAsyncBuffer(Ct);
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
	public static async Task ACallerToken_CancelledMidFill_ReachesItemsAndEnumerations()
	{
		using var cts = CancellationTokenSource
			.CreateLinkedTokenSource(Ct);
		StallingReader? reader = null;
		var run = Run.Create(
			() => reader = new StallingReader(100, 20),
			asyncReads: true);

		// Nobody has touched the buffer when the token fires.
		await using var buffer = run.View.ToAsyncBuffer(cts.Token);
		await Until(() => reader is { Stalled: true }, "the stall");
		cts.Cancel();
		await Until(() => run.Closed, "the connection closed");

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => buffer.Items.Within());
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => buffer.Items.Within());

		// The rows read before the cancel come first, then the cancel,
		// for every enumeration.
		for (int pass = 0; pass < 2; ++pass)
		{
			int rows = 0;
			await Assert.ThrowsAnyAsync<OperationCanceledException>(
				async () =>
				{
					await foreach (var chunk in buffer)
						rows += chunk.Length;
				});
			Assert.Equal(20, rows);
		}
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
			var buffer = run.View.ToAsyncBuffer(Ct);

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
			var untouched = run2.View.ToAsyncBuffer(Ct);
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
		var buffer = run.View.ToAsyncBuffer(Ct);
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
			.ToAsyncBufferStream(cts.Token)
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
		await using var buffer = run.View.ToAsyncBuffer(cts.Token);
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
			var buffer = run.View.ToAsyncBuffer(Ct);
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
