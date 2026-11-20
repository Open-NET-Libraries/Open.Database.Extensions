using System.Buffers;
using System.Data.Common;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Edge cases of the reader extensions:
// cancellation before and in the middle of an iteration
// (the token is checked at the top of each loop, before each read,
// and a cancel throws; once a read has returned no row the result is
// complete and a cancel after that is not an error),
// early stop, pooled rows, deferred execution,
// a result set loaded into a DataTable, NULLs,
// and the argument guards that are eager.
[ExcludeFromCodeCoverage]
public static class DataReaderEdgeTests
{
	/// <summary>A pool that hands out exactly-sized arrays and remembers what was asked for.</summary>
	sealed class SizedPool : ArrayPool<object>
	{
		public List<int> Requests { get; } = [];

		public override object[] Rent(int minimumLength)
		{
			Requests.Add(minimumLength);
			return new object[minimumLength];
		}

		public override void Return(object[] array, bool clearArray = false) { }
	}

	static CancellationTokenSource CancelledSource()
	{
		var cts = new CancellationTokenSource();
		cts.Cancel();
		return cts;
	}

	static async Task<List<object[]>> Collect(IAsyncEnumerable<object[]> rows)
	{
		var list = new List<object[]>();
		await foreach (object[] row in rows)
			list.Add(row);

		return list;
	}

	// ---- ForEach / ForEachAsync ----

	// The token is checked at the top of the loop, before each read.
	// A cancel throws and the read is not made:
	// a cancel during row N lets row N finish, then throws,
	// even when row N was the last.
	// Once a read has returned no row the result is complete,
	// and a cancel after that is not an error.
	[Theory]
	[InlineData(3)]
	[InlineData(10)] // during the last row's handler
	public static void ForEach_CancelledDuringARow_ThrowsBeforeTheNextRead(
		int cancelAtRead)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel();
		});
		int handled = 0;

		Assert.Throws<OperationCanceledException>(() =>
			reader.ForEach(_ => handled++, cts.Token));

		Assert.Equal(cancelAtRead, handled);
		Assert.Equal(cancelAtRead, reader.Reads); // no read after the cancel
	}

	[Fact]
	public static void ForEach_CancelledAsTheEndingReadIsMade_Returns()
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 11) cts.Cancel(); // the read that returns no row
		});
		int handled = 0;

		reader.ForEach(_ => handled++, cts.Token); // does not throw

		Assert.Equal(10, handled);
		Assert.Equal(11, reader.Reads);
	}

	[Theory]
	[InlineData(10)]
	[InlineData(0)] // an empty result is still a read that was not made
	public static void ForEach_CancelledBeforeTheCall_ThrowsWithoutReading(
		int rows)
	{
		using CancellationTokenSource cts = CancelledSource();
		TrackedReader reader = TrackedRows(rows);
		int handled = 0;

		Assert.Throws<OperationCanceledException>(() =>
			reader.ForEach(_ => handled++, cts.Token));

		Assert.Equal(0, handled);
		Assert.Equal(0, reader.Reads);
	}

#pragma warning disable xUnit1051 // The shape under test takes no token.

	[Fact]
	public static void ForEach_WithNoTokenOrNone_ReadsEveryRow()
	{
		TrackedReader none = TrackedRows(10);
		TrackedReader noToken = TrackedRows(10);
		int handled = 0;

		none.ForEach(_ => handled++, CancellationToken.None);
		noToken.ForEach(_ => handled++);

		Assert.Equal(20, handled);
		Assert.Equal(11, none.Reads); // ten rows, then the read that ends
		Assert.Equal(11, noToken.Reads);
	}

#pragma warning restore xUnit1051

	[Theory]
	[InlineData(true)]
	public static async Task ForEachAsync_AwaitsEachHandler_BeforeItReadsTheNextRow(bool useReadAsync)
	{
		TrackedReader reader = TrackedRows(3);
		var readsWhenHandled = new List<int>();

		await reader.ForEachAsync(async _ =>
		{
			await Task.Yield();
			readsWhenHandled.Add(reader.Reads);
		}, useReadAsync, TestContext.Current.CancellationToken);

		Assert.Equal([1, 2, 3], readsWhenHandled);
	}

	[Fact]
	public static async Task ForEachAsync_ACancelledToken_ThrowsWithoutCallingTheHandler()
	{
		TrackedReader reader = TrackedRows(3);
		using CancellationTokenSource cts = CancelledSource();
		bool called = false;

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			await reader.ForEachAsync(_ =>
			{
				called = true;
				return default;
			}, cts.Token));

		Assert.False(called);
		Assert.Equal(0, reader.Reads);
	}

	// ---- Select / SelectAsync ----

	[Fact]
	public static void Select_IsDeferred_AndReadsNoMoreRowsThanTheCallerTakes()
	{
		TrackedReader reader = TrackedRows(5);

		IEnumerable<int> ids = reader.Select(CancellationToken.None, r => r.GetInt32(0));

		Assert.Equal(0, reader.Reads); // nothing is read until it is enumerated
		Assert.Equal([1, 2], ids.Take(2).ToArray());
		Assert.Equal(2, reader.Reads); // and a caller that stops early stops the reading
	}

	// A plain IDataReader (not a DbDataReader) that counts its reads.
	// onRead gets the 1-based number of each read before it is served.
	static (IDataReader Reader, Func<int> Reads) PlainRows(
		int count,
		Action<int> onRead)
	{
		IDataReader reader = FakeReader.Create(
			["Id", "Name", "Note"],
			Rows(count));
		int reads = 0;
		reader
			.When(static x => x.Read())
			.Do(_ => onRead(++reads));

		return (reader, () => reads);
	}

	static (IDataReader Reader, Func<int> Reads) ReaderFor(
		string variant,
		Action<int> onRead)
	{
		if (variant.EndsWith("plain reader", StringComparison.Ordinal))
			return PlainRows(10, onRead);

		TrackedReader tracked = TrackedRows(10, onRead);
		return (tracked, () => tracked.Reads);
	}

	static IAsyncEnumerable<int> Ids(
		string variant,
		IDataReader reader,
		CancellationToken token)
		=> variant == "DbDataReader, transform"
			? ((DbDataReader)reader).SelectAsync(
				static r => (int)r[0],
				token)
			: reader.SelectAsync(
				static r => new ValueTask<int>((int)r[0]),
				token);

	static async Task<List<int>> Collect(IAsyncEnumerable<int> ids)
	{
		var list = new List<int>();
		await foreach (int id in ids)
			list.Add(id);

		return list;
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)] // the token-first overload behaves the same
	public static void Select_CancelledDuringARow_ThrowsBeforeTheNextRead(
		bool tokenFirst)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 3) cts.Cancel();
		});
		IEnumerable<int> ids = tokenFirst
			? reader.Select(cts.Token, static r => r.GetInt32(0))
			: reader.Select(static r => r.GetInt32(0), cts.Token);
		var seen = new List<int>();
		using IEnumerator<int> e = ids.GetEnumerator();

		Assert.Throws<OperationCanceledException>(() =>
		{
			while (e.MoveNext()) seen.Add(e.Current);
		});

		// The rows read before the cancel was seen, and no read after it.
		Assert.Equal([1, 2, 3], seen);
		Assert.Equal(3, reader.Reads);
	}

	[Theory]
	[InlineData(10)] // while the last row is yielded: throws
	[InlineData(11)] // as the read that ends the result is made: returns
	public static void Select_CancelledAtTheEnd_ThrowsOnlyBeforeTheEndingRead(
		int cancelAtRead)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel();
		});
		IEnumerable<int> ids = reader
			.Select(static r => r.GetInt32(0), cts.Token);

		if (cancelAtRead == 10)
		{
			Assert.Throws<OperationCanceledException>(() => ids.ToArray());
			Assert.Equal(10, reader.Reads);
		}
		else
		{
			Assert.Equal(Enumerable.Range(1, 10), ids.ToArray());
			Assert.Equal(11, reader.Reads);
		}
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void Select_CancelledBeforeTheCall_ThrowsOnFirstMoveNext(
		bool tokenFirst)
	{
		using CancellationTokenSource cts = CancelledSource();
		TrackedReader reader = TrackedRows(10);

		// Does not throw: the read is deferred.
		IEnumerable<int> ids = tokenFirst
			? reader.Select(cts.Token, static r => r.GetInt32(0))
			: reader.Select(static r => r.GetInt32(0), cts.Token);

		using IEnumerator<int> e = ids.GetEnumerator();
		Assert.Throws<OperationCanceledException>(() => e.MoveNext());
		Assert.Equal(0, reader.Reads);
	}

	[Fact]
	public static void Select_AConsumerThatStopsEarly_SeesNoLaterCancel()
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 5) cts.Cancel();
		});

		int[] ids = reader
			.Select(static r => r.GetInt32(0), cts.Token)
			.Take(2)
			.ToArray();

		Assert.Equal([1, 2], ids);
		Assert.Equal(2, reader.Reads);
	}

	[Theory]
	[InlineData("DbDataReader, transform")]
	[InlineData("IDataReader, ValueTask, over a DbDataReader")]
	[InlineData("IDataReader, ValueTask, over a plain reader")]
	public static async Task SelectAsync_CancelMidRow_ThrowsBeforeNextRead(
		string variant)
	{
		using var cts = new CancellationTokenSource();
		void Cancel(int n)
		{
			if (n == 3) cts.Cancel();
		}

		(IDataReader reader, Func<int> reads) = ReaderFor(variant, Cancel);
		var seen = new List<int>();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				await foreach (int id in Ids(variant, reader, cts.Token))
					seen.Add(id);
			});

		Assert.Equal([1, 2, 3], seen);
		Assert.Equal(3, reads());
	}

	[Theory]
	[InlineData("DbDataReader, transform")]
	[InlineData("IDataReader, ValueTask, over a DbDataReader")]
	[InlineData("IDataReader, ValueTask, over a plain reader")]
	public static async Task SelectAsync_CancelAtTheEnd_Cases(
		string variant)
	{
		// During the last row (10): throws at the next read.
		using var during = new CancellationTokenSource();
		(IDataReader reader, Func<int> reads) = ReaderFor(
			variant,
			n =>
			{
				if (n == 10) during.Cancel();
			});
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Collect(Ids(variant, reader, during.Token)));
		Assert.Equal(10, reads());

		// As the read that ends the result is made (11): returns.
		using var ending = new CancellationTokenSource();
		(reader, reads) = ReaderFor(
			variant,
			n =>
			{
				if (n == 11) ending.Cancel();
			});
		List<int> seen = await Collect(Ids(variant, reader, ending.Token));
		Assert.Equal(Enumerable.Range(1, 10), seen);
		Assert.Equal(11, reads());
	}

	[Theory]
	[InlineData("DbDataReader, transform")]
	[InlineData("IDataReader, ValueTask, over a DbDataReader")]
	[InlineData("IDataReader, ValueTask, over a plain reader")]
	public static async Task SelectAsync_CancelledBeforeEnumeration_Throws(
		string variant)
	{
		using CancellationTokenSource cts = CancelledSource();
		(IDataReader reader, Func<int> reads) = ReaderFor(
			variant,
			static _ => { });

		// Does not throw: the read is deferred.
		IAsyncEnumerable<int> ids = Ids(variant, reader, cts.Token);

		await using IAsyncEnumerator<int> e = ids
			.GetAsyncEnumerator(CancellationToken.None);
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await e.MoveNextAsync());

		Assert.Equal(0, reads());
	}

	// DEFECT-3b: these already throw, and keep doing so.
	[Fact]
	public static async Task ForEachAsync_And_IterateWhileAsync_Throw()
	{
		static TrackedReader Tracked(CancellationTokenSource cts)
			=> TrackedRows(10, n =>
			{
				if (n == 3) cts.Cancel();
			});

		using var forEach = new CancellationTokenSource();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Tracked(forEach).ForEachAsync(
				static _ => { },
				true,
				forEach.Token));

		using var iterate = new CancellationTokenSource();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Tracked(iterate).IterateWhileAsync(
				static _ => true,
				iterate.Token));
	}

	// Left for the removal of useReadAsync (A7b):
	// this pins only that it throws, not how many rows were handled.
	[Fact]
	public static async Task ForEachAsync_NoReadAsync_CancelMidRead_Throws()
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 3) cts.Cancel();
		});

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await reader.ForEachAsync(
				static _ => default,
				false,
				cts.Token));
	}

	static bool Visit(Action<IDataRecord> each, IDataRecord record)
	{
		each(record);
		return true;
	}

	// The nine async loop forms.
	static ValueTask RunLoop(
		string variant,
		DbDataReader reader,
		Action<IDataRecord> each,
		CancellationToken token)
		=> variant switch
		{
			"ForEachAsync(Action)" => reader.ForEachAsync(each, true, token),
			"ForEachAsync(Func)" => reader.ForEachAsync(
				r =>
				{
					each(r);
					return default;
				},
				true,
				token),
			"IterateWhileAsync(Db)" => reader.IterateWhileAsync(
				r => Visit(each, r),
				token),
			"IterateWhileAsync(Db, useReadAsync)" => reader.IterateWhileAsync(
				r => Visit(each, r),
				true,
				token),
			"IterateWhileAsync(IDataReader)"
				=> ((IDataReader)reader).IterateWhileAsync(
					r => new ValueTask<bool>(Visit(each, r)),
					token),
			"ForEachAsync(Func, no ReadAsync)" => reader.ForEachAsync(
				r =>
				{
					each(r);
					return default;
				},
				false,
				token),
			"IterateWhileAsync(Db, no ReadAsync)" => reader.IterateWhileAsync(
				r => Visit(each, r),
				false,
				token),
			"IterateWhileAsync(IDataReader, no ReadAsync)"
				=> ((IDataReader)reader).IterateWhileAsync(
					r => new ValueTask<bool>(Visit(each, r)),
					false,
					token),
			_ => ((IDataReader)reader).IterateWhileAsync(
				r => new ValueTask<bool>(Visit(each, r)),
				true,
				token),
		};

	[Theory]
	[InlineData("ForEachAsync(Action)")]
	[InlineData("ForEachAsync(Func)")]
	[InlineData("IterateWhileAsync(Db)")]
	[InlineData("IterateWhileAsync(Db, useReadAsync)")]
	[InlineData("IterateWhileAsync(IDataReader)")]
	[InlineData("IterateWhileAsync(IDataReader, useReadAsync)")]
	[InlineData("ForEachAsync(Func, no ReadAsync)")]
	[InlineData("IterateWhileAsync(Db, no ReadAsync)")]
	[InlineData("IterateWhileAsync(IDataReader, no ReadAsync)")]
	public static async Task AsyncLoops_CancelMidRead_ThrowBeforeNextRead(
		string variant)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 3) cts.Cancel();
		});
		int handled = 0;

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await RunLoop(
				variant,
				reader,
				_ => handled++,
				cts.Token));

		Assert.Equal(3, handled);
		Assert.Equal(3, reader.Reads); // no read after the cancel
	}

	[Theory]
	[InlineData("ForEachAsync(Action)")]
	[InlineData("ForEachAsync(Func)")]
	[InlineData("IterateWhileAsync(Db)")]
	[InlineData("IterateWhileAsync(Db, useReadAsync)")]
	[InlineData("IterateWhileAsync(IDataReader)")]
	[InlineData("IterateWhileAsync(IDataReader, useReadAsync)")]
	[InlineData("ForEachAsync(Func, no ReadAsync)")]
	[InlineData("IterateWhileAsync(Db, no ReadAsync)")]
	[InlineData("IterateWhileAsync(IDataReader, no ReadAsync)")]
	public static async Task AsyncLoops_CancelAtTheEnd_Cases(
		string variant)
	{
		// During the last row's handler (10): throws at the next read.
		using var during = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 10) during.Cancel();
		});
		int handled = 0;
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await RunLoop(
				variant,
				reader,
				_ => handled++,
				during.Token));
		Assert.Equal(10, handled);

		// As the read that ends the result is made (11): returns.
		using var ending = new CancellationTokenSource();
		reader = TrackedRows(10, n =>
		{
			if (n == 11) ending.Cancel();
		});
		handled = 0;
		await RunLoop(variant, reader, _ => handled++, ending.Token);
		Assert.Equal(10, handled);
		Assert.Equal(11, reader.Reads);
	}

	// The token goes to ReadAsync, so a cancel during a slow read
	// takes effect at once, without waiting for the server.
	[Theory]
	[InlineData("SelectAsync")]
	[InlineData("ForEachAsync")]
	[InlineData("IterateWhileAsync")]
	public static async Task AsyncLoops_CancelDuringSlowRead_TakesEffect(
		string variant)
	{
		using var cts = new CancellationTokenSource();
		var rig = new TokenRig { Stall = TokenStage.Read }; // never answers
		await using var reader = new TokenReader(rig);
		var run = Task.Run(
			async () =>
			{
				switch (variant)
				{
					case "SelectAsync":
						await foreach (int _ in reader.SelectAsync(
							static r => r.GetInt32(0),
							cts.Token)) { }

						break;
					case "ForEachAsync":
						await reader.ForEachAsync(
							static _ => { },
							true,
							cts.Token);
						break;
					default:
						await reader.IterateWhileAsync(
							static _ => true,
							cts.Token);
						break;
				}
			},
			TestContext.Current.CancellationToken);

		await rig.Entered.Task.WaitAsync(
			TimeSpan.FromSeconds(10),
			TestContext.Current.CancellationToken);
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await run.WaitAsync(
				TimeSpan.FromSeconds(10),
				TestContext.Current.CancellationToken));
	}

	// The command-level helpers hand a plain (non-Db) reader
	// to ForEach and IterateWhile.
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task IterateReaderAsync_OverAPlainReader_CancelThrows(
		bool whileForm)
	{
		using var cts = new CancellationTokenSource();
		var command = new PlainAsyncReaderCommand(static () =>
			FakeReader.Create(["Id", "Name", "Note"], Rows(10)));
		int handled = 0;
		void Handle(IDataRecord _)
		{
			if (++handled == 3) cts.Cancel();
		}

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () =>
			{
				if (whileForm)
				{
					await command.IterateReaderWhileAsync(
						r =>
						{
							Handle(r);
							return true;
						},
						cancellationToken: cts.Token);
				}
				else
				{
					await command.IterateReaderAsync(
						Handle,
						cancellationToken: cts.Token);
				}
			});

		Assert.Equal(3, handled);
	}

	// A stream over a plain IDataReader forwards the token,
	// so a cancel between rows throws instead of a partial result.
	[Fact]
	public static async Task AMappedStream_OverAPlainReader_CancelThrows()
	{
		using var cts = new CancellationTokenSource();
		int mapped = 0;
		var command = new PlainReaderCommand(static () =>
			FakeReader.Create(["Id", "Name", "Note"], Rows(10)));
		MappedQuery<int> query = command.Map(r =>
		{
			if (++mapped == 3) cts.Cancel();
			return (int)r[0];
		});
		int seen = 0;

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await query.ReadAsync(async (rows, ct) =>
			{
				await foreach (int _ in rows.WithCancellation(ct))
					seen++;
			}, cts.Token));

		Assert.Equal(3, mapped);
		Assert.Equal(3, seen);
	}

	// ---- AsEnumerable / AsAsyncEnumerable ----

	[Fact]
	public static void AsEnumerable_WithAPool_RentsEachRowFromIt_SizedForTheOrdinalsAsked()
	{
		var pool = new SizedPool();
		TypedReader reader = Reader([1, "a", null], [2, "b", "n2"]);

		object[][] rows = reader.AsEnumerable(pool, 2, 0).ToArray();

		Assert.Equal([2, 2], pool.Requests);
		Assert.Equal([DBNull.Value, 1], rows[0]); // the NULL is kept, in the order asked
		Assert.Equal(["n2", 2], rows[1]);
	}

	[Theory]
	[InlineData("pool")]
	[InlineData("ordinals and pool")]
	[InlineData("first ordinal and others")]
	public static async Task AsAsyncEnumerable_Variants_YieldTheRowsAsked_AndOnlyThePooledOnesUseThePool(string variant)
	{
		var pool = new SizedPool();
		TypedReader reader = Reader([1, "a", "n1"], [2, "b", "n2"]);
		CancellationToken token = TestContext.Current.CancellationToken;
		IEnumerable<int> ordinals = [2, 0];

		IAsyncEnumerable<object[]> sequence = variant switch
		{
			"pool" => reader.AsAsyncEnumerable(pool, token),
			"ordinals" => reader.AsAsyncEnumerable(ordinals, token),
			"ordinals and pool" => reader.AsAsyncEnumerable(ordinals, pool, token),
			_ => reader.AsAsyncEnumerable(token, 2, 0),
		};
		List<object[]> rows = await Collect(sequence);

		bool allColumns = variant == "pool";
		object[] first = allColumns ? [1, "a", "n1"] : ["n1", 1];
		object[] second = allColumns ? [2, "b", "n2"] : ["n2", 2];
		Assert.Equal(first, rows[0]);
		Assert.Equal(second, rows[1]);
		Assert.Equal(variant is "pool" or "ordinals and pool" ? 2 : 0, pool.Requests.Count); // one rented array per row, and only when a pool was given
	}

	[Theory]
	[InlineData(false, 3)]
	[InlineData(true, 3)]
	public static async Task AsAsyncEnumerable_Cancelled_EndsQuietlyWithTheRowsReadSoFar(bool usePool, int cancelAtRead)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel();
		});

		List<object[]> rows = await Collect(usePool
			? reader.AsAsyncEnumerable(new SizedPool(), cts.Token)
			: reader.AsAsyncEnumerable(cts.Token));

		Assert.InRange(rows.Count, cancelAtRead, 9);
	}

	// ---- IterateWhile ----

	[Theory]
	[InlineData("sync")]
	[InlineData("async ValueTask, ReadAsync")]
	[InlineData("async ValueTask, Read")]
	public static async Task IterateWhile_StopsReadingAtTheFirstFalse(string variant)
	{
		using var cts = new CancellationTokenSource(); // cancellable, never cancelled
		TrackedReader reader = TrackedRows(10);
		int seen = 0;
		bool Continue(IDataRecord _) => ++seen < 3;
		ValueTask<bool> ContinueAsync(IDataRecord _) => new(++seen < 3);

		switch (variant)
		{
			case "sync": reader.IterateWhile(Continue, cts.Token); break;
			case "async, Read": await reader.IterateWhileAsync(Continue, false, cts.Token); break;
			case "async ValueTask, ReadAsync": await reader.IterateWhileAsync(ContinueAsync, true, cts.Token); break;
			default: await reader.IterateWhileAsync(ContinueAsync, false, cts.Token); break;
		}

		Assert.Equal(3, seen);
		Assert.Equal(3, reader.Reads); // the row that stopped it was the last one read
	}

	[Fact]
	public static void IterateWhile_CancelMidRow_ThrowsBeforeNextRead()
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 3) cts.Cancel();
		});
		int seen = 0;

		Assert.Throws<OperationCanceledException>(() =>
			reader.IterateWhile(_ => ++seen > 0, cts.Token));

		Assert.Equal(3, seen);
		Assert.Equal(3, reader.Reads);
	}

	[Theory]
	[InlineData(10)] // during the last row's predicate: throws
	[InlineData(11)] // as the read that ends the result is made: returns
	public static void IterateWhile_CancelAtTheEnd_Cases(
		int cancelAtRead)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel();
		});
		int seen = 0;

		if (cancelAtRead == 10)
		{
			Assert.Throws<OperationCanceledException>(() =>
				reader.IterateWhile(_ => ++seen > 0, cts.Token));
		}
		else
		{
			reader.IterateWhile(_ => ++seen > 0, cts.Token);
		}

		Assert.Equal(10, seen);
	}

	// ---- a result set into a DataTable ----

	static DataTable Table(string name, params object?[][] rows)
	{
		var table = new DataTable(name);
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		foreach (object?[] row in rows)
			table.Rows.Add(row.Select(v => v ?? DBNull.Value).ToArray());

		return table;
	}

	[Fact]
	public static void ToDataTable_LoadsEveryRow_KeepingNulls_AndTheColumnTypes()
	{
		var set = new DataSet();
		set.Tables.Add(Table("only", [1, "x"], [2, null]));
		using DataTableReader reader = set.CreateDataReader();

		var table = reader.ToDataTable();

		Assert.Equal(2, table.Rows.Count);
		Assert.Equal(typeof(int), table.Columns["Id"]!.DataType);
		Assert.Equal("x", table.Rows[0]["Name"]);
		Assert.Equal(DBNull.Value, table.Rows[1]["Name"]);
	}

	// ---- FirstOrdinalResults ----

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static void FirstOrdinalResults_ReadsEveryRowBeforeItReturns_AndTurnsNullsIntoNull(bool providerReader)
	{
		object?[][] rows = [[1], [null], [3]];
		IDataReader reader = providerReader
			? TypedReader.Create([("Id", typeof(int))], rows)
			: FakeReader.Create(["Id"], rows);

		IEnumerable<object?> values = reader.FirstOrdinalResults();

		if (reader is TypedReader typed)
			Assert.Equal(3, typed.RowsRead); // before it is enumerated, so the connection can be released
		Assert.Equal([1, null, 3], values.ToArray());
	}

	// ---- argument guards ----

	[Theory]
	[InlineData("Select transform", "transform")]
	[InlineData("IterateWhile predicate", "predicate")]
	public static void ANullArgument_IsRefusedAtTheCall_ByTheParametersName(string call, string parameter)
	{
		TypedReader reader = Reader(TwoRows);

		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
		{
			switch (call)
			{
				case "ForEach handler": reader.ForEach(null!, CancellationToken.None); break;
				case "Select transform": _ = reader.Select((Func<IDataRecord, int>)null!); break; // not deferred to the enumeration
				case "IterateWhile predicate": reader.IterateWhile(null!, CancellationToken.None); break;
				default: ((IDataReader)null!).ToDataTable(); break;
			}
		});

		Assert.Equal(parameter, ex.ParamName);
	}
}
