using System.Buffers;
using System.Data.Common;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Edge cases of the reader extensions: cancellation before and in the middle of an iteration (quietly or by exception, as the caller asked),
// early stop, pooled rows, deferred execution, a result set loaded into a DataTable, NULLs, and the argument guards that are eager.
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

	[Theory]
	[InlineData(true, 0)]
	[InlineData(true, 3)]
	[InlineData(false, 3)]
	public static void ForEach_Cancelled_StopsBeforeTheEnd_AndThrowsOnlyWhenAsked(bool throwOnCancellation, int cancelAtRead)
	{
		using var cts = new CancellationTokenSource();
		if (cancelAtRead == 0) cts.Cancel(); // before the call
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel(); // during the call
		});
		int handled = 0;
		void Run() => reader.ForEach(_ => handled++, throwOnCancellation, cts.Token);

		if (throwOnCancellation)
			Assert.Throws<OperationCanceledException>(Run);
		else
			Run();

		if (cancelAtRead == 0)
		{
			Assert.Equal(0, handled);
			Assert.Equal(0, reader.Reads);
		}
		else
		{
			Assert.InRange(handled, cancelAtRead, 9); // what was read before the cancel was handled, the rest never was
		}
	}

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

	[Theory]
	[InlineData(true, 0)]
	[InlineData(false, 3)]
	public static void Select_Cancelled_YieldsWhatWasReadSoFar_ThenEndsOrThrows(bool throwOnCancellation, int cancelAtRead)
	{
		using var cts = new CancellationTokenSource();
		if (cancelAtRead == 0) cts.Cancel();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel();
		});
		var seen = new List<int>();
		void Run() => seen.AddRange(reader.Select(cts.Token, r => r.GetInt32(0), throwOnCancellation));

		if (throwOnCancellation)
			Assert.Throws<OperationCanceledException>(Run);
		else
			Run();

		if (cancelAtRead == 0)
		{
			Assert.Empty(seen);
			Assert.Equal(0, reader.Reads);
		}
		else
		{
			Assert.InRange(seen.Count, cancelAtRead, 9);
		}
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(true, true)]
	public static async Task SelectAsync_CancelledInTheMiddle_YieldsWhatWasReadSoFar_ThenEndsOrThrows(bool valueTaskTransform, bool throwOnCancellation)
	{
		using var cts = new CancellationTokenSource();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == 3) cts.Cancel();
		});
		var seen = new List<int>();
		async Task Run()
		{
			IAsyncEnumerable<int> ids = valueTaskTransform
				? ((IDataReader)reader).SelectAsync(r => new ValueTask<int>(r.GetInt32(0)), throwOnCancellation, cts.Token)
				: reader.SelectAsync(r => r.GetInt32(0), throwOnCancellation, cts.Token);

			await foreach (int id in ids)
				seen.Add(id);
		}

		if (throwOnCancellation)
			await Assert.ThrowsAnyAsync<OperationCanceledException>(Run);
		else
			await Run();

		Assert.InRange(seen.Count, 3, 9);
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
		if (cancelAtRead == 0) cts.Cancel();
		TrackedReader reader = TrackedRows(10, n =>
		{
			if (n == cancelAtRead) cts.Cancel();
		});

		List<object[]> rows = await Collect(usePool
			? reader.AsAsyncEnumerable(new SizedPool(), cts.Token)
			: reader.AsAsyncEnumerable(cts.Token));

		if (cancelAtRead == 0)
		{
			Assert.Empty(rows);
			Assert.Equal(0, reader.Reads);
		}
		else
		{
			Assert.InRange(rows.Count, cancelAtRead, 9);
		}
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

		DataTable table = reader.ToDataTable();

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
