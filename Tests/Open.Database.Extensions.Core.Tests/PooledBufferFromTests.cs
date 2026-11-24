using Microsoft.Data.Sqlite;
using System.Buffers;

namespace Open.Database.Extensions.Tests;

// PooledBuffer<T>.From builds a finished buffer from rows a caller already has, so code that returns a buffer can be
// faked without a database; PooledArray<T>.Empty is the one shared view of no rows, and a read that finds none completes
// with it. What it pins: the rows and their order, the copy (the span is not kept), the array going back to the pool on
// dispose (cleared when asked), nothing rented for no rows, and the shared empty view's rules.
[ExcludeFromCodeCoverage]
public static class PooledBufferFromTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A pool that makes its arrays and does what a pool does with the clear flag, recording what happened.
	sealed class RecordingPool<T> : ArrayPool<T>
	{
		public List<int> Rented { get; } = [];

		public List<T[]> Returned { get; } = [];

		public override T[] Rent(int minimumLength)
		{
			Rented.Add(minimumLength);
			return new T[minimumLength];
		}

		public override void Return(T[] array, bool clearArray = false)
		{
			if (clearArray) Array.Clear(array);
			Returned.Add(array);
		}
	}

	[Fact]
	public static async Task From_GivesTheRowsInOrder_AtOnce()
	{
		await using var buffer = PooledBuffer<int>.From([5, 6, 7, 8, 9]);

		Task<PooledArray<int>> items = buffer.Items;
		Assert.True(items.IsCompletedSuccessfully);
		PooledArray<int> rows = await items;

		Assert.Equal(5, rows.Count);
		Assert.Equal([5, 6, 7, 8, 9], rows.ToArray());
		Assert.Equal([5, 6, 7, 8, 9], rows.ToList());
		Assert.Equal(7, rows[2]);
		Assert.Equal([5, 6, 7, 8, 9], [.. rows]);
	}

	[Theory]
	[InlineData(1)]
	[InlineData(2)]
	[InlineData(3)]
	[InlineData(15)]
	[InlineData(16)]
	[InlineData(17)]
	[InlineData(1000)]
	public static async Task From_AnyCount_GivesEveryRow(int count)
	{
		int[] source = [.. Enumerable.Range(100, count)];
		await using var buffer = PooledBuffer<int>.From(source);
		PooledArray<int> rows = await buffer.Items;

		Assert.Equal(count, rows.Count);
		Assert.Equal(source, rows.ToArray());
		Assert.Equal(source[^1], rows[count - 1]);
		Assert.Throws<ArgumentOutOfRangeException>(() => rows[count]);
	}

	[Fact]
	public static async Task From_CopiesTheRows_TheSpanIsNotKept()
	{
		int[] source = [1, 2, 3];
		await using var buffer = PooledBuffer<int>.From(source);
		source[0] = 99;

		int[] copied = (await buffer.Items).ToArray();

		Assert.Equal([1, 2, 3], copied);
	}

	[Fact]
	public static async Task From_RentsOneArray_AndDisposeReturnsIt()
	{
		RecordingPool<int> pool = new();
		var buffer = PooledBuffer<int>.From([1, 2, 3, 4, 5], pool, clearOnReturn: false);
		_ = await buffer.Items;

		// Five rows are one first chunk of eight.
		Assert.Equal([8], pool.Rented);
		Assert.Empty(pool.Returned);

		buffer.Dispose();
		buffer.Dispose();

		Assert.Single(pool.Returned);
	}

	[Fact]
	public static async Task From_WithClearOnReturn_ClearsTheRowsWhenTheArrayGoesBack()
	{
		RecordingPool<string> pool = new();
		var buffer = PooledBuffer<string>.From(["a", "b", "c"], pool, clearOnReturn: true);
		_ = await buffer.Items;

		buffer.Dispose();

		string?[] returned = Assert.Single(pool.Returned);
		Assert.All(returned, Assert.Null);
	}

	[Fact]
	public static async Task From_ADisposedBuffer_RefusesItsView()
	{
		var buffer = PooledBuffer<int>.From([1, 2, 3]);
		PooledArray<int> rows = await buffer.Items;

		buffer.Dispose();

		Assert.Throws<ObjectDisposedException>(() => rows.Count);
#pragma warning disable RCS1021 // An expression body would bind to a Func overload of Throws, which xUnit refuses.
		Assert.Throws<ObjectDisposedException>(() => { _ = buffer.Items; });
#pragma warning restore RCS1021
	}

	[Fact]
	public static void From_ANullPool_IsRefused()
		=> Assert.Equal(
			"pool",
			Assert.Throws<ArgumentNullException>(() => PooledBuffer<int>.From([1], null, false)).ParamName);

	[Fact]
	public static async Task From_NoRows_RentsNothing_AndGivesTheSharedEmptyView()
	{
		RecordingPool<int> pool = new();
		await using var buffer = PooledBuffer<int>.From([], pool, clearOnReturn: true);

		PooledArray<int> rows = await buffer.Items;

		Assert.Same(PooledArray<int>.Empty, rows);
		Assert.Empty(pool.Rented);
		Assert.Empty(pool.Returned);
	}

	[Fact]
	public static void Empty_IsTheSameInstance_HoldsNothing_AndNeedsNoDispose()
	{
		PooledArray<string> empty = PooledArray<string>.Empty;

		Assert.Same(empty, PooledArray<string>.Empty);
		Assert.Empty(empty);
		Assert.Empty(empty.ToArray());
		Assert.Empty(empty.ToList());
		Assert.Throws<ArgumentOutOfRangeException>(() => empty[0]);

		// A dispose does nothing: the view is still the shared one and still works.
		empty.Dispose();
		empty.Dispose();
		Assert.Empty(empty);
		Assert.Same(empty, PooledArray<string>.Empty);
	}

	[Fact]
	public static async Task Empty_StillWorks_AfterTheBufferThatGaveItIsDisposed()
	{
		var buffer = PooledBuffer<int>.From([]);
		PooledArray<int> rows = await buffer.Items;

		buffer.Dispose();

		Assert.Empty(rows);
		Assert.Empty(rows.ToArray());
#pragma warning disable RCS1021 // An expression body would bind to a Func overload of Throws, which xUnit refuses.
		Assert.Throws<ObjectDisposedException>(() => { _ = buffer.Items; });
#pragma warning restore RCS1021
	}

	[Fact]
	public static async Task AReadThatFindsNoRows_CompletesWithTheSharedEmptyView_AndRentsNothing()
	{
		await using SqliteConnection c = new("Data Source=:memory:");
		await c.OpenAsync(Ct);
		RecordingPool<int> pool = new();

		await using var buffer = DbQuery.Text("select 1 where 0")
			.Map<int>()
			.UsePool(pool, clearOnReturn: true)
			.Using(c)
			.ToPooledBuffer(Ct);

		PooledArray<int> rows = await buffer.Items;

		Assert.Same(PooledArray<int>.Empty, rows);
		Assert.Empty(pool.Rented);
	}

	// The v11 rule: a read of no rows completes Items with the shared empty view, so it is not cut off by a dispose.
	[Fact]
	public static async Task AZeroRowRead_AfterTheBufferIsDisposed_StillGivesTheEmptyView_NotAThrow()
	{
		await using SqliteConnection c = new("Data Source=:memory:");
		await c.OpenAsync(Ct);

		var buffer = DbQuery.Text("select 1 where 0").Map<int>().Using(c).ToPooledBuffer(Ct);
		PooledArray<int> rows = await buffer.Items;

		await buffer.DisposeAsync();

		Assert.Same(PooledArray<int>.Empty, rows);
		Assert.Empty(rows);
		Assert.Empty(rows.ToArray());
	}

	[Fact]
	public static async Task AReadThatFindsRows_StillCompletesWithAnOrdinaryView()
	{
		await using SqliteConnection c = new("Data Source=:memory:");
		await c.OpenAsync(Ct);

		await using var buffer = DbQuery.Text("select 1 union all select 2")
			.Map<int>()
			.Using(c)
			.ToPooledBuffer(Ct);
		PooledArray<int> rows = await buffer.Items;

		Assert.NotSame(PooledArray<int>.Empty, rows);
		Assert.Equal([1, 2], rows.ToArray());
	}
}
