using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The examples of the "Async buffers and streams" article. Every one is
// async, and Program.cs checks what the article says about it.
public static class AsyncBuffers
{
	#region buffers_data_layer
	// The data layer: it starts the read and returns at once.
	// Nothing here waits for the rows.
	public static PooledBuffer<Post> LatestPosts(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
		=> connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToAsyncBuffer(
				clearOnDispose: false,
				cancellationToken);
	#endregion

	#region buffers_controller
	// The layer above: it awaits the rows when it needs them,
	// serializes them, and disposes the buffer.
	public static async Task<string> RenderPostsAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		await using PooledBuffer<Post> buffer
			= LatestPosts(connectionFactory, cancellationToken);

		// The read began in the call above and runs on its own
		// producer, so other work can happen here.
		PooledArray<Post> posts = await buffer.Items;

		await using MemoryStream json = new();
		await JsonSerializer.SerializeAsync<IReadOnlyList<Post>>(
			json,
			posts,
			cancellationToken: cancellationToken);

		return Encoding.UTF8.GetString(json.ToArray());
	}
	#endregion

	#region buffers_items_view
	public static async Task<ItemsFacts> ItemsViewAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		await using PooledBuffer<Post> buffer
			= LatestPosts(connectionFactory, cancellationToken);
		PooledArray<Post> posts = await buffer.Items;

		int count = posts.Count;
		Post third = posts[2];

		// foreach uses a struct enumerator: no allocation.
		int idSum = 0;
		foreach (Post post in posts)
			idSum += post.Id;

		// LINQ knows the count, so this copies array by array.
		Post[] copy = posts.ToArray();

		return new(count, third.Id, idSum, copy.Length);
	}
	#endregion

	public readonly record struct ItemsFacts(
		int Count,
		int ThirdId,
		int IdSum,
		int CopyLength);

	#region buffers_skip_take
	public static async Task<int[]> SkipTakeAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		// The same terminals are on every typed view.
		await using PooledBuffer<Post> buffer = connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.Skip(1)
			.Take(2)
			.ToAsyncBuffer(
				clearOnDispose: false,
				cancellationToken);

		PooledArray<Post> posts = await buffer.Items;
		return [.. posts.Select(static p => p.Id)];
	}
	#endregion

	#region buffers_chunk_stream
	public static async Task<List<int>> ChunkStreamAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		List<int> ids = [];

		IAsyncEnumerable<ReadOnlyMemory<Post>> chunks = connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToAsyncBufferStream(
				clearOnReturn: false,
				cancellationToken);

		// Each chunk is valid until the next step of the loop:
		// copy what you keep.
		await foreach (ReadOnlyMemory<Post> chunk in chunks)
		{
			foreach (Post post in chunk.Span)
				ids.Add(post.Id);
		}

		return ids;
	}
	#endregion

	#region buffers_element_stream
	public static async Task<(List<int> Ids, bool SecondThrew)>
		ElementStreamAsync(
			IDbConnectionFactory<SqliteConnection> connectionFactory,
			CancellationToken cancellationToken)
	{
		List<int> ids = [];

		IAsyncEnumerable<Post> posts = connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToBufferedAsyncStream(
				clearOnReturn: false,
				cancellationToken);

		await foreach (Post post in posts)
			ids.Add(post.Id);

		// The stream is single-use: this throws.
		bool secondThrew = false;
		try
		{
			await foreach (Post post in posts)
				ids.Add(post.Id);
		}
		catch (InvalidOperationException)
		{
			secondThrew = true;
		}

		return (ids, secondThrew);
	}
	#endregion

	#region buffers_linq_async
	public static async Task<List<string>> LinqAsyncAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
		=> await connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToBufferedAsyncStream(
				clearOnReturn: false,
				cancellationToken)
			.Where(static post => post.Author == 42)
			.Select(static post => post.Title)
			.ToListAsync(cancellationToken);
	#endregion

	#region buffers_small_result
	public static async Task<Post[]> SmallResultAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
		=> await connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
	#endregion

	#region buffers_overloads
	static MappedQuery<Post> Query(
		IDbConnectionFactory<SqliteConnection> connectionFactory)
		=> connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>();

	public static async Task<int[]> OverloadsAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		MemoryPool<Post> pool,
		CancellationToken cancellationToken)
	{
		// The shared pool: the flag is required.
		await using PooledBuffer<Post> shared = Query(connectionFactory)
			.ToAsyncBuffer(
				clearOnDispose: false,
				cancellationToken);

		// Your own pool: nothing is cleared.
		await using PooledBuffer<Post> own = Query(connectionFactory)
			.ToAsyncBuffer(
				pool,
				cancellationToken);

		// Your own pool, and the flag.
		await using PooledBuffer<Post> both = Query(connectionFactory)
			.ToAsyncBuffer(
				pool,
				clearOnDispose: true,
				cancellationToken);

		return
		[
			(await shared.Items).Count,
			(await own.Items).Count,
			(await both.Items).Count,
		];
	}
	#endregion

	#region buffers_clearing
	public static async Task ClearingAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		MemoryPool<Post> pool,
		CancellationToken cancellationToken)
	{
		// The rows are sensitive: scrub each array as it goes back.
		await using PooledBuffer<Post> buffer = Query(connectionFactory)
			.ToAsyncBuffer(
				pool,
				clearOnDispose: true,
				cancellationToken);

		PooledArray<Post> posts = await buffer.Items;

		// ... use the rows. The clear runs after the dispose,
		// on a background loop, not on this path.
		_ = posts.Count;
	}
	#endregion

	// Not shown in the article: the same read with nothing cleared.
	public static async Task KeepsRowsAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		MemoryPool<Post> pool,
		CancellationToken cancellationToken)
	{
		await using PooledBuffer<Post> buffer = Query(connectionFactory)
			.ToAsyncBuffer(pool, cancellationToken);
		_ = (await buffer.Items).Count;
	}

	// Not shown in the article: a null pool is refused at the call.
	public static bool NullPoolThrows(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		try
		{
			_ = Query(connectionFactory)
				.ToAsyncBuffer(null!, cancellationToken);
			return false;
		}
		catch (ArgumentNullException)
		{
			return true;
		}
	}

	#region buffers_cancel
	public static async Task<bool> CallerCancelAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		// The caller's own cancel is delivered, not swallowed.
		// Here the token is cancelled before the call, so nothing
		// starts; a cancel during the read ends it the same way.
		using CancellationTokenSource cts = new();
		await cts.CancelAsync();

		await using PooledBuffer<Post> buffer = connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToAsyncBuffer(
				clearOnDispose: false,
				cts.Token);

		try
		{
			await buffer.Items;
			return false;
		}
		catch (OperationCanceledException)
		{
			return true;
		}
	}
	#endregion

	#region buffers_dispose_early
	public static async Task<bool> DisposeEarlyAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		MemoryPool<Post> pool,
		CancellationToken cancellationToken)
	{
		PooledBuffer<Post> buffer = Query(connectionFactory)
			.ToAsyncBuffer(
				pool,
				cancellationToken);

		// Cancels the read if it is still running, waits for the
		// producer to stop, and returns the arrays to the pool (with clearing on, it hands them to the cleanup queue).
		await buffer.DisposeAsync();

		// A disposed buffer has no Items.
		try
		{
			_ = buffer.Items;
			return false;
		}
		catch (ObjectDisposedException)
		{
			return true;
		}
	}
	#endregion

	// Not shown in the article: the stream's own cancel, delivered at
	// the first step.
	public static async Task<bool> StreamCancelAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		using CancellationTokenSource cts = new();
		await cts.CancelAsync();

		try
		{
			await foreach (Post _ in Query(connectionFactory)
				.ToBufferedAsyncStream(
					clearOnReturn: false,
					cts.Token))
			{
			}

			return false;
		}
		catch (OperationCanceledException)
		{
			return true;
		}
	}

	// Waits (for a bounded time) until the background clear has handed
	// every array back to the pool.
	public static async Task SettleAsync(CountingPool<Post> pool)
	{
		using CancellationTokenSource limit = new(TimeSpan.FromSeconds(10));
		while (pool.Returned < pool.Rented)
			await Task.Delay(5, limit.Token);
	}
}

#region buffers_counting_pool
// A pool of your own. This one counts what it rents and what comes
// back, and how many returned arrays still held rows.
public sealed class CountingPool<T> : MemoryPool<T>
{
	int _rented;
	int _returned;
	int _dirty;

	public int Rented => Volatile.Read(ref _rented);
	public int Returned => Volatile.Read(ref _returned);
	public int Dirty => Volatile.Read(ref _dirty);

	public override int MaxBufferSize => Array.MaxLength;

	public override IMemoryOwner<T> Rent(int minBufferSize = -1)
	{
		Interlocked.Increment(ref _rented);
		return new Owner(this, new T[Math.Max(minBufferSize, 1)]);
	}

	protected override void Dispose(bool disposing) { }

	void Return(T[] array)
	{
		if (Array.Exists(
			array,
			static item => !EqualityComparer<T>.Default.Equals(item, default!)))
		{
			Interlocked.Increment(ref _dirty);
		}

		Interlocked.Increment(ref _returned);
	}

	sealed class Owner(CountingPool<T> pool, T[] array) : IMemoryOwner<T>
	{
		T[]? _array = array;

		public Memory<T> Memory
			=> _array ?? throw new ObjectDisposedException(nameof(Owner));

		public void Dispose()
		{
			T[]? taken = Interlocked.Exchange(ref _array, null);
			if (taken is not null) pool.Return(taken);
		}
	}
}
#endregion
