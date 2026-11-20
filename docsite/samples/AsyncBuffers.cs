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
			.ToAsyncBuffer(cancellationToken);
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

	#region buffers_finish_query
	// A LINQ query over the view is lazy: it runs when it is
	// enumerated. Finish it before the buffer is disposed.
	public static async Task<List<string>> FinishQueryAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		// Disposing the view disposes the buffer it came from.
		using PooledArray<Post> posts
			= await LatestPosts(connectionFactory, cancellationToken).Items;

		// ToList() runs the query now, while the view is alive.
		return posts
			.Select(static post => $"{post.Id}: {post.Title}")
			.ToList();
	}
	#endregion

	#region buffers_lazy_trap
	// The mistake: the query has not run when the buffer is
	// disposed, so it runs on arrays that are already back in the pool.
	public static async Task<bool> LazyAfterDisposeAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		IEnumerable<int> ids;
		using (PooledArray<Post> posts
			= await LatestPosts(connectionFactory, cancellationToken).Items)
		{
			ids = posts.Select(static post => post.Id);
		}

		try
		{
			_ = ids.ToList();
			return false;
		}
		catch (ObjectDisposedException)
		{
			return true;
		}
	}
	#endregion

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
			.ToAsyncBuffer(cancellationToken);

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
			.ToAsyncBufferStream(cancellationToken);

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
			.ToBufferedAsyncStream(cancellationToken);

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
	// The SQL filters; the C# shapes. No async, await or using:
	// the read starts when the caller enumerates the stream.
	public static IAsyncEnumerable<string> PostLabelsAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		int author,
		CancellationToken cancellationToken)
		=> connectionFactory
			.Command("select * from posts where author = @author order by id")
			.AddParam(author)
			.Map<Post>()
			.ToBufferedAsyncStream(cancellationToken)
			.Select(static post => $"{post.Id}: {post.Title}");
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
		// The shared pool is the default.
		await using PooledBuffer<Post> shared = Query(connectionFactory)
			.ToAsyncBuffer(cancellationToken);

		// Your own pool, chosen after Map. Both arguments are required.
		await using PooledBuffer<Post> own = Query(connectionFactory)
			.UsePool(pool, clear: false)
			.ToAsyncBuffer(cancellationToken);

		// The same, and the rows are cleared as the arrays go back.
		await using PooledBuffer<Post> scrubbed = Query(connectionFactory)
			.UsePool(pool, clear: true)
			.ToAsyncBuffer(cancellationToken);

		return
		[
			(await shared.Items).Count,
			(await own.Items).Count,
			(await scrubbed.Items).Count,
		];
	}
	#endregion

	#region buffers_clearing
	public static async Task ClearingAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		MemoryPool<int> pool,
		bool clear,
		CancellationToken cancellationToken)
	{
		// A row with no references (an int) is cleared only when asked:
		// the pool is yours, the clear is stated by name.
		await using PooledBuffer<int> buffer = connectionFactory
			.Command("select id from posts order by id")
			.Map<int>()
			.UsePool(pool, clear: clear)
			.ToAsyncBuffer(cancellationToken);

		PooledArray<int> ids = await buffer.Items;

		// ... use the rows. The clear runs after the dispose,
		// on a background loop, not on this path.
		_ = ids.Count;
	}
	#endregion

	#region buffers_clearing_shared
	public static async Task<int> ClearingSharedAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		// The shared pool, and the rows are scrubbed as they go back.
		await using PooledBuffer<int> buffer = connectionFactory
			.Command("select id from posts order by id")
			.Map<int>()
			.UsePool(MemoryPool<int>.Shared, clear: true)
			.ToAsyncBuffer(cancellationToken);

		return (await buffer.Items).Count;
	}
	#endregion

	// Not shown in the article: rows that hold references (a Post) are
	// cleared whatever the clear argument says.
	public static async Task ReferenceRowsAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		MemoryPool<Post> pool,
		CancellationToken cancellationToken)
	{
		await using PooledBuffer<Post> buffer = Query(connectionFactory)
			.UsePool(pool, clear: false)
			.ToAsyncBuffer(cancellationToken);
		_ = (await buffer.Items).Count;
	}

	// Not shown in the article: a null pool is refused at the call.
	public static bool NullPoolThrows(
		IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		try
		{
			_ = Query(connectionFactory)
				.UsePool(null!, clear: false);
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
			.ToAsyncBuffer(cts.Token);

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
			.UsePool(pool, clear: false)
			.ToAsyncBuffer(cancellationToken);

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
				.ToBufferedAsyncStream(cts.Token))
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
