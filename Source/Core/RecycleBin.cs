using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// The recycle bin: where a large chunk is cleared and its array returned
// when clearing applies (rows that hold references, UsePool or the
// switch), so the clear and the return are off the read path.
//
// It is INTERNAL ONLY: nothing outside ArrayLease can feed it, and the
// only work it ever runs is ours and bounded (clear the rows of one array,
// then give it back).
//
// Lazy: no loop exists until the first post (the default path, with no
// clearing, and small chunks never touch it).
//
// How it works.
// - One Channel of STRUCT work items (a runner, an array, the number of
//   rows, the configuration), bounded (Capacity), one reader. A post is a
//   struct written into the queue: no delegate, no closure, no allocation
//   once the queue has grown. The runner is one cached object per row type
//   (ChunkRunner<T>); it does the same ClearAndReturn the immediate
//   release does.
// - ONE always-on loop (an async method, no task per item), started once
//   by the first post: it waits for work, drains the available batch with
//   TryRead, runs each item in its own try/catch (an item that throws,
//   such as a pool whose Return throws, costs only that item), and yields
//   ONCE per batch so it does not hog a pool thread. There is no idle
//   exit: a parked await holds no thread.
// - The loop is started with the caller's ExecutionContext SUPPRESSED: it
//   lives for the process (the default bin), so a captured context (an
//   AsyncLocal, a logging scope, an Activity) of whoever posted first would
//   otherwise stay alive and visible to every later item.
//
// What a post can say (the caller decides what to do):
// - Posted: the bin will run it.
// - Full: the bin cannot take it (saturated, or anything went wrong
//   posting). The caller clears and returns the array itself, right
//   away (ArrayLease.Release): a dirty array is never returned to a pool,
//   and none is left to the garbage collector with its rows. No consumer
//   ever waits on the bin. Refusals are counted (Overflowed): a bin that
//   overflows often means clearing cannot keep up with the posts, which is
//   what the inline fallback is for. Post never throws.
//
// What it cannot guard against: a caller's custom pool whose Return blocks
// forever stalls the single loop; up to Capacity dirty arrays then queue
// up, and every later one is cleared and returned by the caller. There is no
// per-item timeout: synchronous work cannot be cancelled anyway. The
// default bin's loop is also a process-lifetime root: in a host with
// collectible AssemblyLoadContexts it pins the context that started it
// until the process ends.
internal class RecycleBin(int capacity = RecycleBin.DefaultCapacity)
{
	/// <summary>
	/// A count, not bytes: far above any healthy backlog. Reaching it means
	/// something is wrong, and the rest is cleared by the callers.
	/// </summary>
	public const int DefaultCapacity = 10_240;

	public enum PostResult
	{
		Posted,
		Full,
	}

	/// <summary>What the loop runs: one cached object per row type.</summary>
	public interface IRunner
	{
		void Run(Array array, int used, object config);
	}

	/// <summary>One array to clear and give back.</summary>
	public readonly struct Work(
		IRunner runner,
		Array array,
		int used,
		object config)
	{
		public IRunner Runner { get; } = runner;

		public Array Array { get; } = array;

		public int Used { get; } = used;

		public object Config { get; } = config;
	}

	/// <summary>The bin every default configuration posts to.</summary>
	public static readonly RecycleBin Default = new();

	readonly Channel<Work> _work = Channel.CreateBounded<Work>(
		new BoundedChannelOptions(capacity)
		{
			SingleReader = true,
			SingleWriter = false,
			AllowSynchronousContinuations = false,
		});

#if NET9_0_OR_GREATER
	readonly Lock _gate = new();
#else
	readonly object _gate = new();
#endif
	Task? _loop;
	long _overflowed;

	/// <summary>How many posts were refused (cleared inline instead).</summary>
	public long Overflowed
		=> Interlocked.Read(ref _overflowed);

	/// <summary>
	/// Hands the work to the loop. Never throws: a refused post is counted
	/// and the caller clears the array itself.
	/// </summary>
	public PostResult Post(Work work)
	{
		PostResult result;
		try
		{
			result = TryPost(work);
		}
		catch (Exception)
		{
			// Whatever went wrong: the caller clears the array itself.
			result = PostResult.Full;
		}

		if (result == PostResult.Full)
			Interlocked.Increment(ref _overflowed);

		return result;
	}

	/// <summary>The post itself (a test can replace it).</summary>
	protected virtual PostResult TryPost(Work work)
	{
		if (Volatile.Read(ref _loop) is null)
		{
			// One loop, started once: a lock, not a racing initializer, and no
			// delegate on the hot path.
			lock (_gate) _loop ??= StartLoop();
		}

		return _work.Writer.TryWrite(work)
			? PostResult.Posted
			: PostResult.Full;
	}

	[SuppressMessage("Roslynator", "RCS1229:Use async/await when necessary")]
	Task StartLoop()
	{
		// Task.Run would capture the starter's ExecutionContext for the
		// life of the process: suppress it.
		using (ExecutionContext.SuppressFlow())
			return Task.Run(DrainAsync);
	}

	async Task DrainAsync()
	{
		ChannelReader<Work> reader = _work.Reader;

		// The channel is never completed, so this never ends by itself.
		// There is no outer guard: nothing here can throw (each item has
		// its own catch), and if the loop ever did die, posts would be
		// accepted up to the capacity and then refused (cleared inline), and a dirty array
		// still never reaches a pool.
		while (await reader.WaitToReadAsync().ConfigureAwait(false))
		{
			while (reader.TryRead(out Work work))
			{
				try { work.Runner.Run(work.Array, work.Used, work.Config); }
				catch { }
			}

			// Once per batch, not per item.
			await Task.Yield();
		}
	}
}