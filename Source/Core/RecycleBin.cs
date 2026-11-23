using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// Threshold: a chunk of 128 KB or more of rows (value rows included) is cleared here (spike 4e); a smaller one is cleared where it is released.
// The only work the bin ever runs is ours and bounded (clear the rows of one array, then give it back). The default path with no clearing, and small chunks, never touch it.
// How it works:
// - One Channel of struct work items (a runner, an array, the number of rows, the configuration), bounded by the capacity, with one reader.
//   A post is a struct written into the queue: no delegate, no closure, no allocation once the queue has grown.
//   The runner is one cached object per row type (ChunkRunner<T>); it does the same ChunkConfig<T>.ClearAndReturn that the immediate release does.
// - One always-on loop (an async method, no task per item), started once by the first post: it waits for work, drains the available batch with TryRead,
//   runs each item in its own try/catch (an item that throws, such as a pool whose Return throws, costs only that item), and yields once per batch
//   so it does not hog a pool thread. There is no idle exit: a parked await holds no thread.
// - The loop is started with the caller's ExecutionContext suppressed: it lives for the process (the default bin), so a captured context
//   (an AsyncLocal, a logging scope, an Activity) of whoever posted first would otherwise stay alive and visible to every later item.
// What it cannot guard against: a caller's custom pool whose Return blocks forever stalls the single loop; up to the capacity of dirty arrays
// then queue up, and every later one is cleared and returned by the caller. There is no per-item timeout: synchronous work cannot be cancelled anyway.
// The default bin's loop is also a process-lifetime root: in a host with collectible AssemblyLoadContexts it pins the context that started it until the process ends.
// A refused post is counted (Overflowed): a bin that overflows often means clearing cannot keep up with the posts, which is what the inline fallback is for.
/// <summary>
/// The recycle bin: where a large chunk is cleared and its array returned
/// when clearing applies, so the clear and the return are off the read path.
/// </summary>
/// <remarks>
/// <para>
/// Rows that hold references are always cleared; other rows are cleared when <c>UsePool</c> or
/// the app-wide switch asks for it.
/// A large chunk is cleared here; a smaller one is cleared where it is released.
/// The bin is internal only: nothing outside <see cref="ArrayLease{T}"/> can feed it.
/// It is lazy: no loop exists until the first post.
/// </para>
/// <para>What a post can say (the caller decides what to do):</para>
/// <list type="bullet">
/// <item>
/// <description>
/// <see cref="PostResult.Posted"/>: the bin will run the work.
/// </description>
/// </item>
/// <item>
/// <description>
/// <see cref="PostResult.Full"/>: the bin cannot take it.
/// The caller clears and returns the array itself, right away
/// (<see cref="ArrayLease{T}.Release"/>), so a dirty array never reaches a pool.
/// No consumer ever waits on the bin, and <see cref="Post"/> never throws.
/// </description>
/// </item>
/// </list>
/// </remarks>
/// <param name="capacity">The most work items the queue holds.</param>
internal class RecycleBin(int capacity = RecycleBin.DefaultCapacity)
{
	// A count, not bytes: far above any healthy backlog. Reaching it means something is wrong, and the rest is cleared by the callers.
	/// <summary>The most work items the default bin queues.</summary>
	public const int DefaultCapacity = 10_240;

	/// <summary>What a post says to the caller.</summary>
	public enum PostResult
	{
		/// <summary>The bin will run the work.</summary>
		Posted,
		/// <summary>The bin cannot take the work: the caller clears the array itself.</summary>
		Full,
	}

	/// <summary>What the loop runs: one cached object per row type.</summary>
	public interface IRunner
	{
		/// <summary>Clears the first <paramref name="used"/> rows and gives the array back.</summary>
		/// <param name="array">The array to clear and return.</param>
		/// <include file="Internals.docs.xml" path="docs/members[@name='used']/*" />
		/// <param name="config">The configuration the array came from.</param>
		void Run(Array array, int used, object config);
	}

	/// <summary>One array to clear and give back.</summary>
	/// <param name="runner">Does the clear and the return.</param>
	/// <param name="array">The array to clear and return.</param>
	/// <param name="used">How many slots, from the start, hold rows.</param>
	/// <param name="config">The configuration the array came from.</param>
	public readonly struct Work(
		IRunner runner,
		Array array,
		int used,
		object config)
	{
		/// <summary>Gets the runner that does the clear and the return.</summary>
		public IRunner Runner { get; } = runner;

		/// <summary>Gets the array to clear and return.</summary>
		public Array Array { get; } = array;

		/// <summary>Gets how many slots, from the start, hold rows.</summary>
		public int Used { get; } = used;

		/// <summary>Gets the configuration the array came from.</summary>
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

	/// <summary>Gets the number of posts that were refused (cleared inline instead).</summary>
	public long Overflowed
		=> Interlocked.Read(ref _overflowed);

	/// <summary>
	/// Hands the work to the loop. Never throws: a refused post is counted
	/// and the caller clears the array itself.
	/// </summary>
	/// <param name="work">The array to clear and give back.</param>
	/// <returns>
	/// <see cref="PostResult.Posted"/> if the bin will run it; otherwise,
	/// <see cref="PostResult.Full"/>.
	/// </returns>
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
	/// <param name="work">The array to clear and give back.</param>
	/// <returns>
	/// <see cref="PostResult.Posted"/> if the work was queued; otherwise,
	/// <see cref="PostResult.Full"/>.
	/// </returns>
	public virtual PostResult TryPost(Work work)
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

	/// <summary>Starts the loop, without the caller's execution context.</summary>
	/// <returns>The task of the loop, which never ends by itself.</returns>
	[SuppressMessage("Roslynator", "RCS1229:Use async/await when necessary")]
	Task StartLoop()
	{
		// Task.Run would capture the starter's ExecutionContext for the
		// life of the process: suppress it.
		using (ExecutionContext.SuppressFlow())
			return Task.Run(DrainAsync);
	}

	/// <summary>Runs every posted item, one after the other, for ever.</summary>
	/// <remarks>The channel is never completed, so this never ends by itself.</remarks>
	/// <returns>A task that represents the asynchronous operation.</returns>
	async Task DrainAsync()
	{
		// There is no outer guard: nothing here can throw (each item has its own catch), and if the loop ever did die,
		// posts would be accepted up to the capacity and then refused (cleared inline), and a dirty array still never reaches a pool.
		ChannelReader<Work> reader = _work.Reader;

		// The channel is never completed: see the remarks.
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