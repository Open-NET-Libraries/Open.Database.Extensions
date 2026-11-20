using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// The recycle bin: where a pooled buffer is cleared and returned when
// clearing applies (rows that hold references, UsePool or the switch),
// so the clear is off the read path.
//
// It is INTERNAL ONLY: nothing outside DeferredClearOwner (see
// MemoryOwnerExtensions) can feed it, and the only work it ever runs is
// ours and bounded (clear a slice of known length, then dispose an owner).
//
// Lazy: nothing exists until a release with clearing is posted. The
// channel and its single loop live in the nested static class Bin, whose
// static initializer the runtime runs on its first access. The default
// path (no clearing) never touches this class.
//
// How it works.
// - One non-generic Channel<Action>, bounded (Capacity), one reader. The
//   channel is never completed, so the loop below never ends by itself.
// - ONE always-on loop (an async method, no task per item), started once
//   by the static initializer: it waits for work, drains the available
//   batch with TryRead, runs each action in its own try/catch (an action
//   that throws, such as a pool whose Return throws, costs only that
//   action), and yields ONCE per batch so it does not hog a pool
//   thread. There is no idle exit: a parked await holds no thread.
//   Nothing else can throw inside it, so there is no outer guard.
// - The loop is started with the caller's ExecutionContext SUPPRESSED:
//   it lives for the process, so a captured context (an AsyncLocal, a
//   logging scope, an Activity) of whoever posted first would otherwise
//   stay alive and visible to every later action.
//
// What a post can say (the caller decides what to do):
// - Posted: the bin will run it.
// - Full: the bin cannot take it (saturated, or anything went wrong
//   posting). A bin that reaches Capacity means something is wrong
//   (clearing cannot keep up, or the loop is stalled), so the caller
//   DISCARDS the owner: it is not cleared and not returned, it is left
//   to the GC (for the shared pool's array-backed owners that costs one
//   future allocation). No consumer ever waits on the bin, and a dirty
//   array is never returned to a pool. Discards are counted (Discarded).
// Post never throws.
//
// What it cannot guard against: a caller's custom pool whose Return
// blocks forever stalls the single loop; up to Capacity dirty arrays
// then queue up, and every later one is discarded. A custom pool must
// tolerate an owner that is never disposed (it is, when the bin is full).
// There is no per-action timeout: synchronous work cannot be cancelled
// anyway. The static loop is also a process-lifetime root: in a host
// with collectible AssemblyLoadContexts it pins the context that
// started it until the process ends.
internal static class RecycleBin
{
	internal enum PostResult
	{
		Posted,
		Full,
	}

	// A test seam: when set, it answers every post (to simulate a full
	// bin, or to count posts). Never set outside tests.
	internal static Func<Action, PostResult>? _hook;

	static long _discarded;

	/// <summary>How many posts were refused (the owners discarded).</summary>
	internal static long Discarded
		=> Interlocked.Read(ref _discarded);

	internal static PostResult Post(Action action)
	{
		PostResult result;
		try
		{
			Func<Action, PostResult>? hook = Volatile.Read(ref _hook);
			result = hook is null ? Bin.Post(action) : hook(action);
		}
		catch (Exception)
		{
			// Whatever went wrong (even the static initializer of Bin):
			// the caller discards the owner.
			result = PostResult.Full;
		}

		if (result == PostResult.Full)
			Interlocked.Increment(ref _discarded);

		return result;
	}

	// Nested static class: its static initializer runs on first access,
	// that is when the first action is posted.
	static class Bin
	{
		// A count, not bytes: far above any healthy backlog. Reaching
		// it means something is wrong, and the rest is discarded.
		const int Capacity = 10_240;

		static readonly Channel<Action> Work
			= Channel.CreateBounded<Action>(
				new BoundedChannelOptions(Capacity)
				{
					SingleReader = true,
					SingleWriter = false,
					AllowSynchronousContinuations = false,
				});

		// Held so the one loop is never garbage; it never completes.
		[SuppressMessage("Roslynator", "RCS1213:Remove unused member declaration")]
		static readonly Task Loop = StartLoop();

		internal static PostResult Post(Action action)
			=> Work.Writer.TryWrite(action)
				? PostResult.Posted
				: PostResult.Full;

		[SuppressMessage("Roslynator", "RCS1229:Use async/await when necessary")]
		static Task StartLoop()
		{
			// Task.Run would capture the starter's ExecutionContext for the
			// life of the process: suppress it.
			using (ExecutionContext.SuppressFlow())
				return Task.Run(DrainAsync);
		}

		static async Task DrainAsync()
		{
			ChannelReader<Action> reader = Work.Reader;

			// The channel is never completed, so this never ends by
			// itself. There is no outer guard: nothing here can throw (each
			// action has its own catch), and if the loop ever did die,
			// posts would be accepted up to Capacity and then discarded,
			// and a dirty array still never reaches a pool.
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
			{
				while (reader.TryRead(out Action? action))
				{
					try { action(); }
					catch { }
				}

				// Once per batch, not per item.
				await Task.Yield();
			}
		}
	}
}
