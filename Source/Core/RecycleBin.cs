using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// The recycle bin: where a pooled buffer is cleared and returned when the
// caller asked for it (clearOnDispose / clearOnReturn: true), so the clear
// is off the read path.
//
// It is INTERNAL ONLY: nothing outside DeferredClearOwner (see
// MemoryOwnerExtensions) can feed it, and the only work it ever runs is
// ours and bounded (clear a slice of known
// length, then dispose an owner).
//
// Lazy: nothing exists until a release with clearing is posted. The
// channel, its single loop and the liveness state live in the nested
// static class Bin, whose static initializer the runtime runs on its
// first access. The default path (no clearing) never touches this class.
//
// How it works.
// - One non-generic Channel<Action>, bounded (Capacity), one reader.
// - ONE always-on loop (an async method, no task per item): it waits for
//   work, drains the available batch with TryRead, runs each action in
//   its own try/catch (an action that throws, such as a pool whose Return
//   throws, costs only that action), and yields ONCE per batch so it
//   does not hog a pool thread. There is no idle exit: a parked await
//   holds no thread.
// - Fault tolerance: an outer guard catches everything and restarts the
//   loop after a short delay; Post checks the loop is alive and restarts
//   it if it ended; nothing escapes, so there is no
//   UnobservedTaskException, and Post never throws.
//
// What a post can say (the caller decides what to do):
// - Posted: the bin will run it.
// - Full: the bin is saturated; the caller clears INLINE, so no consumer
//   ever waits on the bin (the app degrades to the no-bin behaviour).
// - Closed: the bin cannot take work; the caller DROPS the array to the
//   garbage collector. A dirty array is never returned to a pool.
// Returning to a pool is an optimization, never a requirement: dropping
// to the GC is always safe.
//
// The one case the bin cannot guard against: a caller's custom pool whose
// Return blocks forever stalls the single loop (the queue then fills and
// posters clear inline). There is no per-action timeout: synchronous work
// cannot be cancelled anyway.
internal static class RecycleBin
{
	internal enum PostResult
	{
		Posted,
		Full,
		Closed,
	}

	// A test seam: when set, it answers every post (to simulate a full or
	// a closed bin). Never set outside tests.
	internal static Func<Action, PostResult>? Hook;

	static int _posts;

	// How many posts were ever made (to show that the default path makes
	// none).
	internal static int Posts
		=> Volatile.Read(ref _posts);

	internal static PostResult Post(Action action)
	{
		Interlocked.Increment(ref _posts);
		Func<Action, PostResult>? hook = Volatile.Read(ref Hook);
		return hook is null ? Bin.Post(action) : hook(action);
	}

	// Nested static class: its static initializer runs on first access,
	// that is when the first action is posted.
	static class Bin
	{
		const int Capacity = 1024;

		static readonly Channel<Action> _channel
			= Channel.CreateBounded<Action>(
				new BoundedChannelOptions(Capacity)
				{
					SingleReader = true,
					SingleWriter = false,
					FullMode = BoundedChannelFullMode.Wait,
					AllowSynchronousContinuations = false,
				});

		// 1 while a loop is alive.
		static int _running;

		internal static PostResult Post(Action action)
		{
			try
			{
				if (!_channel.Writer.TryWrite(action))
				{
					return _channel.Reader.Completion.IsCompleted
						? PostResult.Closed
						: PostResult.Full;
				}

				EnsureLoop();
				return PostResult.Posted;
			}
			catch (Exception)
			{
				// Never throws out: the caller drops the array.
				return PostResult.Closed;
			}
		}

		// The liveness check: restart the loop if it ended.
		static void EnsureLoop()
		{
			if (Volatile.Read(ref _running) == 1
				|| Interlocked.CompareExchange(ref _running, 1, 0) != 0)
			{
				return;
			}

			_ = Task.Run(LoopAsync);
		}

		static async Task LoopAsync()
		{
			try
			{
				while (true)
				{
					try
					{
						await DrainAsync().ConfigureAwait(false);

						// The channel was completed: nothing more
						// will come.
						return;
					}
					catch (Exception)
					{
						// The outer guard: whatever it was, start
						// again after a short delay.
						await Task.Delay(100).ConfigureAwait(false);
					}
				}
			}
			finally
			{
				Volatile.Write(ref _running, 0);

				// A post that arrived after the loop's last look finds
				// _running 0 and restarts it; one that arrived before
				// is still in the queue: look once more.
				if (_channel.Reader.TryPeek(out _)) EnsureLoop();
			}
		}

		static async Task DrainAsync()
		{
			ChannelReader<Action> reader = _channel.Reader;
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
			{
				while (reader.TryRead(out Action? action))
				{
					try { action(); }
					catch (Exception) { }
				}

				// Once per batch, not per item.
				await Task.Yield();
			}
		}
	}
}
