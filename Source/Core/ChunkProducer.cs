using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

// The one producer behind the buffer terminals (ToAsyncBuffer,
// ToAsyncBufferStream and ToBufferedAsyncStream).
//
// What it does.
// It runs the query through the same internal row-sink path as
// ToArrayAsync and ToListAsync (MappedQueryCore.FillAsync into an
// IRowSink, via ChunkSink), so the connection, the read mode, Skip and
// Take and the read-through of later result sets are the ones every
// other terminal has, and there is no per-row enumerator. It copies each
// row into a buffer rented from the pool,
// and hands each filled buffer to its target (IChunkTarget). The
// target decides what happens to the chunks: the buffer keeps them
// until it is disposed, the stream queues them for its one consumer.
//
// Threads.
// Start runs RunAsync on a thread-pool thread, once. It is the only
// code that writes rows or rents buffers. Everything else (readers,
// dispose) runs on callers' threads and talks to it only through the
// target and through Cancel.
//
// Why each non-obvious construct is here.
// - Task.Factory.StartNew(..., TaskScheduler.Current).Unwrap(), not
//   Task.Run: the producer runs on the AMBIENT scheduler (a design
//   decision). RunAsync is async, so StartNew returns a Task<Task>,
//   which is unwrapped. DenyChildAttach makes a child task that the
//   read creates with AttachedToParent NOT attach to the producer's
//   task (its completion would otherwise wait for the child). The
//   producer's own task is detached because it is not created with
//   AttachedToParent.
//   Consequence: on a single-thread scheduler, the producer's first
//   step (everything up to the first real await, which for a
//   provider that completes synchronously can be the whole read)
//   needs that thread. A caller that blocks that thread waiting for
//   Items.Result can therefore deadlock; await it instead.
// - The linked token source: the query runs on a token that fires when
//   the caller's token fires OR when the owner disposes, so a dispose
//   cancels the database read itself and not only this loop.
// - Both kinds of cancel end the same way here, but are reported
//   differently: a cancel the owner asked for (the target is
//   disposed) is swallowed; a cancel from the caller's token is
//   reported to the target as an OperationCanceledException carrying
//   the CALLER's token; any other failure is reported as it is.
// - The rows read before a failure or a cancel are delivered first:
//   the buffer that is still being filled is handed over (Seal) before
//   Finish.
//
// Invariants.
// - With clearing on (the terminal's clearOnReturn / clearOnDispose),
//   Take wraps each exactly-sized owner so that disposing it clears
//   its rows through the recycle bin (see MemoryOwnerExtensions). It
//   wraps before the Publish attempt, so a chunk the target refuses,
//   and the partial chunk sealed on a cancel or a fault, are cleared
//   too (when the bin takes them; a full bin discards them): they hold
//   rows. Nothing on the consumer's side knows.
// - The memory a pool returns must be at least the capacity asked for.
//   Advance checks it once per chunk, and a pool that gives less breaks
//   its contract: the rented owner is disposed and the read fails with
//   an InvalidOperationException (it goes the normal fault path, so
//   Items and the stream complete with it). It does not clamp: the
//   O(1) indexer needs every chunk exactly its planned size, and a
//   short memory over a shared array would be written past its end.
// - A buffer is owned by exactly one party at a time: the producer
//   while it fills it, then the target (Publish returned true), or the
//   producer again if the target refused it (the target is disposed),
//   in which case it releases it at once.
// - Finish is called exactly once, last, whatever happens: a failure
//   of Seal (a pool whose owner throws) is recorded as the read's
//   error, so Items and the stream still complete. Nothing is written
//   after it. (If it is Slice that throws, the one buffer being filled
//   is not released: it goes to the garbage collector.)
// - The linked source is disposed when RunAsync ends, whatever
//   happens. Cancel is safe at any time: once the read is over (_ended)
//   it does nothing, so a stream that ends normally does not touch the
//   disposed source and throws no exception.
// - The connection ODX opened is closed by the query when the read
//   ends, not by this class.
internal sealed class ChunkProducer<T>(
	MappedQueryCore<T> query,
	IChunkTarget<T> target,
	MemoryPool<T> pool,
	bool clear,
	CancellationToken caller)
{
	// The caller's token and the owner's dispose, as one token:
	// it goes to the query, so a dispose cancels the database read.
	readonly CancellationTokenSource _linked
		= CancellationTokenSource.CreateLinkedTokenSource(caller);

	IMemoryOwner<T>? _owner;

	// The hot path of Add: the buffer being filled, as a window over the
	// array behind its memory. _next is the next free slot (an absolute
	// index into _array) and _limit one past the last slot of this
	// buffer: Add is one compare, one store and one increment, and the
	// slow path (a full or a missing buffer, or memory that is not an
	// array, for which _limit is 0) is a separate method.
	T[]? _array;
	int _offset;
	int _next;
	int _limit;

	// Rows so far, when the buffer is NOT array-backed (see Filled).
	int _count;
	int _index;
	int _capacity;
	int _ended;

	/// <summary>
	/// Starts the producer, once. A token that is already cancelled
	/// starts nothing: the target ends with that cancel.
	/// </summary>
	internal Task Start()
	{
		if (caller.IsCancellationRequested)
		{
			// Nothing starts, and the read is over: a Cancel can do nothing.
			Volatile.Write(ref _ended, 1);
			target.Finish(new OperationCanceledException(caller));
			_linked.Dispose();
			return Task.CompletedTask;
		}

		// StartNew plus Unwrap, not Task.Run: it keeps
		// TaskScheduler.Current (RunAsync is async: unwrap the Task<Task>).
		// A scheduler that throws when queueing leaves nothing started, so
		// the source is disposed here and the exception goes to the caller.
		try
		{
			return Task.Factory.StartNew(
				RunAsync,
				CancellationToken.None,
				TaskCreationOptions.DenyChildAttach,
				TaskScheduler.Current)
				.Unwrap();
		}
		catch
		{
			_linked.Dispose();
			throw;
		}
	}

	/// <summary>Cancels the query. Never throws.</summary>
	/// <remarks>
	/// Does nothing once the read is over. A provider's cancel callback
	/// may throw (the source then throws an
	/// <see cref="AggregateException"/>), and in a narrow race the source
	/// may be disposed already: the owner asked for the cancel, so
	/// neither matters to it.
	/// </remarks>
	internal void Cancel()
	{
		if (Volatile.Read(ref _ended) != 0) return;

		try { _linked.Cancel(); }
		catch (Exception) { }
	}

	async Task RunAsync()
	{
		Exception? error = null;
		try
		{
			await query
				.FillAsync(new ChunkSink<T>(this), _linked.Token)
				.ConfigureAwait(false);
		}
		catch (OperationCanceledException) when (target.IsDisposed)
		{
			// The owner asked for this: not an error.
		}
		catch (OperationCanceledException ex)
			when (caller.IsCancellationRequested)
		{
			// The query ran on the linked token: the reader sees the
			// caller's own.
			error = new OperationCanceledException(
				ex.Message, ex, caller);
		}
		catch (Exception ex)
		{
			error = ex;
		}

		// The read is over: a Cancel can do nothing more.
		Volatile.Write(ref _ended, 1);

		// The rows read so far are delivered, even on a failure. If
		// handing them over fails (a pool whose owner throws), that is
		// the read's error (the first error stays), and Finish still
		// runs: without it Items and the stream would never complete.
		// The source is disposed whatever happens here.
		try { Seal(); }
		catch (Exception ex) { error ??= ex; }

		try
		{
			target.Finish(error);
		}
		finally
		{
			_linked.Dispose();
		}
	}

	// One row, from the read loop (through ChunkSink): straight into the
	// array behind the current buffer's memory (the shared pool's memory
	// is an array). No enumerator, no allocation: the same loop
	// ToArrayAsync uses, with the buffer's bounds held in two fields.
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Add(T row)
	{
		int next = _next;
		if (next < _limit)
		{
			_array![next] = row;
			_next = next + 1;
			return;
		}

		AddSlow(row);
	}

	// How many rows the buffer being filled holds.
	int Filled
		=> _array is not null ? _next - _offset : _count;

	// A full buffer, no buffer yet, or memory that is not an array (it
	// goes through its span).
	void AddSlow(T row)
	{
		if (_owner is null || Filled == _capacity) Advance();

		if (_array is not null)
			_array[_next++] = row;
		else
			_owner!.Memory.Span[_count++] = row;
	}

	// Hands the full buffer on and rents the next one.
	void Advance()
	{
		// The token is checked once per chunk (the read loop looks at it
		// per row in the default Read mode, and the provider does in
		// ReadAsync mode): a cancel that a provider ignores still ends
		// the read here, at the next chunk. The chunk being filled is
		// sealed and delivered, as for every cancel.
		_linked.Token.ThrowIfCancellationRequested();

		if (_owner is not null)
		{
			IMemoryOwner<T> full = Take();
			if (!target.Publish(full))
			{
				full.Release();
				throw new OperationCanceledException();
			}
		}

		_capacity = target.CapacityOf(_index++);
		IMemoryOwner<T> owner = pool.Rent(_capacity);

		// Ask for the memory ONCE, and fail cleanly: an owner that throws
		// when asked is disposed here (nothing else holds it yet), and a
		// pool must give at least what was asked for. Checked once per
		// chunk, before the owner is kept, so nothing is half-open.
		Memory<T> memory;
		try
		{
			memory = owner.Memory;
		}
		catch
		{
			owner.Dispose();
			throw;
		}

		if (memory.Length < _capacity)
		{
			owner.Dispose();
			throw new InvalidOperationException(
				"The pool returned less memory than asked for.");
		}

		_count = 0;
		if (MemoryMarshal.TryGetArray<T>(
			memory,
			out ArraySegment<T> segment)
			&& segment.Array is not null)
		{
			_array = segment.Array;
			_offset = segment.Offset;
			_next = _offset;
			_limit = _offset + _capacity;
		}
		else
		{
			_array = null;
			_offset = 0;
			_next = 0;
			_limit = 0;
		}

		// Kept last: the buffer is open only once the window is set.
		_owner = owner;
	}

	// The buffer being filled, cut to the rows it holds
	// (the same owner when the pool gave exactly that).
	IMemoryOwner<T> Take()
	{
		IMemoryOwner<T> owner = _owner!.Slice(Filled);

		// The writer decides: with clearing on, every chunk it makes is
		// wrapped here, before any Publish, so a refused or leftover chunk
		// (released on a cancel, a fault or at Seal) is cleared too, like any
		// other (when the bin takes it).
		if (clear) owner = owner.ClearOnDispose();
		_owner = null;
		_array = null;
		_offset = 0;
		_next = 0;
		_limit = 0;
		_count = 0;
		return owner;
	}

	// Hands on the buffer that is still being filled, if there is one.
	void Seal()
	{
		if (_owner is null) return;

		// A buffer is opened just before a row is stored in it, so it
		// holds a row unless storing that row failed (the failure is the
		// read's error, and it is recorded first). An empty buffer is
		// never published: it is released, and this is reported (the
		// first error stays).
		if (Filled == 0)
		{
			IMemoryOwner<T> empty = Take();
			empty.Release();
			throw new InvalidOperationException(
				"The producer was left holding a buffer without a row.");
		}

		IMemoryOwner<T> last = Take();
		if (!target.Publish(last)) last.Release();
	}

}
