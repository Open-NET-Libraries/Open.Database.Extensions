using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

// The one producer behind the buffer terminals (ToAsyncBuffer,
// ToAsyncBufferStream and ToBufferedAsyncStream).
//
// What it does.
// It runs the query through the view's own handler stream (so the
// connection, the read mode, Skip and Take and the read-through of
// later result sets are the ones every other terminal has, as for the
// channel feed), copies each row into a buffer rented from the pool,
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
//   which is unwrapped. DenyChildAttach only stops tasks created by
//   the read from attaching to this one; it is detached anyway,
//   because AttachedToParent is not given.
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
//   too: they hold rows. Nothing on the consumer's side knows.
// - The memory a pool returns must be at least the capacity asked for:
//   a Debug.Assert checks it, nothing does in a release build.
// - A buffer is owned by exactly one party at a time: the producer
//   while it fills it, then the target (Publish returned true), or the
//   producer again if the target refused it (the target is disposed),
//   in which case it releases it at once.
// - Finish is called exactly once, last. Nothing is written after it.
// - The linked source is disposed when RunAsync ends, whatever
//   happens; Cancel is safe at any time, before or after that.
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
	T[]? _array;
	int _offset;
	int _count;
	int _index;
	int _capacity;

	/// <summary>
	/// Starts the producer, once. A token that is already cancelled
	/// starts nothing: the target ends with that cancel.
	/// </summary>
	internal Task Start()
	{
		if (caller.IsCancellationRequested)
		{
			target.Finish(new OperationCanceledException(caller));
			_linked.Dispose();
			return Task.CompletedTask;
		}

		// StartNew plus Unwrap, not Task.Run: it keeps
		// TaskScheduler.Current (RunAsync is async: unwrap the Task<Task>).
		return Task.Factory.StartNew(
			RunAsync,
			CancellationToken.None,
			TaskCreationOptions.DenyChildAttach,
			TaskScheduler.Current)
			.Unwrap();
	}

	/// <summary>Cancels the query. Never throws.</summary>
	/// <remarks>
	/// A provider's cancel callback may throw (the source then throws
	/// an <see cref="AggregateException"/>), and the source may be
	/// disposed already: the owner asked for the cancel, so neither
	/// matters to it.
	/// </remarks>
	internal void Cancel()
	{
		try { _linked.Cancel(); }
		catch (Exception) { }
	}

	async Task RunAsync()
	{
		Exception? error = null;
		try
		{
			await query
				.ReadAsync<long>(FillAsync, _linked.Token)
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

		// The rows read so far are delivered, even on a failure.
		// The source is disposed whatever happens here (a pool whose
		// Return throws, for one); Cancel() stays safe after that.
		try
		{
			Seal();
			target.Finish(error);
		}
		finally
		{
			_linked.Dispose();
		}
	}

	async ValueTask<long> FillAsync(
		IAsyncEnumerable<T> rows,
		CancellationToken cancellationToken)
	{
		long total = 0;
		await foreach (T row in rows
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			// A provider that reads synchronously never looks at
			// the token, so the loop does.
			cancellationToken.ThrowIfCancellationRequested();

			if (_owner is null || _count == _capacity) Advance();

			// Straight into the array behind the memory (the shared
			// pool's memory is an array); a memory that is not an array
			// goes through its span.
			if (_array is not null)
				_array[_offset + _count] = row;
			else
				_owner!.Memory.Span[_count] = row;

			++_count;
			++total;
		}

		return total;
	}

	// Hands the full buffer on and rents the next one.
	void Advance()
	{
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
		Debug.Assert(owner.Memory.Length >= _capacity);
		_owner = owner;

		if (MemoryMarshal.TryGetArray<T>(
			owner.Memory,
			out ArraySegment<T> segment)
			&& segment.Array is not null)
		{
			_array = segment.Array;
			_offset = segment.Offset;
		}
		else
		{
			_array = null;
			_offset = 0;
		}
	}

	// The buffer being filled, cut to the rows it holds
	// (the same owner when the pool gave exactly that).
	IMemoryOwner<T> Take()
	{
		IMemoryOwner<T> owner = _owner!.Slice(_count);

		// The writer decides: with clearing on, every chunk it makes is
		// wrapped here, before any Publish, so a refused or leftover chunk
		// (released on a cancel, a fault or at Seal) is cleared too.
		if (clear) owner = owner.ClearOnDispose();
		_owner = null;
		_array = null;
		_offset = 0;
		_count = 0;
		return owner;
	}

	// Hands on the buffer that is still being filled, if it has rows.
	void Seal()
	{
		if (_owner is null) return;

		// Advance runs just before a row is stored: an open buffer
		// always holds at least one row.
		Debug.Assert(_count > 0);
		IMemoryOwner<T> last = Take();
		if (!target.Publish(last)) last.Release();
	}
}
