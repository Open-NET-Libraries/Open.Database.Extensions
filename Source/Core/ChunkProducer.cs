using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Open.Database.Extensions.Core;

// The two buffer terminals of the typed views (ToAsyncBuffer and
// ToAsyncBufferStream) share one producer. It runs the query through
// the view's own handler stream (so the connection, the read mode,
// the skip and take and the read-through are the ones every other
// terminal has, as for the channel feed), fills pooled arrays, and
// hands each filled array to its target. The target decides what
// happens to the chunks: the buffer keeps them until it is disposed,
// the stream queues them for one consumer.

internal sealed class ChunkProducer<T>(
	MappedQueryCore<T> query,
	IChunkTarget<T> target,
	MemoryPool<T> pool,
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
		// TaskScheduler.Current (the lambda is async, so unwrap it).
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
