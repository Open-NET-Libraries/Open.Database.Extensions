using System.Diagnostics;
using System.Threading.Tasks.Sources;

namespace Open.Database.Extensions.Core;

// The rows of a query read on the consumer's own thread: no channel, no producer task.
//
// The read is the query's usual one (ReadAsync with a handler), started by the first pull. The handler's loop is a coroutine of the consumer's loop:
// it hands a row over and parks on the pump gate; the next pull wakes it from the consumer's own call, so the next row is read right there, on the
// consumer's thread, and the pull returns an already completed task. Only when the provider waits (a real asynchronous read) does the pull return an
// incomplete one, and the rest of the read completes it from whatever thread finished the wait.
//
// A consumer that goes stallTimeout without pulling holds the reader and the connection for nothing. A watchdog timer (made when the first row is handed
// over, and only for a finite timeout) looks at the time the last row was handed over; the state word below makes "the consumer pulled" and "the watchdog
// expired" one decision: whichever moves the word first wins, and the loser does nothing.
/// <summary>
/// The rows of a query, one at a time, read as the consumer pulls them, with a stall watchdog.
/// </summary>
/// <remarks>
/// The reader and the connection are held from the first pull until the last row is read, the stream is disposed, or the consumer stalls.
/// </remarks>
internal sealed class DirectStream<T>
	: IAsyncEnumerable<T>, IValueTaskSource<bool>, IValueTaskSource
{
	// The low three bits of the state word are the state; the rest is the time the last row was handed over, in Stopwatch ticks.
	const int StateBits = 3;
	const long StateMask = (1L << StateBits) - 1;

	// Nothing started.
	const int NotStarted = 0;

	// The consumer has a row (or has not asked yet): the read is parked, the watchdog may expire it.
	const int Idle = 1;

	// The consumer asked for the next row and waits for it: the read is running.
	const int Reading = 2;

	// The consumer went too long without a pull: the read was stopped, and the next pull says so.
	const int Expired = 3;

	// The stream was disposed.
	const int Disposed = 4;

	// The read ended: the rows ran out or it failed, and the consumer was told.
	const int Done = 5;

	readonly MappedQueryCore<T> _query;
	readonly TimeSpan _stall;
	readonly long _stallTicks;
	readonly bool _finite;
	readonly CancellationToken _caller;

	int _taken;
	long _word;

	// The consumer waits here for a row (true) or the end (false); the read waits on _pumpGate for the next request.
	// Each side resets its own gate before it signals the other, so a signal never meets an unreset gate.
	ManualResetValueTaskSourceCore<bool> _consumerGate;
	ManualResetValueTaskSourceCore<bool> _pumpGate;

	T _current = default!;
	CancellationToken _enumeration;
	CancellationTokenSource? _read;
	Task? _pump;
	Timer? _timer;

	public DirectStream(
		MappedQueryCore<T> query,
		TimeSpan stallTimeout,
		CancellationToken caller)
	{
		_query = query;
		_stall = stallTimeout;
		_caller = caller;

		// An infinite timeout, or one over the longest a timer can count, makes no timer.
		_finite = stallTimeout != Timeout.InfiniteTimeSpan
			&& stallTimeout.TotalMilliseconds <= int.MaxValue;

		if (_finite)
		{
			_stallTicks = (long)(stallTimeout.TotalSeconds * Stopwatch.Frequency);
		}
	}

	// Whether the watchdog timer exists: it does not before the first row, nor after the stream is over.
	internal bool HasTimer => Volatile.Read(ref _timer) is not null;

	TimeoutException ToException()
		=> new($"The consumer made no pull for {_stall} while the reader and the connection were held; the read was stopped.");

	public IAsyncEnumerator<T> GetAsyncEnumerator(
		CancellationToken cancellationToken = default)
	{
		if (Interlocked.Exchange(ref _taken, 1) != 0)
		{
			throw new InvalidOperationException(
				"This stream can be enumerated only once."
				+ " Call ToListAsync() or ToPooledBuffer() to read the rows more than once.");
		}

		return Read(cancellationToken);
	}

	async IAsyncEnumerator<T> Read(CancellationToken cancellationToken)
	{
		// A token that is already canceled starts nothing: no command and no connection.
		cancellationToken.ThrowIfCancellationRequested();
		_caller.ThrowIfCancellationRequested();

		_enumeration = cancellationToken;
		try
		{
			while (true)
			{
				// A cancel always wins over an expiry that arrives with it.
				cancellationToken.ThrowIfCancellationRequested();
				_caller.ThrowIfCancellationRequested();

				if (!await NextAsync().ConfigureAwait(false)) yield break;
				yield return _current;
			}
		}
		finally
		{
			await DisposeCoreAsync().ConfigureAwait(false);
		}
	}

	#region State

	int State => (int)(Volatile.Read(ref _word) & StateMask);

	// Moves the state from one to another and says whether this call made the move.
	bool TryMove(int from, int to)
	{
		while (true)
		{
			long word = Volatile.Read(ref _word);
			if ((int)(word & StateMask) != from) return false;
			if (Interlocked.CompareExchange(ref _word, (word & ~StateMask) | (uint)to, word) == word) return true;
		}
	}

	// Sets the state whatever it was, and returns what it was.
	int Exchange(int to)
	{
		while (true)
		{
			long word = Volatile.Read(ref _word);
			if (Interlocked.CompareExchange(ref _word, (word & ~StateMask) | (uint)to, word) == word)
				return (int)(word & StateMask);
		}
	}

	#endregion

	#region The consumer's side

	// Called by the loop above, one pull at a time.
	ValueTask<bool> NextAsync()
	{
		if (State == NotStarted)
		{
			Interlocked.Exchange(ref _word, Reading);
			_read = CancellationTokenSource.CreateLinkedTokenSource(_caller, _enumeration);

			var first = new ValueTask<bool>(this, _consumerGate.Version);

			// The first part of the read (the command, the connection, up to the first row) runs here, on the consumer's thread.
			_pump = RunAsync();
			return first;
		}

		if (!TryMove(Idle, Reading)) return Refused();

		_consumerGate.Reset();
		var next = new ValueTask<bool>(this, _consumerGate.Version);

		// Wakes the read from this call: it reads the next row here, on the consumer's thread, or waits for the provider.
		_pumpGate.SetResult(true);
		return next;
	}

	ValueTask<bool> Refused()
		=> State switch
		{
			// The watchdog stopped the read: the pull that follows an expiry is the one that reports it, never a silent end.
			Expired => throw ToException(),
			Reading => throw new InvalidOperationException("The previous pull has not finished: pull the next row only after the last one."),
			Disposed => throw new ObjectDisposedException(nameof(DirectStream<>)),
			_ => default,
		};

	#endregion

	#region The read's side

	async Task RunAsync()
	{
		Exception? error = null;
		try
		{
			await _query
				.ReadAsync(PumpAsync, _read!.Token)
				.ConfigureAwait(false);
		}
		catch (OperationCanceledException ex) when (_caller.IsCancellationRequested)
		{
			error = new OperationCanceledException(ex.Message, ex, _caller);
		}
		catch (OperationCanceledException ex) when (_enumeration.IsCancellationRequested)
		{
			error = new OperationCanceledException(ex.Message, ex, _enumeration);
		}
		catch (Exception) when (State is Expired or Disposed)
		{
			// The watchdog or the owner stopped the read: the next pull reports an expiry, and a disposed stream has no one to tell.
		}
		catch (Exception ex)
		{
			error = ex;
		}

		// The consumer is told only if it is waiting.
		if (!TryMove(Reading, Done)) return;

		StopTimer();
		if (error is null) _consumerGate.SetResult(false);
		else _consumerGate.SetException(error);
	}

	async ValueTask PumpAsync(IAsyncEnumerable<T> rows, CancellationToken token)
	{
		await foreach (T row in rows.WithCancellation(token).ConfigureAwait(false))
		{
			_current = row;

			// The pump gate is reset before the consumer can be told, so its next pull finds it ready.
			_pumpGate.Reset();

			// The watchdog starts when the first row is handed over, and only for a finite timeout.
			if (_finite) _timer ??= StartTimer();

			// The row is handed over only if the consumer is still waiting for it: a stream that was disposed in the meantime has no one to hand it to.
			if (!HandOver()) throw new OperationCanceledException(token);

			// The consumer may pull again from inside the next call, before this method reaches its await: the gate keeps that wake.
			short parked = _pumpGate.Version;
			_consumerGate.SetResult(true);

			// Parked until the next pull wakes the read, or the watchdog or the owner ends it with an exception.
			await new ValueTask(this, parked).ConfigureAwait(false);
		}
	}

	// Reading to Idle, stamped with the time of the hand-over in the same step: the watchdog reads the state and the time as one.
	bool HandOver()
	{
		while (true)
		{
			long word = Volatile.Read(ref _word);
			if ((int)(word & StateMask) != Reading) return false;

			long stamped = (Stopwatch.GetTimestamp() << StateBits) | Idle;
			if (Interlocked.CompareExchange(ref _word, stamped, word) == word) return true;
		}
	}

	#endregion

	#region The watchdog

	Timer StartTimer()
	{
		// A quarter of the timeout, so an expiry is found within a quarter of it, and between 10 ms and a second.
		TimeSpan period = TimeSpan.FromTicks(Math.Min(
			Math.Max(_stall.Ticks / 4, TimeSpan.TicksPerMillisecond * 10),
			TimeSpan.TicksPerSecond));

		return new Timer(
			static state => ((DirectStream<T>)state!).Tick(),
			this,
			period,
			period);
	}

	void StopTimer()
		=> Interlocked.Exchange(ref _timer, null)?.Dispose();

	void Tick()
	{
		long word = Volatile.Read(ref _word);
		int state = (int)(word & StateMask);

		// A stream that is over has no use for the timer.
		if (state is Expired or Disposed or Done)
		{
			StopTimer();
			return;
		}

		if (state != Idle) return;

		if (Stopwatch.GetTimestamp() - (word >> StateBits) < _stallTicks) return;

		// The consumer pulling and this expiry move the same word: the one that moves it first wins.
		if (Interlocked.CompareExchange(ref _word, (word & ~StateMask) | Expired, word) != word) return;

		StopTimer();

		// The command is canceled first, so the provider discards the rest instead of draining it;
		// the read is then woken with the exception, so it unwinds and cleans up, closing the reader and the connection.
		try { _read!.Cancel(); }
		catch { }

		_pumpGate.SetException(ToException());
	}

	#endregion

	async ValueTask DisposeCoreAsync()
	{
		int was = Exchange(Disposed);
		if (was is NotStarted or Disposed) return;

		StopTimer();

		try { _read!.Cancel(); }
		catch { }

		// The read is parked: wake it with a cancel, so it unwinds, and cleans up, here.
		// If the watchdog got there first it did the waking; if the consumer was mid-pull the read sees the state and unwinds itself.
		if (was == Idle) _pumpGate.SetException(new OperationCanceledException(_read!.Token));

		try { await _pump!.ConfigureAwait(false); }
		finally { _read!.Dispose(); }
	}

	#region The two gates

	bool IValueTaskSource<bool>.GetResult(short token)
		=> _consumerGate.GetResult(token);

	ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token)
		=> _consumerGate.GetStatus(token);

	void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
		=> _consumerGate.OnCompleted(continuation, state, token, flags);

	void IValueTaskSource.GetResult(short token)
		=> _pumpGate.GetResult(token);

	ValueTaskSourceStatus IValueTaskSource.GetStatus(short token)
		=> _pumpGate.GetStatus(token);

	void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
		=> _pumpGate.OnCompleted(continuation, state, token, flags);

	#endregion
}
