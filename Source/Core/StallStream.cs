using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// A write loop that waits for room in a full target is not pulling, so it cannot learn of an expiry from MoveNextAsync.
/// <summary>
/// Lets a sibling package learn that a <c>Stream</c> read stopped because the consumer made no pull in time.
/// </summary>
/// <remarks>A write loop that waits for room checks the stream for this interface and waits with a token linked to <see cref="Expired"/>.</remarks>
internal interface IStallSignal
{
	/// <summary>
	/// Gets a token that is canceled when the stream stops because the consumer made no pull in time, and never otherwise.
	/// It is <see cref="CancellationToken.None"/> when the stream has no timeout.
	/// </summary>
	CancellationToken Expired { get; }

	/// <summary>
	/// Creates the exception the stream throws on the pull that follows an expiry.
	/// </summary>
	/// <returns>The <see cref="TimeoutException"/>.</returns>
	TimeoutException ToException();
}

/// <summary>
/// The sizes of the stream buffer, kept out of the generic <see cref="StallStream{T}"/> so they are one value for every row type.
/// </summary>
internal static class StallStream
{
	/// <summary>
	/// Gets the most rows the read runs ahead of the consumer unless the caller asks for another size.
	/// </summary>
	/// <remarks>
	/// A ceiling against a read that outruns its consumer without bound: it counts rows, not bytes.
	/// A buffer this size lets the read ride out a slow consumer instead of waiting on every row.
	/// </remarks>
	public const int DefaultBufferSize = 1024;
}

// The producer reads ahead of the consumer, up to the buffer size in rows, and then waits for room in the channel. That wait is the stall, and it is measured on the producer's own side:
// when it lasts stallTimeout the producer stops the read from inside its own handler, so the cleanup of the query runs on the thread that owns the reader.
// Only the channel, the expiry and disposal flags and the cancellation sources are shared between the two sides.
/// <summary>
/// The rows of a query, one at a time, read by a producer that holds the reader while the consumer pulls.
/// </summary>
/// <remarks>
/// The producer reads up to the buffer size it was given (<see cref="StallStream.DefaultBufferSize"/> for the buffered stream) rows ahead of the consumer, which never touches the reader.
/// </remarks>
internal sealed class StallStream<T> : IAsyncEnumerable<T>, IStallSignal
{
	readonly MappedQueryCore<T> _query;
	readonly TimeSpan _stall;
	readonly int _bufferSize;
	readonly bool _finite;
	readonly CancellationToken _caller;

	// Cancelled when the stream expires; the sibling packages read it.
	readonly CancellationTokenSource? _expiredSource;

	int _taken;
	int _expired;
	int _disposed;

	// The enumeration's own state, set when it starts.
	ChannelWriter<T>? _writer;
	CancellationTokenSource? _read;
	CancellationTokenSource? _wait;
	CancellationToken _enumeration;

	public StallStream(
		MappedQueryCore<T> query,
		TimeSpan stallTimeout,
		int bufferSize,
		CancellationToken caller)
	{
		_query = query;
		_stall = stallTimeout;
		_bufferSize = bufferSize;
		_caller = caller;

		// An infinite timeout, or one over the longest a timer can count, starts no timer and no signal source.
		_finite = stallTimeout != Timeout.InfiniteTimeSpan
			&& stallTimeout.TotalMilliseconds <= int.MaxValue;

		if (_finite) _expiredSource = new CancellationTokenSource();
	}

	public CancellationToken Expired
		=> _expiredSource?.Token ?? default;

	public TimeoutException ToException()
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
		// A token that is already canceled starts nothing: no producer, so no command and no connection.
		cancellationToken.ThrowIfCancellationRequested();
		_caller.ThrowIfCancellationRequested();

		var channel = Channel.CreateBounded<T>(
			new BoundedChannelOptions(_bufferSize)
			{
				SingleReader = true,
				SingleWriter = true,
				FullMode = BoundedChannelFullMode.Wait,
				AllowSynchronousContinuations = false,
			});

		_enumeration = cancellationToken;
		_writer = channel.Writer;
		_read = CancellationTokenSource.CreateLinkedTokenSource(_caller, cancellationToken);
		if (_finite) _wait = CancellationTokenSource.CreateLinkedTokenSource(_read.Token);

		Task producer = StartProducer(channel.Writer);
		try
		{
			ChannelReader<T> reader = channel.Reader;
			while (true)
			{
				// A cancel always wins over an expiry that arrives with it.
				cancellationToken.ThrowIfCancellationRequested();
				_caller.ThrowIfCancellationRequested();

				// Nothing more is delivered once the stream has expired:
				// the next pull is the one that reports it.
				if (Volatile.Read(ref _expired) != 0) throw ToException();

				if (reader.TryRead(out T? row))
				{
					yield return row;
					continue;
				}

				// False: the producer ended without a failure and every row was read.
				// A failure or a cancel is thrown here, after the rows that were delivered.
				if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
					yield break;
			}
		}
		finally
		{
			Volatile.Write(ref _disposed, 1);
			try { _read.Cancel(); }
			catch { }

			// The producer never faults: it catches everything and completes the channel with it.
			try { await producer.ConfigureAwait(false); }
			finally
			{
				_wait?.Dispose();
				_read.Dispose();
			}
		}
	}

	[SuppressMessage("Roslynator", "RCS1229:Use async/await when necessary")]
	Task StartProducer(ChannelWriter<T> writer)
	{
		// StartNew plus Unwrap, not Task.Run: it keeps TaskScheduler.Current, as the buffered reads do.
		try
		{
			return Task.Factory.StartNew(
				() => ProduceAsync(writer),
				CancellationToken.None,
				TaskCreationOptions.DenyChildAttach,
				TaskScheduler.Current)
				.Unwrap();
		}
		catch
		{
			_wait?.Dispose();
			_read!.Dispose();
			throw;
		}
	}

	async Task ProduceAsync(ChannelWriter<T> writer)
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
		catch (Exception) when (Volatile.Read(ref _expired) != 0)
		{
			// The wait ran out: that is the read's error, whatever the cleanup after it threw.
			error = ToException();
		}
		catch (OperationCanceledException) when (Volatile.Read(ref _disposed) != 0)
		{
			// The owner asked for this: not an error.
		}
		catch (Exception ex)
		{
			error = ex;
		}

		writer.TryComplete(error);
	}

	async ValueTask PumpAsync(IAsyncEnumerable<T> rows, CancellationToken token)
	{
		await foreach (T row in rows.WithCancellation(token).ConfigureAwait(false))
			await HandOffAsync(row, token).ConfigureAwait(false);
	}

	async ValueTask HandOffAsync(T row, CancellationToken token)
	{
		ChannelWriter<T> writer = _writer!;

		// Room in the channel: the consumer is keeping up and nothing waits.
		if (writer.TryWrite(row)) return;

		if (_wait is null)
		{
			await writer.WriteAsync(row, token).ConfigureAwait(false);
			return;
		}

		// A timer that fired after the previous wait ended and before it was disarmed: the consumer had pulled,
		// so the source is replaced and no expiry is reported.
		if (_wait.IsCancellationRequested)
		{
			token.ThrowIfCancellationRequested();
			_wait.Dispose();
			_wait = CancellationTokenSource.CreateLinkedTokenSource(_read!.Token);
		}

		// The wait is the stall. A cancel of the read ends it too, and is told apart from an expiry below.
		_wait.CancelAfter(_stall);
		try
		{
			await writer.WriteAsync(row, _wait.Token).ConfigureAwait(false);
		}
		catch (OperationCanceledException) when (!token.IsCancellationRequested)
		{
			throw Expire();
		}

		_wait.CancelAfter(Timeout.InfiniteTimeSpan);
	}

	TimeoutException Expire()
	{
		Volatile.Write(ref _expired, 1);

		// The command is canceled first, so the provider discards the rest instead of draining it;
		// the exception then leaves the handler and the query's own cleanup runs on this side.
		try { _read!.Cancel(); }
		catch { }

		try { _expiredSource!.Cancel(); }
		catch { }

		return ToException();
	}
}
