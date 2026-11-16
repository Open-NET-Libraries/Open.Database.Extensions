using System.Diagnostics;
using System.Threading.Channels;

namespace Open.Database.Extensions.Core;

// The channel terminals of the command-level typed views
// (ToChannelReader and ToChannel) are thin rungs over one feed.
// The feed owns the channel ODX makes, runs the query through the
// view's own handler stream (so the connection, the read mode, the
// skip and take and the read-through are the ones every other
// terminal has), writes each row, and always completes the channel.
internal static class ChannelFeed
{
	/// <summary>
	/// The write loop: writes every row to <paramref name="writer"/>
	/// and returns how many it wrote.
	/// </summary>
	/// <remarks>
	/// Each row is written with <c>TryWrite</c>.
	/// When the channel is full, the loop waits for room
	/// (<c>WaitToWriteAsync</c>) and tries again, so no row is lost
	/// to a bound (a <c>Drop*</c> mode drops inside the channel itself).
	/// When the channel was closed, the loop stops and returns the rows
	/// handed to the writer so far; leaving the loop disposes the
	/// stream, so nothing more is read, and no exception is raised.
	/// When the channel was completed with an error by its owner,
	/// that error is thrown from the wait, so it propagates unchanged.
	/// It never completes the writer: the caller decides that.
	/// </remarks>
	/// <param name="rows">The row stream of a mapped query.</param>
	/// <param name="writer">Where the rows are written.</param>
	/// <param name="cancellationToken">
	/// Stops the loop: the cancel is thrown, not swallowed.
	/// </param>
	/// <returns>The number of rows handed to the writer.</returns>
	internal static async ValueTask<long> WriteAsync<T>(
		IAsyncEnumerable<T> rows,
		ChannelWriter<T> writer,
		CancellationToken cancellationToken)
	{
		Debug.Assert(rows is not null);
		Debug.Assert(writer is not null);

		long count = 0;
		await foreach (T row in rows
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			// A provider that reads synchronously never looks at
			// the token, so the loop does.
			cancellationToken.ThrowIfCancellationRequested();

			while (!writer.TryWrite(row))
			{
				bool room = await writer
					.WaitToWriteAsync(cancellationToken)
					.ConfigureAwait(false);

				// Closed: stop reading, the rows so far stand.
				if (!room) return count;
			}

			++count;
		}

		return count;
	}

	/// <summary>
	/// Makes the channel the caller's options describe.
	/// Only the BCL's two kinds are known; anything else throws.
	/// </summary>
	internal static Channel<T> Create<T>(ChannelOptions? options)
		=> options switch
		{
			null => Channel.CreateUnbounded<T>(),
			BoundedChannelOptions bounded
				=> Channel.CreateBounded<T>(bounded),
			UnboundedChannelOptions unbounded
				=> Channel.CreateUnbounded<T>(unbounded),
			_ => throw new ArgumentException(
				"Only BoundedChannelOptions and UnboundedChannelOptions"
				+ " are supported, or null for an unbounded channel.",
				nameof(options)),
		};
}

internal sealed class ChannelFeed<T>
{
	readonly MappedQueryCore<T> _query;
	readonly Channel<T> _inner;
	readonly TaskCompletionSource<long>? _count;
	readonly CancellationToken _token;
	int _started;

	internal ChannelFeed(
		MappedQueryCore<T> query,
		ChannelOptions? options,
		bool startImmediately,
		bool wantCount,
		CancellationToken cancellationToken)
	{
		// Throws ArgumentException for options that are not the BCL's,
		// at the call (the rungs are not async).
		_inner = ChannelFeed.Create<T>(options);
		_query = query;
		_token = cancellationToken;
		Channel = new FeedChannel<T>(this, _inner);

		if (wantCount)
		{
			_count = new TaskCompletionSource<long>(
				TaskCreationOptions.RunContinuationsAsynchronously);
		}

		// A token that is already cancelled never starts anything:
		// the channel ends with the cancel, so the consumer's read
		// throws it, and nothing is executed or opened.
		if (cancellationToken.IsCancellationRequested)
		{
			_started = 1;
			Cancelled(new OperationCanceledException(cancellationToken));
			return;
		}

		if (startImmediately) Start();
	}

	/// <summary>The channel handed to the caller.</summary>
	internal Channel<T> Channel { get; }

	/// <summary>The reader side of <see cref="Channel"/>.</summary>
	internal ChannelReader<T> Reader
		=> Channel.Reader;

	/// <summary>
	/// The row count of a feed made with a count:
	/// the task stays incomplete until the producer ends.
	/// </summary>
	[SuppressMessage("Roslynator", "RCS1210:Return completed task instead of returning null")]
	internal Task<long>? Count
		=> _count?.Task;

	/// <summary>
	/// Starts the producer, once, however many readers ask.
	/// The producer takes one thread-pool hop first, so the caller
	/// (a call or a read) returns at once, even when the provider
	/// completes the execute and every read synchronously.
	/// </summary>
	internal void Start()
	{
		if (Volatile.Read(ref _started) != 0
			|| Interlocked.Exchange(ref _started, 1) != 0)
		{
			return;
		}

		_ = Task.Run(RunAsync);
	}

	async Task RunAsync()
	{
		try
		{
			long count = await _query
				.ReadAsync<long>(WriteRowsAsync, _token)
				.ConfigureAwait(false);

			_inner.Writer.TryComplete();
			_count?.TrySetResult(count);
		}
		catch (OperationCanceledException ex)
		{
			Cancelled(ex);
		}
		catch (Exception ex)
		{
			_inner.Writer.TryComplete(ex);
			_count?.TrySetException(ex);
		}
	}

	ValueTask<long> WriteRowsAsync(
		IAsyncEnumerable<T> rows,
		CancellationToken cancellationToken)
		=> ChannelFeed.WriteAsync(
			rows, _inner.Writer, cancellationToken);

	void Cancelled(OperationCanceledException ex)
	{
		_inner.Writer.TryComplete(ex);
		_count?.TrySetCanceled(
			ex.CancellationToken.IsCancellationRequested
				? ex.CancellationToken
				: _token);
	}
}

// The reader a deferred feed hands out: the first read starts the
// producer. Reading Completion, Count, CanCount or CanPeek does not.
internal sealed class FeedReader<T>(
	ChannelFeed<T> feed, ChannelReader<T> inner)
	: ChannelReader<T>
{
	public override Task Completion
		=> inner.Completion;

	public override bool CanCount
		=> inner.CanCount;

	public override int Count
		=> inner.Count;

	public override bool CanPeek
		=> inner.CanPeek;

#if NETSTANDARD2_0
	public override bool TryPeek(out T item)
#else
	public override bool TryPeek([MaybeNullWhen(false)] out T item)
#endif
	{
		feed.Start();
		return inner.TryPeek(out item);
	}

#if NETSTANDARD2_0
	public override bool TryRead(out T item)
#else
	public override bool TryRead([MaybeNullWhen(false)] out T item)
#endif
	{
		feed.Start();
		return inner.TryRead(out item);
	}

	public override ValueTask<bool> WaitToReadAsync(
		CancellationToken cancellationToken = default)
	{
		feed.Start();
		return inner.WaitToReadAsync(cancellationToken);
	}

	public override ValueTask<T> ReadAsync(
		CancellationToken cancellationToken = default)
	{
		feed.Start();
		return inner.ReadAsync(cancellationToken);
	}

	// ReadAllAsync is not overridden: the base iterator calls this
	// class's WaitToReadAsync and TryRead, so it starts the feed.
}

// The channel a feed hands out: the reader is the starting wrapper,
// the writer is the inner channel's own.
internal sealed class FeedChannel<T> : Channel<T>
{
	internal FeedChannel(ChannelFeed<T> feed, Channel<T> inner)
	{
		Reader = new FeedReader<T>(feed, inner.Reader);
		Writer = inner.Writer;
	}
}
