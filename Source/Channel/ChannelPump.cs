using System.Diagnostics;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions;

// One frame for every member of the package: the guards run at the
// call (the members are not async), then this frame waits for room,
// runs the feed, counts and completes.
// A feed is the only part that differs: where the rows come from.
internal static class ChannelPump
{
	/// <summary>
	/// Writes the rows of a feed to a channel.
	/// </summary>
	/// <param name="feed">Where the rows come from.</param>
	/// <param name="writer">Where the rows are written.</param>
	/// <param name="complete">
	/// <see langword="true"/> to complete the writer when the pass ends,
	/// with the exception that ended it if there was one.
	/// </param>
	/// <param name="waitFirst">
	/// <see langword="true"/> to wait for room before the feed starts,
	/// so a full target never gets a command run or a connection opened for it.
	/// </param>
	/// <param name="cancellationToken">Stops the pass.</param>
	/// <returns>The number of rows handed to the writer.</returns>
	internal static async ValueTask<long> Run<T, TFeed>(
		TFeed feed,
		ChannelWriter<T> writer,
		bool complete,
		bool waitFirst,
		CancellationToken cancellationToken)
		where TFeed : struct, IFeed<T>
	{
		Debug.Assert(writer is not null);

		// A cancel is an exception too: it completes the writer with
		// it, so a consumer can tell a cancel from a clean end.
		Exception? error = null;
		try
		{
			// Closed before the first row: nothing to write, nothing read.
			if (waitFirst
				&& !await writer
					.WaitToWriteAsync(cancellationToken)
					.ConfigureAwait(false))
			{
				return 0;
			}

			return await feed
				.WriteAsync(writer, cancellationToken)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			error = ex;
			throw;
		}
		finally
		{
			if (complete)
				writer.TryComplete(error);
		}
	}

	/// <summary>
	/// Writes rows from a position of a chunk until the writer has no room.
	/// </summary>
	/// <returns>The position of the first row not written.</returns>
	// The only code that holds a span: nothing can await while it lives.
	internal static int TryWriteFrom<T>(
		ChannelWriter<T> writer,
		ReadOnlyMemory<T> chunk,
		int start)
	{
		ReadOnlySpan<T> rows = chunk.Span;
		int i = start;
		while (i < rows.Length && writer.TryWrite(rows[i]))
			++i;

		return i;
	}
}

/// <summary>
/// Where the rows of one pass come from.
/// </summary>
internal interface IFeed<T>
{
	/// <summary>
	/// Writes every row to the writer.
	/// </summary>
	/// <returns>The number of rows handed to the writer.</returns>
	ValueTask<long> WriteAsync(
		ChannelWriter<T> writer,
		CancellationToken cancellationToken);
}

/// <summary>
/// The buffered feed: Core's chunk stream, read at full speed.
/// </summary>
/// <remarks>
/// The connection is closed when the read ends, however slowly the
/// writer takes the rows. A full target stalls only this loop:
/// the rows wait in the stream's queue.
/// </remarks>
internal readonly struct ChunkFeed<T>(
	IAsyncEnumerable<ReadOnlyMemory<T>> chunks)
	: IFeed<T>
{
	public async ValueTask<long> WriteAsync(
		ChannelWriter<T> writer,
		CancellationToken cancellationToken)
	{
		long count = 0;
		await foreach (ReadOnlyMemory<T> chunk in chunks
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			int i = 0;
			while (true)
			{
				int next = ChannelPump.TryWriteFrom(writer, chunk, i);
				count += next - i;
				i = next;
				if (i == chunk.Length) break;

				// Full, or closed: wait for room, then write the same row.
				// Leaving the loop disposes the stream, which cancels the
				// query and releases every chunk.
				if (!await writer
					.WaitToWriteAsync(cancellationToken)
					.ConfigureAwait(false))
				{
					return count;
				}
			}
		}

		return count;
	}
}

/// <summary>
/// The streaming feed: Core's <c>Stream</c>, one row at a time.
/// </summary>
/// <remarks>
/// The next row is pulled only after the current one was written, so a
/// full target holds the reader and the connection open.
/// </remarks>
internal readonly struct RowFeed<T>(IAsyncEnumerable<T> rows)
	: IFeed<T>
{
	// The stall timeout is Core's: this feed only learns of an expiry
	// while it waits for room, because a loop that is not pulling cannot
	// see it in MoveNextAsync.
	public async ValueTask<long> WriteAsync(
		ChannelWriter<T> writer,
		CancellationToken cancellationToken)
	{
		// Finite timeouts only: the plain path has no signal that can fire,
		// so it waits on the caller's token and creates nothing.
		IStallSignal? signal = rows as IStallSignal;
		using CancellationTokenSource? link
			= signal?.Expired.CanBeCanceled is true
				? CancellationTokenSource.CreateLinkedTokenSource(
					cancellationToken,
					signal.Expired)
				: null;
		CancellationToken wait = link?.Token ?? cancellationToken;

		long count = 0;
		await foreach (T row in rows
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			while (!writer.TryWrite(row))
			{
				// Without a link there is no signal that can fire:
				// the plain wait, nothing created.
				bool room = link is null
					? await writer
						.WaitToWriteAsync(cancellationToken)
						.ConfigureAwait(false)
					: await WaitAsync(
						writer,
						signal!,
						wait,
						cancellationToken).ConfigureAwait(false);

				if (!room) return count;
			}

			++count;
		}

		return count;
	}

	static async ValueTask<bool> WaitAsync(
		ChannelWriter<T> writer,
		IStallSignal signal,
		CancellationToken wait,
		CancellationToken cancellationToken)
	{
		try
		{
			return await writer
				.WaitToWriteAsync(wait)
				.ConfigureAwait(false);
		}

		// The stream expired while the target had no room, and
		// the caller did not cancel: Core's own exception, Core's text.
		catch (OperationCanceledException)
			when (signal.Expired.IsCancellationRequested
				&& !cancellationToken.IsCancellationRequested)
		{
			throw signal.ToException();
		}
	}
}
