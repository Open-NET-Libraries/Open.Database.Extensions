using System.Diagnostics;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions;

// One frame for every member of the package: the guards run at the
// call (the members are not async), then this frame checks the target,
// runs the feed, and completes or faults the target.
// A feed is the only part that differs: where the rows come from.
internal static class BlockPump
{
	/// <summary>
	/// Writes the rows of a feed to a block.
	/// </summary>
	/// <param name="feed">Where the rows come from.</param>
	/// <param name="target">Where the rows are sent.</param>
	/// <param name="complete">
	/// <see langword="true"/> to complete the target when the pass ends,
	/// or to fault it with the exception that ended the pass.
	/// </param>
	/// <param name="checkOpen">
	/// <see langword="true"/> to stop before the feed starts when the
	/// target is already finished, so no command is run for it.
	/// </param>
	/// <param name="cancellationToken">Stops the pass.</param>
	/// <returns>The number of rows the target accepted.</returns>
	internal static async ValueTask<long> Run<T, TFeed>(
		TFeed feed,
		ITargetBlock<T> target,
		bool complete,
		bool checkOpen,
		CancellationToken cancellationToken)
		where TFeed : struct, IFeed<T>
	{
		Debug.Assert(target is not null);

		// A cancel is an exception too: it faults the target with it,
		// so a consumer can tell a cancel from a clean end.
		Exception? error = null;
		try
		{
			// A block has no way to ask for room: only a row can tell.
			// A block that is already finished is the one thing known now.
			if (checkOpen && target.Completion.IsCompleted)
				return 0;

			return await feed
				.WriteAsync(target, cancellationToken)
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
			{
				if (error is null) target.Complete();
				else target.Fault(error);
			}
		}
	}
}

/// <summary>
/// Where the rows of one pass come from.
/// </summary>
internal interface IFeed<T>
{
	/// <summary>
	/// Sends every row to the target.
	/// </summary>
	/// <returns>The number of rows the target accepted.</returns>
	ValueTask<long> WriteAsync(
		ITargetBlock<T> target,
		CancellationToken cancellationToken);
}

/// <summary>
/// The buffered feed: Core's chunk stream, read at full speed.
/// </summary>
/// <remarks>
/// The connection is closed when the read ends, however slowly the
/// target takes the rows. A full target stalls only this loop:
/// the rows wait in the stream's queue.
/// </remarks>
internal readonly struct ChunkFeed<T>(
	IAsyncEnumerable<ReadOnlyMemory<T>> chunks)
	: IFeed<T>
{
	public async ValueTask<long> WriteAsync(
		ITargetBlock<T> target,
		CancellationToken cancellationToken)
	{
		long count = 0;
		await foreach (ReadOnlyMemory<T> chunk in chunks
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			for (int i = 0; i < chunk.Length; ++i)
			{
				// SendAsync waits while a bounded target is full;
				// false means the target declined for good.
				// Leaving the loop disposes the stream, which cancels the
				// query and releases every chunk.
				T row = chunk.Span[i];
				if (!await target
					.SendAsync(row, cancellationToken)
					.ConfigureAwait(false))
				{
					return count;
				}

				++count;
			}
		}

		return count;
	}
}

/// <summary>
/// The streaming feed: Core's <c>Stream</c>, one row at a time.
/// </summary>
/// <remarks>
/// The next row is pulled only after the current one was accepted, so a
/// full target holds the reader and the connection open.
/// The stall timeout is Core's: this feed only learns of an expiry
/// while it waits for the target, because a loop that is not pulling
/// cannot see it in <c>MoveNextAsync</c>.
/// </remarks>
internal readonly struct RowFeed<T>(IAsyncEnumerable<T> rows)
	: IFeed<T>
{
	public async ValueTask<long> WriteAsync(
		ITargetBlock<T> target,
		CancellationToken cancellationToken)
	{
		// Finite timeouts only: the plain path has no signal that can fire,
		// so it waits on the caller's token and creates nothing.
		IStallSignal? signal = rows as IStallSignal;
		using CancellationTokenSource? link
			= signal is not null && signal.Expired.CanBeCanceled
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
			bool accepted = link is null
				? await target
					.SendAsync(row, cancellationToken)
					.ConfigureAwait(false)
				: await SendAsync(
					target,
					row,
					signal!,
					wait,
					cancellationToken).ConfigureAwait(false);

			if (!accepted) return count;
			++count;
		}

		return count;
	}

	static async ValueTask<bool> SendAsync(
		ITargetBlock<T> target,
		T row,
		IStallSignal signal,
		CancellationToken wait,
		CancellationToken cancellationToken)
	{
		try
		{
			return await target
				.SendAsync(row, wait)
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
