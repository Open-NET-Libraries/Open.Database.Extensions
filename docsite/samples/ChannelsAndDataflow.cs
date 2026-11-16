using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class ChannelsAndDataflow
{
	public static async Task<List<Post>> ToChannelSample(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		var received = new List<Post>();

		#region to_channel
		// Returns right away; rows are produced in the background and the channel completes when they are all written.
		ChannelReader<Post> reader = connectionFactory
			.Command("select * from posts order by id")
			.ToChannel<Post>(singleReader: true);

		await foreach (Post post in reader.ReadAllAsync())
			received.Add(post);
		#endregion

		return received;
	}

	// A stand-in for slow per-row work: a web call, a file write,
	// a cache lookup. It is the reason to have a consumer at all.
	static async Task<string> Enrich(
		Post post,
		CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		await Task.Yield();
		return $"#{post.Id} {post.Title}";
	}

	public static async Task<List<string>> ProducerConsumerSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var enriched = new List<string>();

		#region stream_producer_consumer
		// The query is the producer: it reads the rows at database
		// speed and writes them into the channel. Nothing runs until
		// the first read below (the default is a deferred start).
		ChannelReader<Post> reader = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannelReader(cancellationToken);

		// This is the consumer. Its slow work does not hold the
		// query back, because the rows are already in the channel.
		await foreach (Post post in reader.ReadAllAsync(cancellationToken))
		{
			enriched.Add(await Enrich(post, cancellationToken));
		}
		#endregion

		return enriched;
	}

	public static async Task<(
		int HandledWhenReadEnded,
		long Count,
		int Handled,
		long Meanwhile)>
		ProducerReleasesTheConnectionSample(
			SqliteConnection connection,
			CancellationToken cancellationToken)
	{
		int handled = 0;
		var gate = new TaskCompletionSource(
			TaskCreationOptions.RunContinuationsAsynchronously);

		#region stream_release
		// ToChannel hands you both ends. rowCount is a Task<long>
		// that completes when the query has been read to the end.
		var channel = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannel(out var rowCount, cancellationToken);

		// A slow consumer: it takes the first row (which starts the
		// read) and then waits at the gate before it handles anything.
		Task consumer = Task.Run(
			async () =>
			{
				var rows = channel.Reader.ReadAllAsync(cancellationToken);
				await foreach (Post post in rows)
				{
					await gate.Task;
					await Enrich(post, cancellationToken);
					Interlocked.Increment(ref handled);
				}
			},
			cancellationToken);

		// The query has been read to the end, and nothing is handled.
		long count = await rowCount;
		int handledWhenReadEnded = Volatile.Read(ref handled);

		// So the connection is free for other work, right now,
		// while the consumer is still on its first row.
		long meanwhile = await connection
			.Command("select count(*) from posts")
			.ExecuteScalarAsync<long>(cancellationToken);

		gate.SetResult();
		await consumer;
		#endregion

		return (handledWhenReadEnded, count, handled, meanwhile);
	}

	public static async Task<List<int>[]> FanOutSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		const int workers = 3;
		int holding = 0;
		var everyoneHasARow = new TaskCompletionSource(
			TaskCreationOptions.RunContinuationsAsynchronously);

		#region stream_fan_out
		ChannelReader<Post> reader = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannelReader(cancellationToken);

		// Every consumer reads the SAME reader: each row goes to
		// exactly one of them, so the slow work is shared out.
		async Task<List<int>> ConsumeAsync()
		{
			var mine = new List<int>();
			var rows = reader.ReadAllAsync(cancellationToken);
			await foreach (Post post in rows)
			{
				// For this sample only: hold the first row until each
				// consumer has one, so the split is not left to timing.
				if (mine.Count == 0
					&& Interlocked.Increment(ref holding) == workers)
				{
					everyoneHasARow.SetResult();
				}

				await everyoneHasARow.Task;
				await Enrich(post, cancellationToken);
				mine.Add(post.Id);
			}

			return mine;
		}

		List<int>[] shares = await Task.WhenAll(
			ConsumeAsync(),
			ConsumeAsync(),
			ConsumeAsync());
		#endregion

		return shares;
	}

	public static async Task<(int ReadWhileHeld, List<int> Received)>
		BackPressureSample(
			SqliteConnection connection,
			CancellationToken cancellationToken)
	{
		int mapped = 0;
		var firstRowTaken = new TaskCompletionSource(
			TaskCreationOptions.RunContinuationsAsynchronously);
		var gate = new TaskCompletionSource(
			TaskCreationOptions.RunContinuationsAsynchronously);
		var received = new List<int>();

		#region stream_back_pressure
		// A bound is back-pressure: with room for two rows, the query
		// waits for the consumer instead of reading the whole result
		// into memory. (The mapping counts the rows it has read.)
		ChannelReader<Post> reader = connection
			.Command(
				"""
				with recursive n(i) as (
					select 1 union all select i + 1 from n where i < 100)
				select i from n
				""")
			.Map(record =>
			{
				Interlocked.Increment(ref mapped);
				return new Post { Id = record.GetInt32(0) };
			})
			.ToChannelReader(
				new BoundedChannelOptions(2) { SingleReader = true },
				startImmediately: true,
				cancellationToken);

		Task consumer = Task.Run(
			async () =>
			{
				var rows = reader.ReadAllAsync(cancellationToken);
				await foreach (Post post in rows)
				{
					firstRowTaken.TrySetResult();
					await gate.Task;
					received.Add(post.Id);
				}
			},
			cancellationToken);

		// The consumer holds one row and is stuck, so the query can be
		// at most two rows (the room) and one more in hand ahead of it.
		await firstRowTaken.Task;
		int readWhileHeld = Volatile.Read(ref mapped);

		gate.SetResult();
		await consumer;
		#endregion

		return (readWhileHeld, received);
	}

	public static async Task<List<string>> PipelineSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var lines = new List<string>();

		#region stream_pipeline
		// Stage 1: the query, as a producer.
		ChannelReader<Post> posts = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannelReader(cancellationToken);

		// Stage 2: a transform that consumes stage 1 and is the
		// producer of a second channel.
		var formatted = Channel.CreateUnbounded<string>();
		Task transform = Task.Run(
			async () =>
			{
				try
				{
					var rows = posts.ReadAllAsync(cancellationToken);
					await foreach (Post post in rows)
					{
						string line = await Enrich(post, cancellationToken);
						await formatted.Writer.WriteAsync(
							line,
							cancellationToken);
					}

					formatted.Writer.TryComplete();
				}
				catch (Exception ex)
				{
					formatted.Writer.TryComplete(ex);
					throw;
				}
			},
			cancellationToken);

		// Stage 3: the final consumer.
		var results = formatted.Reader.ReadAllAsync(cancellationToken);
		await foreach (string line in results)
			lines.Add(line);

		await transform;
		#endregion

		return lines;
	}

	public static async Task<List<string>> StreamDirectSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region stream_direct
		// No channel needed: one consumer, and its work is as fast as
		// the read. Iterating the query directly is simpler.
		List<string> lines = await connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ReadAsync(
				static async (rows, token) =>
				{
					var list = new List<string>();
					await foreach (Post post in rows.WithCancellation(token))
						list.Add($"#{post.Id} {post.Title}");

					return list;
				},
				cancellationToken);
		#endregion

		return lines;
	}

	public static async Task<(long Count, List<Post> Received)> ToTargetBlockSample(IDbConnectionFactory<SqliteConnection> connectionFactory)
	{
		#region to_target_block
		var block = new BufferBlock<Post>();

		// complete: true completes the block after every row has been delivered (or faults it if reading fails).
		long count = await connectionFactory
			.Command("select * from posts order by id")
			.ToTargetBlockAsync<Post>(block, complete: true);

		// The block completes once every row has been delivered and received.
		var received = new List<Post>();
		while (await block.OutputAvailableAsync())
			received.Add(await block.ReceiveAsync());
		#endregion

		return (count, received);
	}
}
