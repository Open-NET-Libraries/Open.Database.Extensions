using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class ChannelsAndDataflow
{
	public static async Task<List<Post>> ToChannelSample(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region to_channel
		// Returns right away: the rows are read in the background
		// and the channel completes when they are all written.
		ChannelReader<Post> reader = connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannelReader(cancellationToken);

		await foreach (Post post in reader.ReadAllAsync(cancellationToken))
			received.Add(post);
		#endregion

		return received;
	}

	public static async Task<(long Written, List<Post> Received)> ToChannelOwnedSample(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region to_channel_owned
		// You own the channel. A pass only writes: it leaves the
		// channel open, so passes can share it.
		Channel<Post> channel = Channel.CreateUnbounded<Post>();

		long first = await connectionFactory
			.Command("select * from posts where id <= 3 order by id")
			.Map<Post>()
			.ToChannel(channel, cancellationToken);

		// The last pass completes the channel when it ends.
		long second = await connectionFactory
			.Command("select * from posts where id > 3 order by id")
			.Map<Post>()
			.ToChannel(
				channel,
				complete: true,
				cancellationToken);

		await foreach (Post post in channel.Reader.ReadAllAsync(cancellationToken))
			received.Add(post);
		#endregion

		return (first + second, received);
	}

	public static async Task<(long Written, List<Post> Received)> ToChannelHoldingConnectionSample(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region to_channel_holding
		// The channel holds at most two rows. The next row is pulled
		// only after the last one was written, and the connection is
		// held for each row: a slow consumer holds it. To release it as
		// soon as the last row is read, use ToChannel (above).
		Channel<Post> channel = Channel.CreateBounded<Post>(2);

		ValueTask<long> write = connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannelHoldingConnection(
				channel,
				TimeSpan.FromSeconds(30),
				complete: true,
				cancellationToken);

		await foreach (Post post in channel.Reader.ReadAllAsync(cancellationToken))
			received.Add(post);

		long written = await write;
		#endregion

		return (written, received);
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
		// The query is the producer: it reads the rows in the background,
		// in chunks, and the connection is free as soon as the last row
		// is read. Nothing runs until the first read below.
		IAsyncEnumerable<Post> posts = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToBufferedAsyncStream(cancellationToken);

		// This is the consumer. Its slow work does not hold the
		// query back, because the rows are already buffered.
		await foreach (Post post in posts)
		{
			enriched.Add(await Enrich(post, cancellationToken));
		}
		#endregion

		return enriched;
	}

	public static async Task<List<string>> PipelineSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var lines = new List<string>();

		#region stream_pipeline
		// Stage 1: the query, as a producer.
		IAsyncEnumerable<Post> posts = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToBufferedAsyncStream(cancellationToken);

		// Stage 2: a transform that consumes stage 1 and is the
		// producer of a channel.
		var formatted = Channel.CreateUnbounded<string>();
		Task transform = Task.Run(
			async () =>
			{
				try
				{
					await foreach (Post post in posts)
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

	public static async Task<List<string>> BufferSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var lines = new List<string>();

		#region stream_buffer
		// Few rows, one consumer, nothing to overlap: buffer them.
		// The connection is released as soon as the list is built,
		// and the slow work happens after that.
		List<Post> posts = await connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToListAsync(cancellationToken);

		foreach (Post post in posts)
			lines.Add(await Enrich(post, cancellationToken));
		#endregion

		return lines;
	}

	public static async Task<(long Written, List<Post> Received)> ToTargetBlockSample(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region to_target_block
		// The block is yours: its scheduler and its priorities are
		// whatever you chose. The pass only sends to it.
		var stage = new TransformBlock<Post, string>(
			static post => $"#{post.Id} {post.Title}",
			new ExecutionDataflowBlockOptions { BoundedCapacity = 100 });

		var sink = new ActionBlock<string>(
			_ => { },
			new ExecutionDataflowBlockOptions { BoundedCapacity = 100 });

		stage.LinkTo(
			sink,
			new DataflowLinkOptions { PropagateCompletion = true });

		long written = await connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToTargetBlock(stage, complete: true, cancellationToken);

		await sink.Completion;
		#endregion

		// The same rows, for the check below.
		BufferBlock<Post> block = new();
		await connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToTargetBlock(block, complete: true, cancellationToken);

		while (await block.OutputAvailableAsync(cancellationToken))
			received.Add(await block.ReceiveAsync(cancellationToken));

		return (written, received);
	}

	public static async Task<List<Post>> AsSourceBlockSample(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region as_source_block
		// Returns right away: the rows are read in the background
		// and the block is completed when they are all sent.
		IReceivableSourceBlock<Post> source = connectionFactory
			.Command("select * from posts order by id")
			.Map<Post>()
			.AsSourceBlock(
				new DataflowBlockOptions { BoundedCapacity = 100 },
				cancellationToken);

		while (await source.OutputAvailableAsync(cancellationToken))
		{
			while (source.TryReceive(out Post? post))
				received.Add(post);
		}
		#endregion

		return received;
	}
}
