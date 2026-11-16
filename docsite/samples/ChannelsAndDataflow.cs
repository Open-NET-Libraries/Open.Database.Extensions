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

	public static async Task<List<Post>> StreamDefaultSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region stream_default
		// The defaults: an unbounded channel, and a read that is
		// deferred. Nothing runs until the first read below.
		ChannelReader<Post> reader = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannelReader(cancellationToken);

		await foreach (Post post in reader.ReadAllAsync(cancellationToken))
			received.Add(post);
		#endregion

		return received;
	}

	public static async Task<List<Post>> StreamChainedSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();
		var query = connection.Command("select * from posts order by id");

		#region stream_chained
		// One chain: map the rows, stream them through a channel,
		// and iterate. The channel is unbounded, so the read
		// finishes at full speed however slow the loop body is.
		await foreach (var post in query
			.Map<Post>()
			.ToChannelReader()
			.ReadAllAsync(cancellationToken))
		{
			received.Add(post);
		}
		#endregion

		return received;
	}

	public static async Task<List<Post>> StreamDirectSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var query = connection.Command("select * from posts order by id");

		#region stream_direct
		// The same rows by iterating the query directly: the
		// connection stays open for as long as the loop runs.
		List<Post> received = await query.Map<Post>().ReadAsync(
			static async (rows, token) =>
			{
				var list = new List<Post>();
				await foreach (Post post in rows.WithCancellation(token))
					list.Add(post);

				return list;
			},
			cancellationToken);
		#endregion

		return received;
	}

	public static async Task<List<Post>> StreamBoundedSample(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region stream_bounded
		// A bound caps the rows held in memory: the read waits for
		// room, so the reader (and the connection) stays open until
		// the consumer has caught up.
		// startImmediately: true runs the query at the call
		// instead of at the first read.
		ChannelReader<Post> reader = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannelReader(
				new BoundedChannelOptions(2) { SingleReader = true },
				startImmediately: true,
				cancellationToken);

		await foreach (Post post in reader.ReadAllAsync(cancellationToken))
			received.Add(post);
		#endregion

		return received;
	}

	public static async Task<(long Count, List<Post> Received)>
		StreamCountSample(
			SqliteConnection connection,
			CancellationToken cancellationToken)
	{
		var received = new List<Post>();

		#region stream_count
		// ToChannel hands you both ends of the channel; ODX still
		// completes it. The task says how many rows were written.
		var channel = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToChannel(out var rows, cancellationToken);

		await foreach (Post post in channel.Reader
			.ReadAllAsync(cancellationToken))
		{
			received.Add(post);
		}

		long count = await rows;
		#endregion

		return (count, received);
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
