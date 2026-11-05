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
