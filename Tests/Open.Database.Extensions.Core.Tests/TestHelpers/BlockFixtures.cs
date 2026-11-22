using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.Tests;

/// <summary>
/// The blocks the Dataflow tests send to, and how they read them.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class BlockFixtures
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public static BufferBlock<ChannelRow> NewBlock()
		=> new();

	public static BufferBlock<ChannelRow> NewBounded(int capacity)
		=> new(new DataflowBlockOptions { BoundedCapacity = capacity });

	/// <summary>
	/// Receives exactly <paramref name="count"/> rows.
	/// </summary>
	public static async Task<List<int>> Receive(
		IReceivableSourceBlock<ChannelRow> block,
		int count)
	{
		List<int> ids = [];
		while (ids.Count < count)
		{
			ids.Add((await block
				.ReceiveAsync(Ct)
				.Within()).Id);
		}

		return ids;
	}

	/// <summary>
	/// Receives until the block is finished: the ids of the rows, and the
	/// exception the block ended with, if it did.
	/// </summary>
	public static async Task<(List<int> Rows, Exception? Error)> Collect(
		IReceivableSourceBlock<ChannelRow> block)
	{
		List<int> rows = [];
		try
		{
			while (await block.OutputAvailableAsync(Ct).Within())
			{
				while (block.TryReceive(out ChannelRow? row))
					rows.Add(row.Id);
			}

			await block.Completion.Within();
			return (rows, null);
		}
		catch (Exception ex)
		{
			return (rows, ex);
		}
	}

	/// <summary>
	/// <see langword="true"/> when the block takes no more rows.
	/// </summary>
	public static bool IsClosed(ITargetBlock<ChannelRow> block)
		=> !block.Post(new ChannelRow(-1));

	public static void Fault(IDataflowBlock block, Exception error)
		=> block.Fault(error);
}
