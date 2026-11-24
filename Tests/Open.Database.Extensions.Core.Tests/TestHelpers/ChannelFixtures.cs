using System.Data.Common;

namespace Open.Database.Extensions.Tests;

/// <summary>
/// The queries the channel tests read: the four views that implement
/// <see cref="IMappedQuery{T}"/>, over a tallying provider.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class ChannelFixtures
{
	public const string ViewQuery = "query";
	public const string ViewSkip = "skip";
	public const string ViewTake = "take";
	public const string ViewCommand = "command";

	public static TheoryData<string> ViewNames
		=> [ViewQuery, ViewSkip, ViewTake, ViewCommand];

	public static DbTally Tally(
		int rows,
		int failOnRead = 0,
		Action<int>? onRead = null)
		=> new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, rows).Select(i => new object?[] { i, "n" + i })],
			FailOnRead = failOnRead,
			OnRead = onRead,
		});

	/// <summary>
	/// A query over a factory connection: the library opens it and closes it.
	/// </summary>
	public static MappedQuery<ChannelRow> Read(DbTally tally)
		=> tally.Factory.Command("select").Map<ChannelRow>();

	/// <summary>
	/// One of the four views over the same script.
	/// </summary>
	public static IMappedQuery<ChannelRow> View(string name, DbTally tally)
		=> name switch
		{
			ViewQuery => Read(tally),
			ViewSkip => Read(tally).Skip(0),
			ViewTake => Read(tally).Take(int.MaxValue),
			_ => Cmd(tally),
		};

	// A command over a closed connection the caller made:
	// the library opens it for the read and closes it again.
	static MappedCommand<ChannelRow> Cmd(DbTally tally)
	{
		DbCommand command = tally.NewConnection().CreateCommand();
		command.CommandText = "select";
		return command.Map<ChannelRow>();
	}

	public static Channel<ChannelRow> Unbounded()
		=> Channel.CreateUnbounded<ChannelRow>();

	public static Channel<ChannelRow> Bounded(int capacity)
		=> Channel.CreateBounded<ChannelRow>(capacity);

	public static async Task<List<int>> Drain(
		ChannelReader<ChannelRow> reader,
		int count)
	{
		List<int> ids = [];
		while (ids.Count < count)
		{
			ids.Add((await reader
				.ReadAsync(TestContext.Current.CancellationToken)
				.AsTask()
				.Within()).Id);
		}

		return ids;
	}

	/// <summary>
	/// A source that is not ours: the feeds work on any implementation of
	/// the interface.
	/// </summary>
	public sealed class FakeSource(
		IReadOnlyList<ChannelRow> rows,
		Func<TimeSpan, CancellationToken, IAsyncEnumerable<ChannelRow>>? stream = null)
		: IMappedQuery<ChannelRow>
	{
		public TimeSpan? StallTimeout { get; private set; }
		public CancellationToken StreamToken { get; private set; }
		public int ChunkStreams { get; private set; }

		public IAsyncEnumerable<ReadOnlyMemory<ChannelRow>> ToAsyncBufferStream(
			CancellationToken cancellationToken = default)
		{
			++ChunkStreams;
			return Chunks();
		}

		async IAsyncEnumerable<ReadOnlyMemory<ChannelRow>> Chunks()
		{
			ChannelRow[] all = [.. rows];
			for (int i = 0; i < all.Length; i += 4)
			{
				await Task.Yield();
				yield return all.AsMemory(i, Math.Min(4, all.Length - i));
			}
		}

		public IAsyncEnumerable<ChannelRow> Stream(
			TimeSpan stallTimeout,
			CancellationToken cancellationToken = default)
		{
			StallTimeout = stallTimeout;
			StreamToken = cancellationToken;
			return stream is null
				? Rows()
				: stream(stallTimeout, cancellationToken);
		}

		public IAsyncEnumerable<ChannelRow> StreamBuffered(
			TimeSpan stallTimeout,
			CancellationToken cancellationToken = default)
			=> Stream(stallTimeout, cancellationToken);

		public IAsyncEnumerable<ChannelRow> StreamBuffered(
			TimeSpan stallTimeout,
			int bufferSize,
			CancellationToken cancellationToken = default)
			=> Stream(stallTimeout, cancellationToken);

		async IAsyncEnumerable<ChannelRow> Rows()
		{
			foreach (ChannelRow row in rows)
			{
				await Task.Yield();
				yield return row;
			}
		}
	}

	// A ValueTask handed to a method is used: for a call that must throw.
	public static void Drop(ValueTask<long> task)
		=> GC.KeepAlive(task);

	public static List<ChannelRow> Rows(int count)
		=> [.. Enumerable.Range(1, count).Select(static i => new ChannelRow(i))];
}
