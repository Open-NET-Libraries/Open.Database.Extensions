using System.Data.Common;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The library never moves a reader to the next result set: that is for the consumer to decide, because a next result is not always the same shape.
// Every code path that runs a command must leave the reader's NextResult and NextResultAsync uncalled.
[ExcludeFromCodeCoverage]
public static class NeverNextResultTests
{
	sealed class Row
	{
		public int Id { get; set; }
	}

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static DbTally Tally(int rows)
		=> new(new DbScript { Rows = [.. Enumerable.Range(1, rows).Select(i => (object?[])[i, "n" + i])] });

	static ExpressiveDbCommand Expressive(DbTally tally) => tally.NewConnection().Command("select 1");

	static DbCommand Db(DbTally tally) => tally.NewConnection().CreateCommand();

	static void AssertNeverMoved(DbTally tally)
	{
		TrackedReader reader = Assert.Single(tally.Readers);
		Assert.Equal(0, reader.NextResultCalls);
		Assert.Equal(0, reader.NextResultAsyncCalls);
	}

	// ---- Map<T>() ----

	[Fact]
	public static void Map_ToList_Sync()
	{
		var tally = Tally(3);
		Assert.Equal(3, Expressive(tally).Map<Row>().ToList().Count);
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task Map_ToList_Async()
	{
		var tally = Tally(3);
		Assert.Equal(3, (await Expressive(tally).Map<Row>().ToListAsync(Ct)).Count);
		AssertNeverMoved(tally);
	}

	[Fact]
	public static void Map_FirstOrDefault()
	{
		var tally = Tally(3);
		Assert.Equal(1, Expressive(tally).Map<Row>().FirstOrDefault()?.Id);
		Assert.Equal(1, Assert.Single(tally.Readers).Reads);
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task Map_FirstOrDefault_Async()
	{
		var tally = Tally(3);
		Assert.Equal(1, (await Expressive(tally).Map<Row>().FirstOrDefaultAsync(Ct))?.Id);
		Assert.Equal(1, Assert.Single(tally.Readers).Reads);
		AssertNeverMoved(tally);
	}

	[Fact]
	public static void Map_Take_StopsExactlyAtTheEnd()
	{
		var tally = Tally(3);
		Assert.Equal(3, Expressive(tally).Map<Row>().Take(3).ToList().Count);
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task Map_Take_StopsExactlyAtTheEnd_Async()
	{
		var tally = Tally(3);
		Assert.Equal(3, (await Expressive(tally).Map<Row>().Take(3).ToListAsync(Ct)).Count);
		AssertNeverMoved(tally);
	}

	// ---- Results and Retrieve ----

	[Fact]
	public static void Results_Sync()
	{
		var tally = Tally(2);
		Assert.Equal(2, Db(tally).Results<Row>().Count());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task Results_Async()
	{
		var tally = Tally(2);
		Assert.Equal(2, (await Db(tally).ResultsAsync<Row>(cancellationToken: Ct)).Count());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static void Results_OfAnExpressiveCommand()
	{
		var tally = Tally(2);
		Assert.Equal(2, Expressive(tally).Results<Row>().Count());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task Results_OfAnExpressiveCommand_Async()
	{
		var tally = Tally(2);
		Assert.Equal(2, (await Expressive(tally).ResultsAsync<Row>()).Count());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static void Retrieve_Sync()
	{
		var tally = Tally(2);
		Assert.Equal(2, Db(tally).Retrieve().Result.Count);
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task Retrieve_Async()
	{
		var tally = Tally(2);
		Assert.Equal(2, (await Db(tally).RetrieveAsync(cancellationToken: Ct)).Result.Count);
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task Retrieve_OfAnExpressiveCommand_Async()
	{
		var tally = Tally(2);
		Assert.Equal(2, (await Expressive(tally).RetrieveAsync()).Result.Count);
		AssertNeverMoved(tally);
	}

	// ---- ToChannel on a command ----

	static async Task ToChannel_Check(DbTally tally, Func<ChannelWriter<object[]>, ValueTask<long>> call, int rows)
	{
		var channel = System.Threading.Channels.Channel.CreateUnbounded<object[]>();
		Assert.Equal(rows, await call(channel.Writer).AsTask());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static Task ToChannel_Sync_Plain()
	{
		var tally = Tally(3);
		return ToChannel_Check(tally, w => Db(tally).ToChannel(w, true, Ct), 3);
	}

	[Fact]
	public static async Task ToChannel_Sync_WithATransform()
	{
		var tally = Tally(3);
		var channel = System.Threading.Channels.Channel.CreateUnbounded<int>();
		Assert.Equal(3, await Db(tally).ToChannel(channel.Writer, true, r => r.GetInt32(0), Ct).AsTask());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static Task ToChannel_Async_Plain()
	{
		var tally = Tally(3);
		return ToChannel_Check(tally, w => Db(tally).ToChannelAsync(w, true, Ct), 3);
	}

	[Fact]
	public static async Task ToChannel_Async_WithATransform()
	{
		var tally = Tally(3);
		var channel = System.Threading.Channels.Channel.CreateUnbounded<int>();
		Assert.Equal(3, await Db(tally).ToChannelAsync(channel.Writer, true, r => r.GetInt32(0), Ct).AsTask());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task ToChannel_Mapped_Sync()
	{
		var tally = Tally(3);
		var channel = System.Threading.Channels.Channel.CreateUnbounded<Row>();
		Assert.Equal(3, await Expressive(tally).ToChannel<Row>(channel.Writer, true).AsTask());
		AssertNeverMoved(tally);
	}

	[Fact]
	public static async Task ToChannel_Mapped_Async()
	{
		var tally = Tally(3);
		var channel = System.Threading.Channels.Channel.CreateUnbounded<Row>();
		Assert.Equal(3, await Db(tally).ToChannelAsync<Row>(channel.Writer, true, Ct).AsTask());
		AssertNeverMoved(tally);
	}

	// ---- ToTargetBlock on a command (Dataflow) ----

	[Fact]
	public static void ToTargetBlock_OnACommand()
	{
		var tally = Tally(3);
		var target = new BufferBlock<object[]>();
		Assert.Equal(3, Db(tally).ToTargetBlock(target, true));
		Assert.Equal(3, target.Count);
		AssertNeverMoved(tally);
	}

	// ---- several result sets: the caller maps one, calls NextResult itself, and maps the next ----

	sealed class A
	{
		public int Id { get; set; }
	}

	sealed class B
	{
		public string Name { get; set; } = "";
		public int Qty { get; set; }
	}

	/// <summary>A reader over two result sets of different shapes; <see cref="TypedReader.NextResult"/> moves to the second.</summary>
	sealed class TwoSetReader : TypedReader
	{
		readonly TypedReader[] _sets =
		[
			Create([("Id", typeof(int))], [1], [2]),
			Create([("Name", typeof(string)), ("Qty", typeof(int))], ["x", 10], ["y", 20], ["z", 30]),
		];

		int _current;

		public TwoSetReader() : base([], []) { }

		TypedReader Set => _sets[_current];

		public override bool Read() => Set.Read();
		public override int FieldCount => Set.FieldCount;
		public override string GetName(int ordinal) => Set.GetName(ordinal);
		public override Type GetFieldType(int ordinal) => Set.GetFieldType(ordinal);
		public override string GetDataTypeName(int ordinal) => Set.GetDataTypeName(ordinal);
		public override int GetOrdinal(string name) => Set.GetOrdinal(name);
		public override bool IsDBNull(int ordinal) => Set.IsDBNull(ordinal);
		public override object GetValue(int ordinal) => Set.GetValue(ordinal);
		public override int GetInt32(int ordinal) => Set.GetInt32(ordinal);
		public override string GetString(int ordinal) => Set.GetString(ordinal);

		public override bool NextResult()
		{
			base.NextResult();
			if (_current + 1 >= _sets.Length) return false;
			_current++;
			return true;
		}
	}

	[Fact]
	public static void SeveralResultSets_Sync()
	{
		var reader = new TwoSetReader();

		List<A> first = reader.Results<A>().ToList();
		Assert.Equal([1, 2], first.Select(a => a.Id));
		Assert.Equal(0, reader.NextResultCalls);

		Assert.True(reader.NextResult());
		List<B> second = reader.Results<B>().ToList();
		Assert.Equal(["x", "y", "z"], second.Select(b => b.Name));
		Assert.Equal([10, 20, 30], second.Select(b => b.Qty));

		Assert.Equal(1, reader.NextResultCalls);
		Assert.Equal(0, reader.NextResultAsyncCalls);
		Assert.False(reader.NextResult());
	}

	[Fact]
	public static async Task SeveralResultSets_Async()
	{
		var reader = new TwoSetReader();

		List<A> first = (await reader.ResultsBufferedAsync<A>(cancellationToken: Ct)).ToList();
		Assert.Equal([1, 2], first.Select(a => a.Id));
		Assert.Equal(0, reader.NextResultCalls);
		Assert.Equal(0, reader.NextResultAsyncCalls);

		Assert.True(await reader.NextResultAsync(Ct));
		List<B> second = (await reader.ResultsBufferedAsync<B>(cancellationToken: Ct)).ToList();
		Assert.Equal(["x", "y", "z"], second.Select(b => b.Name));
		Assert.Equal([10, 20, 30], second.Select(b => b.Qty));

		// Only the caller's own call: the async one, which the base class runs through the sync one.
		Assert.Equal(1, reader.NextResultAsyncCalls);
		Assert.Equal(1, reader.NextResultCalls);
	}
}
