using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// T10: Channel and Dataflow pick the columns to buffer with the same match a row read uses (MatchingColumns), and then map the buffered rows.
// A column that feeds only a constructor parameter must be kept, an unmapped column dropped, wherever the columns are.
[ExcludeFromCodeCoverage]
public static class ConstructorBindingPipelineTests
{
	// Both members are constructor parameters: no property of the record is ever set.
	public sealed record Entry(int Id, string Name);

	static readonly string[][] Layouts =
	[
		["Id", "Name"],
		["extra", "Id", "Name"],       // an unmapped column before
		["Id", "extra", "Name"],       // between
		["Id", "Name", "extra"],       // after
		["Name", "Id"],                // not in the order of the parameters
		["a", "Name", "b", "Id", "c"], // all around, reversed
	];

	public static TheoryData<int> LayoutIndexes => [.. Enumerable.Range(0, Layouts.Length)];

	const int Count = 40;

	static ImmutableArray<ColumnDef> Columns(string[] layout, string id = "Id", string name = "Name")
		=> [.. layout.Select(c => c == "Id" ? new ColumnDef(id, typeof(int)) : c == "Name" ? new ColumnDef(name, typeof(string)) : new ColumnDef(c, typeof(string)))];

	static object?[][] Rows(ImmutableArray<ColumnDef> columns)
		=> [.. Enumerable.Range(1, Count).Select(i => (object?[])[.. columns.Select(c => c.Type == typeof(int) ? (object)i : c.Name == "Name" || c.Name.EndsWith("name", StringComparison.Ordinal) ? "n" + i : "junk")])];

	static TypedReader Reader(ImmutableArray<ColumnDef> columns) => TypedReader.Create(columns, Rows(columns));

	static void AssertEntries(List<Entry> items)
	{
		Assert.Equal(Count, items.Count);
		for (int i = 0; i < Count; i++)
			Assert.Equal(new Entry(i + 1, "n" + (i + 1)), items[i]);
	}

	static async Task<List<Entry>> Drain(ChannelReader<Entry> reader, CancellationToken ct)
	{
		var items = new List<Entry>();
		await foreach (Entry item in reader.ReadAllAsync(ct).ConfigureAwait(false)) items.Add(item);
		return items;
	}

	// A source block completes only once everything it holds has been received, and the pipelines map on a second task, so the rows are taken as they become available.
	static async Task<List<Entry>> Received(IReceivableSourceBlock<Entry> block, CancellationToken ct)
	{
		var items = new List<Entry>();
		while (await block.OutputAvailableAsync(ct).WaitAsync(Timeout, ct))
		{
			while (block.TryReceive(out Entry? item)) items.Add(item);
		}

		return items;
	}

	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

	// The map renames the parameter Name to a column called full_name; Id still maps by name.
	static readonly RowMap<Entry> NameMap = new([new(nameof(Entry.Name), "full_name")]);

	[Theory]
	[MemberData(nameof(LayoutIndexes))]
	public static async Task ToChannel_KeepsTheColumnsThatFeedOnlyConstructorParameters(int layout)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		ImmutableArray<ColumnDef> columns = Columns(Layouts[layout]);

		AssertEntries(await Drain(Reader(columns).ToChannel<Entry>(true, ct), ct).WaitAsync(Timeout, ct));
		AssertEntries(await Drain(new FakeExecuteReader(() => Reader(columns)).ToChannel<Entry>(true, cancellationToken: TestContext.Current.CancellationToken), ct).WaitAsync(Timeout, ct));
	}

	[Theory]
	[MemberData(nameof(LayoutIndexes))]
	public static async Task ToChannelAsync_KeepsTheColumnsThatFeedOnlyConstructorParameters(int layout)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		ImmutableArray<ColumnDef> columns = Columns(Layouts[layout]);

		AssertEntries(await Drain(Reader(columns).ToChannelAsync<Entry>(true, ct), ct).WaitAsync(Timeout, ct));
	}

	[Theory]
	[MemberData(nameof(LayoutIndexes))]
	public static async Task ToChannel_WithAMapThatNamesAParameter_KeepsItsColumn(int layout)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		ImmutableArray<ColumnDef> columns = Columns(Layouts[layout], name: "full_name");

		AssertEntries(await Drain(Reader(columns).ToChannel(true, NameMap, ct), ct).WaitAsync(Timeout, ct));
		AssertEntries(await Drain(Reader(columns).ToChannelAsync(true, NameMap, ct), ct).WaitAsync(Timeout, ct));
	}

	[Theory]
	[MemberData(nameof(LayoutIndexes))]
	public static async Task ToTargetBlock_KeepsTheColumnsThatFeedOnlyConstructorParameters(int layout)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		ImmutableArray<ColumnDef> columns = Columns(Layouts[layout]);

		var sync = new BufferBlock<Entry>();
		Assert.Equal(Count, Reader(columns).ToTargetBlock(sync, true));
		AssertEntries(await Received(sync, ct));

		var async = new BufferBlock<Entry>();
		Assert.Equal(Count, await Reader(columns).ToTargetBlockAsync(async, true, ct).AsTask().WaitAsync(Timeout, ct));
		AssertEntries(await Received(async, ct));

		var mapped = new BufferBlock<Entry>();
		Assert.Equal(Count, Reader(Columns(Layouts[layout], name: "full_name")).ToTargetBlock(mapped, true, NameMap));
		AssertEntries(await Received(mapped, ct));
	}

	[Theory]
	[MemberData(nameof(LayoutIndexes))]
	public static async Task AsSourceBlock_KeepsTheColumnsThatFeedOnlyConstructorParameters(int layout)
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		ImmutableArray<ColumnDef> columns = Columns(Layouts[layout]);

		AssertEntries(await Received(Reader(columns).AsSourceBlock<Entry>(), ct));
		AssertEntries(await Received(Reader(columns).AsSourceBlockAsync<Entry>(cancellationToken: ct), ct));
	}

	// A column nothing feeds is not buffered, so a layout of many unmapped columns costs the pipeline nothing: it also still builds the record from what is left.
	[Fact]
	public static async Task AResultWithNoColumnTheRecordUses_StillBuildsTheRecordFromItsFallbacks()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		ImmutableArray<ColumnDef> columns = [("junk", typeof(string))];

		List<Entry> items = await Drain(TypedReader.Create(columns, ["a"], ["b"]).ToChannel<Entry>(true, ct), ct).WaitAsync(Timeout, ct);
		Assert.Equal<Entry>([new(0, null!), new(0, null!)], items);
	}
}
