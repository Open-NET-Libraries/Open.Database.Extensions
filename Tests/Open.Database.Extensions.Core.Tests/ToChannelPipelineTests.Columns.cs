using System.Data.Common;
using System.Threading.Channels;
using BoundedChannel = System.Threading.Channels.Channel;
using Kind = Open.Database.Extensions.Tests.ToChannelMappedCompletionTests.Kind;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The mapped overloads must pick the mapped columns out of a reader that has other columns too, wherever those are.
public static partial class ToChannelPipelineTests
{
	sealed class Wide
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	static readonly (string Field, string? Column)[] WideOverrides = [("Id", "Id"), ("Name", "Name")];

	static readonly string[][] Layouts =
	[
		["Extra", "Id", "Name"], // An unmapped column before.
		["Id", "Extra", "Name"], // Between.
		["Id", "Name", "Extra"], // After.
		["Id", "Name"], // Nothing unmapped.
		["A", "Id", "B", "Name", "C"], // All around.
		["Name", "Id"], // Not in the order of the properties.
		["Name", "Extra", "Id"], // Not in the order of the properties, with something in between.
	];

	public static TheoryData<Kind, bool, int> WideLayouts { get; } = BuildWideLayouts();

	static TheoryData<Kind, bool, int> BuildWideLayouts()
	{
		var data = new TheoryData<Kind, bool, int>();
		foreach (Kind kind in Enum.GetValues<Kind>())
			foreach (bool overrides in new[] { false, true })
				for (int layout = 0; layout < Layouts.Length; layout++)
					data.Add(kind, overrides, layout);

		return data;
	}

	public static TheoryData<bool, int> WideReaderLevelLayouts { get; } = BuildWideReaderLevelLayouts();

	static TheoryData<bool, int> BuildWideReaderLevelLayouts()
	{
		var data = new TheoryData<bool, int>();
		foreach (bool useAsync in new[] { false, true })
			for (int layout = 0; layout < Layouts.Length; layout++)
				data.Add(useAsync, layout);

		return data;
	}

	static object?[][] WideRows(string[] layout, int count)
	{
		var rows = new object?[count][];
		for (int i = 0; i < count; i++)
			rows[i] = [.. layout.Select(column => column switch { "Id" => (object?)i, "Name" => "n" + i, _ => "junk-" + column })];

		return rows;
	}

	static void AssertWide(IReadOnlyList<Wide> rows, int expectedCount)
	{
		Assert.Equal(expectedCount, rows.Count);
		for (int i = 0; i < rows.Count; i++)
		{
			Assert.Equal(i, rows[i].Id);
			Assert.Equal("n" + i, rows[i].Name);
		}
	}

	[Theory]
	[MemberData(nameof(WideLayouts))]
	public static async Task Mapped_ColumnsThatAreNotMapped_DoNotShiftTheOnesThatAre(Kind kind, bool overrides, int layoutIndex)
	{
		const int Count = 50;
		CancellationToken ct = TestContext.Current.CancellationToken;
		string[] layout = Layouts[layoutIndex];
		var probe = new Probe();
		var channel = BoundedChannel.CreateBounded<Wide>(TargetCapacity);
		var call = Mapped<Wide>(kind, overrides, probe.Factory(WideRows(layout, Count), columns: layout), ct, mapping: WideOverrides);

		Task<(List<Wide> Items, Exception? Error)> consumer = Task.Run(() => Drain(channel.Reader, ct), ct);
		long produced = await call(channel.Writer, true).AsTask().WaitAsync(Timeout, ct);
		(List<Wide> items, Exception? error) = await consumer.WaitAsync(Timeout, ct);

		Assert.Null(error);
		Assert.Equal(Count, produced);
		AssertWide(items, Count);
	}

	[Theory]
	[MemberData(nameof(WideReaderLevelLayouts))]
	public static async Task ReaderLevel_ColumnsThatAreNotMapped_DoNotShiftTheOnesThatAre(bool useAsync, int layoutIndex)
	{
		const int Count = 50;
		CancellationToken ct = TestContext.Current.CancellationToken;
		string[] layout = Layouts[layoutIndex];
		var channel = BoundedChannel.CreateBounded<Wide>(TargetCapacity);
		SpyReader reader = (SpyReader)new Probe().Factory(WideRows(layout, Count), columns: layout)();

		Task<(List<Wide> Items, Exception? Error)> consumer = Task.Run(() => Drain(channel.Reader, ct), ct);
		long produced = useAsync
			? await reader.ToChannelAsync<Wide>(channel.Writer, true, ct).AsTask().WaitAsync(Timeout, ct)
			: await reader.ToChannel<Wide>(channel.Writer, true, ct).AsTask().WaitAsync(Timeout, ct);
		(List<Wide> items, Exception? error) = await consumer.WaitAsync(Timeout, ct);

		Assert.Null(error);
		Assert.Equal(Count, produced);
		AssertWide(items, Count);
	}
}
