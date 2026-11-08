using Simple = Open.Database.Extensions.Tests.RowMapperTests.Simple;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Every call shape on a DbDataReader for ResultsAsync<T> and ResultsBufferedAsync<T>, including the token-only ones.
// The first job is that each shape compiles without ambiguity; the second is that it binds to the overload we expect,
// which shows in behavior: the map is applied, and a cancelled token cancels.
[ExcludeFromCodeCoverage]
public static class ReaderTokenOverloadTests
{
	static readonly int[] Expected = [1, 2];

	static readonly RowMap<Simple> Renames = new([new(nameof(Simple.Id), "item_id"), new(nameof(Simple.Name), "item_name")]);

	// Columns match the property names: read by name, no map needed.
	static TypedReader Named()
		=> TypedReader.Create([("Id", typeof(int)), ("Name", typeof(string))], [1, "a"], [2, "b"]);

	// Columns do not match the property names: only a map finds them.
	static TypedReader Renamed()
		=> TypedReader.Create([("item_id", typeof(int)), ("item_name", typeof(string))], [1, "a"], [2, "b"]);

	static CancellationTokenSource Cancelled()
	{
		var cts = new CancellationTokenSource();
		cts.Cancel();
		return cts;
	}

	static async Task<int[]> Ids(IAsyncEnumerable<Simple> items, CancellationToken cancellationToken)
	{
		var ids = new List<int>();
		await foreach (Simple s in items.WithCancellation(cancellationToken)) ids.Add(s.Id);
		return [.. ids];
	}

	static int[] Ids(IEnumerable<Simple> items) => [.. items.Select(s => s.Id)];

	// ResultsAsync<T> (streaming)

#pragma warning disable xUnit1051 // The point is to call the shapes that take no token or an explicit one.

	[Fact]
	public static async Task ResultsAsync_NoArguments()
	{
		await using var reader = Named();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync<Simple>(), TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ResultsAsync_Null()
	{
		await using var reader = Named();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync<Simple>(null), TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ResultsAsync_Default()
	{
		await using var reader = Named();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync<Simple>(default), TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ResultsAsync_Map()
	{
		await using var reader = Renamed();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync(Renames), TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ResultsAsync_NoneToken()
	{
		await using var reader = Named();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync<Simple>(CancellationToken.None), TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ResultsAsync_TokenOnly_ReachesTheReader()
	{
		await using var reader = Named();
		using var live = new CancellationTokenSource();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync<Simple>(live.Token), TestContext.Current.CancellationToken));

		await using var other = Named();
		using var cancelled = Cancelled();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await Ids(other.ResultsAsync<Simple>(cancelled.Token), TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ResultsAsync_MapAndToken()
	{
		await using var reader = Renamed();
		using var live = new CancellationTokenSource();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync(Renames, live.Token), TestContext.Current.CancellationToken));

		await using var other = Renamed();
		using var cancelled = Cancelled();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await Ids(other.ResultsAsync(Renames, cancelled.Token), TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ResultsAsync_NullMapAndToken()
	{
		await using var reader = Named();
		using var live = new CancellationTokenSource();
		Assert.Equal(Expected, await Ids(reader.ResultsAsync<Simple>(null, live.Token), TestContext.Current.CancellationToken));

		await using var other = Named();
		using var cancelled = Cancelled();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await Ids(other.ResultsAsync<Simple>(null, cancelled.Token), TestContext.Current.CancellationToken));
	}

	// ResultsBufferedAsync<T> (buffered)

	[Fact]
	public static async Task ResultsBufferedAsync_NoArguments()
	{
		await using var reader = Named();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync<Simple>()));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_Null()
	{
		await using var reader = Named();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync<Simple>(null)));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_Default()
	{
		await using var reader = Named();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync<Simple>(default)));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_Map()
	{
		await using var reader = Renamed();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync(Renames)));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_NoneToken()
	{
		await using var reader = Named();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync<Simple>(CancellationToken.None)));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_TokenOnly_ReachesTheReader()
	{
		await using var reader = Named();
		using var live = new CancellationTokenSource();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync<Simple>(live.Token)));

		await using var other = Named();
		using var cancelled = Cancelled();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await other.ResultsBufferedAsync<Simple>(cancelled.Token));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_MapAndToken()
	{
		await using var reader = Renamed();
		using var live = new CancellationTokenSource();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync(Renames, live.Token)));

		await using var other = Renamed();
		using var cancelled = Cancelled();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await other.ResultsBufferedAsync(Renames, cancelled.Token));
	}

	[Fact]
	public static async Task ResultsBufferedAsync_NullMapAndToken()
	{
		await using var reader = Named();
		using var live = new CancellationTokenSource();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync<Simple>(null, live.Token)));

		await using var other = Named();
		using var cancelled = Cancelled();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await other.ResultsBufferedAsync<Simple>(null, cancelled.Token));
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static async Task ResultsBufferedAsync_MapReadAsyncAndToken(bool useReadAsync)
	{
		await using var reader = Renamed();
		using var live = new CancellationTokenSource();
		Assert.Equal(Expected, Ids(await reader.ResultsBufferedAsync(Renames, useReadAsync, live.Token)));

		await using var other = Renamed();
		using var cancelled = Cancelled();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await other.ResultsBufferedAsync(Renames, useReadAsync, cancelled.Token));
	}

#pragma warning restore xUnit1051
}
