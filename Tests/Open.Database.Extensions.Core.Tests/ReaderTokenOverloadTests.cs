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

#pragma warning restore xUnit1051
}
