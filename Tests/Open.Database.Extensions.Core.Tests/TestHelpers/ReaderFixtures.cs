namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// Readers for the reader, retrieve and query tests: three columns (<c>Id</c>, <c>Name</c>, <c>Note</c>), a row count of the test's choosing,
/// and a tracked reader that counts its reads and can run code (for instance a cancel) as a given read starts.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class ReaderFixtures
{
	public static readonly ImmutableArray<ColumnDef> Columns = [("Id", typeof(int)), ("Name", typeof(string)), ("Note", typeof(string))];

	public static readonly object?[][] TwoRows = [[1, "a", "n1"], [2, null, "n2"]];

	/// <summary>Rows 1..<paramref name="count"/>: <c>Id</c> is the row number, <c>Name</c> is <c>n</c> and the number, <c>Note</c> is NULL.</summary>
	public static object?[][] Rows(int count) => [.. Enumerable.Range(1, count).Select(i => (object?[])[i, $"n{i}", null])];

	public static TypedReader Reader(params object?[][] rows) => TypedReader.Create(Columns, rows);

	/// <summary>A reader over <see cref="Rows"/>; <paramref name="onRead"/> is called with the 1-based number of each read before it is served.</summary>
	public static TrackedReader TrackedRows(int count, Action<int>? onRead = null)
	{
		var tally = new DbTally(new DbScript { Columns = Columns, Rows = Rows(count), OnRead = onRead });
		return new TrackedReader(tally, tally.NewConnection(), CommandBehavior.Default);
	}
}
