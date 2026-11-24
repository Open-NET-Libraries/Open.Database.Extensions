namespace Open.Database.Extensions.Benchmarks;

/// <summary>
/// The streaming rungs: the rows pulled one at a time off the open reader through the library's IAsyncEnumerable terminal
/// (<c>Map(...).Stream(Timeout.InfiniteTimeSpan)</c>, never stalling), each consumed whole with <c>await foreach</c> and ConfigureAwait(false).
/// The reads use the library's default for an async terminal (no <c>AsyncReads()</c>), as V1_Async does. Each loop sums two fields (<see cref="Checksum"/>)
/// so it is not dead code and --verify can compare the result with the rows of the table; the loop body is the same for every rung.
/// </summary>
public static class SyntheticVariantsStream
{
	/// <summary>The value every rung returns for a row: Id plus Big (Big is 0 below width 11, where the row has no such column).</summary>
	public static long Checksum(int id, long big) => id + big;

	/// <summary>The checksum of the table's rows, from the oracle's rows.</summary>
	public static long Expected(List<SynRow> rows)
	{
		long sum = 0;
		foreach (var row in rows) sum += Checksum(row.Id, row.Big);
		return sum;
	}

	public static async ValueTask<long> V1(SyntheticConnection connection)
	{
		long sum = 0;
		await foreach (var row in connection.Command(SyntheticVariants.Sql).Map<SynRow>().Stream(Timeout.InfiniteTimeSpan).ConfigureAwait(false))
			sum += Checksum(row.Id, row.Big);
		return sum;
	}

	// The runtime twins: V1 on a row type that carries a NULL policy (SynRowPolicy.cs).
	public static async ValueTask<long> V1Throw(SyntheticConnection connection)
	{
		long sum = 0;
		await foreach (var row in connection.Command(SyntheticVariants.Sql).Map<SynRowThrow>().Stream(Timeout.InfiniteTimeSpan).ConfigureAwait(false))
			sum += Checksum(row.Id, row.Big);
		return sum;
	}

	public static async ValueTask<long> V1Default(SyntheticConnection connection)
	{
		long sum = 0;
		await foreach (var row in connection.Command(SyntheticVariants.Sql).Map<SynRowDefault>().Stream(Timeout.InfiniteTimeSpan).ConfigureAwait(false))
			sum += Checksum(row.Id, row.Big);
		return sum;
	}

	// The bar: the hand-written selector of V2, streamed.
	public static async ValueTask<long> V2(SyntheticConnection connection, Func<System.Data.IDataRecord, SynRow> selector)
	{
		long sum = 0;
		await foreach (var row in connection.Command(SyntheticVariants.Sql).Map(selector).Stream(Timeout.InfiniteTimeSpan).ConfigureAwait(false))
			sum += Checksum(row.Id, row.Big);
		return sum;
	}

	// For --verify only: the same four streams collected whole, so the rows can be compared member by member (the benchmark methods above return the checksum).
	public static async Task<List<T>> Collect<T>(IAsyncEnumerable<T> stream)
	{
		var list = new List<T>();
		await foreach (var row in stream.ConfigureAwait(false)) list.Add(row);
		return list;
	}
}
