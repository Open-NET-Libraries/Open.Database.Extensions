#nullable enable

namespace Open.Database.Extensions.Tests;

// Reading one column through the command's terminal (cmd.Map<int>().ToArray(),
// Map<int>()) costs what the hand-written selector costs:
// the same bytes per call, so nothing is boxed per row on the typed path.
// Each measurement compares the two in the same run,
// so the budget follows the runtime and the configuration.
[ExcludeFromCodeCoverage]
public static class SingleValueAllocationTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// Bytes per call: after a warm-up,
	// the smallest of three rounds of 1,000 calls
	// (the allocated-bytes counter of this thread is exact,
	// so only a stray JIT or GC adds noise).
	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 100; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 1000; i++) call();
			best = Math.Min(
				best,
				(GC.GetAllocatedBytesForCurrentThread() - before) / 1000);
		}

		return best;
	}

	// What the hand-written version may cost beyond the measured ideal:
	// the two differ by no more than a rounding of the measurement.
	const int Slack = 16;

	// A DataTableReader is a DbDataReader that reports its column type
	// and has typed getters,
	// so the typed (unboxed) path runs.
	static PlainReaderCommand Rows(
		Type column,
		int count,
		Func<int, object> value)
	{
		var table = new DataTable();
		table.Columns.Add("v", column);
		table.Columns.Add("other", typeof(string));
		for (int i = 0; i < count; i++) table.Rows.Add(value(i), "x");
		return new PlainReaderCommand(() => new DataTableReader(table));
	}

	static void NoMoreThanByHand(
		Action library,
		Action byHand,
		string what)
	{
		long mine = BytesPerCall(library);
		long theirs = BytesPerCall(byHand);
		Assert.True(
			mine <= theirs + Slack,
			$"{what}: {mine} B per call through the terminal, {theirs} B by hand (allowed {Slack} more).");
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void ToArrayAndToListOfInt_AllocateNoMoreThanASelector(
		int rows)
	{
		PlainReaderCommand cmd = Rows(typeof(int), rows, i => i);

		NoMoreThanByHand(
			() => cmd.Map<int>().ToArray(),
			() => cmd.Map(static r => r.GetInt32(0)).ToArray(),
			$"ToArray<int> over {rows} rows");
		NoMoreThanByHand(
			() => cmd.Map<int>().ToList(),
			() => cmd.Map(static r => r.GetInt32(0)).ToList(),
			$"ToList<int> over {rows} rows");
		NoMoreThanByHand(
			() => cmd.Map<int>().ToList(),
			() => cmd.Map(static r => r.GetInt32(0)).ToList(),
			$"Map<int>().ToList() over {rows} rows");
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void ALongColumnIntoInt_AllocatesNoMoreThanASelector(int rows)
	{
		PlainReaderCommand cmd = Rows(typeof(long), rows, i => (long)i);

		NoMoreThanByHand(
			() => cmd.Map<int>().ToList(),
			() => cmd.Map(static r => checked((int)r.GetInt64(0))).ToList(),
			$"ToList<int> over a long column, {rows} rows");
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void ToListOfNullableIntAndString_AllocateNoMoreThanASelector(
		int rows)
	{
		PlainReaderCommand ints = Rows(typeof(int), rows, i => i);
		NoMoreThanByHand(
			() => ints.Map<int?>().ToList(),
			() => ints
				.Map(static r => r.IsDBNull(0) ? (int?)null : r.GetInt32(0))
				.ToList(),
			$"ToList<int?> over {rows} rows");

		PlainReaderCommand texts = Rows(typeof(string), rows, i => "t");
		NoMoreThanByHand(
			() => texts.Map<string>().ToList(),
			() => texts
				.Map(static r => r.IsDBNull(0) ? null : r.GetString(0))
				.ToList(),
			$"ToList<string> over {rows} rows");
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void ToListAsyncOfInt_AllocatesNoMoreThanASelector(int rows)
	{
		PlainReaderCommand cmd = Rows(typeof(int), rows, i => i);

		NoMoreThanByHand(
			() => cmd.ToListAsync<int>(Ct).AsTask().GetAwaiter().GetResult(),
			() => cmd
				.Map(static r => r.GetInt32(0))
				.ToListAsync(Ct)
				.AsTask()
				.GetAwaiter()
				.GetResult(),
			$"ToListAsync<int> over {rows} rows");
	}
}
