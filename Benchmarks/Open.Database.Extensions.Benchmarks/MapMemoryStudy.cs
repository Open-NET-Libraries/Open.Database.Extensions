namespace Open.Database.Extensions.Benchmarks;

// How much managed memory does a RowMap<T> hold, and what does each way of passing a map allocate per call?
// dotnet Open.Database.Extensions.Benchmarks.dll --map-memory
// Managed heap only: the machine code the JIT emits for a compiled reader lives outside the GC heap and is not counted.
public static class MapMemoryStudy
{
	public static void Run()
	{
		const int Count = 1000;
		var connection = new BenchConnection(BenchTable.Posts(1).WithNames(RenamedColumns.Names));

		// Warm everything that is built once per process (reflection tables, JIT) so only the mappings are counted.
		RowMap<RenamedPost> warm = (RowMap<RenamedPost>)RenamedColumns.Pairs;
		_ = connection.Command("select 1").Map<RenamedPost>(warm).ToList();

		long before = GC.GetAllocatedBytesForCurrentThread();
		for (int i = 0; i < Count; i++) _ = (RowMap<RenamedPost>)RenamedColumns.Pairs;
		long allocatedPerBuild = (GC.GetAllocatedBytesForCurrentThread() - before) / Count;

		// The same mapping written out at the call site, as application code writes it.
		before = GC.GetAllocatedBytesForCurrentThread();
		for (int i = 0; i < Count; i++) _ = Literal();
		long allocatedPerLiteral = (GC.GetAllocatedBytesForCurrentThread() - before) / Count;

		long baseline = Settled();
		var maps = new RowMap<RenamedPost>[Count];
		for (int i = 0; i < Count; i++) maps[i] = (RowMap<RenamedPost>)RenamedColumns.Pairs;
		long retainedUnused = (Settled() - baseline) / Count;

		long beforeUse = Settled();
		for (int i = 0; i < Count; i++) _ = connection.Command("select 1").Map<RenamedPost>(maps[i]).ToList();
		long retainedAfterFirstRead = (Settled() - beforeUse) / Count;


		// Allocation per call of a one-row List read of the 12-property model (the in-memory provider): a declared map, and the forms that build a new map on every call.
		static long PerCall(Func<List<RenamedPost>> read)
		{
			for (int i = 0; i < 50; i++) _ = read();
			long start = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 200; i++) _ = read();
			return (GC.GetAllocatedBytesForCurrentThread() - start) / 200;
		}

		long callDeclared = PerCall(() => connection.Command("select 1").Map<RenamedPost>(RenamedColumns.Map).ToList());
		long callTuples = PerCall(() => connection.Command("select 1").Map<RenamedPost>(RenamedColumns.Tuples).ToList());
		long callArray = PerCall(() => connection.Command("select 1").Map<RenamedPost>(RenamedColumns.Pairs).ToList());
		long callLiteral = PerCall(() => connection.Command("select 1").Map<RenamedPost>(Literal()).ToList());

		GC.KeepAlive(maps);
		Console.WriteLine($"RowMap<T> with {RenamedColumns.Pairs.Length} fields, managed heap, averaged over {Count} mappings:");
		Console.WriteLine($"  allocated to build one:                 {allocatedPerBuild,6} B");
		Console.WriteLine($"  allocated to build one from a literal:   {allocatedPerLiteral,6} B (the pairs written at the call site)");
		Console.WriteLine($"  retained while unused:                  {retainedUnused,6} B");
		Console.WriteLine($"  retained added by its first read:       {retainedAfterFirstRead,6} B (one result-set layout, typed DbDataReader reader)");
		Console.WriteLine($"Allocated per call, one-row List read of the same model, in-memory provider:");
		Console.WriteLine($"  declared RowMap<T>:                                  {callDeclared,6} B");
		Console.WriteLine($"  tuple overload (prebuilt tuple array):               {callTuples,6} B  (+{callTuples - callDeclared} B: builds a map and compiles the reader every call)");
		Console.WriteLine($"  ImmutableArray of pairs converted at the call site:  {callArray,6} B  (+{callArray - callDeclared} B)");
		Console.WriteLine($"  collection expression written at the call site:      {callLiteral,6} B  (+{callLiteral - callDeclared} B)");
	}

	static RowMap<RenamedPost> Literal() => [new(nameof(RenamedPost.Id), "post_id"), new(nameof(RenamedPost.Title), "post_title"), new(nameof(RenamedPost.Body), "post_body"), new(nameof(RenamedPost.Created), "post_created"), new(nameof(RenamedPost.Price), "post_price"), new(nameof(RenamedPost.IsPublished), "post_is_published"), new(nameof(RenamedPost.ViewCount), "post_view_count"), new(nameof(RenamedPost.Score), "post_score"), new(nameof(RenamedPost.Key), "post_key"), new(nameof(RenamedPost.Rank), "post_rank"), new(nameof(RenamedPost.Big), "post_big"), new(nameof(RenamedPost.Tag), "post_tag")];

	static long Settled()
	{
		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();
		return GC.GetTotalMemory(true);
	}
}
