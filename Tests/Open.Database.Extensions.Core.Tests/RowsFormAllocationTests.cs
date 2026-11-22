using System.Data.Common;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The direct members of DictionaryRow allocate nothing (the names are shared by the rows of the result set, and the struct
// enumerator and the indexers read the row's own array); handing the row to an interface boxes it, which is measured here and
// said in the docs. The per-thread byte counter is exact, so only a stray JIT or GC adds noise.
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class RowsFormAllocationTests
{
	static DictionaryRow Row()
	{
		DbDataReader reader = FakeReader.CreateDb(["Id", "Name", "Other"], [1, "a", 3]);
		var map = DictionaryRowSource.Instance.Mapper(reader);
		reader.Read();
		return map(reader);
	}

	static long BytesPerCall(Action call)
	{
		for (int i = 0; i < 300; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 5; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 2000; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 2000);
		}

		return best;
	}

	[Fact]
	public static void TheDirectMembers_AllocateNothing_AndTheInterfaceBoxesTheRow()
	{
#if DEBUG
		Assert.Skip("Release only.");
#else
		DictionaryRow row = Row();
		object? sink = null;
		int count = 0;

		long byName = BytesPerCall(() => sink = row["Name"]);
		long byCase = BytesPerCall(() => sink = row["NAME"]);
		long tryGet = BytesPerCall(() => count += row.TryGetValue("Other", out sink) ? 1 : 0);
		long ordinal = BytesPerCall(() => sink = row[1]);
		long counts = BytesPerCall(() => count += row.Count + row.FieldCount);
		long enumerate = BytesPerCall(() =>
		{
			foreach (KeyValuePair<string, object?> pair in row) count += pair.Key.Length;
		});
		long boxed = BytesPerCall(() => count += ((IReadOnlyDictionary<string, object?>)row).Count);
		GC.KeepAlive(sink);

		Assert.Equal(0, byName);
		Assert.Equal(0, byCase);
		Assert.Equal(0, tryGet);
		Assert.Equal(0, ordinal);
		Assert.Equal(0, counts);
		Assert.Equal(0, enumerate);
		Assert.True(boxed >= 16, $"Handing the row to an interface boxes it: {boxed} B.");
		Assert.True(count > 0);
#endif
	}
}
