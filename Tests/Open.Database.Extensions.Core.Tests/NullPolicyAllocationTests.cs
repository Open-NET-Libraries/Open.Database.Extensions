#nullable enable

namespace Open.Database.Extensions.Tests;

/// <summary>
/// A NullPolicy or a NullValue adds a NULL test per covered member per row and nothing else:
/// a model with one maps a row for the bytes a model without one does (the model object and its list slot), whether the cells hold values or NULLs it replaces.
/// The per-row bytes are measured in the same run and compared.
/// </summary>
[ExcludeFromCodeCoverage]
public static class NullPolicyAllocationTests
{
	public sealed class Plain
	{
		public int A { get; set; }
		public string B { get; set; } = "";
	}

	public sealed class Throwing
	{
		[NullPolicy(NullAction.Throw)] public int A { get; set; }
		[NullPolicy(NullAction.Throw)] public string B { get; set; } = "";
	}

	public sealed class Defaulted
	{
		[NullPolicy(NullAction.Default)] public int A { get; set; }
		[NullPolicy(NullAction.Default)] public string B { get; set; } = "";
	}

	public sealed class Valued
	{
		[NullValue(-1)] public int A { get; set; }
		[NullValue("")] public string B { get; set; } = "";
	}

	// The measurement's rounding: the two maps do the same work per row, so their bytes per row are equal.
	const double SlackPerRow = 1;

	static object?[][] Rows(int count, bool nulls)
	{
		var rows = new object?[count][];
		for (int i = 0; i < count; i++) rows[i] = nulls ? [null, null] : [i, "b"];
		return rows;
	}

	// Bytes per call: after a warm-up, the smallest of three rounds (the allocated-bytes counter of this thread is exact, so only a stray JIT or GC adds noise).
	static long BytesPerCall(Func<object> call)
	{
		for (int i = 0; i < 50; i++) call();
		long best = long.MaxValue;
		for (int round = 0; round < 3; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 20; i++) call();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 20);
		}

		return best;
	}

	// (bytes at 1000 rows - bytes at 100 rows) / 900: the call, the reader and the command cancel, so only the row remains.
	static double BytesPerRow<TModel>(bool nulls)
		where TModel : class
	{
		object?[][] few = Rows(100, nulls);
		object?[][] many = Rows(1000, nulls);
		ColumnDef[] columns = [new("A", typeof(int)), new("B", typeof(string))];
		FakeDbCommand Command(object?[][] rows) => new(new FakeDbConnection(), () => TypedReader.Create([.. columns], rows));

		long small = BytesPerCall(() => Command(few).Map<TModel>().ToList());
		long large = BytesPerCall(() => Command(many).Map<TModel>().ToList());
		return (large - small) / 900.0;
	}

	public static TheoryData<string, bool> Cases => new()
	{
		{ nameof(Throwing), false },
		{ nameof(Defaulted), false },
		{ nameof(Valued), false },
		{ nameof(Defaulted), true },
		{ nameof(Valued), true },
	};

	[Theory]
	[MemberData(nameof(Cases))]
	public static void ACoveredMember_CostsARowNoBytesBeyondAPlainModel(string model, bool nulls)
	{
		double plain = BytesPerRow<Plain>(false);
		double covered = model switch
		{
			nameof(Throwing) => BytesPerRow<Throwing>(nulls),
			nameof(Defaulted) => BytesPerRow<Defaulted>(nulls),
			_ => BytesPerRow<Valued>(nulls),
		};

		Assert.True(Math.Abs(covered - plain) <= SlackPerRow, $"{model} (NULL cells: {nulls}): {covered:0.##} bytes per row; a plain model: {plain:0.##}.");
	}

	// The stream route: the rows pulled one at a time through the IAsyncEnumerable terminal. The per-row catch of a failing row sits around the map call
	// inside the iterator, so a row costs the stream no bytes beyond what a hand-written selector's row costs: the model object.
	static int Drain<T>(IAsyncEnumerable<T> rows)
	{
		static async ValueTask<int> Core(IAsyncEnumerable<T> rows)
		{
			int n = 0;
			await foreach (T _ in rows.ConfigureAwait(false)) n++;
			return n;
		}

		return Core(rows).AsTask().GetAwaiter().GetResult();
	}

	static double StreamBytesPerRow(bool handWritten)
	{
		object?[][] few = Rows(100, false);
		object?[][] many = Rows(1000, false);
		ColumnDef[] columns = [new("A", typeof(int)), new("B", typeof(string))];
		FakeDbCommand Command(object?[][] rows) => new(new FakeDbConnection(), () => TypedReader.Create([.. columns], rows));

		long Bytes(object?[][] rows)
			=> handWritten
			? BytesPerCall(() => Drain(Command(rows).Map(r => new Plain { A = r.GetInt32(0), B = r.GetString(1) }).Stream(Timeout.InfiniteTimeSpan)))
			: BytesPerCall(() => Drain(Command(rows).Map<Plain>().Stream(Timeout.InfiniteTimeSpan)));

		return (Bytes(many) - Bytes(few)) / 900.0;
	}

	[Fact]
	public static void AStreamedRow_CostsNoBytesBeyondAHandWrittenSelectors()
	{
		double handWritten = StreamBytesPerRow(true);
		double mapped = StreamBytesPerRow(false);

		Assert.True(mapped <= handWritten + SlackPerRow, $"A mapped stream costs {mapped:0.##} bytes per row; a hand-written selector's: {handWritten:0.##}.");
	}
}
