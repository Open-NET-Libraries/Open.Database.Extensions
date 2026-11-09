using System.Runtime.CompilerServices;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Edge cases of the staged Skip/Take queries and of the terminals every IMappedQuery gets from MappedQueryExtensions:
// skipped rows are never mapped, a first or a single row is asked of the provider only when that is safe, a plain IDataReader
// (not a DbDataReader) gives the same answers, the first row is all that FirstOrDefault maps, and the token reaches the query.
[ExcludeFromCodeCoverage]
public static class MappedQueryEdgeTests
{
	static FakeExecuteReader Command(int rowCount, out Func<int> rowsRead)
	{
		TypedReader? last = null;
		rowsRead = () => last?.RowsRead ?? 0;
		return new FakeExecuteReader(() => last = Reader(Rows(rowCount)));
	}

	// ---- staged Skip and Take ----

	[Theory]
	[InlineData(2, "n3", 1)]
	[InlineData(3, null, 0)]
	[InlineData(1, "throws", 1)]
	public static void Skip_ThenSingleOrDefault_SkipsWithoutMapping_AndWantsExactlyOneRowAfterTheSkip(int skip, string? expected, int mapped)
	{
		int calls = 0;
		FakeExecuteReader command = Command(3, out _);
		var query = command.Map(r =>
		{
			calls++;
			return r.GetString(1);
		}).Skip(skip);

		if (expected == "throws")
			Assert.Throws<InvalidOperationException>(() => query.SingleOrDefault());
		else
			Assert.Equal(expected, query.SingleOrDefault());

		Assert.Equal(mapped, calls);
	}

	[Theory]
	[InlineData("take 2", 1)]
	[InlineData("skip 1", 2)]
	public static void FirstOrDefault_MapsOnlyTheFirstRowItNeeds_PeeksOneRowMore_AndNeverAsksForSingleRow(string stage, int expectedId)
	{
		int calls = 0;
		FakeExecuteReader command = Command(5, out Func<int> rowsRead);
		MappedQuery<int> query = command.Map(r =>
		{
			calls++;
			return r.GetInt32(0);
		});

		int id = stage switch
		{
			"take 2" => query.Take(2).FirstOrDefault(),
			"skip 1" => query.Skip(1).FirstOrDefault(),
			_ => query.FirstOrDefault(),
		};

		Assert.Equal(expectedId, id);
		Assert.Equal(1, calls);
		Assert.Equal(expectedId + 1, rowsRead()); // the skipped row was read (it has to be), the one returned, and one more to find out whether the rows end there (a late error comes after the last row); nothing beyond that
		Assert.False(Assert.Single(command.Behaviors).HasFlag(CommandBehavior.SingleRow)); // SingleRow would hide that late error from SqlClient
	}

	[Fact]
	public static void ToDictionary_WithDuplicateKeys_Throws_AndANullKeySelectorIsRefusedBeforeTheCommandRuns()
	{
		FakeExecuteReader command = Command(3, out _);
		MappedQueryWithTake<int> query = command.Map(r => r.GetInt32(0)).Take(3);

		Assert.Throws<ArgumentException>(() => query.ToDictionary(_ => "same"));
		int executed = command.ExecuteCount;
		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => query.ToDictionary<string>(null!));

		Assert.Equal("keySelector", ex.ParamName);
		Assert.Equal(executed, command.ExecuteCount);
	}

	// ---- a plain IDataReader, which has its own path ----

	static string Summarize(IExecuteReader command)
	{
		MappedQueryWithTake<string> query = command.Map(r => $"{r[0]}{r[1]}").Skip(1).Take(3);
		return string.Join("|",
			string.Join(",", query.ToList()),
			string.Join(",", query.ToArray()),
			string.Join(",", query.ToImmutableArray()),
			query.FirstOrDefault(),
			string.Join(",", query.ToHashSet().OrderBy(s => s, StringComparer.Ordinal)),
			string.Join(",", query.ToDictionary(s => s).Keys.OrderBy(s => s, StringComparer.Ordinal)));
	}

	[Fact]
	public static void APlainIDataReader_GivesTheSameAnswersAsADbDataReader()
	{
		string[] columns = ["Id", "Name"];
		object?[][] rows = [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"]];
		var db = new FakeExecuteReader(columns, rows);
		var plain = new PlainReaderCommand(() => FakeReader.Create(columns, rows));
		const string expected = "2b,3c,4d|2b,3c,4d|2b,3c,4d|2b|2b,3c,4d|2b,3c,4d";

		Assert.Equal(expected, Summarize(db));
		Assert.Equal(expected, Summarize(plain));
		Assert.Equal("5e", plain.Map(r => $"{r[0]}{r[1]}").Skip(4).SingleOrDefault());
		Assert.Throws<InvalidOperationException>(() => plain.Map(r => $"{r[0]}{r[1]}").Skip(1).SingleOrDefault());
	}

	// ---- the terminals every IMappedQuery has ----

	sealed class StubQuery<T>(IEnumerable<T> items) : IMappedQuery<T>
	{
		public int Executions { get; private set; }

		public CancellationToken LastToken { get; private set; }

		public void Read(Action<IEnumerable<T>> handler)
		{
			Executions++;
			handler(items);
		}

		public TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler)
		{
			Executions++;
			return handler(items);
		}

		public ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken)
		{
			Executions++;
			LastToken = cancellationToken;
			return handler(Stream(cancellationToken), cancellationToken);
		}

		public ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken)
		{
			Executions++;
			LastToken = cancellationToken;
			return handler(Stream(cancellationToken), cancellationToken);
		}

		async IAsyncEnumerable<T> Stream([EnumeratorCancellation] CancellationToken cancellationToken)
		{
			foreach (T item in items)
			{
				cancellationToken.ThrowIfCancellationRequested();
				await Task.Yield();
				yield return item;
			}
		}
	}

	[Fact]
	public static void EveryCollectionTerminal_ExecutesOnce_InTheOrderTheRowsCame_AndHonorsTheComparer()
	{
		var stub = new StubQuery<string>(["b", "a", "B"]);
		IMappedQuery<string> query = stub;

		Assert.Equal(["b", "a", "B"], query.ToList());
		Assert.Equal(["b", "a", "B"], query.ToArray());
		Assert.Equal(["b", "a", "B"], query.ToImmutableArray().ToArray());
		Assert.Equal(["b", "a"], query.ToHashSet(StringComparer.OrdinalIgnoreCase).ToArray()); // the row that compares equal to an earlier one is dropped
		Assert.Equal(4, stub.Executions);
		Assert.Throws<ArgumentException>(() => query.ToDictionary(s => s, StringComparer.OrdinalIgnoreCase)); // "b" and "B" are the same key
	}

	[Fact]
	public static async Task TheAsyncTerminals_PassTheCallersTokenToTheQuery_AndStopWhenItIsCancelled()
	{
		var stub = new StubQuery<int>([1, 2, 3]);
		IMappedQuery<int> query = stub;
		using var cts = new CancellationTokenSource();

		Assert.Equal([1, 2, 3], await query.ToListAsync(cts.Token));
		Assert.Equal(cts.Token, stub.LastToken);

		cts.Cancel();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await query.ToArrayAsync(cts.Token));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await query.FirstOrDefaultAsync(cts.Token));
	}

	[Fact]
	public static async Task ReadAsync_ANullHandler_IsRefused_ByName()
	{
		IMappedQuery<int> query = new StubQuery<int>([1]);
		CancellationToken token = TestContext.Current.CancellationToken;

		ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(async () =>
			await query.ReadAsync((Func<IAsyncEnumerable<int>, ValueTask>)null!, token));

		Assert.Equal("handler", ex.ParamName);
	}
}
