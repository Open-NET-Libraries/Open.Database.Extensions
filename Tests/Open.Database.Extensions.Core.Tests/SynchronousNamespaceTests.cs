namespace Open.Database.Extensions.Tests;

// The blocking terminals live in Open.Database.Extensions.Synchronous.
// A type from outside the library opts in by implementing the small interface there;
// without it the call says which interface is missing, instead of failing in some other way.
[ExcludeFromCodeCoverage]
public static class SynchronousNamespaceTests
{
	sealed class AsyncOnlyQuery : IMappedQuery<int>
	{
		public ValueTask ReadAsync(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask> handler,
			CancellationToken cancellationToken)
			=> throw new InvalidOperationException("Not reached.");

		public ValueTask<TResult> ReadAsync<TResult>(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask<TResult>> handler,
			CancellationToken cancellationToken)
			=> throw new InvalidOperationException("Not reached.");
	}

	sealed class AsyncOnlyReader : IExecuteReader
	{
		public CancellationToken CancellationToken => default;

		public ValueTask ExecuteReaderAsync(
			Func<IDataReader, ValueTask> handler,
			CommandBehavior behavior = CommandBehavior.Default)
			=> throw new InvalidOperationException("Not reached.");

		public ValueTask<T> ExecuteReaderAsync<T>(
			Func<IDataReader, ValueTask<T>> transform,
			CommandBehavior behavior = CommandBehavior.Default)
			=> throw new InvalidOperationException("Not reached.");

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
			TState state,
			Func<IDataReader, TState, ValueTask<TResult>> transform,
			CommandBehavior behavior = CommandBehavior.Default)
			=> throw new InvalidOperationException("Not reached.");

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
			TState state,
			Func<IDataReader, TState, ValueTask<TResult>> transform,
			CommandBehavior behavior,
			CancellationToken cancellationToken)
			=> throw new InvalidOperationException("Not reached.");
	}

	[Fact]
	public static void AQueryWithoutTheSyncInterface_NamesItWhenCalledBlocking()
	{
		IMappedQuery<int> query = new AsyncOnlyQuery();

		NotSupportedException ex = Assert.Throws<NotSupportedException>(() => query.ToList());

		Assert.Contains(typeof(AsyncOnlyQuery).ToString(), ex.Message);
		Assert.Contains("Open.Database.Extensions.Synchronous.IMappedQuerySync", ex.Message);
		Assert.Contains("async", ex.Message);
	}

	[Fact]
	public static void EveryBlockingTerminalOnAQueryWithoutTheInterface_SaysTheSame()
	{
		IMappedQuery<int> query = new AsyncOnlyQuery();

		Action[] calls =
		[
			() => query.ToArray(),
			() => query.ToImmutableArray(),
			() => query.First(),
			() => query.FirstOrDefault(),
			() => query.Single(),
			() => query.SingleOrDefault(),
			() => query.ToHashSet(),
			() => query.ToDictionary(i => i),
			() => query.ToDictionary(i => i, i => i),
		];

		foreach (Action call in calls)
		{
			NotSupportedException ex = Assert.Throws<NotSupportedException>(call);
			Assert.Contains("IMappedQuerySync", ex.Message);
		}
	}

	[Fact]
	public static void AReaderCommandWithoutTheSyncInterface_NamesItWhenCalledBlocking()
	{
		IExecuteReader command = new AsyncOnlyReader();

		NotSupportedException ex = Assert.Throws<NotSupportedException>(() => command.LoadTable());
		Assert.Contains(typeof(AsyncOnlyReader).ToString(), ex.Message);
		Assert.Contains("Open.Database.Extensions.Synchronous.IExecuteReaderSync", ex.Message);

		Assert.Throws<NotSupportedException>(() => command.LoadTables());
	}

	[Fact]
	public static async Task TheAsyncCallsNeedNoSyncInterface()
	{
		// A type that only supports the async calls is a full IMappedQuery<T> / IExecuteReader:
		// the async terminals reach it (here it throws its own marker, which is how we know the call arrived).
		IMappedQuery<int> query = new AsyncOnlyQuery();

		await Assert.ThrowsAsync<InvalidOperationException>(async () => await query.ToListAsync(TestContext.Current.CancellationToken));
	}

	[Fact]
	public static void TheViewsAndTheirReaderAndTableSiblingsAllImplementTheInterface()
	{
		Assert.IsAssignableFrom<IMappedQuerySync<int>>(default(MappedQuery<int>));
		Assert.IsAssignableFrom<IMappedQuerySync<int>>(default(MappedQueryWithSkip<int>));
		Assert.IsAssignableFrom<IMappedQuerySync<int>>(default(MappedQueryWithTake<int>));
		Assert.IsAssignableFrom<IMappedQuerySync<int>>(default(MappedCommand<int>));
		Assert.IsAssignableFrom<IMappedQuerySync<int>>(default(MappedReader<int>));
		Assert.IsAssignableFrom<IMappedQuerySync<int>>(default(MappedTable<int>));
	}

	static readonly int[] Expected = [1, 2, 3];

	[Fact]
	public static void AViewTypedAsTheInterface_ReadsThroughTheSyncInterface()
	{
		IMappedQuery<int> query = new FakeExecuteReader(["n"], [1], [2], [3]).Map<int>();

		Assert.Equal(Expected, query.ToList());
		Assert.Equal(1, query.First());
		Assert.Equal(3, query.ToArray().Length);
	}
}
