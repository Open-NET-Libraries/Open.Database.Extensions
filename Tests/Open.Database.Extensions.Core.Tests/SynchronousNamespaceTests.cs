namespace Open.Database.Extensions.Tests;

// The blocking terminals live in Open.Database.Extensions.Synchronous.
// A command type from outside the library opts in by implementing the small interface there;
// without it the call says which interface is missing, instead of failing in some other way.
[ExcludeFromCodeCoverage]
public static class SynchronousNamespaceTests
{
	sealed class AsyncOnlyReader : IExecuteReader
	{
		public CancellationToken CancellationToken => default;

		public ValueTask ExecuteReaderAsync(
			Func<IDataReader, ValueTask> handler,
			CommandBehavior behavior = CommandBehavior.Default)
			=> throw new InvalidOperationException("The async-only double was reached.");

		public ValueTask<T> ExecuteReaderAsync<T>(
			Func<IDataReader, ValueTask<T>> transform,
			CommandBehavior behavior = CommandBehavior.Default)
			=> throw new InvalidOperationException("The async-only double was reached.");

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
			TState state,
			Func<IDataReader, TState, ValueTask<TResult>> transform,
			CommandBehavior behavior = CommandBehavior.Default)
			=> throw new InvalidOperationException("The async-only double was reached.");

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
			TState state,
			Func<IDataReader, TState, ValueTask<TResult>> transform,
			CommandBehavior behavior,
			CancellationToken cancellationToken)
			=> throw new InvalidOperationException("The async-only double was reached.");
	}

	[Fact]
	public static void AReaderCommandWithoutTheSyncInterface_NamesItWhenCalledBlocking()
	{
		IExecuteReader command = new AsyncOnlyReader();

		NotSupportedException ex = Assert.Throws<NotSupportedException>(() => command.LoadTable());
		Assert.Contains(typeof(AsyncOnlyReader).ToString(), ex.Message);
		Assert.Contains("Open.Database.Extensions.Synchronous.IExecuteReaderSync", ex.Message);
		Assert.Contains("use the async call", ex.Message);

		NotSupportedException tables = Assert.Throws<NotSupportedException>(() => command.LoadTables());
		Assert.Contains("IExecuteReaderSync", tables.Message);
	}

	[Fact]
	public static void TheFourViewsThatOpenTheConnection_ImplementTheQueryInterface()
	{
		Assert.IsType<IMappedQuery<int>>(default(MappedQuery<int>), exactMatch: false);
		Assert.IsType<IMappedQuery<int>>(default(MappedQueryWithSkip<int>), exactMatch: false);
		Assert.IsType<IMappedQuery<int>>(default(MappedQueryWithTake<int>), exactMatch: false);
		Assert.IsType<IMappedQuery<int>>(default(MappedCommand<int>), exactMatch: false);
	}

	sealed record Row(int Id, string Name);

	static DbTally Tally(int rows)
		=> new(new DbScript
		{
			Rows = [.. Enumerable.Range(1, rows).Select(i => new object?[] { i, "n" + i })],
		});

	// With `using Open.Database.Extensions.Synchronous;` the blocking terminals are there on every view
	// that opens the connection.
	[Fact]
	public static void TheBlockingTerminals_AreOnTheFourViews_WithTheUsing()
	{
		DbTally tally = Tally(3);
		TrackedConnection open = tally.NewOpenConnection();

		MappedQuery<Row> query = open.Command("select").Map<Row>();
		Assert.Equal(3, query.ToList().Count);
		Assert.Equal(3, query.ToArray().Length);
		Assert.Equal(3, query.ToImmutableArray().Length);
		Assert.Equal(3, query.ToDictionary(r => r.Id).Count);
		Assert.Equal(3, query.ToDictionary(r => r.Id, r => r.Name).Count);
		Assert.Equal(3, query.ToHashSet().Count);
		Assert.Equal(1, query.First().Id);
		Assert.Equal(1, query.FirstOrDefault()?.Id);
		Assert.Throws<InvalidOperationException>(() => query.Single());
		Assert.Throws<InvalidOperationException>(() => query.SingleOrDefault());

		MappedQueryWithSkip<Row> skip = open.Command("select").Map<Row>().Skip(1);
		Assert.Equal(2, skip.ToList().Count);
		Assert.Equal(2, skip.ToArray().Length);
		Assert.Equal(2, skip.ToImmutableArray().Length);
		Assert.Equal(2, skip.ToDictionary(r => r.Id).Count);
		Assert.Equal(2, skip.ToDictionary(r => r.Id, r => r.Name).Count);
		Assert.Equal(2, skip.ToHashSet().Count);
		Assert.Equal(2, skip.First().Id);
		Assert.Equal(2, skip.FirstOrDefault()?.Id);

		MappedQueryWithTake<Row> take = open.Command("select").Map<Row>().Take(1);
		Assert.Single(take.ToList());
		Assert.Single(take.ToArray());
		Assert.Single(take.ToImmutableArray());
		Assert.Single(take.ToDictionary(r => r.Id));
		Assert.Single(take.ToDictionary(r => r.Id, r => r.Name));
		Assert.Single(take.ToHashSet());
		Assert.Equal(1, take.First().Id);
		Assert.Equal(1, take.FirstOrDefault()?.Id);
		Assert.Equal(1, take.Single().Id);
		Assert.Equal(1, take.SingleOrDefault()?.Id);

		MappedCommand<Row> command = open.CreateTextCommand("select").Map<Row>();
		Assert.Equal(3, command.ToList().Count);
		Assert.Equal(3, command.ToArray().Length);
		Assert.Equal(3, command.ToImmutableArray().Length);
		Assert.Equal(3, command.ToDictionary(r => r.Id).Count);
		Assert.Equal(3, command.ToDictionary(r => r.Id, r => r.Name).Count);
		Assert.Equal(3, command.ToHashSet().Count);
		Assert.Equal(1, command.First().Id);
		Assert.Equal(1, command.FirstOrDefault()?.Id);
	}
}
