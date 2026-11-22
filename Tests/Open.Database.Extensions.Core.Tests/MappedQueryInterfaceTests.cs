namespace Open.Database.Extensions.Tests;

#nullable enable

// The IMappedQuery<T> extensions (ToListAsync, ToArrayAsync, ...) are what a
// query is collected with when it is held as the interface. A query that is
// ours collects with its OWN terminals, so the pool and the clear rule it was
// given by UsePool are honoured whichever way it is called. A query that is
// not ours (a user's IMappedQuery<T>, even one wrapping ours) has no
// configuration, and is collected without pooling.
[ExcludeFromCodeCoverage]
public static class MappedQueryInterfaceTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	const int N = 100;

	static CountingPool<int> IntPool()
		=> new()
		{
			RoundUp = true,
			Sentinel = static () => -1,
			IsDirty = static a => a.Any(v => v > 0),
		};

	// Ours, with a pool of the caller's: passed on as the interface (boxed).
	static MappedCommand<int> Ours(CountingPool<int> pool)
		=> new FakeDbCommand(
			new FakeDbConnection(),
			() => new ChannelTestReader(N))
			.Map<int>()
			.AsyncReads()
			.UsePool(pool, clearOnReturn: true);

	public static TheoryData<string> Terminals
		=>
		[
			// The sync ToList is not a pooling terminal (a list grows by itself).
			"ToArray",
			"ToImmutableArray",
			"ToHashSet",
			"ToDictionary",
			"ToDictionaryValue",
			"ToListAsync",
			"ToArrayAsync",
			"ToImmutableArrayAsync",
			"ToHashSetAsync",
			"ToDictionaryAsync",
			"ToDictionaryValueAsync",
		];

	// How many rows the terminal produced.
	static async Task<int> Run(string terminal, IMappedQuery<int> q)
		=> terminal switch
		{
			"ToArray" => q.ToArray().Length,
			"ToImmutableArray" => q.ToImmutableArray().Length,
			"ToHashSet" => q.ToHashSet().Count,
			"ToDictionary" => q.ToDictionary(i => i).Count,
			"ToDictionaryValue" => q.ToDictionary(i => i, i => i * 2).Count,
			"ToListAsync" => (await q.ToListAsync(Ct)).Count,
			"ToArrayAsync" => (await q.ToArrayAsync(Ct)).Length,
			"ToImmutableArrayAsync" => (await q.ToImmutableArrayAsync(Ct)).Length,
			"ToHashSetAsync" => (await q.ToHashSetAsync(null, Ct)).Count,
			"ToDictionaryAsync" => (await q.ToDictionaryAsync(i => i, null, Ct)).Count,
			_ => (await q.ToDictionaryAsync(i => i, i => i * 2, null, Ct)).Count,
		};
	[Theory, MemberData(nameof(Terminals))]
	public static async Task UsePool_IsHonoured_ThroughTheInterface(string terminal)
	{
		CountingPool<int> pool = IntPool();

		Assert.Equal(N, await Run(terminal, Ours(pool)));

		// Every array came from the caller's pool, went back once, and was
		// cleared (clearOnReturn: true, value rows).
		Assert.True(pool.Rents > 0, $"{terminal} did not rent from the pool");
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.Unmatched);
		Assert.Equal(0, pool.DirtyReturns);
	}

	// A user's IMappedQuery<T>, here a wrapper around ours: it has no
	// configuration of its own, so it is collected without pooling, and the pool
	// of the query it wraps is not touched.
	sealed class Foreign(IMappedQuerySync<int> inner) : IMappedQuerySync<int>
	{
		public void Read(Action<IEnumerable<int>> handler)
			=> inner.Read(handler);

		public TResult Read<TResult>(Func<IEnumerable<int>, TResult> handler)
			=> inner.Read(handler);

		public ValueTask ReadAsync(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask> handler,
			CancellationToken cancellationToken)
			=> inner.ReadAsync(handler, cancellationToken);

		public ValueTask<TResult> ReadAsync<TResult>(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask<TResult>> handler,
			CancellationToken cancellationToken)
			=> inner.ReadAsync(handler, cancellationToken);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task AForeignQuery_IsCollectedWithoutPooling(string terminal)
	{
		CountingPool<int> pool = IntPool();

		Assert.Equal(N, await Run(terminal, new Foreign(Ours(pool))));

		Assert.Equal(0, pool.Rents);
	}

	[Fact]
	public static async Task AForeignQuery_GivesTheRightRows_InOrder()
	{
		IMappedQuery<int> q = new Foreign(Ours(IntPool()));

		Assert.Equal(Enumerable.Range(1, N), q.ToArray());
		Assert.Equal(Enumerable.Range(1, N), await q.ToArrayAsync(Ct));
		Assert.Equal(Enumerable.Range(1, N), await q.ToListAsync(Ct));
	}

	[Fact]
	public static async Task ANullQuery_IsRefused()
	{
		IMappedQuery<int> q = null!;

		Assert.Throws<ArgumentNullException>(() => q.ToList());
		Assert.Throws<ArgumentNullException>(() => q.ToArray());
		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await q.ToArrayAsync(Ct));
	}
}
