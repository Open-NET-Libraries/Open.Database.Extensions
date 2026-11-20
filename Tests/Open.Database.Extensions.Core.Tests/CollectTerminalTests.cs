using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ToHashSet and ToDictionary (both forms, sync and async) read the rows into
// a chain first and add them after the read, so a duplicate key throws the
// dictionary's own exception once every row has been read.
[ExcludeFromCodeCoverage]
[Collection("ChunkSizing")]
public static class CollectTerminalTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// Ids 1, 2, 2.
	static FakeExecuteReader Duplicated()
		=> new(() => Reader([[1, "a", null], [2, "b", null], [2, "c", null]]));

	static FakeExecuteReader Distinct(int rows)
		=> new(() => Reader(Rows(rows)));

	// What a plain Dictionary.Add says for the same duplicate key.
	static string PlainMessage()
	{
		Dictionary<int, PoolRow> plain = new() { [2] = new PoolRow(2) };
		return Assert.Throws<ArgumentException>(
			() => plain.Add(2, new PoolRow(2))).Message;
	}

	[Fact]
	public static void ToDictionary_Key_DuplicateThrowsTheDictionarysException()
	{
		var ex = Assert.Throws<ArgumentException>(
			() => Duplicated().Map<PoolRow>().ToDictionary(r => r.Id));
		Assert.Equal(PlainMessage(), ex.Message);
	}

	[Fact]
	public static async Task ToDictionaryAsync_Key_DuplicateThrowsTheDictionarysException()
	{
		var ex = await Assert.ThrowsAsync<ArgumentException>(
			async () => await Duplicated().Map<PoolRow>()
				.ToDictionaryAsync(r => r.Id, null, Ct));
		Assert.Equal(PlainMessage(), ex.Message);
	}

	[Fact]
	public static void ToDictionary_KeyAndValue_DuplicateThrowsTheDictionarysException()
	{
		var ex = Assert.Throws<ArgumentException>(
			() => Duplicated().Map<PoolRow>()
				.ToDictionary(r => r.Id, r => r.Id * 10L));
		Assert.Equal(PlainMessage(), ex.Message);
	}

	[Fact]
	public static async Task ToDictionaryAsync_KeyAndValue_DuplicateThrowsTheDictionarysException()
	{
		var ex = await Assert.ThrowsAsync<ArgumentException>(
			async () => await Duplicated().Map<PoolRow>()
				.ToDictionaryAsync(r => r.Id, r => r.Id * 10L, null, Ct));
		Assert.Equal(PlainMessage(), ex.Message);
	}

	[Fact]
	public static async Task ADuplicateKey_ReadsEveryRowFirst()
	{
		// The rows are all read (the reader has no more to give) before the
		// duplicate is found.
		TypedReader? reader = null;
		FakeExecuteReader command = new(() => reader = Reader(
			[[1, "a", null], [2, "b", null], [2, "c", null], [3, "d", null]]));

		await Assert.ThrowsAsync<ArgumentException>(
			async () => await command.Map<PoolRow>()
				.ToDictionaryAsync(r => r.Id, null, Ct));
		Assert.Equal(4, reader!.RowsRead);
	}

	[Fact]
	public static async Task ANullKey_ThrowsArgumentNull_AfterTheRead()
	{
		Assert.Throws<ArgumentNullException>(
			() => Distinct(2).Map<PoolRow>().ToDictionary(_ => (string)null!));
		await Assert.ThrowsAsync<ArgumentNullException>(
			async () => await Distinct(2).Map<PoolRow>()
				.ToDictionaryAsync(_ => (string)null!, null, Ct));
	}

	[Fact]
	public static async Task ToDictionary_Key_WithAComparer()
	{
		FakeExecuteReader command = Distinct(3);

		var sync = command.Map<PoolRow>()
			.ToDictionary(r => "k" + r.Id, StringComparer.OrdinalIgnoreCase);
		Assert.Equal(3, sync.Count);
		Assert.Equal(2, sync["K2"].Id);

		var viaAsync = await command.Map<PoolRow>().ToDictionaryAsync(
			r => "k" + r.Id, StringComparer.OrdinalIgnoreCase, Ct);
		Assert.Equal(3, viaAsync.Count);
		Assert.Equal(3, viaAsync["K3"].Id);
	}

	[Fact]
	public static async Task ToDictionary_KeyAndValue_WithAComparer()
	{
		FakeExecuteReader command = Distinct(3);

		var sync = command.Map<PoolRow>().ToDictionary(
			r => "k" + r.Id, r => r.Id * 10L, StringComparer.OrdinalIgnoreCase);
		Assert.Equal(20L, sync["K2"]);

		var viaAsync = await command.Map<PoolRow>().ToDictionaryAsync(
			r => "k" + r.Id, r => r.Id * 10L,
			StringComparer.OrdinalIgnoreCase, Ct);
		Assert.Equal(30L, viaAsync["K3"]);
		Assert.Equal(3, viaAsync.Count);
	}

	// Equal when the ids end in the same digit.
	sealed class LastDigit : IEqualityComparer<PoolRow>
	{
		public bool Equals(PoolRow? x, PoolRow? y)
			=> x!.Id % 10 == y!.Id % 10;

		public int GetHashCode(PoolRow obj)
			=> obj.Id % 10;
	}

	[Fact]
	public static async Task ToHashSet_WithAComparer_DropsDuplicatesSilently()
	{
		FakeExecuteReader command = new(
			() => Reader([[1, "a", null], [2, "b", null], [11, "c", null]]));

		var sync = command.Map<PoolRow>().ToHashSet(new LastDigit());
		Assert.Equal(2, sync.Count);
		Assert.Contains(new PoolRow(1), sync);

		HashSet<PoolRow> viaAsync = await command.Map<PoolRow>()
			.ToHashSetAsync(new LastDigit(), Ct);
		Assert.Equal(2, viaAsync.Count);

		// Without a comparer the records are all distinct.
		Assert.Equal(3, command.Map<PoolRow>().ToHashSet().Count);
	}

	[Fact]
	public static async Task ToHashSet_WithSkipAndTake_AndNoRows()
	{
		Assert.Equal(
			[3, 4],
			(await Distinct(10).Map<PoolRow>().Skip(2).Take(2).ToHashSetAsync(null, Ct))
				.Select(r => r.Id).Order());
		Assert.Empty(await Distinct(0).Map<PoolRow>().ToHashSetAsync(null, Ct));
		Assert.Empty(Distinct(5).Map<PoolRow>().Take(0).ToHashSet());
		Assert.Empty(await Distinct(0).Map<PoolRow>()
			.ToDictionaryAsync(r => r.Id, null, Ct));
	}

	// ToImmutableArrayAsync is the same read as ToArrayAsync, with the exact
	// array wrapped: the same rows in the same order, nothing copied again.
	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(17)]
	[InlineData(100)]
	public static async Task ToImmutableArrayAsync_IsTheArrayOfToArrayAsync(int n)
	{
		FakeExecuteReader command = Distinct(n);

		PoolRow[] array = await command.Map<PoolRow>().ToArrayAsync(Ct);
		ImmutableArray<PoolRow> immutable = await command.Map<PoolRow>()
			.ToImmutableArrayAsync(Ct);

		Assert.Equal(n, immutable.Length);
		Assert.Equal(array.Select(static r => r.Id), immutable.Select(static r => r.Id));
		Assert.Equal(
			Enumerable.Range(1, n),
			immutable.Select(static r => r.Id));
	}
}
