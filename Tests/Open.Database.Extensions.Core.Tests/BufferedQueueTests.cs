using System.Collections.Immutable;
using System.Data.Common;
using System.Reflection;
using Simple = Open.Database.Extensions.Tests.RowMapperTests.Simple;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The buffered Results family returns the queue it builds: the caller can enumerate it again, count it, or Dequeue to release rows.
[ExcludeFromCodeCoverage]
public static class BufferedQueueTests
{
	static readonly object?[][] IdRows = [[1], [null], [3]];

	static TypedReader Simples() => TypedReader.Create([("Id", typeof(int)), ("Name", typeof(string))], [1, "a"], [2, "b"], [3, "c"]);

	static TypedReader Ids() => TypedReader.Create([("Id", typeof(int))], IdRows);

	static FakeDbCommand DbCommandOver(Func<DbDataReader> factory)
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		return new FakeDbCommand(connection, factory);
	}

	static ConnectionProbe ProbeOver(Func<IDataReader> factory) => new(ConnectionState.Open) { ReaderFactory = factory };

	static void AssertReplayable<T>(Queue<T> queue, int count)
	{
		Assert.Equal(count, queue.Count);
		Assert.Equal(queue.ToArray(), queue.ToArray());
		Assert.Equal(count, queue.Count); // enumerating did not empty it
	}

	// ---- the member list ----

	static bool IsQueueReturn(Type type)
	{
		if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTask<>)) type = type.GetGenericArguments()[0];
		return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Queue<>);
	}

	[Fact]
	public static void EveryBufferedMember_ReturnsAQueue()
	{
		static IEnumerable<MethodInfo> Of(Type type, Func<MethodInfo, bool> filter)
			=> type.GetMethods(BindingFlags.Public | BindingFlags.Static).Where(filter);

		var members = new List<MethodInfo>();
		members.AddRange(Of(typeof(CoreExtensions), m => m.Name is "ResultsBuffered" or "ResultsBufferedAsync"));
		members.AddRange(Of(typeof(CoreExtensions), m => m.Name == "Results" && m.GetParameters()[0].ParameterType == typeof(IDbCommand)));
		members.AddRange(Of(typeof(CoreExtensions), m => m.Name == "ResultsAsync" && m.GetParameters()[0].ParameterType == typeof(DbCommand)));
		members.AddRange(Of(typeof(DataReaderExtensions), m => m.Name.StartsWith("FirstOrdinalResults", StringComparison.Ordinal)));
		members.AddRange(Of(typeof(CommandExtensions), m => m.Name.StartsWith("FirstOrdinalResults", StringComparison.Ordinal)));
		members.AddRange(Of(typeof(IExecuteReaderExtensions), m => m.Name.StartsWith("FirstOrdinalResults", StringComparison.Ordinal)));
		members.AddRange(typeof(ExpressiveDbCommandBase<,,,,>).GetMethods(BindingFlags.Public | BindingFlags.Instance).Where(m => m.Name.StartsWith("FirstOrdinalResults", StringComparison.Ordinal)));

		Assert.Equal(24, members.Count);
		Assert.All(members, m => Assert.True(IsQueueReturn(m.ReturnType), $"{m.DeclaringType!.Name}.{m} returns {m.ReturnType}"));
	}

	// ---- mapped rows ----

	[Fact]
	public static void ResultsBuffered_ReturnsTheQueue_AndEnumeratingDoesNotEmptyIt()
	{
		using var reader = Simples();

		Queue<Simple> queue = reader.ResultsBuffered<Simple>();

		Assert.Equal(3, reader.RowsRead); // the rows are read before it returns
		Assert.Equal(3, queue.Count);
		Assert.Equal([1, 2, 3], queue.Select(s => s.Id));
		Assert.Equal([1, 2, 3], queue.Select(s => s.Id)); // a second pass sees the same rows
		Assert.Equal(1, queue.First().Id);
		Assert.Equal(3, queue.Count);
		AssertReplayable(queue, 3);
	}

	[Fact]
	public static void ResultsBuffered_WithAndWithoutAMap_BothReturnAQueue()
	{
		using var reader = Simples();
		using var other = Simples();

		Assert.IsType<Queue<Simple>>(reader.ResultsBuffered<Simple>(RowMap<Simple>.Default));
		Assert.IsType<Queue<Simple>>(other.ResultsBuffered<Simple>());
	}

	[Fact]
	public static void Dequeue_ReleasesRows_AndTheRestEnumeratesInOrder()
	{
		using var reader = Simples();
		Queue<Simple> queue = reader.ResultsBuffered<Simple>();

		Assert.Equal(1, queue.Dequeue().Id);

		Assert.Equal(2, queue.Count);
		Assert.Equal([2, 3], queue.Select(s => s.Id));
	}

	[Fact]
	public static void ResultsOnACommand_ReturnsAQueue()
	{
		IDbCommand first = DbCommandOver(Simples);
		IDbCommand second = DbCommandOver(Simples);
		Queue<Simple> viaProbe = first.Results<Simple>();
		Queue<Simple> mapped = second.Results<Simple>(RowMap<Simple>.Default);

		AssertReplayable(viaProbe, 3);
		AssertReplayable(mapped, 3);
	}

	[Fact]
	public static async Task ResultsBufferedAsync_AllShapes_ReturnAQueue()
	{
		var ct = TestContext.Current.CancellationToken;
		foreach (bool useReadAsync in new[] { true, false })
		{
			await using var reader = Simples();
			Queue<Simple> queue = await reader.ResultsBufferedAsync<Simple>(null, useReadAsync, ct);
			AssertReplayable(queue, 3);
		}

		await using (var a = Simples()) AssertReplayable(await a.ResultsBufferedAsync<Simple>(ct), 3);
		await using var b = Simples();
		AssertReplayable(await b.ResultsBufferedAsync<Simple>(RowMap<Simple>.Default, ct), 3);
	}

	[Fact]
	public static async Task ResultsAsyncOnADbCommand_AllShapes_ReturnAQueue()
	{
		var ct = TestContext.Current.CancellationToken;
		AssertReplayable(await DbCommandOver(Simples).ResultsAsync<Simple>(cancellationToken: ct), 3);
		AssertReplayable(await DbCommandOver(Simples).ResultsAsync<Simple>(RowMap<Simple>.Default, false, ct), 3);
		AssertReplayable(await DbCommandOver(Simples).ResultsAsync<Simple>(RowMap<Simple>.Default, ct), 3);
		AssertReplayable(await DbCommandOver(Simples).ResultsAsync<Simple>(ct), 3);
	}

	[Fact]
	public static async Task ResultsBufferedAsync_ACancelledToken_Cancels()
	{
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		await using var reader = Simples();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ResultsBufferedAsync<Simple>(cts.Token));
	}

	// ---- the obsolete-bound members still return their rows ----

#pragma warning disable CS0618 // Kept covered until 12.
	[Fact]
	public static void ObsoleteIExecuteReaderResults_StillReturnsItsRows()
	{
		var command = new FakeExecuteReader(Simples);

		IEnumerable<Simple> rows = command.Results<Simple>();

		Assert.Equal([1, 2, 3], rows.Select(s => s.Id));
		Assert.Equal([1, 2, 3], rows.Select(s => s.Id));
	}
#pragma warning restore CS0618

	// ---- first-column values ----

	static void AssertNullsNotDBNull(Queue<object?> queue)
	{
		Assert.Equal([1, null, 3], queue.ToArray());
		Assert.DoesNotContain(queue, v => v is DBNull);
		AssertReplayable(queue, 3);
	}

	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static void FirstOrdinalResults_OnAReader_HoldsNullNotDBNull(bool providerReader)
	{
		IDataReader reader = providerReader ? Ids() : FakeReader.Create(["Id"], IdRows);

		Queue<object?> queue = reader.FirstOrdinalResults();

		if (reader is TypedReader typed) Assert.Equal(queue.Count, typed.RowsRead);
		AssertNullsNotDBNull(queue);
	}

	[Fact]
	public static void FirstOrdinalResults_OnACommandAndAnExecuteReader_HoldNullNotDBNull()
	{
		AssertNullsNotDBNull(ProbeOver(() => FakeReader.Create(["Id"], IdRows)).Command.FirstOrdinalResults());
		AssertNullsNotDBNull(DbCommandOver(Ids).FirstOrdinalResults<object?>());
		AssertNullsNotDBNull(new FakeExecuteReader(Ids).FirstOrdinalResults());
		AssertNullsNotDBNull(new PlainReaderCommand(() => FakeReader.Create(["Id"], IdRows)).FirstOrdinalResults());
	}

	[Fact]
	public static async Task FirstOrdinalResultsAsync_HoldsNullNotDBNull()
	{
		var ct = TestContext.Current.CancellationToken;
		foreach (bool useReadAsync in new[] { true, false })
		{
			await using var reader = Ids();
			AssertNullsNotDBNull(await reader.FirstOrdinalResultsAsync(useReadAsync, ct));
		}

		AssertNullsNotDBNull(await DbCommandOver(Ids).FirstOrdinalResultsAsync(CommandBehavior.Default, true, ct));
	}

	[Fact]
	public static async Task FirstOrdinalResultsAsyncOfT0_ReturnsAQueue()
	{
		var ct = TestContext.Current.CancellationToken;
		foreach (bool useReadAsync in new[] { true, false })
		{
			await using var reader = Ids();
			Queue<int?> queue = await reader.FirstOrdinalResultsAsync<int?>(useReadAsync, ct);
			Assert.Equal([1, null, 3], queue.ToArray());
			AssertReplayable(queue, 3);
		}

		Queue<int?> viaCommand = await DbCommandOver(Ids).FirstOrdinalResultsAsync<int?>(CommandBehavior.Default, false, ct);
		Assert.Equal([1, null, 3], viaCommand.ToArray());
	}

	[Fact]
	public static void FirstOrdinalResultsOfT0_ReturnsAQueue_OnEveryReceiver()
	{
		Assert.Equal([1, null, 3], Ids().FirstOrdinalResults<int?>().ToArray());
		Assert.Equal([1, null, 3], FakeReader.Create(["Id"], IdRows).FirstOrdinalResults<int?>().ToArray());
		Assert.Equal([1, null, 3], ProbeOver(() => FakeReader.Create(["Id"], IdRows)).Command.FirstOrdinalResults<int?>().ToArray());
		Assert.Equal([1, null, 3], DbCommandOver(Ids).FirstOrdinalResults<int?>().ToArray());
		Assert.Equal([1, null, 3], new FakeExecuteReader(Ids).FirstOrdinalResults<int?>().ToArray());
	}

	// The cast is done while the queue is built: a bad cast on the non-provider paths throws at the call, not at enumeration.
	[Fact]
	public static void FirstOrdinalResultsOfT0_NonProviderReader_ThrowsAtTheCall_OnABadCast()
	{
		Assert.ThrowsAny<Exception>(() => FakeReader.Create(["Id"], IdRows).FirstOrdinalResults<int>());
		Assert.Throws<InvalidCastException>(() => FakeReader.Create(["Id"], [1], [2]).FirstOrdinalResults<string>());
		Assert.ThrowsAny<Exception>(() => ProbeOver(() => FakeReader.Create(["Id"], IdRows)).Command.FirstOrdinalResults<int>());
		Assert.Throws<InvalidCastException>(() => ProbeOver(() => FakeReader.Create(["Id"], [1])).Command.FirstOrdinalResults<string>());
	}

	[Fact]
	public static void FirstOrdinalResultsOfT0_NonProviderReader_NullReader_Throws()
		=> Assert.Throws<ArgumentNullException>(() => ((IDataReader)null!).FirstOrdinalResults<int>());
}