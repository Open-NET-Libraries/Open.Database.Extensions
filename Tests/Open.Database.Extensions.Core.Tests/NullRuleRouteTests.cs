using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The NULL rule with no policy, on every route: a NULL into a non-nullable member throws ValueMappingException naming the model and the 0-based row of the result set,
// with the provider's error inside; the provider's own failures and cancellation pass through as they are.
[ExcludeFromCodeCoverage]
[Collection("LocalDB DDL")]
public static class NullRuleRouteTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public sealed class Plain
	{
		public int Id { get; set; }
	}

	static FakeDbCommand Command(params object?[][] rows)
		=> new(new FakeDbConnection(), () => TypedReader.Create([new ColumnDef("Id", typeof(int))], rows));

	public sealed class Tagged
	{
		public object Tag { get; set; } = "";
		public object? Maybe { get; set; } = "";
	}

	// A non-nullable object is like a non-nullable string: a NULL throws (unchecked, it would get DBNull.Value), and object? gets null.
	[Theory]
	[InlineData(typeof(string))]
	[InlineData(typeof(object))]
	public static void ANonNullableObject_RejectsANull_LikeAString(Type columnType)
	{
		FakeDbCommand Read(string column, params object?[][] rows)
			=> new(new FakeDbConnection(), () => TypedReader.Create([new ColumnDef(column, columnType)], rows));

		object?[] nullRow = [null];
		NullAssert.Model<Tagged>(1, () => Read("Tag", ["a"], nullRow).Map<Tagged>().ToList());
		Assert.Equal("a", Assert.Single(Read("Tag", ["a"]).Map<Tagged>().ToList()).Tag);
		Assert.Null(Assert.Single(Read("Maybe", nullRow).Map<Tagged>().ToList()).Maybe);
	}

	[Fact]
	public static async Task EveryRoute_NamesTheModelAndTheRow()
	{
		object?[] nullRow = [null];
		NullAssert.Model<Plain>(1, () => Command([1], [null]).Map<Plain>().ToList());
		await NullAssert.ModelAsync<Plain>(1, async () => await Command([1], [null]).Map<Plain>().ToListAsync(Ct));
		await NullAssert.ModelAsync<Plain>(1, async () => await Command([1], [null]).Map<Plain>().AsyncReads().ToListAsync(Ct));
		NullAssert.Model<Plain>(0, () => Command(nullRow).Map<Plain>().First());
		await NullAssert.ModelAsync<Plain>(0, async () => await Command(nullRow).Map<Plain>().SingleAsync(Ct));
		await NullAssert.ModelAsync<Plain>(2, async () =>
		{
			await foreach (Plain _ in Command([1], [2], [null]).Map<Plain>().Stream(Timeout.InfiniteTimeSpan, Ct)) { }
		});
		await NullAssert.ModelAsync<Plain>(2, async () =>
		{
			await foreach (Plain _ in Command([1], [2], [null]).Map<Plain>().AsyncReads().Stream(Timeout.InfiniteTimeSpan, Ct)) { }
		});
		await NullAssert.ModelAsync<Plain>(1, async () =>
		{
			await foreach (Plain _ in Command([1], [null]).Map<Plain>().ToChannelReader(Ct).ReadAllAsync(Ct)) { }
		});
		await NullAssert.ModelAsync<Plain>(1, () => Command([1], [null]).Map<Plain>().AsSourceBlock(Ct).Completion);
		NullAssert.Model<ChannelRow>(1, () => Command([1], [null]).Map<ChannelRow>().ToList()); // through the constructor
	}

	[Fact]
	public static void AnArrayMapper_NamesNoRow_ItsCallerOwnsTheLoop()
	{
		Func<object?[], Plain> map = RowMap<Plain>.Default.ArrayMapper(["Id"]);
		Assert.Equal(3, map([3]).Id);
		NullAssert.Check<Plain, System.Data.SqlTypes.SqlNullValueException>(
			"A NULL was read into a non-nullable member of Plain. Declare the member nullable (int?, string?).",
			Assert.Throws<ValueMappingException>(() => map([null])));
		NullAssert.Check<Plain, FormatException>(
			"Unable to map a row to 'Plain'.",
			Assert.Throws<ValueMappingException>(() => map(["x"])));
	}

	[Fact]
	public static void TheRow_CountsFromTheStartOfTheResultSet_NotFromTheSkip()
	{
		NullAssert.Model<Plain>(2, () => Command([1], [2], [null]).Map<Plain>().Skip(1).ToList());
		NullAssert.Model<Plain>(2, () => Command([1], [2], [null]).Map<Plain>().Skip(2).First());
	}

	sealed class ProviderFailure() : DbException("the provider failed");

	// A reader whose typed read of Id fails as the provider would.
	sealed class FailingReader(Exception failure) : TypedReader([new ColumnDef("Id", typeof(int))], [[1]])
	{
		public override int GetInt32(int ordinal) => throw failure;
	}

	[Fact]
	public static void TheProvidersOwnFailure_AndCancellation_PassThrough()
	{
		var provider = new ProviderFailure();
		Assert.Same(provider, Assert.Throws<ProviderFailure>(
			() => new FakeDbCommand(new FakeDbConnection(), () => new FailingReader(provider)).Map<Plain>().ToList()));

		var canceled = new OperationCanceledException();
		Assert.Same(canceled, Assert.Throws<OperationCanceledException>(
			() => new FakeDbCommand(new FakeDbConnection(), () => new FailingReader(canceled)).Map<Plain>().ToList()));

		NullAssert.Check<Plain, InvalidOperationException>(
			"Unable to map row 0 to 'Plain'.",
			Assert.Throws<ValueMappingException>(
				() => new FakeDbCommand(new FakeDbConnection(), () => new FailingReader(new InvalidOperationException())).Map<Plain>().ToList()));
	}

	// Microsoft.Data.Sqlite's typed getters throw InvalidOperationException on NULL: the general message, with that error inside.
	[Fact]
	public static void Sqlite_ANull_IsTheGeneralMessage()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "select 1 as Id union all select null";

		NullAssert.Check<Plain, InvalidOperationException>(
			"Unable to map row 1 to 'Plain'.",
			Assert.Throws<ValueMappingException>(() => command.Map<Plain>().ToList()));
	}

	// SqlClient's typed getters throw SqlNullValueException on NULL: the NULL message.
	[Fact]
	public static async Task SqlClient_ANull_IsTheNullMessage()
	{
		LocalDbFixture.SkipUnlessAvailable();
		await using SqlConnection connection = LocalDbFixture.Create();

		await NullAssert.ModelAsync<Plain>(1, async () => await connection
			.Command("select Id from (values (1, 0), (cast(null as int), 1)) as v(Id, o) order by o")
			.Map<Plain>()
			.ToListAsync(Ct));
	}
}
