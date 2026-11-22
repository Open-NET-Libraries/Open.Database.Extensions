using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The map-first definition: DbQuery.Map<T>() gives an immutable definition with no connection yet;
// Using(source) gives the ordinary MappedQuery<T> view with the definition's options applied, and the parameters
// can be added before the source (UnboundCall) or after it, with the same command either way.
// What it pins: the same rows as the chain that maps after the source; every source (connection, transaction,
// pool, factory) with the ownership rules of DbQuery; the definition-time options; both orders of params; the typed
// form; a default struct throws; and the definition is never changed by what is built from it.
[ExcludeFromCodeCoverage]
public static class MappedDbQueryTests
{
	sealed record Post(int Id, int AuthorId, string Title);

	readonly record struct AuthorViews(int AuthorId, int Views);

	const string PostsSql
		= "select Id, AuthorId, Title from Posts where AuthorId = @authorId and Views >= @views order by Id";

	const string PostsTypedSql
		= "select Id, AuthorId, Title from Posts where AuthorId = @AuthorId and Views >= @Views order by Id";

	const string AllPostsSql = "select Id, AuthorId, Title from Posts order by Id";

	const string LaterFailure = "select 1; select abs(-9223372036854775808)";

	static readonly DbQuery Posts = DbQuery.Text(PostsSql);

	static readonly DbQuery<AuthorViews> PostsTyped = DbQuery.Text(PostsTypedSql).Params<AuthorViews>();

	// The definitions, as a command factory would hold them.
	static readonly MappedDbQuery<Post> PostsMapped = Posts.Map<Post>();

	static readonly MappedDbQuery<AuthorViews, Post> PostsTypedMapped = PostsTyped.Map<Post>();

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	sealed class Db : IDisposable
	{
		static int _count;

		readonly SqliteConnection _keeper;

		public Db()
		{
			ConnectionString = $"Data Source=mappeddbquery{Interlocked.Increment(ref _count)};Mode=Memory;Cache=Shared";
			_keeper = new SqliteConnection(ConnectionString);
			_keeper.Open();
			using SqliteCommand command = _keeper.CreateCommand();
			command.CommandText = """
				create table Posts (Id integer, AuthorId integer, Title text, Views integer);
				insert into Posts values (1, 7, 'a', 10), (2, 7, 'b', 50), (3, 8, 'c', 60), (4, 7, 'd', 90);
				""";
			command.ExecuteNonQuery();
		}

		public string ConnectionString { get; }

		public Tracked Connect() => new(ConnectionString);

		public void Dispose() => _keeper.Dispose();
	}

	sealed class Tracked(string connectionString) : SqliteConnection(connectionString)
	{
		public int Opens { get; private set; }

		public int Closes { get; private set; }

		public bool DisposeCalled { get; private set; }

		public override void Open()
		{
			Opens++;
			base.Open();
		}

		public override void Close()
		{
			// A close of a connection that is already closed is not a close.
			if (State != ConnectionState.Closed) Closes++;
			base.Close();
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing) DisposeCalled = true;
			base.Dispose(disposing);
		}
	}

	sealed class Factory(Db db) : IDbConnectionFactory<Tracked>
	{
		public List<Tracked> Made { get; } = [];

		public Tracked Create()
		{
			Tracked connection = db.Connect();
			Made.Add(connection);
			return connection;
		}

		IDbConnection IDbConnectionFactory.Create() => Create();
	}

	sealed class Pool(Db db) : IDbConnectionPool<Tracked>
	{
		public List<Tracked> Taken { get; } = [];

		public List<Tracked> Given { get; } = [];

		public Tracked Take()
		{
			Tracked connection = db.Connect();
			Taken.Add(connection);
			return connection;
		}

		IDbConnection IDbConnectionPool.Take() => Take();

		public void Give(IDbConnection connection) => Given.Add((Tracked)connection);
	}

	static readonly int[] SevenFromFifty = [2, 4];

	static int[] Ids(IEnumerable<Post> posts) => [.. posts.Select(static p => p.Id)];

	// ---- what a definition is ----

	[Fact]
	public static void Map_IsImmutable_AndEachOptionBranches()
	{
		MappedDbQuery<Post> single = PostsMapped.SingleResult();
		MappedDbQuery<Post> async = PostsMapped.AsyncReads();
		MappedDbQuery<Post> pooled = PostsMapped.UsePool(new CountingPool<Post>(), false);

		Assert.NotSame(PostsMapped, single);
		Assert.NotSame(PostsMapped, async);
		Assert.NotSame(PostsMapped, pooled);
		Assert.NotSame(single, async);
	}

	[Fact]
	public static void MapOfNullMap_ReadsAsMapOfT()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		Assert.Equal(
			[1, 2, 3, 4],
			Ids(DbQuery.Text(AllPostsSql).Map<Post>((RowMap<Post>?)null).Using(c).ToList()));
	}

	[Fact]
	public static void MapOfASingleValue_ReadsTheFirstColumn()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		Assert.Equal(
			[1L, 2L, 3L, 4L],
			DbQuery.Text("select Id from Posts order by Id").Map<long>().Using(c).ToList());
	}

	[Fact]
	public static void MapOfASelector_ReadsWithIt()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		Assert.Equal(
			["a", "b", "c", "d"],
			DbQuery.Text(AllPostsSql).Map(static r => r.GetString(2)).Using(c).ToList());
	}

	[Fact]
	public static void MapOfANullSelector_IsRefused()
		=> Assert.Equal(
			"selector",
			Assert.Throws<ArgumentNullException>(
				() => DbQuery.Text(AllPostsSql).Map<Post>((Func<IDataRecord, Post>)null!)).ParamName);

	// ---- every source ----

	[Fact]
	public static async Task Using_ADbConnection_ReadsTheRows_OpenedLateAndClosedAgain()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		List<Post> posts = await DbQuery.Text(AllPostsSql).Map<Post>().Using(c).ToListAsync(Ct);

		Assert.Equal([1, 2, 3, 4], Ids(posts));
		Assert.Equal(1, c.Opens);
		Assert.Equal(1, c.Closes);
		Assert.False(c.DisposeCalled);
	}

	[Fact]
	public static async Task Using_AFactory_DisposesWhatItMade()
	{
		using var db = new Db();
		var factory = new Factory(db);

		List<Post> posts = await DbQuery.Text(AllPostsSql).Map<Post>().Using(factory).ToListAsync(Ct);

		Assert.Equal([1, 2, 3, 4], Ids(posts));
		Tracked made = Assert.Single(factory.Made);
		Assert.True(made.DisposeCalled);
	}

	[Fact]
	public static async Task Using_APool_GivesBackWhatItTook()
	{
		using var db = new Db();
		var pool = new Pool(db);

		List<Post> posts = await DbQuery.Text(AllPostsSql).Map<Post>().Using(pool).ToListAsync(Ct);

		Assert.Equal([1, 2, 3, 4], Ids(posts));
		Assert.Same(Assert.Single(pool.Taken), Assert.Single(pool.Given));
	}

	[Fact]
	public static async Task Using_ATransaction_ReadsInsideIt_AndLeavesTheConnectionOpen()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		await c.OpenAsync(Ct);
		using DbTransaction transaction = await c.BeginTransactionAsync(Ct);

		// A row only this transaction can see (and the provider refuses a command that is not enlisted in a pending
		// transaction), so a read that sees it ran inside the transaction.
		await using (DbCommand insert = c.CreateCommand())
		{
			insert.Transaction = transaction;
			insert.CommandText = "insert into Posts (Id, AuthorId, Title, Views) values (99, 1, 'inside', 0)";
			await insert.ExecuteNonQueryAsync(Ct);
		}

		List<Post> posts = await DbQuery.Text(AllPostsSql).Map<Post>().Using(transaction).ToListAsync(Ct);

		Assert.Equal([1, 2, 3, 4, 99], Ids(posts));
		Assert.Equal(ConnectionState.Open, c.State);

		await transaction.RollbackAsync(Ct);
		List<Post> afterRollback = await DbQuery.Text(AllPostsSql).Map<Post>().Using(c).ToListAsync(Ct);
		Assert.Equal([1, 2, 3, 4], Ids(afterRollback));
	}

	[Fact]
	public static async Task AFailingRead_ThroughAFactoryOrAPool_StillReleasesWhatTheSourceOwns()
	{
		using var db = new Db();
		var factory = new Factory(db);
		var pool = new Pool(db);
		MappedDbQuery<Post> broken = DbQuery.Text("select Id from NoSuchTable").Map<Post>();

		await Assert.ThrowsAsync<SqliteException>(async () => await broken.Using(factory).ToListAsync(Ct));
		await Assert.ThrowsAsync<SqliteException>(async () => await broken.Using(pool).ToListAsync(Ct));

		Assert.True(Assert.Single(factory.Made).DisposeCalled);
		Assert.Same(Assert.Single(pool.Taken), Assert.Single(pool.Given));
	}

	[Fact]
	public static void Using_ANullSource_IsRefused()
	{
		MappedDbQuery<Post> mapped = DbQuery.Text(AllPostsSql).Map<Post>();

		Assert.Equal("connection", Assert.Throws<ArgumentNullException>(() => mapped.Using((DbConnection)null!)).ParamName);
		Assert.Equal("transaction", Assert.Throws<ArgumentNullException>(() => mapped.Using((DbTransaction)null!)).ParamName);
		Assert.Equal("pool", Assert.Throws<ArgumentNullException>(() => mapped.Using((IDbConnectionPool<Tracked>)null!)).ParamName);
		Assert.Equal("factory", Assert.Throws<ArgumentNullException>(() => mapped.Using((IDbConnectionFactory<Tracked>)null!)).ParamName);
	}

	// ---- the options of the definition reach every view it makes ----

	[Fact]
	public static async Task SingleResult_OnTheDefinition_StopsAfterTheFirstSet()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		MappedDbQuery<int> definition = DbQuery.Text(LaterFailure).Map<int>();

		await Assert.ThrowsAsync<SqliteException>(async () => await definition.Using(c).ToListAsync(Ct));
		Assert.Equal([1], await definition.SingleResult().Using(c).ToListAsync(Ct));
	}

	[Fact]
	public static void TheViewOfADefinition_IsTheViewOfTheClassicChain()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		Assert.Equal(
			DbQuery.Text(AllPostsSql).Using(c).Map<Post>().SingleResult().AsyncReads(),
			DbQuery.Text(AllPostsSql).Map<Post>().SingleResult().AsyncReads().Using(c));
		Assert.NotEqual(
			DbQuery.Text(AllPostsSql).Using(c).Map<Post>(),
			DbQuery.Text(AllPostsSql).Map<Post>().SingleResult().Using(c));
	}

	[Fact]
	public static async Task UsePool_OnTheDefinition_RentsFromThePool_AndEveryArrayGoesBackCleared()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		var pool = new CountingPool<Post>
		{
			RoundUp = true,
			IsDirty = static a => a.Any(p => p is not null),
		};

		List<Post> posts = await DbQuery.Text(AllPostsSql).Map<Post>().UsePool(pool, true).Using(c).ToListAsync(Ct);

		Assert.Equal([1, 2, 3, 4], Ids(posts));
		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static void UsePool_OfANullPool_IsRefused()
		=> Assert.Equal(
			"pool",
			Assert.Throws<ArgumentNullException>(
				() => DbQuery.Text(AllPostsSql).Map<Post>().UsePool(null!, false)).ParamName);

	// ---- params before the source ----

	[Fact]
	public static async Task ParamsBeforeUsing_GiveTheRowsOfTheClassicChain()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		List<Post> classic = await Posts.Using(c).AddParam("authorId", 7).AddParam("views", 50).Map<Post>().ToListAsync(Ct);
		List<Post> mapFirst = await PostsMapped.AddParam("authorId", 7).AddParam("views", 50).Using(c).ToListAsync(Ct);

		Assert.Equal(SevenFromFifty, Ids(classic));
		Assert.Equal(Ids(classic), Ids(mapFirst));
	}

	[Fact]
	public static void ParamsBeforeUsing_AreReplayedInTheOrderAdded()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		var replayed = (DbQueryCall)PostsMapped.AddParam("authorId", 7).AddParam("views", 50).Using(c).Command;
		DbQueryCall sameOrder = Posts.Using(c).AddParam("authorId", 7).AddParam("views", 50);
		DbQueryCall otherOrder = Posts.Using(c).AddParam("views", 50).AddParam("authorId", 7);

		// A call is equal to another only with the same values in the same order.
		Assert.Equal(sameOrder, replayed);
		Assert.NotEqual(otherOrder, replayed);
	}

	[Fact]
	public static void ThreeValuesHeldBeforeUsing_AreReplayedOldestFirst()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		var replayed = (DbQueryCall)PostsMapped.AddParam("a", 1).AddParam("b", 2L).AddParam("c", "three").Using(c).Command;
		DbQueryCall classic = Posts.Using(c).AddParam("a", 1).AddParam("b", 2L).AddParam("c", "three");
		DbQueryCall reordered = Posts.Using(c).AddParam("c", "three").AddParam("b", 2L).AddParam("a", 1);
		DbQueryCall swappedMiddle = Posts.Using(c).AddParam("a", 1).AddParam("c", "three").AddParam("b", 2L);

		Assert.Equal(classic, replayed);
		Assert.NotEqual(reordered, replayed);
		Assert.NotEqual(swappedMiddle, replayed);
	}

	[Fact]
	public static async Task ParamsBeforeUsing_NamedFromTheVariables_GiveTheRows()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		// The same command either way: one named like the SQL text, one derived from the variable.
		int authorId = 7;
		int views = 50;
		List<Post> derived = await PostsMapped.AddParam(authorId).AddParam(views).Using(c).ToListAsync(Ct);

		Assert.Equal(SevenFromFifty, Ids(derived));
	}

	[Fact]
	public static async Task ParamsBeforeUsing_AStringValue_AndAnObjectOfParams()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		MappedDbQuery<Post> byTitle = DbQuery.Text("select Id, AuthorId, Title from Posts where Title = @title").Map<Post>();

		List<Post> fromString = await byTitle.AddParam("title", "b").Using(c).ToListAsync(Ct);
		List<Post> fromNullableString = await byTitle.AddParam("title", (string?)"b").Using(c).ToListAsync(Ct);
		List<Post> fromObject = await PostsMapped.AddParams(new { authorId = 7, views = 50 }).Using(c).ToListAsync(Ct);

		Assert.Equal([2], Ids(fromString));
		Assert.Equal([2], Ids(fromNullableString));
		Assert.Equal([2, 4], Ids(fromObject));
	}

	// ---- params after the source ----

	[Fact]
	public static async Task ParamsAfterUsing_GiveTheRowsOfBothOtherOrders()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		int authorId = 7;
		int views = 50;

		List<Post> before = await PostsMapped.AddParam("authorId", 7).AddParam("views", 50).Using(c).ToListAsync(Ct);
		List<Post> named = await PostsMapped.Using(c).AddParam("authorId", 7).AddParam("views", 50).ToListAsync(Ct);
		List<Post> derived = await PostsMapped.Using(c).AddParam(authorId).AddParam(views).ToListAsync(Ct);
		List<Post> members = await PostsMapped.Using(c).AddParams(new { authorId = 7, views = 50 }).ToListAsync(Ct);

		Assert.Equal(SevenFromFifty, Ids(before));
		Assert.Equal(SevenFromFifty, Ids(named));
		Assert.Equal(SevenFromFifty, Ids(derived));
		Assert.Equal(SevenFromFifty, Ids(members));
	}

	[Fact]
	public static async Task ParamsAfterUsing_AStringValue_AndAMixOfBothOrders()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		MappedDbQuery<Post> byTitle = DbQuery.Text("select Id, AuthorId, Title from Posts where Title = @title").Map<Post>();

		List<Post> fromString = await byTitle.Using(c).AddParam("title", "b").ToListAsync(Ct);
		List<Post> fromNullableString = await byTitle.Using(c).AddParam("title", (string?)"b").ToListAsync(Ct);
		List<Post> mixed = await PostsMapped.AddParam("authorId", 7).Using(c).AddParam("views", 50).ToListAsync(Ct);

		Assert.Equal([2], Ids(fromString));
		Assert.Equal([2], Ids(fromNullableString));
		Assert.Equal(SevenFromFifty, Ids(mixed));
	}

	[Fact]
	public static async Task ParamsAfterUsing_KeepTheOptions_AndLeaveTheViewItCameFromUnchanged()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		MappedDbQuery<int> definition = DbQuery.Text("select @a; select abs(-9223372036854775808)").Map<int>();

		// SingleResult on the definition survives the added parameter: the failing second set is never read.
		List<int> single = await definition.SingleResult().Using(c).AddParam("a", 5).ToListAsync(Ct);
		await Assert.ThrowsAsync<SqliteException>(async () => await definition.Using(c).AddParam("a", 5).ToListAsync(Ct));

		MappedQuery<int> view = definition.SingleResult().Using(c);
		MappedQuery<int> withParam = view.AddParam("a", 5);

		Assert.Equal([5], single);
		Assert.NotEqual(view, withParam);
		Assert.IsType<DbQueryUsing>(view.Command);
		Assert.IsType<DbQueryCall>(withParam.Command, exactMatch: false);
	}

	[Fact]
	public static void ParamsAfterUsing_OnAViewNotMadeFromADefinition_Throw()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		MappedQuery<int> classic = c.Command("select 1").Map<int>();

		const string expected
			= "AddParam after Map applies only to a query made from a DbQuery definition; for a command, call AddParam before Map.";
		int a = 1;

		Assert.Equal(expected, Assert.Throws<InvalidOperationException>(() => classic.AddParam("a", 1)).Message);
		Assert.Equal(expected, Assert.Throws<InvalidOperationException>(() => classic.AddParam("a", "text")).Message);
		Assert.Equal(expected, Assert.Throws<InvalidOperationException>(() => classic.AddParam(a)).Message);
		Assert.Equal(expected, Assert.Throws<InvalidOperationException>(() => classic.AddParams(new { a = 1 })).Message);
	}

	[Fact]
	public static async Task AnUnboundCall_IsNotChangedByWhatIsAddedToIt()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		UnboundCall<Post> authorOnly = PostsMapped.AddParam("authorId", 7);
		UnboundCall<Post> fifty = authorOnly.AddParam("views", 50);
		UnboundCall<Post> ninety = authorOnly.AddParam("views", 90);

		List<Post> fiftyRows = await fifty.Using(c).ToListAsync(Ct);
		List<Post> ninetyRows = await ninety.Using(c).ToListAsync(Ct);

		Assert.Equal(SevenFromFifty, Ids(fiftyRows));
		Assert.Equal([4], Ids(ninetyRows));
	}

	[Fact]
	public static async Task AnUnboundCall_GivenEachKindOfSource()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		var factory = new Factory(db);
		var pool = new Pool(db);
		UnboundCall<Post> call = PostsMapped.AddParam("authorId", 7).AddParam("views", 50);

		List<Post> fromConnection = await call.Using(c).ToListAsync(Ct);
		List<Post> fromFactory = await call.Using(factory).ToListAsync(Ct);
		List<Post> fromPool = await call.Using(pool).ToListAsync(Ct);

		// A connection with a pending transaction is read only through that transaction.
		await c.OpenAsync(Ct);
		using DbTransaction transaction = await c.BeginTransactionAsync(Ct);
		List<Post> fromTransaction = await call.Using(transaction).ToListAsync(Ct);

		Assert.Equal(SevenFromFifty, Ids(fromConnection));
		Assert.Equal(SevenFromFifty, Ids(fromFactory));
		Assert.Equal(SevenFromFifty, Ids(fromPool));
		Assert.Equal(SevenFromFifty, Ids(fromTransaction));
		Assert.True(Assert.Single(factory.Made).DisposeCalled);
		Assert.Same(Assert.Single(pool.Taken), Assert.Single(pool.Given));
	}

	[Fact]
	public static void AnUnboundCall_ARefusedSource_NamesIt()
	{
		UnboundCall<Post> call = PostsMapped.AddParam("authorId", 7);

		Assert.Equal("connection", Assert.Throws<ArgumentNullException>(() => call.Using((DbConnection)null!)).ParamName);
		Assert.Equal("transaction", Assert.Throws<ArgumentNullException>(() => call.Using((DbTransaction)null!)).ParamName);
		Assert.Equal("pool", Assert.Throws<ArgumentNullException>(() => call.Using((IDbConnectionPool<Tracked>)null!)).ParamName);
		Assert.Equal("factory", Assert.Throws<ArgumentNullException>(() => call.Using((IDbConnectionFactory<Tracked>)null!)).ParamName);
	}

	[Fact]
	public static void AParamWithoutAName_IsRefused()
		=> Assert.Equal(
			"name",
			Assert.Throws<ArgumentNullException>(() => PostsMapped.AddParam(null!, 1)).ParamName);

	// ---- the typed form ----

	[Fact]
	public static async Task TypedParams_BeforeAndAfterUsing_GiveTheRowsOfTheClassicChain()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		var values = new AuthorViews(7, 50);

		List<Post> classic = await PostsTyped.Using(c).AddParams(values).Map<Post>().ToListAsync(Ct);
		List<Post> before = await PostsTypedMapped.AddParams(values).Using(c).ToListAsync(Ct);
		List<Post> after = await PostsTypedMapped.Using(c).AddParams(values).ToListAsync(Ct);

		Assert.Equal(SevenFromFifty, Ids(classic));
		Assert.Equal(Ids(classic), Ids(before));
		Assert.Equal(Ids(classic), Ids(after));
	}

	[Fact]
	public static async Task TypedDefinition_CarriesItsOptions()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		MappedDbQuery<AuthorViews, int> definition = DbQuery.Text(LaterFailure).Params<AuthorViews>().Map<int>();

		await Assert.ThrowsAsync<SqliteException>(
			async () => await definition.Using(c).AddParams(default).ToListAsync(Ct));
		Assert.Equal([1], await definition.SingleResult().Using(c).AddParams(default).ToListAsync(Ct));
		Assert.Equal([1], await definition.SingleResult().AddParams(default).Using(c).ToListAsync(Ct));
	}

	// ---- a default struct cannot be used ----

	[Fact]
	public static void ADefaultUnboundCall_Throws()
	{
		UnboundCall<Post> none = default;
		UnboundCall<AuthorViews, Post> typedNone = default;
		MappedDbQueryUsing<AuthorViews, Post> usingNone = default;

		int authorId = 7;

		// Every member of a default struct says so: none is left to a null reference.
		Assert.Throws<InvalidOperationException>(() => none.AddParam("authorId", 7));
		Assert.Throws<InvalidOperationException>(() => none.AddParam("title", "text"));
		Assert.Throws<InvalidOperationException>(() => none.AddParam(authorId));
		Assert.Throws<InvalidOperationException>(() => none.AddParams(new { authorId }));
		Assert.Throws<InvalidOperationException>(() => none.Using((DbConnection)null!));
		Assert.Throws<InvalidOperationException>(() => none.Using((DbTransaction)null!));
		Assert.Throws<InvalidOperationException>(() => none.Using((IDbConnectionPool<Tracked>)null!));
		Assert.Throws<InvalidOperationException>(() => none.Using((IDbConnectionFactory<Tracked>)null!));
		Assert.Throws<InvalidOperationException>(() => typedNone.Using((DbConnection)null!));
		Assert.Throws<InvalidOperationException>(() => typedNone.Using((DbTransaction)null!));
		Assert.Throws<InvalidOperationException>(() => typedNone.Using((IDbConnectionPool<Tracked>)null!));
		Assert.Throws<InvalidOperationException>(() => typedNone.Using((IDbConnectionFactory<Tracked>)null!));
		Assert.Throws<InvalidOperationException>(() => usingNone.AddParams(default));
	}

	// ---- nothing is shared between calls ----

	[Fact]
	public static async Task TheDefinition_IsUnchanged_ByTheCallsMadeFromIt()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		await PostsMapped.AddParam("authorId", 7).AddParam("views", 90).Using(c).ToListAsync(Ct);
		List<Post> other = await PostsMapped.AddParam("authorId", 8).AddParam("views", 0).Using(c).ToListAsync(Ct);

		Assert.Equal([3], Ids(other));
	}

	[Fact]
	public static async Task ConcurrentCalls_OfOneDefinition_DoNotShareState()
	{
		using var db = new Db();

		int[][] results = await Task.WhenAll(
			Enumerable.Range(0, 16).Select(async i =>
			{
				using Tracked c = db.Connect();
				int authorId = i % 2 == 0 ? 7 : 8;
				return Ids(await PostsMapped.AddParam("authorId", authorId).AddParam("views", 0).Using(c).ToListAsync(Ct));
			}));

		for (int i = 0; i < results.Length; i++)
			Assert.Equal(i % 2 == 0 ? [1, 2, 4] : [3], results[i]);
	}
}
