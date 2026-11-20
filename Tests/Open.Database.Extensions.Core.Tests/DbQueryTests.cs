using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The definition-first query on SQLite (a real provider, in memory).
// What it pins: a definition and a held call never change (they branch);
// values accumulate in the order added; the typed and the ad hoc form send
// the same parameters; derived names come from the variables; the timeout, the
// cancellation token and the transaction reach the command; every terminal
// reads through the later result sets; and the connection is opened late,
// closed only when this call opened it, disposed only when it came from a
// factory and given back only when it came from a pool.
[ExcludeFromCodeCoverage]
public static class DbQueryTests
{
	sealed record Post(int Id, int AuthorId, string Title);

	readonly record struct AuthorViews(int AuthorId, int Views);

	const string PostsSql
		= "select Id, AuthorId, Title from Posts where AuthorId = @authorId and Views >= @views order by Id";

	const string PostsTypedSql
		= "select Id, AuthorId, Title from Posts where AuthorId = @AuthorId and Views >= @Views order by Id";

	static readonly DbQuery Posts = DbQuery.Text(PostsSql);

	static readonly DbQuery<AuthorViews> PostsTyped
		= DbQuery.Text(PostsTypedSql).Params<AuthorViews>();

	// One in-memory database for the whole test, shared by every connection
	// made to it for as long as the keeper stays open.
	sealed class Db : IDisposable
	{
		static int _count;

		readonly SqliteConnection _keeper;

		public Db()
		{
			ConnectionString
				= $"Data Source=dbquery{Interlocked.Increment(ref _count)};Mode=Memory;Cache=Shared";
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

	// A connection that counts what is done to it and remembers its commands.
	sealed class Tracked(string connectionString) : SqliteConnection(connectionString)
	{
		public int Opens { get; private set; }

		public int Closes { get; private set; }

		public bool DisposeCalled { get; private set; }

		public List<DbCommand> Commands { get; } = [];

		public override void Open()
		{
			Opens++;
			base.Open();
		}

		public override void Close()
		{
			Closes++;
			base.Close();
		}

		protected override DbCommand CreateDbCommand()
		{
			DbCommand command = base.CreateDbCommand();
			Commands.Add(command);
			return command;
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

		System.Data.IDbConnection IDbConnectionFactory.Create() => Create();
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

		System.Data.IDbConnection IDbConnectionPool.Take() => Take();

		public void Give(System.Data.IDbConnection connection)
			=> Given.Add((Tracked)connection);
	}

	static int[] Ids(List<Post> posts) => [.. posts.Select(static p => p.Id)];

	// ---- the two forms ----

	[Fact]
	public static async Task AdHoc_NamesComeFromTheVariables()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		int authorId = 7;
		int views = 50;

		List<Post> posts = await Posts
			.Using(c)
			.AddParam(authorId)
			.AddParam(views)
			.Map<Post>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([2, 4], Ids(posts));
		Assert.Equal(
			["authorId", "views"],
			c.Commands[0].Parameters.Cast<DbParameter>().Select(static p => p.ParameterName));
	}

	sealed class Filter
	{
		public int AuthorId { get; init; }

		public int Views { get; init; }
	}

	[Fact]
	public static async Task AdHoc_AChainOfMembersIsNamedForTheLast()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		var filter = new Filter { AuthorId = 8, Views = 0 };

		List<Post> posts = await DbQuery
			.Text(PostsTypedSql)
			.Using(c)
			.AddParam(filter.AuthorId)
			.AddParam(filter.Views)
			.Map<Post>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([3], Ids(posts));
	}

	[Fact]
	public static void AdHoc_AnExpressionThatIsNotAVariableIsRefused()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		int n = 3;

		ArgumentException ex = Assert.Throws<ArgumentException>(
			() => Posts.Using(c).AddParam(n + 1));
		Assert.Contains("n + 1", ex.Message);
		Assert.Contains("AddParam(\"name\", value)", ex.Message);
	}

	[Fact]
	public static async Task Named_StringAndGenericOverloads()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		string author = "7";

		List<Post> posts = await DbQuery
			.Text("select Id, AuthorId, Title from Posts where AuthorId = @a and Title = @t and Views >= @v")
			.Using(c)
			.AddParam("a", author)
			.AddParam("t", "b")
			.AddParam("v", 50)
			.Map<Post>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([2], Ids(posts));
	}

	[Fact]
	public static async Task Typed_AddParams_SendsTheMembersByName()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		int authorId = 7;
		int views = 50;

		List<Post> posts = await PostsTyped
			.Using(c)
			.AddParams(new(AuthorId: authorId, Views: views))
			.Map<Post>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([2, 4], Ids(posts));
		Assert.Equal(
			["AuthorId", "Views"],
			c.Commands[0].Parameters.Cast<DbParameter>().Select(static p => p.ParameterName));
	}

	[Fact]
	public static async Task TheTwoForms_ReturnTheSameRows()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		int authorId = 7;
		int views = 10;

		List<Post> adHoc = await Posts
			.Using(c)
			.AddParam(authorId)
			.AddParam(views)
			.Map<Post>()
			.ToListAsync(TestContext.Current.CancellationToken);
		List<Post> typed = await PostsTyped
			.Using(c)
			.AddParams(new(authorId, views))
			.Map<Post>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(adHoc, typed);
		Assert.Equal([1, 2, 4], Ids(typed));
	}

	[Fact]
	public static async Task AQueryWithNoValues_MapsDirectlyOnUsing()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		List<int> ids = await DbQuery
			.Text("select Id from Posts order by Id")
			.Using(c)
			.Map<int>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([1, 2, 3, 4], ids);
	}

	[Fact]
	public static async Task ANullIsSentAsADatabaseNull()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		int? none = null;

		List<int> ids = await DbQuery
			.Text("select Id from Posts where @none is null and AuthorId = 8")
			.Using(c)
			.AddParam(none)
			.Map<int>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([3], ids);
		DbParameter p = c.Commands[0].Parameters.Cast<DbParameter>().Single();
		Assert.Equal(System.Data.DbType.Int32, p.DbType);
	}

	// ---- immutable ----

	[Fact]
	public static async Task AHeldCall_Branches_AndIsNeverChanged()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		int authorId = 7;

		DbQueryCall held = Posts.Using(c).AddParam(authorId);
		DbQueryCall low = held.AddParam("views", 0);
		DbQueryCall high = held.AddParam("views", 90);

		int[] lowIds = Ids(await low.Map<Post>().ToListAsync(TestContext.Current.CancellationToken));
		int[] highIds = Ids(await high.Map<Post>().ToListAsync(TestContext.Current.CancellationToken));
		Assert.Equal([1, 2, 4], lowIds);
		Assert.Equal([4], highIds);

		// The held call still has one value: the provider reports the other as missing.
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await held.Map<Post>().ToListAsync(TestContext.Current.CancellationToken));
		// The first run was `low`: both values, in the order added.
		Assert.Equal(
			["authorId", "views"],
			c.Commands[0].Parameters.Cast<DbParameter>().Select(static p => p.ParameterName));
		Assert.Single(c.Commands[2].Parameters);
	}

	[Fact]
	public static void TheDefinition_NeverChanges_SetTimeoutReturnsANewOne()
	{
		DbQuery a = DbQuery.Text("select 1");
		DbQuery b = a.SetTimeout(5);

		Assert.NotSame(a, b);
		Assert.NotEqual(a, b);
		Assert.Equal(a, DbQuery.Text("select 1"));
		Assert.Equal(b, DbQuery.Text("select 1").SetTimeout(5));
		Assert.NotEqual(DbQuery.Text("select 1"), DbQuery.Procedure("select 1"));
	}

	[Fact]
	public static void ADefinitionNeedsText()
	{
		Assert.Throws<ArgumentNullException>(() => DbQuery.Text(null!));
		Assert.Throws<ArgumentException>(() => DbQuery.Procedure(" "));
	}

	[Fact]
	public static void ANameNeedsText()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		DbQueryUsing using_ = Posts.Using(c);

		Assert.Throws<ArgumentNullException>(() => using_.AddParam(null!, 1));
		Assert.Throws<ArgumentException>(() => using_.AddParam(" ", 1));
		Assert.Throws<ArgumentNullException>(() => Posts.Using((DbConnection)null!));
	}

	[Fact]
	public static async Task Values_Accumulate_InTheOrderAdded()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		await DbQuery
			.Text("select @a, @b, @c")
			.Using(c)
			.AddParam("a", 1)
			.AddParam("b", "two")
			.AddParams(new { c = 3 })
			.Map<int>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(
			["a", "b", "c"],
			c.Commands[0].Parameters.Cast<DbParameter>().Select(static p => p.ParameterName));
	}

	// ---- the command ----

	[Fact]
	public static async Task TheTimeoutAndTheTextReachTheCommand()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		await DbQuery.Text("select 1").SetTimeout(7).Using(c).Map<int>()
			.ToListAsync(TestContext.Current.CancellationToken);

		DbCommand command = c.Commands.Single();
		Assert.Equal(7, command.CommandTimeout);
		Assert.Equal("select 1", command.CommandText);
		Assert.Equal(System.Data.CommandType.Text, command.CommandType);
	}

	[Fact]
	public static async Task AQueryWithoutATimeout_KeepsTheProvidersDefault()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		using SqliteCommand fresh = c.CreateCommand();

		await DbQuery.Text("select 1").Using(c).Map<int>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(fresh.CommandTimeout, c.Commands[^1].CommandTimeout);
	}

	[Fact]
	public static async Task ATransactionRunsTheCommandInIt()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		await c.OpenAsync(TestContext.Current.CancellationToken);
		using DbTransaction tx = await c.BeginTransactionAsync(TestContext.Current.CancellationToken);

		await DbQuery.Text("select 1").Using(tx).Map<int>()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Same(tx, c.Commands.Single().Transaction);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static async Task ACancelledToken_ThrowsBeforeTheConnectionIsTouched()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await DbQuery.Text("select 1").Using(c).Map<int>().ToListAsync(cts.Token));

		Assert.Equal(0, c.Opens);
		Assert.Empty(c.Commands);
	}

	[Fact]
	public static async Task AGivenToken_GovernsTheCall_AndTheConnectionIsLeftAsFound()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		using var cts = new CancellationTokenSource();

		List<int> ids = await DbQuery.Text("select Id from Posts order by Id")
			.Using(c)
			.Map<int>()
			.ToListAsync(cts.Token);

		Assert.Equal(4, ids.Count);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	// ---- read-through ----

	const string LaterFailure = "select 1; select abs(-9223372036854775808)";

	[Fact]
	public static async Task EveryTerminal_ReadsThroughTheLaterSets_SoALaterFailureIsThrown()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		MappedQuery<int> query = DbQuery.Text(LaterFailure).Using(c).Map<int>();

		await Assert.ThrowsAsync<SqliteException>(
			async () => await query.ToListAsync(TestContext.Current.CancellationToken));
		Assert.Throws<SqliteException>(() => query.ToList());
	}

	[Fact]
	public static async Task SingleResult_StopsAfterTheFirstSet()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		List<int> ids = await DbQuery.Text(LaterFailure).Using(c)
			.Map<int>()
			.SingleResult()
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([1], ids);
	}

	[Fact]
	public static async Task AfterAFailure_TheConnectionIsClosedAgain()
	{
		using var db = new Db();
		using Tracked c = db.Connect();

		await Assert.ThrowsAsync<SqliteException>(
			async () => await DbQuery.Text(LaterFailure).Using(c).Map<int>()
				.ToListAsync(TestContext.Current.CancellationToken));

		Assert.Equal(ConnectionState.Closed, c.State);
		Assert.Equal(1, c.Opens);
		Assert.False(c.DisposeCalled);
	}

	// ---- who owns the connection ----

	[Fact]
	public static async Task AClosedConnection_IsOpenedLate_AndClosedAgain_NeverDisposed()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		DbQueryCall call = Posts.Using(c).AddParam("authorId", 7).AddParam("views", 0);

		// Building the call touches nothing.
		Assert.Equal(0, c.Opens);
		Assert.Equal(ConnectionState.Closed, c.State);

		await call.Map<Post>().ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(1, c.Opens);
		Assert.True(c.Closes >= 1);
		Assert.Equal(ConnectionState.Closed, c.State);
		Assert.False(c.DisposeCalled);

		// Usable again.
		Assert.Equal(3, (await call.Map<Post>().ToListAsync(TestContext.Current.CancellationToken)).Count);
		Assert.Equal(2, c.Opens);
	}

	[Fact]
	public static async Task AnOpenConnection_StaysOpen_AndIsNotOpenedAgain()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		await c.OpenAsync(TestContext.Current.CancellationToken);

		await Posts.Using(c).AddParam("authorId", 7).AddParam("views", 0)
			.Map<Post>().ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(1, c.Opens);
		Assert.Equal(ConnectionState.Open, c.State);
	}

	[Fact]
	public static async Task AFactory_MakesAConnectionPerRun_AndDisposesIt()
	{
		using var db = new Db();
		var factory = new Factory(db);
		DbQueryCall call = Posts.Using(factory).AddParam("authorId", 7).AddParam("views", 0);

		Assert.Empty(factory.Made);

		await call.Map<Post>().ToListAsync(TestContext.Current.CancellationToken);
		await call.Map<Post>().ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal(2, factory.Made.Count);
		Assert.All(factory.Made, static c =>
		{
			Assert.Equal(1, c.Opens);
			Assert.True(c.DisposeCalled);
			Assert.Equal(ConnectionState.Closed, c.State);
		});
	}

	[Fact]
	public static async Task APool_GivesTheConnectionBack_AfterEveryRun()
	{
		using var db = new Db();
		var pool = new Pool(db);

		await Posts.Using(pool).AddParam("authorId", 7).AddParam("views", 0)
			.Map<Post>().ToListAsync(TestContext.Current.CancellationToken);

		Tracked taken = Assert.Single(pool.Taken);
		Assert.Same(taken, Assert.Single(pool.Given));
		Assert.False(taken.DisposeCalled);
		Assert.Equal(1, taken.Opens);
	}

	[Fact]
	public static async Task APool_GetsTheConnectionBack_WhenTheRunFails()
	{
		using var db = new Db();
		var pool = new Pool(db);

		await Assert.ThrowsAsync<SqliteException>(
			async () => await DbQuery.Text(LaterFailure).Using(pool).Map<int>()
				.ToListAsync(TestContext.Current.CancellationToken));

		Assert.Single(pool.Given);
	}

	[Fact]
	public static void ASourceThatMakesNoConnection_IsReported()
	{
		var pool = new NullPool();

		Assert.Throws<InvalidOperationException>(
			() => DbQuery.Text("select 1").Using(pool).Map<int>().ToList());
	}

	sealed class NullPool : IDbConnectionPool<SqliteConnection>
	{
		public SqliteConnection Take() => null!;

		System.Data.IDbConnection IDbConnectionPool.Take() => null!;

		public void Give(System.Data.IDbConnection connection) { }
	}

	[Fact]
	public static void ADefaultStruct_SaysItWasNotCreatedByUsing()
	{
		DbQueryUsing unbound = default;

		Assert.Throws<InvalidOperationException>(() => unbound.AddParam("a", 1));
		Assert.Throws<InvalidOperationException>(() => _ = unbound.Source);
	}

	// ---- the synchronous members of IExecuteReader (not shown in any example) ----

	[Fact]
	public static void TheReaderMembers_RunOnTheSameOpenLatePath()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		IExecuteReader reader = Posts.Using(c).AddParam("authorId", 7).AddParam("views", 0);
		var ids = new List<int>();

		reader.ExecuteReader(r =>
		{
			while (r.Read()) ids.Add(r.GetInt32(0));
		});
		int count = reader.ExecuteReader(static r =>
		{
			int n = 0;
			while (r.Read()) n++;
			return n;
		});

		Assert.Equal([1, 2, 4], ids);
		Assert.Equal(3, count);
		Assert.Equal(2, c.Opens);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static async Task TheAsyncReaderMembers_RunOnTheSamePath()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		IExecuteReader reader = Posts.Using(c).AddParam("authorId", 7).AddParam("views", 0);

		int count = await reader.ExecuteReaderAsync(async r =>
		{
			int n = 0;
			while (await ((DbDataReader)r).ReadAsync(TestContext.Current.CancellationToken)) n++;
			return n;
		});
		bool ran = false;
		await reader.ExecuteReaderAsync(r =>
		{
			ran = true;
			return ValueTask.CompletedTask;
		});

		Assert.Equal(3, count);
		Assert.True(ran);
		Assert.Equal(ConnectionState.Closed, c.State);
	}

	[Fact]
	public static void TheReaderMembers_RefuseANullFunction()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		IExecuteReader reader = Posts.Using(c);

		Assert.Throws<ArgumentNullException>(() => reader.ExecuteReader((Action<IDataReader>)null!));
		Assert.Throws<ArgumentNullException>(() => reader.ExecuteReader((Func<IDataReader, int>)null!));
		Assert.Equal(0, c.Opens);
	}

	// ---- the memoize seam ----

	[Fact]
	public static void TheSeam_Source_IsTheConnection_TheFactory_OrThePool()
	{
		using var db = new Db();
		using Tracked c = db.Connect();
		var factory = new Factory(db);

		Assert.Same(c, Posts.Using(c).Source);
		Assert.Same(factory, Posts.Using(factory).AddParam("authorId", 7).Source);
		Assert.Same(factory, Posts.Using(factory).AddParam("authorId", 7).AddParam("views", 1).Source);
	}

	[Fact]
	public static void TheSeam_EqualCalls_AreEqual_AndHashTheSame()
	{
		using var db = new Db();
		var factory = new Factory(db);
		int authorId = 7;

		DbQueryCall a = Posts.Using(factory).AddParam(authorId).AddParam("views", 3);
		DbQueryCall b = Posts.Using(factory).AddParam(authorId).AddParam("views", 3);
		DbQueryCall differentValue = Posts.Using(factory).AddParam(authorId).AddParam("views", 4);
		DbQueryCall differentName = Posts.Using(factory).AddParam(authorId).AddParam("other", 3);
		DbQueryCall differentSource = Posts.Using(new Factory(db)).AddParam(authorId).AddParam("views", 3);
		DbQueryCall differentQuery = DbQuery.Text("select 2").Using(factory).AddParam(authorId).AddParam("views", 3);

		Assert.Equal(a, b);
		Assert.Equal(a.GetHashCode(), b.GetHashCode());
		Assert.NotEqual(a, differentValue);
		Assert.NotEqual(a, differentName);
		Assert.NotEqual(a, differentSource);
		Assert.NotEqual(a, differentQuery);
		Assert.Equal(Posts.Using(factory), Posts.Using(factory));
		Assert.NotEqual(Posts.Using(factory), Posts.Using(new Factory(db)));
	}

	[Fact]
	public static void TheSeam_TypedCalls_CompareTheirValuesWithoutABox()
	{
		using var db = new Db();
		var factory = new Factory(db);

		DbQueryCall a = PostsTyped.Using(factory).AddParams(new(7, 1));
		DbQueryCall b = PostsTyped.Using(factory).AddParams(new(7, 1));
		DbQueryCall c = PostsTyped.Using(factory).AddParams(new(7, 2));

		Assert.Equal(a, b);
		Assert.Equal(a.GetHashCode(), b.GetHashCode());
		Assert.NotEqual(a, c);
	}

	[Fact]
	public static void TheSeam_TheViewsExposeTheirCommand_AndKeyOnTheWholeQuery()
	{
		using var db = new Db();
		var factory = new Factory(db);
		DbQueryCall call = Posts.Using(factory).AddParam("authorId", 7).AddParam("views", 0);
		RowMap<Post> map = RowMap<Post>.Default;

		MappedQuery<Post> query = call.Map(map);
		MappedQueryWithSkip<Post> skipped = query.Skip(1);
		MappedQueryWithTake<Post> taken = query.Take(2);

		Assert.Same(call, query.Command);
		Assert.Same(call, skipped.Command);
		Assert.Same(call, taken.Command);

		Assert.Equal(query, call.Map(map));
		Assert.Equal(query.GetHashCode(), call.Map(map).GetHashCode());
		Assert.NotEqual(query, query.SingleResult());
		Assert.Equal(skipped, call.Map(map).Skip(1));
		Assert.NotEqual(skipped, call.Map(map).Skip(2));
		Assert.Equal(taken, call.Map(map).Take(2));
		Assert.NotEqual(taken, call.Map(map).Take(3));
	}

	[Fact]
	public static void TheSeam_ASelectorKeysOnItsDelegate()
	{
		using var db = new Db();
		DbQuery query = DbQuery.Text("select 1");
		using Tracked c = db.Connect();

		Assert.Equal(query.Using(c).Map(Selectors.First), query.Using(c).Map(Selectors.First));
		Assert.NotEqual(
			query.Using(c).Map(Selectors.First),
			query.Using(c).Map(Selectors.Second));
	}

	static class Selectors
	{
		public static readonly Func<IDataRecord, int> First = static r => r.GetInt32(0);

		public static readonly Func<IDataRecord, int> Second = static r => r.GetInt32(0) + 1;
	}

	[Fact]
	public static void AMappedQuery_DefaultValue_StillEqualsItself()
	{
		MappedQuery<int> a = default;
		MappedQuery<int> b = default;

		Assert.True(a.Equals(b));
		Assert.Equal(a.GetHashCode(), b.GetHashCode());
	}
}
