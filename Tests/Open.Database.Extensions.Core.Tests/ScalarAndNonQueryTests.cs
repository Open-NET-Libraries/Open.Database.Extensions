using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// DbQuery.Scalar<T>() and DbNonQuery on SQLite (a real provider, in memory):
// what a scalar converts, what a write counts, the values in either order, the
// typed form, branching, equality and the default values of the hops.
[ExcludeFromCodeCoverage]
public static class ScalarAndNonQueryTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	readonly record struct AuthorViews(int AuthorId, int Views);

	readonly record struct TitleById(string Title, int Id);

	const string CountSql
		= "select count(*) from Posts where AuthorId = @authorId and Views >= @views";

	const string CountTypedSql
		= "select count(*) from Posts where AuthorId = @AuthorId and Views >= @Views";

	const string SetTitleSql
		= "update Posts set Title = @title where Id = @id";

	static readonly ScalarDbQuery<int> Count
		= DbQuery.Text(CountSql).Scalar<int>();

	static readonly DbNonQuery SetTitle
		= DbNonQuery.Text(SetTitleSql);

	// One in-memory database per test, alive for as long as this is.
	sealed class Db : IDisposable
	{
		static int _count;

		public Db()
		{
			ConnectionString
				= $"Data Source=scalar{Interlocked.Increment(ref _count)};Mode=Memory;Cache=Shared";
			Connection = new SqliteConnection(ConnectionString);
			Connection.Open();
			using SqliteCommand command = Connection.CreateCommand();
			command.CommandText = """
				create table Posts (Id integer primary key, AuthorId integer, Title text, Views integer);
				insert into Posts values (1, 7, 'a', 10), (2, 7, 'b', 50), (3, 8, 'c', 60), (4, 7, 'd', 90);
				""";
			command.ExecuteNonQuery();
		}

		public string ConnectionString { get; }

		public SqliteConnection Connection { get; }

		public string? Title(int id)
			=> Connection
				.Command("select Title from Posts where Id = @id")
				.AddParam("id", id)
				.ExecuteScalar<string?>();

		public void Dispose() => Connection.Dispose();
	}

	// ---- the values, in every form ----

	[Fact]
	public static async Task EveryWayToGiveValues_ReadsTheSameScalar()
	{
		using var db = new Db();
		SqliteConnection c = db.Connection;
		const int authorId = 7;
		const int views = 50;
		DbQuery text = DbQuery.Text(CountSql);
		DbQuery typedText = DbQuery.Text(CountTypedSql);

		int[] counts =
		[
			await Count.Using(c).AddParam(authorId).AddParam(views).ExecuteScalarAsync(Token),
			await Count.AddParam(authorId).AddParam(views).Using(c).ExecuteScalarAsync(Token),
			await Count.AddParam(authorId).Using(c).AddParam(views).ExecuteScalarAsync(Token),
			await Count.Using(c).AddParam("authorId", authorId).AddParam("views", views).ExecuteScalarAsync(Token),
			await Count.AddParam("authorId", authorId).AddParam("@views", views).Using(c).ExecuteScalarAsync(Token),
			await Count.Using(c).AddParams(new { authorId, views }).ExecuteScalarAsync(Token),
			await Count.AddParams(new { authorId, views }).Using(c).ExecuteScalarAsync(Token),
			await typedText.Params<AuthorViews>().Scalar<int>().Using(c).AddParams(new(authorId, views)).ExecuteScalarAsync(Token),
			await typedText.Params<AuthorViews>().Scalar<int>().AddParams(new(authorId, views)).Using(c).ExecuteScalarAsync(Token),
			await typedText.Scalar<int>().Params<AuthorViews>().Using(c).AddParams(new(authorId, views)).ExecuteScalarAsync(Token),
			await typedText.Scalar<int>().Params<AuthorViews>().AddParams(new(authorId, views)).Using(c).ExecuteScalarAsync(Token),
			// The same text read as rows.
			(await text.Map<int>().Using(c).AddParam(authorId).AddParam(views).ToListAsync(Token))[0],
		];

		Assert.All(counts, static n => Assert.Equal(2, n));
	}

	[Fact]
	public static async Task EveryWayToGiveValues_WritesTheSameRow()
	{
		using var db = new Db();
		SqliteConnection c = db.Connection;
		const int id = 2;
		string title = "derived";

		Assert.Equal(1, await SetTitle.Using(c).AddParam(title).AddParam(id).ExecuteNonQueryAsync(Token));
		Assert.Equal("derived", db.Title(id));

		title = "before";
		Assert.Equal(1, await SetTitle.AddParam(title).AddParam(id).Using(c).ExecuteNonQueryAsync(Token));
		Assert.Equal("before", db.Title(id));

		Assert.Equal(1, await SetTitle.Using(c).AddParam("title", "named").AddParam("@id", id).ExecuteNonQueryAsync(Token));
		Assert.Equal("named", db.Title(id));

		Assert.Equal(1, await SetTitle.AddParam("@title", "named first").AddParam("id", id).Using(c).ExecuteNonQueryAsync(Token));
		Assert.Equal("named first", db.Title(id));

		Assert.Equal(1, await SetTitle.AddParams(new { id, title = "object" }).Using(c).ExecuteNonQueryAsync(Token));
		Assert.Equal("object", db.Title(id));

		DbNonQuery<TitleById> typed
			= DbNonQuery.Text("update Posts set Title = @Title where Id = @Id").Params<TitleById>();
		Assert.Equal(1, await typed.Using(c).AddParams(new("typed", id)).ExecuteNonQueryAsync(Token));
		Assert.Equal("typed", db.Title(id));

		Assert.Equal(1, await typed.AddParams(new("typed first", id)).Using(c).ExecuteNonQueryAsync(Token));
		Assert.Equal("typed first", db.Title(id));

		// A NULL string value is sent as a database null.
		Assert.Equal(1, await SetTitle.Using(c).AddParam("title", null).AddParam(id).ExecuteNonQueryAsync(Token));
		Assert.Null(db.Title(id));
	}

	[Fact]
	public static async Task AHeldCall_CanBeBranched_AndIsNotChanged()
	{
		using var db = new Db();
		BoundCall<ScalarKind<int>> author = Count.Using(db.Connection).AddParam("authorId", 7);
		BoundCall<ScalarKind<int>> all = author.AddParam("views", 0);
		BoundCall<ScalarKind<int>> popular = author.AddParam("views", 80);

		Assert.Equal(3, await all.ExecuteScalarAsync(Token));
		Assert.Equal(1, await popular.ExecuteScalarAsync(Token));
		Assert.Equal(3, await all.ExecuteScalarAsync(Token));

		PendingCall<ScalarKind<int>> pending = Count.AddParam("authorId", 7);
		PendingCall<ScalarKind<int>> pendingAll = pending.AddParam("views", 0);
		PendingCall<ScalarKind<int>> pendingPopular = pending.AddParam("views", 80);

		Assert.Equal(3, await pendingAll.Using(db.Connection).ExecuteScalarAsync(Token));
		Assert.Equal(1, await pendingPopular.Using(db.Connection).ExecuteScalarAsync(Token));
		Assert.Equal(3, await pendingAll.Using(db.Connection).ExecuteScalarAsync(Token));
	}

	[Fact]
	public static async Task AFactory_RunsAWriteAndAScalar()
	{
		using var db = new Db();
		var factory = new DbConnectionFactory<SqliteConnection>(
			() => new SqliteConnection(db.ConnectionString));
		const int id = 1;
		const string title = "factory";
		const int authorId = 7;
		const int views = 0;

		Assert.Equal(1, await SetTitle.Using(factory).AddParam(id).AddParam(title).ExecuteNonQueryAsync(Token));
		Assert.Equal(title, db.Title(id));
		Assert.Equal(3, await Count.Using(factory).AddParam(authorId).AddParam(views).ExecuteScalarAsync(Token));
	}

	// ---- what a scalar converts: the rule of the command ----

	static async Task<string> Outcome<T>(Func<ValueTask<T>> run)
	{
		try
		{
			T value = await run();
			return value is null ? "null" : $"{value.GetType().Name} {value}";
		}
		catch (ValueMappingException)
		{
			return nameof(ValueMappingException);
		}
	}

	static async Task Same<T>(string sql, string expected)
	{
		using var db = new Db();
		string scalar = await Outcome(
			() => DbQuery.Text(sql).Scalar<T>().Using(db.Connection).ExecuteScalarAsync(Token));
		string command = await Outcome(
			() => db.Connection.Command(sql).ExecuteScalarAsync<T>(Token));

		Assert.Equal(expected, scalar);
		Assert.Equal(command, scalar);
	}

	[Fact]
	public static async Task AValue_IsConverted_AsTheCommandConvertsIt()
	{
		await Same<int>("select count(*) from Posts", "Int32 4");
		await Same<long>("select 5000000000", "Int64 5000000000");
		await Same<bool>("select exists(select 1 from Posts where Id = 1)", "Boolean True");
		await Same<string>("select Title from Posts where Id = 1", "String a");
		await Same<decimal>("select 12", "Decimal 12");
		await Same<int?>("select max(Id) from Posts", "Int32 4");
		await Same<DateTime?>("select null", "null");
		await Same<int>("select Id from Posts order by Id", "Int32 1");
	}

	[Fact]
	public static async Task Null_IsNull_ForANullableOrReferenceType_AndAnErrorOtherwise()
	{
		await Same<int?>("select null", "null");
		await Same<string>("select null", "null");
		await Same<byte[]>("select null", "null");
		await Same<int>("select null", nameof(ValueMappingException));
	}

	[Fact]
	public static async Task NoRow_IsNull_ForANullableOrReferenceType_AndAnErrorOtherwise()
	{
		await Same<int?>("select 1 where 0", "null");
		await Same<string>("select 1 where 0", "null");
		await Same<int>("select 1 where 0", nameof(ValueMappingException));
		await Same<DateTime>("select 1 where 0", nameof(ValueMappingException));
	}

	[Fact]
	public static async Task AValueOutOfRange_OrThatDoesNotConvert_IsAnError()
	{
		await Same<int>("select 5000000000", nameof(ValueMappingException));
		await Same<Guid>("select 'not a guid'", nameof(ValueMappingException));
	}

	[Fact]
	public static async Task TheNoRowError_NamesTheTerminal()
	{
		using var db = new Db();

		ValueMappingException ex = await Assert.ThrowsAsync<ValueMappingException>(
			async () => await DbQuery.Text("select 1 where 0").Scalar<int>().Using(db.Connection).ExecuteScalarAsync(Token));

		Assert.Contains("ExecuteScalarAsync<", ex.Message);
		Assert.Equal(typeof(int), ex.TargetType);
	}

	// ---- what a write counts ----

	[Fact]
	public static async Task AWrite_ReturnsTheRowsTheProviderReports()
	{
		using var db = new Db();
		SqliteConnection c = db.Connection;
		DbNonQuery byAuthor = DbNonQuery.Text("update Posts set Views = Views + 1 where AuthorId = @authorId");

		Assert.Equal(0, await byAuthor.Using(c).AddParam("authorId", 99).ExecuteNonQueryAsync(Token));
		Assert.Equal(1, await byAuthor.Using(c).AddParam("authorId", 8).ExecuteNonQueryAsync(Token));
		Assert.Equal(3, await byAuthor.Using(c).AddParam("authorId", 7).ExecuteNonQueryAsync(Token));
		Assert.Equal(-1, await DbNonQuery.Text("select 1").Using(c).ExecuteNonQueryAsync(Token));
	}

	[Fact]
	public static async Task AnInsert_ReadsItsIdAsAScalar_OrItsCountAsAWrite()
	{
		using var db = new Db();
		const string insert = "insert into Posts (AuthorId, Title, Views) values (7, 'new', 0)";

		long id = await DbQuery.Text(insert + "; select last_insert_rowid()")
			.Scalar<long>()
			.Using(db.Connection)
			.ExecuteScalarAsync(Token);
		int rows = await DbNonQuery.Text(insert).Using(db.Connection).ExecuteNonQueryAsync(Token);

		Assert.Equal(5, id);
		Assert.Equal(1, rows);
	}

	[Fact]
	public static async Task AWrite_InATransaction_IsUndoneWithIt()
	{
		using var db = new Db();
		await using (DbTransaction transaction = await db.Connection.BeginTransactionAsync(Token))
		{
			Assert.Equal(1, await SetTitle.Using(transaction).AddParam("title", "tx").AddParam("id", 1).ExecuteNonQueryAsync(Token));
			Assert.Equal("tx", await DbQuery.Text("select Title from Posts where Id = 1").Scalar<string?>().Using(transaction).ExecuteScalarAsync(Token));
			await transaction.RollbackAsync(Token);
		}

		Assert.Equal("a", db.Title(1));
	}

	// ---- typed values on a mapped definition ----

	[Fact]
	public static async Task Params_AfterMap_ReadsTheSameRows_AndKeepsTheOptions()
	{
		using var db = new Db();
		SqliteConnection c = db.Connection;
		// The second statement fails: only a read of the first result set alone does not see it.
		const string sql = "select Id from Posts where AuthorId = @AuthorId and Views >= @Views order by Id; select abs(-9223372036854775808)";
		MappedDbQuery<int> mapped = DbQuery.Text(sql).Map<int>();
		AuthorViews values = new(7, 50);

		MappedDbQuery<AuthorViews, int> typed = mapped.Params<AuthorViews>();
		await Assert.ThrowsAsync<SqliteException>(
			async () => await typed.Using(c).AddParams(values).ToListAsync(Token));

		MappedDbQuery<AuthorViews, int> single = mapped.SingleResult().Params<AuthorViews>();
		Assert.Equal([2, 4], await single.Using(c).AddParams(values).ToListAsync(Token));
		Assert.Equal([2, 4], await single.AddParams(values).Using(c).ToListAsync(Token));
		Assert.Equal([2, 4], await mapped.Params<AuthorViews>().SingleResult().AsyncReads().AddParams(values).Using(c).ToListAsync(Token));
		Assert.Equal(
			[2, 4],
			await DbQuery.Text(sql).Params<AuthorViews>().Map<int>().SingleResult().Using(c).AddParams(values).ToListAsync(Token));
	}

	// ---- equality ----

	[Fact]
	public static void ADbNonQuery_IsEqualByTextKindAndTimeout_AndNeverToADbQuery()
	{
		DbNonQuery text = DbNonQuery.Text("delete from Posts");

		Assert.Equal(text, DbNonQuery.Text("delete from Posts"));
		Assert.Equal(text.GetHashCode(), DbNonQuery.Text("delete from Posts").GetHashCode());
		Assert.NotEqual(text, DbNonQuery.Text("delete from Authors"));
		Assert.NotEqual(text, DbNonQuery.Procedure("delete from Posts"));
		Assert.NotEqual(text, text.SetTimeout(5));
		Assert.Equal(text.SetTimeout(5), text.SetTimeout(5));
		Assert.False(text.Equals(DbQuery.Text("delete from Posts")));
		Assert.False(text.Equals(null));
	}

	[Fact]
	public static void ABoundCall_IsEqualByDefinitionSourceValuesAndKind()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		using var other = new SqliteConnection("Data Source=:memory:");
		const string sql = "select @id";
		DbQuery query = DbQuery.Text(sql);

		BoundCall<ScalarKind<int>> call = query.Scalar<int>().Using(connection).AddParam("id", 1);
		BoundCall<ScalarKind<int>> same = DbQuery.Text(sql).Scalar<int>().AddParam("id", 1).Using(connection);

		Assert.Equal(call, same);
		Assert.True(call == same);
		Assert.False(call != same);
		Assert.Equal(call.GetHashCode(), same.GetHashCode());
		Assert.Same(connection, call.Source);
		Assert.NotEqual(call, query.Scalar<int>().Using(connection).AddParam("id", 2));
		Assert.NotEqual(call, query.Scalar<int>().Using(other).AddParam("id", 1));

		// The kind is part of the type, and of the hash.
		BoundCall<ScalarKind<long>> asLong = query.Scalar<long>().Using(connection).AddParam("id", 1);
		BoundCall<NonQueryKind> write = DbNonQuery.Text(sql).Using(connection).AddParam("id", 1);
		Assert.False(call.Equals(asLong));
		Assert.False(call.Equals(write));
		Assert.NotEqual(call.GetHashCode(), asLong.GetHashCode());
		Assert.NotEqual(call.GetHashCode(), write.GetHashCode());
	}

	// ---- arguments and default values ----

	[Fact]
	public static void ADefaultHop_ThrowsWhenItIsUsed()
	{
		using var c = new SqliteConnection("Data Source=:memory:");
		const int id = 1;
		BoundCall<ScalarKind<int>> bound = default;
		BoundCall<NonQueryKind> write = default;
		PendingCall<NonQueryKind> pending = default;
		BoundParams<NonQueryKind, AuthorViews> boundParams = default;
		PendingParams<NonQueryKind, AuthorViews> pendingParams = default;

		Action[] uses =
		[
			() => bound.ExecuteScalarAsync(Token).AsTask(),
			() => write.ExecuteNonQueryAsync(Token).AsTask(),
			() => _ = bound.Source,
			() => bound.AddParam("id", id),
			() => bound.AddParam("id", "x"),
			() => bound.AddParam(id),
			() => bound.AddParams(new { id }),
			() => pending.AddParam("id", id),
			() => pending.AddParam("id", "x"),
			() => pending.AddParam(id),
			() => pending.AddParams(new { id }),
			() => pending.Using(c),
			() => boundParams.AddParams(default),
			() => pendingParams.Using(c),
		];

		Assert.All(uses, static use => Assert.Throws<InvalidOperationException>(use));

		// A default value can still be compared and hashed.
		Assert.True(bound.Equals(default));
		_ = bound.GetHashCode();
	}

	[Fact]
	public static void ANullArgument_NamesItsParameter()
	{
		static string? Name(Action use)
			=> Assert.Throws<ArgumentNullException>(use).ParamName;

		DbNonQuery write = SetTitle;
		ScalarDbQuery<int> scalar = Count;
		DbNonQuery<TitleById> typedWrite = write.Params<TitleById>();
		ScalarDbQuery<AuthorViews, int> typedScalar = scalar.Params<AuthorViews>();

		Assert.Equal("sql", Name(static () => DbNonQuery.Text(null!)));
		Assert.Equal("name", Name(static () => DbNonQuery.Procedure(null!)));
		Assert.Throws<ArgumentException>(static () => DbNonQuery.Text("  "));

		Assert.Equal("connection", Name(() => write.Using((DbConnection)null!)));
		Assert.Equal("transaction", Name(() => write.Using((DbTransaction)null!)));
		Assert.Equal("pool", Name(() => write.Using((IDbConnectionPool<SqliteConnection>)null!)));
		Assert.Equal("factory", Name(() => write.Using((IDbConnectionFactory<SqliteConnection>)null!)));
		Assert.Equal("connection", Name(() => scalar.Using((DbConnection)null!)));
		Assert.Equal("transaction", Name(() => scalar.Using((DbTransaction)null!)));
		Assert.Equal("pool", Name(() => scalar.Using((IDbConnectionPool<SqliteConnection>)null!)));
		Assert.Equal("factory", Name(() => scalar.Using((IDbConnectionFactory<SqliteConnection>)null!)));
		Assert.Equal("connection", Name(() => typedWrite.Using((DbConnection)null!)));
		Assert.Equal("transaction", Name(() => typedWrite.Using((DbTransaction)null!)));
		Assert.Equal("pool", Name(() => typedWrite.Using((IDbConnectionPool<SqliteConnection>)null!)));
		Assert.Equal("factory", Name(() => typedWrite.Using((IDbConnectionFactory<SqliteConnection>)null!)));
		Assert.Equal("connection", Name(() => typedScalar.Using((DbConnection)null!)));
		Assert.Equal("transaction", Name(() => typedScalar.Using((DbTransaction)null!)));
		Assert.Equal("pool", Name(() => typedScalar.Using((IDbConnectionPool<SqliteConnection>)null!)));
		Assert.Equal("factory", Name(() => typedScalar.Using((IDbConnectionFactory<SqliteConnection>)null!)));
		Assert.Equal("connection", Name(() => write.AddParam("id", 1).Using((DbConnection)null!)));

		Assert.Equal("name", Name(() => write.AddParam(null!, 1)));
		Assert.Equal("name", Name(() => scalar.AddParam(null!, "x")));
	}
}
