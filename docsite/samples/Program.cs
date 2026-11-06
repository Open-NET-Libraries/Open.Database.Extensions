using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;
using Samples;

// Runs every sample against a shared in-memory SQLite database and checks what the documentation says about it.
// A failed check throws, so the process exits with a non-zero code.
int checks = 0;

void Check(bool condition, string what)
{
	checks++;
	if (!condition) throw new InvalidOperationException("Sample check failed: " + what);
	Console.WriteLine("ok - " + what);
}

using SqliteConnection keeper = Db.OpenKeeper();
Db.Seed();

IDbConnectionFactory<SqliteConnection> factory = Db.Factory;
using SqliteConnection connection = factory.Create();
connection.Open();

// Expressive commands: factory form and connection form.
Post[] fromFactory = await ExpressiveCommands.FactoryForm(factory);
Check(fromFactory.Select(p => p.Id).SequenceEqual([1, 2, 3]), "factory form returns posts 1-3");
Check(fromFactory[0].Title == "Post 1" && fromFactory[0].Body == "Body 1" && fromFactory[1].Body is null, "factory form maps strings and NULL");

Post[] fromConnection = ExpressiveCommands.ConnectionForm(connection);
Check(fromConnection.Select(p => p.Id).SequenceEqual([1, 2, 3]), "connection form returns posts 1-3");
Check(connection.State == ConnectionState.Open, "connection form leaves an open connection open");

Check(ExpressiveCommands.FirstPost(factory)?.Title == "Post 3", "FirstOrDefault by id");
Check(ExpressiveCommands.ConditionalParam(factory, null).Length == 5, "AddParamIf skipped: all rows");
Check(ExpressiveCommands.ConditionalParam(factory, 7).Length == 2, "AddParamIf applied: author 7 has 2 posts");

(ConnectionState closedBefore, ConnectionState closedAfter, ConnectionState openBefore, ConnectionState openAfter) = ExpressiveCommands.Ownership(connection);
Check(closedBefore == ConnectionState.Closed && closedAfter == ConnectionState.Closed, "a closed connection is closed again after the call");
Check(openBefore == ConnectionState.Open && openAfter == ConnectionState.Open, "an open connection stays open");

(long count, int updated) = ExpressiveCommands.ScalarAndNonQuery(factory);
Check(count == 3 && updated == 3, "scalar and non-query");
Check(ExpressiveCommands.ConvertedScalar(factory) == 5, "ExecuteScalar with a conversion");
Check(Throws<InvalidCastException>(() => factory.Command("select count(*) from posts").ExecuteScalar<int>()), "ExecuteScalar<int> over a SQLite integer is a failed cast");

// Mapping.
Db.Seed();
Post[] all = Mapping.MapDefault(connection);
Check(all.Length == 5 && all[0].Title == "Post 1" && all[0].Author == 42 && all[4].Author == 7, "default mapping, long to int");
Person[] declared = Mapping.MapDeclared(connection);
Check(declared.Length == 2 && declared[0].Id == 1 && declared[0].FirstName == "Ada" && declared[1].LastName == "Turing", "a declared RowMap maps the renamed columns");
Person[] namesOnly = Mapping.MapNamesOnly(connection);
Check(namesOnly[0].FirstName == "Ada" && namesOnly[0].LastName == "", "a null column in a RowMap means do not map the property, even by name");
Person[] callSite = Mapping.MapCallSite(connection);
Check(callSite.Length == 2 && callSite[1].FirstName == "Alan" && callSite[1].LastName == "Turing", "a new RowMap at the call site maps the renamed columns");
Person[] tuples = Mapping.MapTuples(connection);
Check(tuples.Length == 2 && tuples[1].FirstName == "Alan" && tuples[1].LastName == "Turing", "the tuple overload maps the renamed columns");
Check(ReferenceEquals(PersonMaps.PersonMap, PersonMaps.PersonMap), "a map held with field ??= is built once and shared");
Check(ReferenceEquals(HoldingAMap.BackingMap, HoldingAMap.BackingMap), "a map held in a backing field is built once and shared");
Check(Mapping.MapUnknownProperty().Contains("'Headline'", StringComparison.Ordinal), "a RowMap with an unknown property throws when it is created");
Check(Mapping.MapPaging(connection).Select(p => p.Id).SequenceEqual([2, 3]), "Skip(1).Take(2)");
(Post? first, Post? single) = Mapping.FirstAndSingle(connection);
Check(first?.Id == 1 && single?.Id == 2, "first and single");
Check(Throws<InvalidOperationException>(() => connection.Command("select * from posts").Map<Post>().SingleOrDefault()), "SingleOrDefault throws for more than one row");
Check(Mapping.MapSelector(connection)[0] == "Post 1 (10 views)", "Map(selector)");
Check(Mapping.StreamRows(connection) == 15, "Read streams rows inside the handler");
Check((await Mapping.ByIdAsync(connection, CancellationToken.None)).Count == 5, "ToDictionaryAsync");

// Channels and Dataflow.
List<Post> viaChannel = await ChannelsAndDataflow.ToChannelSample(factory);
Check(viaChannel.Select(p => p.Id).SequenceEqual([1, 2, 3, 4, 5]), "ToChannel delivers every row in order");
(long delivered, List<Post> viaBlock) = await ChannelsAndDataflow.ToTargetBlockSample(factory);
Check(delivered == 5 && viaBlock.Count == 5, "ToTargetBlockAsync delivers every row");

// Transactions.
Check(Transactions.Conditional(factory, true) is (true, 1), "returning true commits");
Check(Transactions.Conditional(factory, false) is (false, 0), "returning false rolls back");

// Dapper side by side.
Check(DapperComparison.DapperList(connection).Select(p => p.Id).SequenceEqual(DapperComparison.OdeList(connection).Select(p => p.Id)), "same list");
Check(DapperComparison.DapperFirst(connection)?.Title == DapperComparison.OdeFirst(connection)?.Title, "same first");
Check(DapperComparison.DapperScalar(connection) == DapperComparison.OdeScalar(connection), "same scalar");
Check(DapperComparison.DapperExecute(connection) == DapperComparison.OdeExecute(connection), "same execute");
Check(DapperComparison.DapperRecords(connection).Count == 5, "Dapper binds a positional record");

// Before and after: plain ADO.NET and the extensions return the same data.
Db.Seed();
List<Post> plainList = BeforeAfter.PlainList();
Post[] odeList = await BeforeAfter.OdeList(factory);
Check(plainList.Count == 3 && plainList.Select(p => (p.Id, p.Author, p.Title, p.Body)).SequenceEqual(odeList.Select(p => (p.Id, p.Author, p.Title, p.Body))), "plain ADO.NET list equals the extension list");
Post? plainFirst = BeforeAfter.PlainFirst();
Post? odeFirst = BeforeAfter.OdeFirst(factory);
Check(plainFirst is not null && odeFirst is not null && (plainFirst.Id, plainFirst.Title, plainFirst.Body) == (odeFirst.Id, odeFirst.Title, odeFirst.Body), "plain ADO.NET first row equals the extension first row");
Check(BeforeAfter.PlainScalar() == 3 && BeforeAfter.OdeScalar(factory) == 3, "plain and extension scalar agree");
Check(BeforeAfter.PlainNonQuery() == 3 && BeforeAfter.OdeNonQuery(factory) == 3, "plain and extension non-query agree");
// The comparisons in the docs: both sides return the same data.
Db.Seed();
string connectionString = Db.ConnectionString;
Check(ConnectionPattern.DapperList(connectionString).Select(p => p.Id).SequenceEqual(ConnectionPattern.OdeList(factory).Select(p => p.Id)), "Dapper and factory list agree");
Check(ConnectionPattern.OdeListOwn(connectionString).Select(p => p.Id).SequenceEqual([1, 2, 3]), "own-connection list");
Check(ConnectionPattern.PlainListAligned(connectionString).Select(p => p.Id).SequenceEqual(ConnectionPattern.OdeListAligned(factory).Select(p => p.Id)), "plain ADO.NET and factory aligned list agree");
Check(ConnectionPattern.DapperFirst(connectionString)?.Title == ConnectionPattern.OdeFirst(factory)?.Title, "Dapper and factory first row agree");
Check(ConnectionPattern.DapperScalar(connectionString) == 3 && ConnectionPattern.OdeScalar(factory) == 3, "Dapper and factory scalar agree");
Check(ConnectionPattern.DapperNonQuery(connectionString) == 3 && ConnectionPattern.OdeNonQuery(factory) == 3, "Dapper and factory non-query agree");
Check(DependencyInjectionSample.ResolveAndUse().Count == 3, "a factory registered once is injected");
Check(ConnectionPattern.RenamedColumns(connection) is [{ FirstName: "Ada" }, { FirstName: "Alan" }], "renamed columns with nameof");
Check(Throws<InvalidOperationException>(() => connection.Command("select * from posts").Map<Post>().SingleOrDefault()), "SingleOrDefault throws for more than one row (again)");
Check(factory.Command("select * from posts where id = 999").Map<Post>().FirstOrDefault() is null, "FirstOrDefault is null for no rows");
// Transactions: ADO.NET needs an open connection to begin one; Dapper begins nothing itself.
long RowsFrom100() => factory.Command("select count(*) from posts where id >= 100").ExecuteScalar<long>();
void ClearFrom100() => factory.Command("delete from posts where id >= 100").ExecuteNonQuery();
ConnectionPattern.DapperTransaction(connectionString);
Check(RowsFrom100() == 2, "the Dapper transaction committed two rows");
ClearFrom100();
ConnectionPattern.OdeTransaction(factory);
Check(RowsFrom100() == 2, "ExecuteTransaction committed two rows");
ClearFrom100();
Check(Throws<InvalidOperationException>(() => factory.Using(c => c.ExecuteTransaction(tx =>
{
	tx.Command(Sql.InsertSql).AddParam("id", 100).ExecuteNonQuery();
	throw new InvalidOperationException("boom");
}))), "ExecuteTransaction propagates the exception");
Check(RowsFrom100() == 0, "ExecuteTransaction rolled back when the action threw");
using (var closedConnection = new SqliteConnection(connectionString))
{
	closedConnection.ExecuteTransaction(tx => tx.Command("select 1").ExecuteScalar());
	Check(closedConnection.State == ConnectionState.Closed, "ExecuteTransaction closes a connection it opened");
}

connection.ExecuteTransaction(tx => tx.Command("select 1").ExecuteScalar());
Check(connection.State == ConnectionState.Open, "ExecuteTransaction leaves an open connection open");
Console.WriteLine($"All {checks} sample checks passed.");
return 0;

static bool Throws<TException>(Action action) where TException : Exception
{
	try
	{
		action();
		return false;
	}
	catch (TException)
	{
		return true;
	}
}
