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
Check(ExpressiveCommands.ParamNamedFromTheArgument(factory).Length == 2, "AddParam(search.Author) names the parameter from the argument");

(ConnectionState closedBefore, ConnectionState closedAfter, ConnectionState openBefore, ConnectionState openAfter) = ExpressiveCommands.Ownership(connection);
Check(closedBefore == ConnectionState.Closed && closedAfter == ConnectionState.Closed, "a closed connection is closed again after the call");
Check(openBefore == ConnectionState.Open && openAfter == ConnectionState.Open, "an open connection stays open");

(long count, int updated) = ExpressiveCommands.ScalarAndNonQuery(factory);
Check(count == 3 && updated == 3, "scalar and non-query");
Check(ExpressiveCommands.ConvertedScalar(factory) == 5, "ExecuteScalar with a conversion");
Check(Throws<InvalidCastException>(() => factory.Command("select count(*) from posts").ExecuteScalar<int>()), "ExecuteScalar<int> over a SQLite integer is a failed cast");
Check(factory.Command("select max(id) from posts where id > 1000").ExecuteScalar<long>() == 0, "ExecuteScalar<long> over NULL is default");
Check(factory.Command("select id from posts where id > 1000").ExecuteScalar<long>() == 0, "ExecuteScalar<long> over no rows is default");
Check(factory.Command("select max(id) from posts where id > 1000").ExecuteScalar<long?>() is null, "ExecuteScalar<long?> over NULL is null");
Check(factory.Command("select max(id) from posts where id <= 3").ExecuteScalar<long?>() == 3, "ExecuteScalar<long?> reads a value");
Check(await factory.Command("select id from posts where id > 1000").ExecuteScalarAsync<long>() == 0, "ExecuteScalarAsync<long> over no rows is default");
Check(factory.Command(Sql.CountSql).AddParam("author", 42).ExecuteScalar<long>() == 3 && factory.Command(Sql.CountSql).AddParam("@author", 42).ExecuteScalar<long>() == 3, "a parameter name is sent as written: SQLite accepts it with or without the @");

// Mapping.
Db.Seed();
Post[] all = Mapping.MapDefault(connection);
Check(all.Length == 5 && all[0].Title == "Post 1" && all[0].Author == 42 && all[4].Author == 7, "default mapping, long to int");
Person[] declared = Mapping.MapDeclared(connection);
Check(declared.Length == 2 && declared[0].Id == 1 && declared[0].FirstName == "Ada" && declared[1].LastName == "Turing", "a declared RowMap maps the renamed columns");
Person[] namesOnly = Mapping.MapNamesOnly(connection);
Check(namesOnly[0].FirstName == "Ada" && namesOnly[0].LastName is "", "a null column in a RowMap means do not map the property, even by name");
Person[] callSite = Mapping.MapCallSite(connection);
Check(callSite.Length == 2 && callSite[1].FirstName == "Alan" && callSite[1].LastName == "Turing", "a new RowMap at the call site maps the renamed columns");
Person[] pairs = Mapping.MapPairs(connection);
Check(pairs.Length == 2 && pairs[1].FirstName == "Alan" && pairs[1].LastName == "Turing", "Map<T> with pairs maps the renamed columns");
RowMap<Person>[] declarationForms = Mapping.DeclarationForms();
Check(declarationForms.All(form => connection.Command("select person_id, first_name, last_name from people order by person_id").Map<Person>(form).ToArray() is [{ Id: 1, FirstName: "Ada", LastName: "Lovelace" }, { Id: 2, FirstName: "Alan" }]), "a RowMap declared with the collection syntax (one pair or several), the constructor (one pair or several), an ImmutableArray or a copy maps that pair and matches the rest by name");
Check(declarationForms[0].Select(pair => pair.Key).SequenceEqual([nameof(Person.Id), nameof(Person.FirstName)]) && declarationForms[0].SequenceEqual(declarationForms[^1]), "a map enumerates the pairs it was declared with, and a copy built from them declares the same pairs");
Check(PersonMaps.PersonNamesOnlyMap.Count() == 3 && PersonMaps.PersonNamesOnlyMap.Single(pair => pair.Value is null).Key == nameof(Person.LastName), "an ignored property is enumerated with a null column");
Person[] snake = Mapping.MapSnakeCase(connection);
Check(snake[0] is { Id: 0, FirstName: "Ada", LastName: "Lovelace" }, "snake_case columns feed PascalCase properties with no map");
Person? exactFirst = connection.Command("select 'Alan' as first_name, 'Ada' as FirstName").Map<Person>().FirstOrDefault();
Person? exactSecond = connection.Command("select 'Ada' as FirstName, 'Alan' as first_name").Map<Person>().FirstOrDefault();
Check(exactFirst?.FirstName == "Ada" && exactSecond?.FirstName == "Ada", "an exact column beats an underscore match, in either column order");
RowMap<Person> nullPair = new([new(nameof(Person.FirstName), null)]);
Check(connection.Command("select 'Alan' as first_name").Map<Person>(nullPair).FirstOrDefault()?.FirstName is "", "a null pair stops the underscore match");
RowMap<Person> otherPair = new([new(nameof(Person.FirstName), "given_name")]);
Check(connection.Command("select 'Alan' as first_name").Map<Person>(otherPair).FirstOrDefault()?.FirstName is "", "a property named by a pair is not matched by an underscore column");
Check(Mapping.MapNulls(connection) is { Quantity: 1, Discount: null, Note: null, Title: "" }, "NULL keeps the initializer of a non-nullable property (a value type, or a reference with nullable annotations on) and gives null to nullable properties");
PostRecord[] records = Mapping.MapRecord(connection);
Check(records.Length == 5 && records[0] == new PostRecord(1, 42, "Post 1", "Body 1") && records[1] == new PostRecord(2, 42, "Post 2", null), "a positional record is built through its constructor, NULL included");
Check(Mapping.MapRecordExtra(connection)[0] == new PostSummary(1, "Post 1") { Views = 10 }, "a record with an init property: the constructor, then the init property from its column, unmatched columns ignored, an init property with no column keeps its initializer");
Check(Mapping.MapRecordDeclared(connection) is [{ Title: "Post 1" }, ..], "a pair that names a positional property feeds its constructor parameter from the column");
(PostInput[] inputs, ValueMappingException? missingTitle) = Mapping.MapRequired(connection);
Check(inputs is [{ Title: "Post 1", Body: "Body 1" }, { Title: "Post 2", Body: null }, ..] && inputs.Length == 5, "a required property is mapped when a column feeds it");
Check(missingTitle is { PropertyName: "Title", ColumnName: "Title", InnerException: null }, "a result shape with no column for a required property throws ValueMappingException at its first row");
Check(missingTitle?.Message == "No column feeds the required property 'Title' (String) of 'PostInput'; expected a column named 'Title'.", "the missing required column message names the property, its type, the model and the column looked for");
Check(connection.Command("select body from posts where id = 999").Map<PostInput>().ToArray().Length == 0, "an empty result does not report a missing required column");
Check(connection.Command("select null as Title, 'x' as Body").Map<PostInput>().FirstOrDefault() is { Title: null, Body: "x" }, "a NULL in the column that feeds a required property is not an error: the property keeps what it had");
(Review? withNulls, Review? withoutColumns) = Mapping.MapCtorNullColumn(connection);
Check(withNulls == new Review(7, "untitled", 3, null), "a NULL into a non-nullable parameter takes its declared default, and a NULL into a nullable one is null");
Check(withoutColumns == new Review(7), "a parameter with no column takes its declared default, a nullable one included");
Check(Mapping.MapUnmappable(connection) == "'PostCard' cannot be mapped: it has 2 public constructors, PostCard(String title) and PostCard(String title, String body), and none without parameters. Make one of them non-public, or use Map(selector).", "a type with two public constructors and none without parameters throws at the call");
ValueMappingException? bad = Mapping.MapValueError(connection);
Check(bad is { PropertyName: "Id", ColumnName: "id" } && bad.TargetType == typeof(int) && bad.InnerException is not null, "an overflow throws ValueMappingException with the property, column, target type and inner exception");
Check(bad?.Message == "Unable to set value of property 'Id' (Int32) from column 'id'.", "the ValueMappingException message names the property, its type and the column");
Exception? badException = bad;
Check(badException is DataException && badException is not InvalidOperationException, "ValueMappingException is a DataException, not an InvalidOperationException");
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
Check(DapperComparison.OdeRecords(connection).SequenceEqual(DapperComparison.DapperRecords(connection)), "Map<PostRecord>() builds the same positional records as Dapper");

// Before and after: plain ADO.NET and the extensions return the same data.
Db.Seed();
List<Post> plainList = BeforeAfter.PlainList(Db.ConnectionString);
Post[] odeList = await BeforeAfter.OdeList(factory);
Check(plainList.Count == 3 && plainList.Select(p => (p.Id, p.Author, p.Title, p.Body)).SequenceEqual(odeList.Select(p => (p.Id, p.Author, p.Title, p.Body))), "plain ADO.NET list equals the extension list");
Post? plainFirst = BeforeAfter.PlainFirst(Db.ConnectionString);
Post? odeFirst = BeforeAfter.OdeFirst(factory);
Check(plainFirst is not null && odeFirst is not null && (plainFirst.Id, plainFirst.Title, plainFirst.Body) == (odeFirst.Id, odeFirst.Title, odeFirst.Body), "plain ADO.NET first row equals the extension first row");
Check(BeforeAfter.PlainScalar(Db.ConnectionString) == 3 && BeforeAfter.OdeScalar(factory) == 3, "plain and extension scalar agree");
Check(BeforeAfter.PlainNonQuery(Db.ConnectionString) == 3 && BeforeAfter.OdeNonQuery(factory) == 3, "plain and extension non-query agree");
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
long newId = ExpressiveCommands.InsertAndReadId(factory);
Check(newId == 6 && factory.Command("select title from posts where id = 6").ExecuteScalar<string>() == "New", "an insert and select last_insert_rowid() in one command return the generated id to ExecuteScalar<long>()");
factory.Command("delete from posts where id = 6").ExecuteNonQuery();
// Deprecated in 11.0: each old call and the call that replaces it give the same result.
Db.Seed();
(string resultsBefore, string resultsAfter) = Deprecated.Results(connection);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "Results<T>() and Map<T>().ToList() return the same rows");
(resultsBefore, resultsAfter) = await Deprecated.ResultsAsync(connection);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "ResultsAsync<T>() and Map<T>().ToListAsync() return the same rows");
(resultsBefore, resultsAfter) = Deprecated.ResultsFromDataTable(connection);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "ResultsFromDataTable<T>() and ToDataTable().To<T>(map, true) return the same rows");
(resultsBefore, resultsAfter) = Deprecated.SelectorForms(connection);
Check(resultsBefore == "1|1,2|4,5|2,3" && resultsBefore == resultsAfter, "FirstOrDefault, Take, Skip and SkipThenTake with a selector match their Map forms");
Check(Deprecated.SkippedRowSelector(connection) == (true, false), "a selector that throws on a skipped row failed Skip, and does not fail Map(...).Skip");
Check(Deprecated.TakeOfZero(connection) == (false, true), "Take(0) did not run the command; Map(...).Take(0) does");
(resultsBefore, resultsAfter) = await Deprecated.TakeAsyncAndToListAsync(connection);
Check(resultsBefore == "1,2|1,2,3,4,5" && resultsBefore == resultsAfter, "TakeAsync and ToListAsync with a selector match their Map forms");
Check(Deprecated.Collection(connection) == 10, "QueryResultCollection holds rows; a List<T> from Map holds the same");
(string dictBefore, string dictAfter, string dictBeforeAsync, string dictAfterAsync) = await Deprecated.MappedDictionaries(connection);
Check(dictBefore.Length > 0 && dictBefore == dictAfter && dictBeforeAsync == dictAfterAsync && dictBefore == dictBeforeAsync, "AsMappedDictionaries and Retrieve().DequeueAsMappedDictionaries() (awaited too) return the same dictionaries");
(resultsBefore, resultsAfter) = Deprecated.DequeueAs(connection);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "Retrieve().DequeueAs<T>() and Map<T>().ToList() return the same rows");

// The buffered Results calls return the queue they build.
Db.Seed();
(int queueCount, string queueFirstPass, string queueSecondPass, int queueLeft) = BufferedQueue.Replay(connection);
Check(queueCount == 5 && queueFirstPass == "1,2,3,4,5" && queueSecondPass == queueFirstPass, "Results<T>() returns a queue that can be enumerated again");
Check(queueLeft == 0, "TryDequeue releases every row");
Queue<object?> queueBodies = await BufferedQueue.FirstColumn(connection);
Check(queueBodies.Count == 5 && queueBodies.Contains(null) && !queueBodies.Any(value => value is DBNull), "FirstOrdinalResultsAsync() returns a queue that holds null, never DBNull");
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
