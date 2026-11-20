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
Post[] fromFactory = await ExpressiveCommands.FactoryForm(factory, CancellationToken.None);
Check(fromFactory.Select(p => p.Id).SequenceEqual([1, 2, 3]), "factory form returns posts 1-3");
Check(fromFactory[0].Title == "Post 1" && fromFactory[0].Body == "Body 1" && fromFactory[1].Body is null, "factory form maps strings and NULL");

Post[] fromConnection = await ExpressiveCommands.ConnectionFormAsync(connection, CancellationToken.None);
Check(fromConnection.Select(p => p.Id).SequenceEqual([1, 2, 3]), "connection form returns posts 1-3");
Check(connection.State == ConnectionState.Open, "connection form leaves an open connection open");

Check((await ExpressiveCommands.FirstPostAsync(factory, CancellationToken.None))?.Title == "Post 3", "FirstOrDefault by id");
Check((await ExpressiveCommands.ConditionalParamAsync(factory, null, CancellationToken.None)).Length == 5, "AddParamIf skipped: all rows");
Check((await ExpressiveCommands.ConditionalParamAsync(factory, 7, CancellationToken.None)).Length == 2, "AddParamIf applied: author 7 has 2 posts");
Check((await ExpressiveCommands.ParamNamedFromTheArgumentAsync(factory, CancellationToken.None)).Length == 2, "AddParam(search.Author) names the parameter from the argument");

(ConnectionState closedBefore, ConnectionState closedAfter, ConnectionState openBefore, ConnectionState openAfter) = await ExpressiveCommands.OwnershipAsync(connection, CancellationToken.None);
Check(closedBefore == ConnectionState.Closed && closedAfter == ConnectionState.Closed, "a closed connection is closed again after the call");
Check(openBefore == ConnectionState.Open && openAfter == ConnectionState.Open, "an open connection stays open");

(long count, int updated) = await ExpressiveCommands.ScalarAndNonQueryAsync(factory, CancellationToken.None);
Check(count == 3 && updated == 3, "scalar and non-query");
Check(await ExpressiveCommands.ConvertedScalarAsync(factory, CancellationToken.None) == 5, "ExecuteScalar with a conversion");
Check(await ExpressiveCommands.ScalarReadAsInt(factory, CancellationToken.None) == 5, "ExecuteScalarAsync<int> reads a SQLite bigint as int");
Check(factory.Command("select count(*) from posts").ExecuteScalar<int>() == 5, "ExecuteScalar<int> over a SQLite integer converts, as Map<int>() does");
Check(Throws<ValueMappingException>(() => factory.Command("select 5000000000").ExecuteScalar<int>()), "ExecuteScalar<int> over a value that does not fit throws ValueMappingException");
Check(factory.Command("select max(id) from posts where id > 1000").ExecuteScalar<long>() == 0, "ExecuteScalar<long> over NULL is default");
Check(factory.Command("select id from posts where id > 1000").ExecuteScalar<long>() == 0, "ExecuteScalar<long> over no rows is default");
Check(factory.Command("select max(id) from posts where id > 1000").ExecuteScalar<long?>() is null, "ExecuteScalar<long?> over NULL is null");
Check(factory.Command("select max(id) from posts where id <= 3").ExecuteScalar<long?>() == 3, "ExecuteScalar<long?> reads a value");
Check(await factory.Command("select id from posts where id > 1000").ExecuteScalarAsync<long>() == 0, "ExecuteScalarAsync<long> over no rows is default");
Check(factory.Command(Sql.CountSql).AddParam("author", 42).ExecuteScalar<long>() == 3 && factory.Command(Sql.CountSql).AddParam("@author", 42).ExecuteScalar<long>() == 3, "a parameter name is sent as written: SQLite accepts it with or without the @");

// Mapping.
Db.Seed();
Post[] all = await Mapping.MapDefaultAsync(connection, CancellationToken.None);
Check(all.Length == 5 && all[0].Title == "Post 1" && all[0].Author == 42 && all[4].Author == 7, "default mapping, long to int");
Person[] declared = await Mapping.MapDeclaredAsync(connection, CancellationToken.None);
Check(declared.Length == 2 && declared[0].Id == 1 && declared[0].FirstName == "Ada" && declared[1].LastName == "Turing", "a declared RowMap maps the renamed columns");
Person[] namesOnly = await Mapping.MapNamesOnlyAsync(connection, CancellationToken.None);
Check(namesOnly[0].FirstName == "Ada" && namesOnly[0].LastName is "", "a null column in a RowMap means do not map the property, even by name");
Person[] callSite = await Mapping.MapCallSiteAsync(connection, CancellationToken.None);
Check(callSite.Length == 2 && callSite[1].FirstName == "Alan" && callSite[1].LastName == "Turing", "a new RowMap at the call site maps the renamed columns");
Person[] pairs = await Mapping.MapPairsAsync(connection, CancellationToken.None);
Check(pairs.Length == 2 && pairs[1].FirstName == "Alan" && pairs[1].LastName == "Turing", "Map<T> with pairs maps the renamed columns");
RowMap<Person>[] declarationForms = Mapping.DeclarationForms();
Check(declarationForms.All(form => connection.Command("select person_id, first_name, last_name from people order by person_id").Map<Person>(form).ToArray() is [{ Id: 1, FirstName: "Ada", LastName: "Lovelace" }, { Id: 2, FirstName: "Alan" }]), "a RowMap declared with the collection syntax (one pair or several), the constructor (one pair or several), an ImmutableArray or a copy maps that pair and matches the rest by name");
Check(declarationForms[0].Select(pair => pair.Key).SequenceEqual([nameof(Person.Id), nameof(Person.FirstName)]) && declarationForms[0].SequenceEqual(declarationForms[^1]), "a map enumerates the pairs it was declared with, and a copy built from them declares the same pairs");
Check(PersonMaps.PersonNamesOnlyMap.Count() == 3 && PersonMaps.PersonNamesOnlyMap.Single(pair => pair.Value is null).Key == nameof(Person.LastName), "an ignored property is enumerated with a null column");
Order[] converted = await Mapping.MapConverted(connection, CancellationToken.None);
Check(converted is [{ Id: 1, OrderNum: "000042", FirstLoad: "14:05", Customer: "Ada" }, { Id: 2, OrderNum: "000007", FirstLoad: "", Customer: "" }] && converted[0].Summary == "000042 at 14:05", "converters turn order_num into text, the timestamp into HH:mm and trim the customer, a NULL cell is not converted and keeps the property's initial value, and the rest maps by name");
Order[] convertedNull = await Mapping.MapConvertedNull(connection, CancellationToken.None);
Check(convertedNull is [{ OrderNum: "42", FirstLoad: "14:05", Customer: "  Ada  " }, { OrderNum: "7", FirstLoad: "--:--" }], "a converter whose TColumn is nullable is called with null and what it returns is the value of the property; the other columns map without a converter");
Person[] snake = await Mapping.MapSnakeCaseAsync(connection, CancellationToken.None);
Check(snake[0] is { Id: 0, FirstName: "Ada", LastName: "Lovelace" }, "snake_case columns feed PascalCase properties with no map");
Post[] readerView = await Mapping.MapReaderView(connection, CancellationToken.None);
Check(readerView.Length == 5 && readerView[0].Title == "Post 1", "reader.Map<T>() maps the rows of the caller's reader");
Post[] tableView = await Mapping.MapTableView(connection, CancellationToken.None);
Check(tableView.Length == 5 && tableView[0].Title == "Post 1", "table.Map<T>() maps the rows of a DataTable");
Post[] commandView = await Mapping.MapCommandView(connection, CancellationToken.None);
Check(commandView.Length == 3 && connection.State == ConnectionState.Open, "command.Map<T>() on an open connection leaves it open");
using (SqliteConnection closedConnection = factory.Create())
{
	Check(closedConnection.State == ConnectionState.Closed, "a new connection is closed");
	Post[] viaClosed = await Mapping.MapCommandView(closedConnection, CancellationToken.None);
	Check(viaClosed.Length == 3 && closedConnection.State == ConnectionState.Closed, "command.Map<T>() opens a closed connection and closes it again");
}
Person? exactFirst = connection.Command("select 'Alan' as first_name, 'Ada' as FirstName").Map<Person>().FirstOrDefault();
Person? exactSecond = connection.Command("select 'Ada' as FirstName, 'Alan' as first_name").Map<Person>().FirstOrDefault();
Check(exactFirst?.FirstName == "Ada" && exactSecond?.FirstName == "Ada", "an exact column beats an underscore match, in either column order");
RowMap<Person> nullPair = new([new(nameof(Person.FirstName), null)]);
Check(connection.Command("select 'Alan' as first_name").Map<Person>(nullPair).FirstOrDefault()?.FirstName is "", "a null pair stops the underscore match");
RowMap<Person> otherPair = new([new(nameof(Person.FirstName), "given_name")]);
Check(connection.Command("select 'Alan' as first_name").Map<Person>(otherPair).FirstOrDefault()?.FirstName is "", "a property named by a pair is not matched by an underscore column");
Check((await Mapping.MapNullsAsync(connection, CancellationToken.None)) is { Quantity: 1, Discount: null, Note: null, Title: "" }, "NULL keeps the initializer of a non-nullable property (a value type, or a reference with nullable annotations on) and gives null to nullable properties");
Check(connection.Command("select null").Map<int>().Single() == 0 && connection.Command("select null").Map<int?>().Single() is null && connection.Command("select null").Map<string>().Single() is null, "a single value: NULL into int is 0, into int? and string is null");
Check(Throws<Exception>(() => Dapper.SqlMapper.QuerySingle<int>(connection, "select null")), "Dapper QuerySingle<int> over NULL throws");
Check(connection.Command("select 1 as Id, 'Ada' as FirstName, 'Lovelace' as LastName").Map<Person>(new RowMap<Person>([new(nameof(Person.LastName), null)])).Single() is { Id: 1, FirstName: "Ada", LastName: "" }
	&& connection.Command("select 1 as Id, 'Ada' as FirstName, 'Lovelace' as LastName").Map<Person>().Single() is { LastName: "Lovelace" }, "new(nameof(Person.LastName), null) needs no null!: the member stays unmapped although a column of that name exists, and the other members still map");
Check(Throws<InvalidOperationException>(() => connection.Command("select 1 as Id, 'Ada' as Name").Map<PairRecord>()) && Throws<InvalidOperationException>(() => connection.Command("select 1 as Id, 'Ada' as Name").Map<AllOptional>()), "a record with a second public constructor, and a constructor with only optional parameters next to another one, are the two-or-more-public-constructors case");
Check(connection.Command("select 1 as Id, 'Ada' as Name").Map<ParameterlessRecord>().Single() is { Id: 1, Name: "Ada" }, "a record with a second, parameterless public constructor is built with new(), then its properties are set");
PostRecord[] records = await Mapping.MapRecordAsync(connection, CancellationToken.None);
Check(records.Length == 5 && records[0] == new PostRecord(1, 42, "Post 1", "Body 1") && records[1] == new PostRecord(2, 42, "Post 2", null), "a positional record is built through its constructor, NULL included");
Check((await Mapping.MapRecordExtraAsync(connection, CancellationToken.None))[0] == new PostSummary(1, "Post 1") { Views = 10 }, "a record with an init property: the constructor, then the init property from its column, unmatched columns ignored, an init property with no column keeps its initializer");
Check((await Mapping.MapRecordDeclaredAsync(connection, CancellationToken.None)) is [{ Title: "Post 1" }, ..], "a pair that names a positional property feeds its constructor parameter from the column");
(PostInput[] inputs, ValueMappingException? missingTitle) = await Mapping.MapRequiredAsync(connection, CancellationToken.None);
Check(inputs is [{ Title: "Post 1", Body: "Body 1" }, { Title: "Post 2", Body: null }, ..] && inputs.Length == 5, "a required property is mapped when a column feeds it");
Check(missingTitle is { PropertyName: "Title", ColumnName: "Title", InnerException: null }, "a result shape with no column for a required property throws ValueMappingException at its first row");
Check(missingTitle?.Message == "No column feeds the required property 'Title' (String) of 'PostInput'; expected a column named 'Title'.", "the missing required column message names the property, its type, the model and the column looked for");
Check(connection.Command("select body from posts where id = 999").Map<PostInput>().ToArray().Length == 0, "an empty result does not report a missing required column");
Check(connection.Command("select null as Title, 'x' as Body").Map<PostInput>().FirstOrDefault() is { Title: null, Body: "x" }, "a NULL in the column that feeds a required property is not an error: the property keeps what it had");
(Review? withNulls, Review? withoutColumns) = await Mapping.MapCtorNullColumnAsync(connection, CancellationToken.None);
Check(withNulls == new Review(7, "untitled", 3, null), "a NULL into a non-nullable parameter takes its declared default, and a NULL into a nullable one is null");
Check(withoutColumns == new Review(7), "a parameter with no column takes its declared default, a nullable one included");
Check(Mapping.MapUnmappable(connection) == "'PostCard' cannot be mapped: it has 2 public constructors, PostCard(String title) and PostCard(String title, String body), and none without parameters. Make one of them non-public, or use Map(selector).", "a type with two public constructors and none without parameters throws at the call");
ValueMappingException? bad = await Mapping.MapValueErrorAsync(connection, CancellationToken.None);
Check(bad is { PropertyName: "Id", ColumnName: "id" } && bad.TargetType == typeof(int) && bad.InnerException is not null, "an overflow throws ValueMappingException with the property, column, target type and inner exception");
Check(bad?.Message == "Unable to set value of property 'Id' (Int32) from column 'id'.", "the ValueMappingException message names the property, its type and the column");
Exception? badException = bad;
Check(badException is DataException && badException is not InvalidOperationException, "ValueMappingException is a DataException, not an InvalidOperationException");
Check(ReferenceEquals(PersonMaps.PersonMap, PersonMaps.PersonMap), "a map held with field ??= is built once and shared");
Check(ReferenceEquals(HoldingAMap.BackingMap, HoldingAMap.BackingMap), "a map held in a backing field is built once and shared");
Check(Mapping.MapUnknownProperty().Contains("'Headline'", StringComparison.Ordinal), "a RowMap with an unknown property throws when it is created");
Check((await Mapping.MapPagingAsync(connection, CancellationToken.None)).Select(p => p.Id).SequenceEqual([2, 3]), "Skip(1).Take(2)");
(Post? first, Post? single) = await Mapping.FirstAndSingleAsync(connection, CancellationToken.None);
Check(first?.Id == 1 && single?.Id == 2, "first and single");
Check(Throws<InvalidOperationException>(() => connection.Command("select * from posts").Map<Post>().SingleOrDefault()), "SingleOrDefault throws for more than one row");
Check((await Mapping.MapSelectorAsync(connection, CancellationToken.None))[0] == "Post 1 (10 views)", "Map(selector)");
Check(
	(await Mapping.MapSingleValueAsync(connection, CancellationToken.None)).SequenceEqual(
		connection
			.Command("select id from posts order by id")
			.Map(static r => (int)r.GetInt64(0))
			.ToList())
	&& (await Mapping.MapSingleValueAsync(connection, CancellationToken.None)).Length == 5,
	"Map<int>() reads the first column, a SQLite long converted to int");
Check(
	(await ExpressiveCommands.ColumnOfIds(factory, CancellationToken.None))
		.SequenceEqual([1, 2, 3, 4, 5]),
	"ToArrayAsync<int>(token) on a command");
Check(Mapping.StreamRows(connection) == 15, "Read streams rows inside the handler");
Check((await Mapping.ByIdAsync(connection, CancellationToken.None)).Count == 5, "ToDictionaryAsync");
(List<PostSummary> summaries, PostSummary? newest) = await Mapping.QueryFactoryCallers(factory, CancellationToken.None);
Check(summaries.Select(s => s.Id).SequenceEqual([3, 2, 1]) && newest == new PostSummary(3, "Post 3"), "a query factory returns a MappedQuery<PostSummary> (a factory map) and each caller picks the terminal");
int projected = 0;
var lazyIds = connection.Command("select * from posts order by id").Map(record =>
{
	projected++;
	return record.GetInt32(0);
});
Check(projected == 0, "Map(selector) runs nothing until a terminal runs");
Check((await lazyIds.ToListAsync(CancellationToken.None)).Count == 5 && projected == 5, "the selector runs once per row");
Check(await lazyIds.FirstOrDefaultAsync(CancellationToken.None) == 1 && projected == 6, "a terminal that stops early maps only the rows it reads, and runs the command again");
Dictionary<int, string> bodiesById = await Mapping.BodiesByIdAsync(connection, CancellationToken.None);
Check(bodiesById.Count == 5 && bodiesById[1] == "Body 1" && bodiesById[2] == "", "ToDictionaryAsync with a value selector keeps only the value (NULL as empty text)");
Check(connection.Command("select * from posts order by id").Map<Post>().First().Id == 1, "First returns the first of several rows");
Check(Throws<InvalidOperationException>(() => connection.Command("select * from posts").Map<Post>().Single()), "Single throws for more than one row");
Check(Throws<InvalidOperationException>(() => connection.Command("select * from posts where id = 999").Map<Post>().First()), "First throws for no rows");
Check(Throws<InvalidOperationException>(() => connection.Command("select * from posts where id = 999").Map<Post>().Single()), "Single throws for no rows");

// Channels and Dataflow.
List<Post> viaChannel = await ChannelsAndDataflow.ToChannelSample(factory, CancellationToken.None);
Check(viaChannel.Select(p => p.Id).SequenceEqual([1, 2, 3, 4, 5]), "ToChannel delivers every row in order");
(long delivered, List<Post> viaBlock) = await ChannelsAndDataflow.ToTargetBlockSample(factory, CancellationToken.None);
Check(delivered == 5 && viaBlock.Count == 5, "ToTargetBlockAsync delivers every row");

// Streaming from a mapped query: the query is the producer, and the
// consumers do the slow work while it is still being read.
List<string> expectedLines =
[
	.. Enumerable
		.Range(1, 5)
		.Select(static i => $"#{i} Post {i}"),
];

List<string> pcLines = await ChannelsAndDataflow
	.ProducerConsumerSample(connection, CancellationToken.None);
Check(
	pcLines.SequenceEqual(expectedLines),
	"a producer/consumer over ToBufferedAsyncStream() handles every row, in order");

List<string> pipelineLines = await ChannelsAndDataflow
	.PipelineSample(connection, CancellationToken.None);
Check(
	pipelineLines.SequenceEqual(expectedLines),
	"query -> ToBufferedAsyncStream() -> transform -> channel -> final consumer delivers every row in order");

List<string> bufferLines = await ChannelsAndDataflow
	.BufferSample(connection, CancellationToken.None);
Check(
	bufferLines.SequenceEqual(expectedLines),
	"buffering with ToListAsync returns the same rows as the stream");

// Async buffers and streams.
string rendered = await AsyncBuffers
	.RenderPostsAsync(factory, CancellationToken.None);
Check(
	rendered.StartsWith("[{\"Id\":1,", StringComparison.Ordinal)
		&& rendered.Contains("\"Title\":\"Post 5\"", StringComparison.Ordinal)
		&& rendered.EndsWith("}]", StringComparison.Ordinal),
	"a buffer made by a data layer is awaited and serialized by the layer above");

AsyncBuffers.ItemsFacts facts = await AsyncBuffers
	.ItemsViewAsync(factory, CancellationToken.None);
Check(
	facts is { Count: 5, ThirdId: 3, IdSum: 15, CopyLength: 5 },
	"the Items view has a count, an indexer, an enumerator and a LINQ copy");

Check(
	(await AsyncBuffers.SkipTakeAsync(factory, CancellationToken.None))
		.SequenceEqual([2, 3]),
	"ToAsyncBuffer after Skip(1).Take(2) holds posts 2 and 3");

Check(
	(await AsyncBuffers.ChunkStreamAsync(factory, CancellationToken.None))
		.SequenceEqual([1, 2, 3, 4, 5]),
	"ToAsyncBufferStream yields every row in order");

(List<int> elementIds, bool secondThrew) = await AsyncBuffers
	.ElementStreamAsync(factory, CancellationToken.None);
Check(
	elementIds.SequenceEqual([1, 2, 3, 4, 5]) && secondThrew,
	"ToBufferedAsyncStream yields every row in order and is single-use");

Check(
	(await AsyncBuffers.LinqAsyncAsync(factory, CancellationToken.None))
		.SequenceEqual(["Post 1", "Post 2", "Post 3"]),
	"System.Linq over ToBufferedAsyncStream projects the rows");

Check(
	(await AsyncBuffers.SmallResultAsync(factory, CancellationToken.None))
		.Length == 5,
	"ToArrayAsync returns the small result");

CountingPool<Post> overloadPool = new();
Check(
	(await AsyncBuffers.OverloadsAsync(
		factory,
		overloadPool,
		CancellationToken.None)).All(static n => n == 5),
	"the three ToAsyncBuffer overloads each hold the five rows");
await AsyncBuffers.SettleAsync(overloadPool);
Check(
	overloadPool.Rented == overloadPool.Returned && overloadPool.Rented > 0,
	"disposing the buffers returns every array to the pool");

CountingPool<Post> keepPool = new();
await AsyncBuffers.KeepsRowsAsync(
	factory,
	keepPool,
	CancellationToken.None);
await AsyncBuffers.SettleAsync(keepPool);
Check(
	keepPool.Dirty == keepPool.Returned && keepPool.Returned > 0,
	"nothing is cleared by default: the arrays go back with their rows");

CountingPool<Post> clearPool = new();
await AsyncBuffers.ClearingAsync(
	factory,
	clearPool,
	CancellationToken.None);
await AsyncBuffers.SettleAsync(clearPool);
Check(
	clearPool.Dirty == 0 && clearPool.Returned > 0,
	"clearOnDispose: true clears the rows before the arrays go back");

Check(
	AsyncBuffers.NullPoolThrows(factory, CancellationToken.None),
	"a null pool throws ArgumentNullException at the call");

Check(
	await AsyncBuffers.CallerCancelAsync(factory),
	"the caller's own cancel reaches Items");
Check(
	await AsyncBuffers.StreamCancelAsync(factory),
	"the caller's own cancel reaches a stream");

CountingPool<Post> earlyPool = new();
Check(
	await AsyncBuffers.DisposeEarlyAsync(
		factory,
		earlyPool,
		CancellationToken.None)
		&& earlyPool.Rented == earlyPool.Returned,
	"DisposeAsync returns the arrays and a disposed buffer has no Items");

// Transactions.
Check((await Transactions.ConditionalAsync(factory, true, CancellationToken.None)) is (true, 1), "returning true commits");
Check((await Transactions.ConditionalAsync(factory, false, CancellationToken.None)) is (false, 0), "returning false rolls back");

// Dapper side by side.
Check((await DapperComparison.DapperListAsync(connection)).Select(p => p.Id).SequenceEqual((await DapperComparison.OdeListAsync(connection, CancellationToken.None)).Select(p => p.Id)), "same list");
Check((await DapperComparison.DapperFirstAsync(connection))?.Title == (await DapperComparison.OdeFirstAsync(connection, CancellationToken.None))?.Title, "same first");
Check(await DapperComparison.DapperScalarAsync(connection) == await DapperComparison.OdeScalarAsync(connection, CancellationToken.None), "same scalar");
Check(await DapperComparison.DapperExecuteAsync(connection) == await DapperComparison.OdeExecuteAsync(connection, CancellationToken.None), "same execute");
Check((await DapperComparison.DapperRecordsAsync(connection)).Count == 5, "Dapper binds a positional record");
Check((await DapperComparison.OdeRecordsAsync(connection, CancellationToken.None)).SequenceEqual(await DapperComparison.DapperRecordsAsync(connection)), "Map<PostRecord>() builds the same positional records as Dapper");

// Before and after: plain ADO.NET and the extensions return the same data.
Db.Seed();
List<Post> plainList = await BeforeAfter.PlainListAsync(Db.ConnectionString, CancellationToken.None);
Post[] odeList = await BeforeAfter.OdeList(factory, CancellationToken.None);
Check(plainList.Count == 3 && plainList.Select(p => (p.Id, p.Author, p.Title, p.Body)).SequenceEqual(odeList.Select(p => (p.Id, p.Author, p.Title, p.Body))), "plain ADO.NET list equals the extension list");
Post? plainFirst = await BeforeAfter.PlainFirstAsync(Db.ConnectionString, CancellationToken.None);
Post? odeFirst = await BeforeAfter.OdeFirstAsync(factory, CancellationToken.None);
Check(plainFirst is not null && odeFirst is not null && (plainFirst.Id, plainFirst.Title, plainFirst.Body) == (odeFirst.Id, odeFirst.Title, odeFirst.Body), "plain ADO.NET first row equals the extension first row");
Check(await BeforeAfter.PlainScalarAsync(Db.ConnectionString, CancellationToken.None) == 3 && await BeforeAfter.OdeScalarAsync(factory, CancellationToken.None) == 3, "plain and extension scalar agree");
Check(await BeforeAfter.PlainNonQueryAsync(Db.ConnectionString, CancellationToken.None) == 3 && await BeforeAfter.OdeNonQueryAsync(factory, CancellationToken.None) == 3, "plain and extension non-query agree");
// The comparisons in the docs: both sides return the same data.
Db.Seed();
string connectionString = Db.ConnectionString;
Check((await ConnectionPattern.DapperList(connectionString)).Select(p => p.Id).SequenceEqual((await ConnectionPattern.OdeList(factory, CancellationToken.None)).Select(p => p.Id)), "Dapper and factory list agree");
Check((await ConnectionPattern.OdeListOwn(connectionString, CancellationToken.None)).Select(p => p.Id).SequenceEqual([1, 2, 3]), "own-connection list");
Check((await ConnectionPattern.PlainListAligned(connectionString, CancellationToken.None)).Select(p => p.Id).SequenceEqual((await ConnectionPattern.OdeListAligned(factory, CancellationToken.None)).Select(p => p.Id)), "plain ADO.NET and factory aligned list agree");
Check((await ConnectionPattern.DapperFirstAsync(connectionString))?.Title == (await ConnectionPattern.OdeFirstAsync(factory, CancellationToken.None))?.Title, "Dapper and factory first row agree");
Check(await ConnectionPattern.DapperScalarAsync(connectionString) == 3 && await ConnectionPattern.OdeScalarAsync(factory, CancellationToken.None) == 3, "Dapper and factory scalar agree");
Check(await ConnectionPattern.DapperNonQueryAsync(connectionString) == 3 && await ConnectionPattern.OdeNonQueryAsync(factory, CancellationToken.None) == 3, "Dapper and factory non-query agree");
Check((await DependencyInjectionSample.ResolveAndUseAsync(CancellationToken.None)).Count == 3, "a factory registered once is injected");
Check((await ConnectionPattern.RenamedColumnsAsync(connection, CancellationToken.None)) is [{ FirstName: "Ada" }, { FirstName: "Alan" }], "renamed columns with nameof");
Check(Throws<InvalidOperationException>(() => connection.Command("select * from posts").Map<Post>().SingleOrDefault()), "SingleOrDefault throws for more than one row (again)");
Check(factory.Command("select * from posts where id = 999").Map<Post>().FirstOrDefault() is null, "FirstOrDefault is null for no rows");
// Transactions: ADO.NET needs an open connection to begin one; Dapper begins nothing itself.
long RowsFrom100() => factory.Command("select count(*) from posts where id >= 100").ExecuteScalar<long>();
void ClearFrom100() => factory.Command("delete from posts where id >= 100").ExecuteNonQuery();
await ConnectionPattern.DapperTransactionAsync(connectionString);
Check(RowsFrom100() == 2, "the Dapper transaction committed two rows");
ClearFrom100();
await ConnectionPattern.OdeTransactionAsync(factory, CancellationToken.None);
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
Check(await Transactions.Async(factory, CancellationToken.None) == 1, "ExecuteTransactionAsync commits when the action returns");
using (CancellationTokenSource cancelledInAction = new())
{
	bool cancelled = false;
	try
	{
		await factory.UsingAsync(c => c.ExecuteTransactionAsync(async tx =>
		{
			await tx.Command(Sql.InsertSql).AddParam("id", 100).ExecuteNonQueryAsync();
			cancelledInAction.Cancel();
		}, cancelledInAction.Token));
	}
	catch (OperationCanceledException) { cancelled = true; }

	Check(cancelled && RowsFrom100() == 0, "ExecuteTransactionAsync: a token cancelled by the time the action returns throws OperationCanceledException and nothing commits");
}

using (CancellationTokenSource liveToken = new())
using (CommitTokenConnection recording = new(connectionString))
{
	await recording.ExecuteTransactionAsync(_ => ValueTask.CompletedTask, liveToken.Token);
	Check(recording.CommitTokens is [{ CanBeCanceled: false }], "ExecuteTransactionAsync sends the commit without the caller's token (CancellationToken.None), though the token was passed and live");
}
long newId = await ExpressiveCommands.InsertAndReadIdAsync(factory, CancellationToken.None);
Check(newId == 6 && factory.Command("select title from posts where id = 6").ExecuteScalar<string>() == "New", "an insert and select last_insert_rowid() in one command return the generated id to ExecuteScalar<long>()");
factory.Command("delete from posts where id = 6").ExecuteNonQuery();
long draftId = await Mapping.InsertReturningIdAsync(factory, CancellationToken.None);
Check(draftId == 6 && factory.Command("select title from posts where id = 6").ExecuteScalar<string>() == "Draft", "an insert returns its new id, read with SingleAsync");
factory.Command("delete from posts where id = 6").ExecuteNonQuery();
// Deprecated in 11.0: each old call and the call that replaces it give the same result.
Db.Seed();
(string resultsBefore, string resultsAfter) = Deprecated.Results(connection);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "Results<T>() and Map<T>().ToList() return the same rows");
(resultsBefore, resultsAfter) = await Deprecated.ResultsAsync(connection, CancellationToken.None);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "ResultsAsync<T>() and Map<T>().ToListAsync() return the same rows");
(resultsBefore, resultsAfter) = Deprecated.ResultsFromDataTable(connection);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "ResultsFromDataTable<T>() and ToDataTable() read with ToDataTable().Map(map).ToList() return the same rows");
(resultsBefore, resultsAfter) = Deprecated.SelectorForms(connection);
Check(resultsBefore == "1|1,2|4,5|2,3" && resultsBefore == resultsAfter, "FirstOrDefault, Take, Skip and SkipThenTake with a selector match their Map forms");
Check(Deprecated.SkippedRowSelector(connection) == (true, false), "a selector that throws on a skipped row failed Skip, and does not fail Map(...).Skip");
Check(Deprecated.TakeOfZero(connection) == (false, true), "Take(0) did not run the command; Map(...).Take(0) does");
(resultsBefore, resultsAfter) = await Deprecated.TakeAsyncAndToListAsync(connection, CancellationToken.None);
Check(resultsBefore == "1,2|1,2,3,4,5" && resultsBefore == resultsAfter, "TakeAsync and ToListAsync with a selector match their Map forms");
Check(await Deprecated.CollectionAsync(connection, CancellationToken.None) == 10, "QueryResultCollection holds rows; a List<T> from Map holds the same");
(string dictBefore, string dictAfter, string dictBeforeAsync, string dictAfterAsync) = await Deprecated.MappedDictionaries(connection);
Check(dictBefore.Length > 0 && dictBefore == dictAfter && dictBeforeAsync == dictAfterAsync && dictBefore == dictBeforeAsync, "AsMappedDictionaries and Retrieve().DequeueAsMappedDictionaries() (awaited too) return the same dictionaries");
(resultsBefore, resultsAfter) = Deprecated.DequeueAs(connection);
Check(resultsBefore.Length > 0 && resultsBefore == resultsAfter, "Retrieve().DequeueAs<T>() and Map<T>().ToList() return the same rows");

// AsyncReads(): the rows are the same as with the default.
// SQLite cannot show which read was used;
// AsyncReadsTests in the Core tests pins that.
Db.Seed();
string readDefault = Describe(
	await AsyncReads.DefaultAsync(connection, CancellationToken.None));
string readOptIn = Describe(
	await AsyncReads.OptInAsync(connection, CancellationToken.None));
Check(
	readDefault == "1:Post 1|2:Post 2|3:Post 3|4:Post 4|5:Post 5"
		&& readDefault == readOptIn,
	"the default and AsyncReads() return the same five posts");
Check(
	Describe(
		await AsyncReads.ViewAsync(connection, CancellationToken.None))
		== readOptIn,
	"AsyncReads() after Map<T>() returns the rows the command form does");
Check(
	Describe(
		await AsyncReads.DbCommandAsync(connection, CancellationToken.None))
		== readOptIn,
	"AsyncReads() on a plain IDbCommand's mapped view returns the same rows");
string titlesOptIn = string.Join(
	"|",
	await AsyncReads.FuncAsync(connection, CancellationToken.None));
string titlesDefault = string.Join(
	"|",
	await connection
		.Command("select title from posts order by id")
		.Map(static record
			=> record.GetString(0))
		.ToListAsync(CancellationToken.None));
Check(
	titlesDefault == "Post 1|Post 2|Post 3|Post 4|Post 5"
		&& titlesDefault == titlesOptIn,
	"Map(Func) with and without AsyncReads() return the same titles");
(List<Post> skipBefore, List<Post> skipAfter)
	= await AsyncReads.OrderAsync(connection, CancellationToken.None);
Check(
	Describe(skipBefore) == "2:Post 2|3:Post 3"
		&& Describe(skipAfter) == Describe(skipBefore),
	"AsyncReads() before or after Skip(1).Take(2) returns posts 2 and 3");
Check(
	Describe(
		connection
			.Command("select * from posts order by id")
			.AsyncReads()
			.Map<Post>()
			.ToList()) == readDefault,
	"a synchronous terminal after AsyncReads() returns the same rows");
// Reader cancellation: the token is checked before each read,
// and a cancel throws.
Db.Seed();
(int[] canceledSeen, bool canceledThrew)
	= await ReaderCancellation.SelectAsyncCanceled(connection);
Check(
	canceledThrew && canceledSeen.SequenceEqual([1, 2]),
	"a canceled SelectAsync throws OperationCanceledException, "
	+ "and the rows before the cancel were seen");
(canceledSeen, canceledThrew)
	= ReaderCancellation.ForEachCanceled(connection);
Check(
	canceledThrew && canceledSeen.SequenceEqual([1, 2]),
	"the sync form: a canceled ForEach throws "
	+ "OperationCanceledException, "
	+ "and the rows before the cancel were seen");

// Shaping rows: the recipes of the article.
Db.Seed();
await ShapingRows.SeedAsync(connection, CancellationToken.None);
(Plant[] plantsInitializer, Plant[] plantsFactory) = await ShapingRows.NullToEmptyAsync(connection, CancellationToken.None);
Check(
	plantsInitializer is [{ Id: 1, Code: "PRESS", SortDescr: "Stamping" }, { Id: 2, Code: "", SortDescr: "" }],
	"a NULL keeps the initializer of a non-nullable member and the converter is not called for it");
Check(
	plantsFactory is [{ Id: 1, Code: "PRESS", SortDescr: "Stamping" }, { Id: 2, Code: "", SortDescr: "" }],
	"a factory parameter declared string? receives null for a NULL, so the function can write ?? \"\"");
(PostStamp[] stampsAfterClose, PostStampInit[] stampsInQuery) = await ShapingRows.ValueFromOutsideAsync(connection, CancellationToken.None);
Check(
	stampsAfterClose.Length == 5 && stampsAfterClose.All(static stamp => stamp.Serial == "S-100"),
	"a value from outside the row is set on the rows after the close");
Check(
	stampsInQuery.Length == 5 && stampsInQuery.All(static stamp => stamp.Serial == "S-100") && stampsInQuery[2] is { Id: 3, Title: "Post 3" },
	"a value from outside the row can come back from the query as a column");
(Employee[] employeesIgnored, Employee[] employeesListed, Employee[] employeesNoManagerColumn) = await ShapingRows.ChooseWhatIsReadAsync(connection, CancellationToken.None);
Check(
	employeesIgnored is [{ Badge: "E-1", Desk: "", Manager: "Kim" }, { Badge: "E-2", Desk: "", Manager: "Lee" }],
	"a null column in the map leaves the member unmapped although a column of its name exists");
Check(
	employeesListed is [{ Badge: "E-1", Desk: "", Manager: "Kim" }, { Badge: "E-2", Desk: "", Manager: "Lee" }],
	"a factory map fills only what its function takes: the desk column is read by nobody");
Check(
	employeesNoManagerColumn is [{ Badge: "E-1", Manager: "unknown" }, { Badge: "E-2", Manager: "unknown" }],
	"a factory parameter no column feeds is null (its default) and nothing throws");
(StrictEmployee[] strictFound, ValueMappingException? strictMissing) = await ShapingRows.StrictAsync(connection, CancellationToken.None);
Check(
	strictFound is [{ Badge: "E-1", Manager: "Kim" }, { Badge: "E-2", Manager: "Lee" }],
	"a required member is mapped when a column feeds it");
Check(
	strictMissing is { PropertyName: "Badge" },
	"a result with no column for a required member throws ValueMappingException at its first row");
(WideReading wide, WideEnds wideEnds) = await ShapingRows.WideAsync(connection, CancellationToken.None);
Check(
	wide.Channel01 == 1 && wide.Channel09 == 9 && wide.Channel17 == 17,
	"a record with 17 constructor parameters maps through Map<T>()");
Check(
	wideEnds == new WideEnds(1, 17),
	"a factory with 17 parameters maps too");
(Post[] filterInSql, Post[] filterAfterClose, Post[] filterBusy, Post[] filterWithNulls) = await ShapingRows.FilterAsync(connection, CancellationToken.None);
Check(
	filterInSql.Select(static post => post.Id).SequenceEqual([3, 4, 5]),
	"a filter in the query's WHERE returns only the wanted rows");
Check(
	filterAfterClose.Select(static post => post.Id).SequenceEqual([3, 4, 5]),
	"a filter over the array after the close returns the wanted rows");
Check(
	filterWithNulls.Length == 5 && filterWithNulls.Count(static post => post is null) == 2 && filterBusy.Select(static post => post.Id).SequenceEqual([3, 4, 5]),
	"a factory that returns null leaves a null in the result for each skipped row, and the list is filtered after the close");
(long? lastNone, long? lastSome) = await ShapingRows.OneNamedColumnAsync(connection, CancellationToken.None);
Check(
	lastNone is null && lastSome == 3,
	"Map<long?>() over one selected column gives null for a NULL and the value otherwise");
Check(
	ShapingRows.SingleValueFactoryIsRejected(),
	"a factory map for a single value is rejected: a single value is not a row model");

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

static string Describe(IEnumerable<Post> posts)
	=> string.Join(
		"|",
		posts.Select(static p => $"{p.Id}:{p.Title}"));
