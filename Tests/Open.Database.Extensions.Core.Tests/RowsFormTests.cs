using Microsoft.Data.Sqlite;
using Open.Database.Extensions.Core;
using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Tests;

#nullable enable

// Rows without a model: AsObjectArrays() and AsDictionaryRows() are mapped views like Map<T>(), over a row source that reads
// every column once, in order. What it pins: the values and NULLs, the names (duplicates, case, missing, NULL), a default
// row, an empty result, a result set with no columns, rows that keep their values after the connection closes and never
// share an array, the read mode, read-through and SingleResult, pooling (the arrays that hold the rows are cleared),
// every receiver, and that nothing about a column list is retained after its result set.
[ExcludeFromCodeCoverage]
public static class RowsFormTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static SqliteConnection Open()
	{
		var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		return connection;
	}

	const string TwoRows = "select 1 as Id, 'a' as Name, null as Missing union all select 2, 'b', null";

	// ---- AsObjectArrays ----

	[Fact]
	public static async Task ObjectArrays_HoldTheValues_AndANullIsNull()
	{
		await using SqliteConnection c = Open();

		List<object?[]> rows = await c.Command(TwoRows).AsObjectArrays().ToListAsync(Ct);

		Assert.Equal(2, rows.Count);
		Assert.Equal([1L, "a", null], rows[0]);
		Assert.Equal([2L, "b", null], rows[1]);
	}

	[Fact]
	public static async Task ObjectArrays_KeepTheirValues_AfterTheConnectionIsClosed_AndNoTwoRowsShareAnArray()
	{
		List<object?[]> rows;
		await using (SqliteConnection c = Open())
			rows = await c.Command(TwoRows).AsObjectArrays().ToListAsync(Ct);

		Assert.NotSame(rows[0], rows[1]);
		Assert.Equal([1L, "a", null], rows[0]);
		rows[0][0] = 99L;
		Assert.Equal(2L, rows[1][0]);
	}

	[Fact]
	public static async Task ObjectArrays_OfNoRows_IsEmpty_AndOfNoColumns_IsAnEmptyArray()
	{
		await using SqliteConnection c = Open();

		Assert.Empty(await c.Command("select 1 where 0").AsObjectArrays().ToListAsync(Ct));

		DbDataReader noColumns = FakeReader.CreateDb([], [[]]);
		List<object?[]> rows = await new FakeDbCommand(new FakeDbConnection(), () => noColumns).AsObjectArrays().ToListAsync(Ct);
		Assert.Empty(Assert.Single(rows));
	}

	// ---- AsDictionaryRows ----

	[Fact]
	public static async Task DictionaryRows_HoldTheNames_TheValuesAndANullCell()
	{
		await using SqliteConnection c = Open();

		List<DictionaryRow> rows = await c.Command(TwoRows).AsDictionaryRows().ToListAsync(Ct);

		DictionaryRow first = rows[0];
		Assert.Equal(3, first.Count);
		Assert.Equal(3, first.FieldCount);
		Assert.Equal(1L, first["Id"]);
		Assert.Equal("a", first["Name"]);
		Assert.True(first.TryGetValue("Missing", out object? value));
		Assert.Null(value);
		Assert.True(first.ContainsKey("Missing"));
		Assert.Equal(["Id", "Name", "Missing"], first.Keys);
		Assert.Equal([1L, "a", null], first.Values);
		Assert.Equal(2L, rows[1]["Id"]);
	}

	[Fact]
	public static async Task DictionaryRow_FindsAnExactName_ThenIgnoringCase_AndAMissingNameThrows()
	{
		await using SqliteConnection c = Open();
		DictionaryRow row = (await c.Command("select 1 as Id, 2 as id, 3 as Other").AsDictionaryRows().ToListAsync(Ct)).Single();

		Assert.Equal(1L, row["Id"]);
		Assert.Equal(2L, row["id"]);
		Assert.Equal(1L, row["ID"]);
		Assert.Equal(3L, row["OTHER"]);
		Assert.Equal(3, row.Count);
		Assert.False(row.TryGetValue("nope", out object? none));
		Assert.Null(none);
		Assert.False(row.ContainsKey("nope"));
		Assert.Throws<KeyNotFoundException>(() => row["nope"]);
		Assert.Throws<ArgumentNullException>(() => row[null!]);
		Assert.Throws<ArgumentNullException>(() => row.TryGetValue(null!, out _));
	}

	[Fact]
	public static async Task DictionaryRow_OfTwoColumnsWithTheSameName_FirstWins_AndTheOrdinalReachesBoth()
	{
		await using SqliteConnection c = Open();
		DictionaryRow row = (await c.Command("select 1 as id, 2 as id").AsDictionaryRows().ToListAsync(Ct)).Single();

		Assert.Single(row.Keys);
		Assert.Equal(2, row.FieldCount);
		Assert.Equal(1L, row["id"]);
		Assert.Equal(1L, row[0]);
		Assert.Equal(2L, row[1]);
		Assert.Single(row);
		Assert.Equal(KeyValuePair.Create<string, object?>("id", 1L), row.Single());
		var copy = row.ToDictionary();
		Assert.Equal(1L, Assert.Single(copy).Value);
		Assert.Throws<ArgumentOutOfRangeException>(() => row[2]);
		Assert.Throws<ArgumentOutOfRangeException>(() => row[-1]);
	}

	[Fact]
	public static async Task DictionaryRow_ToDictionary_IsACopyTheCallerOwns()
	{
		await using SqliteConnection c = Open();
		DictionaryRow row = (await c.Command(TwoRows).AsDictionaryRows().ToListAsync(Ct))[0];

		var copy = row.ToDictionary();
		copy["Id"] = 100L;

		Assert.Equal(1L, row["Id"]);
		Assert.Equal(3, copy.Count);
		Assert.Null(copy["Missing"]);
	}

	[Fact]
	public static void ADefaultDictionaryRow_IsEmpty_NeverANullReference()
	{
		DictionaryRow row = default;

		Assert.Empty(row.Keys);
		Assert.Equal(0, row.FieldCount);
		Assert.Empty(row);
		Assert.Empty(row.Keys);
		Assert.Empty(row.Values);
		Assert.Empty(row.ToDictionary());
		Assert.False(row.ContainsKey("a"));
		Assert.False(row.TryGetValue("a", out object? value));
		Assert.Null(value);
		Assert.Throws<KeyNotFoundException>(() => row["a"]);
		Assert.Throws<ArgumentOutOfRangeException>(() => row[0]);
		foreach (KeyValuePair<string, object?> _ in row) Assert.Fail("A default row has nothing to enumerate.");
	}

	[Fact]
	public static async Task DictionaryRows_OfNoRows_AreEmpty_AndOfNoColumns_AnEmptyRow()
	{
		await using SqliteConnection c = Open();

		Assert.Empty(await c.Command("select 1 as Id where 0").AsDictionaryRows().ToListAsync(Ct));

		DbDataReader noColumns = FakeReader.CreateDb([], [[]]);
		DictionaryRow row = Assert.Single(await new FakeDbCommand(new FakeDbConnection(), () => noColumns).AsDictionaryRows().ToListAsync(Ct));
		Assert.Empty(row.Keys);
		Assert.Equal(0, row.FieldCount);
	}

	[Fact]
	public static async Task DictionaryRows_OfOneResultSet_ShareOneListOfNames()
	{
		await using SqliteConnection c = Open();
		List<DictionaryRow> rows = await c.Command(TwoRows).AsDictionaryRows().ToListAsync(Ct);

		// The keys of every row are the one array the index holds.
		Assert.Same(rows[0].Keys, rows[1].Keys);
	}

	[Fact]
	public static void TheKeys_CannotBeCastBackToTheSharedArray_AndAModelIsRefused()
	{
		DictionaryRow row = FirstRow("select 1 as a, 2 as b");

		Assert.IsNotType<string[]>(row.Keys, exactMatch: false);
		Assert.Equal(["a", "b"], row.Keys);

		// DictionaryRow is not a row model: a map of it says so, and names the way to read rows without a model.
		InvalidOperationException ex = Assert.Throws<InvalidOperationException>(static () => RowMap<DictionaryRow>.Default);
		Assert.Contains("AsDictionaryRows()", ex.Message);
	}

	// The call the docs name: Map<DictionaryRow>() throws InvalidOperationException, and the message names AsDictionaryRows().
	[Fact]
	public static void MapOfDictionaryRow_ThroughThePublicCall_ThrowsAndNamesAsDictionaryRows()
	{
		using SqliteConnection c = Open();

		InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
			() => c.Command("select 1 as a").Map<DictionaryRow>().ToList());

		Assert.Contains("AsDictionaryRows()", ex.Message);
	}

	[Fact]
	public static void DictionaryRow_AsAnInterface_Works_OnABoxedCopy()
	{
		DictionaryRow row = FirstRow("select 1 as a, 2 as b");
#pragma warning disable CA1859 // The point of the test is the interface path.
		IReadOnlyDictionary<string, object?> boxed = row;
#pragma warning restore CA1859

		Assert.Equal(2, boxed.Count);
		Assert.Equal(2L, boxed["b"]);
		Assert.Equal(["a", "b"], boxed.Select(static pair => pair.Key));
	}

	static DictionaryRow FirstRow(string sql)
	{
		using SqliteConnection c = Open();
		return c.Command(sql).AsDictionaryRows().ToListAsync(Ct).AsTask().GetAwaiter().GetResult().First();
	}

	// ---- the read: mode, later sets, SingleResult, pooling ----

	[Fact]
	public static async Task TheRowsFormReadsEveryColumnOnce_InOrder_WithSequentialAccess()
	{
		var behaviors = new List<CommandBehavior>();
		static DbDataReader Reader() => FakeReader.CreateDb(["a", "b"], [1, "x"]);

		var first = new FakeDbCommand(new FakeDbConnection(), Reader);
		await first.AsObjectArrays().ToListAsync(Ct);
		behaviors.AddRange(first.Behaviors);
		var second = new FakeDbCommand(new FakeDbConnection(), Reader);
		await second.AsDictionaryRows().SingleResult().ToListAsync(Ct);
		behaviors.AddRange(second.Behaviors);

		// The library adds CloseConnection when it opened the connection itself: not part of the read mode.
		Assert.Equal(
			[CommandBehavior.SequentialAccess, CommandBehavior.SingleResult | CommandBehavior.SequentialAccess],
			behaviors.Select(static b => b & ~CommandBehavior.CloseConnection));
	}

	[Fact]
	public static async Task TheRowsFormReadsThroughALaterSet_AndSingleResultDoesNot()
	{
		await using SqliteConnection c = Open();
		const string later = "select 1 as a; select abs(-9223372036854775808)";

		await Assert.ThrowsAsync<SqliteException>(async () => await c.Command(later).AsObjectArrays().ToListAsync(Ct));
		await Assert.ThrowsAsync<SqliteException>(async () => await c.Command(later).AsDictionaryRows().ToListAsync(Ct));
		Assert.Single(await c.Command(later).SingleResult().AsObjectArrays().ToListAsync(Ct));
		Assert.Single(await c.Command(later).AsObjectArrays().SingleResult().ToListAsync(Ct));
	}

	[Fact]
	public static async Task TheRowsFormReturnsTheFirstSetOnly_WhenLaterSetsSucceed()
	{
		await using SqliteConnection c = Open();

		List<object?[]> rows = await c.Command("select 1 as a; select 2 as a, 3 as b").AsObjectArrays().ToListAsync(Ct);

		Assert.Equal([1L], Assert.Single(rows));
	}

	[Fact]
	public static async Task TheArraysThatHoldTheRows_AreClearedWhenTheyGoBack_TheRowsAreTheCallers()
	{
		await using SqliteConnection c = Open();
		var pool = new CountingPool<object?[]>
		{
			RoundUp = true,
			IsDirty = static a => a.Any(static r => r is not null),
		};

		List<object?[]> rows = await c.Command(TwoRows).AsObjectArrays().UsePool(pool, clearOnReturn: false).ToListAsync(Ct);

		Assert.Equal(2, rows.Count);
		Assert.Equal([1L, "a", null], rows[0]);
		Assert.True(pool.Rents > 0);
		Assert.Equal(pool.Rents, pool.Returns);
		Assert.Equal(0, pool.DirtyReturns);
	}

	[Fact]
	public static async Task SkipAndTake_AndTheBufferTerminals_Work()
	{
		await using SqliteConnection c = Open();

		List<object?[]> second = await c.Command(TwoRows).AsObjectArrays().Skip(1).Take(1).ToListAsync(Ct);
		await using PooledBuffer<DictionaryRow> buffer = c.Command(TwoRows).AsDictionaryRows().ToPooledBuffer(Ct);
		PooledArray<DictionaryRow> items = await buffer.Items;

		Assert.Equal([2L, "b", null], Assert.Single(second));
		Assert.Equal(2, items.Count);
		Assert.Equal("b", items[1]["Name"]);
	}

	// ---- every receiver ----

	[Fact]
	public static async Task EveryReceiver_GivesTheSameRows()
	{
		await using SqliteConnection c = Open();
		const string sql = "select 7 as a, null as b";

		await using SqliteCommand command = c.CreateCommand();
		command.CommandText = sql;
		using var table = new DataTable();
		await using (SqliteDataReader loader = command.ExecuteReader()) table.Load(loader);

		List<object?[]> fromExecute = await c.Command(sql).AsObjectArrays().ToListAsync(Ct);
		List<object?[]> fromCommand = await command.AsObjectArrays().ToListAsync(Ct);
		List<object?[]> fromTable = await table.AsObjectArrays().ToListAsync(Ct);
		List<object?[]> fromDefinition = await DbQuery.Text(sql).AsObjectArrays().Using(c).ToListAsync(Ct);
		List<object?[]> fromReader;
		await using (SqliteDataReader reader = command.ExecuteReader())
			fromReader = await reader.AsObjectArrays().ToListAsync(Ct);

		foreach (List<object?[]> rows in new[] { fromExecute, fromCommand, fromDefinition, fromReader })
			Assert.Equal([7L, null], Assert.Single(rows));
		Assert.Equal(7L, Assert.Single(fromTable)[0]);
		Assert.Null(Assert.Single(fromTable)[1]);

		DictionaryRow named = Assert.Single(await DbQuery.Text(sql).AsDictionaryRows().Using(c).ToListAsync(Ct));
		Assert.Equal(7L, named["a"]);
		Assert.Single(await command.AsDictionaryRows().ToListAsync(Ct));
		Assert.Single(await table.AsDictionaryRows().ToListAsync(Ct));
		await using (SqliteDataReader reader = command.ExecuteReader())
			Assert.Single(await reader.AsDictionaryRows().ToListAsync(Ct));
	}

	readonly record struct Args(int P);

	[Fact]
	public static async Task TheTypedDefinition_TakesTheRowsFormToo()
	{
		await using SqliteConnection c = Open();
		MappedDbQuery<Args, DictionaryRow> definition = DbQuery.Text("select @P as P").Params<Args>().AsDictionaryRows();

		DictionaryRow row = Assert.Single(await definition.Using(c).AddParams(new(5)).ToListAsync(Ct));

		Assert.Equal(5L, row["P"]);
		Assert.Equal([5L], Assert.Single(await DbQuery.Text("select @P as P").Params<Args>().AsObjectArrays().Using(c).AddParams(new(6)).ToListAsync(Ct)).Select(static v => v is long l ? l - 1 : -1));
	}

	[Fact]
	public static void ANullReceiver_IsRefused()
	{
		Assert.Equal("command", Assert.Throws<ArgumentNullException>(() => ((IExecuteReader)null!).AsObjectArrays()).ParamName);
		Assert.Equal("command", Assert.Throws<ArgumentNullException>(() => ((IDbCommand)null!).AsDictionaryRows()).ParamName);
		Assert.Equal("reader", Assert.Throws<ArgumentNullException>(() => ((IDataReader)null!).AsObjectArrays()).ParamName);
		Assert.Equal("table", Assert.Throws<ArgumentNullException>(() => ((DataTable)null!).AsDictionaryRows()).ParamName);
	}

	// ---- nothing about a column list is kept ----

	[MethodImpl(MethodImplOptions.NoInlining)]
	static WeakReference ReadWithUniqueColumns(int n)
	{
		string[] columns = [.. Enumerable.Range(0, 3).Select(i => $"col_{n}_{i}")];
		DbDataReader reader = FakeReader.CreateDb(columns, [1, 2, 3]);
		Func<DbDataReader, DictionaryRow> map = DictionaryRowSource.Instance.Mapper(reader);
		reader.Read();
		DictionaryRow row = map(reader);
		Assert.Equal(2, row[columns[1]]);

		// What a long-lived cache would hold on to: the mapper (it owns the names) and the row (it shares them).
		return new WeakReference(new object[] { map, row });
	}

	[Fact]
	public static void ManyDistinctColumnLists_LeaveNothingBehind()
	{
		var references = new List<WeakReference>();
		for (int i = 0; i < 300; i++) references.Add(ReadWithUniqueColumns(i));

		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();

		Assert.All(references, static r => Assert.False(r.IsAlive, "A column list is still reachable after its result set."));
	}

	[Fact]
	public static void TheSources_HoldNoCacheOfTheirOwn()
	{
		foreach (Type type in new[] { typeof(DictionaryRowSource), typeof(ObjectArraySource), typeof(ColumnIndex) })
		{
			// The only static a source or an index has is the shared instance or a delegate: never a collection that could grow.
			foreach (FieldInfo field in type.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
			{
				Assert.False(
					typeof(System.Collections.IEnumerable).IsAssignableFrom(field.FieldType) && field.FieldType != typeof(string),
					$"{type.Name}.{field.Name} is a collection.");
			}
		}
	}
}
