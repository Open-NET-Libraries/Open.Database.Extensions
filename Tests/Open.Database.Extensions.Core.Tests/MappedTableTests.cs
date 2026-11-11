using System.Data;

#nullable enable

namespace Open.Database.Extensions.Tests;

// table.Map<T>() reads a DataTable you already have, through a new
// DataTableReader per terminal, and leaves the table as it found it.
[ExcludeFromCodeCoverage]
public static class MappedTableTests
{
	sealed record Person
	{
		public required int Id { get; init; }
		public string? Name { get; init; }
	}

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static DataTable Table(int count)
	{
		DataTable table = new();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		for (int i = 1; i <= count; i++)
		{
			table.Rows.Add(i, "n" + i);
		}

		return table;
	}

	static string Show(IEnumerable<Person?> people)
		=> string.Join(",", people.Select(p => p is null ? "null" : $"{p.Id}/{p.Name}"));

	[Fact]
	public static void EveryTerminal_GivesTheSameRowsAsAReaderView()
	{
		using DataTable table = Table(3);
		MappedTable<Person> view = table.Map<Person>();

		using DataTableReader reader = table.CreateDataReader();
		string expected = Show(reader.Map<Person>().ToList());
		Assert.Equal("1/n1,2/n2,3/n3", expected);

		Assert.Equal(expected, Show(view.ToList()));
		Assert.Equal(expected, Show(view.ToArray()));
		Assert.Equal(expected, Show(view.ToImmutableArray()));
		Assert.Equal(expected, Show(view.ToHashSet()));
		Assert.Equal(expected, Show(view.ToDictionary(p => p.Id).Values));
		Assert.Equal(expected, Show(view.Read(items => items.ToList())));
		Assert.Equal("1/n1", Show([view.FirstOrDefault()]));
		Assert.Equal("2/n2,3/n3", Show(view.Skip(1).ToList()));
		Assert.Equal("1/n1,2/n2", Show(view.Take(2).ToList()));
		Assert.Equal("2/n2", Show(view.Skip(1).Take(1).ToList()));
		Assert.Equal("2/n2", Show(view.Skip(1).SingleResult().Take(1).ToList()));
		Assert.Throws<InvalidOperationException>(() => view.SingleOrDefault());
	}

	[Fact]
	public static async Task EveryAsyncTerminal_GivesTheSameRowsAsTheSyncOnes()
	{
		using DataTable table = Table(3);
		MappedTable<Person> view = table.Map<Person>();
		string expected = Show(view.ToList());

		Assert.Equal(expected, Show(await view.ToListAsync(Ct)));
		Assert.Equal(expected, Show(await view.ToArrayAsync(Ct)));
		Assert.Equal(expected, Show(await view.ToImmutableArrayAsync(Ct)));
		Assert.Equal(expected, Show(await view.ToHashSetAsync(null, Ct)));
		Assert.Equal(
			expected,
			Show((await view.ToDictionaryAsync(p => p.Id, null, Ct)).Values));
		Assert.Equal("1/n1", Show([await view.FirstOrDefaultAsync(Ct)]));
		Assert.Equal("2/n2,3/n3", Show(await view.Skip(1).ToListAsync(Ct)));
		Assert.Equal("1/n1,2/n2", Show(await view.Take(2).ToListAsync(Ct)));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await view.SingleOrDefaultAsync(Ct));

		List<Person> read = [];
		await view.ReadAsync(
			async (items, token) =>
			{
				await foreach (Person p in items.WithCancellation(token))
					read.Add(p);
			},
			Ct);
		Assert.Equal(expected, Show(read));
	}

	// Each terminal reads through its own reader, so the view repeats,
	// and the table is neither cleared nor changed.
	[Fact]
	public static void TheSameView_CanBeReadMoreThanOnce_AndLeavesTheTable()
	{
		using DataTable table = Table(3);
		MappedTable<Person> view = table.Map<Person>();

		Assert.Equal("1/n1,2/n2,3/n3", Show(view.ToList()));
		Assert.Equal("1/n1,2/n2,3/n3", Show(view.ToList()));
		Assert.Equal(3, table.Rows.Count);
		Assert.Equal(2, table.Columns.Count);

		// The view reads the rows the table has when a terminal runs.
		table.Rows.Add(4, "n4");
		Assert.Equal(4, view.ToList().Count);
	}

	// A reader abandoned part way is still disposed with its call.
	[Fact]
	public static void AnAbandonedRead_StillLeavesTheViewUsable()
	{
		using DataTable table = Table(3);
		MappedTable<Person> view = table.Map<Person>();

		Assert.Equal(1, view.Read(items => items.Take(1).Count()));
		Assert.Equal(3, view.ToList().Count);
	}

	[Fact]
	public static void NullMap_ReadsAsTheNoArgumentOverloadDoes()
	{
		using DataTable table = Table(2);
		string expected = Show(table.Map<Person>().ToList());

		Assert.Equal(expected, Show(table.Map((RowMap<Person>?)null).ToList()));
		Assert.Equal(expected, Show(table.Map<Person>([]).ToList()));
		Assert.Equal(expected, Show(table.Map(RowMap<Person>.Default).ToList()));
	}

	[Fact]
	public static void PairsAndSelector_ReadTheTable()
	{
		using DataTable table = Table(2);
		DataColumn name = table.Columns["Name"]!;
		name.ColumnName = "Label";

		Assert.Equal(
			"1/n1,2/n2",
			Show(table.Map<Person>(new KeyValuePair<string, string?>("Name", "Label")).ToList()));
		Assert.Equal(
			"1/n1,2/n2",
			Show(table.Map<Person>(
				new KeyValuePair<string, string?>("Id", "Id"),
				new KeyValuePair<string, string?>("Name", "Label")).ToList()));
		Assert.Equal(
			"10,20",
			string.Join(",", table.Map(r => r.GetInt32(0) * 10).ToList()));
	}

	[Fact]
	public static void ASingleValue_ReadsTheFirstColumn()
	{
		using DataTable table = Table(3);

		Assert.Equal("1,2,3", string.Join(",", table.Map<int>().ToList()));
		Assert.Equal("2,3", string.Join(",", table.Map<int>().Skip(1).ToList()));
	}

	[Fact]
	public static async Task AnEmptyTable_GivesNothing()
	{
		using DataTable table = Table(0);
		MappedTable<Person> view = table.Map<Person>();

		Assert.Empty(view.ToList());
		Assert.Null(view.FirstOrDefault());
		Assert.Null(view.SingleOrDefault());
		Assert.Empty(await view.ToArrayAsync(Ct));
		Assert.Null(await view.FirstOrDefaultAsync(Ct));
	}

	[Fact]
	public static async Task ACancelledToken_GivesACancelledTask_NotAnExceptionAtTheCall()
	{
		using DataTable table = Table(2);
		MappedTable<Person> view = table.Map<Person>();
		using CancellationTokenSource cts = new();
		cts.Cancel();

		Task<List<Person>> task = view.ToListAsync(cts.Token).AsTask();
		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
		Assert.True(task.IsCanceled);
		Assert.Equal(2, table.Rows.Count);
	}

	[Fact]
	public static async Task ADefaultView_ThrowsInvalidOperation()
	{
		MappedTable<int> view = default;

		Assert.Throws<InvalidOperationException>(() => view.ToList());
		Assert.Throws<InvalidOperationException>(() => view.FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => view.Skip(1));
		Assert.Throws<InvalidOperationException>(() => view.Take(1));
		await Assert.ThrowsAsync<InvalidOperationException>(
			async () => await view.ToListAsync(Ct));
		Assert.Equal(view, new MappedTable<int>());
	}

	[Fact]
	public static void ANullTable_ThrowsArgumentNull()
	{
		DataTable? table = null;

		Assert.Throws<ArgumentNullException>(() => table!.Map<Person>());
		Assert.Throws<ArgumentNullException>(() => table!.Map((RowMap<Person>?)null));
		Assert.Throws<ArgumentNullException>(() => table!.Map<Person>([]));
		Assert.Throws<ArgumentNullException>(
			() => table!.Map<Person>(new KeyValuePair<string, string?>("Id", "Id")));
		Assert.Throws<ArgumentNullException>(() => table!.Map(r => r.GetInt32(0)));
		Assert.Throws<ArgumentNullException>(
			() => new DataTable().Map((Func<IDataRecord, int>)null!));
	}
}
