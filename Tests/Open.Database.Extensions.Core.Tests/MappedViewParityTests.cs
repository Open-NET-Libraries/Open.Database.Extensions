using System.Collections;
using System.Data.Common;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// reader.Map<T>() and command.Map<T>() give the same results as
// Map<T>() on an expressive command, terminal by terminal,
// in their synchronous and asynchronous forms.
[ExcludeFromCodeCoverage]
public static class MappedViewParityTests
{
	internal sealed record Person
	{
		public required int Id { get; init; }
		public string? Name { get; init; }
		public string? Note { get; init; }
	}

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly string[] Terminals =
	[
		"ToList", "ToArray", "ToImmutableArray", "FirstOrDefault",
		"SingleOrDefault", "ToDictionary", "ToHashSet", "Read", "ReadResult",
		"Skip", "Take", "SkipTake",
	];

	static readonly string[] Sets = ["three", "nulls", "one", "empty"];

	public static TheoryData<string, string> Cases
	{
		get
		{
			TheoryData<string, string> data = [];
			foreach (string set in Sets)
			{
				foreach (string terminal in Terminals)
				{
					data.Add(set, terminal);
				}
			}

			return data;
		}
	}

	static object?[][] Data(string set)
		=> set switch
		{
			"three" => Rows(3),
			"nulls" => TwoRows,
			"one" => Rows(1),
			"empty" => [],
			_ => throw new ArgumentOutOfRangeException(nameof(set)),
		};

	static MappedQuery<Person> Baseline(object?[][] rows)
		=> new FakeExecuteReader(() => Reader(rows)).Map<Person>();

	static MappedReader<Person> ReaderView(object?[][] rows)
		=> Reader(rows).Map<Person>();

	static MappedCommand<Person> CommandView(object?[][] rows)
		=> new FakeDbCommand(new FakeDbConnection(), () => Reader(rows))
			.Map<Person>();

	static string Capture(Func<object?> run)
	{
		try
		{
			return Show(run());
		}
		catch (Exception ex)
		{
			return ex.GetType().Name;
		}
	}

	static string Show(object? value)
		=> value switch
		{
			null => "null",
			Person p => $"{p.Id}/{p.Name ?? "~"}/{p.Note ?? "~"}",
			IDictionary d => string.Join(
				",",
				d.Keys.Cast<object>().Select(k => $"{k}:{Show(d[k])}")),
			IEnumerable e => "[" + string.Join(
				",",
				e.Cast<object?>().Select(Show)) + "]",
			_ => value.ToString() ?? "",
		};

	[Theory, MemberData(nameof(Cases))]
	public static void EveryTerminal_GivesTheSameResultAsAMappedQuery(
		string set,
		string terminal)
	{
		object?[][] rows = Data(set);
		string expected = Run(Baseline(rows), terminal);

		Assert.Equal(expected, Run(ReaderView(rows), terminal));
		Assert.Equal(expected, Run(CommandView(rows), terminal));
	}

	[Theory, MemberData(nameof(Cases))]
	public static async Task EveryAsyncTerminal_GivesTheSameResultAsAMappedQuery(
		string set,
		string terminal)
	{
		object?[][] rows = Data(set);
		string expected = await RunAsync(Baseline(rows), terminal, Ct);

		Assert.Equal(expected, await RunAsync(ReaderView(rows), terminal, Ct));
		Assert.Equal(expected, await RunAsync(CommandView(rows), terminal, Ct));
	}

	[Fact]
	public static void TheBaselineIsNotEmpty()
	{
		Assert.Equal("[1/n1/~,2/n2/~,3/n3/~]", Run(Baseline(Rows(3)), "ToList"));
		Assert.Equal("[1/a/n1,2/~/n2]", Run(ReaderView(TwoRows), "ToList"));
		Assert.Equal("null", Run(CommandView([]), "FirstOrDefault"));
		Assert.Equal("[2/n2/~]", Run(ReaderView(Rows(3)), "SkipTake"));
	}

	// A single value reads the first column, as Map<T>() on a command does.
	[Fact]
	public static void ASingleValue_ReadsColumnZero()
	{
		ImmutableArray<ColumnDef> columns = [("V", typeof(string))];
		object?[][] rows = [["a"], [null], ["c"]];
		static TypedReader Rd(ImmutableArray<ColumnDef> c, object?[][] r)
			=> TypedReader.Create(c, r);
		string?[] expected = ["a", null, "c"];

		Assert.Equal(expected, Rd(columns, rows).Map<string>().ToList());
		Assert.Equal(
			expected,
			new FakeDbCommand(new FakeDbConnection(), () => Rd(columns, rows))
				.Map<string>()
				.ToList());
		Assert.Equal(
			expected,
			new FakeExecuteReader(() => Rd(columns, rows))
				.Map<string>()
				.ToList());

		ImmutableArray<ColumnDef> ints = [("N", typeof(int))];
		Assert.Equal(
			[1, 2],
			Rd(ints, [[1], [2]]).Map<int>().ToArray());
		Assert.Equal(
			2,
			Rd(ints, [[1], [2]]).Map<int>().Skip(1).FirstOrDefault());
	}

	[Fact]
	public static async Task ASingleValue_ReadsColumnZero_Async()
	{
		ImmutableArray<ColumnDef> columns = [("V", typeof(string))];
		object?[][] rows = [["a"], [null]];

		string?[] expected = ["a", null];
		Assert.Equal(
			expected,
			await TypedReader.Create(columns, rows).Map<string>().ToListAsync(Ct));
		Assert.Equal(
			expected,
			await new FakeDbCommand(
				new FakeDbConnection(),
				() => TypedReader.Create(columns, rows))
				.Map<string>()
				.ToListAsync(Ct));
	}

	// The selector and the RowMap overloads, on both views.
	[Fact]
	public static async Task TheOtherOverloads_MapOnBothViews()
	{
		static TypedReader Rd() => Reader(Rows(2));
		static FakeDbCommand Cmd()
			=> new(new FakeDbConnection(), () => Reader(Rows(2)));
		RowMap<Person> map = new(
			new KeyValuePair<string, string?>(nameof(Person.Note), "Name"),
			new KeyValuePair<string, string?>(nameof(Person.Name), null));

		Assert.Equal([1, 2], Rd().Map(r => r.GetInt32(0)).ToList());
		Assert.Equal([1, 2], Cmd().Map(r => r.GetInt32(0)).ToList());
		Assert.Equal([1, 2], await Cmd().Map(r => r.GetInt32(0)).ToListAsync(Ct));

		Assert.Equal("n1", Rd().Map(map).FirstOrDefault()!.Note);
		Assert.Equal("n1", Cmd().Map(map).FirstOrDefault()!.Note);
		Assert.Equal(
			"n2",
			Rd().Map<Person>(new KeyValuePair<string, string?>("Note", null))
				.Skip(1)
				.FirstOrDefault()!.Name);
		Assert.Equal(
			"n1",
			Cmd().Map<Person>(
				new KeyValuePair<string, string?>("Note", "Name"),
				new KeyValuePair<string, string?>("Name", "Note"))
				.FirstOrDefault()!.Note);

		// A null map reads like the default one.
		Assert.Equal("n1", Rd().Map((RowMap<Person>?)null).FirstOrDefault()!.Name);
		Assert.Equal("n1", Cmd().Map((RowMap<Person>?)null).FirstOrDefault()!.Name);
		Assert.Equal(
			"n1",
			new FakeExecuteReader(() => Reader(Rows(1)))
				.Map((RowMap<Person>?)null)
				.FirstOrDefault()!.Name);
	}

	[Fact]
	public static void ANullReceiverOrSelector_Throws()
	{
		IDataReader? reader = null;
		IDbCommand? command = null;
		RowMap<Person> map = RowMap<Person>.Default;

		Assert.Throws<ArgumentNullException>(() => reader!.Map<Person>());
		Assert.Throws<ArgumentNullException>(() => reader!.Map(map));
		Assert.Throws<ArgumentNullException>(
			() => reader!.Map<Person>(new KeyValuePair<string, string?>("Id", "Id")));
		Assert.Throws<ArgumentNullException>(() => reader!.Map(r => 1));
		Assert.Throws<ArgumentNullException>(() => command!.Map<Person>());
		Assert.Throws<ArgumentNullException>(() => command!.Map(map));
		Assert.Throws<ArgumentNullException>(() => command!.Map(r => 1));
		Assert.Throws<ArgumentNullException>(
			() => Reader(Rows(1)).Map((Func<IDataRecord, int>)null!));
		Assert.Throws<ArgumentNullException>(
			() => new FakeDbCommand(new FakeDbConnection(), () => Reader(Rows(1)))
				.Map((Func<IDataRecord, int>)null!));
	}

	// A default struct of the whole family says it was not created by
	// Map<T>(): InvalidOperationException, never NullReferenceException.
	[Fact]
	public static void ADefaultStruct_ThrowsInvalidOperation_OnEveryMember()
	{
		static void Throws(Action use)
		{
			InvalidOperationException ex
				= Assert.Throws<InvalidOperationException>(use);
			Assert.Contains("Map<T>()", ex.Message);
		}

		// An async terminal reports it synchronously or as a faulted task.
		static void Fails<TResult>(Func<ValueTask<TResult>> use)
		{
			InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
				() => use().AsTask().GetAwaiter().GetResult());
			Assert.Contains("Map<T>()", ex.Message);
		}

		static void FailsPlain(Func<ValueTask> use)
		{
			InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
				() => use().AsTask().GetAwaiter().GetResult());
			Assert.Contains("Map<T>()", ex.Message);
		}

		static ValueTask H1(IAsyncEnumerable<int> items, CancellationToken token)
			=> default;

		static ValueTask<int> H2(IAsyncEnumerable<int> items, CancellationToken token)
			=> new(0);

		static void All<TQuery>(TQuery q, Action<TQuery> more)
			where TQuery : IMappedQuery<int>
		{
			Throws(() => q.Read(_ => { }));
			Throws(() => q.Read(items => items.Count()));
			FailsPlain(() => q.ReadAsync(H1, default));
			Fails(() => q.ReadAsync(H2, default));
			more(q);
		}

		Throws(() => default(MappedQuery<int>).ToList());
		All(default(MappedQuery<int>), q =>
		{
			Throws(() => q.ToList());
			Throws(() => q.ToArray());
			Throws(() => q.FirstOrDefault());
			Throws(() => q.SingleOrDefault());
			Throws(() => q.ToImmutableArray());
			Throws(() => q.ToDictionary(i => i));
			Throws(() => q.ToHashSet());
			Fails(() => q.ToListAsync());
			Fails(() => q.ToArrayAsync());
			Fails(() => q.FirstOrDefaultAsync());
			Fails(() => q.SingleOrDefaultAsync());
			Fails(() => q.ToImmutableArrayAsync());
			Fails(() => q.ToDictionaryAsync(i => i));
			Fails(() => q.ToHashSetAsync());
			Throws(() => q.Skip(1));
			Throws(() => q.Take(1));
			Throws(() => q.SingleResult());
		});
		All(default(MappedQueryWithSkip<int>), q =>
		{
			Throws(() => q.ToList());
			Fails(() => q.ToListAsync());
			Throws(() => q.Take(1));
			Throws(() => q.SingleResult());
		});
		All(default(MappedQueryWithTake<int>), q =>
		{
			Throws(() => q.ToList());
			Fails(() => q.FirstOrDefaultAsync());
			Throws(() => q.SingleResult());
		});
		All(default(MappedReader<int>), q =>
		{
			Throws(() => q.ToList());
			Throws(() => q.ToArray());
			Throws(() => q.FirstOrDefault());
			Throws(() => q.SingleOrDefault());
			Throws(() => q.ToImmutableArray());
			Throws(() => q.ToDictionary(i => i));
			Throws(() => q.ToHashSet());
			Fails(() => q.ToListAsync());
			Fails(() => q.ToArrayAsync());
			Fails(() => q.FirstOrDefaultAsync());
			Fails(() => q.SingleOrDefaultAsync());
			Fails(() => q.ToImmutableArrayAsync());
			Fails(() => q.ToDictionaryAsync(i => i));
			Fails(() => q.ToHashSetAsync());
			Throws(() => q.Skip(1));
			Throws(() => q.Take(1));
		});
		All(default(MappedCommand<int>), q =>
		{
			Throws(() => q.ToList());
			Throws(() => q.ToArray());
			Throws(() => q.FirstOrDefault());
			Throws(() => q.SingleOrDefault());
			Throws(() => q.ToImmutableArray());
			Throws(() => q.ToDictionary(i => i));
			Throws(() => q.ToHashSet());
			Fails(() => q.ToListAsync());
			Fails(() => q.ToArrayAsync());
			Fails(() => q.FirstOrDefaultAsync());
			Fails(() => q.SingleOrDefaultAsync());
			Fails(() => q.ToImmutableArrayAsync());
			Fails(() => q.ToDictionaryAsync(i => i));
			Fails(() => q.ToHashSetAsync());
			Throws(() => q.Skip(1));
			Throws(() => q.Take(1));
			Throws(() => q.SingleResult());
		});

		MappedReader<int> reader = default;
		MappedCommand<int> command = default;
		Assert.Equal(reader, new MappedReader<int>());
		Assert.Equal(command, new MappedCommand<int>());
	}

	// No map, in every spelling, reads exactly as Map<T>() does.
	[Fact]
	public static void NoMap_ReadsAsTheNoArgumentOverloadDoes()
	{
		ImmutableArray<ColumnDef> ints = [("N", typeof(int))];
		static TypedReader Ints(ImmutableArray<ColumnDef> c)
			=> TypedReader.Create(c, [1], [2]);
		static TypedReader People() => Reader(Rows(2));
		static FakeDbCommand Cmd(Func<DbDataReader> reader)
			=> new(new FakeDbConnection(), reader);

		// A single value reads column 0.
		Assert.Equal([1, 2], Ints(ints).Map<int>(null).ToList());
		Assert.Equal([1, 2], Ints(ints).Map<int>([]).ToList());
		Assert.Equal([1, 2], Ints(ints).Map((RowMap<int>?)null).ToList());
		Assert.Equal([1, 2], Cmd(() => Ints(ints)).Map<int>(null).ToList());
		Assert.Equal([1, 2], Cmd(() => Ints(ints)).Map<int>([]).ToList());
		Assert.Equal(
			[1, 2],
			Cmd(() => Ints(ints)).Map((RowMap<int>?)null).ToList());
		Assert.Equal(
			[1, 2],
			new FakeExecuteReader(() => Ints(ints)).Map<int>(null).ToList());
		Assert.Equal(
			[1, 2],
			new FakeExecuteReader(() => Ints(ints)).Map<int>([]).ToList());
		Assert.Equal(
			[1, 2],
			new FakeExecuteReader(() => Ints(ints))
				.Map((RowMap<int>?)null)
				.ToList());

		// A row model reads by name.
		string expected = Show(People().Map<Person>().ToList());
		Assert.Equal(expected, Show(People().Map<Person>(null).ToList()));
		Assert.Equal(expected, Show(People().Map<Person>([]).ToList()));
		Assert.Equal(expected, Show(Cmd(People).Map<Person>(null).ToList()));
		Assert.Equal(expected, Show(Cmd(People).Map<Person>([]).ToList()));
		Assert.Equal(
			expected,
			Show(new FakeExecuteReader(People).Map<Person>(null).ToList()));
		Assert.Equal(
			expected,
			Show(new FakeExecuteReader(People).Map<Person>([]).ToList()));
	}

	// ---- the generated terminals, once per type ----

	static string Run(MappedQuery<Person> q, string terminal)
		=> Capture(() => terminal switch
		{
			"ToList" => q.ToList(),
			"ToArray" => q.ToArray(),
			"ToImmutableArray" => q.ToImmutableArray(),
			"FirstOrDefault" => q.FirstOrDefault(),
			"SingleOrDefault" => q.SingleOrDefault(),
			"ToDictionary" => q.ToDictionary(p => p.Id),
			"ToHashSet" => q.ToHashSet(),
			"Read" => ReadAll(q),
			"ReadResult" => q.Read(items => items.ToList()),
			"Skip" => q.Skip(1).ToList(),
			"Take" => q.Take(1).ToList(),
			"SkipTake" => q.Skip(1).Take(1).ToList(),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		});

	static List<Person> ReadAll(MappedQuery<Person> q)
	{
		List<Person> list = [];
		q.Read(items => list.AddRange(items));
		return list;
	}

	static async Task<string> RunAsync(
		MappedQuery<Person> q,
		string terminal,
		CancellationToken ct)
	{
		try
		{
			object? result = terminal switch
			{
				"ToList" => await q.ToListAsync(ct),
				"ToArray" => await q.ToArrayAsync(ct),
				"ToImmutableArray" => await q.ToImmutableArrayAsync(ct),
				"FirstOrDefault" => await q.FirstOrDefaultAsync(ct),
				"SingleOrDefault" => await q.SingleOrDefaultAsync(ct),
				"ToDictionary" => await q.ToDictionaryAsync(p => p.Id, null, ct),
				"ToHashSet" => await q.ToHashSetAsync(null, ct),
				"Read" => await ReadAllAsync(q, ct),
				"ReadResult" => await q.ReadAsync(
					async (items, token) =>
					{
						List<Person> list = [];
						await foreach (Person p in items.WithCancellation(token))
							list.Add(p);
						return list;
					},
					ct),
				"Skip" => await q.Skip(1).ToListAsync(ct),
				"Take" => await q.Take(1).ToListAsync(ct),
				"SkipTake" => await q.Skip(1).Take(1).ToListAsync(ct),
				_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
			};
			return Show(result);
		}
		catch (Exception ex)
		{
			return ex.GetType().Name;
		}
	}

	static async Task<List<Person>> ReadAllAsync(
		MappedQuery<Person> q,
		CancellationToken ct)
	{
		List<Person> list = [];
		await q.ReadAsync(
			async (items, token) =>
			{
				await foreach (Person p in items.WithCancellation(token))
					list.Add(p);
			},
			ct);
		return list;
	}

	static string Run(MappedReader<Person> q, string terminal)
		=> Capture(() => terminal switch
		{
			"ToList" => q.ToList(),
			"ToArray" => q.ToArray(),
			"ToImmutableArray" => q.ToImmutableArray(),
			"FirstOrDefault" => q.FirstOrDefault(),
			"SingleOrDefault" => q.SingleOrDefault(),
			"ToDictionary" => q.ToDictionary(p => p.Id),
			"ToHashSet" => q.ToHashSet(),
			"Read" => ReadAll(q),
			"ReadResult" => q.Read(items => items.ToList()),
			"Skip" => q.Skip(1).ToList(),
			"Take" => q.Take(1).ToList(),
			"SkipTake" => q.Skip(1).Take(1).ToList(),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		});

	static List<Person> ReadAll(MappedReader<Person> q)
	{
		List<Person> list = [];
		q.Read(items => list.AddRange(items));
		return list;
	}

	static async Task<string> RunAsync(
		MappedReader<Person> q,
		string terminal,
		CancellationToken ct)
	{
		try
		{
			object? result = terminal switch
			{
				"ToList" => await q.ToListAsync(ct),
				"ToArray" => await q.ToArrayAsync(ct),
				"ToImmutableArray" => await q.ToImmutableArrayAsync(ct),
				"FirstOrDefault" => await q.FirstOrDefaultAsync(ct),
				"SingleOrDefault" => await q.SingleOrDefaultAsync(ct),
				"ToDictionary" => await q.ToDictionaryAsync(p => p.Id, null, ct),
				"ToHashSet" => await q.ToHashSetAsync(null, ct),
				"Read" => await ReadAllAsync(q, ct),
				"ReadResult" => await q.ReadAsync(
					async (items, token) =>
					{
						List<Person> list = [];
						await foreach (Person p in items.WithCancellation(token))
							list.Add(p);
						return list;
					},
					ct),
				"Skip" => await q.Skip(1).ToListAsync(ct),
				"Take" => await q.Take(1).ToListAsync(ct),
				"SkipTake" => await q.Skip(1).Take(1).ToListAsync(ct),
				_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
			};
			return Show(result);
		}
		catch (Exception ex)
		{
			return ex.GetType().Name;
		}
	}

	static async Task<List<Person>> ReadAllAsync(
		MappedReader<Person> q,
		CancellationToken ct)
	{
		List<Person> list = [];
		await q.ReadAsync(
			async (items, token) =>
			{
				await foreach (Person p in items.WithCancellation(token))
					list.Add(p);
			},
			ct);
		return list;
	}

	static string Run(MappedCommand<Person> q, string terminal)
		=> Capture(() => terminal switch
		{
			"ToList" => q.ToList(),
			"ToArray" => q.ToArray(),
			"ToImmutableArray" => q.ToImmutableArray(),
			"FirstOrDefault" => q.FirstOrDefault(),
			"SingleOrDefault" => q.SingleOrDefault(),
			"ToDictionary" => q.ToDictionary(p => p.Id),
			"ToHashSet" => q.ToHashSet(),
			"Read" => ReadAll(q),
			"ReadResult" => q.Read(items => items.ToList()),
			"Skip" => q.Skip(1).ToList(),
			"Take" => q.Take(1).ToList(),
			"SkipTake" => q.Skip(1).Take(1).ToList(),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		});

	static List<Person> ReadAll(MappedCommand<Person> q)
	{
		List<Person> list = [];
		q.Read(items => list.AddRange(items));
		return list;
	}

	static async Task<string> RunAsync(
		MappedCommand<Person> q,
		string terminal,
		CancellationToken ct)
	{
		try
		{
			object? result = terminal switch
			{
				"ToList" => await q.ToListAsync(ct),
				"ToArray" => await q.ToArrayAsync(ct),
				"ToImmutableArray" => await q.ToImmutableArrayAsync(ct),
				"FirstOrDefault" => await q.FirstOrDefaultAsync(ct),
				"SingleOrDefault" => await q.SingleOrDefaultAsync(ct),
				"ToDictionary" => await q.ToDictionaryAsync(p => p.Id, null, ct),
				"ToHashSet" => await q.ToHashSetAsync(null, ct),
				"Read" => await ReadAllAsync(q, ct),
				"ReadResult" => await q.ReadAsync(
					async (items, token) =>
					{
						List<Person> list = [];
						await foreach (Person p in items.WithCancellation(token))
							list.Add(p);
						return list;
					},
					ct),
				"Skip" => await q.Skip(1).ToListAsync(ct),
				"Take" => await q.Take(1).ToListAsync(ct),
				"SkipTake" => await q.Skip(1).Take(1).ToListAsync(ct),
				_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
			};
			return Show(result);
		}
		catch (Exception ex)
		{
			return ex.GetType().Name;
		}
	}

	static async Task<List<Person>> ReadAllAsync(
		MappedCommand<Person> q,
		CancellationToken ct)
	{
		List<Person> list = [];
		await q.ReadAsync(
			async (items, token) =>
			{
				await foreach (Person p in items.WithCancellation(token))
					list.Add(p);
			},
			ct);
		return list;
	}
}
