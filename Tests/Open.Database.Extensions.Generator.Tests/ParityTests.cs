using Microsoft.Data.Sqlite;
using Open.Database.Extensions.Generator.Models;
using System.Data;

namespace Open.Database.Extensions.Generator.Tests;

/// <summary>
/// Each test reads the same data twice: <c>q.Map(RowMap&lt;T&gt;.Default)</c> (the runtime map: an argument, never intercepted)
/// and <c>q.Map&lt;T&gt;()</c> (intercepted), then asks for the same rows, or the same exception, and for what the generated code did.
/// </summary>
public partial class ParityTests
{
	static readonly DateTime Day = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
	static readonly Guid KeyA = new("6f1c2b0e-3a4d-4e5f-8a9b-0c1d2e3f4a5b");
	static readonly Guid KeyB = new("0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d");

	// ---- helpers ----

	static Exception? Catch(Action action)
	{
		try
		{
			action();
			return null;
		}
		catch (Exception ex)
		{
			return ex;
		}
	}

	/// <summary>Both produced the same rows, or both threw the same exception.</summary>
	static void SameOutcome<T>(Func<List<T>> runtime, Func<List<T>> generated)
	{
		List<T>? expected = null, actual = null;
		Exception? r = Catch(() => expected = runtime());
		Exception? g = Catch(() => actual = generated());
		if (r is null && g is null) Parity.SameRows(expected!, actual!);
		else Parity.SameError(r, g);
	}

	/// <summary>Both threw (the runtime too), the same exception.</summary>
	static void SameThrow<T>(Func<List<T>> runtime, Func<List<T>> generated)
		=> Parity.SameError(Catch(() => runtime()), Catch(() => generated()));

	/// <summary>Intercepted and served by generated code.</summary>
	static void Served(Counts c, int sets = 1)
		=> Assert.True(
			c.Intercepted == sets && c.Served == sets && c.Declined == 0,
			$"expected {sets} intercepted and served: {c}");

	static void Declined(Counts c)
		=> Assert.True(c.Intercepted == 1 && c.Served == 0 && c.Declined == 1 && c.RowsServed == 0, $"expected 1 intercepted and declined: {c}");

	// ---- the undecorated twin: read by name ----

	static readonly Col[] PostColumns =
	[
		Col.Of<int>("Id"),
		Col.Of<DateTime>("Created"),
		Col.Of<string>("Title"),
		Col.Of<decimal>("Price"),
		Col.Of<int>("Qty"),
		Col.Of<bool>("IsActive"),
		Col.Of<Guid>("Key"),
		Col.Of<double>("Score"),
		Col.Of<string>("Note"),
		Col.Of<byte[]>("Blob"),
		Col.Of<long>("Big"),
		Col.Of<int>("State"),
		Col.Of<string>("Label"),
	];

	static readonly object?[][] PostRows =
	[
		[1, Day, "first", 1.25m, 3, true, KeyA, 0.5, "a note", new byte[] { 1, 2, 3 }, 10L, 2, "one"],
		// NULL in every nullable member (Qty, Note, Blob and the [AllowNull] Label): null. A non-nullable member is never NULL here: the generated
		// reader reads it as a hand-written selector does, and a NULL there throws (see ParityTests.Null.cs).
		[2, Day, "second", 0m, null, false, KeyA, 0.0, null, null, 0L, 1, null],
		[3, Day.AddDays(1), "third", -7m, null, false, KeyB, -1.5, null, Array.Empty<byte>(), long.MaxValue, 0, null],
	];

	[Fact]
	public void Plain_full_layout_is_served()
	{
		var q = new TableQuery(PostColumns, PostRows);
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		// The columns are the members in their declared order: the identity layout, read at constant ordinals (mode 5).
		Assert.Equal(1, c.Identity);
		Assert.Equal(PostRows.Length, c.RowsServed);
		Assert.Null(generated[1].Label);
	}

	[Fact]
	public void Plain_columns_by_case_and_underscores_in_any_order()
	{
		var q = new TableQuery(
			[
				Col.Of<string>("NOTE"),
				Col.Of<bool>("is_active"),
				Col.Of<int>("id"),
				Col.Of<string>("TITLE"),
				Col.Of<int>("_State_"),
				Col.Of<decimal>("price"),
			],
			["n", true, 5, "t", 1, 2.5m],
			[null, false, 6, "u", 2, 0m]);
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	[Fact]
	public void Plain_extra_and_missing_columns_are_served_guarded()
	{
		var q = new TableQuery(
			[Col.Of<int>("Id"), Col.Of<string>("Unused"), Col.Of<string>("Title"), Col.Of<int>("Other")],
			[1, "x", "one", 9],
			[2, null, "two", null]);
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Guarded);
	}

	[Fact]
	public void Plain_converting_reads_match_the_runtime()
	{
		var q = new TableQuery(
			[
				Col.Of<long>("Id"), // long → int
				Col.Of<string>("Created"), // string → DateTime (invariant)
				Col.Of<string>("Price"), // string → decimal (invariant)
				Col.Of<short>("Qty"), // short → int?
				Col.Of<int>("IsActive"), // integral → bool
				Col.Of<int>("Score"), // integral → double
				Col.Of<int>("Big"), // int → long
				Col.Of<long>("State"), // long → enum
			],
			[7L, "2026-03-04T05:06:07", "12.50", (short)4, 1, 3, 11, 2L],
			[8L, "2026-03-05", "-0.01", null, 0, -2, int.MinValue, 0L]);

		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	[Fact]
	public void Plain_overflow_reports_the_row_with_the_runtime_inner_exception()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<long>("Big"), Col.Of<long>("Qty")], [1, 2L, 5_000_000_000L]);

		var start = Counts.Now();
		Exception? generated = Catch(() => q.Map<PlainPost>().ToList());
		Counts c = Counts.Now().Since(start);

		// Both name the model and the row, the conversion's exception inside.
		RowError<OverflowException>(generated, typeof(PlainPost), 0);
		Parity.SameError(Catch(() => q.Map(RowMap<PlainPost>.Default).ToList()), generated);
		Served(c);
	}

	[Fact]
	public void Plain_unsupported_conversion_declines_to_the_runtime()
	{
		// No typed getter reads a string into an int: the runtime's general path decides, so the generated code hands the result set to it.
		var q = new TableQuery([Col.Of<string>("Id"), Col.Of<string>("Title")], ["12", "a"], ["x", "b"]);

		var start = Counts.Now();
		SameOutcome(
			() => q.Map(RowMap<PlainPost>.Default).ToList(),
			() => q.Map<PlainPost>().ToList());
		Counts c = Counts.Now().Since(start);

		Declined(c);
	}

	// ---- the decorated twin: declared names and ordinals are hard rules ----

	static readonly Col[] DecoratedColumns =
	[
		Col.Of<int>("post_id"),
		Col.Of<DateTime>("created_on"),
		Col.Of<string>("title"),
		Col.Of<decimal>("price"),
		Col.Of<int>("qty"),
		Col.Of<bool>("is_active"),
		Col.Of<Guid>("post_key"),
		Col.Of<double>("score"),
		Col.Of<string>("note"),
		Col.Of<byte[]>("blob"),
		Col.Of<long>("big"),
		Col.Of<int>("state"),
		Col.Of<string>("label"),
	];

	[Fact]
	public void Decorated_declared_layout_is_served()
	{
		var q = new TableQuery(DecoratedColumns, PostRows);
		var runtime = q.Map(RowMap<DecoratedPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<DecoratedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Literal);
		Assert.All(generated, p => Assert.Equal("kept", p.Display));
	}

	[Fact]
	public void Decorated_names_ignore_case_and_a_not_mapped_column_is_ignored()
	{
		Col[] columns = [.. DecoratedColumns.Select(x => x with { Name = x.Name.ToUpperInvariant() }), Col.Of<string>("Display")];
		object?[][] rows = [.. PostRows.Select(r => (object?[])[.. r, "from the result"])];
		var q = new TableQuery(columns, rows);
		var runtime = q.Map(RowMap<DecoratedPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<DecoratedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.All(generated, p => Assert.Equal("kept", p.Display));
	}

	[Fact]
	public void Decorated_column_out_of_its_order_throws_the_runtime_exception()
	{
		Col[] columns = [.. DecoratedColumns];
		(columns[0], columns[2]) = (columns[2], columns[0]);
		object?[][] rows = [.. PostRows.Select(r => { object?[] x = [.. r]; (x[0], x[2]) = (x[2], x[0]); return x; })];
		var q = new TableQuery(columns, rows);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<DecoratedPost>.Default).ToList(),
			() => q.Map<DecoratedPost>().ToList());
		Counts c = Counts.Now().Since(start);

		Declined(c);
	}

	[Fact]
	public void Decorated_missing_columns_throw_the_runtime_exception()
	{
		var q = new TableQuery([.. DecoratedColumns.Take(5)], [1, Day, "t", 1m, 2]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<DecoratedPost>.Default).ToList(),
			() => q.Map<DecoratedPost>().ToList());
		Counts c = Counts.Now().Since(start);

		Declined(c);
	}

	[Fact]
	public void Decorated_underscore_spelling_is_not_a_declared_name()
	{
		// "postid" is not "post_id": a declared name is matched exactly or ignoring case, never ignoring underscores.
		Col[] columns = [.. DecoratedColumns];
		columns[0] = Col.Of<int>("postid");
		var q = new TableQuery(columns, PostRows[0]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<DecoratedPost>.Default).ToList(),
			() => q.Map<DecoratedPost>().ToList());
		Declined(Counts.Now().Since(start));
	}

	[Fact]
	public void Named_columns_without_ordinals_are_served()
	{
		var q = new TableQuery(
			[Col.Of<string>("NOTE"), Col.Of<string>("Title"), Col.Of<int>("POST_ID"), Col.Of<int>("Id")],
			["n", "t", 4, 99],
			[null, "u", 5, 98]);
		var runtime = q.Map(RowMap<NamedPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<NamedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	[Fact]
	public void Named_column_missing_throws_the_runtime_exception()
	{
		var q = new TableQuery([Col.Of<int>("post_id"), Col.Of<string>("Note")], [1, "n"]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<NamedPost>.Default).ToList(),
			() => q.Map<NamedPost>().ToList());
		Declined(Counts.Now().Since(start));
	}

	// ---- constructors ----

	[Fact]
	public void Record_full_layout_is_served()
	{
		var q = new TableQuery(
			[Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<int>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")],
			[1, "one", 2, 1, "t", "n"],
			[2, "two", null, 2, "b", null]);
		var runtime = q.Map(RowMap<PlainRecord>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainRecord>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	[Fact]
	public void Record_absent_parameters_take_their_defaults()
	{
		var q = new TableQuery([Col.Of<string>("title"), Col.Of<int>("ID")], ["one", 1], ["two", 2]);
		var runtime = q.Map(RowMap<PlainRecord>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainRecord>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(PostState.Archived, generated[0].State);
		Assert.Equal("none", generated[0].Tag);
	}

	[Fact]
	public void Decorated_record_is_served()
	{
		var q = new TableQuery(
			[Col.Of<int>("post_id"), Col.Of<string>("title"), Col.Of<int>("qty"), Col.Of<int>("state"), Col.Of<string>("Note")],
			[1, "one", 2, 1, "from the result"],
			[2, "two", null, 0, null]);
		var runtime = q.Map(RowMap<DecoratedRecord>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<DecoratedRecord>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.All(generated, r => Assert.Equal("kept", r.Note));
	}

	// ---- init-only and required members ----

	[Fact]
	public void Init_full_layout_is_served()
	{
		var q = new TableQuery(
			[Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note"), Col.Of<decimal>("Price")],
			[1, "one", "n", 2m],
			[2, "two", null, 0m]);
		var runtime = q.Map(RowMap<PlainInit>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainInit>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	[Fact]
	public void Init_absent_member_declines_the_layout()
	{
		// Note (init-only) has no column: the runtime leaves it as the type made it, which an object initializer cannot express, so the result set goes to the runtime.
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [2, "two"]);

		var start = Counts.Now();
		SameOutcome(
			() => q.Map(RowMap<PlainInit>.Default).ToList(),
			() => q.Map<PlainInit>().ToList());
		Counts c = Counts.Now().Since(start);

		Declined(c);
	}

	[Fact]
	public void Init_required_column_missing_throws_the_runtime_exception()
	{
		var q = new TableQuery([Col.Of<string>("Title")], ["one"]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<PlainInit>.Default).ToList(),
			() => q.Map<PlainInit>().ToList());
		Declined(Counts.Now().Since(start));
	}

	[Fact]
	public void Decorated_init_is_served()
	{
		var q = new TableQuery(
			[Col.Of<int>("post_id"), Col.Of<string>("title"), Col.Of<string>("note"), Col.Of<decimal>("price")],
			[1, "one", "n", 2m],
			[2, "two", null, 0m]);
		var runtime = q.Map(RowMap<DecoratedInit>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<DecoratedInit>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	// ---- other shapes ----

	[Fact]
	public void Struct_and_derived_models_are_served()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note")], [1, "one", "n"], [2, null, null]);
		var runtimeStruct = q.Map(RowMap<PlainStruct>.Default).ToList();
		var runtimeDerived = q.Map(RowMap<DerivedPost>.Default).ToList();

		var start = Counts.Now();
		var generatedStruct = q.Map<PlainStruct>().ToList();
		var generatedDerived = q.Map<DerivedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtimeStruct, generatedStruct);
		Parity.SameRows(runtimeDerived, generatedDerived);
		Served(c, 2);
	}

	// ---- receivers, terminals, re-execution ----

	[Fact]
	public void Data_reader_receiver_is_served()
	{
		var q = new TableQuery(PostColumns, PostRows);
		List<PlainPost> runtime;
		using (System.Data.Common.DbDataReader reader = q.OpenReader()) runtime = reader.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		List<PlainPost> generated;
		using (System.Data.Common.DbDataReader reader = q.OpenReader()) generated = reader.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	[Fact]
	public void Command_receiver_is_served_on_a_provider()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		using (SqliteCommand setup = connection.CreateCommand())
		{
			setup.CommandText = """
				CREATE TABLE post (Id INTEGER, Title TEXT, Qty INTEGER, State INTEGER, Tag TEXT, Note TEXT);
				INSERT INTO post VALUES (1, 'one', 2, 1, 'a', 'n');
				INSERT INTO post VALUES (2, 'two', NULL, 2, 'b', NULL);
				""";
			setup.ExecuteNonQuery();
		}

		using SqliteCommand command = connection.CreateCommand();
		command.CommandText = "SELECT Id, Title, Qty, State, Tag, Note FROM post ORDER BY Id";
		IDbCommand dbCommand = command;
		var runtime = dbCommand.Map(RowMap<PlainRecord>.Default).ToList();

		var start = Counts.Now();
		var generated = dbCommand.Map<PlainRecord>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Assert.Equal(1, c.Intercepted);
		Served(c);
	}

	[Fact]
	public async Task Async_terminal_is_served()
	{
		var q = new TableQuery(PostColumns, PostRows);
		List<PlainPost> runtime = await q.Map(RowMap<PlainPost>.Default).ToListAsync(TestContext.Current.CancellationToken);

		var start = Counts.Now();
		List<PlainPost> generated = await q.Map<PlainPost>().ToListAsync(TestContext.Current.CancellationToken);
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	[Fact]
	public void Skip_and_take_read_the_same_rows()
	{
		var q = new TableQuery(PostColumns, PostRows);
		var runtime = q.Map(RowMap<PlainPost>.Default).Skip(1).Take(1).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().Skip(1).Take(1).ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.RowsServed);
	}

	[Fact]
	public void One_call_site_follows_each_layout_it_meets()
	{
		var byName = new TableQuery(PostColumns, PostRows);
		var reordered = new TableQuery(
			[Col.Of<string>("Title"), Col.Of<long>("Id"), Col.Of<string>("Unused")],
			["one", 1L, "x"],
			["two", 2L, null]);
		TableQuery[] queries = [byName, reordered, byName, reordered];

		var start = Counts.Now();
		foreach (TableQuery q in queries)
		{
			var runtime = q.Map(RowMap<PlainPost>.Default).ToList();
			var generated = q.Map<PlainPost>().ToList();
			Parity.SameRows(runtime, generated);
		}

		Counts c = Counts.Now().Since(start);
		Served(c, queries.Length);
	}

	[Fact]
	public void A_deferred_query_executed_twice_is_served_twice()
	{
		var q = new TableQuery(PostColumns, PostRows);
		MappedQuery<PlainPost> query = q.Map<PlainPost>();
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var first = query.ToList();
		var second = query.ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, first);
		Parity.SameRows(runtime, second);
		Assert.Equal(0, c.Intercepted); // The interceptor ran when the query was defined, before start.
		Assert.Equal(2, c.Served);
	}

	static List<T> Generic<T>(TableQuery q) => q.Map<T>().ToList();

	[Fact]
	public void A_generic_call_site_is_not_intercepted()
	{
		var q = new TableQuery(PostColumns, PostRows);
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		List<PlainPost> generic = Generic<PlainPost>(q);
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generic);
		Assert.Equal(default, c);
	}

	[Fact]
	public void An_invalid_declaration_is_left_to_the_runtime()
	{
		var q = new TableQuery([Col.Of<int>("Id")], [1]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<InvalidOrderPost>.Default).ToList(),
			() => q.Map<InvalidOrderPost>().ToList());
		Counts c = Counts.Now().Since(start);

		Assert.Equal(default, c);
	}
}
