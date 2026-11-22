using Open.Database.Extensions.Generator.Models;
using System.Data;
using System.Data.Common;

namespace Open.Database.Extensions.Generator.Tests;

// The read modes, SequentialAccess readers, several result sets on one reader, keyword names, the shapes left to the runtime,
// and the benchmark twins against the runtime map.
public partial class ParityTests
{
	// ---- read modes ----

	[Fact]
	public void Full_layout_with_a_converting_read_is_served_without_presence_tests()
	{
		// Every member fed in ascending order; Id and Qty come as long: the converting read is chosen once per layout (mode 4), not guarded.
		var q = new TableQuery(
			[Col.Of<long>("Id"), Col.Of<string>("Title"), Col.Of<long>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")],
			[1L, "one", 2L, 1, "t", "n"],
			[2L, null, null, null, null, null]);
		var runtime = q.Map(RowMap<PlainRecord>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainRecord>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Converting);
	}

	[Fact]
	public void Order_without_a_name_reads_at_constant_ordinals()
	{
		var q = new TableQuery([Col.Of<int>("ID"), Col.Of<string>("title")], [1, "one"], [2, null]);
		var runtime = q.Map(RowMap<OrderOnlyPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<OrderOnlyPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Literal);
	}

	[Fact]
	public void Declaration_order_other_than_Order_still_reads_at_constant_ordinals()
	{
		var q = new TableQuery([Col.Of<int>("post_id"), Col.Of<string>("title")], [1, "one"], [2, null]);
		var runtime = q.Map(RowMap<ReorderedPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<ReorderedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Literal);
	}

	[Fact]
	public void Exact_tier_wins_over_ignore_case()
	{
		// "id" and "Id" both match Id: the exact name takes it, the other column feeds nothing.
		var q = new TableQuery([Col.Of<int>("id"), Col.Of<int>("Id"), Col.Of<string>("Title")], [1, 2, "t"]);
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(2, generated[0].Id);
	}

	[Fact]
	public void Decorated_required_null_row_is_built_by_the_runtime()
	{
		var q = new TableQuery(
			[Col.Of<int>("post_id"), Col.Of<string>("title"), Col.Of<string>("note"), Col.Of<decimal>("price")],
			[1, "one", "n", 2m],
			[2, null, null, null]);
		var runtime = q.Map(RowMap<DecoratedInit>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<DecoratedInit>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c, rowsDeclined: 1);
	}

	[Fact]
	public void Keyword_named_members_are_served()
	{
		var q = new TableQuery([Col.Of<int>("event"), Col.Of<string>("class")], [1, "a"], [2, null]);
		var runtime = q.Map(RowMap<KeywordPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<KeywordPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
	}

	// ---- SequentialAccess: each cell once, in ascending column order ----

	[Fact]
	public void Sequential_reader_kept_null_rows_are_not_read_twice()
	{
		// The NULL Title (init-only, non-nullable) row goes to the runtime: from the values already read, never by reading the row again.
		var q = new TableQuery(
			[Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note"), Col.Of<decimal>("Price")],
			[1, "one", null, 1m],
			[2, null, "n", 2m])
		{ Sequential = true };

		List<PlainInit> runtime;
		using (DbDataReader reader = q.OpenReader()) runtime = reader.Map(RowMap<PlainInit>.Default).ToList();

		var start = Counts.Now();
		List<PlainInit> generated;
		using (DbDataReader reader = q.OpenReader()) generated = reader.Map<PlainInit>().ToList();
		var query = q.Map<PlainInit>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Parity.SameRows(runtime, query);
		Served(c, sets: 2, rowsDeclined: 2);
	}

	[Fact]
	public void Sequential_reader_unordered_layout_is_read_in_column_order()
	{
		var q = new TableQuery(
			[Col.Of<string>("Note"), Col.Of<bool>("IsActive"), Col.Of<int>("Id"), Col.Of<string>("Title")],
			["n", true, 5, "t"],
			[null, false, 6, null])
		{ Sequential = true };
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		var reordered = new TableQuery([Col.Of<int>("post_id"), Col.Of<string>("title")], [1, "one"]) { Sequential = true }
			.Map<ReorderedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Assert.Equal("one", reordered[0].Title);
		Served(c, sets: 2);
		Assert.Equal(1, c.Guarded);
		Assert.Equal(1, c.Literal);
	}

	// ---- one reader, several result sets ----

	[Fact]
	public void A_reader_view_mapped_again_after_NextResult_follows_the_new_layout()
	{
		static DataSet Sets()
		{
			var set = new DataSet();
			set.Tables.Add(TableQuery.Table(PostColumns, PostRows));
			set.Tables.Add(TableQuery.Table([Col.Of<string>("Title"), Col.Of<long>("Id")], ["second", 7L], [null, 8L]));
			return set;
		}

		List<PlainPost> runtimeFirst, runtimeSecond;
		using (DataTableReader reader = Sets().CreateDataReader())
		{
			MappedReader<PlainPost> view = reader.Map(RowMap<PlainPost>.Default);
			runtimeFirst = view.ToList();
			Assert.True(reader.NextResult());
			runtimeSecond = view.ToList();
		}

		var start = Counts.Now();
		List<PlainPost> first, second;
		using (DataTableReader reader = Sets().CreateDataReader())
		{
			MappedReader<PlainPost> view = reader.Map<PlainPost>();
			first = view.ToList();
			Assert.True(reader.NextResult());
			second = view.ToList();
		}

		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtimeFirst, first);
		Parity.SameRows(runtimeSecond, second);
		Assert.Equal(1, c.Intercepted);
		Assert.Equal(2, c.Served);
	}

	[Fact]
	public void Known_limit_a_next_set_with_the_same_column_count_keeps_the_first_binding()
	{
		// THE KNOWN SILENT DIFFERENCE (README, "Known limits"; a ship blocker until Core has a per-result-set hook). Same reader, one query value
		// run again after NextResult, the second set with the same column count in another order: the runtime reads it right, the generated
		// reader keeps the first set's ordinals and swaps the values. When a hook closes this, this test fails: turn it into a parity test.
		static DataSet Sets()
		{
			var set = new DataSet();
			set.Tables.Add(TableQuery.Table([Col.Of<int>("Id"), Col.Of<int>("Qty")], [1, 10]));
			set.Tables.Add(TableQuery.Table([Col.Of<int>("Qty"), Col.Of<int>("Id")], [20, 2]));
			return set;
		}

		PlainPost runtimeSecond, generatedSecond;
		using (DataTableReader reader = Sets().CreateDataReader())
		{
			MappedReader<PlainPost> view = reader.Map(RowMap<PlainPost>.Default);
			_ = view.ToList();
			Assert.True(reader.NextResult());
			runtimeSecond = view.ToList().Single();
		}

		using (DataTableReader reader = Sets().CreateDataReader())
		{
			MappedReader<PlainPost> view = reader.Map<PlainPost>();
			_ = view.ToList();
			Assert.True(reader.NextResult());
			generatedSecond = view.ToList().Single();
		}

		Assert.Equal((2, 20), (runtimeSecond.Id, runtimeSecond.Qty));
		Assert.Equal((20, 2), (generatedSecond.Id, generatedSecond.Qty)); // Pinned: wrong, silently.
	}

	// ---- left to the runtime: not intercepted ----

	[Fact]
	public void Shapes_the_generator_does_not_serve_are_not_intercepted()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [2, null]);

		var start = Counts.Now();
		SameOutcome(
			() => q.Map(RowMap<TwoConstructorsPost>.Default).ToList(),
			() => q.Map<TwoConstructorsPost>().ToList());
		SameOutcome(
			() => q.Map(RowMap<OverridePost>.Default).ToList(),
			() => q.Map<OverridePost>().ToList());
		SameOutcome(
			() => q.Data.Map(RowMap<PlainPost>.Default).ToList(),
			() => q.Data.Map<PlainPost>().ToList());
		Counts c = Counts.Now().Since(start);

		Assert.Equal(default, c);
	}

	[Fact]
	public void Contradicting_declarations_are_left_to_the_runtime()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<DuplicateOrderPost>.Default).ToList(),
			() => q.Map<DuplicateOrderPost>().ToList());
		SameThrow(
			() => q.Map(RowMap<NotMappedColumnPost>.Default).ToList(),
			() => q.Map<NotMappedColumnPost>().ToList());
		Counts c = Counts.Now().Since(start);

		Assert.Equal(default, c);
	}

	// ---- the benchmark twins (SG.md 5f) against the runtime map ----

	static readonly Col[] Catalog =
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
		Col.Of<string>("Body"),
		Col.Of<decimal>("Amount"),
		Col.Of<DateTime>("Stamp"),
		Col.Of<string>("Tag"),
		Col.Of<double>("Ratio"),
		Col.Of<bool>("Flag"),
		Col.Of<Guid>("Link"),
		Col.Of<long>("Count"),
		Col.Of<string>("Summary"),
	];

	static TableQuery CatalogQuery(int width)
	{
		object?[] full = [1, Day, "t", 12.50m, 3, true, KeyA, -0.0, "n", new byte[] { 1 }, 9L, "b", 1.10m, Day, "g", 0.25, false, KeyB, 4L, "s"];
		object?[] nulls = [2, Day, "u", 0m, null, false, KeyB, 1.5, null, null, 0L, "c", null, null, "h", 2.5, null, null, 5L, "z"];
		return new TableQuery([.. Catalog.Take(width)], [.. full.Take(width)], [.. nulls.Take(width)]);
	}

	[Fact]
	public void Benchmark_twins_match_the_runtime()
	{
		TableQuery w1 = CatalogQuery(1), w3 = CatalogQuery(3), w5 = CatalogQuery(5), w10 = CatalogQuery(10), w20 = CatalogQuery(20);

		var start = Counts.Now();
		Parity.SameRows(w1.Map(RowMap<GenRow1>.Default).ToList(), w1.Map<GenRow1>().ToList());
		Parity.SameRows(w1.Map(RowMap<GenRowD1>.Default).ToList(), w1.Map<GenRowD1>().ToList());
		Parity.SameRows(w3.Map(RowMap<GenRow3>.Default).ToList(), w3.Map<GenRow3>().ToList());
		Parity.SameRows(w3.Map(RowMap<GenRowD3>.Default).ToList(), w3.Map<GenRowD3>().ToList());
		Parity.SameRows(w5.Map(RowMap<GenRow5>.Default).ToList(), w5.Map<GenRow5>().ToList());
		Parity.SameRows(w5.Map(RowMap<GenRowD5>.Default).ToList(), w5.Map<GenRowD5>().ToList());
		Parity.SameRows(w10.Map(RowMap<GenRow10>.Default).ToList(), w10.Map<GenRow10>().ToList());
		Parity.SameRows(w10.Map(RowMap<GenRowD10>.Default).ToList(), w10.Map<GenRowD10>().ToList());
		Parity.SameRows(w20.Map(RowMap<GenRow20>.Default).ToList(), w20.Map<GenRow20>().ToList());
		Parity.SameRows(w20.Map(RowMap<GenRowD20>.Default).ToList(), w20.Map<GenRowD20>().ToList());
		Counts c = Counts.Now().Since(start);

		// The undecorated twins read every catalog column exactly, in order (mode 1); the decorated ones at constant ordinals (mode 3).
		Served(c, sets: 10);
		Assert.Equal(5, c.Full);
		Assert.Equal(5, c.Literal);
	}
}
