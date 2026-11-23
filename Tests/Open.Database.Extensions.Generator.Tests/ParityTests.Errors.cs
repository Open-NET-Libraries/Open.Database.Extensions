using Open.Database.Extensions.Generator.Models;

namespace Open.Database.Extensions.Generator.Tests;

// The identity read mode (5), and error attribution without per-cell stores: each member's read and set is its own try region,
// so a failure names the member the runtime map names (RowMap.Guard/MapError), in every read mode, on a plain and a SequentialAccess reader.
public partial class ParityTests
{
	// ---- the identity layout: the members in their declared order, read at constant ordinals ----

	[Fact]
	public void Identity_layout_with_trailing_columns_reads_at_constant_ordinals()
	{
		// Columns after the members feed nothing: the runtime ignores them, so the layout is still the identity.
		Col[] columns = [.. PostColumns, Col.Of<string>("Extra"), Col.Of<int>("More")];
		object?[][] rows = [.. PostRows.Select(r => (object?[])[.. r, "x", 9])];
		var q = new TableQuery(columns, rows);
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Identity);
	}

	[Fact]
	public void A_column_between_the_members_is_full_not_identity()
	{
		// Every member fed, ascending, exact, but Price sits at 4, not 3: the layout's ordinals are read (mode 1).
		Col[] columns = [.. PostColumns[..3], Col.Of<string>("Unused"), .. PostColumns[3..]];
		object?[][] rows = [.. PostRows.Select(r => (object?[])[.. r[..3], "u", .. r[3..]])];
		var q = new TableQuery(columns, rows);
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Full);
		Assert.Equal(0, c.Identity);
	}

	[Fact]
	public void Identity_skips_a_member_not_mapped()
	{
		var identity = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [null, null]);
		var shifted = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Display"), Col.Of<string>("Title")], [1, "d", "one"], [2, null, null]);

		var start = Counts.Now();
		Parity.SameRows(identity.Map(RowMap<SkippedPost>.Default).ToList(), identity.Map<SkippedPost>().ToList());
		Counts first = Counts.Now().Since(start);
		start = Counts.Now();
		Parity.SameRows(shifted.Map(RowMap<SkippedPost>.Default).ToList(), shifted.Map<SkippedPost>().ToList());
		Counts second = Counts.Now().Since(start);

		Served(first);
		Assert.Equal(1, first.Identity);
		Served(second);
		Assert.Equal(1, second.Full);
	}

	[Fact]
	public void Identity_on_a_sequential_reader()
	{
		var q = new TableQuery(PostColumns, PostRows) { Sequential = true };
		var runtime = q.Map(RowMap<PlainPost>.Default).ToList();

		var start = Counts.Now();
		var generated = q.Map<PlainPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(runtime, generated);
		Served(c);
		Assert.Equal(1, c.Identity);
	}

	// ---- the model's own code throws ----

	[Fact]
	public void A_throwing_setter_names_its_member()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<int>("Qty"), Col.Of<string>("Title")], [1, 2, "a"], [2, -1, "b"]);

		var start = Counts.Now();
		Exception? generated = Catch(() => q.Map<CheckedPost>().ToList());
		Counts c = Counts.Now().Since(start);

		Parity.SameError(Catch(() => q.Map(RowMap<CheckedPost>.Default).ToList()), generated);
		Assert.Equal("Qty", Assert.IsType<ValueMappingException>(generated).PropertyName);
		Served(c);
		Assert.Equal(1, c.Identity);
	}

	[Fact]
	public void A_throwing_constructor_is_not_attributed_to_a_column()
	{
		var q = new TableQuery([Col.Of<int>("Id")], [1]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<FailingPost>.Default).ToList(),
			() => q.Map<FailingPost>().ToList());
		Served(Counts.Now().Since(start));
	}

	[Fact]
	public void A_throwing_record_constructor_is_not_attributed_to_a_column()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note")], [1, "a", null], [-1, "b", "n"]);

		var start = Counts.Now();
		SameThrow(
			() => q.Map(RowMap<CheckedRecord>.Default).ToList(),
			() => q.Map<CheckedRecord>().ToList());
		Served(Counts.Now().Since(start));
	}

	[Fact]
	public void A_throwing_init_accessor_and_a_throwing_setter_after_it_name_their_members()
	{
		Col[] columns = [Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note")];
		var init = new TableQuery(columns, [1, "t", "n"], [-2, "t", "n"]);
		var set = new TableQuery(columns, [1, "t", "n"], [3, "t", ""]);

		var start = Counts.Now();
		SameThrow(
			() => init.Map(RowMap<CheckedInit>.Default).ToList(),
			() => init.Map<CheckedInit>().ToList());
		Exception? generated = Catch(() => set.Map<CheckedInit>().ToList());
		Parity.SameError(Catch(() => set.Map(RowMap<CheckedInit>.Default).ToList()), generated);
		Counts c = Counts.Now().Since(start);

		Assert.Equal("Note", Assert.IsType<ValueMappingException>(generated).PropertyName);
		Served(c, sets: 2);
	}

	// ---- the provider fails in the middle of a row ----

	// What a provider can throw from a cell read: a non-Db failure is attributed to the member; its own DbException and a cancellation pass through untouched.
	static readonly Func<Exception>[] Faults =
	[
		() => new InvalidOperationException("The connection dropped."),
		() => new InvalidCastException("The value is not of the reported type."),
		() => new TestDbException("The server refused the read."),
		() => new OperationCanceledException("The read was cancelled."),
	];

	// The calls of one model, written where T is closed (a Map<T>() inside a generic method is not intercepted).
	static readonly Func<TableQuery, object> PostRuntime = q => q.Map(RowMap<PlainPost>.Default).ToList();
	static readonly Func<TableQuery, object> PostGenerated = q => q.Map<PlainPost>().ToList();
	static readonly Func<TableQuery, object> DecoratedRuntime = q => q.Map(RowMap<DecoratedPost>.Default).ToList();
	static readonly Func<TableQuery, object> DecoratedGenerated = q => q.Map<DecoratedPost>().ToList();
	static readonly Func<TableQuery, object> RecordRuntime = q => q.Map(RowMap<PlainRecord>.Default).ToList();
	static readonly Func<TableQuery, object> RecordGenerated = q => q.Map<PlainRecord>().ToList();

	// Every fault at one cell: the same exception as the runtime map's, from the expected read mode.
	static void FaultParity(
		Func<TableQuery, object> runtime,
		Func<TableQuery, object> generatedMap,
		Col[] columns,
		object?[][] rows,
		int row,
		int ordinal,
		Func<Counts, long> mode,
		bool sequential = false)
	{
		foreach (Func<Exception> fault in Faults)
		{
			var q = new TableQuery(columns, rows) { Sequential = sequential, Wrap = r => new FaultReader(r, row, ordinal, fault) };

			var start = Counts.Now();
			Exception? generated = Catch(() => generatedMap(q));
			Counts c = Counts.Now().Since(start);

			Parity.SameError(Catch(() => runtime(q)), generated);
			Served(c);
			Assert.Equal(1, mode(c));
		}
	}

	[Fact]
	public void A_provider_failure_mid_row_is_reported_as_the_runtime_reports_it()
	{
		// Identity (5): an exact read of a non-nullable member, a nullable one, and a general (GetValue) one.
		FaultParity(PostRuntime, PostGenerated, PostColumns, PostRows, 2, 6, c => c.Identity);
		FaultParity(PostRuntime, PostGenerated, PostColumns, PostRows, 2, 8, c => c.Identity);
		FaultParity(PostRuntime, PostGenerated, PostColumns, PostRows, 2, 9, c => c.Identity);
		FaultParity(PostRuntime, PostGenerated, PostColumns, PostRows, 2, 6, c => c.Identity, sequential: true);

		// Full (1): the same member one column further.
		Col[] shifted = [.. PostColumns[..3], Col.Of<string>("Unused"), .. PostColumns[3..]];
		object?[][] shiftedRows = [.. PostRows.Select(r => (object?[])[.. r[..3], "u", .. r[3..]])];
		FaultParity(PostRuntime, PostGenerated, shifted, shiftedRows, 2, 7, c => c.Full);

		// Literal (3), at a declared ordinal.
		FaultParity(DecoratedRuntime, DecoratedGenerated, DecoratedColumns, PostRows, 2, 6, c => c.Literal);

		// Guarded (2): some members absent, the columns out of slot order.
		Col[] partial = [Col.Of<string>("Note"), Col.Of<bool>("IsActive"), Col.Of<int>("Id"), Col.Of<string>("Title")];
		FaultParity(PostRuntime, PostGenerated, partial, [["n", true, 5, "t"], [null, false, 6, "u"]], 1, 2, c => c.Guarded);
		FaultParity(PostRuntime, PostGenerated, partial, [["n", true, 5, "t"], [null, false, 6, "u"]], 1, 2, c => c.Guarded, sequential: true);

		// A constructor parameter and a member set after the constructor, at constant ordinals (5) and through converting reads (4).
		Col[] record = [Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<int>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")];
		object?[][] recordRows = [[1, "one", 2, 1, "t", "n"], [2, "two", 3, 2, "u", "m"]];
		FaultParity(RecordRuntime, RecordGenerated, record, recordRows, 1, 3, c => c.Identity);
		FaultParity(RecordRuntime, RecordGenerated, record, recordRows, 1, 5, c => c.Identity);
		Col[] converting = [Col.Of<long>("Id"), Col.Of<string>("Title"), Col.Of<long>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")];
		object?[][] convertingRows = [[1L, "one", 2L, 1, "t", "n"], [2L, "two", 3L, 2, "u", "m"]];
		FaultParity(RecordRuntime, RecordGenerated, converting, convertingRows, 1, 2, c => c.Converting);
		FaultParity(RecordRuntime, RecordGenerated, converting, convertingRows, 1, 5, c => c.Converting);
	}

	[Fact]
	public void A_failure_names_the_member_and_column()
	{
		// The reference itself: the runtime names the member and column being read, and so does the generated reader.
		var q = new TableQuery(PostColumns, PostRows) { Wrap = r => new FaultReader(r, 2, 6, () => new InvalidOperationException("The connection dropped.")) };
		var error = Assert.IsType<ValueMappingException>(Catch(() => q.Map<PlainPost>().ToList()));
		Assert.Equal("Key", error.PropertyName);
		Assert.Equal("Key", error.ColumnName);
		Assert.IsType<InvalidOperationException>(error.InnerException);
	}
}
