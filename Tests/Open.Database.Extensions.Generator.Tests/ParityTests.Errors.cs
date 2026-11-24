using Open.Database.Extensions.Generator.Models;
using System.Data.SqlTypes;

namespace Open.Database.Extensions.Generator.Tests;

// The identity read mode (5), and the error report: a generated row reader is one try region per row, so a failure is reported as a
// ValueMappingException naming the model type and the row (0-based), the exception inside, worded by that exception's type; no member or column:
// the runtime map's report, equal to it. A DbException and a cancellation pass through untouched, as the runtime map lets them.
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
		var identity = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [2, null]);
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

	// ---- the report ----

	/// <summary>
	/// The message a failed row gets, by the type of the exception inside: Core's RowErrors.Create for a model, word for word (it is internal, so it
	/// is written out here; SameRowError checks the runtime gives it too). The model is named as the runtime names it: Type.Name.
	/// </summary>
	static string RowMessage(Exception inner, Type model, int row)
		=> inner switch
		{
			SqlNullValueException
				=> $"A NULL was read into a non-nullable member of {model.Name} at row {row}. Declare the member nullable (int?, string?).",
			InvalidCastException
				=> $"Unable to map row {row} to '{model.Name}': a NULL was read into a non-nullable member, or a column's type does not convert to its member's. Declare the member nullable (int?, string?), or match the member's type to the column's.",
			_ => $"Unable to map row {row} to '{model.Name}'.",
		};

	/// <summary>The report of a failed row: the model type and the row, no member or column, the exception of this type inside.</summary>
	static ValueMappingException RowError<TInner>(Exception? error, Type model, int row)
		where TInner : Exception
	{
		var e = Assert.IsType<ValueMappingException>(error);
		var inner = Assert.IsType<TInner>(e.InnerException);
		Assert.Equal(model, e.TargetType);
		Assert.Equal("", e.PropertyName);
		Assert.Equal("", e.ColumnName);
		Assert.Equal(RowMessage(inner, model, row), e.Message);
		return e;
	}

	/// <summary>
	/// The generated reader and the runtime map fail the same row the same way: both reports have the row shape (<see cref="RowError{TInner}"/>), and
	/// they are equal (type, message, inner exception type and message, member, column, target type).
	/// </summary>
	static ValueMappingException SameRowError<TInner>(Func<object> runtime, Func<object> generated, Type model, int row)
		where TInner : Exception
	{
		Exception? r = Catch(() => runtime());
		Exception? g = Catch(() => generated());
		RowError<TInner>(r, model, row);
		Parity.SameError(r, g);
		return RowError<TInner>(g, model, row);
	}

	// ---- the model's own code throws ----

	[Fact]
	public void A_throwing_setter_reports_the_row()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<int>("Qty"), Col.Of<string>("Title")], [1, 2, "a"], [2, -1, "b"]);

		var start = Counts.Now();
		Exception? generated = Catch(() => q.Map<CheckedPost>().ToList());
		Counts c = Counts.Now().Since(start);

		var e = RowError<ArgumentOutOfRangeException>(generated, typeof(CheckedPost), 1);
		Assert.StartsWith("Qty cannot be negative.", e.InnerException!.Message, StringComparison.Ordinal);
		// The runtime map reports the same: the model and the row, no member.
		Parity.SameError(Catch(() => q.Map(RowMap<CheckedPost>.Default).ToList()), generated);
		Served(c);
		Assert.Equal(1, c.Identity);
	}

	[Fact]
	public void A_throwing_constructor_reports_the_row()
	{
		var q = new TableQuery([Col.Of<int>("Id")], [1]);

		var start = Counts.Now();
		var e = SameRowError<NotSupportedException>(
			() => q.Map(RowMap<FailingPost>.Default).ToList(),
			() => q.Map<FailingPost>().ToList(),
			typeof(FailingPost),
			0);
		Served(Counts.Now().Since(start));
		Assert.Equal("This model cannot be created.", e.InnerException!.Message);
	}

	[Fact]
	public void A_throwing_record_constructor_reports_the_row()
	{
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note")], [1, "a", null], [-1, "b", "n"]);

		var start = Counts.Now();
		SameRowError<ArgumentOutOfRangeException>(
			() => q.Map(RowMap<CheckedRecord>.Default).ToList(),
			() => q.Map<CheckedRecord>().ToList(),
			typeof(CheckedRecord),
			1);
		Served(Counts.Now().Since(start));
	}

	[Fact]
	public void A_throwing_init_accessor_and_a_throwing_setter_after_it_report_the_row()
	{
		Col[] columns = [Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note")];
		var init = new TableQuery(columns, [1, "t", "n"], [-2, "t", "n"]);
		var set = new TableQuery(columns, [1, "t", "n"], [3, "t", "m"], [4, "t", ""]);
		var sequential = new TableQuery(columns, [1, "t", ""]) { Sequential = true };

		var start = Counts.Now();
		SameRowError<ArgumentOutOfRangeException>(() => init.Map(RowMap<CheckedInit>.Default).ToList(), () => init.Map<CheckedInit>().ToList(), typeof(CheckedInit), 1);
		SameRowError<ArgumentException>(() => set.Map(RowMap<CheckedInit>.Default).ToList(), () => set.Map<CheckedInit>().ToList(), typeof(CheckedInit), 2);
		SameRowError<ArgumentException>(() => sequential.Map(RowMap<CheckedInit>.Default).ToList(), () => sequential.Map<CheckedInit>().ToList(), typeof(CheckedInit), 0);
		Served(Counts.Now().Since(start), sets: 3);
	}

	// ---- the provider fails in the middle of a row ----

	// What a provider can throw from a cell read: anything but its own DbException or a cancellation is reported with the row; those two pass through.
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

	// Every fault at one cell, from the expected read mode: a DbException or a cancellation exactly as the runtime map throws it (the same instance
	// type and message, unwrapped); anything else reported with the model and the row, the fault inside.
	static void FaultReport(
		Func<TableQuery, object> runtime,
		Func<TableQuery, object> generatedMap,
		Type model,
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

			Exception expected = fault();
			// The runtime map's outcome on the same fault is the generated reader's, whichever it is.
			Parity.SameError(Catch(() => runtime(q)), generated);
			if (expected is not (TestDbException or OperationCanceledException))
			{
				var e = Assert.IsType<ValueMappingException>(generated);
				Assert.IsType(expected.GetType(), e.InnerException);
				Assert.Equal(expected.Message, e.InnerException!.Message);
				Assert.Equal(model, e.TargetType);
				Assert.Equal("", e.PropertyName);
				Assert.Equal("", e.ColumnName);
				Assert.Equal(RowMessage(expected, model, row), e.Message);
			}

			Served(c);
			Assert.Equal(1, mode(c));
		}
	}

	[Fact]
	public void A_provider_failure_mid_row_reports_the_row_in_every_read_mode()
	{
		// Identity (5): an exact read of a non-nullable member, a nullable one, and a general (GetValue) one.
		FaultReport(PostRuntime, PostGenerated, typeof(PlainPost), PostColumns, PostRows, 2, 6, c => c.Identity);
		FaultReport(PostRuntime, PostGenerated, typeof(PlainPost), PostColumns, PostRows, 2, 8, c => c.Identity);
		FaultReport(PostRuntime, PostGenerated, typeof(PlainPost), PostColumns, PostRows, 2, 9, c => c.Identity);
		FaultReport(PostRuntime, PostGenerated, typeof(PlainPost), PostColumns, PostRows, 2, 6, c => c.Identity, sequential: true);

		// Full (1): the same member one column further.
		Col[] shifted = [.. PostColumns[..3], Col.Of<string>("Unused"), .. PostColumns[3..]];
		object?[][] shiftedRows = [.. PostRows.Select(r => (object?[])[.. r[..3], "u", .. r[3..]])];
		FaultReport(PostRuntime, PostGenerated, typeof(PlainPost), shifted, shiftedRows, 2, 7, c => c.Full);

		// Literal (3), at a declared ordinal.
		FaultReport(DecoratedRuntime, DecoratedGenerated, typeof(DecoratedPost), DecoratedColumns, PostRows, 2, 6, c => c.Literal);

		// Guarded (2): some members absent, the columns out of slot order.
		Col[] partial = [Col.Of<string>("Note"), Col.Of<bool>("IsActive"), Col.Of<int>("Id"), Col.Of<string>("Title")];
		object?[][] partialRows = [["n", true, 5, "t"], [null, false, 6, "u"]];
		FaultReport(PostRuntime, PostGenerated, typeof(PlainPost), partial, partialRows, 1, 2, c => c.Guarded);
		FaultReport(PostRuntime, PostGenerated, typeof(PlainPost), partial, partialRows, 1, 2, c => c.Guarded, sequential: true);

		// A constructor parameter and a member set after the constructor, at constant ordinals (5) and through converting reads (4).
		Col[] record = [Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<int>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")];
		object?[][] recordRows = [[1, "one", 2, 1, "t", "n"], [2, "two", 3, 2, "u", "m"]];
		FaultReport(RecordRuntime, RecordGenerated, typeof(PlainRecord), record, recordRows, 1, 3, c => c.Identity);
		FaultReport(RecordRuntime, RecordGenerated, typeof(PlainRecord), record, recordRows, 1, 5, c => c.Identity);
		Col[] converting = [Col.Of<long>("Id"), Col.Of<string>("Title"), Col.Of<long>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")];
		object?[][] convertingRows = [[1L, "one", 2L, 1, "t", "n"], [2L, "two", 3L, 2, "u", "m"]];
		FaultReport(RecordRuntime, RecordGenerated, typeof(PlainRecord), converting, convertingRows, 1, 2, c => c.Converting);
		FaultReport(RecordRuntime, RecordGenerated, typeof(PlainRecord), converting, convertingRows, 1, 5, c => c.Converting);
	}

	[Fact]
	public void The_row_index_counts_from_each_execution_and_counts_the_rows_mapped()
	{
		// The fault sits at the third row the reader reads. One mapped query run twice: each execution binds a new reader, so the count starts again.
		var q = new TableQuery(PostColumns, PostRows) { Wrap = r => new FaultReader(r, 2, 6, () => new InvalidOperationException("The connection dropped.")) };
		var mapped = q.Map<PlainPost>();
		SameRowError<InvalidOperationException>(() => q.Map(RowMap<PlainPost>.Default).ToList(), () => mapped.ToList(), typeof(PlainPost), 2);
		SameRowError<InvalidOperationException>(() => q.Map(RowMap<PlainPost>.Default).ToList(), () => mapped.ToList(), typeof(PlainPost), 2);

		// A KNOWN DIFFERENCE, pinned on both sides: Core skips rows without calling the selector, so the generated reader counts the rows it mapped
		// and reports the reader's third row as row 1; the runtime counts the skipped row and reports row 2. Closing it needs a hook in Core.
		RowError<InvalidOperationException>(Catch(() => q.Map<PlainPost>().Skip(1).ToList()), typeof(PlainPost), 1);
		RowError<InvalidOperationException>(Catch(() => q.Map(RowMap<PlainPost>.Default).Skip(1).ToList()), typeof(PlainPost), 2);
	}
}
