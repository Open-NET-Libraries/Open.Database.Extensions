using Open.Database.Extensions.Generator.Models;
using System.Collections.Concurrent;
using System.Data.SqlTypes;

namespace Open.Database.Extensions.Generator.Tests;

/// <summary>
/// A model whose setter throws a ValueMappingException of its own (here, in the tests' own assembly: the models library references nothing of ODX).
/// The row's handler wraps it like any other failure, as the runtime map's does.
/// </summary>
public sealed class ReportingPost
{
	public int Id { get; set; }

	public string Title
	{
		get => "";
		set => throw new ValueMappingException("from the setter", nameof(Title), "Title", typeof(string));
	}
}

// The NULL rule in the other layouts: constant declared ordinals (the decorated models), the init-only and required models, a NULL late in a long
// result, a generic and a nested model's name, interface members, and several threads at once (the row counter and the shared layout cache).
// In each: a NULL in a member declared non-nullable throws a ValueMappingException naming the model and the row (the provider's exception
// inside, no member or column named), equal to the runtime map's on the same data; a NULL in a nullable member gives null, as the runtime gives it.
public partial class ParityTests
{
	// The members of DecoratedPost by column, null for a nullable member (Qty, Note, Blob, Label).
	static readonly string?[] DecoratedMembers = ["Id", "Created", "Title", "Price", null, "IsActive", "Key", "Score", null, null, "Big", "State", null];

	static object?[] DecoratedRowWithNull(int column)
	{
		object?[] row = [.. PostRows[0]];
		row[column] = null;
		return row;
	}

	[Fact]
	public void Null_in_each_non_nullable_member_of_a_decorated_model_reports_the_row_at_constant_ordinals()
	{
		var start = Counts.Now();
		int reported = 0;
		for (int column = 0; column < DecoratedMembers.Length; column++)
		{
			if (DecoratedMembers[column] is not { } member) continue;

			var q = new TableQuery(DecoratedColumns, PostRows[0], DecoratedRowWithNull(column), PostRows[2]);
			SameRowError<InvalidCastException>(() => q.Map(RowMap<DecoratedPost>.Default).ToList(), () => q.Map<DecoratedPost>().ToList(), typeof(DecoratedPost), 1);
			reported++;
		}

		Assert.Equal(9, reported);
		Assert.Equal(reported, Counts.Now().Since(start).Literal);
	}

	[Fact]
	public void Null_in_each_nullable_member_of_a_decorated_model_equals_the_runtime_row()
	{
		// Qty (int?), Note (string?), Blob (byte[]?) and the [AllowNull] Label, each NULL on its own and all together.
		var rows = new List<object?[]> { PostRows[0] };
		foreach (int column in new[] { 4, 8, 9, 12 })
			rows.Add(DecoratedRowWithNull(column));

		rows.Add([2, Day, "second", 0m, null, false, KeyA, 0.0, null, null, 0L, 1, null]);
		var q = new TableQuery(DecoratedColumns, [.. rows]);

		var start = Counts.Now();
		var generated = q.Map<DecoratedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(q.Map(RowMap<DecoratedPost>.Default).ToList(), generated);
		Served(c);
		Assert.Equal(1, c.Literal);
		Assert.Null(generated[1].Qty);
		Assert.Null(generated[2].Note);
		Assert.Null(generated[3].Blob);
		Assert.Null(generated[4].Label);
	}

	static readonly Col[] DecoratedInitColumns =
	[
		Col.Of<int>("post_id"),
		Col.Of<string>("title"),
		Col.Of<string>("note"),
		Col.Of<decimal>("price"),
	];

	[Fact]
	public void Null_in_a_non_nullable_member_of_a_decorated_init_model_reports_the_row()
	{
		// Id and Title are required init-only (read into locals, then an object initializer), Price is set after it; all at declared ordinals.
		var start = Counts.Now();
		foreach (int column in new[] { 0, 1, 3 })
		{
			object?[] row = [1, "one", "n", 2m];
			row[column] = null;
			var q = new TableQuery(DecoratedInitColumns, [2, "two", null, 1m], row);
			SameRowError<InvalidCastException>(() => q.Map(RowMap<DecoratedInit>.Default).ToList(), () => q.Map<DecoratedInit>().ToList(), typeof(DecoratedInit), 1);
		}

		Assert.Equal(3, Counts.Now().Since(start).Literal);
	}

	[Fact]
	public void Null_in_the_nullable_member_of_a_decorated_init_model_equals_the_runtime_row()
	{
		var q = new TableQuery(DecoratedInitColumns, [1, "one", null, 2m], [2, "two", "n", 3m], [3, "three", null, 4m]);

		var start = Counts.Now();
		var generated = q.Map<DecoratedInit>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(q.Map(RowMap<DecoratedInit>.Default).ToList(), generated);
		Served(c);
		Assert.Equal(1, c.Literal);
		Assert.Null(generated[0].Note);
	}

	[Fact]
	public void Null_in_a_non_nullable_member_of_an_undecorated_init_model_reports_the_row_and_a_nullable_one_matches()
	{
		// The undecorated init model at constant ordinals (5): Id, Title (required init-only), Note (init-only, nullable), Price.
		Col[] columns = [Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note"), Col.Of<decimal>("Price")];
		var nullable = new TableQuery(columns, [1, "one", null, 2m], [2, "two", null, 3m]);
		Parity.SameRows(nullable.Map(RowMap<PlainInit>.Default).ToList(), nullable.Map<PlainInit>().ToList());

		var title = new TableQuery(columns, [1, "one", null, 2m], [2, null, "n", 3m]);
		SameRowError<InvalidCastException>(() => title.Map(RowMap<PlainInit>.Default).ToList(), () => title.Map<PlainInit>().ToList(), typeof(PlainInit), 1);
	}

	[Fact]
	public void Null_late_in_a_long_result_reports_its_row()
	{
		// Identity read (5), and constant ordinals (3): the row index in the message is the row's, however far into the result it is.
		const int Rows = 3000, At = 2741;
		object?[][] post = [.. Enumerable.Range(0, Rows).Select(i => i == At ? PostRowWithNull(0) : PostRows[i % 3])];
		var identity = new TableQuery(PostColumns, post);
		SameRowError<InvalidCastException>(() => identity.Map(RowMap<PlainPost>.Default).ToList(), () => identity.Map<PlainPost>().ToList(), typeof(PlainPost), At);

		object?[][] decorated = [.. Enumerable.Range(0, Rows).Select(i => i == At ? DecoratedRowWithNull(2) : PostRows[i % 3])];
		var literal = new TableQuery(DecoratedColumns, decorated);
		SameRowError<InvalidCastException>(() => literal.Map(RowMap<DecoratedPost>.Default).ToList(), () => literal.Map<DecoratedPost>().ToList(), typeof(DecoratedPost), At);

		// Each execution counts from its own first row: the same query again reports the same row.
		RowError<InvalidCastException>(Catch(() => identity.Map<PlainPost>().ToList()), typeof(PlainPost), At);
	}

	[Fact]
	public void A_generic_and_a_nested_model_are_named_as_the_runtime_names_them()
	{
		// The runtime names a model by Type.Name: TaggedPost<int> is "TaggedPost`1", a nested model is "Inner". The generated report says the same.
		Col[] columns = [Col.Of<int>("Id"), Col.Of<string>("Title")];
		var generic = new TableQuery(columns, [1, "one"], [2, null]);
		var e = SameRowError<InvalidCastException>(() => generic.Map(RowMap<TaggedPost<int>>.Default).ToList(), () => generic.Map<TaggedPost<int>>().ToList(), typeof(TaggedPost<int>), 1);
		Assert.Contains("'TaggedPost`1'", e.Message, StringComparison.Ordinal);

		var nested = new TableQuery(columns, [1, "one"], [2, null]);
		e = SameRowError<InvalidCastException>(() => nested.Map(RowMap<PostHolder.Inner>.Default).ToList(), () => nested.Map<PostHolder.Inner>().ToList(), typeof(PostHolder.Inner), 1);
		Assert.Contains("'Inner'", e.Message, StringComparison.Ordinal);
	}

	[Fact]
	public void A_non_nullable_interface_member_reports_a_NULL_read()
	{
		// A general read (GetValue) of a non-nullable member checks for NULL as the runtime's does, so the report is the runtime's NULL report
		// (SqlNullValueException inside), not a cast error. (A provider reports a column as the interface type itself only in a typed table like
		// this one, which is what a generated read needs.)
		Col[] columns = [Col.Of<int>("Id"), Col.Of<IComparable>("Value"), Col.Of<IConvertible>("Other")];
		var value = new TableQuery(columns, [1, "a", "x"], [2, null, "y"]);

		var start = Counts.Now();
		SameRowError<SqlNullValueException>(() => value.Map(RowMap<InterfacePost>.Default).ToList(), () => value.Map<InterfacePost>().ToList(), typeof(InterfacePost), 1);
		Counts c = Counts.Now().Since(start);
		Served(c);
	}

	[Fact]
	public void A_non_nullable_interface_member_DBNull_converts_to_still_reports_a_NULL_read()
	{
		// DBNull implements IConvertible, so a cast of DBNull would succeed and hide the NULL: the check makes it the runtime's NULL report too.
		Col[] columns = [Col.Of<int>("Id"), Col.Of<IComparable>("Value"), Col.Of<IConvertible>("Other")];
		var clean = new TableQuery(columns, [1, "a", "x"], [2, "b", "y"]);
		var q = new TableQuery(columns, [1, "a", "x"], [2, "b", null]);

		var start = Counts.Now();
		Parity.SameRows(clean.Map(RowMap<InterfacePost>.Default).ToList(), clean.Map<InterfacePost>().ToList());
		SameRowError<SqlNullValueException>(() => q.Map(RowMap<InterfacePost>.Default).ToList(), () => q.Map<InterfacePost>().ToList(), typeof(InterfacePost), 1);
		Counts c = Counts.Now().Since(start);

		Served(c, sets: 2);
	}

	[Fact]
	public void A_NULL_in_a_nullable_member_gives_null_not_an_exception()
	{
		// Title is nullable: its NULL gives null, no exception, and the rows are the runtime's. (GeneratedSourceTests checks the
		// generated file itself: IsDBNull exactly for the nullable members.)
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [2, null], [3, null]);
		var generated = q.Map<SkippedPost>().ToList();
		Parity.SameRows(q.Map(RowMap<SkippedPost>.Default).ToList(), generated);
		Assert.Null(generated[1].Title);
	}

	[Fact]
	public void A_ValueMappingException_from_the_model_is_reported_with_the_row_as_the_runtime_reports_it()
	{
		// A model's own ValueMappingException is a failure like any other: both maps wrap it in the row's report, the model's exception inside.
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"]);
		var e = SameRowError<ValueMappingException>(() => q.Map(RowMap<ReportingPost>.Default).ToList(), () => q.Map<ReportingPost>().ToList(), typeof(ReportingPost), 0);
		Assert.Equal("from the setter", e.InnerException!.Message);
	}

	[Fact]
	public async Task Several_threads_each_report_their_own_row_and_share_the_layout_cache()
	{
		// Twelve threads (more than the cache's eight slots), each its own query over PlainPost with its own column list (so its own layout, replacing
		// others in the cache) and its NULL at its own row. Every evaluation runs its own binding and row counter: each thread's report names its own row,
		// and a clean evaluation in between returns every row. Nothing else may be thrown.
		const int Threads = 12, Rounds = 150, Rows = 40;
		var failures = new ConcurrentQueue<string>();
		using var start = new Barrier(Threads);

		void Work(int thread)
		{
			int at = 3 + thread;
			Col[] columns = [.. PostColumns, Col.Of<string>("Extra" + thread)];
			object?[][] clean = [.. Enumerable.Range(0, Rows).Select(i => (object?[])[.. PostRows[i % 3], "x"])];
			object?[][] nulled = [.. clean.Select((r, i) => i == at ? [.. PostRowWithNull(0), "x"] : r)];
			var good = new TableQuery(columns, clean);
			var bad = new TableQuery(columns, nulled);
			start.SignalAndWait();
			for (int round = 0; round < Rounds; round++)
			{
				try
				{
					int count = good.Map<PlainPost>().ToList().Count;
					if (count != Rows) failures.Enqueue($"thread {thread}: {count} rows");

					Exception? error = Catch(() => bad.Map<PlainPost>().ToList());
					if (error is not ValueMappingException { InnerException: { } inner } e || e.Message != RowMessage(inner, typeof(PlainPost), at) || e.TargetType != typeof(PlainPost))
						failures.Enqueue($"thread {thread}, round {round}: expected a report of row {at}, got {error}");
				}
				catch (Exception ex)
				{
					failures.Enqueue($"thread {thread}, round {round}: {ex}");
				}
			}
		}

		await Task.WhenAll(Enumerable.Range(0, Threads).Select(t => Task.Factory.StartNew(() => Work(t), TestContext.Current.CancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default)));
		Assert.True(failures.IsEmpty, string.Join(Environment.NewLine, failures.Take(10)));
	}
}
