using Open.Database.Extensions.Generator.Models;
using System.Collections.Concurrent;

namespace Open.Database.Extensions.Generator.Tests;

/// <summary>
/// A model whose setter throws a ValueMappingException of its own (here, in the tests' own assembly: the models library references nothing of ODX).
/// The row's handler leaves such an exception as it is.
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
// In each: a NULL in a member declared non-nullable throws a ValueMappingException naming the model and the row on the generated path (the
// provider's exception inside in Release; the member and the column named in Debug); the runtime map's behaviour on the same data is its own
// expectation (it keeps the member's value, it does not throw), pinned beside it; a NULL in a nullable member gives null, as the runtime gives it.
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
			NullThrows<InvalidCastException>(Catch(() => q.Map<DecoratedPost>().ToList()), typeof(DecoratedPost), 1, member, DecoratedColumns[column].Name);
			reported++;

			// The runtime map's own behaviour on this data: no exception, the member keeps its value.
			var runtime = q.Map(RowMap<DecoratedPost>.Default).ToList();
			Assert.Equal(3, runtime.Count);
			if (member == "Title") Assert.Equal("untitled", runtime[1].Title);
			if (member == "State") Assert.Equal(PostState.Active, runtime[1].State);
			if (member == "Id") Assert.Equal(0, runtime[1].Id);
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
		string[] members = ["Id", "Title", "Note", "Price"];
		foreach (int column in new[] { 0, 1, 3 })
		{
			object?[] row = [1, "one", "n", 2m];
			row[column] = null;
			var q = new TableQuery(DecoratedInitColumns, [2, "two", null, 1m], row);
			NullThrows<InvalidCastException>(Catch(() => q.Map<DecoratedInit>().ToList()), typeof(DecoratedInit), 1, members[column], DecoratedInitColumns[column].Name);

			// The runtime builds the row instead of throwing: what it gives is its own expectation (Price keeps its default).
			var runtime = q.Map(RowMap<DecoratedInit>.Default).ToList();
			Assert.Equal(2, runtime.Count);
			if (column == 3) Assert.Equal(0m, runtime[1].Price);
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
		NullThrows<InvalidCastException>(Catch(() => title.Map<PlainInit>().ToList()), typeof(PlainInit), 1, "Title", "Title");
		Assert.Equal(2, title.Map(RowMap<PlainInit>.Default).ToList().Count); // The runtime builds it.
	}

	[Fact]
	public void Null_late_in_a_long_result_reports_its_row()
	{
		// Identity read (5), and constant ordinals (3): the row index in the message is the row's, however far into the result it is.
		const int Rows = 3000, At = 2741;
		object?[][] post = [.. Enumerable.Range(0, Rows).Select(i => i == At ? PostRowWithNull(0) : PostRows[i % 3])];
		var identity = new TableQuery(PostColumns, post);
		NullThrows<InvalidCastException>(Catch(() => identity.Map<PlainPost>().ToList()), typeof(PlainPost), At, "Id", "Id");

		object?[][] decorated = [.. Enumerable.Range(0, Rows).Select(i => i == At ? DecoratedRowWithNull(2) : PostRows[i % 3])];
		var literal = new TableQuery(DecoratedColumns, decorated);
		NullThrows<InvalidCastException>(Catch(() => literal.Map<DecoratedPost>().ToList()), typeof(DecoratedPost), At, "Title", "title");

		// Each execution counts from its own first row: the same query again reports the same row.
		NullThrows<InvalidCastException>(Catch(() => identity.Map<PlainPost>().ToList()), typeof(PlainPost), At, "Id", "Id");
	}

	[Fact]
	public void A_generic_and_a_nested_model_are_named_by_their_arguments_and_outer_type()
	{
		// TaggedPost<int> is "TaggedPost<Int32>" (not "TaggedPost`1", not "Int32>"); a nested model carries its outer type.
		Col[] columns = [Col.Of<int>("Id"), Col.Of<string>("Title")];
		var generic = new TableQuery(columns, [1, "one"], [2, null]);
		NullThrows<InvalidCastException>(Catch(() => generic.Map<TaggedPost<int>>().ToList()), typeof(TaggedPost<int>), 1, "Title", "Title", "TaggedPost<Int32>");
		Parity.SameRows(generic.Map(RowMap<TaggedPost<int>>.Default).ToList(), [new TaggedPost<int> { Id = 1, Title = "one" }, new TaggedPost<int> { Id = 2, Title = "untitled" }]);

		var nested = new TableQuery(columns, [1, "one"], [2, null]);
		NullThrows<InvalidCastException>(Catch(() => nested.Map<PostHolder.Inner>().ToList()), typeof(PostHolder.Inner), 1, "Title", "Title", "PostHolder.Inner");
	}

	[Fact]
	public void A_non_nullable_interface_member_throws_on_a_NULL_when_the_cast_of_DBNull_fails()
	{
		// The cast of DBNull to IComparable throws, like a class member's: not a silent null. (A provider reports a column as the interface type
		// itself only in a typed table like this one, which is what a generated read needs.)
		Col[] columns = [Col.Of<int>("Id"), Col.Of<IComparable>("Value"), Col.Of<IConvertible>("Other")];
		var value = new TableQuery(columns, [1, "a", "x"], [2, null, "y"]);

		var start = Counts.Now();
		NullThrows<InvalidCastException>(Catch(() => value.Map<InterfacePost>().ToList()), typeof(InterfacePost), 1, "Value", "Value");
		Counts c = Counts.Now().Since(start);
		Served(c);

		// The runtime map keeps the member's value instead.
		Assert.Equal("", value.Map(RowMap<InterfacePost>.Default).ToList()[1].Value);
	}

	[Fact]
	public void A_non_nullable_interface_member_DBNull_converts_to_stays_tested()
	{
		// DBNull implements IConvertible: the cast of DBNull succeeds, nothing would throw, so the member is tested and a NULL gives null.
		Col[] columns = [Col.Of<int>("Id"), Col.Of<IComparable>("Value"), Col.Of<IConvertible>("Other")];
		var q = new TableQuery(columns, [1, "a", "x"], [2, "b", null]);

		var start = Counts.Now();
		var generated = q.Map<InterfacePost>().ToList();
		Counts c = Counts.Now().Since(start);

		Served(c);
		Assert.Equal("x", generated[0].Other);
		Assert.Null(generated[1].Other);
		// The runtime map keeps the member's value instead: its own expectation.
		Assert.Equal("", q.Map(RowMap<InterfacePost>.Default).ToList()[1].Other);
	}

	[Fact]
	public void A_nullable_member_never_calls_the_Debug_check_and_a_NULL_there_gives_null_in_both_configurations()
	{
		// Title is nullable: its NULL gives null, no exception in Debug or Release, and the rows are the runtime's. (GeneratedIlTests checks the
		// generated file itself: no check before a tested read.)
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [2, null], [3, null]);
		var generated = q.Map<SkippedPost>().ToList();
		Parity.SameRows(q.Map(RowMap<SkippedPost>.Default).ToList(), generated);
		Assert.Null(generated[1].Title);
	}

	[Fact]
	public void A_ValueMappingException_from_the_model_is_not_wrapped_a_second_time()
	{
		// The row's handler leaves a ValueMappingException as it is (the Debug check throws one): here a model's own setter does, in both configurations.
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"]);
		var e = Assert.IsType<ValueMappingException>(Catch(() => q.Map<ReportingPost>().ToList()));
		Assert.Equal("from the setter", e.Message);
		Assert.Null(e.InnerException);
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
					if (error is not ValueMappingException e || !e.Message.Contains($"at row {at}", StringComparison.Ordinal) || e.TargetType != typeof(PlainPost))
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
