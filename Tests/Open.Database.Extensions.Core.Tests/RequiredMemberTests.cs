using Open.Database.Extensions.Core;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks.Dataflow;
using static Open.Database.Extensions.Tests.ConstructorBindingTests;

#nullable enable

namespace Open.Database.Extensions.Tests;

// `required` members (phase D). A `required` property maps like any other property, and every result shape must have a column for it: when none does, building the row reader for that shape
// throws ValueMappingException, which is when its first row is mapped, so a query with no rows never reports it. A constructor parameter's default never feeds a required property.
// A constructor marked [SetsRequiredMembers] promises to set them, so nothing is enforced. A required public field is a type-shape error (UnmappableTypeTests, T17).
// The cases follow the design note: T15 (enforcement), T16 ([SetsRequiredMembers]); the by-name detection is checked with attributes that are not the framework's own.
[ExcludeFromCodeCoverage]
public static class RequiredMemberTests
{
	// ---- the models ----

	public sealed class RequiredPost
	{
		public required int Id { get; set; }
		public required string Title { get; set; }
		public string? Body { get; set; }
	}

	public sealed class RequiredInit
	{
		public required int Id { get; init; }
		public required string Title { get; init; }
	}

	// Two required properties whose names a column reaches only through the case and underscore tiers.
	public sealed class RequiredCased
	{
		public required int PostId { get; set; }
		public required string PostTitle { get; set; }
	}

	public class RequiredBase { public required int Id { get; set; } }

	public sealed class RequiredDerived : RequiredBase { public string? Title { get; set; } }

	public struct RequiredStruct
	{
		public required int Id { get; set; }
		public string? Title { get; set; }
	}

	// Positional parameters (phase C) and required properties (phase D) in one plan.
	public sealed record RecordMixed(int Id, string Title)
	{
		public required string Body { get; init; }
		public int Views { get; set; }
	}

	// The property is required and a constructor parameter of the same name has a default: the default must not satisfy it.
	public sealed record Defaulted(int Id, string Title = "fallback")
	{
		public required string Title { get; init; } = Title;
	}

	public sealed class DefaultedClass(string title = "fallback")
	{
		public required string Title { get; init; } = title;
	}

	[method: SetsRequiredMembers]
	public sealed class SetsViaParameterless()
	{
		public required int Id { get; set; } = -1;
		public required string Title { get; set; } = "ctor";
	}

	[method: SetsRequiredMembers]
	public sealed class SetsViaConstructor(int id)
	{
		public required int Id { get; init; } = id;
		public required string Title { get; init; } = "ctor";
	}

	// A required field is a type-shape error, unless the constructor sets the required members itself.
	[method: SetsRequiredMembers]
	// A required field is a type-shape error, unless the constructor sets the required members itself.
	public sealed class SetsRequiredField()
	{
		public required int Id = 7;
		public string? Title { get; set; }
	}

	static string Missing(string type, string property, string propertyType, string column)
		=> $"No column feeds the required property '{property}' ({propertyType}) of '{type}'; expected a column named '{column}'.";

	static readonly ImmutableArray<ColumnDef> IdTitle = [("Id", typeof(int)), ("Title", typeof(string))];
	static readonly ImmutableArray<ColumnDef> IdOnly = [("Id", typeof(int))];

	static ValueMappingException AssertMissing<T>(Route route, ImmutableArray<ColumnDef> columns, object?[] row, string expectedMessage, RowMap<T>? map = null)
		=> Assert.Throws<ValueMappingException>(() => Read(route, columns, row, map)).Verify(expectedMessage);

	static ValueMappingException Verify(this ValueMappingException ex, string message)
	{
		Assert.Equal(message, ex.Message);
		Assert.Null(ex.InnerException);
		return ex;
	}

	// ---- T15: a required property is mapped when a column feeds it ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ARequiredProperty_IsMapped_WhenAColumnFeedsIt(Route route)
	{
		RequiredPost post = Assert.Single(Read<RequiredPost>(route, [("Body", typeof(string)), ("Title", typeof(string)), ("Id", typeof(int))], ["b", "t", 7]));
		Assert.Equal((7, "t", "b"), (post.Id, post.Title, post.Body));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ARequiredInitProperty_IsSetThroughItsSetter(Route route)
	{
		RequiredInit item = Assert.Single(Read<RequiredInit>(route, IdTitle, [3, "three"]));
		Assert.Equal((3, "three"), (item.Id, item.Title));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void TheCaseAndUnderscoreTiers_FeedRequiredProperties(Route route)
	{
		RequiredCased ignoringCase = Assert.Single(Read<RequiredCased>(route, [("POSTID", typeof(int)), ("posttitle", typeof(string))], [1, "one"]));
		Assert.Equal((1, "one"), (ignoringCase.PostId, ignoringCase.PostTitle));

		RequiredCased underscored = Assert.Single(Read<RequiredCased>(route, [("post_id", typeof(int)), ("Post_Title", typeof(string))], [2, "two"]));
		Assert.Equal((2, "two"), (underscored.PostId, underscored.PostTitle));
	}

	static readonly RowMap<RequiredPost> PostMap = [new(nameof(RequiredPost.Id), "post_id"), new(nameof(RequiredPost.Title), "post_title")];

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APair_FeedsARequiredProperty_FromItsColumn(Route route)
	{
		RequiredPost post = Assert.Single(Read(route, [("post_title", typeof(string)), ("post_id", typeof(int)), ("Body", typeof(string))], ["t", 5, "b"], PostMap));
		Assert.Equal((5, "t", "b"), (post.Id, post.Title, post.Body));
	}

	[Fact]
	public static void ARequiredPropertyOfABaseType_IsEnforced()
	{
		Assert.Equal(1, Assert.Single(Read<RequiredDerived>(Route.Typed, IdTitle, [1, "t"])).Id);
		AssertMissing<RequiredDerived>(Route.Typed, [("Title", typeof(string))], ["t"], Missing("RequiredDerived", "Id", "Int32", "Id"));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ARequiredPropertyOfAStruct_IsMappedAndEnforced(Route route)
	{
		RequiredStruct item = Assert.Single(Read<RequiredStruct>(route, IdTitle, [4, "four"]));
		Assert.Equal((4, "four"), (item.Id, item.Title));
		AssertMissing<RequiredStruct>(route, [("Title", typeof(string))], ["four"], Missing("RequiredStruct", "Id", "Int32", "Id"));
	}

	// ---- T15: a missing column is reported when the plan for the shape is built, with the exact message ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ARequiredProperty_WithNoColumn_Throws_WithTheExactMessage(Route route)
	{
		ValueMappingException ex = AssertMissing<RequiredPost>(route, IdOnly, [1], Missing("RequiredPost", "Title", "String", "Title"));
		Assert.Equal("Title", ex.ColumnName); // the column looked for
		Assert.Equal("Title", ex.PropertyName);
		Assert.Equal(typeof(string), ex.TargetType);

		ex = AssertMissing<RequiredPost>(route, [("Title", typeof(string))], ["t"], Missing("RequiredPost", "Id", "Int32", "Id"));
		Assert.Equal(("Id", "Id", typeof(int)), (ex.ColumnName, ex.PropertyName, ex.TargetType));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void WhenNoColumnFeedsAnyOfThem_ARequiredPropertyIsStillNamed(Route route)
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read<RequiredPost>(route, [("other", typeof(string))], ["x"]));
		Assert.StartsWith("No column feeds the required property '", ex.Message, StringComparison.Ordinal);
		Assert.Null(ex.InnerException);
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void TheMessage_NamesTheColumnOfThePair_AndAPairIsExact(Route route)
	{
		// The pair says post_title: a column called Title does not feed it, so the column looked for is the pair's.
		ValueMappingException ex = AssertMissing(route, [("post_id", typeof(int)), ("Title", typeof(string))], [1, "t"], Missing("RequiredPost", "Title", "String", "post_title"), PostMap);
		Assert.Equal("post_title", ex.ColumnName);

		AssertMissing(route, [("Title", typeof(string)), ("post_title", typeof(string))], ["a", "b"], Missing("RequiredPost", "Id", "Int32", "post_id"), PostMap);
	}

	[Fact]
	public static void TheCheckIsPerResultShape_AFailureIsNotRemembered_AndAGoodShapeKeepsWorking()
	{
		var map = new RowMap<RequiredPost>();

		ImmutableArray<ColumnDef> good = IdTitle;
		ImmutableArray<ColumnDef> bad = IdOnly;
		for (int i = 0; i < 3; i++)
		{
			Assert.Equal("t", Assert.Single(Read(Route.Typed, good, [1, "t"], map)).Title);
			Assert.Equal(Missing("RequiredPost", "Title", "String", "Title"), Assert.Throws<ValueMappingException>(() => Read(Route.Typed, bad, [1], map)).Message);
			Assert.Equal("u", Assert.Single(Read(Route.Boxed, good, [1, "u"], map)).Title);
			Assert.Equal(Missing("RequiredPost", "Title", "String", "Title"), Assert.Throws<ValueMappingException>(() => Read(Route.Buffered, bad, [1], map)).Message);
		}
	}

	// ---- T15: an empty result never reports a missing column: no row, so no plan ----

	[Fact]
	public static async Task AnEmptyResult_NeverReportsAMissingColumn_OnTheReaderRoutes()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		var command = new FakeExecuteReader(["Id"]);

		Assert.Empty(command.Map<RequiredPost>().ToList());
		Assert.Empty(command.Map<RequiredPost>().ToArray());
		Assert.Null(command.Map<RequiredPost>().FirstOrDefault());
		Assert.Empty(await command.Map<RequiredPost>().ToListAsync(ct));
		Assert.Null(await command.Map<RequiredPost>().FirstOrDefaultAsync(ct));
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Empty(command.Results<RequiredPost>());
		#pragma warning restore CS0618

		Assert.Empty(FakeReader.Create(["Id"]).Results<RequiredPost>());
		Assert.Empty(FakeReader.Create(["Id"]).ResultsBuffered<RequiredPost>());
		Assert.Empty(TypedReader.Create(IdOnly).Results<RequiredPost>());
		Assert.Empty(await TypedReader.Create(IdOnly).ResultsBufferedAsync<RequiredPost>(ct));

		var streamed = new List<RequiredPost>();
		await foreach (RequiredPost post in TypedReader.Create(IdOnly).ResultsAsync<RequiredPost>(cancellationToken: ct)) streamed.Add(post);
		Assert.Empty(streamed);

		Assert.True(command.ExecuteCount > 0); // the queries ran: nothing was refused at the call
	}

	// The buffered routes (and the pipelines, below) know their columns before any row is mapped, and build the plan when the buffer is handed over, so there an empty result reports it too:
	// this pins what they do today (a later change may build the plan with the first row).
	[Fact]
	public static void TheBufferedRoutes_BuildThePlanWhenTheBufferIsHandedOver_SoEvenAnEmptyOneReportsAMissingColumn()
	{
		string message = Missing("RequiredPost", "Title", "String", "Title");
		var result = new QueryResult<Queue<object[]>>([0], ["Id"], new Queue<object[]>());
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Equal(message, Assert.Throws<ValueMappingException>(() => result.DequeueAs<RequiredPost>()).Message);
		#pragma warning restore CS0618

		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		Assert.Equal(message, Assert.Throws<ValueMappingException>(() => table.To<RequiredPost>()).Message);

		// With every column present an empty buffer maps to nothing.
		var complete = new QueryResult<Queue<object[]>>([0, 1], ["Id", "Title"], new Queue<object[]>());
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Empty(complete.DequeueAs<RequiredPost>());
		#pragma warning restore CS0618
	}

	// ---- T15: a null column for a required property in a pair cannot be ignored ----

	[Fact]
	public static void ANullPairColumn_OnARequiredProperty_Throws_AtMapCreation()
	{
		const string message = "The property 'Title' is required, so it cannot be mapped to null. Map it to a column.";

		ArgumentException ex = Assert.Throws<ArgumentException>(() => new RowMap<RequiredPost>(new KeyValuePair<string, string?>(nameof(RequiredPost.Title), null)));
		Assert.Equal(message + " (Parameter 'mappings')", ex.Message);
		Assert.Equal("mappings", ex.ParamName);

		Assert.Equal(message + " (Parameter 'mappings')", Assert.Throws<ArgumentException>(() => new RowMap<RequiredPost>([new("Id", "id"), new("Title", null)])).Message);
		Assert.Equal(message + " (Parameter 'mappings')", Assert.Throws<ArgumentException>(() => RowMap<RequiredPost>.From([new("Title", null)])).Message);
		Assert.Throws<ArgumentException>(() => { RowMap<RequiredPost> map = [new("Title", null)]; });
		Assert.Throws<ArgumentException>(() => new RowMap<RequiredPost>(new List<KeyValuePair<string, string?>> { new("Title", null) }));
		Assert.Throws<ArgumentException>(() => new RowMap<RequiredPost>(ImmutableArray.Create(new KeyValuePair<string, string?>("Title", null))));

		// A property that is not required can still be ignored, a required one can be named, and a required one is allowed by its own column.
		RowMap<RequiredPost> ok = [new("Body", null), new("Id", "post_id"), new("Title", "Title")];
		Assert.Equal(3, ok.Count());
	}

	[Fact]
	public static void ANullPairColumn_OnARequiredProperty_ThatIsAConstructorParameterToo_Throws()
	{
		Assert.Throws<ArgumentException>(() => new RowMap<Defaulted>(new KeyValuePair<string, string?>("Title", null)));
		Assert.Throws<ArgumentException>(() => new RowMap<Defaulted>(new KeyValuePair<string, string?>("title", null))); // the parameter's own name, ignoring case
		Assert.Throws<ArgumentException>(() => new RowMap<DefaultedClass>(new KeyValuePair<string, string?>("title", null)));
		_ = new RowMap<Defaulted>(new KeyValuePair<string, string?>("Id", null)); // the positional parameter is not required: it may be ignored
	}

	// ---- T15: a constructor parameter's default does not satisfy a required property of the same name ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void AParameterDefault_DoesNotSatisfy_ARequiredProperty(Route route)
	{
		AssertMissing<Defaulted>(route, IdOnly, [1], Missing("Defaulted", "Title", "String", "Title"));
		Assert.Equal(Missing("DefaultedClass", "Title", "String", "Title"), Assert.Throws<ValueMappingException>(() => Read<DefaultedClass>(route, IdOnly, [1])).Message);

		// With a column the parameter (which feeds the property) receives it: the default is not used.
		Defaulted item = Assert.Single(Read<Defaulted>(route, IdTitle, [1, "from the column"]));
		Assert.Equal((1, "from the column"), (item.Id, item.Title));
		Assert.Equal("c", Assert.Single(Read<DefaultedClass>(route, [("title", typeof(string))], ["c"])).Title);
	}

	[Fact]
	public static void ThePairOfAParameterThatIsRequired_NamesTheColumnLookedFor()
	{
		RowMap<Defaulted> map = [new(nameof(Defaulted.Title), "heading")];
		Assert.Equal(Missing("Defaulted", "Title", "String", "heading"), Assert.Throws<ValueMappingException>(() => Read(Route.Typed, IdTitle, [1, "t"], map)).Message);
		Assert.Equal("h", Assert.Single(Read(Route.Typed, [("Id", typeof(int)), ("heading", typeof(string))], [1, "h"], map)).Title);
	}

	// ---- a record with positional parameters and required properties: phase C binding and phase D enforcement in one plan ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ARecordWithPositionalParametersAndRequiredExtras_BindsAndEnforcesInOnePlan(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Body", typeof(string)), ("Views", typeof(int)), ("Title", typeof(string)), ("Id", typeof(int))];
		RecordMixed item = Assert.Single(Read<RecordMixed>(route, columns, ["the body", 12, "the title", 9]));
		Assert.Equal((9, "the title", "the body", 12), (item.Id, item.Title, item.Body, item.Views));

		// A positional parameter with no column is its fallback, not an error; the required property is the one that needs its column.
		RecordMixed noId = Assert.Single(Read<RecordMixed>(route, [("Body", typeof(string)), ("Title", typeof(string))], ["b", "t"]));
		Assert.Equal((0, "t", "b", 0), (noId.Id, noId.Title, noId.Body, noId.Views));

		AssertMissing<RecordMixed>(route, IdTitle, [1, "t"], Missing("RecordMixed", "Body", "String", "Body"));
	}

	// ---- the async and object[] routes, and the pipelines ----

	[Fact]
	public static async Task Async_AndBufferedRoutes_MapARequiredType_AndReportAMissingColumn()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		object?[][] rows = [[1, "one"], [2, "two"]];
		string message = Missing("RequiredPost", "Title", "String", "Title");

		Assert.Equal([(1, "one"), (2, "two")], (await TypedReader.Create(IdTitle, rows).ResultsBufferedAsync<RequiredPost>(ct)).Select(p => (p.Id, p.Title)));
		var streamed = new List<(int, string)>();
		await foreach (RequiredPost post in TypedReader.Create(IdTitle, rows).ResultsAsync<RequiredPost>(cancellationToken: ct)) streamed.Add((post.Id, post.Title));
		Assert.Equal([(1, "one"), (2, "two")], streamed);

		Assert.Equal(message, (await Assert.ThrowsAsync<ValueMappingException>(async () => await TypedReader.Create(IdOnly, [1], [2]).ResultsBufferedAsync<RequiredPost>(ct))).Message);
		Assert.Equal(message, (await Assert.ThrowsAsync<ValueMappingException>(async () =>
		{
			await foreach (RequiredPost _ in TypedReader.Create(IdOnly, [1], [2]).ResultsAsync<RequiredPost>(cancellationToken: ct)) { }
		})).Message);

		Assert.Equal(message, (await Assert.ThrowsAsync<ValueMappingException>(async () => await new FakeExecuteReader(() => TypedReader.Create(IdOnly, [1])).Map<RequiredPost>().ToListAsync(ct))).Message);

		var result = new QueryResult<Queue<object[]>>([0, 1], ["Id", "Title"], new Queue<object[]>(rows!));
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Equal([1, 2], (await Task.FromResult(result).DequeueAs<RequiredPost>()).Select(p => p.Id));
		#pragma warning restore CS0618
		var one = new QueryResult<Queue<object[]>>([0], ["Id"], new Queue<object[]>([[1]]));
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Equal(message, (await Assert.ThrowsAsync<ValueMappingException>(async () => await new ValueTask<QueryResult<Queue<object[]>>>(one).DequeueAs<RequiredPost>())).Message);
		#pragma warning restore CS0618
	}

	[Fact]
	public static void TheObjectArrayRoutes_MapARequiredType_AndReportAMissingColumn()
	{
		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Title", typeof(string));
		table.Rows.Add(1, "one");
		table.Rows.Add(2, "two");
		Assert.Equal([(1, "one"), (2, "two")], table.To<RequiredPost>().Select(p => (p.Id, p.Title)));

		var withoutTitle = new DataTable();
		withoutTitle.Columns.Add("Id", typeof(int));
		withoutTitle.Rows.Add(1);
		Assert.Equal(Missing("RequiredPost", "Title", "String", "Title"), Assert.Throws<ValueMappingException>(() => withoutTitle.To<RequiredPost>()).Message);
	}

	// A table is only cleared (clearSourceTable) once the rows could be mapped: a missing required column fails the call and leaves the caller's rows where they were.
	[Fact]
	public static void ADataTable_IsNotCleared_WhenARequiredColumnIsMissing()
	{
		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		table.Rows.Add(1);
		table.Rows.Add(2);

		_ = Assert.Throws<ValueMappingException>(() => table.To<RequiredPost>(clearSourceTable: true));
		Assert.Equal(2, table.Rows.Count);

		table.Columns.Add("Title", typeof(string));
		foreach (DataRow row in table.Rows) row["Title"] = "t";
		Assert.Equal(2, table.To<RequiredPost>(clearSourceTable: true).Count());
		Assert.Equal(0, table.Rows.Count); // a mappable table is cleared, as ever
	}

	static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

	static async Task<List<T>> Drain<T>(ChannelReader<T> reader, CancellationToken ct)
	{
		var items = new List<T>();
		await foreach (T item in reader.ReadAllAsync(ct).ConfigureAwait(false)) items.Add(item);
		return items;
	}

	static async Task<List<T>> Received<T>(IReceivableSourceBlock<T> block, CancellationToken ct)
	{
		var items = new List<T>();
		while (await block.OutputAvailableAsync(ct).WaitAsync(Timeout, ct))
		{
			while (block.TryReceive(out T? item)) items.Add(item);
		}

		return items;
	}

	[Fact]
	public static async Task Channel_MapsARequiredType_AndReportsAMissingColumn()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		object?[][] rows = [[1, "one"], [2, "two"], [3, "three"]];
		string message = Missing("RequiredPost", "Title", "String", "Title");

		Assert.Equal([1, 2, 3], (await Drain(TypedReader.Create(IdTitle, rows).ToChannel<RequiredPost>(true, ct), ct).WaitAsync(Timeout, ct)).Select(p => p.Id));
		Assert.Equal([1, 2, 3], (await Drain(TypedReader.Create(IdTitle, rows).ToChannelAsync<RequiredPost>(true, ct), ct).WaitAsync(Timeout, ct)).Select(p => p.Id));
		Assert.Equal(
			[1, 2, 3],
			(await Drain(
				new FakeExecuteReader(() => TypedReader.Create(IdTitle, rows))
					.ToChannel<RequiredPost>(
						true,
						cancellationToken: TestContext.Current.CancellationToken),
				ct)
				.WaitAsync(Timeout, ct))
				.Select(p => p.Id));
		Assert.Equal([1, 2, 3], (await Drain(TypedReader.Create([("post_id", typeof(int)), ("post_title", typeof(string))], rows).ToChannel(true, PostMap, ct), ct).WaitAsync(Timeout, ct)).Select(p => p.Id));

		ValueMappingException ex = await Assert.ThrowsAsync<ValueMappingException>(() => Drain(TypedReader.Create(IdOnly, [1], [2]).ToChannel<RequiredPost>(true, ct), ct).WaitAsync(Timeout, ct));
		Assert.Equal(message, ex.Message);
		ex = await Assert.ThrowsAsync<ValueMappingException>(() => Drain(TypedReader.Create(IdOnly, [1], [2]).ToChannelAsync<RequiredPost>(true, ct), ct).WaitAsync(Timeout, ct));
		Assert.Equal(message, ex.Message);

		// The pipeline binds the columns of the reader before its first row, so it reports the missing column even when no row follows (pinned, as for the buffered routes).
		ex = await Assert.ThrowsAsync<ValueMappingException>(() => Drain(TypedReader.Create(IdOnly).ToChannel<RequiredPost>(true, ct), ct).WaitAsync(Timeout, ct));
		Assert.Equal(message, ex.Message);

		// With every column present, no rows is an empty channel.
		Assert.Empty(await Drain(TypedReader.Create(IdTitle).ToChannel<RequiredPost>(true, ct), ct).WaitAsync(Timeout, ct));
	}

	[Fact]
	public static async Task Dataflow_MapsARequiredType_AndReportsAMissingColumn()
	{
		CancellationToken ct = TestContext.Current.CancellationToken;
		object?[][] rows = [[1, "one"], [2, "two"], [3, "three"]];
		string message = Missing("RequiredPost", "Title", "String", "Title");

		var sync = new BufferBlock<RequiredPost>();
		Assert.Equal(3, TypedReader.Create(IdTitle, rows).ToTargetBlock(sync, true));
		Assert.Equal([1, 2, 3], (await Received(sync, ct)).Select(p => p.Id));

		var async = new BufferBlock<RequiredPost>();
		Assert.Equal(3, await TypedReader.Create(IdTitle, rows).ToTargetBlockAsync(async, true, ct).AsTask().WaitAsync(Timeout, ct));
		Assert.Equal([1, 2, 3], (await Received(async, ct)).Select(p => p.Id));

		Assert.Equal([1, 2, 3], (await Received(TypedReader.Create(IdTitle, rows).AsSourceBlock<RequiredPost>(), ct)).Select(p => p.Id));
		Assert.Equal([1, 2, 3], (await Received(TypedReader.Create(IdTitle, rows).AsSourceBlockAsync<RequiredPost>(cancellationToken: ct), ct)).Select(p => p.Id));

		var failing = new BufferBlock<RequiredPost>();
		ValueMappingException ex = Assert.Throws<ValueMappingException>(() => TypedReader.Create(IdOnly, [1], [2]).ToTargetBlock(failing, true));
		Assert.Equal(message, ex.Message);
		ex = await Assert.ThrowsAsync<ValueMappingException>(() => TypedReader.Create(IdOnly, [1], [2]).ToTargetBlockAsync(new BufferBlock<RequiredPost>(), true, ct).AsTask().WaitAsync(Timeout, ct));
		Assert.Equal(message, ex.Message);

		Assert.Equal(message, Assert.Throws<ValueMappingException>(() => TypedReader.Create(IdOnly, [1], [2]).AsSourceBlock<RequiredPost>()).Message);
		Assert.Equal(message, (await Assert.ThrowsAsync<ValueMappingException>(async () => await TypedReader.Create(IdOnly, [1], [2]).AsSourceBlockAsync<RequiredPost>(cancellationToken: ct).Completion)).Message);

		// The pipelines bind the columns of the reader before its first row, so they report the missing column even when no row follows (pinned, as for the buffered routes).
		Assert.Equal(message, Assert.Throws<ValueMappingException>(() => TypedReader.Create(IdOnly).ToTargetBlock(new BufferBlock<RequiredPost>(), true)).Message);

		// With every column present, no rows is a target that gets nothing.
		Assert.Equal(0, TypedReader.Create(IdTitle).ToTargetBlock(new BufferBlock<RequiredPost>(), true));
	}

	// ---- T16: [SetsRequiredMembers] on the chosen constructor: nothing is enforced, the properties are ordinary ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void WhenTheParameterlessConstructorSetsRequiredMembers_NothingIsEnforced(Route route)
	{
		// No column at all: the constructor's own values stay.
		SetsViaParameterless none = Assert.Single(Read<SetsViaParameterless>(route, [("other", typeof(string))], ["x"]));
		Assert.Equal((-1, "ctor"), (none.Id, none.Title));

		// A column feeds the property as for any other property.
		SetsViaParameterless some = Assert.Single(Read<SetsViaParameterless>(route, [("Title", typeof(string))], ["from the column"]));
		Assert.Equal((-1, "from the column"), (some.Id, some.Title));

		SetsViaParameterless all = Assert.Single(Read<SetsViaParameterless>(route, IdTitle, [4, "four"]));
		Assert.Equal((4, "four"), (all.Id, all.Title));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void WhenTheChosenConstructorSetsRequiredMembers_NothingIsEnforced_AndColumnsStillFeedThem(Route route)
	{
		// The constructor takes `id`; its property Id is the same slot, fed once through it. Title is an ordinary init property.
		SetsViaConstructor none = Assert.Single(Read<SetsViaConstructor>(route, [("other", typeof(string))], ["x"]));
		Assert.Equal((0, "ctor"), (none.Id, none.Title)); // no column: the parameter's fallback, then the constructor's own values

		SetsViaConstructor all = Assert.Single(Read<SetsViaConstructor>(route, IdTitle, [5, "five"]));
		Assert.Equal((5, "five"), (all.Id, all.Title));

		SetsViaConstructor idOnly = Assert.Single(Read<SetsViaConstructor>(route, IdOnly, [6]));
		Assert.Equal((6, "ctor"), (idOnly.Id, idOnly.Title));
	}

	[Fact]
	public static void WhenTheConstructorSetsRequiredMembers_ANullPairColumnIsAllowed_LikeForAnyProperty()
	{
		RowMap<SetsViaParameterless> map = [new("Title", null), new("Id", "key")];
		SetsViaParameterless item = Assert.Single(Read(Route.Typed, [("Title", typeof(string)), ("key", typeof(int))], ["ignored", 8], map));
		Assert.Equal((8, "ctor"), (item.Id, item.Title));

		_ = new RowMap<SetsViaConstructor>(new KeyValuePair<string, string?>("Title", null));
	}

	[Fact]
	public static void WhenTheConstructorSetsRequiredMembers_ARequiredFieldIsNotAnError()
	{
		// Its constructor sets the field: the mapper never needs to, so the type maps (the field itself is not mapped).
		SetsRequiredField item = Assert.Single(Read<SetsRequiredField>(Route.Typed, [("Id", typeof(int)), ("Title", typeof(string))], [99, "t"]));
		Assert.Equal((7, "t"), (item.Id, item.Title));
	}

	// ---- the attributes are matched by FULL NAME: a copy declared by a consumer counts, and the library names no type netstandard lacks ----
	// The test project is net10.0, whose framework has both attributes, so these models are built at run time with attributes of the same full names that are NOT the framework's types
	// (declared in a dynamic assembly), the way a consumer's polyfill on netstandard2.0 would be.

	const string RequiredName = "System.Runtime.CompilerServices.RequiredMemberAttribute";
	const string SetsName = "System.Diagnostics.CodeAnalysis.SetsRequiredMembersAttribute";

	static Type Polyfilled(string requiredAttribute, string? setsAttribute = null)
	{
		var module = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("Polyfilled" + Guid.NewGuid().ToString("N")), AssemblyBuilderAccess.Run).DefineDynamicModule("Polyfilled");
		ConstructorInfo required = DefineAttribute(module, requiredAttribute);
		ConstructorInfo? sets = setsAttribute is null ? null : DefineAttribute(module, setsAttribute);

		TypeBuilder model = module.DefineType("PolyfilledModel", TypeAttributes.Public | TypeAttributes.Sealed);
		ConstructorBuilder constructor = model.DefineDefaultConstructor(MethodAttributes.Public);
		if (sets is not null) constructor.SetCustomAttribute(new CustomAttributeBuilder(sets, []));

		AddProperty(model, "Id", typeof(int), required);
		AddProperty(model, "Title", typeof(string), required);
		return model.CreateType();
	}

	static ConstructorInfo DefineAttribute(ModuleBuilder module, string fullName)
	{
		TypeBuilder attribute = module.DefineType(fullName, TypeAttributes.Public | TypeAttributes.Sealed, typeof(Attribute));
		attribute.DefineDefaultConstructor(MethodAttributes.Public);
		return attribute.CreateType().GetConstructor(Type.EmptyTypes)!;
	}

	static void AddProperty(TypeBuilder type, string name, Type propertyType, ConstructorInfo requiredAttribute)
	{
		FieldBuilder field = type.DefineField("_" + name, propertyType, FieldAttributes.Private);
		PropertyBuilder property = type.DefineProperty(name, PropertyAttributes.None, propertyType, null);
		const MethodAttributes accessor = MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig;

		MethodBuilder get = type.DefineMethod("get_" + name, accessor, propertyType, Type.EmptyTypes);
		ILGenerator il = get.GetILGenerator();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, field);
		il.Emit(OpCodes.Ret);

		MethodBuilder set = type.DefineMethod("set_" + name, accessor, null, [propertyType]);
		il = set.GetILGenerator();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Stfld, field);
		il.Emit(OpCodes.Ret);

		property.SetGetMethod(get);
		property.SetSetMethod(set);
		property.SetCustomAttribute(new CustomAttributeBuilder(requiredAttribute, []));
	}

	// The message of the ValueMappingException the buffered route raises for a row with only the Id column, or null when the row maps.
	internal static string? MissingTitle<T>()
	{
		var result = new QueryResult<Queue<object[]>>([0], ["Id"], new Queue<object[]>([[1]]));
		try
		{
			#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
			Assert.Single(result.DequeueAs<T>());
			#pragma warning restore CS0618
			return null;
		}
		catch (ValueMappingException ex)
		{
			return ex.Message;
		}
	}

	static string? MissingTitleOf(Type model)
		=> (string?)typeof(RequiredMemberTests).GetMethod(nameof(MissingTitle), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(model).Invoke(null, null);

	[Fact]
	public static void TheRequiredAttribute_IsMatchedByFullName_NotByTheFrameworkType()
	{
		Type model = Polyfilled(RequiredName);
		PropertyInfo id = model.GetProperty("Id")!;
		Assert.Equal(RequiredName, Assert.Single(id.CustomAttributes).AttributeType.FullName);
		Assert.NotEqual(typeof(System.Runtime.CompilerServices.RequiredMemberAttribute).Assembly, id.CustomAttributes.Single().AttributeType.Assembly); // a copy, not the framework's type

		Assert.Equal(Missing("PolyfilledModel", "Title", "String", "Title"), MissingTitleOf(model)); // enforced, by name

		// The same names with another namespace or another simple name are not the attribute.
		Assert.Null(MissingTitleOf(Polyfilled("Other.Namespace.RequiredMemberAttribute")));
		Assert.Null(MissingTitleOf(Polyfilled("System.Runtime.CompilerServices.RequiredMember")));
	}

	[Fact]
	public static void TheSetsRequiredMembersAttribute_IsMatchedByFullName_NotByTheFrameworkType()
	{
		// A constructor that carries a copy named like the framework's: nothing is enforced.
		Type model = Polyfilled(RequiredName, SetsName);
		Assert.NotEqual(typeof(SetsRequiredMembersAttribute).Assembly, model.GetConstructor(Type.EmptyTypes)!.CustomAttributes.Single().AttributeType.Assembly);
		Assert.Null(MissingTitleOf(model));

		// Another namespace: not the attribute, so the required properties are still enforced.
		Assert.Equal(Missing("PolyfilledModel", "Title", "String", "Title"), MissingTitleOf(Polyfilled(RequiredName, "Other.Namespace.SetsRequiredMembersAttribute")));
	}
}
