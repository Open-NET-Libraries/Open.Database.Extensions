using Open.Database.Extensions.Core;
using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Characterization tests: they pin what the mapping core (RowMap<T> and the row readers it compiles) does TODAY, so a refactor of its internals can be
// proved to change nothing. Every expectation below was observed on the code as it stands; none of them is a design statement.
// The one rule that did change after they were written, phase E (a NULL never overwrites a member the C# type declares non-nullable), is pinned in the NULL tests below as it is now;
// NullRuleMatrixTests holds the full matrix.
[ExcludeFromCodeCoverage]
public static class MappingCharacterizationTests
{
	// The two routes a row takes to a map: a live reader, and rows already buffered as object[] (the Retrieve/QueryResult route).
	static List<T> Read<T>(bool buffered, ImmutableArray<ColumnDef> columns, object?[] row, RowMap<T>? map = null)
		where T : new()
	{
		if (!buffered)
		{
			var command = new FakeExecuteReader(() => TypedReader.Create(columns, [row]));
			return (map is null ? command.Map<T>() : command.Map(map)).ToList();
		}

		var result = new QueryResult<Queue<object?[]>>([.. Enumerable.Range(0, columns.Length)], [.. columns.Select(c => c.Name)], new Queue<object?[]>([row]));
		return result.DequeueAs(map).ToList();
	}

	static ImmutableArray<ColumnDef> Columns(params string[] names) => [.. names.Select(n => new ColumnDef(n, typeof(object)))];

	// ---- T6: what a NULL does to a property ----

	public enum Shade { None, Red, Green }

	public sealed class NullProbe
	{
		public int Count { get; set; } = 7;                       // non-nullable value type
		public Shade Level { get; set; } = Shade.Green;           // non-nullable value type (an enum counts)
		public int? Maybe { get; set; } = 5;                      // Nullable<T>
		public string Title { get; set; } = "init";               // reference type declared non-nullable, with an initializer
#pragma warning disable CS8618 // Deliberately no initializer: a non-nullable reference with none.
		public string Bare { get; set; }
#pragma warning restore CS8618
		public string? Note { get; set; } = "init";               // reference type declared nullable, with an initializer
	}

	static readonly string[] NullProbeNames = [nameof(NullProbe.Count), nameof(NullProbe.Level), nameof(NullProbe.Maybe), nameof(NullProbe.Title), nameof(NullProbe.Bare), nameof(NullProbe.Note)];

	// Typed columns take the provider's typed getters; boxed (object) columns take the general conversion path.
	static readonly ImmutableArray<ColumnDef> TypedProbeColumns =
	[
		("Count", typeof(int)), ("Level", typeof(int)), ("Maybe", typeof(int)), ("Title", typeof(string)), ("Bare", typeof(string)), ("Note", typeof(string)),
	];

	static readonly ImmutableArray<ColumnDef> BoxedProbeColumns = Columns(NullProbeNames);

	// A row of valid cells, except that the cell of `nullColumn` is NULL (all of them when `nullColumn` is "*").
	static object?[] ProbeRow(string? nullColumn)
		=> [.. NullProbeNames.Select(n => nullColumn == "*" || n == nullColumn ? null : n switch
		{
			"Count" => (object)1,
			"Level" => (int)Shade.Red,
			"Maybe" => 2,
			"Title" => "t",
			"Bare" => "b",
			_ => "n",
		})];

	// Map<T>() with no map, RowMap<T>.Default, and a declared map whose one pair names a property by its own name.
	public static TheoryData<bool, bool, int> ProbeCombos
	{
		get
		{
			var data = new TheoryData<bool, bool, int>();
			foreach (bool boxed in (bool[])[false, true])
				foreach (bool buffered in (bool[])[false, true])
					for (int map = 0; map < 3; map++)
						data.Add(boxed, buffered, map);

			return data;
		}
	}

	static readonly RowMap<NullProbe> DeclaredProbeMap = new([new(nameof(NullProbe.Title), "Title")]);

	static RowMap<NullProbe>? ProbeMap(int kind) => kind switch { 0 => null, 1 => RowMap<NullProbe>.Default, _ => DeclaredProbeMap };

	static NullProbe ReadProbe(bool boxed, bool buffered, int map, string? nullColumn)
		=> Assert.Single(Read(buffered, boxed ? BoxedProbeColumns : TypedProbeColumns, ProbeRow(nullColumn), ProbeMap(map)));

	[Theory]
	[MemberData(nameof(ProbeCombos))]
	public static void ARealValue_InEveryColumn_Maps(bool boxed, bool buffered, int map)
	{
		NullProbe item = ReadProbe(boxed, buffered, map, null);
		Assert.Equal(1, item.Count);
		Assert.Equal(Shade.Red, item.Level);
		Assert.Equal(2, item.Maybe);
		Assert.Equal("t", item.Title);
		Assert.Equal("b", item.Bare);
		Assert.Equal("n", item.Note);
	}

	[Theory]
	[MemberData(nameof(ProbeCombos))]
	public static void NullInEveryColumn_KeepsWhatTheTypeDeclaresNonNullable(bool boxed, bool buffered, int map)
	{
		NullProbe item = ReadProbe(boxed, buffered, map, "*");

		// A non-nullable value type keeps what new T() gave it (its initializer): the rule that stays.
		Assert.Equal(7, item.Count);
		Assert.Equal(Shade.Green, item.Level);

		// Nullable<T> and a reference declared nullable receive null, whatever their initializer: the rule that stays.
		Assert.Null(item.Maybe);
		Assert.Null(item.Note);

		// Phase E: a reference declared NON-nullable (nullable annotations on) keeps its initializer too, as a value type does ("init", where it was null before).
		// Without an initializer nothing assigned it, so it stays null.
		Assert.Equal("init", item.Title);
		Assert.Null(item.Bare);
	}

	[Theory]
	[MemberData(nameof(ProbeCombos))]
	public static void NullInOneColumn_OnlyThatPropertyIsAffected(bool boxed, bool buffered, int map)
	{
		foreach (string column in NullProbeNames)
		{
			NullProbe item = ReadProbe(boxed, buffered, map, column);
			NullProbe expected = new() { Count = 1, Level = Shade.Red, Maybe = 2, Title = "t", Bare = "b", Note = "n" };
			switch (column)
			{
				case "Count": expected.Count = 7; break;
				case "Level": expected.Level = Shade.Green; break;
				case "Maybe": expected.Maybe = null; break;
				case "Title": expected.Title = "init"; break; // phase E: a NULL does not overwrite a non-nullable reference (it was null before)
				case "Bare": expected.Bare = null!; break;
				default: expected.Note = null; break;
			}

			Assert.Equal((expected.Count, expected.Level, expected.Maybe, expected.Title, expected.Bare, expected.Note), (item.Count, item.Level, item.Maybe, item.Title, item.Bare, item.Note));
		}
	}

	[Fact]
	public static void AColumnThatIsNotThere_LeavesTheInitializer_ForEveryKindOfProperty()
	{
		// No column at all (the property is simply not fed): the constructor/initializer value stays, nullable or not.
		NullProbe item = Assert.Single(Read<NullProbe>(false, Columns("Count"), [3]));
		Assert.Equal(3, item.Count);
		Assert.Equal(Shade.Green, item.Level);
		Assert.Equal(5, item.Maybe);
		Assert.Equal("init", item.Title);
		Assert.Null(item.Bare);
		Assert.Equal("init", item.Note);
	}

	// ---- T8: pairs and validation for properties ----

	public sealed class Post
	{
		public int Id { get; set; }
		public string? Title { get; set; }
		public string? Body { get; set; } = "init";
	}

	static readonly RowMap<Post> TitleMap = new([new(nameof(Post.Title), "post_title")]);

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void APair_FeedsItsProperty_AndTheOthersMapByName(bool buffered)
	{
		Post post = Assert.Single(Read(buffered, Columns("Id", "post_title", "BODY"), [1, "t", "b"], TitleMap));
		Assert.Equal((1, "t", "b"), (post.Id, post.Title, post.Body));
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void APropertyWithAPair_ReadsOnlyItsColumn_NotItsOwnName(bool buffered)
	{
		// The pair says post_title; a column called Title is not the property's column any more, so it feeds nothing.
		Post post = Assert.Single(Read(buffered, Columns("Id", "Title"), [1, "ignored"], TitleMap));
		Assert.Equal(1, post.Id);
		Assert.Null(post.Title);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void ANullColumn_MeansDoNotMap_SoTheInitializerStays(bool buffered)
	{
		var ignoreBody = new RowMap<Post>([new(nameof(Post.Body), null)]);
		Post post = Assert.Single(Read(buffered, Columns("Id", "Body"), [1, "from the column"], ignoreBody));
		Assert.Equal(1, post.Id);
		Assert.Equal("init", post.Body); // not null, not the column: the property was never touched
	}

	[Fact]
	public static void TheColumnSideOfAPair_IgnoresCase()
	{
		var upper = new RowMap<Post>([new(nameof(Post.Title), "POST_TITLE")]);
		Assert.Equal("t", Assert.Single(Read(false, Columns("post_title"), ["t"], upper)).Title);
	}

	[Fact]
	public static void APairNamedTwice_ThrowsArgumentException_WithTheMessagePinned()
	{
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Title), "a"), new(nameof(Post.Title), "b")]));
		Assert.Equal("mappings", ex.ParamName);
		Assert.StartsWith("The property 'Title' is mapped more than once.", ex.Message);
	}

	[Fact]
	public static void AnUnknownName_ThrowsArgumentException_EvenWithANullColumn_WithTheMessagePinned()
	{
		foreach (string? column in (string?[])["x", null])
		{
			var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new("Nope", column)])); // "Nope" is deliberately not a property
			Assert.Equal("mappings", ex.ParamName);
			Assert.StartsWith($"'{typeof(Post)}' has no settable public property named 'Nope'.", ex.Message);
		}

		// The property side is exact: "title" is not Title.
		Assert.Throws<ArgumentException>(() => new RowMap<Post>([new("title", "x")]));
	}

	[Fact]
	public static void ANullPropertyName_AndAnEmptyColumnName_Throw()
	{
		var noName = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(null!, "x")]));
		Assert.StartsWith("A mapping has no property name.", noName.Message);

		var empty = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Title), "")]));
		Assert.StartsWith("The property 'Title' is mapped to an empty column name. Use null to ignore it.", empty.Message);
	}

	[Fact]
	public static void TwoPairsOnOneColumn_Throw_NamingBothPropertiesAndTheColumn()
	{
		// Columns are compared ignoring case; the message names the earlier property, the later one and the later one's spelling of the column.
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Body), "X"), new(nameof(Post.Title), "x")]));
		Assert.Equal("mappings", ex.ParamName);
		Assert.StartsWith("The properties 'Title' and 'Body' both read column 'X'. Map one of them to another column, or to null to ignore it.", ex.Message);
	}

	[Fact]
	public static void APairOntoTheColumnOfAnUnmentionedProperty_Throws()
	{
		// Id is not mentioned, so it still reads the column "Id"; Title would too.
		var ex = Assert.Throws<ArgumentException>(() => new RowMap<Post>([new(nameof(Post.Title), "Id")]));
		Assert.StartsWith("The properties 'Id' and 'Title' both read column 'Id'.", ex.Message);

		// Mapping the other one away (to null, or to its own other column) makes room.
		_ = new RowMap<Post>([new(nameof(Post.Title), "Id"), new(nameof(Post.Id), null)]);
		_ = new RowMap<Post>([new(nameof(Post.Title), "Id"), new(nameof(Post.Id), "post_id")]);
	}

	[Fact]
	public static void SwappingTwoColumns_IsFine_AndPairsAreOrderIndependent()
	{
		var swap = new RowMap<Post>([new(nameof(Post.Title), "Body"), new(nameof(Post.Body), "Title")]);
		var swapReversed = new RowMap<Post>([new(nameof(Post.Body), "Title"), new(nameof(Post.Title), "Body")]);
		foreach (RowMap<Post> map in (RowMap<Post>[])[swap, swapReversed])
		{
			Post post = Assert.Single(Read(false, Columns("Title", "Body"), ["first", "second"], map));
			Assert.Equal(("second", "first"), (post.Title, post.Body));
		}
	}

	[Fact]
	public static void AMapEnumeratesThePairsItWasDeclaredWith()
	{
		IEnumerable<KeyValuePair<string, string?>> pairs = new RowMap<Post>([new(nameof(Post.Body), null), new(nameof(Post.Title), "post_title")]);
		Assert.Equal<KeyValuePair<string, string?>>([new(nameof(Post.Title), "post_title"), new(nameof(Post.Body), null)], pairs); // declaration order of the properties, not of the pairs
		Assert.Empty((IEnumerable<KeyValuePair<string, string?>>)RowMap<Post>.Default);
	}

	// ---- T9: case and underscore tiers; an exact match wins in either column order ----

	public sealed class Tiers
	{
		public int PostId { get; set; }
		public string? PostTitle { get; set; }
	}

	[Theory]
	[InlineData(false, "PostId", "PostTitle")]
	[InlineData(false, "postid", "POSTTITLE")]
	[InlineData(false, "post_id", "Post_Title")]
	[InlineData(false, "_post_id_", "p_o_s_t_t_i_t_l_e")]
	[InlineData(true, "PostId", "PostTitle")]
	[InlineData(true, "postid", "POSTTITLE")]
	[InlineData(true, "post_id", "Post_Title")]
	[InlineData(true, "_post_id_", "p_o_s_t_t_i_t_l_e")]
	public static void EachTierFeedsTheProperty(bool buffered, string idColumn, string titleColumn)
	{
		Tiers item = Assert.Single(Read<Tiers>(buffered, Columns(idColumn, titleColumn), [5, "t"]));
		Assert.Equal((5, "t"), (item.PostId, item.PostTitle));
	}

	// The winner is the column of that name; the loser feeds nothing. Each cell holds 10 x (its position + 1), so the value says which column fed.
	[Theory]
	[InlineData("postid", "PostId", "PostId")]       // exact beats ignoring case
	[InlineData("PostId", "postid", "PostId")]
	[InlineData("post_id", "PostId", "PostId")]      // exact beats underscores
	[InlineData("PostId", "post_id", "PostId")]
	[InlineData("post_id", "postid", "postid")]      // ignoring case beats underscores
	[InlineData("postid", "post_id", "postid")]
	[InlineData("post_id", "POST_ID", "post_id")]    // the same tier: the first column wins
	[InlineData("POST_ID", "post_id", "POST_ID")]
	public static void ABetterTierWins_WhateverTheColumnOrder(string first, string second, string winner)
	{
		foreach (bool buffered in (bool[])[false, true])
		{
			Tiers item = Assert.Single(Read<Tiers>(buffered, Columns(first, second), [10, 20]));
			Assert.Equal(winner == first ? 10 : 20, item.PostId);
			Assert.Null(item.PostTitle);
		}
	}

	[Fact]
	public static void AColumnFeedsOneProperty_AndAFilledPropertyIsNeverTakenByALowerTier()
	{
		// PostId is filled by the exact column; "post_id" would be its underscore match but the property is taken, and PostTitle is a different property.
		Tiers item = Assert.Single(Read<Tiers>(false, Columns("post_id", "PostId", "PostTitle"), [10, 20, "t"]));
		Assert.Equal((20, "t"), (item.PostId, item.PostTitle));
	}

	[Fact]
	public static void APropertyWithAPair_NeverUsesTheUnderscoreTier_EvenForItsOwnName()
	{
		Assert.Equal(5, Assert.Single(Read<Tiers>(false, Columns("post_id"), [5])).PostId); // no pair: the underscore tier feeds it

		var pinned = new RowMap<Tiers>([new(nameof(Tiers.PostId), "PostId")]); // a pair, though to its own name: exact and final
		Assert.Equal(0, Assert.Single(Read(false, Columns("post_id"), [5], pinned)).PostId);
		Assert.Equal(6, Assert.Single(Read(false, Columns("postid"), [6], pinned)).PostId); // ignoring case still applies to a pair
	}

	[Fact]
	public static void TheMatchingStaysTheSame_WhenTheShapeIsMetAgain()
	{
		// Interleaved shapes through one map: each comes back from the layout cache, and must feed the same columns.
		var map = new RowMap<Tiers>([new(nameof(Tiers.PostTitle), "title_col")]);
		for (int round = 0; round < 3; round++)
		{
			Tiers a = Assert.Single(Read(false, Columns("post_id", "title_col"), [1, "a"], map));
			Tiers b = Assert.Single(Read(false, Columns("title_col", "PostId", "post_id"), ["b", 2, 3], map));
			Assert.Equal((1, "a", 2, "b"), (a.PostId, a.PostTitle, b.PostId, b.PostTitle));
		}
	}

	// ---- T13: errors ----

	public sealed class Entry
	{
		public int Id { get; set; }
		public int? Count { get; set; }
		public string? Name { get; set; }
	}

	static readonly RowMap<Entry> RenamedEntry = new([new(nameof(Entry.Id), "entry_id"), new(nameof(Entry.Count), "entry_count"), new(nameof(Entry.Name), "entry_name")]);

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void AnUnconvertibleValue_ThrowsValueMappingException_NamingThePropertyColumnAndType(bool buffered)
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read(buffered, Columns("entry_id", "entry_count", "entry_name"), ["abc", 1, "x"], RenamedEntry));
		Assert.Equal("Id", ex.PropertyName);
		Assert.Equal("entry_id", ex.ColumnName);
		Assert.Equal(typeof(int), ex.TargetType);
		Assert.Equal("Unable to set value of property 'Id' (Int32) from column 'entry_id'.", ex.Message);
		Assert.IsType<FormatException>(ex.InnerException);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void AnUnconvertibleValue_IntoANullableProperty_ReportsTheNullableType(bool buffered)
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read(buffered, Columns("entry_id", "entry_count", "entry_name"), [1, "abc", "x"], RenamedEntry));
		Assert.Equal("Count", ex.PropertyName);
		Assert.Equal("entry_count", ex.ColumnName);
		Assert.Equal(typeof(int?), ex.TargetType); // the declared type, Nullable<int>, not its underlying type
		Assert.Equal("Unable to set value of property 'Count' (Int32?) from column 'entry_count'.", ex.Message);
	}

	[Fact]
	public static void AnUnconvertibleValue_NamesTheColumnAsTheResultSpellsIt_WhenMatchedByName()
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read<Entry>(false, Columns("ID"), ["abc"]));
		Assert.Equal(("Id", "ID", typeof(int)), (ex.PropertyName, ex.ColumnName, ex.TargetType));
	}

	// Typed columns raise the failure from a typed read, not from the general conversion; the report is the same.
	[Fact]
	public static void AnOverflowOnTheTypedPath_IsReportedTheSameWay()
	{
		ImmutableArray<ColumnDef> columns = [("entry_id", typeof(long)), ("entry_count", typeof(int)), ("entry_name", typeof(string))];
		var ex = Assert.Throws<ValueMappingException>(() => Read(false, columns, [4_294_967_301L, 1, "x"], RenamedEntry));
		Assert.Equal(("Id", "entry_id", typeof(int)), (ex.PropertyName, ex.ColumnName, ex.TargetType));
		Assert.IsType<OverflowException>(ex.InnerException);
	}

	public sealed class ThrowingSetter
	{
		public int Id { get; set; }
		public string? Name { get => null; set => throw new NotSupportedException("no name for you"); }
	}

	[Fact]
	public static void APropertySetterThatThrows_IsReportedAsAValueMappingException_WithTheInnerException()
	{
		var ex = Assert.Throws<ValueMappingException>(() => Read<ThrowingSetter>(false, Columns("Id", "Name"), [1, "x"]));
		Assert.Equal(("Name", "Name", typeof(string)), (ex.PropertyName, ex.ColumnName, ex.TargetType));
		Assert.IsType<NotSupportedException>(ex.InnerException);
	}

	// A model whose parameterless constructor throws.
	public sealed class ThrowingConstructor
	{
		public static Exception? Failure;
		public int Id { get; set; }
		public ThrowingConstructor() => throw Failure!;
	}

	sealed class ProviderFailure : DbException
	{
		public ProviderFailure() : base("the provider failed") { }
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void AConstructorThatThrows_IsWrapped_AsInvalidOperationException_WithTheOriginalInside(bool buffered)
	{
		var original = new ArgumentException("constructor said no");
		ThrowingConstructor.Failure = original;

		var ex = Assert.Throws<InvalidOperationException>(() => Read<ThrowingConstructor>(buffered, Columns("Id"), [1]));
		Assert.IsNotType<ValueMappingException>(ex);
		Assert.Equal($"Unable to create '{typeof(ThrowingConstructor)}' from the current row.", ex.Message);
		Assert.Same(original, ex.InnerException);
	}

	[Fact]
	public static void AnInvalidOperationExceptionFromTheConstructor_PassesThroughUnwrapped()
	{
		var original = new InvalidOperationException("constructor refused");
		ThrowingConstructor.Failure = original;

		var ex = Assert.Throws<InvalidOperationException>(() => Read<ThrowingConstructor>(false, Columns("Id"), [1]));
		Assert.Same(original, ex);
	}

	[Fact]
	public static void AProviderException_AndCancellation_FromTheConstructor_PassThroughUntouched()
	{
		var provider = new ProviderFailure();
		ThrowingConstructor.Failure = provider;
		Assert.Same(provider, Assert.Throws<ProviderFailure>(() => Read<ThrowingConstructor>(false, Columns("Id"), [1])));

		var canceled = new OperationCanceledException();
		ThrowingConstructor.Failure = canceled;
		Assert.Same(canceled, Assert.Throws<OperationCanceledException>(() => Read<ThrowingConstructor>(false, Columns("Id"), [1])));
	}

	[Fact]
	public static void AFailureOnOneRow_DoesNotPoisonTheNextRead()
	{
		var map = RowMap<Entry>.Default;
		Assert.Throws<ValueMappingException>(() => Read(false, Columns("Id"), ["abc"], map));
		Assert.Equal(3, Assert.Single(Read(false, Columns("Id"), [3], map)).Id);
	}
}
