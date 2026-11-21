using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A model member that declares its column ([Column(Name)], [Column(Order = n)]) or that it is not mapped
// ([NotMapped]) is a hard instruction to the runtime RowMap: when the result does not say the same, it
// throws, once per result set (never per row); an undecorated member keeps reading by name.
[ExcludeFromCodeCoverage]
public static class RowMapDecoratorTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// ---- the models --------------------------------------------------------------

	public sealed class Named
	{
		[Column("post_id")]
		public int Id { get; set; }

		public string? Title { get; set; }
	}

	public sealed class Ordered
	{
		[Column(Order = 0)]
		public int Id { get; set; }

		[Column("headline", Order = 1)]
		public string? Title { get; set; }

		public string? Extra { get; set; }
	}

	public sealed record Positional(
		[property: Column(Order = 0)] int Id,
		[property: Column(Name_Title, Order = 1)] string? Title);

	const string Name_Title = "headline";

	public sealed class Skipped
	{
		public int Id { get; set; }

		[NotMapped]
		public string Note { get; set; } = "kept";
	}

	public sealed class BothAttributes
	{
		[NotMapped, Column("x")]
		public int A { get; set; }
	}

	public sealed class NotMappedRequired
	{
		[NotMapped]
		public required int A { get; set; }
	}

	public sealed class NotMappedButConstructed(int a)
	{
		[NotMapped]
		public int A { get; } = a;
	}

	public sealed class SameOrder
	{
		[Column(Order = 0)]
		public int A { get; set; }

		[Column(Order = 0)]
		public int B { get; set; }
	}

	public sealed class SameName
	{
		[Column("x")]
		public int A { get; set; }

		[Column("X")]
		public int B { get; set; }
	}

	static FakeExecuteReader Cmd(ImmutableArray<ColumnDef> columns, params object?[][] rows)
		=> new(() => TypedReader.Create(columns, rows));

	static ImmutableArray<ColumnDef> Columns(params (string Name, Type Type)[] columns)
		=> [.. columns.Select(c => (ColumnDef)c)];

	// ---- [Column(Name)] ----------------------------------------------------------

	[Theory]
	[InlineData("post_id")]
	[InlineData("POST_ID")]
	public static async Task AColumnName_IsMatchedExactly_ElseIgnoringCase(string column)
	{
		var command = Cmd(Columns((column, typeof(int)), ("Title", typeof(string))), [7, "a"]);

		Named row = Assert.Single(command.Map<Named>().ToList());
		Assert.Equal(7, row.Id);
		Assert.Equal("a", row.Title);

		row = Assert.Single(await command.Map<Named>().ToListAsync(Ct));
		Assert.Equal(7, row.Id);
	}

	[Theory]
	[InlineData("PostId")]
	[InlineData("postid")]
	[InlineData("Id")]
	public static async Task ADeclaredName_IsNotInferred_ByUnderscoresOrByTheMemberName(string lookalike)
	{
		var command = Cmd(Columns((lookalike, typeof(int))), [7]);

		var sync = Assert.Throws<ValueMappingException>(() => command.Map<Named>().ToList());
		var async = await Assert.ThrowsAsync<ValueMappingException>(
			async () => await command.Map<Named>().ToListAsync(Ct));

		foreach (ValueMappingException ex in new[] { sync, async })
		{
			// The member and the column it needs, named.
			Assert.Equal("Id", ex.PropertyName);
			Assert.Equal("post_id", ex.ColumnName);
			Assert.Contains("'Id'", ex.Message);
			Assert.Contains("post_id", ex.Message);
		}
	}

	[Fact]
	public static void AnUndecoratedMember_StillReadsByName_NextToADecoratedOne()
	{
		// Title is undecorated: tier three (ignoring underscores) still finds it.
		var command = Cmd(Columns(("post_id", typeof(int)), ("TITLE", typeof(string))), [1, "t"]);

		Named row = Assert.Single(command.Map<Named>().ToList());

		Assert.Equal("t", row.Title);
	}

	// ---- [Column(Order = n)] -----------------------------------------------------

	[Fact]
	public static async Task AnOrder_ReadsTheColumnAtThatOrdinal_AndExtraColumnsAreAllowed()
	{
		// Id at 0, headline at 1; Extra is undecorated and found by name; ordinals nobody declares are ignored.
		var command = Cmd(
			Columns(("Id", typeof(int)), ("headline", typeof(string)), ("Extra", typeof(string)), ("Unused", typeof(int))),
			[1, "h", "e", 9]);

		Ordered row = Assert.Single(command.Map<Ordered>().ToList());
		Assert.Equal((1, "h", "e"), (row.Id, row.Title, row.Extra));

		row = Assert.Single(await command.Map<Ordered>().ToListAsync(Ct));
		Assert.Equal((1, "h", "e"), (row.Id, row.Title, row.Extra));
	}

	[Fact]
	public static async Task AnOrder_ThatMeetsAnotherColumn_Throws_NamingTheMemberTheOrdinalAndWhatIsThere()
	{
		var command = Cmd(Columns(("headline", typeof(string)), ("Id", typeof(int))), ["h", 1]);

		var sync = Assert.Throws<ValueMappingException>(() => command.Map<Ordered>().ToList());
		var async = await Assert.ThrowsAsync<ValueMappingException>(
			async () => await command.Map<Ordered>().ToListAsync(Ct));

		foreach (ValueMappingException ex in new[] { sync, async })
		{
			Assert.Contains("'Id'", ex.Message);
			Assert.Contains("column 0", ex.Message);
			Assert.Contains("'headline'", ex.Message);
			Assert.Equal("Id", ex.PropertyName);
		}
	}

	[Fact]
	public static async Task AnOrder_BeyondTheResult_Throws()
	{
		var command = Cmd(Columns(("Id", typeof(int))), [1]);

		var sync = Assert.Throws<ValueMappingException>(() => command.Map<Ordered>().ToList());
		var async = await Assert.ThrowsAsync<ValueMappingException>(
			async () => await command.Map<Ordered>().ToListAsync(Ct));

		Assert.Contains("the result has 1 column", sync.Message);
		Assert.Contains("column 1", async.Message);
	}

	[Theory]
	[InlineData("Id")]
	[InlineData("ID")]
	public static void AnOrder_WithoutAName_NeedsTheMemberName_ExactlyOrIgnoringCase(string column)
	{
		var command = Cmd(Columns((column, typeof(int)), ("headline", typeof(string))), [3, "h"]);

		Assert.Equal(3, Assert.Single(command.Map<Ordered>().ToList()).Id);
	}

	[Fact]
	public static async Task AnOrder_WithoutAName_DoesNotAcceptAnUnderscoreLookalike()
	{
		// A declaration is a hard rule: I_d at ordinal 0 is not Id, though an undecorated member would match it.
		var command = Cmd(Columns(("I_d", typeof(int)), ("headline", typeof(string))), [3, "h"]);

		var sync = Assert.Throws<ValueMappingException>(() => command.Map<Ordered>().ToList());
		var async = await Assert.ThrowsAsync<ValueMappingException>(
			async () => await command.Map<Ordered>().ToListAsync(Ct));

		foreach (ValueMappingException ex in new[] { sync, async })
		{
			Assert.Equal("Id", ex.PropertyName);
			Assert.Contains("'I_d'", ex.Message);
		}
	}

	[Fact]
	public static void AnOrder_WithAName_NeedsThatNameAtThatOrdinal_NotTheMemberName()
	{
		// Title is declared "headline" at 1: a column named Title there is not it.
		var command = Cmd(Columns(("Id", typeof(int)), ("Title", typeof(string))), [1, "t"]);

		var ex = Assert.Throws<ValueMappingException>(() => command.Map<Ordered>().ToList());

		Assert.Equal("Title", ex.PropertyName);
		Assert.Contains("'headline'", ex.Message);
	}

	[Fact]
	public static void AnOrder_OnARecordPositionalProperty_IsHonoured()
	{
		var good = Cmd(Columns(("Id", typeof(int)), ("headline", typeof(string))), [1, "h"]);
		Assert.Equal(new Positional(1, "h"), Assert.Single(good.Map<Positional>().ToList()));

		var swapped = Cmd(Columns(("headline", typeof(string)), ("Id", typeof(int))), ["h", 1]);
		Assert.Throws<ValueMappingException>(() => swapped.Map<Positional>().ToList());
	}

	// ---- [NotMapped] -------------------------------------------------------------

	[Fact]
	public static void ANotMappedMember_IsNeverRead_EvenWhenAColumnLooksLikeIt()
	{
		var command = Cmd(Columns(("Id", typeof(int)), ("Note", typeof(string)), ("NOTE", typeof(string)), ("no_te", typeof(string))), [1, "a", "b", "c"]);

		Skipped row = Assert.Single(command.Map<Skipped>().ToList());

		Assert.Equal(1, row.Id);
		Assert.Equal("kept", row.Note);
	}

	[Fact]
	public static void APairOnANotMappedMember_MapsItAllTheSame()
	{
		// A pair overrides the declaration: the member exists for the map that names it.
		var map = new RowMap<Skipped>(new("Note", "memo"));
		var command = Cmd(Columns(("Id", typeof(int)), ("memo", typeof(string)), ("Note", typeof(string))), [1, "from memo", "from Note"]);

		Skipped row = Assert.Single(command.Map(map).ToList());

		Assert.Equal("from memo", row.Note);

		// A pair to null is what it says: unread.
		var unread = new RowMap<Skipped>(new("Note", null));
		Assert.Equal("kept", Assert.Single(command.Map(unread).ToList()).Note);
	}

	public static TheoryData<Type> ContradictoryTypes
		=> [typeof(BothAttributes), typeof(NotMappedRequired), typeof(NotMappedButConstructed), typeof(SameOrder), typeof(SameName)];

	[Theory]
	[MemberData(nameof(ContradictoryTypes))]
	public static void AContradictionInWhatAModelDeclares_IsATypeError_AtTheCall(Type type)
	{
		var command = Cmd(Columns(("A", typeof(int))), [1]);

		var ex = Assert.Throws<InvalidOperationException>(() => Map(type, command));

		Assert.Contains("cannot be mapped", ex.Message);
	}

	[Fact]
	public static void TheTypeErrors_NameTheMembers()
	{
		Assert.Contains("[NotMapped] and [Column]", Assert.Throws<InvalidOperationException>(() => RowMap<BothAttributes>.Default).Message);
		Assert.Contains("required", Assert.Throws<InvalidOperationException>(() => RowMap<NotMappedRequired>.Default).Message);
		Assert.Contains("constructor parameter", Assert.Throws<InvalidOperationException>(() => RowMap<NotMappedButConstructed>.Default).Message);
		Assert.Contains("Order = 0", Assert.Throws<InvalidOperationException>(() => RowMap<SameOrder>.Default).Message);
		Assert.Contains("\"x\"", Assert.Throws<InvalidOperationException>(() => RowMap<SameName>.Default).Message);
	}

	static void Map(Type type, FakeExecuteReader command)
	{
		// The same entry point a caller uses, closed over the model type.
		var method = typeof(RowMapDecoratorTests).GetMethod(nameof(MapOf), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
		try
		{
			method.MakeGenericMethod(type).Invoke(null, [command]);
		}
		catch (System.Reflection.TargetInvocationException ex)
		{
			throw ex.InnerException!;
		}
	}

	static void MapOf<T>(FakeExecuteReader command)
		=> command.Map<T>().ToList();

	// ---- explicit pairs on a decorated member -------------------------------------

	[Fact]
	public static void APairThatAgreesWithTheDeclaration_ReadsTheSameColumn()
	{
		var same = new RowMap<Named>(new("Id", "post_id"));
		var otherCase = new RowMap<Named>(new("Id", "POST_ID"));

		var command = Cmd(Columns(("post_id", typeof(int))), [4]);

		Assert.Equal(4, Assert.Single(command.Map(same).ToList()).Id);
		Assert.Equal(4, Assert.Single(command.Map(otherCase).ToList()).Id);
	}

	// The owner's rule: a pair is an override. It replaces every declaration on THAT member for that map, with no throw;
	// the other members keep theirs.
	[Fact]
	public static async Task APair_OverridesTheDeclarationsOfItsMember_NotOfTheOthers()
	{
		// Id is declared "post_id"; the pair says "id": that column is read, and post_id is never asked for.
		var other = new RowMap<Named>(new("Id", "id"));
		var command = Cmd(Columns(("id", typeof(int)), ("Title", typeof(string))), [4, "t"]);
		Named row = Assert.Single(command.Map(other).ToList());
		Assert.Equal(4, row.Id);
		Assert.Equal(4, Assert.Single(await command.Map(other).ToListAsync(Ct)).Id);

		// A declared ordinal is not checked for the overridden member (Id is declared at 0, and is read at 2 here) ...
		var moved = new RowMap<Ordered>(new("Id", "elsewhere"));
		var shuffled = Cmd(Columns(("first", typeof(string)), ("headline", typeof(string)), ("elsewhere", typeof(int))), ["f", "h", 9]);
		Ordered moved1 = Assert.Single(shuffled.Map(moved).ToList());
		Assert.Equal((9, "h"), (moved1.Id, moved1.Title));

		// ... while another member keeps its own: Title is declared "headline" at 1, and is not there.
		var wrong = Cmd(Columns(("first", typeof(string)), ("other", typeof(string)), ("elsewhere", typeof(int))), ["f", "o", 9]);
		var ex = Assert.Throws<ValueMappingException>(() => wrong.Map(moved).ToList());
		Assert.Equal("Title", ex.PropertyName);
	}

	[Fact]
	public static void APairToNull_LeavesADeclaredMemberUnread_WithNoThrow()
	{
		var map = new RowMap<Named>(new("Id", null));
		var command = Cmd(Columns(("Title", typeof(string))), ["t"]);

		Named row = Assert.Single(command.Map(map).ToList());

		Assert.Equal(0, row.Id);
		Assert.Equal("t", row.Title);
	}

	[Fact]
	public static void AConverterWithAColumn_OverridesTheDeclaredColumnToo()
	{
		var map = RowMap<Named>.Default.Convert<int, int>("Id", "id", i => i + 1);
		var command = Cmd(Columns(("id", typeof(int))), [4]);

		Assert.Equal(5, Assert.Single(command.Map(map).ToList()).Id);
	}
	[Fact]
	public static void AConverter_OnADecoratedMember_StillReadsItsDeclaredColumn()
	{
		var map = RowMap<Named>.Default.Convert<int, int>("Id", i => i * 2);
		var command = Cmd(Columns(("post_id", typeof(int))), [4]);

		Assert.Equal(8, Assert.Single(command.Map(map).ToList()).Id);
	}

	// ---- the layout is checked per result set, and a failure is never cached --------

	[Fact]
	public static async Task AFailingShape_ThrowsEveryTime_AndAGoodShapeAfterItMaps()
	{
		RowMap<Ordered> map = RowMap<Ordered>.Default;
		var bad = Cmd(Columns(("headline", typeof(string)), ("Id", typeof(int))), ["h", 1]);
		var good = Cmd(Columns(("Id", typeof(int)), ("headline", typeof(string))), [1, "h"]);

		Assert.Throws<ValueMappingException>(() => bad.Map(map).ToList());
		Assert.Throws<ValueMappingException>(() => bad.Map(map).ToList());
		await Assert.ThrowsAsync<ValueMappingException>(async () => await bad.Map(map).ToListAsync(Ct));

		Assert.Equal("h", Assert.Single(good.Map(map).ToList()).Title);
		Assert.Throws<ValueMappingException>(() => bad.Map(map).ToList());
		Assert.Equal("h", Assert.Single(await good.Map(map).ToListAsync(Ct)).Title);
	}

	[Fact]
	public static void ManyRows_AreReadByOneCheckedLayout()
	{
		var command = Cmd(Columns(("Id", typeof(int)), ("headline", typeof(string))), [1, "a"], [2, "b"], [3, "c"]);

		Assert.Equal([1, 2, 3], command.Map<Ordered>().ToList().Select(r => r.Id));
	}

	[Fact]
	public static void TheBufferedRoute_ChecksTheSame()
	{
		var map = RowMap<Ordered>.Default;
		ImmutableArray<string> swapped = ["headline", "Id"];
		ImmutableArray<string> right = ["Id", "headline"];

		Assert.Throws<ValueMappingException>(() => map.ArrayMapper(swapped));
		Assert.Equal(1, map.ArrayMapper(right)([1, "h"]).Id);
	}

	// ---- NULL is the existing table: a decorated member behaves as its undecorated twin ----

	public sealed class PlainNulls
	{
		public int Count { get; set; } = 7;
		public int? Maybe { get; set; } = 5;
		public string Text { get; set; } = "t";
		public string? Free { get; set; } = "f";
		[AllowNull]
		public string Allowed { get; set; } = "a";
	}

	public sealed class DecoratedNulls
	{
		[Column("Count", Order = 0)]
		public int Count { get; set; } = 7;

		[Column("Maybe", Order = 1)]
		public int? Maybe { get; set; } = 5;

		[Column("Text", Order = 2)]
		public string Text { get; set; } = "t";

		[Column("Free", Order = 3)]
		public string? Free { get; set; } = "f";

		[AllowNull, Column("Allowed", Order = 4)]
		public string Allowed { get; set; } = "a";
	}

	public sealed record PlainParameters(int Count = 3, string? Text = "d", string Required = "r");

	public sealed record DecoratedParameters(
		[property: Column("Count", Order = 0)] int Count = 3,
		[property: Column("Text", Order = 1)] string? Text = "d",
		[property: Column("Required", Order = 2)] string Required = "r");

	static readonly ImmutableArray<ColumnDef> NullColumns = Columns(
		("Count", typeof(int)), ("Maybe", typeof(int)), ("Text", typeof(string)), ("Free", typeof(string)), ("Allowed", typeof(string)));

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(2)]
	[InlineData(3)]
	[InlineData(4)]
	public static void ANullCell_GivesADecoratedMember_WhatItGivesItsUndecoratedTwin(int nullAt)
	{
		object?[] row = [1, 2, "x", "y", "z"];
		row[nullAt] = null;

		PlainNulls plain = Assert.Single(Cmd(NullColumns, row).Map<PlainNulls>().ToList());
		DecoratedNulls decorated = Assert.Single(Cmd(NullColumns, row).Map<DecoratedNulls>().ToList());

		Assert.Equal(
			(plain.Count, plain.Maybe, plain.Text, plain.Free, plain.Allowed),
			(decorated.Count, decorated.Maybe, decorated.Text, decorated.Free, decorated.Allowed));
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(2)]
	public static void ANullCell_GivesADecoratedConstructorParameter_WhatItGivesItsUndecoratedTwin(int nullAt)
	{
		var columns = Columns(("Count", typeof(int)), ("Text", typeof(string)), ("Required", typeof(string)));
		object?[] row = [1, "x", "y"];
		row[nullAt] = null;

		PlainParameters plain = Assert.Single(Cmd(columns, row).Map<PlainParameters>().ToList());
		DecoratedParameters decorated = Assert.Single(Cmd(columns, row).Map<DecoratedParameters>().ToList());

		Assert.Equal((plain.Count, plain.Text, plain.Required), (decorated.Count, decorated.Text, decorated.Required));
	}
}
