using System.Threading.Tasks.Dataflow;
using static Open.Database.Extensions.Tests.ConstructorBindingTests;

#nullable enable

namespace Open.Database.Extensions.Tests;

// [NullPolicy] and [NullValue]: a member they cover is tested for NULL before its value is read, whatever its type (nullable members included).
// Throw names the member, the column and the row; Default keeps the initializer (a parameter: its declared default);
// None adds no test (the type's NULL rules apply), and a member's policy overrides the type's.
// A null value is that value, converted when the map is created, and wins over any policy.
[ExcludeFromCodeCoverage]
public static class NullPolicyTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	public enum Tier { Low, Mid, High }

	public sealed class Members
	{
		[NullPolicy(NullAction.Throw)] public int Count { get; set; }
		[NullPolicy(NullAction.Default)] public int Kept { get; set; } = 7;
		[NullValue(42)] public int Constant { get; set; }
		[NullPolicy(NullAction.None)] public int Unchecked { get; set; }
		[NullPolicy(NullAction.Throw)] public int? MaybeThrow { get; set; }
		[NullPolicy(NullAction.Default)] public int? MaybeKept { get; set; } = 5;
		[NullPolicy(NullAction.Default)] public int? MaybeNoInit { get; set; }
		[NullValue(9)] public int? MaybeConstant { get; set; }
		[NullPolicy(NullAction.None)] public int? MaybeNone { get; set; } = 3;
		[NullPolicy(NullAction.Throw)] public string? Text { get; set; }
		[NullValue("none")] public string Label { get; set; } = "";
		[NullValue(2)] public Tier Level { get; set; }
		[NullValue("2024-01-02")] public DateTime Day { get; set; }
		[NullValue(2.5)] public decimal Amount { get; set; }
		[NullValue(7)] public long Big { get; set; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class ThrowAll
	{
		public int Strict { get; set; }
		public int? Maybe { get; set; }
		public string? Text { get; set; }
		[NullPolicy(NullAction.None)] public int? Hot { get; set; } = 1;
		[NullPolicy(NullAction.None)] public int HotStrict { get; set; }
		[NullPolicy(NullAction.Default)] public int Kept { get; set; } = 4;
		[NullValue(11)] public int Fixed { get; set; }
	}

	[NullPolicy(NullAction.Default)]
	public sealed class DefaultAll
	{
		public int Strict { get; set; } = 1;
		public int? Maybe { get; set; } = 2;
		public string Name { get; set; } = "n";
		[NullPolicy(NullAction.Throw)] public int Checked { get; set; }
	}

	[NullPolicy(NullAction.None)]
	public sealed class NoneAll
	{
		public int Strict { get; set; }
		public int? Maybe { get; set; } = 2;
		[NullPolicy(NullAction.Throw)] public int Checked { get; set; }
	}

	public sealed record Positional(
		[NullPolicy(NullAction.Default)] int Count = 3,
		[NullPolicy(NullAction.Throw)] int? Maybe = null,
		[NullValue(8)] int Fixed = 0,
		[NullPolicy(NullAction.Default)] int? MaybeKept = 6);

	public sealed record OnProperty([property: NullPolicy(NullAction.Default)] int Count = 4);

	[NullPolicy(NullAction.Default)]
	public sealed record PositionalAll(int Count = 6, string Name = "x", int? Maybe = 2)
	{
		public int Extra { get; set; } = 9;
	}

	[NullPolicy(NullAction.Throw)]
	public sealed record PositionalThrow(int Count, int? Maybe);

	public sealed class Built([NullPolicy(NullAction.Default)] int count = 5)
	{
		public int Count { get; } = count;
		[NullPolicy(NullAction.Default)] public int Extra { get; set; } = 9;
		[NullPolicy(NullAction.Throw)] public int? Checked { get; set; }
	}

	// A null value wins over any policy: the type's Throw, and the member's own Throw, Default or None.
	[NullPolicy(NullAction.Throw)]
	public sealed class ValueWins
	{
		[NullValue(1)] public int UnderType { get; set; }
		[NullPolicy(NullAction.Throw), NullValue(2)] public int OverOwnThrow { get; set; }
		[NullPolicy(NullAction.Default), NullValue(3)] public int OverOwnDefault { get; set; } = 30;
		[NullPolicy(NullAction.None), NullValue(4)] public int? OverOwnNone { get; set; }
	}

	public sealed record ValueOnParameter([NullPolicy(NullAction.Throw), NullValue(5)] int Count, [property: NullValue("p")] string Name);

	public sealed class Misfit
	{
		[NullValue("not a number")] public int A { get; set; }
	}

	public sealed class ATypeValue
	{
		[NullValue(typeof(int))] public int A { get; set; }
	}

	public sealed class AnArrayValue
	{
		[NullValue(new[] { 1 })] public int A { get; set; }
	}

	public sealed class UndefinedAction
	{
		[NullPolicy((NullAction)7)] public int A { get; set; }
	}

	// [NullPolicy(0)] is NullAction.None: no check.
	public sealed class ZeroIsNone
	{
		[NullPolicy(0)] public int? A { get; set; } = 1;
	}

	public sealed class NullIntoInt
	{
		[NullValue(null)] public int A { get; set; }
	}

	public sealed class NullIntoNullable
	{
		[NullValue(null)] public int? A { get; set; } = 1;
	}

	public sealed class Counted
	{
		[NullPolicy(NullAction.Throw)] public int Id { get; set; }
	}

	// One row whose only column is `column`, holding `cell`.
	static TModel One<TModel>(Route route, string column, Type type, object? cell)
		=> Assert.Single(Read<TModel>(route, [new ColumnDef(column, type)], [cell]));

	static ValueMappingException Thrown<TModel>(Route route, string column, Type type, object? cell)
		=> Assert.Throws<ValueMappingException>(() => Read<TModel>(route, [new ColumnDef(column, type)], [cell]));

	// The Throw error: the member, the column, the member's type and the row; no inner exception.
	static void AssertThrow<TModel, TMember>(ValueMappingException ex, string member, string column, int row)
	{
		string at = row < 0 ? "" : $" at row {row}";
		Assert.Equal($"A NULL was read from column '{column}' into '{member}' of {typeof(TModel).Name}{at}, whose null policy is Throw.", ex.Message);
		Assert.Equal(member, ex.PropertyName);
		Assert.Equal(column, ex.ColumnName);
		Assert.Equal(typeof(TMember), ex.TargetType);
		Assert.Null(ex.InnerException);
	}

	// ---- member level ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void MemberLevel_EachAction_OnANonNullableMember(Route route)
	{
		AssertThrow<Members, int>(Thrown<Members>(route, "Count", typeof(int), null), "Count", "Count", 0);
		Assert.Equal(5, One<Members>(route, "Count", typeof(int), 5).Count);
		Assert.Equal(7, One<Members>(route, "Kept", typeof(int), null).Kept);
		Assert.Equal(1, One<Members>(route, "Kept", typeof(int), 1).Kept);
		Assert.Equal(42, One<Members>(route, "Constant", typeof(int), null).Constant);
		Assert.Equal(1, One<Members>(route, "Constant", typeof(int), 1).Constant);
		NullAssert.Model<Members>(0, () => Read<Members>(route, [new ColumnDef("Unchecked", typeof(int))], [null]));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APolicyAlwaysApplies_ANullableMemberIncluded(Route route)
	{
		AssertThrow<Members, int?>(Thrown<Members>(route, "MaybeThrow", typeof(int), null), "MaybeThrow", "MaybeThrow", 0);
		AssertThrow<Members, string>(Thrown<Members>(route, "Text", typeof(string), null), "Text", "Text", 0);
		Assert.Equal(5, One<Members>(route, "MaybeKept", typeof(int), null).MaybeKept);
		Assert.Null(One<Members>(route, "MaybeNoInit", typeof(int), null).MaybeNoInit);
		Assert.Equal(9, One<Members>(route, "MaybeConstant", typeof(int), null).MaybeConstant);
		Assert.Null(One<Members>(route, "MaybeNone", typeof(int), null).MaybeNone);
		Assert.Equal(4, One<Members>(route, "MaybeThrow", typeof(int), 4).MaybeThrow);
		Assert.Equal(4, One<Members>(route, "MaybeConstant", typeof(int), 4).MaybeConstant);
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void AValue_IsConvertedToTheMembersType(Route route)
	{
		Assert.Equal("none", One<Members>(route, "Label", typeof(string), null).Label);
		Assert.Equal(Tier.High, One<Members>(route, "Level", typeof(int), null).Level);
		Assert.Equal(new DateTime(2024, 1, 2), One<Members>(route, "Day", typeof(DateTime), null).Day);
		Assert.Equal(2.5m, One<Members>(route, "Amount", typeof(decimal), null).Amount);
		Assert.Equal(7L, One<Members>(route, "Big", typeof(long), null).Big);
		Assert.Equal(Tier.Mid, One<Members>(route, "Level", typeof(int), 1).Level);
	}

	// ---- type level, and a member overriding it ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void TypeLevel_Throw_CoversEveryMember_AndAMemberOverridesIt(Route route)
	{
		AssertThrow<ThrowAll, int>(Thrown<ThrowAll>(route, "Strict", typeof(int), null), "Strict", "Strict", 0);
		AssertThrow<ThrowAll, int?>(Thrown<ThrowAll>(route, "Maybe", typeof(int), null), "Maybe", "Maybe", 0);
		AssertThrow<ThrowAll, string>(Thrown<ThrowAll>(route, "Text", typeof(string), null), "Text", "Text", 0);
		Assert.Null(One<ThrowAll>(route, "Hot", typeof(int), null).Hot);
		NullAssert.Model<ThrowAll>(0, () => Read<ThrowAll>(route, [new ColumnDef("HotStrict", typeof(int))], [null]));
		Assert.Equal(4, One<ThrowAll>(route, "Kept", typeof(int), null).Kept);
		Assert.Equal(11, One<ThrowAll>(route, "Fixed", typeof(int), null).Fixed);
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void TypeLevel_Default_And_None(Route route)
	{
		Assert.Equal(1, One<DefaultAll>(route, "Strict", typeof(int), null).Strict);
		Assert.Equal(2, One<DefaultAll>(route, "Maybe", typeof(int), null).Maybe);
		Assert.Equal("n", One<DefaultAll>(route, "Name", typeof(string), null).Name);
		AssertThrow<DefaultAll, int>(Thrown<DefaultAll>(route, "Checked", typeof(int), null), "Checked", "Checked", 0);

		NullAssert.Model<NoneAll>(0, () => Read<NoneAll>(route, [new ColumnDef("Strict", typeof(int))], [null]));
		Assert.Null(One<NoneAll>(route, "Maybe", typeof(int), null).Maybe);
		AssertThrow<NoneAll, int>(Thrown<NoneAll>(route, "Checked", typeof(int), null), "Checked", "Checked", 0);
	}

	// ---- constructor parameters ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void AParameter_TakesItsPolicy_DefaultGivesItsDeclaredDefault(Route route)
	{
		ImmutableArray<ColumnDef> columns = [new("Count", typeof(int)), new("Fixed", typeof(int)), new("MaybeKept", typeof(int))];
		Assert.Equal(new Positional(3, null, 8, 6), Assert.Single(Read<Positional>(route, columns, [null, null, null])));
		Assert.Equal(new Positional(1, null, 2, 3), Assert.Single(Read<Positional>(route, columns, [1, 2, 3])));
		AssertThrow<Positional, int?>(Thrown<Positional>(route, "Maybe", typeof(int), null), "Maybe", "Maybe", 0);

		Assert.Equal(4, One<OnProperty>(route, "Count", typeof(int), null).Count);

		ImmutableArray<ColumnDef> all = [new("Count", typeof(int)), new("Name", typeof(string)), new("Maybe", typeof(int)), new("Extra", typeof(int))];
		PositionalAll defaulted = Assert.Single(Read<PositionalAll>(route, all, [null, null, null, null]));
		Assert.Equal(new PositionalAll(6, "x", 2) { Extra = 9 }, defaulted);

		AssertThrow<PositionalThrow, int?>(
			Assert.Throws<ValueMappingException>(() => Read<PositionalThrow>(route, [new("Count", typeof(int)), new("Maybe", typeof(int))], [1, null])),
			"Maybe", "Maybe", 0);

		ImmutableArray<ColumnDef> built = [new("Count", typeof(int)), new("Extra", typeof(int))];
		Built b = Assert.Single(Read<Built>(route, built, [null, null]));
		Assert.Equal(5, b.Count);
		Assert.Equal(9, b.Extra);
		Built set = Assert.Single(Read<Built>(route, built, [1, 2]));
		Assert.Equal(1, set.Count);
		Assert.Equal(2, set.Extra);
		AssertThrow<Built, int?>(Thrown<Built>(route, "Checked", typeof(int), null), "Checked", "Checked", 0);
	}

	// ---- the row; a pair or a converter overrides the policy ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void TheError_NamesTheRow(Route route)
	{
		ValueMappingException ex = Assert.Throws<ValueMappingException>(
			() => ReadRows<Members>(route, [new ColumnDef("Count", typeof(int))], [[1], [2], [null]]));
		AssertThrow<Members, int>(ex, "Count", "Count", 2);
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APair_OverridesThePolicy_TheMemberFollowsTheNullRulesOfItsType(Route route)
	{
		RowMap<Members> map = [new(nameof(Members.Count), "n"), new(nameof(Members.MaybeKept), "m")];
		NullAssert.Model<Members>(1, () => ReadRows(route, [new ColumnDef("n", typeof(int))], [[1], [null]], map));
		Assert.Null(Assert.Single(ReadRows(route, [new ColumnDef("m", typeof(int))], [[null]], map)).MaybeKept);
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void AConverter_OverridesThePolicy(Route route)
	{
		ImmutableArray<ColumnDef> columns = [new("Constant", typeof(int))];

		// A converter of a non-nullable column is not called on NULL, and the non-nullable member rejects it: the policy's 42 does not apply.
		int calls = 0;
		RowMap<Members> plain = RowMap<Members>.Default.Convert<int, int>(nameof(Members.Constant), v => { calls++; return v * 2; });
		Assert.Equal([6], ReadRows(route, columns, [[3]], plain).Select(m => m.Constant));
		NullAssert.Model<Members>(0, () => ReadRows(route, columns, [[null]], plain));
		Assert.Equal(1, calls);

		// A converter of a nullable column receives the NULL itself.
		RowMap<Members> sees = RowMap<Members>.Default.Convert<int?, int>(nameof(Members.Constant), v => v ?? -1);
		Assert.Equal([-1, 3], ReadRows(route, columns, [[null], [3]], sees).Select(m => m.Constant));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APairOrAConverter_LeavesTheTypesPolicy(Route route)
	{
		// ThrowAll is Throw: a pair overrides Kept's own Default, so the type's Throw covers it, naming the pair's column.
		RowMap<ThrowAll> paired = [new(nameof(ThrowAll.Kept), "k")];
		AssertThrow<ThrowAll, int>(
			Assert.Throws<ValueMappingException>(() => ReadRows(route, [new ColumnDef("k", typeof(int))], [[1], [null]], paired)),
			"Kept", "k", 1);

		// DefaultAll is Default: a converter overrides Checked's own Throw; the converter is not called for the NULL, and the type's Default keeps the member.
		RowMap<DefaultAll> converted = RowMap<DefaultAll>.Default.Convert<int, int>(nameof(DefaultAll.Checked), v => v + 1);
		Assert.Equal([0, 3], ReadRows(route, [new ColumnDef("Checked", typeof(int))], [[null], [2]], converted).Select(m => m.Checked));

		// A converter of a nullable column receives the NULL: no policy applies.
		RowMap<DefaultAll> sees = RowMap<DefaultAll>.Default.Convert<int?, int>(nameof(DefaultAll.Strict), v => v ?? -1);
		Assert.Equal(-1, Assert.Single(ReadRows(route, [new ColumnDef("Strict", typeof(int))], [[null]], sees)).Strict);
	}

	[Fact]
	public static void AFactoryParameter_TakesItsPolicy_AndAValueThatDoesNotFitThrowsAtFrom()
	{
		var map = RowMap<Counted>.From(([NullPolicy(NullAction.Default)] int id = 4) => new Counted { Id = id });
		object?[] nullRow = [null];
		Assert.Equal([4, 2], new FakeExecuteReader(["id"], nullRow, [2]).Map(map).ToList().Select(c => c.Id));

		ArgumentException misfit = Assert.Throws<ArgumentException>(
			() => RowMap<Counted>.From(([NullValue("x")] int id) => new Counted { Id = id }));
		Assert.Equal("factory", misfit.ParamName);
	}

	[Fact]
	public static void AnArrayMapper_AppliesThePolicy_AConverterIncluded()
	{
		Func<object?[], DefaultAll> plain = RowMap<DefaultAll>.Default.ArrayMapper(["Strict", "Name"]);
		Assert.Equal((1, "n"), (plain([null, null]).Strict, plain([null, null]).Name));
		Assert.Equal((5, "x"), (plain([5, "x"]).Strict, plain([5, "x"]).Name));

		Func<object?[], DefaultAll> converted = RowMap<DefaultAll>.Default.Convert<int, int>(nameof(DefaultAll.Checked), v => v + 1).ArrayMapper(["Checked"]);
		Assert.Equal(0, converted([null]).Checked);
		Assert.Equal(3, converted([2]).Checked);
	}

	// ---- what cannot be honoured: ArgumentException when the map is created ----

	[Fact]
	public static void AValueThatDoesNotFit_OrIsNotAConstant_ThrowsWhenTheMapIsCreated()
	{
		ArgumentException misfit = Assert.Throws<ArgumentException>(() => new RowMap<Misfit>());
		Assert.Equal("The null value of 'A' of 'Misfit', not a number (String), does not convert to Int32.", misfit.Message);
		Assert.Throws<ArgumentException>(() => RowMap<Misfit>.Default);
		Assert.Throws<ArgumentException>(() => new FakeExecuteReader(["A"], [1]).Map<Misfit>());

		Assert.Equal(
			"The null value of 'A' of 'NullIntoInt', null, does not convert to Int32.",
			Assert.Throws<ArgumentException>(() => RowMap<NullIntoInt>.Default).Message);
		Assert.Equal(
			"'A' of 'ATypeValue' declares a [NullValue] of type Type: a null value is null, a string, a bool, a char, a number or an enum value.",
			Assert.Throws<ArgumentException>(() => RowMap<ATypeValue>.Default).Message);
		Assert.Equal(
			"'A' of 'AnArrayValue' declares a [NullValue] of type Int32[]: a null value is null, a string, a bool, a char, a number or an enum value.",
			Assert.Throws<ArgumentException>(() => RowMap<AnArrayValue>.Default).Message);
		Assert.Equal(
			"'A' of 'UndefinedAction' declares [NullPolicy(7)], which is not a NullAction.",
			Assert.Throws<ArgumentException>(() => RowMap<UndefinedAction>.Default).Message);

		object?[] nullRow = [null];
		Assert.Null(Assert.Single(new FakeExecuteReader(["A"], nullRow).Map<ZeroIsNone>().ToList()).A);
		Assert.Equal(5, Assert.Single(new FakeExecuteReader(["A"], [5]).Map<NullIntoNullable>().ToList()).A);
		Assert.Null(Assert.Single(new FakeExecuteReader(["A"], nullRow).Map<NullIntoNullable>().ToList()).A);
	}

	// ---- a null value wins ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ANullValue_WinsOverEveryPolicy_TheMembersOwnIncluded(Route route)
	{
		Assert.Equal(1, One<ValueWins>(route, "UnderType", typeof(int), null).UnderType);
		Assert.Equal(2, One<ValueWins>(route, "OverOwnThrow", typeof(int), null).OverOwnThrow);
		Assert.Equal(3, One<ValueWins>(route, "OverOwnDefault", typeof(int), null).OverOwnDefault);
		Assert.Equal(4, One<ValueWins>(route, "OverOwnNone", typeof(int), null).OverOwnNone);
		Assert.Equal(8, One<ValueWins>(route, "OverOwnThrow", typeof(int), 8).OverOwnThrow);

		ImmutableArray<ColumnDef> columns = [new("Count", typeof(int)), new("Name", typeof(string))];
		Assert.Equal(new ValueOnParameter(5, "p"), Assert.Single(Read<ValueOnParameter>(route, columns, [null, null])));
		Assert.Equal(new ValueOnParameter(1, "n"), Assert.Single(Read<ValueOnParameter>(route, columns, [1, "n"])));
	}

	// ---- every route applies it ----

	static FakeDbCommand Command(params object?[][] rows)
		=> new(new FakeDbConnection(), () => TypedReader.Create([new ColumnDef("Id", typeof(int))], rows));

	[Fact]
	public static async Task EveryRoute_AppliesThePolicy()
	{
		static void Check(ValueMappingException ex, int row) => AssertThrow<Counted, int>(ex, "Id", "Id", row);

		Check(Assert.Throws<ValueMappingException>(() => Command([1], [null]).Map<Counted>().ToList()), 1);
		Check(await Assert.ThrowsAsync<ValueMappingException>(async () => await Command([1], [null]).Map<Counted>().ToListAsync(Ct)), 1);
		Check(await Assert.ThrowsAsync<ValueMappingException>(async () => await Command([1], [null]).Map<Counted>().AsyncReads().ToListAsync(Ct)), 1);
		object?[] nullRow = [null];
		Check(Assert.Throws<ValueMappingException>(() => Command(nullRow).Map<Counted>().First()), 0);
		Check(await Assert.ThrowsAsync<ValueMappingException>(async () => await Command(nullRow).Map<Counted>().SingleAsync(Ct)), 0);

		Check(await Assert.ThrowsAsync<ValueMappingException>(async () =>
		{
			await foreach (Counted _ in Command([1], [null]).Map<Counted>().Stream(Timeout.InfiniteTimeSpan, Ct)) { }
		}), 1);
		Check(await Assert.ThrowsAsync<ValueMappingException>(async () =>
		{
			await foreach (Counted _ in Command([1], [null]).Map<Counted>().AsyncReads().Stream(Timeout.InfiniteTimeSpan, Ct)) { }
		}), 1);

		Check(await Assert.ThrowsAsync<ValueMappingException>(async () =>
		{
			await foreach (Counted _ in Command([1], [null]).Map<Counted>().ToChannelReader(Ct).ReadAllAsync(Ct)) { }
		}), 1);
		Check(await Assert.ThrowsAsync<ValueMappingException>(() => Command([1], [null]).Map<Counted>().AsSourceBlock(Ct).Completion), 1);

		// An array mapper's caller owns the loop: no row.
		Func<object?[], Counted> fromArray = RowMap<Counted>.Default.ArrayMapper(["Id"]);
		Assert.Equal(3, fromArray([3]).Id);
		Check(Assert.Throws<ValueMappingException>(() => fromArray([null])), -1);
	}
}
