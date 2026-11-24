using System.Reflection;
using static Open.Database.Extensions.Tests.ConstructorBindingTests;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Phase E, T6: a NULL never overwrites a member the C# type declares non-nullable (nullable annotations on).
//   - a value type other than Nullable<T>: a property keeps what its initializer gave it, a constructor parameter gets its fallback (the declared default, else default);
//   - Nullable<T>, a reference declared nullable (`string?`, `[AllowNull]` on a property), a reference in a context without nullable annotations, and a member whose type is a type parameter: null;
//   - a reference declared non-nullable (`string`): a property keeps its initializer (null when it has none), a constructor parameter gets its fallback.
// The matrix is {property, constructor parameter} x {the kinds of member} x {NULL cell, real cell} x {a typed reader, a reader reporting object, buffered object[] rows}.
// The reader of the compiler's annotations is internal to the library, so its agreement with the runtime's own NullabilityInfoContext is checked by what it does: a NULL keeps the member exactly
// when NullabilityInfoContext says its write state is NotNull. The Core test project targets net10.0 only, where NullabilityInfoContext exists; the library reads the metadata by hand on every target.
[ExcludeFromCodeCoverage]
public static class NullRuleMatrixTests
{
	// ---- the models with nullable annotations ON ----

	public sealed class PropsOn
	{
		public int Value { get; set; } = 7;                          // non-nullable value type
		public int? Maybe { get; set; } = 5;                         // Nullable<T>
		public string KeepsInit { get; set; } = "init";              // reference declared non-nullable, with an initializer
#pragma warning disable CS8618 // Deliberately no initializer: a non-nullable reference with none.
		public string NoInit { get; set; }                           // reference declared non-nullable, without one
#pragma warning restore CS8618
		public string? Nullable { get; set; } = "init";              // reference declared nullable
		[AllowNull] public string Allowed { get; set; } = "init";    // [AllowNull]: null may be written to it
	}

	// A type parameter: T is a string here, but the member is declared `T`, which the annotations cannot call non-nullable.
	public sealed class PropsGeneric<T>
	{
		public PropsGeneric() => Value = (T)(object)"init";

		public T Value { get; set; }
	}

	// The parameters match a get-only property each (ignoring case): one member, fed through the constructor.
	public sealed class ParamsOn(string noDefault, int value = 7, int? maybe = 5, string keeps = "d", string? nullable = "d")
	{
		public string NoDefault { get; } = noDefault;                // reference declared non-nullable, no declared default
		public int Value { get; } = value;                           // non-nullable value type
		public int? Maybe { get; } = maybe;                          // Nullable<T>
		public string Keeps { get; } = keeps;                        // reference declared non-nullable, with a declared default
		public string? Nullable { get; } = nullable;                 // reference declared nullable
	}

	public sealed class ParamsGeneric<T>(T value)
	{
		public T Value { get; } = value;
	}

	public sealed record Titled(int Id, string Title);

	public sealed record Defaulted(int Id, string Title = "n/a");

	public sealed class Named
	{
		public string Name { get; set; } = "";
	}

	public sealed class RequiredNotNull
	{
		public required string Name { get; set; }
		public required string Label { get; set; } = "init";
	}

	// ---- the models with nullable annotations OFF: a reference type is oblivious, so a NULL is null as before ----

#nullable disable
	public sealed class PropsOff
	{
		public string Oblivious { get; set; } = "init";
	}

	public sealed class ParamsOff(string oblivious = "d")
	{
		public string Oblivious { get; } = oblivious;
	}
#nullable enable

	// ---- the table ----

	/// <summary>What a NULL into a member that rejects it gives: no value, the row throws.</summary>
	static readonly object Rejected = new();

	// One member of one model: how to map a row whose only column is that member's (NULL, or a real cell) and read the member back, and what it must be in each case.
	sealed record Case(string Name, Func<Route, bool, object?> Read, object? OnNull, object? OnValue);

	// `cell` is the real value the column carries; the typed reader reports `columnType`, the others object.
	// A member that rejects NULL is read through NullAssert: the row throws, naming the model and row 0.
	static Case Of<TModel>(string name, string column, Type columnType, object cell, Func<TModel, object?> member, object? onNull)
		=> new(name,
			(route, isNull) =>
			{
				List<TModel> Rows() => Read<TModel>(route, [new ColumnDef(column, columnType)], [isNull ? null : cell]);
				if (!isNull || onNull != Rejected) return member(Assert.Single(Rows()));

				NullAssert.Model<TModel>(0, () => Rows());
				return Rejected;
			},
			onNull,
			cell);

	static readonly Case[] Cases =
	[
		// properties: a non-nullable one rejects NULL, whatever its initializer
		Of<PropsOn>("property: non-nullable value", nameof(PropsOn.Value), typeof(int), 1, m => m.Value, Rejected),
		Of<PropsOn>("property: Nullable<T>", nameof(PropsOn.Maybe), typeof(int), 2, m => m.Maybe, null),
		Of<PropsOn>("property: non-nullable reference with initializer", nameof(PropsOn.KeepsInit), typeof(string), "v", m => m.KeepsInit, Rejected),
		Of<PropsOn>("property: non-nullable reference without initializer", nameof(PropsOn.NoInit), typeof(string), "v", m => m.NoInit, Rejected),
		Of<PropsOn>("property: nullable reference", nameof(PropsOn.Nullable), typeof(string), "v", m => m.Nullable, null),
		Of<PropsOff>("property: oblivious reference", nameof(PropsOff.Oblivious), typeof(string), "v", m => m.Oblivious, null),
		Of<PropsGeneric<string>>("property: generic type parameter", nameof(PropsGeneric<>.Value), typeof(string), "v", m => m.Value, null),
		Of<PropsOn>("property: [AllowNull]", nameof(PropsOn.Allowed), typeof(string), "v", m => m.Allowed, null),

		// constructor parameters: a non-nullable one rejects NULL, whatever its declared default (which feeds only a parameter no column feeds)
		Of<ParamsOn>("parameter: non-nullable value", nameof(ParamsOn.Value), typeof(int), 1, m => m.Value, Rejected),
		Of<ParamsOn>("parameter: Nullable<T>", nameof(ParamsOn.Maybe), typeof(int), 2, m => m.Maybe, null),
		Of<ParamsOn>("parameter: non-nullable reference with default", nameof(ParamsOn.Keeps), typeof(string), "v", m => m.Keeps, Rejected),
		Of<ParamsOn>("parameter: non-nullable reference without default", nameof(ParamsOn.NoDefault), typeof(string), "v", m => m.NoDefault, Rejected),
		Of<ParamsOn>("parameter: nullable reference", nameof(ParamsOn.Nullable), typeof(string), "v", m => m.Nullable, null),
		Of<ParamsOff>("parameter: oblivious reference", nameof(ParamsOff.Oblivious), typeof(string), "v", m => m.Oblivious, null),
		Of<ParamsGeneric<string>>("parameter: generic type parameter", nameof(ParamsGeneric<>.Value), typeof(string), "v", m => m.Value, null),
		Of<Defaulted>("parameter: positional record, non-nullable reference with default", nameof(Defaulted.Title), typeof(string), "v", m => m.Title, Rejected),
		Of<Titled>("parameter: positional record, non-nullable reference without default", nameof(Titled.Title), typeof(string), "v", m => m.Title, Rejected),
	];

	static readonly Dictionary<string, Case> ByName = Cases.ToDictionary(c => c.Name);

	public static TheoryData<string, Route, bool> Matrix
	{
		get
		{
			var data = new TheoryData<string, Route, bool>();
			foreach (Case c in Cases)
			{
				foreach (Route route in (Route[])[Route.Typed, Route.Boxed])
				{
					foreach (bool isNull in (bool[])[true, false])
						data.Add(c.Name, route, isNull);
				}
			}

			return data;
		}
	}

	[Theory]
	[MemberData(nameof(Matrix))]
	public static void ANullCell_FollowsTheDeclaredType_AndARealCellIsAlwaysUsed(string name, Route route, bool isNull)
	{
		Case c = ByName[name];
		Assert.Equal(isNull ? c.OnNull : c.OnValue, c.Read(route, isNull));
	}

	// ---- the reader agrees with the runtime's own reading of the annotations (net10.0: NullabilityInfoContext) ----

	static NullabilityState WriteState(PropertyInfo property) => new NullabilityInfoContext().Create(property).WriteState;

	static NullabilityState WriteState<T>(string parameter)
		=> new NullabilityInfoContext().Create(typeof(T).GetConstructors().Single().GetParameters().Single(p => p.Name == parameter)).WriteState;

	// The members the brief names: a string, a string? and an oblivious property, a record positional parameter, a string and a string? constructor parameter (and [AllowNull], which counts as nullable).
	// A member declared as a type parameter is left out on purpose: the library calls it oblivious, whatever NullabilityInfoContext says of the closed type.
	static readonly Dictionary<string, Func<NullabilityState>> Runtime = new()
	{
		["property: non-nullable reference with initializer"] = () => WriteState(typeof(PropsOn).GetProperty(nameof(PropsOn.KeepsInit))!),
		["property: nullable reference"] = () => WriteState(typeof(PropsOn).GetProperty(nameof(PropsOn.Nullable))!),
		["property: oblivious reference"] = () => WriteState(typeof(PropsOff).GetProperty(nameof(PropsOff.Oblivious))!),
		["property: [AllowNull]"] = () => WriteState(typeof(PropsOn).GetProperty(nameof(PropsOn.Allowed))!),
		["parameter: non-nullable reference with default"] = () => WriteState<ParamsOn>("keeps"),
		["parameter: nullable reference"] = () => WriteState<ParamsOn>("nullable"),
		["parameter: oblivious reference"] = () => WriteState<ParamsOff>("oblivious"),
		["parameter: positional record, non-nullable reference with default"] = () => WriteState<Defaulted>(nameof(Defaulted.Title)),
	};

	public static TheoryData<string, Route> Agreement
	{
		get
		{
			var data = new TheoryData<string, Route>();
			foreach (string name in Runtime.Keys)
			{
				foreach (Route route in (Route[])[Route.Typed, Route.Boxed])
					data.Add(name, route);
			}

			return data;
		}
	}

	[Theory]
	[MemberData(nameof(Agreement))]
	public static void ANullIsRejected_ExactlyWhenNullabilityInfoContextSaysTheMemberIsNotNull(string name, Route route)
	{
		bool declaredNotNull = Runtime[name]() == NullabilityState.NotNull;
		bool rejected = ByName[name].Read(route, true) == Rejected;
		Assert.Equal(declaredNotNull, rejected);
	}

	// The two states NullabilityInfoContext reports for a plain `string` and `string?`, so the table above cannot pass by agreeing on Unknown for everything.
	[Fact]
	public static void NullabilityInfoContext_TellsTheModelsApart()
	{
		Assert.Equal(NullabilityState.NotNull, Runtime["property: non-nullable reference with initializer"]());
		Assert.Equal(NullabilityState.Nullable, Runtime["property: nullable reference"]());
		Assert.Equal(NullabilityState.Unknown, Runtime["property: oblivious reference"]());
		Assert.Equal(NullabilityState.Nullable, Runtime["property: [AllowNull]"]());
		Assert.Equal(NullabilityState.NotNull, Runtime["parameter: non-nullable reference with default"]());
		Assert.Equal(NullabilityState.Nullable, Runtime["parameter: nullable reference"]());
		Assert.Equal(NullabilityState.Unknown, Runtime["parameter: oblivious reference"]());
		Assert.Equal(NullabilityState.NotNull, Runtime["parameter: positional record, non-nullable reference with default"]());
	}

	// ---- every member NULL at once, on every route ----

	static readonly ImmutableArray<ColumnDef> PropsOnColumns =
	[
		("Value", typeof(int)), ("Maybe", typeof(int)), ("KeepsInit", typeof(string)), ("NoInit", typeof(string)), ("Nullable", typeof(string)), ("Allowed", typeof(string)),
	];

	static readonly ImmutableArray<ColumnDef> ParamsOnColumns =
	[
		("NoDefault", typeof(string)), ("Value", typeof(int)), ("Maybe", typeof(int)), ("Keeps", typeof(string)), ("Nullable", typeof(string)),
	];

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void EveryPropertyNull_IsAnError_AndEveryNullableOneNull_IsNull(Route route)
	{
		NullAssert.Model<PropsOn>(0, () => Read<PropsOn>(route, PropsOnColumns, [null, null, null, null, null, null]));

		PropsOn item = Assert.Single(Read<PropsOn>(route, PropsOnColumns, [1, null, "k", "n", null, null]));
		Assert.Equal((1, null, "k", "n", null, null), (item.Value, item.Maybe, item.KeepsInit, item.NoInit, item.Nullable, item.Allowed));

		PropsOff off = Assert.Single(Read<PropsOff>(route, [("Oblivious", typeof(string))], [null]));
		Assert.Null(off.Oblivious);
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void EveryParameterNull_IsAnError_AndEveryNullableOneNull_IsNull(Route route)
	{
		NullAssert.Model<ParamsOn>(0, () => Read<ParamsOn>(route, ParamsOnColumns, [null, null, null, null, null]));

		ParamsOn item = Assert.Single(Read<ParamsOn>(route, ParamsOnColumns, ["n", 1, null, "k", null]));
		Assert.Equal(("n", 1, null, "k", null), (item.NoDefault, item.Value, item.Maybe, item.Keeps, item.Nullable));

		ParamsOff off = Assert.Single(Read<ParamsOff>(route, [("Oblivious", typeof(string))], [null]));
		Assert.Null(off.Oblivious);
	}

	// ---- the cases the owner named ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void AStringPropertyWithAnInitializer_RejectsANullColumn_NamingTheRow(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Name", typeof(string))];
		NullAssert.Model<Named>(0, () => ReadRows<Named>(route, columns, [[null], ["given"]]));
		NullAssert.Model<Named>(2, () => ReadRows<Named>(route, columns, [["a"], ["b"], [null]]));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APositionalRecord_WithAStringParameter_RejectsANullColumn(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Title", typeof(string))];
		NullAssert.Model<Titled>(1, () => ReadRows<Titled>(route, columns, [[2, "given"], [1, null]]));
	}

	// The declared default feeds a parameter only when no column feeds it: a NULL in a column that does is an error.
	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APositionalRecord_WithADefaultedStringParameter_GetsTheDefault_OnlyWhenNoColumnFeedsIt(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Title", typeof(string))];
		NullAssert.Model<Defaulted>(0, () => ReadRows<Defaulted>(route, columns, [[1, null], [2, "given"]]));
		Assert.Equal(new Defaulted(2, "given"), Assert.Single(Read<Defaulted>(route, columns, [2, "given"])));

		// No column at all gets the default.
		Assert.Equal(new Defaulted(3, "n/a"), Assert.Single(Read<Defaulted>(route, [("Id", typeof(int))], [3])));
	}

	// A required property needs its column, and a NULL in it is the NULL error like any other non-nullable member's.
	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ARequiredStringProperty_WithANullCell_IsAnError(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Name", typeof(string)), ("Label", typeof(string))];

		NullAssert.Model<RequiredNotNull>(0, () => Read<RequiredNotNull>(route, columns, [null, "l"]));
		NullAssert.Model<RequiredNotNull>(0, () => Read<RequiredNotNull>(route, columns, ["n", null]));

		RequiredNotNull given = Assert.Single(Read<RequiredNotNull>(route, columns, ["n", "l"]));
		Assert.Equal(("n", "l"), (given.Name, given.Label));
	}

	// A map changes which column feeds a member, not what a NULL does to it.
	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ADeclaredMap_FollowsTheSameRule(Route route)
	{
		RowMap<Named> named = [new(nameof(Named.Name), "full_name")];
		NullAssert.Model<Named>(0, () => Read(route, [("full_name", typeof(string))], [null], named));

		RowMap<Defaulted> defaulted = [new(nameof(Defaulted.Title), "post_title")];
		NullAssert.Model<Defaulted>(0, () => Read(route, [("Id", typeof(int)), ("post_title", typeof(string))], [1, null], defaulted));
	}
}