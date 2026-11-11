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

	// One member of one model: how to map a row whose only column is that member's (NULL, or a real cell) and read the member back, and what it must be in each case.
	sealed record Case(string Name, Func<Route, bool, object?> Read, object? OnNull, object? OnValue);

	// `cell` is the real value the column carries; the typed reader reports `columnType`, the others object.
	static Case Of<TModel>(string name, string column, Type columnType, object cell, Func<TModel, object?> member, object? onNull)
		=> new(name,
			(route, isNull) => member(Assert.Single(Read<TModel>(route, [new ColumnDef(column, columnType)], [isNull ? null : cell]))),
			onNull,
			cell);

	static readonly Case[] Cases =
	[
		// properties: what the model's initializer gave stays when the type declares the property non-nullable
		Of<PropsOn>("property: non-nullable value", nameof(PropsOn.Value), typeof(int), 1, m => m.Value, 7),
		Of<PropsOn>("property: Nullable<T>", nameof(PropsOn.Maybe), typeof(int), 2, m => m.Maybe, null),
		Of<PropsOn>("property: non-nullable reference with initializer", nameof(PropsOn.KeepsInit), typeof(string), "v", m => m.KeepsInit, "init"),
		Of<PropsOn>("property: non-nullable reference without initializer", nameof(PropsOn.NoInit), typeof(string), "v", m => m.NoInit, null),
		Of<PropsOn>("property: nullable reference", nameof(PropsOn.Nullable), typeof(string), "v", m => m.Nullable, null),
		Of<PropsOff>("property: oblivious reference", nameof(PropsOff.Oblivious), typeof(string), "v", m => m.Oblivious, null),
		Of<PropsGeneric<string>>("property: generic type parameter", nameof(PropsGeneric<>.Value), typeof(string), "v", m => m.Value, null),
		Of<PropsOn>("property: [AllowNull]", nameof(PropsOn.Allowed), typeof(string), "v", m => m.Allowed, null),

		// constructor parameters: the fallback (the declared default, else default) when the type declares the parameter non-nullable
		Of<ParamsOn>("parameter: non-nullable value", nameof(ParamsOn.Value), typeof(int), 1, m => m.Value, 7),
		Of<ParamsOn>("parameter: Nullable<T>", nameof(ParamsOn.Maybe), typeof(int), 2, m => m.Maybe, null),
		Of<ParamsOn>("parameter: non-nullable reference with default", nameof(ParamsOn.Keeps), typeof(string), "v", m => m.Keeps, "d"),
		Of<ParamsOn>("parameter: non-nullable reference without default", nameof(ParamsOn.NoDefault), typeof(string), "v", m => m.NoDefault, null),
		Of<ParamsOn>("parameter: nullable reference", nameof(ParamsOn.Nullable), typeof(string), "v", m => m.Nullable, null),
		Of<ParamsOff>("parameter: oblivious reference", nameof(ParamsOff.Oblivious), typeof(string), "v", m => m.Oblivious, null),
		Of<ParamsGeneric<string>>("parameter: generic type parameter", nameof(ParamsGeneric<>.Value), typeof(string), "v", m => m.Value, null),
		Of<Defaulted>("parameter: positional record, non-nullable reference with default", nameof(Defaulted.Title), typeof(string), "v", m => m.Title, "n/a"),
		Of<Titled>("parameter: positional record, non-nullable reference without default", nameof(Titled.Title), typeof(string), "v", m => m.Title, null),
	];

	static readonly Dictionary<string, Case> ByName = Cases.ToDictionary(c => c.Name);

	public static TheoryData<string, Route, bool> Matrix
	{
		get
		{
			var data = new TheoryData<string, Route, bool>();
			foreach (Case c in Cases)
			{
				foreach (Route route in (Route[])[Route.Typed, Route.Boxed, Route.Buffered])
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
	// A member declared as a type parameter is left out on purpose: the library calls it oblivious, which is today's rule, whatever NullabilityInfoContext says of the closed type.
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
				foreach (Route route in (Route[])[Route.Typed, Route.Boxed, Route.Buffered])
					data.Add(name, route);
			}

			return data;
		}
	}

	[Theory]
	[MemberData(nameof(Agreement))]
	public static void ANullKeepsAMember_ExactlyWhenNullabilityInfoContextSaysItIsNotNull(string name, Route route)
	{
		bool declaredNotNull = Runtime[name]() == NullabilityState.NotNull;

		// Every one of these has a non-null initializer or default, so a result that is not null is a NULL that kept it.
		bool kept = ByName[name].Read(route, true) is not null;
		Assert.Equal(declaredNotNull, kept);
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

	static readonly object?[] PropsOnNulls = [null, null, null, null, null, null];
	static readonly object?[] ParamsOnNulls = [null, null, null, null, null];

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void EveryPropertyNull_KeepsWhatTheTypeDeclaresNonNullable(Route route)
	{
		PropsOn item = Assert.Single(Read<PropsOn>(route, PropsOnColumns, PropsOnNulls));
		Assert.Equal((7, null, "init", null, null, null), (item.Value, item.Maybe, item.KeepsInit, item.NoInit, item.Nullable, item.Allowed));

		PropsOff off = Assert.Single(Read<PropsOff>(route, [("Oblivious", typeof(string))], [null]));
		Assert.Null(off.Oblivious);
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void EveryParameterNull_GetsTheFallbackOfWhatTheTypeDeclaresNonNullable(Route route)
	{
		ParamsOn item = Assert.Single(Read<ParamsOn>(route, ParamsOnColumns, ParamsOnNulls));
		Assert.Equal((null, 7, null, "d", null), (item.NoDefault, item.Value, item.Maybe, item.Keeps, item.Nullable));

		ParamsOff off = Assert.Single(Read<ParamsOff>(route, [("Oblivious", typeof(string))], [null]));
		Assert.Null(off.Oblivious);
	}

	[Fact]
	public static void ADataTable_FollowsTheSameRule()
	{
		DataTable props = new();
		foreach (ColumnDef column in PropsOnColumns) props.Columns.Add(column.Name, column.Type);
		props.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		PropsOn item = Assert.Single(props.To<PropsOn>());
#pragma warning restore CS0618
		Assert.Equal((7, null, "init", null, null, null), (item.Value, item.Maybe, item.KeepsInit, item.NoInit, item.Nullable, item.Allowed));

		DataTable parameters = new();
		foreach (ColumnDef column in ParamsOnColumns) parameters.Columns.Add(column.Name, column.Type);
		parameters.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		ParamsOn built = Assert.Single(parameters.To<ParamsOn>());
#pragma warning restore CS0618
		Assert.Equal((null, 7, null, "d", null), (built.NoDefault, built.Value, built.Maybe, built.Keeps, built.Nullable));

		DataTable named = new();
		named.Columns.Add("Name", typeof(string));
		named.Rows.Add(DBNull.Value);
		named.Rows.Add("given");
#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Equal(["", "given"], named.To<Named>().Select(n => n.Name));
#pragma warning restore CS0618

		DataTable defaulted = new();
		defaulted.Columns.Add("Id", typeof(int));
		defaulted.Columns.Add("Title", typeof(string));
		defaulted.Rows.Add(1, DBNull.Value);
		defaulted.Rows.Add(2, "given");
#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Equal([new(1, "n/a"), new(2, "given")], defaulted.To<Defaulted>());
#pragma warning restore CS0618
	}

	// ---- the cases the owner named ----

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void AStringPropertyWithAnEmptyInitializer_KeepsItWhenTheColumnIsNull(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Name", typeof(string))];
		Assert.Equal(["", "given"], ReadRows<Named>(route, columns, [[null], ["given"]]).Select(n => n.Name));
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APositionalRecord_WithAStringParameter_GetsNull_WhenTheColumnIsNull(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Title", typeof(string))];
		Assert.Equal<Titled>([new(1, null!), new(2, "given")], ReadRows<Titled>(route, columns, [[1, null], [2, "given"]])); // no declared default: the fallback is default(string)
	}

	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void APositionalRecord_WithADefaultedStringParameter_GetsTheDefault_WhenTheColumnIsNull(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Title", typeof(string))];
		Assert.Equal<Defaulted>([new(1, "n/a"), new(2, "given")], ReadRows<Defaulted>(route, columns, [[1, null], [2, "given"]]));

		// No column at all gets the default too, as before.
		Assert.Equal(new Defaulted(3, "n/a"), Assert.Single(Read<Defaulted>(route, [("Id", typeof(int))], [3])));
	}

	// Phase D enforces that the COLUMN is there; a NULL in it is not an error, and a required property is a property like any other: it keeps its initializer, else it stays null.
	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ARequiredStringProperty_WithANullCell_IsNotAnError_AndKeepsWhatItHad(Route route)
	{
		ImmutableArray<ColumnDef> columns = [("Name", typeof(string)), ("Label", typeof(string))];

		RequiredNotNull item = Assert.Single(Read<RequiredNotNull>(route, columns, [null, null]));
		Assert.Null(item.Name);          // nothing assigned it: it is null, as it was
		Assert.Equal("init", item.Label); // the initializer stays

		RequiredNotNull given = Assert.Single(Read<RequiredNotNull>(route, columns, ["n", "l"]));
		Assert.Equal(("n", "l"), (given.Name, given.Label));
	}

	// A map changes which column feeds a member, not what a NULL does to it.
	[Theory]
	[MemberData(nameof(Routes), MemberType = typeof(ConstructorBindingTests))]
	public static void ADeclaredMap_FollowsTheSameRule(Route route)
	{
		RowMap<Named> named = [new(nameof(Named.Name), "full_name")];
		Assert.Equal("", Assert.Single(Read(route, [("full_name", typeof(string))], [null], named)).Name);

		RowMap<Defaulted> defaulted = [new(nameof(Defaulted.Title), "post_title")];
		Assert.Equal("n/a", Assert.Single(Read(route, [("Id", typeof(int)), ("post_title", typeof(string))], [1, null], defaulted)).Title);
	}
}
