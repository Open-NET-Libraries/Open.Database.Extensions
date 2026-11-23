#nullable enable

namespace Open.Database.Extensions.Tests.NullPolicyMatrix;

// The NullPolicy parity matrix: what every reader of a row (the runtime reader, and a generated one) must do with a NULL.
// It is data only (no test framework, no reader): a test project links this folder and drives each case through its own reader.
// Axes: the declaration (no attribute, None, Throw, Default on the type or the member, a member overriding the type,
// and [NullValue] alone, under each type policy, and beside the member's own policy),
// the target (property, required init property, constructor parameter, positional record parameter),
// the kind of member (int, int?, string, string?, an enum, Guid, DateTime), and the data (a value, a NULL alone, first, in the middle, last).
// Every case feeds all seven columns; the case's own column holds the data, the others a value.

/// <summary>An enum member of the matrix models.</summary>
public enum Tier { Low, Mid, High }

/// <summary>What a NULL does to a member once its policy is resolved.</summary>
public enum Effective
{
	/// <summary>No test: a nullable member is null, a non-nullable one fails with the type's NULL rule (model and row only).</summary>
	Unchecked,

	/// <summary>A NULL throws, naming the member, the column and the row.</summary>
	Throw,

	/// <summary>A NULL leaves the initializer, or gives a parameter its declared default.</summary>
	Default,

	/// <summary>A NULL gives the member's <c>[NullValue]</c>.</summary>
	Constant,
}

/// <summary>Where the member is.</summary>
public enum Target { Prop, Init, Ctor, Record }

/// <summary>Where the NULL is, if anywhere.</summary>
public enum Data { Value, NullOnly, NullFirst, NullMid, NullLast }

/// <summary>What the case expects of its column's NULL.</summary>
public enum Outcome { Value, Null, Default, Constant, NullRuleError, PolicyError }

/// <summary>One row of <see cref="Matrix.Policies"/>.</summary>
public sealed class Policy(string name, string onType, string onMember, Effective effective)
{
	public string Name { get; } = name;
	public string OnType { get; } = onType;
	public string OnMember { get; } = onMember;
	public Effective Effective { get; } = effective;
}

/// <summary>One row of <see cref="Matrix.Kinds"/>: a member, its column, and the values it can take.</summary>
public sealed class Kind(string member, Type memberType, Type columnType, bool nullable, object cell, object? value, object? @default, object? constant)
{
	public string Member { get; } = member;
	public Type MemberType { get; } = memberType;
	public Type ColumnType { get; } = columnType;
	public bool Nullable { get; } = nullable;

	/// <summary>The cell a row holds when it is not NULL.</summary>
	public object Cell { get; } = cell;

	/// <summary>What <see cref="Cell"/> maps to.</summary>
	public object? Value { get; } = value;

	/// <summary>The initializer, and the declared default of a parameter.</summary>
	public object? Default { get; } = @default;

	/// <summary>The constant of <c>[NullValue(...)]</c>, converted to the member's type.</summary>
	public object? Constant { get; } = constant;
}

/// <summary>The error a case expects.</summary>
public sealed class ExpectedError(string message, string propertyName, string columnName, Type targetType, Type? innerType)
{
	public string Message { get; } = message;
	public string PropertyName { get; } = propertyName;
	public string ColumnName { get; } = columnName;
	public Type TargetType { get; } = targetType;

	/// <summary>The type of the inner exception (a provider whose typed getters throw SqlNullValueException on NULL, as SqlClient's do), or null for none.</summary>
	public Type? InnerType { get; } = innerType;
}

/// <summary>A model whose <c>[NullValue]</c> does not fit its member, and the message of the <see cref="ArgumentException"/> creating its map throws.</summary>
public sealed class Misfit(Type model, string message)
{
	public Type Model { get; } = model;
	public string Message { get; } = message;
}

/// <summary>One case of the matrix.</summary>
public sealed class Case(Policy policy, Target target, Kind kind, Data data)
{
	public Policy Policy { get; } = policy;
	public Target Target { get; } = target;
	public Kind Kind { get; } = kind;
	public Data Data { get; } = data;

	public string Name => $"{Policy.Name}/{Target}/{Kind.Member}/{Data}";

	public Type Model => typeof(Matrix).GetNestedType($"P_{Policy.Name}_{Target}") ?? throw new InvalidOperationException("No model for " + Name);

	/// <summary>The 0-based row of the NULL, or -1 for none.</summary>
	public int NullRow => Data switch
	{
		Data.NullOnly or Data.NullFirst => 0,
		Data.NullMid => 1,
		Data.NullLast => 2,
		_ => -1,
	};

	/// <summary>The rows: every column holds a value except the case's own column in <see cref="NullRow"/>.</summary>
	public object?[][] Rows
	{
		get
		{
			int count = Data is Data.Value or Data.NullOnly ? 1 : 3;
			var rows = new object?[count][];
			for (int r = 0; r < count; r++)
			{
				rows[r] = new object?[Matrix.Kinds.Length];
				for (int c = 0; c < Matrix.Kinds.Length; c++)
					rows[r][c] = r == NullRow && Matrix.Kinds[c] == Kind ? null : Matrix.Kinds[c].Cell;
			}

			return rows;
		}
	}

	public Outcome Outcome => NullRow < 0 ? Outcome.Value : Matrix.OnNull(Policy.Effective, Kind.Nullable);

	/// <summary>The value of the case's member in each row, when no row fails.</summary>
	public object?[] Values
	{
		get
		{
			object?[] values = new object?[Rows.Length];
			for (int r = 0; r < values.Length; r++)
			{
				values[r] = r != NullRow ? Kind.Value : Outcome switch
				{
					Outcome.Null => null,
					Outcome.Default => Kind.Default,
					Outcome.Constant => Kind.Constant,
					_ => throw new InvalidOperationException(Name + " fails; it has no values."),
				};
			}

			return values;
		}
	}

	/// <summary>The error, or null when no row fails.</summary>
	public ExpectedError? Error => Outcome switch
	{
		Outcome.NullRuleError => new(
			$"A NULL was read into a non-nullable member of {Model.Name} at row {NullRow}. Declare the member nullable (int?, string?).",
			"",
			"",
			Model,
			typeof(System.Data.SqlTypes.SqlNullValueException)),
		Outcome.PolicyError => new(
			$"A NULL was read from column '{Kind.Member}' into '{Kind.Member}' of {Model.Name} at row {NullRow}, whose null policy is Throw.",
			Kind.Member,
			Kind.Member,
			Kind.MemberType,
			null),
		_ => null,
	};
}

public static partial class Matrix
{
	// What each model declares, and what it makes of a NULL: a member's [NullValue] wins over any policy, else the member's own policy, else the type's;
	// None and no attribute add no test.
	public static readonly Policy[] Policies =
	[
		//  name                          on the type             on the member         what a NULL does
		new("NoAttr",                     "",                     "",                   Effective.Unchecked),
		new("MemberNone",                 "",                     "None",               Effective.Unchecked),
		new("MemberThrow",                "",                     "Throw",              Effective.Throw),
		new("MemberDefault",              "",                     "Default",            Effective.Default),
		new("MemberValue",                "",                     "value",              Effective.Constant),
		new("ClassNone",                  "None",                 "",                   Effective.Unchecked),
		new("ClassThrow",                 "Throw",                "",                   Effective.Throw),
		new("ClassDefault",               "Default",              "",                   Effective.Default),
		new("ClassThrowMemberNone",       "Throw",                "None",               Effective.Unchecked),
		new("ClassThrowMemberDefault",    "Throw",                "Default",            Effective.Default),
		new("ClassDefaultMemberThrow",    "Default",              "Throw",              Effective.Throw),
		new("ClassNoneMemberValue",       "None",                 "value",              Effective.Constant),
		new("ClassThrowMemberValue",      "Throw",                "value",              Effective.Constant),
		new("ClassDefaultMemberValue",    "Default",              "value",              Effective.Constant),
		new("MemberNoneAndValue",         "",                     "None, value",        Effective.Constant),
		new("MemberThrowAndValue",        "",                     "Throw, value",       Effective.Constant),
		new("MemberDefaultAndValue",      "",                     "Default, value",     Effective.Constant),
	];

	// A [NullValue] that does not fit its member: ArgumentException when the map is created, before any row. The models are in NullPolicyMatrix.Misfits.cs.
	public static readonly Misfit[] Misfits =
	[
		new(typeof(BadIntFromText), "The null value of 'A' of 'BadIntFromText', x (String), does not convert to Int32."),
		new(typeof(BadIntFromNull), "The null value of 'A' of 'BadIntFromNull', null, does not convert to Int32."),
		new(typeof(BadGuidFromText), "The null value of 'A' of 'BadGuidFromText', nope (String), does not convert to Guid."),
		new(typeof(BadDateFromText), "The null value of 'A' of 'BadDateFromText', not a date (String), does not convert to DateTime."),
		new(typeof(BadByteOverflow), "The null value of 'A' of 'BadByteOverflow', 300 (Int32), does not convert to Byte."),
		new(typeof(BadRecordParameter), "The null value of 'A' of 'BadRecordParameter', x (String), does not convert to Int32."),
	];

	static readonly Guid G1 = new("00000000-0000-0000-0000-000000000001");
	static readonly Guid G2 = new("00000000-0000-0000-0000-000000000002");

	// The members of every model, in column order.
	public static readonly Kind[] Kinds =
	[
		//  member  member type       column type       nullable  cell                       value                      default    constant
		new("I",    typeof(int),      typeof(int),      false,    1,                         1,                         7,         42),
		new("NI",   typeof(int?),     typeof(int),      true,     1,                         1,                         5,         9),
		new("S",    typeof(string),   typeof(string),   false,    "v",                       "v",                       "d",       "c"),
		new("NS",   typeof(string),   typeof(string),   true,     "v",                       "v",                       "d",       "c"),
		new("E",    typeof(Tier),     typeof(int),      false,    0,                         Tier.Low,                  Tier.Mid,  Tier.High),
		new("G",    typeof(Guid),     typeof(Guid),     false,    G1,                        G1,                        Guid.Empty, G2),
		new("T",    typeof(DateTime), typeof(DateTime), false,    new DateTime(2020, 1, 1),  new DateTime(2020, 1, 1),  default(DateTime), new DateTime(2024, 1, 2)),
	];

	/// <summary>What a NULL gives a member: the table every reader follows.</summary>
	public static Outcome OnNull(Effective effective, bool nullable) => effective switch
	{
		Effective.Unchecked => nullable ? Outcome.Null : Outcome.NullRuleError,
		Effective.Throw => Outcome.PolicyError,
		Effective.Default => Outcome.Default,
		_ => Outcome.Constant,
	};

	/// <summary>Every case: policies x targets x kinds x data.</summary>
	public static IEnumerable<Case> Cases()
	{
		foreach (Policy policy in Policies)
		{
			foreach (Target target in Targets)
			{
				foreach (Kind kind in Kinds)
				{
					foreach (Data data in Datas)
						yield return new Case(policy, target, kind, data);
				}
			}
		}
	}

	// Listed, not Enum.GetValues: a linking project may target a framework without the generic overload.
	static readonly Target[] Targets = [Target.Prop, Target.Init, Target.Ctor, Target.Record];

	static readonly Data[] Datas = [Data.Value, Data.NullOnly, Data.NullFirst, Data.NullMid, Data.NullLast];
}
