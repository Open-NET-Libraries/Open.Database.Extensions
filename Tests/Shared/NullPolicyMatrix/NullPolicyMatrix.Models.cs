// The models of the matrix: one per row of Policies x Targets in NullPolicyMatrix.cs, one member per row of Kinds; keep the three in step.
// One model per (policy, target): P_<policy>_<target>, seven members, one per kind. Default = the initializer, and the declared default of a parameter.
#nullable enable

namespace Open.Database.Extensions.Tests.NullPolicyMatrix;

public static partial class Matrix
{
	public sealed class P_NoAttr_Prop
	{
		public int I { get; set; } = 7;
		public int? NI { get; set; } = 5;
		public string S { get; set; } = "d";
		public string? NS { get; set; } = "d";
		public Tier E { get; set; } = Tier.Mid;
		public Guid G { get; set; }
		public DateTime T { get; set; }
	}

	public sealed class P_NoAttr_Init
	{
		public required int I { get; init; } = 7;
		public required int? NI { get; init; } = 5;
		public required string S { get; init; } = "d";
		public required string? NS { get; init; } = "d";
		public required Tier E { get; init; } = Tier.Mid;
		public required Guid G { get; init; }
		public required DateTime T { get; init; }
	}

	public sealed class P_NoAttr_Ctor(
		int i = 7,
		int? ni = 5,
		string s = "d",
		string? ns = "d",
		Tier e = Tier.Mid,
		Guid g = default,
		DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_NoAttr_Record(
		int I = 7,
		int? NI = 5,
		string S = "d",
		string? NS = "d",
		Tier E = Tier.Mid,
		Guid G = default,
		DateTime T = default);

	public sealed class P_MemberNone_Prop
	{
		[NullPolicy(NullAction.None)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.None)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.None)] public string S { get; set; } = "d";
		[NullPolicy(NullAction.None)] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.None)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.None)] public Guid G { get; set; }
		[NullPolicy(NullAction.None)] public DateTime T { get; set; }
	}

	public sealed class P_MemberNone_Init
	{
		[NullPolicy(NullAction.None)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.None)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.None)] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.None)] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.None)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.None)] public required Guid G { get; init; }
		[NullPolicy(NullAction.None)] public required DateTime T { get; init; }
	}

	public sealed class P_MemberNone_Ctor(
		[NullPolicy(NullAction.None)] int i = 7,
		[NullPolicy(NullAction.None)] int? ni = 5,
		[NullPolicy(NullAction.None)] string s = "d",
		[NullPolicy(NullAction.None)] string? ns = "d",
		[NullPolicy(NullAction.None)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.None)] Guid g = default,
		[NullPolicy(NullAction.None)] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_MemberNone_Record(
		[NullPolicy(NullAction.None)] int I = 7,
		[NullPolicy(NullAction.None)] int? NI = 5,
		[NullPolicy(NullAction.None)] string S = "d",
		[NullPolicy(NullAction.None)] string? NS = "d",
		[NullPolicy(NullAction.None)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.None)] Guid G = default,
		[NullPolicy(NullAction.None)] DateTime T = default);

	public sealed class P_MemberThrow_Prop
	{
		[NullPolicy(NullAction.Throw)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.Throw)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.Throw)] public string S { get; set; } = "d";
		[NullPolicy(NullAction.Throw)] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.Throw)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.Throw)] public Guid G { get; set; }
		[NullPolicy(NullAction.Throw)] public DateTime T { get; set; }
	}

	public sealed class P_MemberThrow_Init
	{
		[NullPolicy(NullAction.Throw)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.Throw)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.Throw)] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.Throw)] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.Throw)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.Throw)] public required Guid G { get; init; }
		[NullPolicy(NullAction.Throw)] public required DateTime T { get; init; }
	}

	public sealed class P_MemberThrow_Ctor(
		[NullPolicy(NullAction.Throw)] int i = 7,
		[NullPolicy(NullAction.Throw)] int? ni = 5,
		[NullPolicy(NullAction.Throw)] string s = "d",
		[NullPolicy(NullAction.Throw)] string? ns = "d",
		[NullPolicy(NullAction.Throw)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.Throw)] Guid g = default,
		[NullPolicy(NullAction.Throw)] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_MemberThrow_Record(
		[NullPolicy(NullAction.Throw)] int I = 7,
		[NullPolicy(NullAction.Throw)] int? NI = 5,
		[NullPolicy(NullAction.Throw)] string S = "d",
		[NullPolicy(NullAction.Throw)] string? NS = "d",
		[NullPolicy(NullAction.Throw)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.Throw)] Guid G = default,
		[NullPolicy(NullAction.Throw)] DateTime T = default);

	public sealed class P_MemberDefault_Prop
	{
		[NullPolicy(NullAction.Default)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.Default)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.Default)] public string S { get; set; } = "d";
		[NullPolicy(NullAction.Default)] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.Default)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.Default)] public Guid G { get; set; }
		[NullPolicy(NullAction.Default)] public DateTime T { get; set; }
	}

	public sealed class P_MemberDefault_Init
	{
		[NullPolicy(NullAction.Default)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.Default)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.Default)] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.Default)] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.Default)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.Default)] public required Guid G { get; init; }
		[NullPolicy(NullAction.Default)] public required DateTime T { get; init; }
	}

	public sealed class P_MemberDefault_Ctor(
		[NullPolicy(NullAction.Default)] int i = 7,
		[NullPolicy(NullAction.Default)] int? ni = 5,
		[NullPolicy(NullAction.Default)] string s = "d",
		[NullPolicy(NullAction.Default)] string? ns = "d",
		[NullPolicy(NullAction.Default)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.Default)] Guid g = default,
		[NullPolicy(NullAction.Default)] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_MemberDefault_Record(
		[NullPolicy(NullAction.Default)] int I = 7,
		[NullPolicy(NullAction.Default)] int? NI = 5,
		[NullPolicy(NullAction.Default)] string S = "d",
		[NullPolicy(NullAction.Default)] string? NS = "d",
		[NullPolicy(NullAction.Default)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.Default)] Guid G = default,
		[NullPolicy(NullAction.Default)] DateTime T = default);

	public sealed class P_MemberValue_Prop
	{
		[NullValue(42)] public int I { get; set; } = 7;
		[NullValue(9)] public int? NI { get; set; } = 5;
		[NullValue("c")] public string S { get; set; } = "d";
		[NullValue("c")] public string? NS { get; set; } = "d";
		[NullValue(2)] public Tier E { get; set; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public Guid G { get; set; }
		[NullValue("2024-01-02")] public DateTime T { get; set; }
	}

	public sealed class P_MemberValue_Init
	{
		[NullValue(42)] public required int I { get; init; } = 7;
		[NullValue(9)] public required int? NI { get; init; } = 5;
		[NullValue("c")] public required string S { get; init; } = "d";
		[NullValue("c")] public required string? NS { get; init; } = "d";
		[NullValue(2)] public required Tier E { get; init; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public required Guid G { get; init; }
		[NullValue("2024-01-02")] public required DateTime T { get; init; }
	}

	public sealed class P_MemberValue_Ctor(
		[NullValue(42)] int i = 7,
		[NullValue(9)] int? ni = 5,
		[NullValue("c")] string s = "d",
		[NullValue("c")] string? ns = "d",
		[NullValue(2)] Tier e = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid g = default,
		[NullValue("2024-01-02")] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_MemberValue_Record(
		[NullValue(42)] int I = 7,
		[NullValue(9)] int? NI = 5,
		[NullValue("c")] string S = "d",
		[NullValue("c")] string? NS = "d",
		[NullValue(2)] Tier E = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid G = default,
		[NullValue("2024-01-02")] DateTime T = default);

	[NullPolicy(NullAction.None)]
	public sealed class P_ClassNone_Prop
	{
		public int I { get; set; } = 7;
		public int? NI { get; set; } = 5;
		public string S { get; set; } = "d";
		public string? NS { get; set; } = "d";
		public Tier E { get; set; } = Tier.Mid;
		public Guid G { get; set; }
		public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.None)]
	public sealed class P_ClassNone_Init
	{
		public required int I { get; init; } = 7;
		public required int? NI { get; init; } = 5;
		public required string S { get; init; } = "d";
		public required string? NS { get; init; } = "d";
		public required Tier E { get; init; } = Tier.Mid;
		public required Guid G { get; init; }
		public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.None)]
	public sealed class P_ClassNone_Ctor(
		int i = 7,
		int? ni = 5,
		string s = "d",
		string? ns = "d",
		Tier e = Tier.Mid,
		Guid g = default,
		DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.None)]
	public sealed record P_ClassNone_Record(
		int I = 7,
		int? NI = 5,
		string S = "d",
		string? NS = "d",
		Tier E = Tier.Mid,
		Guid G = default,
		DateTime T = default);

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrow_Prop
	{
		public int I { get; set; } = 7;
		public int? NI { get; set; } = 5;
		public string S { get; set; } = "d";
		public string? NS { get; set; } = "d";
		public Tier E { get; set; } = Tier.Mid;
		public Guid G { get; set; }
		public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrow_Init
	{
		public required int I { get; init; } = 7;
		public required int? NI { get; init; } = 5;
		public required string S { get; init; } = "d";
		public required string? NS { get; init; } = "d";
		public required Tier E { get; init; } = Tier.Mid;
		public required Guid G { get; init; }
		public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrow_Ctor(
		int i = 7,
		int? ni = 5,
		string s = "d",
		string? ns = "d",
		Tier e = Tier.Mid,
		Guid g = default,
		DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.Throw)]
	public sealed record P_ClassThrow_Record(
		int I = 7,
		int? NI = 5,
		string S = "d",
		string? NS = "d",
		Tier E = Tier.Mid,
		Guid G = default,
		DateTime T = default);

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefault_Prop
	{
		public int I { get; set; } = 7;
		public int? NI { get; set; } = 5;
		public string S { get; set; } = "d";
		public string? NS { get; set; } = "d";
		public Tier E { get; set; } = Tier.Mid;
		public Guid G { get; set; }
		public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefault_Init
	{
		public required int I { get; init; } = 7;
		public required int? NI { get; init; } = 5;
		public required string S { get; init; } = "d";
		public required string? NS { get; init; } = "d";
		public required Tier E { get; init; } = Tier.Mid;
		public required Guid G { get; init; }
		public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefault_Ctor(
		int i = 7,
		int? ni = 5,
		string s = "d",
		string? ns = "d",
		Tier e = Tier.Mid,
		Guid g = default,
		DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.Default)]
	public sealed record P_ClassDefault_Record(
		int I = 7,
		int? NI = 5,
		string S = "d",
		string? NS = "d",
		Tier E = Tier.Mid,
		Guid G = default,
		DateTime T = default);

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberNone_Prop
	{
		[NullPolicy(NullAction.None)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.None)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.None)] public string S { get; set; } = "d";
		[NullPolicy(NullAction.None)] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.None)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.None)] public Guid G { get; set; }
		[NullPolicy(NullAction.None)] public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberNone_Init
	{
		[NullPolicy(NullAction.None)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.None)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.None)] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.None)] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.None)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.None)] public required Guid G { get; init; }
		[NullPolicy(NullAction.None)] public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberNone_Ctor(
		[NullPolicy(NullAction.None)] int i = 7,
		[NullPolicy(NullAction.None)] int? ni = 5,
		[NullPolicy(NullAction.None)] string s = "d",
		[NullPolicy(NullAction.None)] string? ns = "d",
		[NullPolicy(NullAction.None)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.None)] Guid g = default,
		[NullPolicy(NullAction.None)] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.Throw)]
	public sealed record P_ClassThrowMemberNone_Record(
		[NullPolicy(NullAction.None)] int I = 7,
		[NullPolicy(NullAction.None)] int? NI = 5,
		[NullPolicy(NullAction.None)] string S = "d",
		[NullPolicy(NullAction.None)] string? NS = "d",
		[NullPolicy(NullAction.None)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.None)] Guid G = default,
		[NullPolicy(NullAction.None)] DateTime T = default);

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberDefault_Prop
	{
		[NullPolicy(NullAction.Default)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.Default)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.Default)] public string S { get; set; } = "d";
		[NullPolicy(NullAction.Default)] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.Default)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.Default)] public Guid G { get; set; }
		[NullPolicy(NullAction.Default)] public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberDefault_Init
	{
		[NullPolicy(NullAction.Default)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.Default)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.Default)] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.Default)] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.Default)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.Default)] public required Guid G { get; init; }
		[NullPolicy(NullAction.Default)] public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberDefault_Ctor(
		[NullPolicy(NullAction.Default)] int i = 7,
		[NullPolicy(NullAction.Default)] int? ni = 5,
		[NullPolicy(NullAction.Default)] string s = "d",
		[NullPolicy(NullAction.Default)] string? ns = "d",
		[NullPolicy(NullAction.Default)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.Default)] Guid g = default,
		[NullPolicy(NullAction.Default)] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.Throw)]
	public sealed record P_ClassThrowMemberDefault_Record(
		[NullPolicy(NullAction.Default)] int I = 7,
		[NullPolicy(NullAction.Default)] int? NI = 5,
		[NullPolicy(NullAction.Default)] string S = "d",
		[NullPolicy(NullAction.Default)] string? NS = "d",
		[NullPolicy(NullAction.Default)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.Default)] Guid G = default,
		[NullPolicy(NullAction.Default)] DateTime T = default);

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefaultMemberThrow_Prop
	{
		[NullPolicy(NullAction.Throw)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.Throw)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.Throw)] public string S { get; set; } = "d";
		[NullPolicy(NullAction.Throw)] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.Throw)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.Throw)] public Guid G { get; set; }
		[NullPolicy(NullAction.Throw)] public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefaultMemberThrow_Init
	{
		[NullPolicy(NullAction.Throw)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.Throw)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.Throw)] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.Throw)] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.Throw)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.Throw)] public required Guid G { get; init; }
		[NullPolicy(NullAction.Throw)] public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefaultMemberThrow_Ctor(
		[NullPolicy(NullAction.Throw)] int i = 7,
		[NullPolicy(NullAction.Throw)] int? ni = 5,
		[NullPolicy(NullAction.Throw)] string s = "d",
		[NullPolicy(NullAction.Throw)] string? ns = "d",
		[NullPolicy(NullAction.Throw)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.Throw)] Guid g = default,
		[NullPolicy(NullAction.Throw)] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.Default)]
	public sealed record P_ClassDefaultMemberThrow_Record(
		[NullPolicy(NullAction.Throw)] int I = 7,
		[NullPolicy(NullAction.Throw)] int? NI = 5,
		[NullPolicy(NullAction.Throw)] string S = "d",
		[NullPolicy(NullAction.Throw)] string? NS = "d",
		[NullPolicy(NullAction.Throw)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.Throw)] Guid G = default,
		[NullPolicy(NullAction.Throw)] DateTime T = default);

	[NullPolicy(NullAction.None)]
	public sealed class P_ClassNoneMemberValue_Prop
	{
		[NullValue(42)] public int I { get; set; } = 7;
		[NullValue(9)] public int? NI { get; set; } = 5;
		[NullValue("c")] public string S { get; set; } = "d";
		[NullValue("c")] public string? NS { get; set; } = "d";
		[NullValue(2)] public Tier E { get; set; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public Guid G { get; set; }
		[NullValue("2024-01-02")] public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.None)]
	public sealed class P_ClassNoneMemberValue_Init
	{
		[NullValue(42)] public required int I { get; init; } = 7;
		[NullValue(9)] public required int? NI { get; init; } = 5;
		[NullValue("c")] public required string S { get; init; } = "d";
		[NullValue("c")] public required string? NS { get; init; } = "d";
		[NullValue(2)] public required Tier E { get; init; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public required Guid G { get; init; }
		[NullValue("2024-01-02")] public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.None)]
	public sealed class P_ClassNoneMemberValue_Ctor(
		[NullValue(42)] int i = 7,
		[NullValue(9)] int? ni = 5,
		[NullValue("c")] string s = "d",
		[NullValue("c")] string? ns = "d",
		[NullValue(2)] Tier e = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid g = default,
		[NullValue("2024-01-02")] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.None)]
	public sealed record P_ClassNoneMemberValue_Record(
		[NullValue(42)] int I = 7,
		[NullValue(9)] int? NI = 5,
		[NullValue("c")] string S = "d",
		[NullValue("c")] string? NS = "d",
		[NullValue(2)] Tier E = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid G = default,
		[NullValue("2024-01-02")] DateTime T = default);

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberValue_Prop
	{
		[NullValue(42)] public int I { get; set; } = 7;
		[NullValue(9)] public int? NI { get; set; } = 5;
		[NullValue("c")] public string S { get; set; } = "d";
		[NullValue("c")] public string? NS { get; set; } = "d";
		[NullValue(2)] public Tier E { get; set; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public Guid G { get; set; }
		[NullValue("2024-01-02")] public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberValue_Init
	{
		[NullValue(42)] public required int I { get; init; } = 7;
		[NullValue(9)] public required int? NI { get; init; } = 5;
		[NullValue("c")] public required string S { get; init; } = "d";
		[NullValue("c")] public required string? NS { get; init; } = "d";
		[NullValue(2)] public required Tier E { get; init; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public required Guid G { get; init; }
		[NullValue("2024-01-02")] public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.Throw)]
	public sealed class P_ClassThrowMemberValue_Ctor(
		[NullValue(42)] int i = 7,
		[NullValue(9)] int? ni = 5,
		[NullValue("c")] string s = "d",
		[NullValue("c")] string? ns = "d",
		[NullValue(2)] Tier e = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid g = default,
		[NullValue("2024-01-02")] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.Throw)]
	public sealed record P_ClassThrowMemberValue_Record(
		[NullValue(42)] int I = 7,
		[NullValue(9)] int? NI = 5,
		[NullValue("c")] string S = "d",
		[NullValue("c")] string? NS = "d",
		[NullValue(2)] Tier E = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid G = default,
		[NullValue("2024-01-02")] DateTime T = default);

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefaultMemberValue_Prop
	{
		[NullValue(42)] public int I { get; set; } = 7;
		[NullValue(9)] public int? NI { get; set; } = 5;
		[NullValue("c")] public string S { get; set; } = "d";
		[NullValue("c")] public string? NS { get; set; } = "d";
		[NullValue(2)] public Tier E { get; set; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public Guid G { get; set; }
		[NullValue("2024-01-02")] public DateTime T { get; set; }
	}

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefaultMemberValue_Init
	{
		[NullValue(42)] public required int I { get; init; } = 7;
		[NullValue(9)] public required int? NI { get; init; } = 5;
		[NullValue("c")] public required string S { get; init; } = "d";
		[NullValue("c")] public required string? NS { get; init; } = "d";
		[NullValue(2)] public required Tier E { get; init; } = Tier.Mid;
		[NullValue("00000000-0000-0000-0000-000000000002")] public required Guid G { get; init; }
		[NullValue("2024-01-02")] public required DateTime T { get; init; }
	}

	[NullPolicy(NullAction.Default)]
	public sealed class P_ClassDefaultMemberValue_Ctor(
		[NullValue(42)] int i = 7,
		[NullValue(9)] int? ni = 5,
		[NullValue("c")] string s = "d",
		[NullValue("c")] string? ns = "d",
		[NullValue(2)] Tier e = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid g = default,
		[NullValue("2024-01-02")] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	[NullPolicy(NullAction.Default)]
	public sealed record P_ClassDefaultMemberValue_Record(
		[NullValue(42)] int I = 7,
		[NullValue(9)] int? NI = 5,
		[NullValue("c")] string S = "d",
		[NullValue("c")] string? NS = "d",
		[NullValue(2)] Tier E = Tier.Mid,
		[NullValue("00000000-0000-0000-0000-000000000002")] Guid G = default,
		[NullValue("2024-01-02")] DateTime T = default);

	public sealed class P_MemberNoneAndValue_Prop
	{
		[NullPolicy(NullAction.None), NullValue(42)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.None), NullValue(9)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.None), NullValue("c")] public string S { get; set; } = "d";
		[NullPolicy(NullAction.None), NullValue("c")] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.None), NullValue(2)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.None), NullValue("00000000-0000-0000-0000-000000000002")] public Guid G { get; set; }
		[NullPolicy(NullAction.None), NullValue("2024-01-02")] public DateTime T { get; set; }
	}

	public sealed class P_MemberNoneAndValue_Init
	{
		[NullPolicy(NullAction.None), NullValue(42)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.None), NullValue(9)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.None), NullValue("c")] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.None), NullValue("c")] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.None), NullValue(2)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.None), NullValue("00000000-0000-0000-0000-000000000002")] public required Guid G { get; init; }
		[NullPolicy(NullAction.None), NullValue("2024-01-02")] public required DateTime T { get; init; }
	}

	public sealed class P_MemberNoneAndValue_Ctor(
		[NullPolicy(NullAction.None), NullValue(42)] int i = 7,
		[NullPolicy(NullAction.None), NullValue(9)] int? ni = 5,
		[NullPolicy(NullAction.None), NullValue("c")] string s = "d",
		[NullPolicy(NullAction.None), NullValue("c")] string? ns = "d",
		[NullPolicy(NullAction.None), NullValue(2)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.None), NullValue("00000000-0000-0000-0000-000000000002")] Guid g = default,
		[NullPolicy(NullAction.None), NullValue("2024-01-02")] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_MemberNoneAndValue_Record(
		[NullPolicy(NullAction.None), NullValue(42)] int I = 7,
		[NullPolicy(NullAction.None), NullValue(9)] int? NI = 5,
		[NullPolicy(NullAction.None), NullValue("c")] string S = "d",
		[NullPolicy(NullAction.None), NullValue("c")] string? NS = "d",
		[NullPolicy(NullAction.None), NullValue(2)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.None), NullValue("00000000-0000-0000-0000-000000000002")] Guid G = default,
		[NullPolicy(NullAction.None), NullValue("2024-01-02")] DateTime T = default);

	public sealed class P_MemberThrowAndValue_Prop
	{
		[NullPolicy(NullAction.Throw), NullValue(42)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.Throw), NullValue(9)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.Throw), NullValue("c")] public string S { get; set; } = "d";
		[NullPolicy(NullAction.Throw), NullValue("c")] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.Throw), NullValue(2)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.Throw), NullValue("00000000-0000-0000-0000-000000000002")] public Guid G { get; set; }
		[NullPolicy(NullAction.Throw), NullValue("2024-01-02")] public DateTime T { get; set; }
	}

	public sealed class P_MemberThrowAndValue_Init
	{
		[NullPolicy(NullAction.Throw), NullValue(42)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.Throw), NullValue(9)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.Throw), NullValue("c")] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.Throw), NullValue("c")] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.Throw), NullValue(2)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.Throw), NullValue("00000000-0000-0000-0000-000000000002")] public required Guid G { get; init; }
		[NullPolicy(NullAction.Throw), NullValue("2024-01-02")] public required DateTime T { get; init; }
	}

	public sealed class P_MemberThrowAndValue_Ctor(
		[NullPolicy(NullAction.Throw), NullValue(42)] int i = 7,
		[NullPolicy(NullAction.Throw), NullValue(9)] int? ni = 5,
		[NullPolicy(NullAction.Throw), NullValue("c")] string s = "d",
		[NullPolicy(NullAction.Throw), NullValue("c")] string? ns = "d",
		[NullPolicy(NullAction.Throw), NullValue(2)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.Throw), NullValue("00000000-0000-0000-0000-000000000002")] Guid g = default,
		[NullPolicy(NullAction.Throw), NullValue("2024-01-02")] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_MemberThrowAndValue_Record(
		[NullPolicy(NullAction.Throw), NullValue(42)] int I = 7,
		[NullPolicy(NullAction.Throw), NullValue(9)] int? NI = 5,
		[NullPolicy(NullAction.Throw), NullValue("c")] string S = "d",
		[NullPolicy(NullAction.Throw), NullValue("c")] string? NS = "d",
		[NullPolicy(NullAction.Throw), NullValue(2)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.Throw), NullValue("00000000-0000-0000-0000-000000000002")] Guid G = default,
		[NullPolicy(NullAction.Throw), NullValue("2024-01-02")] DateTime T = default);

	public sealed class P_MemberDefaultAndValue_Prop
	{
		[NullPolicy(NullAction.Default), NullValue(42)] public int I { get; set; } = 7;
		[NullPolicy(NullAction.Default), NullValue(9)] public int? NI { get; set; } = 5;
		[NullPolicy(NullAction.Default), NullValue("c")] public string S { get; set; } = "d";
		[NullPolicy(NullAction.Default), NullValue("c")] public string? NS { get; set; } = "d";
		[NullPolicy(NullAction.Default), NullValue(2)] public Tier E { get; set; } = Tier.Mid;
		[NullPolicy(NullAction.Default), NullValue("00000000-0000-0000-0000-000000000002")] public Guid G { get; set; }
		[NullPolicy(NullAction.Default), NullValue("2024-01-02")] public DateTime T { get; set; }
	}

	public sealed class P_MemberDefaultAndValue_Init
	{
		[NullPolicy(NullAction.Default), NullValue(42)] public required int I { get; init; } = 7;
		[NullPolicy(NullAction.Default), NullValue(9)] public required int? NI { get; init; } = 5;
		[NullPolicy(NullAction.Default), NullValue("c")] public required string S { get; init; } = "d";
		[NullPolicy(NullAction.Default), NullValue("c")] public required string? NS { get; init; } = "d";
		[NullPolicy(NullAction.Default), NullValue(2)] public required Tier E { get; init; } = Tier.Mid;
		[NullPolicy(NullAction.Default), NullValue("00000000-0000-0000-0000-000000000002")] public required Guid G { get; init; }
		[NullPolicy(NullAction.Default), NullValue("2024-01-02")] public required DateTime T { get; init; }
	}

	public sealed class P_MemberDefaultAndValue_Ctor(
		[NullPolicy(NullAction.Default), NullValue(42)] int i = 7,
		[NullPolicy(NullAction.Default), NullValue(9)] int? ni = 5,
		[NullPolicy(NullAction.Default), NullValue("c")] string s = "d",
		[NullPolicy(NullAction.Default), NullValue("c")] string? ns = "d",
		[NullPolicy(NullAction.Default), NullValue(2)] Tier e = Tier.Mid,
		[NullPolicy(NullAction.Default), NullValue("00000000-0000-0000-0000-000000000002")] Guid g = default,
		[NullPolicy(NullAction.Default), NullValue("2024-01-02")] DateTime t = default)
	{
		public int I { get; } = i;
		public int? NI { get; } = ni;
		public string S { get; } = s;
		public string? NS { get; } = ns;
		public Tier E { get; } = e;
		public Guid G { get; } = g;
		public DateTime T { get; } = t;
	}

	public sealed record P_MemberDefaultAndValue_Record(
		[NullPolicy(NullAction.Default), NullValue(42)] int I = 7,
		[NullPolicy(NullAction.Default), NullValue(9)] int? NI = 5,
		[NullPolicy(NullAction.Default), NullValue("c")] string S = "d",
		[NullPolicy(NullAction.Default), NullValue("c")] string? NS = "d",
		[NullPolicy(NullAction.Default), NullValue(2)] Tier E = Tier.Mid,
		[NullPolicy(NullAction.Default), NullValue("00000000-0000-0000-0000-000000000002")] Guid G = default,
		[NullPolicy(NullAction.Default), NullValue("2024-01-02")] DateTime T = default);
}
