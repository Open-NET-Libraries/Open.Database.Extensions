#nullable enable

namespace Open.Database.Extensions.Tests.NullPolicyMatrix;

/// <summary>
/// One case of <see cref="Matrix.Hierarchy"/>: a NULL into one member of a derived model, alone in its row,
/// and what it gives: the member's value, or the Throw error naming the member.
/// </summary>
public sealed class HierarchyCase(Type model, string member, Type memberType, bool throws, object? value)
{
	public Type Model { get; } = model;
	public string Member { get; } = member;
	public Type MemberType { get; } = memberType;
	public bool Throws { get; } = throws;

	/// <summary>The member's value after the NULL, when it does not throw.</summary>
	public object? Value { get; } = value;

	public string Name => $"{Model.Name}.{Member}";

	/// <summary>The message of the Throw error, when it throws.</summary>
	public string Message => $"A NULL was read from column '{Member}' into '{Member}' of {Model.Name} at row 0, whose null policy is Throw.";
}

// A policy on a base type covers its derived models. The nearest declaration wins: the derived type's own policy, then the base type's;
// a member's own policy beats both, and a [NullValue] beats all.
public static partial class Matrix
{
	[NullPolicy(NullAction.Throw)]
	public class H_Base
	{
		public int Plain { get; set; } = 7;
		[NullPolicy(NullAction.Default)] public int Own { get; set; } = 8;
		[NullValue(9)] public int Valued { get; set; }
	}

	// The base type's Throw covers it.
	public sealed class H_Covered : H_Base
	{
		public int Added { get; set; } = 6;
		[NullPolicy(NullAction.None)] public int? Loose { get; set; } = 1;
	}

	// Its own Default overrides the base type's Throw.
	[NullPolicy(NullAction.Default)]
	public sealed class H_Overrides : H_Base
	{
		public int Added { get; set; } = 6;
		[NullPolicy(NullAction.Throw)] public int Strict { get; set; }
		[NullPolicy(NullAction.Throw), NullValue(5)] public int Fixed { get; set; }
	}

	public static readonly HierarchyCase[] Hierarchy =
	[
		//  model                   member     member type    throws  value
		new(typeof(H_Covered),     "Plain",   typeof(int),   true,   null),   // the base covers the derived
		new(typeof(H_Covered),     "Added",   typeof(int),   true,   null),   // ... its own members too
		new(typeof(H_Covered),     "Own",     typeof(int),   false,  8),      // the member's own policy beats the base's
		new(typeof(H_Covered),     "Valued",  typeof(int),   false,  9),      // [NullValue] beats all
		new(typeof(H_Covered),     "Loose",   typeof(int?),  false,  null),   // a member's None beats the base's Throw
		new(typeof(H_Overrides),   "Plain",   typeof(int),   false,  7),      // the derived type's Default beats the base's Throw
		new(typeof(H_Overrides),   "Added",   typeof(int),   false,  6),
		new(typeof(H_Overrides),   "Own",     typeof(int),   false,  8),
		new(typeof(H_Overrides),   "Valued",  typeof(int),   false,  9),
		new(typeof(H_Overrides),   "Strict",  typeof(int),   true,   null),   // the member's own Throw beats the derived type's Default
		new(typeof(H_Overrides),   "Fixed",   typeof(int),   false,  5),      // [NullValue] beats the member's own policy
	];
}
