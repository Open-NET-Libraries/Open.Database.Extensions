namespace Open.Database.Extensions;

/// <summary>
/// What a <see cref="NullPolicyAttribute"/> does when a member's column is NULL.
/// </summary>
/// <remarks>
/// <see cref="None"/> adds no check. <see cref="Throw"/> and <see cref="Default"/> test each member they cover for NULL before its value is read:
/// one NULL test per covered member per row.
/// </remarks>
/// <seealso cref="NullValueAttribute"/>
public enum NullAction
{
	/// <summary>
	/// No check: the member's type is trusted, at hand-written speed.
	/// </summary>
	/// <remarks>
	/// The same as no policy. On a member it overrides the type's policy, so one hot member can opt out.
	/// A NULL into a nullable member is <see langword="null"/>;
	/// into a non-nullable one it surfaces as the provider's own exception inside a <see cref="ValueMappingException"/> that names the model type and the row (the member and the column are not known).
	/// To find which member and column, use <see cref="Throw"/>.
	/// </remarks>
	/// <seealso cref="Throw"/>
	None,

	/// <summary>
	/// Checks the member, and a NULL throws <see cref="ValueMappingException"/> naming the member, the column and the 0-based row.
	/// </summary>
	/// <remarks>
	/// The value is not read. It costs one NULL test per covered member per row.
	/// Use it to track a NULL down, or to require a value from a nullable member.
	/// </remarks>
	/// <seealso cref="None"/>
	/// <seealso cref="Default"/>
	Throw,

	/// <summary>
	/// Checks the member, and a NULL leaves it as it is: its initializer, else <see langword="default"/>.
	/// </summary>
	/// <remarks>
	/// A constructor parameter receives its declared default value, else <see langword="default"/>.
	/// It costs one NULL test per covered member per row.
	/// For a value of your choosing, use <see cref="NullValueAttribute"/>.
	/// </remarks>
	/// <seealso cref="Throw"/>
	/// <seealso cref="None"/>
	Default,
}

/// <summary>
/// Declares what a NULL does to a member when a row is mapped: on the model type for every member, or on one member.
/// </summary>
/// <remarks>
/// <para>
/// A policy always applies, nullable members included.
/// On the type it covers every member, and a member's own policy overrides it (<see cref="NullAction.None"/> included).
/// A <see cref="NullValueAttribute"/> on a member overrides any policy, the member's own included.
/// A positional record's parameter takes the policy on the parameter or on its property.
/// A pair or a converter of a <see cref="RowMap{T}"/> that names the member is explicit and overrides the member's own declarations in that map (the type's policy still covers it);
/// a converter whose column type is a <see cref="Nullable{T}"/> receives the NULL itself, and no policy applies.
/// A converter whose column type is a reference type (<see cref="string"/>) is not called for a NULL; the member then follows the NULL rules and the type's policy.
/// A policy on a base type covers a derived model's members. The nearest declaration wins: the derived type's own policy, then its base type's;
/// a member's own policy beats both, and a <see cref="NullValueAttribute"/> beats all.
/// The declarations are read once per type, when its slots are built.
/// </para>
/// <para>
/// Every check costs one NULL test per covered member per row. A member with no policy is not checked (<see cref="NullAction.None"/>).
/// </para>
/// </remarks>
/// <example>
/// <code>
/// [NullPolicy(NullAction.Throw)]
/// public sealed class Post
/// {
///     public int Id { get; init; }
///
///     [NullValue(0)]
///     public int Likes { get; init; }
///
///     [NullPolicy(NullAction.Default)]
///     public string Title { get; init; } = "(untitled)";
/// }
/// </code>
/// </example>
/// <seealso cref="NullAction"/>
/// <seealso cref="NullValueAttribute"/>
/// <seealso cref="ValueMappingException"/>
/// <param name="action">What a NULL does: no check, a throw, or the member's default.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false, Inherited = true)]
public sealed class NullPolicyAttribute(NullAction action) : Attribute
{
	/// <summary>
	/// Gets what a NULL does.
	/// </summary>
	/// <remarks>
	/// <see cref="NullAction.Throw"/> and <see cref="NullAction.Default"/> cost one NULL test per covered member per row; <see cref="NullAction.None"/> costs nothing.
	/// </remarks>
	public NullAction Action { get; } = action;
}

/// <summary>
/// Declares the value a NULL gives a member when a row is mapped: the member is checked for NULL, and a NULL is replaced with the value.
/// </summary>
/// <remarks>
/// <para>
/// It always applies, nullable members included, and it overrides any <see cref="NullPolicyAttribute"/>: the type's, and the member's own.
/// It costs one NULL test per row.
/// A pair or a converter of a <see cref="RowMap{T}"/> that names the member overrides it in that map, as it does a member's policy.
/// A converter whose column type is a reference type (<see cref="string"/>) is not called for a NULL; the member then follows the NULL rules and the type's policy.
/// </para>
/// <para>
/// The value is a constant: <see langword="null"/>, a string, a bool, a char, a number or an enum value.
/// It is converted to the member's type (its underlying type for a <see cref="Nullable{T}"/>) as a column's value is, when the map is created:
/// text to a <see cref="DateTime"/> or a <see cref="Guid"/>, a number to an enum or to another number type.
/// A value of another kind (a <see cref="Type"/>, an array), or one that does not convert, throws <see cref="ArgumentException"/> when the map is created.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public sealed class Post
/// {
///     [NullValue(0)]
///     public int Likes { get; init; }
///
///     [NullValue("(untitled)")]
///     public string Title { get; init; } = "";
/// }
/// </code>
/// </example>
/// <seealso cref="NullPolicyAttribute"/>
/// <seealso cref="NullAction"/>
/// <param name="value">The value a NULL gives the member: <see langword="null"/>, a string, a bool, a char, a number or an enum value.</param>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
public sealed class NullValueAttribute(object? value) : Attribute
{
	/// <summary>
	/// Gets the value a NULL gives the member, as declared; the map converts it to the member's type.
	/// </summary>
	public object? Value { get; } = value;
}
