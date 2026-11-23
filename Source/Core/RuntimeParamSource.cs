using System.Linq.Expressions;

namespace Open.Database.Extensions;

// An unloaded type takes its entry with it. The getters take the object, so no reflection runs after the first time a type is seen.
/// <summary>
/// The public instance properties and fields of a runtime type and a compiled getter for each, built once per type, for <c>AddParams(object)</c> when the argument is typed <see cref="object"/>
/// and the static type has no members of its own. The members are found the way <see cref="ParamSource{T}"/> finds them.
/// </summary>
/// <remarks>One immutable entry per type, kept as long as the type is alive.</remarks>
sealed class RuntimeParamSource
{
	static readonly ConditionalWeakTable<Type, RuntimeParamSource> Cache = new();
	static readonly ConditionalWeakTable<Type, RuntimeParamSource>.CreateValueCallback Create = type => new RuntimeParamSource(type);

	/// <summary>Gets the members of a runtime type, found once per type.</summary>
	/// <param name="type">The runtime type.</param>
	/// <returns>The members of <paramref name="type"/>.</returns>
	internal static RuntimeParamSource Of(Type type) => Cache.GetValue(type, Create);

	/// <summary>Gets the names of the members.</summary>
	internal string[] Names { get; }
	/// <summary>Gets the compiled getter of each member, in the order of <see cref="Names"/>.</summary>
	internal Func<object, object?>[] Getters { get; }
	/// <summary>Gets the type of each member, in the order of <see cref="Names"/>.</summary>
	internal Type[] Types { get; }

	RuntimeParamSource(Type type)
	{
		var names = new List<string>();
		var getters = new List<Func<object, object?>>();
		var types = new List<Type>();
		var seen = new HashSet<string>(StringComparer.Ordinal);
		ParameterExpression source = Expression.Parameter(typeof(object), "source");
		UnaryExpression typed = Expression.Convert(source, type);

		foreach (MemberInfo member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance))
		{
			// Readable properties without index parameters, and fields; a member hidden by name keeps the first.
			bool usable = member is PropertyInfo { CanRead: true } property
				? property.GetIndexParameters().Length == 0 && property.GetGetMethod() is not null
				: member is FieldInfo;
			if (!usable) continue;

			Type memberType = member is PropertyInfo p ? p.PropertyType : ((FieldInfo)member).FieldType;
			if (!seen.Add(member.Name)) continue;

			Expression value = Expression.Convert(Expression.MakeMemberAccess(typed, member), typeof(object));
			names.Add(member.Name);
			types.Add(memberType);
			getters.Add(Expression.Lambda<Func<object, object?>>(value, source).Compile());
		}

		Names = [.. names];
		Getters = [.. getters];
		Types = [.. types];
	}
}
