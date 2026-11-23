using System.Linq.Expressions;

namespace Open.Database.Extensions;

/// <summary>
/// The public instance properties and fields of <typeparamref name="T"/> and a compiled getter for each, built once per type, for <c>AddParams(object)</c>.
/// </summary>
/// <typeparam name="T">The type whose members are read.</typeparam>
static class ParamSource<T>
{
	/// <summary>The names of the members.</summary>
	internal static readonly string[] Names;
	/// <summary>The compiled getter of each member, in the order of <see cref="Names"/>.</summary>
	internal static readonly Func<T, object?>[] Getters;
	/// <summary>The type of each member, in the order of <see cref="Names"/>.</summary>
	internal static readonly Type[] Types;

	// One copy per type: every command gets the same getters, so nothing is keyed or expired.
	static ParamSource()
	{
		var names = new List<string>();
		var getters = new List<Func<T, object?>>();
		var types = new List<Type>();
		var seen = new HashSet<string>(StringComparer.Ordinal);
		ParameterExpression source = Expression.Parameter(typeof(T), "source");

		foreach (MemberInfo member in typeof(T).GetMembers(BindingFlags.Public | BindingFlags.Instance))
		{
			// Readable properties without index parameters, and fields; a member hidden by name keeps the first.
			bool usable = member is PropertyInfo { CanRead: true } property
				? property.GetIndexParameters().Length == 0 && property.GetGetMethod() is not null
				: member is FieldInfo;
			if (!usable || !seen.Add(member.Name)) continue;

			Expression value = Expression.Convert(Expression.MakeMemberAccess(source, member), typeof(object));
			names.Add(member.Name);
			types.Add(member is PropertyInfo p ? p.PropertyType : ((FieldInfo)member).FieldType);
			getters.Add(Expression.Lambda<Func<T, object?>>(value, source).Compile());
		}

		Names = [.. names];
		Getters = [.. getters];
		Types = [.. types];
	}
}
