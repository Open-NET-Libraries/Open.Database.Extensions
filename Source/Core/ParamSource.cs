using System.Linq.Expressions;

namespace Open.Database.Extensions;

/// <summary>
/// The public instance properties and fields of <typeparamref name="T"/> and a compiled getter for each, built once per type, for <c>AddParams(object)</c>.
/// </summary>
/// <remarks>One immutable copy per type, like the tables of <c>RowMap&lt;T&gt;</c>: a getter for a type is the same for every command, so there is nothing to key or expire.</remarks>
static class ParamSource<T>
{
	internal static readonly string[] Names;
	internal static readonly Func<T, object?>[] Getters;
	internal static readonly Type[] Types;

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
