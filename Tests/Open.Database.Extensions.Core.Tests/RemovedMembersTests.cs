using System.Reflection;

#nullable enable

namespace Open.Database.Extensions.Tests;

// 11.0 removes every member that earlier releases marked [Obsolete] (and the Results family, DequeueEach and the per-row command terminals that Map replaces),
// so the Core package ships nothing obsolete. A member that is deprecated again in a later release is a new decision, not an accident: this fails so it is made on purpose.
[ExcludeFromCodeCoverage]
public static class RemovedMembersTests
{
	const BindingFlags All = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly;

	[Fact]
	public static void TheCorePackage_ShipsNothingObsolete()
	{
		var obsolete = new List<string>();
		foreach (Type type in typeof(CoreExtensions).Assembly.GetExportedTypes())
		{
			if (type.GetCustomAttribute<ObsoleteAttribute>() is not null) obsolete.Add(type.FullName!);
			foreach (MemberInfo member in type.GetMembers(All))
			{
				if (member.GetCustomAttribute<ObsoleteAttribute>() is not null)
					obsolete.Add($"{type.FullName}.{member.Name}");
			}
		}

		Assert.Empty(obsolete);
	}
}
