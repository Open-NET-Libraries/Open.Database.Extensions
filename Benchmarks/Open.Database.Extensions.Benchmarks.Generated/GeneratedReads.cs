using System.Reflection;
using Open.Database.Extensions.Synchronous;
using Open.Database.Extensions.Generator.Models;

namespace Open.Database.Extensions.Benchmarks.Generated;

/// <summary>
/// Gen and GenD: a plain <c>Map&lt;T&gt;()</c> with a closed T on each catalog width's twin, intercepted by the prototype generator.
/// The caller passes the command (the Benchmarks project's <c>connection.Command(Sql)</c>, as V2 builds it).
/// </summary>
/// <remarks>
/// The generated reader enters ODX's loop through the public selector overload, <c>Map(Func&lt;IDataRecord, T&gt;)</c>, the entry V2 uses;
/// it tests IsDBNull on the members the types declare nullable and reads a non-nullable member unchecked (GenRowN declares Title, Body, Tag and Blob nullable,
/// which V2 reads with no test; GenRowT20 declares the strings non-nullable, as V2 reads them)
/// and binds its layout once per result set.
/// Twins exist for the catalog widths (1, 3, 5, 10, 20), as V2's hand-written selectors do; another width (ODX_SYN_WIDTHS, --widths) throws, as V2's Hand does.
/// The twins of widths 1 to 10 have only that many members, while V2 builds a full SynRow at every width. At width 20, Gen tests four members V2 does not:
/// the like-for-like bars are Gen_T against V2 and Ctl_T, and Gen against Ctl.
/// </remarks>
public static class GeneratedReads
{
	/// <summary>The undecorated twin of the width: matched by name.</summary>
	public static object Gen(IExecuteReader command, int width) => width switch
	{
		1 => command.Map<GenRow1>().ToList(),
		3 => command.Map<GenRow3>().ToList(),
		5 => command.Map<GenRow5>().ToList(),
		10 => command.Map<GenRow10>().ToList(),
		20 => command.Map<GenRow20>().ToList(),
		_ => throw new ArgumentOutOfRangeException(nameof(width), "Generated twins exist for widths 1, 3, 5, 10 and 20."),
	};

	/// <summary>The decorated twin of the width: [Column(Name, Order)] on every member, read at constant ordinals.</summary>
	public static object GenD(IExecuteReader command, int width) => width switch
	{
		1 => command.Map<GenRowD1>().ToList(),
		3 => command.Map<GenRowD3>().ToList(),
		5 => command.Map<GenRowD5>().ToList(),
		10 => command.Map<GenRowD10>().ToList(),
		20 => command.Map<GenRowD20>().ToList(),
		_ => throw new ArgumentOutOfRangeException(nameof(width), "Generated twins exist for widths 1, 3, 5, 10 and 20."),
	};

	/// <summary>Gen_T: the width-20 twin whose member types are V2's assumptions (non-nullable Title, Body, Tag): the generated reads are V2's reads.</summary>
	public static object GenT20(IExecuteReader command) => command.Map<GenRowT20>().ToList();

	/// <summary>How many Map&lt;T&gt;() calls of this assembly the generator intercepted: the methods of its generated interceptor class (11 when it ran).</summary>
	public static int InterceptedCallSites()
	{
		foreach (Type type in typeof(GeneratedReads).Assembly.GetTypes())
		{
			if (type.Namespace == "Open.Database.Extensions.Generated" && type.Name.EndsWith("OdxInterceptors", StringComparison.Ordinal))
				return type.GetMethods(BindingFlags.Public | BindingFlags.Static).Count(m => m.Name.StartsWith("Map_", StringComparison.Ordinal));
		}

		return 0;
	}
}
