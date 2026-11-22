using System.Reflection;
using Open.Database.Extensions.Generator.Models;

namespace Open.Database.Extensions.Benchmarks.Generated;

/// <summary>
/// Gen and GenD: a plain <c>Map&lt;T&gt;()</c> with a closed T on each catalog width's twin, intercepted by the prototype generator.
/// The caller passes the command (the Benchmarks project's <c>connection.Command(Sql)</c>, as V2 builds it).
/// </summary>
/// <remarks>
/// The generated reader enters ODX's loop through the public selector overload, <c>Map(Func&lt;IDataRecord, T&gt;)</c>, the entry V2 uses;
/// it tests IsDBNull on every member whose NULL the runtime map would keep or assign (V2 tests the nullable members only) and binds its layout once per result set.
/// Twins exist for the catalog widths (1, 3, 5, 10, 20), as V2's hand-written selectors do; another width (ODX_SYN_WIDTHS, --widths) throws, as V2's Hand does.
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

	/// <summary>How many Map&lt;T&gt;() calls of this assembly the generator intercepted: the methods of its generated interceptor class (10 when it ran).</summary>
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
