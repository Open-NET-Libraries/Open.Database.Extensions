namespace Open.Database.Extensions.Core;

/// <summary>
/// Decides whether the used part of a pooled array is cleared before it goes
/// back to a pool.
/// </summary>
/// <remarks>
/// <para>
/// One rule for every pooled array ODX rents (the collectors' chunk chain,
/// the buffers and the streams):
/// </para>
/// <list type="number">
/// <item>
/// <description>
/// A row type that holds references is <em>always</em> cleared: a pooled
/// array must never keep a row alive. Nothing turns this off.
/// </description>
/// </item>
/// <item>
/// <description>
/// Otherwise (plain value rows) the query decides when it says so:
/// <c>UsePool(pool, clearOnReturn: true)</c> clears, and
/// <c>UsePool(pool, clearOnReturn: false)</c> does not, whatever the switch
/// says.
/// </description>
/// </item>
/// <item>
/// <description>
/// Otherwise the app-wide switch decides (default: off).
/// </description>
/// </item>
/// </list>
/// <para>
/// The switch is the AppContext setting named by <see cref="SwitchName"/>.
/// It is read once, into a <see langword="static"/> <see langword="readonly"/> field, so a query that never asks
/// pays nothing for it.
/// Tests do not change it: the decision is a pure method
/// (<c>Resolve</c> with the switch as an argument), and
/// the reading is a method of its own (<see cref="ReadSwitch"/>).
/// </para>
/// </remarks>
internal static class PoolClearing
{
	/// <summary>The name of the AppContext switch.</summary>
	public const string SwitchName
		= "Open.Database.Extensions.ClearPooledArrays";

	/// <summary>The cached value of the switch.</summary>
	public static readonly bool All = ReadSwitch();

	/// <summary>
	/// Reads the switch from the AppContext (the one place it is read).
	/// </summary>
	/// <returns>
	/// <see langword="true"/> if the switch is set and on; otherwise,
	/// <see langword="false"/>.
	/// </returns>
	public static bool ReadSwitch()
		=> AppContext.TryGetSwitch(SwitchName, out bool on) && on;

	/// <summary>
	/// Determines whether the used rows of an array of
	/// <typeparamref name="T"/> must be cleared, given what the query said.
	/// </summary>
	/// <include file="Internals.docs.xml" path="docs/members[@name='row']/*" />
	/// <param name="clear">
	/// What the query said: <see langword="null"/> for nothing, so the switch
	/// decides.
	/// </param>
	/// <returns>
	/// <see langword="true"/> if the rows must be cleared; otherwise,
	/// <see langword="false"/>.
	/// </returns>
	public static bool Resolve<T>(bool? clear)
		=> Resolve<T>(clear, All);

	/// <summary>
	/// The same decision with the switch given: what the query said wins
	/// over the switch, and a row type that holds references is always
	/// cleared.
	/// </summary>
	/// <include file="Internals.docs.xml" path="docs/members[@name='row']/*" />
	/// <param name="clear">
	/// What the query said: <see langword="null"/> for nothing, so
	/// <paramref name="all"/> decides.
	/// </param>
	/// <param name="all">The value of the app-wide switch.</param>
	/// <returns>
	/// <see langword="true"/> if the rows must be cleared; otherwise,
	/// <see langword="false"/>.
	/// </returns>
	public static bool Resolve<T>(bool? clear, bool all)
		=> (clear ?? all) || Holds<T>.References;

	/// <summary>
	/// The floor: whether a row type is, or contains, a reference.
	/// </summary>
	/// <remarks>
	/// The per-type value is a JIT constant on the shipped runtimes; where the
	/// helper is absent (netstandard2.0) every type is treated as holding
	/// references.
	/// </remarks>
	/// <include file="Internals.docs.xml" path="docs/members[@name='row']/*" />
	static class Holds<T>
	{
#if NETSTANDARD2_0
		public const bool References = true;
#else
		public static readonly bool References
			= RuntimeHelpers.IsReferenceOrContainsReferences<T>();
#endif
	}
}
