namespace Open.Database.Extensions.Core;

// Whether a pooled array is cleared (its used part) before it goes back
// to a pool. One rule for every pooled array ODX rents (the collectors'
// chunk chain, the buffers and the streams):
//
// 1. A row type that holds references is ALWAYS cleared: a pooled array
//    must never keep a row alive. Nothing turns this off.
// 2. Otherwise (plain value rows) the query decides when it says so:
//    UsePool(pool, clearOnReturn: true) clears, and
//    UsePool(pool, clearOnReturn: false) does not, whatever the switch
//    says.
// 3. Otherwise the app-wide switch decides (default: off).
//
// The switch is the AppContext setting named below. It is read ONCE, into
// a static readonly field, so a query that never asks pays nothing for it.
// Tests do not change it: the decision is a pure method (Resolve with the
// switch as an argument), and the reading is a method of its own.
internal static class PoolClearing
{
	/// <summary>The name of the AppContext switch.</summary>
	public const string SwitchName
		= "Open.Database.Extensions.ClearPooledArrays";

	// The cached value of the switch.
	public static readonly bool All = ReadSwitch();

	/// <summary>
	/// Reads the switch from the AppContext (the one place it is read).
	/// </summary>
	public static bool ReadSwitch()
		=> AppContext.TryGetSwitch(SwitchName, out bool on) && on;

	/// <summary>
	/// Whether the used rows of an array of <typeparamref name="T"/> must
	/// be cleared, given what the query said (<see langword="null"/>:
	/// nothing, so the switch decides).
	/// </summary>
	public static bool Resolve<T>(bool? clear)
		=> Resolve<T>(clear, All);

	/// <summary>
	/// The same decision with the switch given: what the query said wins
	/// over the switch, and a row type that holds references is always
	/// cleared.
	/// </summary>
	public static bool Resolve<T>(bool? clear, bool all)
		=> (clear ?? all) || Holds<T>.References;

	// The floor: a row type that is, or contains, a reference. The
	// per-type value is a JIT constant on the shipped runtimes; where
	// the helper is absent (netstandard2.0) every type is treated as
	// holding references.
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
