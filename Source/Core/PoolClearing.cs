namespace Open.Database.Extensions.Core;

// Whether a pooled array is cleared (its used part) before it goes back
// to a pool. One rule for every pooled array ODX rents (the collectors'
// chunk chain, the buffers and the streams):
//
// 1. A row type that holds references is ALWAYS cleared: a pooled array
//    must never keep a row alive. Nothing turns this off.
// 2. Otherwise (plain value rows) the query decides when it says so:
//    UsePool(pool, clear: true) clears, UsePool(pool, clear: false) does
//    not, whatever the switch says.
// 3. Otherwise the app-wide switch decides (default: off).
//
// The switch is the AppContext setting named below. It is read ONCE, into
// a static, so a query that never asks pays nothing for it. The field is
// not readonly only so that a test can put a value in and take it out
// again (like the pool seam of ChunkChain); nothing else writes it.
internal static class PoolClearing
{
	/// <summary>The name of the AppContext switch.</summary>
	public const string SwitchName
		= "Open.Database.Extensions.ClearPooledArrays";

	// The cached value of the switch (test seam).
	internal static bool _all = ReadSwitch();

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
		=> Holds<T>.References || (clear ?? _all);

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
