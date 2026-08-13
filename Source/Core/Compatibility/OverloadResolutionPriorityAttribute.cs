#if !NET10_0_OR_GREATER
// Polyfill for System.Runtime.CompilerServices.OverloadResolutionPriorityAttribute.
// The attribute ships in-box on .NET 9+, but net10.0 is this library's modern target, so
// everything below it (the netstandard shims and net472) gets this internal definition.
// The C# 13+ compiler recognizes it by full metadata name and honors a source-defined copy,
// decoding the priority from the attribute blob without requiring the type to be public or
// from the runtime. Verified on netstandard2.0 both in-assembly (the ns2.0 compiler honors it)
// and cross-assembly (a net10 consumer honors this internal attribute read from ns2.0 metadata),
// with a no-attribute negative control confirming causation. This is what lets the modern span
// overloads added during API modernization win overload resolution uniformly across every TFM.
namespace System.Runtime.CompilerServices;

// Internal (the conventional shape for a compiler-recognized attribute polyfill): the C# compiler
// honors it by full metadata name without the type being public, and Channel/Dataflow — the only
// sibling assemblies that *apply* it — see it via [InternalsVisibleTo] (see the .csproj). Keeping it
// internal also stops it leaking into the netstandard public surface (which is what tripped CS1591).
[AttributeUsage(
	AttributeTargets.Method | AttributeTargets.Constructor | AttributeTargets.Property,
	AllowMultiple = false,
	Inherited = false)]
internal sealed class OverloadResolutionPriorityAttribute(int priority) : Attribute
{
	/// <summary>The priority of the target within its overload set.</summary>
	public int Priority { get; } = priority;
}
#endif
