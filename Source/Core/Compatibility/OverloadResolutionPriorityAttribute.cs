#if !NET10_0_OR_GREATER
// Polyfill for System.Runtime.CompilerServices.OverloadResolutionPriorityAttribute.
// The attribute ships in-box on .NET 9+, but net10.0 is this library's modern target, so
// everything below it (the netstandard shims and net472) gets this internal definition.
// The C# 13+ compiler recognizes it by full name and honors a source-defined copy — verified
// on net472 (the polyfilled overload wins overload resolution exactly as the in-box one does),
// which is what lets the modern span overloads added during API modernization win uniformly
// across every target framework.
namespace System.Runtime.CompilerServices;

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
