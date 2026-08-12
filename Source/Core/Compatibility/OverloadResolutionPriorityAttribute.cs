#if !NET9_0_OR_GREATER
// Polyfill for System.Runtime.CompilerServices.OverloadResolutionPriorityAttribute,
// which ships in-box only on .NET 9+ (recognized by the C# 13+ compiler by full name).
// Defining it internally on the netstandard shims lets the modern (span) overloads added
// during API modernization win overload resolution uniformly across every target framework.
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
