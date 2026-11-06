#if !NET8_0_OR_GREATER
// Polyfill for System.Runtime.CompilerServices.CollectionBuilderAttribute (in-box from .NET 8), so RowMap<T> can be
// created from a collection expression on every target framework. Internal, like the OverloadResolutionPriority polyfill:
// the C# compiler recognizes the attribute by its full metadata name.
namespace System.Runtime.CompilerServices;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface, Inherited = false)]
internal sealed class CollectionBuilderAttribute(Type builderType, string methodName) : Attribute
{
	/// <summary>The type that has the method that builds the collection.</summary>
	public Type BuilderType { get; } = builderType;

	/// <summary>The name of the method that builds the collection.</summary>
	public string MethodName { get; } = methodName;
}
#endif
