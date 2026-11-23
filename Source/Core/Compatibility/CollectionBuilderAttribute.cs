#if !NET8_0_OR_GREATER
namespace System.Runtime.CompilerServices;

// Polyfill for System.Runtime.CompilerServices.CollectionBuilderAttribute.
// The attribute is in-box from .NET 8; this definition lets RowMap<T> be
// created from a collection expression on every target framework.
// It is internal, like the polyfill of the overload resolution priority
// attribute: the C# compiler recognizes the attribute by its full metadata
// name.
/// <summary>
/// Names the method that builds a collection from a collection expression.
/// </summary>
/// <param name="builderType">The type that has the method that builds the collection.</param>
/// <param name="methodName">The name of the method that builds the collection.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface, Inherited = false)]
internal sealed class CollectionBuilderAttribute(Type builderType, string methodName) : Attribute
{
	/// <summary>The type that has the method that builds the collection.</summary>
	public Type BuilderType { get; } = builderType;

	/// <summary>The name of the method that builds the collection.</summary>
	public string MethodName { get; } = methodName;
}
#endif
