#if !NET5_0_OR_GREATER
// Polyfill for System.Runtime.CompilerServices.CallerArgumentExpressionAttribute.
// The attribute ships in-box on .NET 5+, so only the netstandard shims need this internal definition.
// The C# 10+ compiler recognizes it by full metadata name, so a source-defined internal copy works
// in-assembly and, read from the library's metadata, for a consumer on any target (verified for a
// net10.0 consumer of a netstandard2.0 library: same text as the in-box attribute).
namespace System.Runtime.CompilerServices;

[AttributeUsage(AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
internal sealed class CallerArgumentExpressionAttribute(string parameterName) : Attribute
{
	/// <summary>The name of the parameter whose argument expression is captured.</summary>
	public string ParameterName { get; } = parameterName;
}
#endif
