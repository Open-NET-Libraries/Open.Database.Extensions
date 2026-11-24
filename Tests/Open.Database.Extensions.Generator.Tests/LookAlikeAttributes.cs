// Attributes that only share the simple names of Core's NullPolicy and NullValue: another namespace, no meaning to ODX.
// LookAlikeAttributes (ParityTests.Policy.cs) carries them, and must still be served by generated code.
namespace Open.Database.Extensions.Generator.Tests.LookAlike;

[AttributeUsage(AttributeTargets.All)]
public sealed class NullPolicyAttribute(int unrelated) : Attribute
{
	public int Unrelated { get; } = unrelated;
}

[AttributeUsage(AttributeTargets.All)]
public sealed class NullValueAttribute(object? unrelated) : Attribute
{
	public object? Unrelated { get; } = unrelated;
}
