namespace Open.Database.Extensions.Tests;

/// <summary>
/// A column of a test reader: a name and the CLR type it reports. A purpose-built readonly struct (not a ValueTuple) so the readers can keep an
/// <c>ImmutableArray&lt;ColumnDef&gt;</c> and read it by <c>ref readonly</c> or through a span. The implicit conversion from a tuple is a call-site
/// convenience only: <c>[("Id", typeof(int)), new("Name", typeof(string))]</c> converts once and nothing tuple-typed is stored.
/// </summary>
/// <param name="Name">The column name.</param>
/// <param name="Type">The CLR type the column reports (<c>GetFieldType</c>).</param>
[ExcludeFromCodeCoverage]
internal readonly record struct ColumnDef(string Name, Type Type)
{
	/// <summary>Converts a <c>(name, type)</c> tuple once, at the call site.</summary>
	public static implicit operator ColumnDef((string Name, Type Type) t) => new(t.Name, t.Type);
}
