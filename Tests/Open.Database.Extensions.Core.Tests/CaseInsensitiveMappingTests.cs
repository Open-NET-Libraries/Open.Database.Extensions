using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Tests;

#nullable enable

// Guards the Phase 2 change: column -> property matching uses an OrdinalIgnoreCase comparer
// (no per-column ToUpperInvariant allocation), so any column casing resolves to the property.
[ExcludeFromCodeCoverage]
public static class CaseInsensitiveMappingTests
{

	[Theory]
	[InlineData("id")]
	[InlineData("ID")]
	[InlineData("Id")]
	public static void MatchingOrdinals_MatchesRequestedName_RegardlessOfCase(string requested)
	{
		IDataReader reader = FakeReader.Create(["Id", "Name"], [1, "a"]);
		Assert.True(reader.Read());

		ColumnOrdinal[] match = reader.MatchingOrdinals([requested]).ToArray();

		Assert.Equal([new("Id", 0)], match);
	}
}
