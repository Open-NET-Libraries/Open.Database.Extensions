using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Tests;

#nullable enable

// Guards the Phase 2 change: column -> property matching uses an OrdinalIgnoreCase comparer
// (no per-column ToUpperInvariant allocation), so any column casing resolves to the property.
[ExcludeFromCodeCoverage]
public static class CaseInsensitiveMappingTests
{
	[Theory]
	[InlineData("firstname")]
	[InlineData("FIRSTNAME")]
	[InlineData("FirstName")]
	[InlineData("fIrStNaMe")]
	public static void Transformer_MatchesColumn_RegardlessOfCase(string columnName)
	{
		var queue = new Queue<object?[]>();
		queue.Enqueue(["Ada"]);
		var result = new QueryResult<Queue<object?[]>>([0], [columnName], queue);

		List<TransformerResultsTests.Person> people = [.. result.DequeueAs<TransformerResultsTests.Person>()];

		Assert.Equal("Ada", Assert.Single(people).FirstName);
	}

	[Theory]
	[InlineData("id")]
	[InlineData("ID")]
	[InlineData("Id")]
	public static void GetMatchingOrdinals_MatchesRequestedName_RegardlessOfCase(string requested)
	{
		IDataReader reader = FakeReader.Create(["Id", "Name"], [1, "a"]);
		Assert.True(reader.Read());

		(string Name, int Ordinal)[] match = reader.GetMatchingOrdinals([requested]);

		Assert.Equal([("Id", 0)], match);
	}
}
