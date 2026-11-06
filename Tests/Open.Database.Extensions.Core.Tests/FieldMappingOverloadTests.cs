using Person = Open.Database.Extensions.Tests.TransformerResultsTests.Person;

#nullable enable

namespace Open.Database.Extensions.Tests;
// Assumption test for the no-map call: it must compile (no ambiguity) and read by column name.
[ExcludeFromCodeCoverage]
public static class FieldMappingOverloadTests
{
	static IDataReader Reader() => FakeReader.Create(["given_name", "family_name"], ["Ada", "Lovelace"]);

	[Fact]
	public static void NoOverrides_LeavesUnmatchedNull()
		=> Assert.Null(Reader().Results<Person>().Single().FirstName);
}
