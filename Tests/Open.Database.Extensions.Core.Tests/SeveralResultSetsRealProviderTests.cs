using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Tests;

#nullable enable

// Several result sets on a real provider (SQL Server LocalDB): the library maps a set, the caller calls NextResult and maps the next set.
// The tests are reported as skipped when LocalDB is missing (LocalDbFixture).
[ExcludeFromCodeCoverage]
public static class SeveralResultSetsRealProviderTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	sealed class Item
	{
		public int Id { get; set; }
	}

	sealed class Person
	{
		public string Name { get; set; } = "";
		public int Age { get; set; }
		public string? Note { get; set; }
	}

	const string TwoSets = "select 1 as Id union all select 2 union all select 3; select 'ann' as Name, 30 as Age, null as Note union all select 'bob', 41, 'x'";

	// An error raised after the last row of the one set.
	const string RowsThenError = "select 1 as Id union all select 2; raiserror('late', 16, 1);";

	[Fact]
	public static void TwoSets_AreReadByMappingOne_CallingNextResult_AndMappingTheNext()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using SqlConnection connection = LocalDbFixture.Create();
		using SqlCommand command = new(TwoSets, connection);

		command.ExecuteReader((IDataReader reader) =>
		{
			Assert.Equal([1, 2, 3], reader.Results<Item>().ToList().Select(i => i.Id));

			Assert.True(reader.NextResult());
			List<Person> people = reader.Results<Person>().ToList();
			Assert.Equal(["ann", "bob"], people.Select(p => p.Name));
			Assert.Equal([30, 41], people.Select(p => p.Age));
			Assert.Equal([null, "x"], people.Select(p => p.Note));

			Assert.False(reader.NextResult());
		});
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static async Task TwoSets_AreReadByMappingOne_CallingNextResultAsync_AndMappingTheNext_Async()
	{
		LocalDbFixture.SkipUnlessAvailable();
		await using SqlConnection connection = LocalDbFixture.Create();
		await using SqlCommand command = new(TwoSets, connection);

		await command.ExecuteReaderAsync(async (DbDataReader reader) =>
		{
			Assert.Equal([1, 2, 3], (await reader.ResultsBufferedAsync<Item>(cancellationToken: Token)).Select(i => i.Id));

			Assert.True(await reader.NextResultAsync(Token));
			IEnumerable<Person> people = await reader.ResultsBufferedAsync<Person>(cancellationToken: Token);
			Assert.Equal(["ann", "bob"], people.Select(p => p.Name));

			Assert.False(await reader.NextResultAsync(Token));
		}, cancellationToken: Token);
		Assert.Equal(ConnectionState.Closed, connection.State);
	}

	[Fact]
	public static void AnErrorAfterTheRows_IsNotRaisedByMap_AndIsRaisedByTheCallersNextResult()
	{
		LocalDbFixture.SkipUnlessAvailable();
		using SqlConnection connection = LocalDbFixture.Create();

		// A call on a command reads one result set and stops: the late error does not reach it.
		Assert.Equal([1, 2], connection.Command(RowsThenError).Map<Item>().ToList().Select(i => i.Id));

		// The same batch through a handler: the caller's NextResult moves past the set and raises it.
		SqlException ex = Assert.Throws<SqlException>(() => connection.Command(RowsThenError).ExecuteReader(reader =>
		{
			Assert.Equal([1, 2], reader.Results<Item>().ToList().Select(i => i.Id));
			reader.NextResult();
		}));
		Assert.Contains("late", ex.Message, StringComparison.Ordinal);
	}
}
