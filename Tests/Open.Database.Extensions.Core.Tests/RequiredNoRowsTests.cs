using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Tests;

#nullable enable

// A `required` property with no column to feed it, over a result with NO rows: which terminals report it.
// The row reader is built at the first row of a shape, buffered terminals and streams alike, so a result with no rows reports nothing
// and a result with a row throws ValueMappingException on its first row.
[ExcludeFromCodeCoverage]
public static class RequiredNoRowsTests
{
	public sealed class RequiredPost
	{
		public required int Id { get; set; }
		public required string Title { get; set; }
	}

	// A result with a column for Id only, and no rows.
	const string NoRows = "select 1 as id where 0";
	const string OneRow = "select 1 as id";

	static async Task<SqliteConnection> OpenAsync()
	{
		var connection = new SqliteConnection("Data Source=:memory:");
		await connection.OpenAsync(TestContext.Current.CancellationToken);
		return connection;
	}

	static readonly string[] Names = ["ToListAsync", "ToArrayAsync", "ToPooledBuffer", "FirstOrDefaultAsync", "Stream", "ToPooledStream"];

	static async Task<Exception?> RunAsync(string name, string sql)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		await using SqliteConnection connection = await OpenAsync();
		try
		{
			var query = connection.Command(sql).Map<RequiredPost>();
			switch (name)
			{
				case "ToListAsync": _ = await query.ToListAsync(token); break;
				case "ToArrayAsync": _ = await query.ToArrayAsync(token); break;
				case "FirstOrDefaultAsync": _ = await query.FirstOrDefaultAsync(token); break;
				case "ToPooledBuffer":
					await using (var buffer = query.ToPooledBuffer(token))
					{
						_ = await buffer.Items;
					}

					break;
				case "ToPooledStream":
					await foreach (RequiredPost _ in query.ToPooledStream(token)) { }

					break;
				default:
					await foreach (RequiredPost _ in query.Stream(Timeout.InfiniteTimeSpan, token)) { }

					break;
			}

			return null;
		}
		catch (Exception ex)
		{
			return ex;
		}
	}

	public static TheoryData<string> Terminals => [.. Names];

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task WithARow_EveryTerminalThrows_ValueMappingException(string name)
		=> Assert.IsType<ValueMappingException>(await RunAsync(name, OneRow));

	[Theory]
	[MemberData(nameof(Terminals))]
	public static async Task WithNoRows_NothingIsReported(string name)
		=> Assert.Null(await RunAsync(name, NoRows));
}
