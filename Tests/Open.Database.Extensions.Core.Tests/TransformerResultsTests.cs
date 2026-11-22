using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class TransformerResultsTests
{
	internal sealed class Person
	{
		public string? FirstName { get; set; }
		public string? LastName { get; set; }
		public int Age { get; set; }
	}

	static DataTable BuildTable()
	{
		var table = new DataTable();
		table.Columns.Add("FirstName", typeof(string));
		table.Columns.Add("last_name", typeof(string));
		table.Columns.Add("AGE", typeof(int));
		table.Rows.Add("John", "Doe", 30);
		table.Rows.Add("Jane", "Roe", 25);
		return table;
	}
}
