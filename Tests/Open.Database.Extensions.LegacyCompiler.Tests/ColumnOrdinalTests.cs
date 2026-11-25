using System.Collections.Generic;
using System.Data;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// The (string Name, int Ordinal) tuple is ColumnOrdinal.
	// A caller that deconstructed the tuple in a foreach, or read
	// .Name and .Ordinal, compiles and runs unchanged on every target:
	// net472 (the netstandard2.0 build, C# 7.3), net8.0 (netstandard2.1, C# 9),
	// net10.0 (C# 12) and net11.0 (C# 13).
	public static class ColumnOrdinalTests
	{
		static DataTableReader Reader()
		{
			var table = new DataTable();
			table.Columns.Add("Id", typeof(int));
			table.Columns.Add("Title", typeof(string));
			table.Rows.Add(1, "First");

			DataTableReader reader = table.CreateDataReader();
			Assert.True(reader.Read());
			return reader;
		}

		static readonly string[] Requested = { "title", "id" };
		static readonly string[] Expected = { "Title:1", "Id:0" };

		[Fact]
		public static void GetOrdinalMapping_DeconstructsInAForeach()
		{
			var seen = new List<string>();
			using (DataTableReader reader = Reader())
			{
				foreach (var (name, ordinal) in reader.GetOrdinalMapping(Requested))
					seen.Add(name + ":" + ordinal);
			}

			Assert.Equal(Expected, seen);
		}

		[Fact]
		public static void MatchingOrdinals_DeconstructsInAForeach()
		{
			var seen = new List<string>();
			using (DataTableReader reader = Reader())
			{
				foreach (var (name, ordinal) in reader.MatchingOrdinals(Requested))
					seen.Add(name + ":" + ordinal);
			}

			Assert.Equal(Expected, seen);
		}

		[Fact]
		public static void OrdinalMapping_NameAndOrdinalReadAsBefore()
		{
			var seen = new List<string>();
			using (DataTableReader reader = Reader())
			{
				foreach (ColumnOrdinal column in reader.OrdinalMapping(Requested))
					seen.Add(column.Name + ":" + column.Ordinal);
			}

			Assert.Equal(Expected, seen);
		}
	}
}
