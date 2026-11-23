using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions.Generator.Models;
using System.Data;
using System.Data.SqlTypes;

namespace Open.Database.Extensions.Generator.Tests;

// NULL by the member's type. A member declared nullable (int?, string?, [AllowNull], a reference in code with annotations off) is tested and gets
// null, as the runtime map gives it: parity. A member declared non-nullable is read with its typed getter alone, as a hand-written selector reads it:
// a NULL there is exceptional, the provider's getter throws, and the generated reader reports the model and the row, the provider's exception inside.
// The runtime map keeps the member's value instead (it does not throw): that difference is the decided rule for generated code, pinned here.
// In a Debug build of the generated code, a [Conditional("DEBUG")] check before each such read throws the same ValueMappingException type itself,
// with the member and the column named (PropertyName, ColumnName), instead of waiting for the provider's getter. These tests run in both configurations.
public partial class ParityTests
{
	/// <summary>
	/// A NULL in a non-nullable member throws a ValueMappingException naming the model and the row, in Debug and in Release. Release: the provider's
	/// exception of this type inside, no member or column. Debug: the member and the column named (PropertyName, ColumnName), no inner exception.
	/// <paramref name="name"/> is the model as the generator names it (a plain type's name by default).
	/// </summary>
	static ValueMappingException NullThrows<TInner>(Exception? error, Type model, int row, string member, string column, string? name = null)
		where TInner : Exception
	{
#if DEBUG
		var e = Assert.IsType<ValueMappingException>(error);
		Assert.Null(e.InnerException);
		Assert.Equal(model, e.TargetType);
		Assert.Equal(member, e.PropertyName);
		Assert.Equal(column, e.ColumnName);
		Assert.Equal(
			$"A NULL was read into the non-nullable member {name ?? model.Name}.{member} (column '{column}') at row {row}. Declare the member nullable (int?, string?).",
			e.Message);
		return e;
#else
		return RowError<TInner>(error, model, row, name);
#endif
	}

	// The non-nullable members of PlainPost, by column: each type the generator reads alone (Int32, DateTime, String, Decimal, Boolean, Guid, Double,
	// Int64, an enum through Int32).
	static readonly int[] NonNullablePostColumns = [0, 1, 2, 3, 5, 6, 7, 10, 11];

	static object?[] PostRowWithNull(int column)
	{
		object?[] row = [.. PostRows[0]];
		row[column] = null;
		return row;
	}

	[Fact]
	public void Null_in_each_non_nullable_member_reports_the_row_in_every_read_mode()
	{
		var start = Counts.Now();
		foreach (int column in NonNullablePostColumns)
		{
			string member = PostColumns[column].Name;

			// Identity (5): the members in their order; a NULL in the middle of a result set (the row before it is read).
			var identity = new TableQuery(PostColumns, PostRows[0], PostRowWithNull(column), PostRows[2]);
			NullThrows<InvalidCastException>(Catch(() => identity.Map<PlainPost>().ToList()), typeof(PlainPost), 1, member, member);
			// The runtime map keeps the member's value instead: no exception.
			Assert.Equal(3, identity.Map(RowMap<PlainPost>.Default).ToList().Count);

			// The same on a SequentialAccess reader: the getter is the one read of the cell.
			var sequential = new TableQuery(PostColumns, PostRowWithNull(column)) { Sequential = true };
			NullThrows<InvalidCastException>(Catch(() => sequential.Map<PlainPost>().ToList()), typeof(PlainPost), 0, member, member);

			// Full (1): one column between the members.
			Col[] shifted = [.. PostColumns[..1], Col.Of<string>("Unused"), .. PostColumns[1..]];
			object?[] row = PostRowWithNull(column);
			var full = new TableQuery(shifted, [.. row[..1], "u", .. row[1..]]);
			NullThrows<InvalidCastException>(Catch(() => full.Map<PlainPost>().ToList()), typeof(PlainPost), 0, member, member);
		}

		Counts c = Counts.Now().Since(start);
		Assert.Equal(NonNullablePostColumns.Length * 2, c.Identity);
		Assert.Equal(NonNullablePostColumns.Length, c.Full);
	}

	[Fact]
	public void Null_in_a_non_nullable_member_reports_the_row_when_guarded_or_converting()
	{
		// Guarded (2): members absent, columns out of order.
		var guarded = new TableQuery([Col.Of<string>("Note"), Col.Of<int>("Id"), Col.Of<string>("Title")], ["n", 1, "t"], [null, null, "u"]);
		NullThrows<InvalidCastException>(Catch(() => guarded.Map<PlainPost>().ToList()), typeof(PlainPost), 1, "Id", "Id");

		// Converting (4): a long column into the int member.
		var converting = new TableQuery(
			[Col.Of<long>("Id"), Col.Of<string>("Title"), Col.Of<long>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")],
			[1L, "one", 2L, 1, "t", "n"],
			[null, "two", null, 2, "b", null]);
		NullThrows<InvalidCastException>(Catch(() => converting.Map<PlainRecord>().ToList()), typeof(PlainRecord), 1, "Id", "Id");
	}

	[Fact]
	public void Null_in_a_non_nullable_constructor_parameter_or_init_member_reports_the_row()
	{
		// A positional record's parameter (string Title), read into its local before the constructor runs.
		var record = new TableQuery(
			[Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<int>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")],
			[1, null, 2, 1, "t", "n"]);
		NullThrows<InvalidCastException>(Catch(() => record.Map<PlainRecord>().ToList()), typeof(PlainRecord), 0, "Title", "Title");

		// A required init-only member (int Id), and a settable one after the object initializer (decimal Price).
		Col[] init = [Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<string>("Note"), Col.Of<decimal>("Price")];
		NullThrows<InvalidCastException>(
			Catch(() => new TableQuery(init, [null, "one", null, 1m]).Map<PlainInit>().ToList()), typeof(PlainInit), 0, "Id", "Id");
		NullThrows<InvalidCastException>(
			Catch(() => new TableQuery(init, [1, "one", null, 1m], [2, "two", null, null]).Map<PlainInit>().ToList()), typeof(PlainInit), 1, "Price", "Price");
		NullThrows<InvalidCastException>(
			Catch(() => new TableQuery(init, [1, "one", null, null]) { Sequential = true }.Map<PlainInit>().ToList()), typeof(PlainInit), 0, "Price", "Price");
	}

	[Fact]
	public void Null_in_nullable_members_matches_the_runtime_in_every_shape()
	{
		// int?, string?, byte[]?, an [AllowNull] string, in identity and SequentialAccess; a nullable parameter and member of a record; an oblivious string.
		var start = Counts.Now();
		var post = new TableQuery(PostColumns, PostRows[1], PostRows[0], PostRows[1]);
		Parity.SameRows(post.Map(RowMap<PlainPost>.Default).ToList(), post.Map<PlainPost>().ToList());
		var sequential = new TableQuery(PostColumns, PostRows[1]) { Sequential = true };
		Parity.SameRows(sequential.Map(RowMap<PlainPost>.Default).ToList(), sequential.Map<PlainPost>().ToList());
		var record = new TableQuery(
			[Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<int>("Qty"), Col.Of<int>("State"), Col.Of<string>("Tag"), Col.Of<string>("Note")],
			[1, "one", null, 1, "t", null]);
		Parity.SameRows(record.Map(RowMap<PlainRecord>.Default).ToList(), record.Map<PlainRecord>().ToList());
		var oblivious = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [2, null]);
		var generated = oblivious.Map<ObliviousPost>().ToList();
		Parity.SameRows(oblivious.Map(RowMap<ObliviousPost>.Default).ToList(), generated);
		Counts c = Counts.Now().Since(start);

		Assert.Null(generated[1].Title);
		Assert.Equal(4, c.Served);
	}

	[Fact]
	public void Null_honest_twin_reads_as_V2_and_reports_a_null_string()
	{
		// GenRowT20: the benchmark twin whose strings V2 reads alone. Its rows equal the runtime's while those strings hold values,
		// NULL in its nullable members included; a NULL in Title throws, as V2's GetString would.
		Col[] catalog = [.. Catalog];
		var q = new TableQuery(catalog,
			[1, Day, "t", 12.50m, 3, true, KeyA, -0.0, "n", new byte[] { 1 }, 9L, "b", 1.10m, Day, "g", 0.25, false, KeyB, 4L, "s"],
			[2, Day, "u", 0m, null, false, KeyB, 1.5, null, null, 0L, "c", null, null, "h", 2.5, null, null, 5L, null]);
		var start = Counts.Now();
		Parity.SameRows(q.Map(RowMap<GenRowT20>.Default).ToList(), q.Map<GenRowT20>().ToList());
		Counts c = Counts.Now().Since(start);
		Served(c);
		Assert.Equal(1, c.Identity);

		var nullTitle = new TableQuery(catalog, [3, Day, null, 0m, null, false, KeyB, 1.5, null, null, 0L, "c", null, null, "h", 2.5, null, null, 5L, null]);
		NullThrows<InvalidCastException>(Catch(() => nullTitle.Map<GenRowT20>().ToList()), typeof(GenRowT20), 0, "Title", "Title");
	}

	[Fact]
	public void A_null_gives_a_ValueMappingException_in_Debug_and_Release_with_the_member_only_in_Debug()
	{
		// The same type in both configurations, so a consumer's negative test passes in both. The Debug check is [Conditional("DEBUG")]: this
		// project's Debug build compiles it into the generated code, its Release build does not (GeneratedIlTests: the Release IL is the same without it).
		var q = new TableQuery([Col.Of<int>("Id"), Col.Of<string>("Title")], [1, "one"], [null, "two"]);
		var e = Assert.IsType<ValueMappingException>(Catch(() => q.Map<PlainPost>().ToList()));
		Assert.Equal(typeof(PlainPost), e.TargetType);
		Assert.Contains("at row 1", e.Message, StringComparison.Ordinal);
#if DEBUG
		Assert.Equal("Id", e.PropertyName);
		Assert.Equal("Id", e.ColumnName);
		Assert.Null(e.InnerException); // Thrown by the check itself, and not wrapped a second time by the row's handler.
#else
		Assert.Equal("", e.PropertyName);
		Assert.Equal("", e.ColumnName);
		Assert.IsType<InvalidCastException>(e.InnerException);
#endif
	}


	// ---- real providers: what a NULL in a non-nullable member does, end to end ----

	[Fact]
	public void Null_on_Sqlite_reports_the_row_and_nullable_members_match()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		using (SqliteCommand setup = connection.CreateCommand())
		{
			setup.CommandText = """
				CREATE TABLE post (Id INTEGER, Title TEXT, Qty INTEGER, State INTEGER, Tag TEXT, Note TEXT);
				INSERT INTO post VALUES (1, 'one', NULL, 1, 'a', NULL);
				INSERT INTO post VALUES (NULL, 'two', 2, 2, 'b', 'n');
				""";
			setup.ExecuteNonQuery();
		}

		using SqliteCommand command = connection.CreateCommand();
		IDbCommand dbCommand = command;
		command.CommandText = "SELECT Id, Title, Qty, State, Tag, Note FROM post WHERE Id = 1";
		Parity.SameRows(dbCommand.Map(RowMap<PlainRecord>.Default).ToList(), dbCommand.Map<PlainRecord>().ToList());

		// Microsoft.Data.Sqlite's getter throws InvalidOperationException on a NULL (ProviderNullTests): not classified (its other failures share
		// the type), so the plain report. SQLite sorts NULL last in a descending order: the second row.
		command.CommandText = "SELECT Id, Title, Qty, State, Tag, Note FROM post ORDER BY Id DESC";
		NullThrows<InvalidOperationException>(Catch(() => dbCommand.Map<PlainRecord>().ToList()), typeof(PlainRecord), 1, "Id", "Id");
	}

	[Fact]
	public void Null_on_SqlClient_reports_a_null_read_and_nullable_members_match()
	{
		LocalDb.SkipUnlessAvailable();
		using var connection = new SqlConnection(LocalDb.ConnectionString);
		connection.Open();
		using SqlCommand command = connection.CreateCommand();
		IDbCommand dbCommand = command;

		command.CommandText = "SELECT 1 AS Id, N'one' AS Title, CAST(NULL AS int) AS Qty, 1 AS State, N'a' AS Tag, CAST(NULL AS nvarchar(10)) AS Note";
		Parity.SameRows(dbCommand.Map(RowMap<PlainRecord>.Default).ToList(), dbCommand.Map<PlainRecord>().ToList());

		// SqlClient's getter throws SqlNullValueException on a NULL (ProviderNullTests), in SequentialAccess too: the NULL message.
		command.CommandText = "SELECT 1 AS Id, CAST(NULL AS nvarchar(10)) AS Title, 2 AS Qty, 1 AS State, N'a' AS Tag, N'n' AS Note";
		NullThrows<SqlNullValueException>(Catch(() => dbCommand.Map<PlainRecord>().ToList()), typeof(PlainRecord), 0, "Title", "Title");
		using (SqlDataReader reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
			NullThrows<SqlNullValueException>(Catch(() => reader.Map<PlainRecord>().ToList()), typeof(PlainRecord), 0, "Title", "Title");
	}
}
