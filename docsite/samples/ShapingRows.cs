using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The samples of the "Shaping rows" article. Each one is a recipe for getting
// the row shape you want from Map<T>(): none of them needs a new API.

// A model whose text members declare their own NULL value.
#region shape_plant_model
public class Tag
{
	public int Id { get; init; }
	public string Label { get; init; } = "";
	public string Description { get; init; } = "";
}
#endregion

// A model with a member the row never feeds.
#region shape_stamp_model
public class PostStamp
{
	public int Id { get; init; }
	public string Title { get; init; } = "";
	public string Serial { get; set; } = "";
}
#endregion

// The same, with an init-only member: only the query can feed it.
#region shape_stamp_init_model
public class PostStampInit
{
	public int Id { get; init; }
	public string Title { get; init; } = "";
	public string Serial { get; init; } = "";
}
#endregion

// A model with a column you do not want read.
#region shape_employee_model
public class Author
{
	public string Handle { get; init; } = "";
	public string City { get; init; } = "";
	public string Editor { get; init; } = "";
}
#endregion

// A model that must have a column for one member in every result.
#region shape_strict_model
public class StrictAuthor
{
	public required string Handle { get; init; }
	public string Editor { get; init; } = "";
}
#endregion

// More than 16 parameters: a constructor has no such limit.
#region shape_wide_model
public record WideReading(
	int Channel01,
	int Channel02,
	int Channel03,
	int Channel04,
	int Channel05,
	int Channel06,
	int Channel07,
	int Channel08,
	int Channel09,
	int Channel10,
	int Channel11,
	int Channel12,
	int Channel13,
	int Channel14,
	int Channel15,
	int Channel16,
	int Channel17);
#endregion

// Check-only: the first and last of the 17.
public sealed record WideEnds(int First, int Last);

public static class ShapingMaps
{
		#region shape_null_initializer
		// A NULL never overwrites a non-nullable member,
		// and the converter is not called for it:
		// Label keeps its initializer ("") and Description keeps its own.
		// The member's declaration is where its NULL value lives.
		public static RowMap<Tag> TagMap
			=> field ??= RowMap<Tag>
				.From([new(nameof(Tag.Label), "tag_name")])
				.Convert<string, string>(
					nameof(Tag.Label),
					static label
						=> label.Trim());
		#endregion

		#region shape_null_factory
		// A factory parameter declared string? receives null for a NULL,
		// so the function can decide what NULL means.
		// The pairs are not needed: tag_name matches tagName.
		public static RowMap<Tag> TagFactoryMap
			=> field ??= RowMap<Tag>.From(
				static (int id, string? tagName, string? description)
					=> new Tag
					{
						Id = id,
						Label = tagName?.Trim() ?? "",
						Description = description ?? "",
					});
		#endregion

		#region shape_ignore
		// A null column means "do not map this member":
		// City keeps its initializer,
		// even though the result has a city column.
		public static RowMap<Author> NoCityMap
			=> field ??= [
				new(nameof(Author.City), null),
			];
		#endregion

		#region shape_factory_lists
		// A factory map fills only what its function takes:
		// no member is set from a column afterwards.
		// A parameter with no column gets its declared default, else default,
		// and nothing throws.
		public static RowMap<Author> ListedOnlyMap
			=> field ??= RowMap<Author>.From(
				static (string handle, string? editor)
					=> new Author
					{
						Handle = handle,
						Editor = editor ?? "unknown",
					});
		#endregion

	#region shape_filter_factory
	// A factory may return null: the row is then a null in the result.
	// views rides along only to make the decision,
	// and no member holds it.
	public static RowMap<Post> BusyPostMap
		=> field ??= RowMap<Post>.From(
			static (int id, string title, int views)
				=> views > 20
					? new Post { Id = id, Title = title }
					: null);
	#endregion

	// Check-only: a factory is not limited to 16 parameters.
	public static RowMap<WideEnds> WideFactoryMap
		=> field ??= RowMap<WideEnds>.From(
#pragma warning disable RCS1163 // The factory takes one parameter per column on purpose: it shows that a factory is not limited to 16.
			static (
				int channel01,
				int channel02,
				int channel03,
				int channel04,
				int channel05,
				int channel06,
				int channel07,
				int channel08,
				int channel09,
				int channel10,
				int channel11,
				int channel12,
				int channel13,
				int channel14,
				int channel15,
				int channel16,
				int channel17)
				=> new WideEnds(channel01, channel17));
#pragma warning restore RCS1163
}

public static class ShapingRows
{
	/// <summary>(Re)creates the tables of these samples.</summary>
	public static async Task SeedAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		string[] statements =
		[
				"drop table if exists tags",
				"create table tags (id integer primary key, tag_name text null, description text null)",
				"insert into tags (id, tag_name, description) values (1, '  NEWS ', 'Weekly'), (2, null, null)",
				"drop table if exists authors",
				"create table authors (handle text not null, city text null, editor text null)",
				"insert into authors (handle, city, editor) values ('ada', 'London', 'Kim'), ('grace', null, 'Lee')",
		];

		foreach (string sql in statements)
		{
			await connection
				.Command(sql)
				.ExecuteNonQueryAsync(cancellationToken);
		}
	}

		public static async Task<(Tag[] Initializer, Tag[] Factory)> NullToEmptyAsync(
			SqliteConnection connection,
			CancellationToken cancellationToken)
		{
			#region shape_null_initializer_use
			Tag[] fromInitializer = await connection
				.Command("select id, tag_name, description from tags order by id")
				.Map(ShapingMaps.TagMap)
				.ToArrayAsync(cancellationToken);

			Tag[] fromFactory = await connection
				.Command("select id, tag_name, description from tags order by id")
				.Map(ShapingMaps.TagFactoryMap)
				.ToArrayAsync(cancellationToken);
			#endregion

			return (fromInitializer, fromFactory);
		}

	public static async Task<(PostStamp[] AfterClose, PostStampInit[] InQuery)> ValueFromOutsideAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		const string serial = "S-100";

		#region shape_set_after_close
		// The reader is closed when ToArrayAsync returns,
		// so the rows are yours to change.
		PostStamp[] stamps = await connection
			.Command("select id, title from posts order by id")
			.Map<PostStamp>()
			.ToArrayAsync(cancellationToken);

		foreach (PostStamp stamp in stamps)
			stamp.Serial = serial;
		#endregion

		#region shape_set_in_query
		// Or let the query return it as a column:
		// the map then fills Serial like any other member,
		// even when it is init-only.
		PostStampInit[] fromQuery = await connection
			.Command("select id, title, @serial as serial from posts order by id")
			.AddParam("serial", serial)
			.Map<PostStampInit>()
			.ToArrayAsync(cancellationToken);
		#endregion

		return (stamps, fromQuery);
	}

		public static async Task<(Author[] Ignored, Author[] Listed, Author[] NoEditorColumn)> ChooseWhatIsReadAsync(
			SqliteConnection connection,
			CancellationToken cancellationToken)
		{
			#region shape_ignore_use
			Author[] ignored = await connection
				.Command("select handle, city, editor from authors order by handle")
				.Map(ShapingMaps.NoCityMap)
				.ToArrayAsync(cancellationToken);
			#endregion

			Author[] listed = await connection
				.Command("select handle, city, editor from authors order by handle")
				.Map(ShapingMaps.ListedOnlyMap)
				.ToArrayAsync(cancellationToken);

			#region shape_factory_missing
			// No column feeds editor: it is null, so the function says "unknown".
			// The factory did not throw.
			Author[] noEditorColumn = await connection
				.Command("select handle from authors order by handle")
				.Map(ShapingMaps.ListedOnlyMap)
				.ToArrayAsync(cancellationToken);
			#endregion

			return (ignored, listed, noEditorColumn);
		}

		public static async Task<(StrictAuthor[] Found, ValueMappingException? Missing)> StrictAsync(
			SqliteConnection connection,
			CancellationToken cancellationToken)
		{
			StrictAuthor[] found = await connection
				.Command("select handle, editor from authors order by handle")
				.Map<StrictAuthor>()
				.ToArrayAsync(cancellationToken);

			#region shape_required_use
			ValueMappingException? missing = null;
			try
			{
				// No column feeds Handle, so the first row throws.
				_ = await connection
					.Command("select editor from authors order by handle")
					.Map<StrictAuthor>()
					.ToArrayAsync(cancellationToken);
			}
			catch (ValueMappingException ex)
			{
				missing = ex;
			}
			#endregion

			return (found, missing);
		}

	public static async Task<(WideReading Reading, WideEnds Ends)> WideAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		const string sql = """
			select
				1 as channel01, 2 as channel02, 3 as channel03,
				4 as channel04, 5 as channel05, 6 as channel06,
				7 as channel07, 8 as channel08, 9 as channel09,
				10 as channel10, 11 as channel11, 12 as channel12,
				13 as channel13, 14 as channel14, 15 as channel15,
				16 as channel16, 17 as channel17
			""";

		#region shape_wide_use
		// The query selects channel01 to channel17.
		// The record's constructor takes all 17: no factory is needed.
		WideReading reading = await connection
			.Command(sql)
			.Map<WideReading>()
			.SingleAsync(cancellationToken);
		#endregion

		WideEnds ends = await connection
			.Command(sql)
			.Map(ShapingMaps.WideFactoryMap)
			.SingleAsync(cancellationToken);

		return (reading, ends);
	}

	public static async Task<(Post[] InSql, Post[] AfterClose, Post[] Busy, Post[] WithNulls)> FilterAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region shape_filter_where
			// Recommended: filter in the query's WHERE.
			// The database skips the rows, so they never reach the reader.
		Post[] inSql = await connection
			.Command("select id, title from posts where views > @views order by id")
			.AddParam("views", 20)
			.Map<Post>()
			.ToArrayAsync(cancellationToken);
		#endregion

		#region shape_filter_after_close
		// When the decision needs code, read the rows and filter the array:
		// the reader is closed by then.
		Post[] all = await connection
			.Command("select id, title from posts order by id")
			.Map<Post>()
			.ToArrayAsync(cancellationToken);

		Post[] afterClose = [.. all.Where(static post => post.Id > 2)];
		#endregion

		#region shape_filter_factory_use
		// The nulls the factory returned are in the list until you drop them.
		List<Post> withNulls = await connection
			.Command("select id, title, views from posts order by id")
			.Map(ShapingMaps.BusyPostMap)
			.ToListAsync(cancellationToken);

		Post[] busy = [.. withNulls.Where(static post => post is not null)];
		#endregion

		return (inSql, afterClose, busy, [.. withNulls]);
	}

	public static async Task<(long? None, long? Some)> OneNamedColumnAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region shape_one_value
		// Select only the column you want:
		// Map<long?>() reads the first column of each row as one value.
		// max() over no rows is NULL, which is null here.
		long? none = await connection
			.Command("select max(id) from posts where author = @author")
			.AddParam("author", 99)
			.Map<long?>()
			.FirstOrDefaultAsync(cancellationToken);

		long? some = await connection
			.Command("select max(id) from posts where author = @author")
			.AddParam("author", 42)
			.Map<long?>()
			.FirstOrDefaultAsync(cancellationToken);
		#endregion

		return (none, some);
	}

	public static bool SingleValueFactoryIsRejected()
	{
		try
		{
			_ = RowMap<long>.From(static (long id) => id);
			return false;
		}
		catch (InvalidOperationException)
		{
			return true;
		}
	}
}
