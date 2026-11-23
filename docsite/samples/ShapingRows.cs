using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The samples of the "Shaping rows" article. Each one is a recipe for getting
// the row shape you want from Map<T>(): none of them needs a new API.

// A model whose text members declare their own NULL value.
#region shape_plant_model
public class Plant
{
	public int Id { get; init; }
	public string Code { get; init; } = "";
	public string SortDescr { get; init; } = "";
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
public class Employee
{
	public string Badge { get; init; } = "";
	public string Desk { get; init; } = "";
	public string Manager { get; init; } = "";
}
#endregion

// A model that must have a column for one member in every result.
#region shape_strict_model
public class StrictEmployee
{
	public required string Badge { get; init; }
	public string Manager { get; init; } = "";
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
	// Code keeps its initializer ("") and SortDescr keeps its own.
	// The member's declaration is where its NULL value lives.
	public static RowMap<Plant> PlantMap
		=> field ??= RowMap<Plant>
			.From([new(nameof(Plant.Code), "plant_type")])
			.Convert<string, string>(
				nameof(Plant.Code),
				static code
					=> code.Trim());
	#endregion

	#region shape_null_factory
	// A factory parameter declared string? receives null for a NULL,
	// so the function can decide what NULL means.
	// The pairs are not needed: plant_type matches plantType.
	public static RowMap<Plant> PlantFactoryMap
		=> field ??= RowMap<Plant>.From(
			static (int id, string? plantType, string? sortDescr)
				=> new Plant
				{
					Id = id,
					Code = plantType?.Trim() ?? "",
					SortDescr = sortDescr ?? "",
				});
	#endregion

	#region shape_ignore
	// A null column means "do not map this member":
	// Desk keeps its initializer,
	// even though the result has a desk column.
	// The null is the C# null, and it needs no ! after it.
	public static RowMap<Employee> NoDeskMap
		=> field ??= [
			new(nameof(Employee.Desk), null),
		];
	#endregion

	#region shape_factory_lists
	// A factory map fills only what its function takes:
	// no member is set from a column afterwards.
	// A parameter with no column gets its declared default, else default,
	// and nothing throws.
	public static RowMap<Employee> ListedOnlyMap
		=> field ??= RowMap<Employee>.From(
			static (string badge, string? manager)
				=> new Employee
				{
					Badge = badge,
					Manager = manager ?? "unknown",
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
			"drop table if exists plants",
			"create table plants (id integer primary key, plant_type text null, sort_descr text null)",
			"insert into plants (id, plant_type, sort_descr) values (1, '  PRESS ', 'Stamping'), (2, null, null)",
			"drop table if exists employees",
			"create table employees (badge text not null, desk text null, manager text null)",
			"insert into employees (badge, desk, manager) values ('E-1', 'D-9', 'Kim'), ('E-2', null, 'Lee')",
		];

		foreach (string sql in statements)
		{
			await connection
				.Command(sql)
				.ExecuteNonQueryAsync(cancellationToken);
		}
	}

	public static async Task<(Plant[] Initializer, Plant[] Factory)> NullToEmptyAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region shape_null_initializer_use
		Plant[] fromInitializer = await connection
			.Command("select id, plant_type, sort_descr from plants order by id")
			.Map(ShapingMaps.PlantMap)
			.ToArrayAsync(cancellationToken);

		Plant[] fromFactory = await connection
			.Command("select id, plant_type, sort_descr from plants order by id")
			.Map(ShapingMaps.PlantFactoryMap)
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

	public static async Task<(Employee[] Ignored, Employee[] Listed, Employee[] NoManagerColumn)> ChooseWhatIsReadAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region shape_ignore_use
		Employee[] ignored = await connection
			.Command("select badge, desk, manager from employees order by badge")
			.Map(ShapingMaps.NoDeskMap)
			.ToArrayAsync(cancellationToken);
		#endregion

		Employee[] listed = await connection
			.Command("select badge, desk, manager from employees order by badge")
			.Map(ShapingMaps.ListedOnlyMap)
			.ToArrayAsync(cancellationToken);

		#region shape_factory_missing
		// No column feeds manager: it is null, so the function says "unknown".
		// The factory did not throw.
		Employee[] noManagerColumn = await connection
			.Command("select badge from employees order by badge")
			.Map(ShapingMaps.ListedOnlyMap)
			.ToArrayAsync(cancellationToken);
		#endregion

		return (ignored, listed, noManagerColumn);
	}

	public static async Task<(StrictEmployee[] Found, ValueMappingException? Missing)> StrictAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		StrictEmployee[] found = await connection
			.Command("select badge, manager from employees order by badge")
			.Map<StrictEmployee>()
			.ToArrayAsync(cancellationToken);

		#region shape_required_use
		ValueMappingException? missing = null;
		try
		{
			// No column feeds Badge, so the first row throws.
			_ = await connection
				.Command("select manager from employees order by badge")
				.Map<StrictEmployee>()
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
		// The best place for a filter is the query's WHERE:
		// the database skips the rows, and the connection closes sooner.
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
